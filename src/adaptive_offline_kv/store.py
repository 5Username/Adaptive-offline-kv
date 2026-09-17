from __future__ import annotations

import json
import os
import struct
import threading
import time
from collections import OrderedDict, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


class AdaptiveKV:
    """
    Zero-dependency adaptive key-value store.

    - Log-structured append-only writes
    - Automatic background compaction
    - Access-pattern learning + predictive prefetch
    - Crash-safe (atomic rename)
    """

    def __init__(
        self,
        path: str | Path,
        max_memory_items: int = 10_000,
        prefetch_depth: int = 8,
        compaction_threshold: int = 4,
    ):
        self.root = Path(path)
        self.root.mkdir(parents=True, exist_ok=True)

        self.data_file = self.root / "data.log"
        self.index_file = self.root / "index.json"
        self.meta_file = self.root / "meta.json"

        self.max_memory_items = max_memory_items
        self.prefetch_depth = prefetch_depth
        self.compaction_threshold = compaction_threshold

        self._lock = threading.RLock()
        self._memory: OrderedDict[str, Any] = OrderedDict()
        self._index: Dict[str, Tuple[int, int]] = {}  # key -> (offset, length)
        self._access_graph: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._last_key: Optional[str] = None
        self._write_count = 0
        self._closed = False

        self._load()
        self._start_compaction_thread()

    def put(self, key: str, value: Any) -> None:
        if self._closed:
            raise RuntimeError("Store is closed")
        with self._lock:
            payload = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            offset = self._append(key, payload)
            self._index[key] = (offset, len(payload))
            self._memory[key] = value
            self._memory.move_to_end(key)
            if len(self._memory) > self.max_memory_items:
                self._memory.popitem(last=False)

            self._record_access(key)
            self._write_count += 1
            if self._write_count % 64 == 0:
                self._persist_index()

    def get(self, key: str, default: Any = None) -> Any:
        if self._closed:
            raise RuntimeError("Store is closed")
        with self._lock:
            if key in self._memory:
                self._memory.move_to_end(key)
                self._record_access(key)
                self._prefetch(key)
                return self._memory[key]

            if key not in self._index:
                return default

            offset, length = self._index[key]
            value = self._read_at(offset, length)
            self._memory[key] = value
            self._memory.move_to_end(key)
            if len(self._memory) > self.max_memory_items:
                self._memory.popitem(last=False)

            self._record_access(key)
            self._prefetch(key)
            return value

    def delete(self, key: str) -> bool:
        with self._lock:
            existed = key in self._index or key in self._memory
            self._index.pop(key, None)
            self._memory.pop(key, None)
            self._append(key, b"__TOMBSTONE__")
            self._write_count += 1
            return existed

    def keys(self) -> List[str]:
        with self._lock:
            return list(self._index.keys())

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._persist_index()
            self._closed = True

    # ------------------------------------------------------------------
    # Internal methods
    # ------------------------------------------------------------------

    def _append(self, key: str, payload: bytes) -> int:
        key_b = key.encode("utf-8")
        header = struct.pack("<II", len(key_b), len(payload))
        with open(self.data_file, "ab") as f:
            offset = f.tell()
            f.write(header)
            f.write(key_b)
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        return offset

    def _read_at(self, offset: int, length: int) -> Any:
        with open(self.data_file, "rb") as f:
            f.seek(offset)
            key_len, val_len = struct.unpack("<II", f.read(8))
            f.read(key_len)  # skip key
            raw = f.read(val_len)
            if raw == b"__TOMBSTONE__":
                return None
            return json.loads(raw.decode("utf-8"))

    def _record_access(self, key: str) -> None:
        if self._last_key is not None and self._last_key != key:
            self._access_graph[self._last_key][key] += 1
        self._last_key = key

    def _prefetch(self, key: str) -> None:
        if key not in self._access_graph:
            return
        candidates = sorted(
            self._access_graph[key].items(),
            key=lambda x: x[1],
            reverse=True,
        )[: self.prefetch_depth]

        for next_key, _ in candidates:
            if next_key in self._memory or next_key not in self._index:
                continue
            try:
                offset, length = self._index[next_key]
                value = self._read_at(offset, length)
                self._memory[next_key] = value
                self._memory.move_to_end(next_key)
                if len(self._memory) > self.max_memory_items:
                    self._memory.popitem(last=False)
            except Exception:
                pass

    def _load(self) -> None:
        if self.index_file.exists():
            try:
                with open(self.index_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self._index = {k: tuple(v) for k, v in data.get("index", {}).items()}
                    self._access_graph = defaultdict(
                        lambda: defaultdict(int),
                        {k: defaultdict(int, v) for k, v in data.get("graph", {}).items()},
                    )
            except Exception:
                self._rebuild_index()
        else:
            self._rebuild_index()

    def _rebuild_index(self) -> None:
        self._index.clear()
        if not self.data_file.exists():
            return
        with open(self.data_file, "rb") as f:
            while True:
                header = f.read(8)
                if len(header) < 8:
                    break
                key_len, val_len = struct.unpack("<II", header)
                key = f.read(key_len).decode("utf-8")
                payload = f.read(val_len)
                offset = f.tell() - 8 - key_len - val_len
                if payload == b"__TOMBSTONE__":
                    self._index.pop(key, None)
                else:
                    self._index[key] = (offset, val_len)

    def _persist_index(self) -> None:
        tmp = self.index_file.with_suffix(".tmp")
        data = {
            "index": {k: list(v) for k, v in self._index.items()},
            "graph": {k: dict(v) for k, v in self._access_graph.items()},
        }
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, separators=(",", ":"))
        os.replace(tmp, self.index_file)

    def _start_compaction_thread(self) -> None:
        def worker():
            while not self._closed:
                time.sleep(30)
                try:
                    self._compact_if_needed()
                except Exception:
                    pass

        t = threading.Thread(target=worker, daemon=True)
        t.start()

    def _compact_if_needed(self) -> None:
        with self._lock:
            if len(self._index) < 100:
                return
            if self.data_file.exists() and self.data_file.stat().st_size < 2_000_000:
                return

            new_file = self.root / "data.compact"
            new_index: Dict[str, Tuple[int, int]] = {}

            with open(new_file, "wb") as out:
                for key, (offset, length) in list(self._index.items()):
                    try:
                        value = self._read_at(offset, length)
                        if value is None:
                            continue
                        payload = json.dumps(value, separators=(",", ":")).encode("utf-8")
                        key_b = key.encode("utf-8")
                        header = struct.pack("<II", len(key_b), len(payload))
                        pos = out.tell()
                        out.write(header)
                        out.write(key_b)
                        out.write(payload)
                        new_index[key] = (pos, len(payload))
                    except Exception:
                        continue

            os.replace(new_file, self.data_file)
            self._index = new_index
            self._persist_index()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

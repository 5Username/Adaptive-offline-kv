import time
import tempfile
from adaptive_offline_kv import AdaptiveKV

def run():
    with tempfile.TemporaryDirectory() as d:
        kv = AdaptiveKV(d)
        N = 20_000

        t0 = time.perf_counter()
        for i in range(N):
            kv.put(f"k{i}", {"i": i, "data": "x" * 32})
        t1 = time.perf_counter()
        print(f"Put {N}: {(N/(t1-t0)):.0f} ops/sec")

        t0 = time.perf_counter()
        for i in range(N):
            kv.get(f"k{i}")
        t1 = time.perf_counter()
        print(f"Get {N}: {(N/(t1-t0)):.0f} ops/sec")

        kv.close()

if __name__ == "__main__":
    run()

# Adaptive Offline KV

Ultra-lightweight, zero-dependency, adaptive key-value store with predictive prefetching and automatic compaction.

Designed for edge devices, intermittent connectivity, and low-resource environments.

## Features

- Pure Python, zero external dependencies
- Log-structured storage with automatic compaction
- Adaptive access-pattern learning + predictive prefetch
- Extremely low memory footprint
- Atomic writes + crash recovery
- Simple and clean API
- Built-in benchmarks

## Quick Start

```python
from adaptive_offline_kv import AdaptiveKV

kv = AdaptiveKV("./data")

kv.put("user:42", {"name": "Amina", "city": "Nairobi"})
print(kv.get("user:42"))

# TCP stress acceptance test

Run on Linux with PHP CLI, ext-pcntl, Python 3 and installed Composer dependencies:

```bash
python3 tests/Stress/run.py --seconds=60
```

The independent Python client uses raw loopback TCP and validates HTTP upgrade, binary echoes, fragmented messages with an interleaved Ping, Pong, clean closing handshakes and abrupt disconnects. It maintains 64 active connections and creates/closes 16 additional connections per cycle. After five warmup cycles it runs for at least 60 seconds and 50 cycles (duration configurable). The test checks exact server-side connection, disconnection and message counters, zero application errors, empty final registry, file descriptor recovery and bounded sampled RSS growth (16 MiB above post-warmup RSS).

The RSS and FD thresholds detect gross accumulation under this specific workload, not every possible leak. Sampling is after churn batches, not a continuous memory profiler. The test does not measure performance; PHPBench handles benchmark workloads. No generated output is committed.

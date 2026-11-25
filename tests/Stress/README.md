# TCP stress acceptance test

Run on Linux with PHP CLI, ext-pcntl, Python 3 and installed Composer dependencies:

```bash
python3 tests/Stress/run.py --seconds=60
```

The independent Python client uses raw loopback TCP and validates HTTP upgrade, binary echoes, fragmented messages with an interleaved Ping, Pong, clean closing handshakes and abrupt disconnects. It maintains 64 active connections and creates/closes 16 additional connections per cycle. After five warmup cycles it runs for at least 60 seconds and 50 cycles (duration configurable). The test checks exact server-side connection, disconnection and message counters, zero application errors, empty final registry, file descriptor recovery and bounded sampled RSS growth (16 MiB above post-warmup RSS).

The RSS and FD thresholds detect gross accumulation under this specific workload, not every possible leak. Sampling is after churn batches, not a continuous memory profiler. The test does not measure performance; PHPBench handles benchmark workloads. No generated output is committed.

Before warmup, a separate slow-reading client with a reduced TCP receive buffer sends six 512 KiB binary messages without consuming their echoes. Six healthy-client barriers must complete while its reads are delayed. The slow client then receives each exact echo in order and completes its closing handshake. This checks recovery and progress across connections, not the exact number of kernel-blocked writes. Acceptance always uses the checked-out source, ignoring benchmark source overrides.

For a longer run, manually dispatch the TCP stress workflow and select 600, 3600, or 7200 seconds. Ordinary pushes and pull requests still use 60 seconds. A long run can be cancelled if a newer revision is pushed to the same branch. No long-run result is implied by a successful short test.

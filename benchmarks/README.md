# Runtime microbenchmarks

Run from the project root after installing Composer dependencies:

```bash
php -d opcache.enable_cli=0 benchmarks/runtime.php
```

The optional first argument is the path to a compatible checkout's Composer autoloader. The comparative workflow uses the pre-refactor commit `2e547491d07bd0840dd0bff674b35c145fd0acc6` as its baseline and executes both revisions on one PHP 8.4 runner, with one warm-up and five measured samples per workload.

The benchmark exercises masked encoding/decoding for 64-byte and 1-MiB payloads, in-memory reception and echo of 20,000 messages, and draining a 2-MiB outgoing frame in 1-KiB nonblocking writes. Correctness checks remain enabled. It reports median, minimum and maximum wall time and peak additional PHP-managed memory. The socket double deliberately excludes network and kernel costs, so these results do not represent end-to-end network throughput or total RSS. The current revision includes its per-connection budget bookkeeping.

No hard speed threshold is applied on a shared runner; compare the individual samples, not just a single timing.

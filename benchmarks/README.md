# Runtime benchmarks

PHPBench is a development-only dependency. Install Composer dependencies and run:

```bash
composer bench
# or run a single subject
vendor/bin/phpbench run --filter=benchSmallEcho --report=aggregate
```

The suite measures frame encoding and decoding (64-byte and 1-MiB payloads), HTTP upgrade acceptance and rejection, message assembly, small-message echo, queued bursts, partial writes and connection creation/cleanup. Each subject uses explicit revolutions, warmup and seven isolated iterations. Correctness checks remain enabled. No fixed timing threshold is enforced on a shared CI runner. PHPBench reports time per revolution, peak process memory and statistical dispersion.

The comparative workflow runs the same subjects against pre-refactor commit `2e547491d07bd0840dd0bff674b35c145fd0acc6` and the checked-out revision. Both load the same Composer dependencies and use the same PHP binary on each runner. The bootstrap verifies which source tree is loaded. The environment variable `WEBSOCKET_BENCH_SOURCE` selects the source directory. For a local comparison:

```bash
git worktree add --detach /tmp/ws-baseline 2e547491d07bd0840dd0bff674b35c145fd0acc6
WEBSOCKET_BENCH_SOURCE=/tmp/ws-baseline/src vendor/bin/phpbench run --tag=baseline --report=aggregate
vendor/bin/phpbench run --tag=head --ref=baseline --report=aggregate
```

Current connection benchmarks include shared buffer budgets and the restricted application handle. The baseline uses its original public transport API. The socket double excludes kernel/network costs; these are not end-to-end throughput or total RSS measurements. Full network load testing is separate. Results are saved as CI artifacts and are not committed.

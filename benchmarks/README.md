# Runtime benchmarks

PHPBench is a development-only dependency. Install Composer dependencies and run:

```bash
composer bench
# or run a single subject
vendor/bin/phpbench run --filter=benchSmallEcho --report=aggregate
```

The comparative group measures frame encoding and decoding (64-byte and 1-MiB payloads), HTTP upgrade acceptance and rejection, message assembly, small-message echo, queued bursts, partial writes and connection creation/cleanup. The current-only runtime group measures deferred callbacks, timer registration and cancellation, pending handshakes, matched Ping/Pong and the completed closing handshake. The baseline does not implement the same runtime contracts for the latter group. Each subject uses explicit revolutions, warmup and seven isolated iterations. Correctness checks remain enabled. No fixed timing threshold is enforced on a shared CI runner. PHPBench reports time per revolution, peak process memory and statistical dispersion.

The comparative workflow runs the same subjects against pre-refactor commit `2e547491d07bd0840dd0bff674b35c145fd0acc6` and the checked-out revision. Both load the same Composer dependencies and use the same PHP binary on each runner. The bootstrap verifies which source tree is loaded. The environment variable `WEBSOCKET_BENCH_SOURCE` selects the source directory. For a local comparison:

```bash
git worktree add --detach /tmp/ws-baseline 2e547491d07bd0840dd0bff674b35c145fd0acc6
WEBSOCKET_BENCH_SOURCE=/tmp/ws-baseline/src vendor/bin/phpbench run --group=comparative --tag=baseline --report=aggregate
vendor/bin/phpbench run --group=comparative --tag=head --ref=baseline --report=aggregate
vendor/bin/phpbench run --group=runtime --report=aggregate
```

Current connection benchmarks include shared buffer budgets (one pair per fixture, reused across connections) and the restricted application handle. Frame benchmarks use independently specified wire data. Small-message subjects do not allocate unrelated large payload fixtures. The baseline uses its original public transport API. The socket double excludes kernel/network costs; these are not end-to-end throughput or total RSS measurements. The network group uses a real PHP server on an ephemeral loopback port: 512 echo messages in batches across 32 established TCP connections, and a batch of 32 sequential HTTP upgrades and complete closing handshakes. Server startup and fixture connection setup are excluded where appropriate. The network group requires loopback sockets and subprocess execution and is verified on Linux CI. These are bounded network workloads, not a sustained load or production RSS test. Run them with `vendor/bin/phpbench run --group=network --report=aggregate`. Results are saved as CI artifacts and are not committed.

The workflow also compares the frame codec, connection lifecycle, small-message and handshake hot paths against the recent pre-optimization checkpoint `f534b369829a70688c0d08b78984047624fe2122` on the same runner. The manual workflow input `checkpoint` accepts a different full commit SHA. These additional PHPBench reports distinguish recent optimization effects from changes accumulated since the original baseline.

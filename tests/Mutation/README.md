# Targeted mutation checks

Run after installing development dependencies:

```bash
python3 tests/Mutation/run.py
```

Each mutation restores a specific historical failure or weakens one tested invariant. The runner copies the source directory to a temporary location, validates that each anchor matches exactly once, checks that the original selected tests pass, verifies the autoloaded file via reflection, lints the modified source, then repeats the same tests and inspects the JUnit report. A syntax error, timeout, unmatched test, skipped test or unrelated runner failure is not counted as a successful detection. Repository files are never modified.

This is a deliberately curated regression mutation suite, not an exhaustive general-purpose mutation score or a substitute for line/branch coverage. Update a mutation deliberately when its implementation anchor changes. The CI artifacts contain the actual detected/surviving mutations.
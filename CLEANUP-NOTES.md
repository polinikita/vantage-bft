# Cleanup Notes

Record of the Job 1 (`fab local` removal) and Job 2 (repo-wide simplification pass)
work. Semantics-preserving throughout: no protocol behavior, wire format, metric
name, or CLI-surface change (except the sanctioned `fab local` task removal).

Baseline (before any change), `CARGO_BUILD_JOBS=4 cargo test --workspace -j 4 --
--test-threads=4`:
- crypto: 7 passed
- network: 6 passed
- store: 4 passed
- worker: 6 passed
- primary: 161 passed, 6 ignored
- config/metrics: 0 (no unit tests)
- 0 failures across the board

---

## Job 1 — remove `fab local`

### Investigation

Traced every symbol `benchmark/benchmark/local.py` touches to confirm it does not
share removal-eligible code with `fab remote`:

- `benchmark/fabfile.py`: `local` task — only consumer of `LocalBench`. Removed the
  task and the `from benchmark.local import LocalBench` line.
- `benchmark/benchmark/local.py` (`LocalBench`): deleted outright. Its only caller
  was `fabfile.py`'s `local` task.
- `benchmark/benchmark/config.py` (`LocalCommittee`): only constructed by
  `LocalBench.run()` (`grep -rn LocalCommittee` over the tree hits exactly the
  class definition and that one call site). Deleted.
- `benchmark/benchmark/commands.py` (`CommandMaker`): every method `LocalBench`
  calls (`cleanup`, `clean_logs`, `compile`, `generate_key`, `run_primary`,
  `run_worker`, `run_client`, `kill`, `alias_binaries`) is *also* called by
  `remote.py` (`_config`, `_update`, `_run_single`, `kill`). None removed.
  `CommandMaker.kill()` returns `'tmux kill-server'` — this is NOT local-only:
  `remote.py`'s own `kill()` method (used by `fab remote`/`fab kill`) calls it, and
  `remote.py::_background_run` independently shells out to `tmux new -d` on the
  remote host over SSH. tmux is `fab remote`'s own background-process mechanism,
  not local-only machinery — kept intact, untouched.
- `benchmark/benchmark/utils.py` (`PathMaker`, `scrape_metrics`, `Print`,
  `BenchError`): every `PathMaker` method `LocalBench` uses is also used by
  `remote.py` (committee/parameters/key/db/log/metrics file paths are identical
  between the two vehicles). `scrape_metrics` is called by both `LocalBench.run()`
  and `Bench._run_single()`. Nothing local-only found. No changes.
- `benchmark/benchmark/logs.py` (`LogParser`): used by both `LocalBench.run()` and
  `Bench._logs()` identically (same `LogParser.process(dir, faults=...)` call
  shape). No changes.
- `benchmark/benchmark/plot.py`, `aggregate.py`: operate purely on `fab remote`'s
  `results/*.txt` files (`PathMaker.result_file`/`agg_file`), never touch
  `LocalBench`/`LocalCommittee`. Kept as-is — `fab plot` stays functional.
- `benchmark/benchmark/instance.py`, `gcp_instance.py`, `settings.py`: AWS-only,
  untouched by this job.

Net result: **no tmux-dependent machinery was local-only** — `fab remote` uses tmux
too (over SSH) — so nothing beyond `local.py`'s own `_background_run`/`_kill_nodes`
methods (removed with the whole file) qualified for removal there. Nothing in
`commands.py`, `utils.py`, or `logs.py` was local-only; all of it is shared with
`fab remote` and stays.

### Changes

- Deleted `benchmark/benchmark/local.py`.
- `benchmark/benchmark/config.py`: deleted the `LocalCommittee` class.
- `benchmark/fabfile.py`: deleted the `local` task and its `LocalBench` import.
- `benchmark/README.md`: rewrote the "Local Benchmarks" section to document
  `cargo run --release --features benchmark --bin node -- local-benchmark`
  instead of `fab local` (the Rust in-process vehicle already introduced by
  PHASE2-SPEC.md #8, `node/src/local_benchmark.rs`), and adjusted the AWS section's
  cross-reference from "Run Local Benchmarks" to the new section. Did NOT touch
  PHASE*-SPEC/NOTES.md, IMPLEMENTATION-PLAN.md, or MODERNIZATION-NOTES.md — those
  are historical records of runs already performed with `fab local` and stay as-is
  per instructions.

### Verification

- `fab -l` (from the session venv) lists exactly: create, destroy, info, install,
  kill, logs, plot, remote, start, stop — `local` is gone, nothing else changed.
- `python -m py_compile` clean on every touched `.py` file.
- Dry sanity import of the remote path (`from benchmark.remote import Bench,
  BenchError`, `from benchmark.fabfile import remote, plot, kill, logs, create,
  destroy, start, stop, info, install`) succeeds with no AWS calls made.
- Full throttled suite green after this milestone (counts below).

---

## Job 2 — repo-wide simplification pass

(filled in per milestone below)

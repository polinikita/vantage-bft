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
- `config/src/lib.rs` / `metrics/src/snapshot.rs`: two code comments referencing
  the now-deleted `config.py::LocalCommittee`/`fab local` were corrected for
  accuracy (comment-only, no logic touched).

### Verification

- `fab -l` (from the session venv) lists exactly: create, destroy, info, install,
  kill, logs, plot, remote, start, stop — `local` is gone, nothing else changed.
- `python -m py_compile` clean on every touched `.py` file.
- Dry sanity import of the remote path (`from benchmark.remote import Bench,
  BenchError`, `import fabfile` and inspecting its exported tasks) succeeds with
  no AWS calls made.
- Full throttled suite green after this milestone (counts below).

---

## Job 2 — repo-wide simplification pass

### Milestone 1 — shared `Thresholds` type (committee threshold derivation)

`AgbEngine::new`, `Pacemaker::new`, `Resolver::new`, and `ControlLog::new` each
independently derived the same party-count BFT constants inline:
`f = (n-1)/3`, `f+1`, `2f+1`, and (`ControlLog` only) `n-f`. Extracted
`primary/src/vantage/threshold.rs::Thresholds` (`from_party_count(n)` /
`from_committee(&Committee)`) and switched all four constructors to compute via
it. Zero public-API signature changes — every constructor keeps its exact
existing parameter list; only the internal arithmetic is now centralized. Also
verified `Repairer`/`LaneManager` (the other named call sites) use
`Committee::quorum_threshold()`/`validity_threshold()` — the pre-existing,
already-centralized STAKE-weighted thresholds, semantically distinct from these
PARTY-COUNT thresholds (D4-3: "fast-seal thresholds count parties, not stake")
— so nothing there was actually duplicated; left untouched, and `Thresholds`'s
doc comment calls out the distinction explicitly so it isn't rediscovered as a
"duplicate" later.

Also removed `ControlLog`'s `f_parties` field (dead code: stored at
construction, never read anywhere — confirmed pre-existing via `git stash`
before this milestone's edits, i.e. not a side effect of the `Thresholds`
switch). `Thresholds::f_parties` itself is still stored on the shared type
(part of its general-purpose value even though this one caller only reads the
derived fields).

Full suite green after this milestone: crypto 7, network 6, store 4, worker 6,
primary 161/6 ignored, 0 failures.

### Milestone 2 — agb.rs/control.rs statement-census duplication (investigated,
### partial: within-file only)

Investigated: `agb.rs`'s echo/ready per-view census queries
(`ready_stage_non_grade1_count`, `noready_count`, `echo_grade1_count_for`,
`echo_any_grade_count_for`, `nonmatching_echo_count`, `matching_echo_count`) vs.
`control.rs`'s Bracha echo/ready tallies (`recheck_bracha_ready`,
`recheck_bracha_deliver`, both built on `ControlLog::tally`).

**Conclusion: these are NOT the same algorithm, so they were not unified** —
`agb.rs`'s queries all share one shape (filter a per-view `HashMap<PublicKey,
Statement>`'s values by a PREDICATE over the statement's kind/grade/payload,
then count); `control.rs`'s `tally` is a different shape (GROUP the per-round
`HashMap<PublicKey, ControlProposal>`'s values by exact value equality into
`HashMap<ControlProposal, usize>`, then look for a bucket at/above threshold —
Bracha's "matching messages" quorum-intersection rule, not a kind/grade
predicate). Forcing these into one abstraction would have papered over a real
semantic difference (predicate-count vs. value-majority-tally), exactly the
kind of unsafe unification the task flagged (by analogy with "stake vs.
party-count" for the threshold type above). `control.rs`'s `tally` was already
a single private helper reused by its 3 call sites — no duplication to remove
there.

What WAS duplicated, safely, within `agb.rs` alone: six query methods repeated
the exact `self.views.get(&view).map_or(0, |s| s.X_statements.values()
.filter(pred).count())` boilerplate, differing only in `pred`. Extracted two
private helpers, `echo_count(view, pred)` / `ready_count(view, pred)`, and
rewrote all six as one-line calls. Pure extract-method: same `HashMap` reads,
same filter predicates (copied verbatim into closures), same short-circuit on
a view with no state yet (`map_or(0, ...)`), same return type. `ready_stage_total`
(the one caller that wants an unconditional `.len()`, no predicate) was left
as its own one-liner rather than forced through `ready_count(view, |_| true)`
(that would trade a direct `.len()` for a `.filter().count()` walk of every
statement for no readability gain).

Full suite green after this milestone (same counts as Milestone 1).

### Milestone 3 — lanes.rs chain walks (partial unification, by design)

Investigated all four author-pinned height-decreasing chain walks in
`lanes.rs`: `direct_prefix_ok`, `verified_prefix_through_genesis`,
`collect_verified_chain`, `collect_verified_suffix` (the task named 3, but
`collect_verified_chain` and `collect_verified_suffix` are two distinct
functions, not one — see below).

**Did a full single-generic-walker unification — deliberately did NOT.**
Each of the four differs in at least one of: the per-node validity predicate
(`direct && payload_ok` vs. `block_ok(...)`), whether/which memoization field
it consults and where in the check order the memo produces an early
**success** exit (`direct_prefix_ok`/`verified_prefix_through_genesis` only —
`collect_verified_chain`/`collect_verified_suffix` have no memo to check), the
stop condition (genesis vs. a caller-supplied watermark, with a fork-check
only in the watermark case), the output shape (`bool` vs. `Option<Vec<Digest>>`
vs. an ascending suffix that excludes its own watermark), and mutability
(`&mut self` for the two memoized variants, `&self` for the two collectors).
Forcing all of this through one generic core would need a 4th control-flow
branch beyond "stop"/"fail"/"validate" for the memo early-success case, sitting
at a different point in the check order than in the collectors — exactly the
kind of subtle-divergence risk the task flagged. A fully generic walker would
also stop being more readable than four short, separately-auditable functions.

**What WAS extracted, safely**: the two-line author-pin + height-match check
(`entry.block.author != author` / `entry.block.height != expected_height`),
byte-for-byte identical across all four (only the failure value differs:
`false` in two, `None` in the other two, and it sits in the exact same
position in the check order in every one — right after the entry fetch,
before the per-variant validity check). Extracted as
`BlockEntry::pinned_at(author, expected_height) -> bool`, used by all four in
place of the two inline checks. Nothing else about any of the four functions'
control flow, memoization, or check order changed.

Also fixed a stale doc comment: `collect_verified_chain`'s docstring claimed
"`verified_prefix_through_genesis` is now a thin wrapper over this (no logic
duplication between the two)" — true when written, but the later D6-7 gate
amendment gave `verified_prefix_through_genesis` its own inlined,
`chain_verified`-memoized copy of the walk instead of delegating, so the
comment no longer matched the code. Corrected it to describe the actual
current relationship (comment-only change, no logic touched). Also noted
`collect_verified_chain` has zero callers anywhere in the workspace (grepped
lib and test sources) — its own doc/PHASE4-SPEC.md #9 intent explicitly says
it's kept as public API for a future caller needing the raw hash sequence, not
accidental leftover, so it was NOT deleted as dead code; the comment now says
so plainly instead of vaguely gesturing at "shape parallel with
`collect_verified_suffix`".

Full suite green after this milestone (same counts as Milestone 1).

### Milestone 4 — node.rs: extract-method on `run`'s select body and
### `execute`'s match arms

`VantageCore::run`'s `tokio::select!` body and `execute`'s effect-dispatch
`match` had grown large. Extracted, all pure code motion (no reordering, no
new conditionals):

- **Timer firing**: the `agb_sleep`/`control_sleep` branches' pop-and-fire
  loops (each ~25-35 lines, inline in `run`) became `fire_agb_timers(now) ->
  Vec<Effect>` / `fire_control_timers(now) -> Vec<Effect>`; the select
  branches now just call the method and `self.execute(effects, now).await`.
- **Payload-sync bookkeeping**: the `rx_payload_ready` branch's inline
  set-bookkeeping + conditional `set_payload_ready`/`recheck_all`/`execute`
  became `on_payload_ready(header_digest, digest, worker_id)`.
- **Header-sealing dedup**: the `rx_our_digests` branch's inner-if body and the
  `header_timer` branch's ENTIRE body were byte-for-byte identical (drain
  `self.digests`, zero `payload_size`, `publish_own`, `execute`) — genuine
  duplication, not just a similar shape. Extracted as `seal_own_header(now)`;
  each select branch now calls it then resets `header_timer` itself (a local
  pinned future owned by `run`, not a struct field, so the reset call couldn't
  move into the method without threading a `Pin<&mut Sleep>` param through for
  no real benefit).
- **`execute`'s ~19 "serialize a `PrimaryMessage` variant + broadcast/send_to
  one peer" arms**: every one of these differed only in which
  `PrimaryMessage` variant to build and its constructor arguments (order
  preserved exactly) — extracted `broadcast_message(message)` /
  `send_message(peer, message)`, replacing `let bytes = bincode::serialize
  (&PrimaryMessage::X(...)).expect("serializes"); self.broadcast(Bytes::from
  (bytes)).await;` (repeated ~14 times) and the `send_to` analog (~5 times)
  with one-line calls. Removes ~40 lines of repeated boilerplate; the wire
  message each arm constructs, and whether it broadcasts or unicasts, is
  unchanged.

Left the other `execute` arms (`BlockCached`, `Fixed`, `CompletionReportable`,
`ApplyAnchor`, etc.) as inline match bodies — each is already a short,
single-concern, well-commented block with no boilerplate shared across arms;
extracting them into named methods would have been pure indirection (a rename
of `queue.extend(x)` to `queue.extend(self.on_x(...))`) with no dedup or
clarity gain, so left alone per the "never behavior change... don't force it"
standard.

Full suite green after this milestone (same counts as Milestone 1).

### Milestone 4.5 — agb.rs module split: investigated, NOT done

`agb.rs` is the largest vantage file (~1230 lines). Inspected its top-level
structure to judge whether a "wire types / engine / meta" split would be a
pure move:

- Lines ~1-220: genuinely separable — public wire/value types with no engine
  state (`ResolutionEntry`, `ViewProposal`, `Echo`, `ReadyGrade`, `Ready`,
  `Outcome`, `TimerKind`, `ResponseStage`) plus a few free pure functions
  (`formed`, `aux_refs`, `proposer`). A `wire.rs` submodule here looks
  achievable as a pure move.
- Lines ~220-296: private per-view state representations (`Fixed`,
  `EchoStatement`, `ReadyStatement`, `Lock`, `ViewState` + its `Default`) used
  ONLY by the engine below.
- Lines ~296-1230: `pub struct AgbEngine` + one ~930-line `impl` block — R1-R4
  lifecycle, fast-seal/lock logic, timer arming, metrics, and every per-view
  query accessor, all genuinely one cohesive state machine with no further
  clean 2-way cut inside it (there is no separable "meta" concern distinct
  from "engine" here — the module plan's own framing is a single per-view
  engine, not three).

Decided NOT to perform the split. A 2-way wire/engine cut is plausible, but the
task's own conditional ("ONLY if a pure move ... otherwise leave") plus the
cost/benefit here tipped it: moving ~1230 lines across a new module boundary
in an audited file, re-verifying every cross-file `use crate::vantage::agb::
{...}` path (control.rs, resolve.rs, node.rs, tests) still resolves
identically, for a file that already has clear section-comment boundaries and
no reported navigability complaint, is meaningful risk for an organizational
payoff only. Recorded the wire.rs/engine.rs boundary analysis above so a
future pass doesn't have to redo this reconnaissance.

### Milestone 5 — local_benchmark.rs per-node builder helpers

`run`'s per-node loop (spawn primary, then spawn `workers` workers + one
client task each) was ~85 lines inline. Extracted `spawn_node_primary(i,
keypair, node_dir, committee, parameters) -> Result<(Registry, Reporter,
(label, addr))>` and `spawn_node_workers(i, name, node_dir, workers,
committee, parameters, tx_size, rate_share, mode, all_worker_addresses) ->
Result<Vec<(Registry, Reporter, (label, addr))>>`, pure code motion (identical
channel wiring, identical `Primary::spawn`/`Worker::spawn`/`Client` calls in
the same order, identical no-op `rx_output` drain task, identical
`metrics_targets`/`primary_metrics`/`worker_metrics` push order preserved by
the caller). `run`'s loop body is now ~20 lines.

Caveat surfaced by this move: `local_benchmark.rs` is compiled only under
`--features benchmark` (`#[cfg(feature = "benchmark")] mod local_benchmark;`
in `main.rs`), so a plain `cargo build --workspace` silently skips it
entirely — it built "successfully" with a stale/cached artifact even after
this edit until verified with `cargo build -p node --features benchmark`
explicitly (which caught one real error: `PublicKey` needed importing from
`crypto`, since the extracted `spawn_node_workers` signature names it
explicitly where the original inline code only ever used `keypair.name` by
field access). Recorded here since every other milestone's `cargo build
--workspace`/`cargo test --workspace` gate does NOT exercise this file at
all — this milestone's build/smoke verification is the only one that does.

Verification beyond the standard suite (this file has no unit tests): built
`--release --features benchmark` and ran `node local-benchmark --nodes 4
--workers 1 --rate 5000 --duration 5 --protocol vantage` both before (`git
stash`) and after this change — matching consensus TPS/latency/seal-route
shape (24,676 sample txs both runs, ~42ms avg latency both). The post-RESULTS
`panicked at .../reliable_sender.rs:91` messages are pre-existing
shutdown-teardown noise (background tasks' channels closing when the process
exits non-gracefully) reproduced identically on the pre-change build — not a
regression.

Full suite green after this milestone (same counts as Milestone 1).

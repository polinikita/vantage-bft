# Phase 2 notes — deviations, decisions, inventory

Companion to `PHASE2-SPEC.md` (same role `MODERNIZATION-NOTES.md` played for Phase 1).
No git commits were made (working tree left dirty for review). This file is written
incrementally as the phase progresses; see the end for final gate results.

---

## 3. blake3 digests — correction to the assumed frontier

The work order I started from claimed three "remaining live Sha512 sites":
`primary/src/committer.rs:257`, `primary/src/messages.rs:1093`, `primary/src/messages.rs:1216`
(with `messages.rs:700` and `:951` called out separately as inert comments to also clean).

On inspection this is not what's on disk. Rust block comments (`/* */`) nest/span
arbitrarily, and a plain `grep` for the string `Sha512` cannot tell live code from code
inside a still-open block comment. Tracing actual comment delimiters:

- `primary/src/messages.rs`: a single `/* ... */` runs from line 1027
  (`/*#[derive(Serialize, Deserialize, Default, Clone)]`) to line 1228 (`}*/`) with no
  intervening delimiters. It encloses a legacy `Block`/`AcceptVote` pair (pre-Autobahn,
  HotStuff-style `qc`/`tc`/`view` shape, superseded by the live `Header`/`Certificate`/
  `QC`/`AcceptVote` used everywhere else in the file) — and therefore both `:1093` and
  `:1216` are dead, not live.
- `primary/src/committer.rs`: `fn order_dag` (a Bullshark/Tusk-style DAG pre-order-flatten
  helper — the doc comment even says so) has its entire body commented out, lines 204–304
  (`/*let mut already_ordered = HashSet::new();` … `ordered.sort_by_key(|x| x.round());*/`),
  live body reduced to `let ordered = Vec::new(); /* dead */ ordered`. Confirmed
  additionally by grep: `order_dag` has zero call sites anywhere in `primary/src` besides
  its own definition — it is unreferenced dead code inside a struct (`Committer`) that
  is otherwise live (`Committer::spawn` from `primary/src/primary.rs:211`). `:257` sits
  inside this dead body.

So every one of `committer.rs:257`, `messages.rs:700/951/1093/1216` is inside a comment.
**There are zero live `Sha512`/`ed25519_dalek` references left in `primary/src` or
`worker/src`.** The §3 gate (`grep -rn "Sha512\|ed25519_dalek" --include="*.rs" primary/src
worker/src` → only comment hits) is already satisfied on disk; `Cargo.lock` carries exactly
one `ed25519-dalek` (2.2.0), and the Path-A remnants are gone (`curve25519-dalek` only
4.1.3, `sha2` only 0.10.9, `rand`/`rand_core` only 0.8.7/0.6.4 — no 3.2.1/0.9.9/0.7.3
duplicates).

**Decision:** left all of the above comments untouched, including `committer.rs`'s —
the spec's explicit instruction for `messages.rs` ("plus commented ones — leave comments
alone") applies for the same reason (genuinely dead, deliberately-retained legacy code,
consistent with this codebase's general practice of keeping superseded implementations as
comments rather than deleting them) to `committer.rs`'s dead `order_dag` body. No `.rs`
edit was made for §3. If the intent was actually to delete these comment blocks outright
(as opposed to migrating live hash calls), that's a distinct, larger cleanup decision
than what "replace every SHA-512 hash site with blake3" calls for — flagging rather than
guessing.

## 6. Test rot — six ignored `core_tests`, sleep fix, harness refresh

### Six-test inventory (ratified disposition: `#[ignore]`, not deleted, not fixed)

All in `primary/src/tests/core_tests.rs`. Exact attribute text quoted from source.

| Test | Line | Reason |
|---|---|---|
| `process_header` | 15 | `#[ignore = "pre-existing: stale expectation that the Core's first broadcast is a bare Vote; the current ride-share/parallel-proposal path emits otherwise, so the assertion fails; predates Phase 2 (see PHASE2-NOTES.md core_tests inventory)"]` |
| `process_prepare` | 603 | `#[ignore = "pre-existing single-Core integration test: awaits a 2f+1 quorum outcome that never forms with one Core, so it hangs; predates Phase 2 (see PHASE2-NOTES.md core_tests inventory)"]` |
| `generate_confirm` | 736 | `#[ignore = "pre-existing single-Core integration test: awaits a 2f+1 quorum confirm outcome that never forms with one Core, so it hangs; predates Phase 2 (see PHASE2-NOTES.md core_tests inventory)"]` |
| `generate_commit` | 885 | `#[ignore = "pre-existing single-Core integration test: awaits a 2f+1 quorum commit outcome that never forms with one Core, so it hangs; predates Phase 2 (see PHASE2-NOTES.md core_tests inventory)"]` |
| `generate_pipelined_prepare` | 1091 | `#[ignore = "pre-existing single-Core integration test: awaits a 2f+1 quorum pipelined-prepare outcome that never forms with one Core, so it hangs; predates Phase 2 (see PHASE2-NOTES.md core_tests inventory)"]` |
| `sync_missing_proposals` | 1351 | `#[ignore = "pre-existing single-Core integration test: awaits a 2f+1 quorum sync outcome that never forms with one Core, so it hangs; predates Phase 2 (see PHASE2-NOTES.md core_tests inventory)"]` |

Disposition per spec §6: these document intended Prepare/Confirm/Commit behavior for
later phases (3+, which build the multi-Core/mocked-quorum harness these tests actually
need); not deleted, not repaired with protocol-logic changes, no multi-Core harness built
in Phase 2. The other six of the original twelve E0061 failures (arg-count drift, 24→27)
are fixed and pass: `process_certificates`, `process_header_invalid_height`,
`process_header_missing_parent`, `process_header_missing_payload`, `process_votes`,
`local_timeout_view` — plus `proposer_tests` (see below).

### `propose_normal` race — sleep fix applied

Pre-existing race, same family as the one `propose_payload` already guards against:
`Proposer` starts with `last_parent = Some(genesis)` / height 0; if the worker-digest
message is processed before the test's own parent-certificate message lands, the
proposer seals a height-0 header against stale parent state instead of the intended
sequence. `propose_payload` avoids this with `sleep(Duration::from_millis(500)).await;`
between sending the genesis cert and sending the digest; `propose_normal` lacked it.

Applied the identical fix — same sleep, same placement (immediately after the genesis-cert
send, before the digest send) — to `propose_normal` in
`primary/src/tests/proposer_tests.rs`. Test-only; no proposer/protocol source touched.
`cargo test -p primary --lib` after the change: 11 passed, 6 ignored, 0 failed (unchanged
pass count, race window closed).

### QuorumWaiter removal (done, predates this session)

`worker/src/quorum_waiter.rs` and `worker/src/tests/quorum_waiter_tests.rs` deleted;
`QuorumWaiterMessage` fully gone. `batch_maker_tests.rs` rewritten against the actual
`Vec<u8>` digest channel `batch_maker.rs` sends today. Verified: `grep -rn
"QuorumWaiter|quorum_waiter" worker/` hits only inside comments (dead wiring left as
commentary in `batch_maker.rs`/`worker.rs`, same "keep as historical comment" style as
§3). `cargo test -p worker --lib`: 6/6 pass.

### `benchmark/requirements.txt` refresh

Was `boto3==1.16.0` / `fabric==2.6.0` / `matplotlib==3.3.4` (none installable against
Python 3.12; `google-cloud-compute` needed by `gcp_instance.py` was never listed — same
staleness MODERNIZATION-NOTES.md §6 flagged for Phase 1's ad hoc venv). Refreshed to the
versions actually verified in the session venv (fabric 3.2.3 running the fabfile
unmodified, per spec assumption):

```
boto3==1.43.53
fabric>=3.2
matplotlib==3.11.1
google-cloud-compute==1.50.0
```

---

## 1. Protocol enum

`config/src/lib.rs` (`Protocol` enum, `Default`, `Parameters::protocol` with
`#[serde(default)]`, `reconcile_protocol()`, the `info!("Protocol: {:?}", ..)` log line)
and `node/src/main.rs` (calls `reconcile_protocol()` after loading parameters; matches on
`parameters.protocol`, both Autobahn variants fall through to the existing assembly,
`Vantage` returns `anyhow::anyhow!("...implemented in Phase 3...")`) were already fully
correct on disk — verified against every bullet in spec §1, no `.rs` changes needed.

The harness half was not done; fixed:
- `benchmark/fabfile.py`: both `local` and `remote` tasks' `node_params` had
  `'use_optimistic_tips': True` where the spec wants `'protocol': 'autobahn-optimistic'`
  (protocol subsumes the flag now that `reconcile_protocol` derives it). Replaced both
  (identical line in both tasks).
- `benchmark/benchmark/config.py::NodeParameters`: confirmed unknown keys pass through
  unchanged (`__init__` only type-checks a fixed required subset into `inputs`, then
  stores the whole `json` dict as `self.json`, which is what `print()` dumps to the
  parameters file) — no change needed, matches the spec's "verified" claim.
- `benchmark/benchmark/logs.py`: added `'protocol': search(r'Protocol: (\w+)', log).group(1)`
  to `_parse_primaries`'s `configs` dict (the `Protocol: {:?}` line's `Debug` output is a
  bare identifier like `AutobahnOptimistic`, so a plain `\w+` capture suffices) and added
  `f' Protocol: {protocol}\n'` as the first line of the SUMMARY `+ CONFIG:` block.

## 2. `autobahn-seamless` — audit result: already complete, no gap found

Full inventory of every `use_optimistic_tips` read in `primary/src` (six sites in
`core.rs`: 270/272 own-header tip update, 382/398 received-header tip update, 872
proposal-cut construction, 1503 `enough_coverage`) and every `current_certified_tips`
touch point (same sites plus the two genesis-initialization assignments) traced by hand.
Each branch is symmetric with its `current_proposal_tips` counterpart and internally
consistent:

- Tip bookkeeping (`process_own_header`, `process_header`) inserts into
  `current_certified_tips` from `header.parent_cert.height`/`.header_digest` under
  seamless — i.e., the *parent's certificate* height, not the header's own (not yet
  certified) height — mirroring the optimistic branch's raw-tip insert one-for-one.
- `set_consensus_proposal` (cut construction for a Prepare this node originates) and
  `enough_coverage` both already branch on `use_optimistic_tips` to pick the right map.
- Downstream consumers that the spec flagged for audit —
  `Synchronizer::get_proposals`/`missing_payload` (sync/fetch of proposal payloads),
  `is_valid`/`process_prepare_message` (incoming-proposal handling), `try_prepare_waiting_slots`/
  `is_prepare_ticket_ready` (ticket/waiting-slot re-checks) — all operate generically on
  `proposals: HashMap<PublicKey, Proposal>` by header digest, independent of which
  protocol populated the map, so none needed a new branch. In particular
  `Synchronizer::get_proposals` only resolves *header* bytes by digest (never a
  certificate/QC); certification of the parent car is established once, earlier, in
  `process_header`'s existing `ensure!(stake >= validity_threshold() || height == 0, ...)`
  check on `header.parent_cert.votes` (an f+1 check, not 2f+1 — pre-existing Autobahn
  definition of "certified enough to chain from," unconditional on protocol, untouched).
- One intentional asymmetry, not a gap: an author inserts its **own** just-created header
  into `current_certified_tips` immediately (`process_own_header`, comment: "Add early
  here, so that enough coverage will include leader tip") — i.e., before that header's own
  certificate exists. This is deliberate, pre-existing Autobahn design (a node trusting its
  own tip needs no external certificate) and applies symmetrically to both protocols; it
  is exactly the case the new invariant assert (below) excludes.
- The `//TODO: Fix check coverage as well. (new proposals..)` comment at `core.rs:397`
  (right above the certified-tips update) is old and unresolved in
  `upstream/autobahn-blips` too (same wording, checked read-only per the spec's
  suggestion) — a long-standing author's note, not evidence of an active gap in `main`.

**Cross-check against `upstream/autobahn-blips`** (read-only, per spec): the branch shares
a merge-base with `main` at `d0331d9` (already an ancestor of `main`'s current HEAD — the
certified-tips groundwork is not something to port *from* blips, `main` already descends
from it) and then diverges by 933 lines in `core.rs` alone. Blips solves incoming-proposal
readiness with an extra `Synchronizer::optimistic_tips_ready` special case
(`self.use_optimistic_tips && !optimistic_tips_ready(..)` vs. an unconditional
`get_proposals(..)` otherwise); `main` has no such method and instead calls the generic,
protocol-agnostic `get_proposals` unconditionally from `is_consensus_ready` for every
Prepare. Since `get_proposals` only ever resolves header bytes (never certificates), this
is not a missing feature in `main` — it's a simpler unified design that makes blips'
special case unnecessary. Concluded `main`'s implementation is not a subset of blips
lacking a specific piece; it is a different (and, for this narrow question, sufficient)
design. Nothing ported from blips, consistent with "reuse-first / no parallel machinery."

**Conclusion: no source gap found; no protocol-logic edits made.** The only change is the
requested invariant check:

- `primary/src/core.rs::set_consensus_proposal`: added a `debug_assert!` right after the
  cut's proposals are finalized (own-tip insert included), gated on
  `!self.use_optimistic_tips`, checking every entry except `self.name` satisfies
  `proposal.height <= current_certified_tips[pk].height`. Holds trivially today (the map
  is a fresh clone of `current_certified_tips` at that point) — its value is as a
  regression canary against a future change that lets an uncertified height leak into a
  seamless cut, which is exactly the property spec §2 names.

**Caveat for the fab-local seamless gate:** the workspace has no `[profile.release]`
section, so `debug_assertions` is off by default in the `--release` build `fab local`
uses — meaning this `debug_assert!` compiles out of the release binary entirely. Gate
item 4 ("the §2 certified-only assertion never fires") is therefore only actively checked
in debug builds (`cargo test`/debug-mode manual runs); in the release fab run the claim is
vacuously true (the check isn't compiled in) rather than actively verified. Enabling
`debug-assertions = true` workspace-wide (or per-crate) in `[profile.release]` would make
it observable there too, at the cost of turning on every `debug_assert!`/
`cfg(debug_assertions)` path in every dependency for that run — a workspace-wide behavior/
performance change that could put the throughput-parity gate (item 2, `autobahn-optimistic`
reproducing the 240k tx/s Phase-1 number) at risk. Left the profile untouched rather than
make that call unilaterally; flagging for a decision. No unit test exercises
`use_optimistic_tips: false` today either (all twelve `core_tests` use
`Parameters::default()`, which is `true`) — building a seamless-specific test harness
would run into the same single-Core-can't-reach-quorum limitation documented for the six
ignored tests in §6, so none was added; functional validation of seamless relies on the
`fab local, autobahn-seamless` gate run (item 4) itself.

---

## 4. Transaction format + payload modes

`node/src/benchmark_client.rs`: wire format is now
`[1 B marker][8 B id, BE][8 B UTC-millis submission timestamp, LE][payload…]`, 17 B
header (bytes 0..9 byte-identical to before, so `worker/src/batch_maker.rs`'s sample-tx
extraction — `tx[0]==0 && tx.len()>8`, `tx[1..9]` as the id — needed no change; verified
no other `.rs` site indexes into transaction bytes). Timestamp via
`SystemTime::now().duration_since(UNIX_EPOCH)` millis, `put_u64_le`; every tx gets one,
samples included. Added a local `TransactionMode { AllZero, Random }` enum (parsed from a
plain string via `--mode`, not a `clap::ValueEnum` derive — avoids adding the `derive`
feature to the shared workspace `clap` dep for one CLI flag; `.value_parser(["all-zero",
"random"])` does the validation clap-side). `AllZero` keeps the existing `tx.resize(size,
0)`; `Random` fills the remainder with `rand::thread_rng()` bytes (handles `size == 17`
as a zero-length fill correctly). Size floor raised `>= 9` → `>= 17`, error message and
the stale "at least 16 bytes" comment (which never matched the old `< 9` check) both
corrected to state why (marker + id + timestamp).

Harness plumbing (mechanical, `commands.py`'s `run_client` signature changed so every
caller needed updating regardless):
- `benchmark/benchmark/commands.py::CommandMaker.run_client`: new `mode='all-zero'`
  param, appends `--mode {mode}`.
- `benchmark/benchmark/config.py::BenchParameters`: new `self.tx_mode`, additive
  (`'tx_mode' in json` else `'all-zero'`), validated against the two allowed strings.
- `benchmark/benchmark/local.py` / `remote.py`: both `run_client(...)` call sites now pass
  `mode=self.tx_mode` / `mode=bench_parameters.tx_mode`.
- `benchmark/fabfile.py`: `'tx_mode': 'all-zero'` added next to `'tx_size'` in both
  `local` and `remote` tasks' `bench_params`. (Left the unrelated `plot` task's
  `plot_params['tx_size']` alone — that task only filters already-generated result files,
  no live client invocation.)

All Python files `py_compile` clean; `cargo test --workspace` unchanged (still the same
green set as §1–3).

---

## 5. Real transaction latency — starfish-style prometheus metrics

### New `metrics` crate

`metrics/src/{stat,metrics,prometheus}.rs`, mirroring starfish's own file layout.
Ported minimally (only what the Phase-2 `Metrics` struct needs):
- `stat.rs`: `PreciseHistogram<T>`/`HistogramSender<T>`/`histogram()`/`DivUsize` ported
  near-verbatim. Added one thing starfish's file doesn't have: an exact `max()`
  accessor (`pcts([999])` only *approximates* the max — `len*999/1000` rounds down and
  under-reports by an increasing margin as `len` grows — spec's label set includes
  `"max"`, so this reads the true last element of the sorted points instead).
- `metrics.rs`: `Metrics` holds exactly the five fields the spec lists (not starfish's
  full multi-hundred-line struct — dozens of its fields have no Autobahn/Vantage
  equivalent yet). `HistogramReporter`/`AsPrometheusMetric` ported, extended to publish
  `max` alongside starfish's p25/p50/p75/p90/p99/sum/count. Two deliberate deviations:
  `std::sync::Mutex` instead of `parking_lot::Mutex` (single low-contention lock only the
  10s reporter task touches; not worth a new dependency) and `log::info!/error!` instead
  of `tracing` (this workspace's existing logging stack everywhere else).
- `prometheus.rs`: `start_prometheus_server`/`METRICS_ROUTE` ported; dropped
  `tower_http::CompressionLayer` (an internal endpoint scraped a handful of times per
  run needs no compression) and starfish's custom `runtime::{Handle,JoinHandle}`
  wrapper (plain `tokio::spawn` suffices with one runtime).

### Data flow

- `primary/src/primary.rs`: `PrimaryWorkerMessage::Committed(Vec<Digest>)` appended
  **last** (bincode compat). `Primary::spawn` boots a `prometheus::Registry` +
  `Metrics`/`MetricReporter` pair and `start_prometheus_server` **unconditionally**
  (not `#[cfg(feature = "benchmark")]`) on `committee.primary(&name)?.metrics` —
  matches spec's "the metrics server itself is always on ... it also serves Phase-3+
  protocol metrics." Primary never observes into its own copy in Phase 2 (nothing it
  does needs the five transaction-latency fields), so its endpoint reports the metric
  names with no data until first observation (`HistogramReporter::report` is a no-op on
  an empty histogram) — this is the "near-empty registry" the spec anticipates.
- `primary/src/committer.rs` (the "Committed B..." log site, `process_commit_message`):
  gained `name: PublicKey` (new first `spawn` param), `network: SimpleSender`, and
  `worker_addresses: HashMap<WorkerId, SocketAddr>` (our own workers' `primary_to_worker`,
  from a new `Committee::our_workers_by_id` accessor — `our_workers` existed but
  discarded the id, which routing by `WorkerId` needs). All three new fields/the
  `our_workers_by_id` computation are `#[cfg(feature = "benchmark")]`. For each
  committed header, groups `header.payload` (`HashMap<Digest, WorkerId>`) by id and
  unicasts `PrimaryWorkerMessage::Committed(digests)` to *our own* worker of that id —
  not the header author's worker, which we have no connection to. This is correct
  because batches are gossiped worker-to-worker by matching id (see `worker/src/
  synchronizer.rs`'s existing sync/retry logic): our local worker-`W` likely already
  holds (or can be asked for) a replica of any authority's worker-`W` batch. A miss is
  the expected, tolerated case for a remote author's batch we never happened to receive
  a gossip copy of.
- `worker/src/worker.rs`: same always-on registry/`Metrics`/server boot, on
  `committee.worker(&name,&id)?.metrics`; the resulting `Arc<Metrics>` is stored on
  `Worker` and threaded into `Synchronizer::spawn` as a new (always-present) parameter.
- `worker/src/synchronizer.rs` (already the sole receiver of every
  `PrimaryWorkerMessage` on the worker side — reused in place, no new task/channel):
  new `PrimaryWorkerMessage::Committed(digests)` arm calls a new
  `#[cfg(feature = "benchmark")] observe_committed` method: dedup via an
  (unbounded-for-the-run, benchmark-only) `HashSet<Digest>`; `store.read` (never
  `notify_read` -- a miss must not block, only count `latency_misses`); bincode-decode
  the stored bytes back to `WorkerMessage::Batch(transactions)`; per transaction,
  read the §4 timestamp at bytes `9..17` LE, `now.saturating_sub(ts)` (tolerates clock
  skew instead of panicking), `observe()` the latency, `inc_by` the squared-microseconds
  counter (`saturating_mul` guards a pathological latency's square against `u64`
  overflow, though real values are nowhere near that boundary), and bump
  `committed_transactions`/`committed_bytes`.

### Addresses (committee schema)

`config::PrimaryAddresses`/`WorkerAddresses` both gained a `metrics: SocketAddr` field
(no `#[serde(default)]` — committee.json is regenerated every run, so there's no
back-compat need, matching spec). `benchmark/benchmark/config.py::Committee.__init__`
port math: primary block grows 2→3 ports (`primary_to_primary`, `worker_to_primary`,
`metrics`), each worker block grows 3→4 (`primary_to_worker`, `transactions`,
`worker_to_worker`, `metrics`) — with `workers=1` (the `fab local`/`fab remote`
default) that's 6→8 ports per authority, matching the spec's stated delta exactly.
Added `Committee.primary_metrics_addresses`/`workers_metrics_addresses` accessors
(mirroring the existing `primary_addresses`/`workers_addresses`) for the harness to
enumerate scrape targets; `ips()` extended to include the new addresses too (no actual
effect on the returned host set today, since every metrics port shares a host with an
already-listed field on the same authority, but keeps the method visibly complete
against the schema).

Rust-side fixture fallout (struct literals + a port-remapping helper, `PrimaryAddresses`/
`WorkerAddresses` now require the field): `config::Committee::new` (test/simple
constructor — reuses its single `address` for `metrics` too),
`primary/src/tests/common.rs` and `worker/src/tests/common.rs`'s `committee()` fixtures
(new `600+i`/`700+i` ports) and their `committee_with_base_port` helpers (now remap the
`metrics` port like every other field, for consistency — harmless that this wasn't
strictly required for any test to pass).

### Harness consumption

- `benchmark/benchmark/utils.py`: new `scrape_metrics(address, filename)` (stdlib
  `urllib`, per spec) and two `PathMaker` entries,
  `metrics_primary_file(i)`/`metrics_worker_file(i, j)` → `metrics-primary-<i>.txt` /
  `metrics-worker-<i>-<j>.txt` beside the logs. Best-effort: a scrape failure warns and
  skips (doesn't raise), so one unreachable node doesn't abort the run — but see below,
  a failed scrape is still surfaced in the final RESULTS block, not silently dropped.
- `local.py` / `remote.py`: scrape every primary and worker metrics address right
  before killing the nodes (symmetric, both call the same `Committee` accessors).
- `logs.py`: `LogParser` gained a `metrics` constructor param (list of raw scraped
  texts; `process()` globs `metrics-worker-*.txt`) and `_parse_worker_metrics`
  (plain regex against the Prometheus text-exposition format — `name{v="label"} value`
  for the gauge, `name value` for the two counters — same approach starfish's own
  `measurements.rs` uses, verified by hand against a synthetic exposition body matching
  its own test fixture's shape). `_real_transaction_latency` aggregates **worker**
  scrapes only (primary's copy is never observed into in Phase 2, so it would only ever
  contribute zeros): exact global avg/stddev from summed count/sum/sum-of-squares
  (`stddev = sqrt(squared_sum/count - avg^2)`), p50/p90/p99 as the **median across
  nodes** of each node's own exact percentile (spec's explicit choice — a true global
  percentile isn't recoverable from already-reduced per-node quantiles). `result()`
  adds the exact line shape the spec asks for: `Real transaction latency: avg X ms
  (stddev Y), p50/p90/p99 …/…/… ms (N txs, M misses)`, or an explicit
  `no metrics scraped (0/N worker(s) reporting)` / `[WARNING: only R/N worker(s)
  scraped]` note rather than silently reporting a partial/zeroed number -- gate item 3
  ("scrape must succeed on all nodes") needs this failure mode to be visible, not
  swallowed.
- Sanity-checked the regex + aggregation (parse a synthetic exposition body shaped
  exactly like starfish's own `measurements.rs` test fixture; two-node and
  missing-scrape aggregation) directly against the Python module before the first fab
  run; see the fab-local gate section below for the real thing.

### Semantics note

Quantiles are cumulative over the whole run: `MetricReporter`'s periodic task
`receive_all()`s into the histogram every 10s but never clears it, so p50/p90/p99
reflect every observation since boot (warm-up included), matching starfish's own
end-of-run summaries — not a rolling/windowed view of only the last 10s.

## 7. RocksDB tuning — starfish parity (added mid-phase, user directive 2026-07-23)

Ported from `~/code/starfish/crates/starfish-core/src/rocks_store.rs` (read-only) into
`store/src/lib.rs`. Starfish splits one DB into metadata-vs-bulk-data column families;
this artifact already separates the same concerns into per-component `Store`
*instances* (primary's store for headers/certs/payload markers, each worker's own store
for batch bytes), so starfish's two per-CF option profiles became two profiles on
`Store::new_with_profile`, not column families — single default CF per DB, unchanged.
`Store::new(path)` still exists and is exactly `new_with_profile(path,
StoreProfile::Metadata)`, so every existing call site/test fixture is unaffected.

- `StoreProfile::Metadata` (level compaction — rocksdb's default style, so no explicit
  `set_compaction_style` call — L0 triggers 4/48/64, 16 KiB blocks, 128 MiB LRU cache):
  wired to the primary's store (`node/src/main.rs` picks the profile from
  `matches.subcommand_name()` before constructing the one `Store` that either
  `Primary::spawn` or `Worker::spawn` receives).
- `StoreProfile::Data` (`DBCompactionStyle::Universal`, L0 triggers 80/96/128, 128 KiB
  blocks, 512 MiB LRU cache): wired to worker stores (the `"worker"` subcommand branch).
- Shared DB-wide options (both profiles): `create_if_missing`, fd-limit raise
  (`fdlimit::raise_fd_limit()` → `set_max_open_files(to/8)`), table-cache sharding,
  LZ4 + bottommost Zstd compression (+ zstd max-train-bytes), 2 GiB db-write-buffer /
  256 MiB write-buffer / 6 max-write-buffers, 2 GiB WAL cap, 8-way parallelism,
  `use_fsync(false)`, 64 MiB writable-file buffer, 128 MiB target-file-size,
  pipelined write, 0.02 memtable-prefix-bloom-ratio. Both profiles' block tables get
  `set_bloom_filter(10.0, false)` + `set_pin_l0_filter_and_index_blocks_in_cache(true)`
  (starfish's shared `block_options` helper, ported as-is). All writes go through an
  explicit `WriteOptions` with `set_sync(false)` (`db.put_opt`, replacing the old
  `db.put`) rather than relying on the (already-`false`) rocksdb default, matching
  spec's ask to make it explicit.
- Cargo: workspace `rocksdb` gained `features = ["lz4", "zstd"]`; new workspace dep
  `fdlimit = "0.3"` (same version starfish itself pins). No API renames needed going
  from starfish's rocksdb 0.22 pin to this workspace's 0.23 — every setter used
  (`set_block_cache`/`set_bloom_filter`/`set_pin_l0_filter_and_index_blocks_in_cache`/
  `set_bottommost_zstd_max_train_bytes`/`set_db_write_buffer_size`/etc.) compiled
  unchanged; `store` (and then the full workspace) built clean on the first try.
  Omitted starfish's `multi-threaded-cf` feature and `ColumnFamilyDescriptor`/
  `open_cf_descriptors` entirely, per spec ("no column families... the store task
  solely owns the DB").
- Added `log = { workspace = true }` to `store/Cargo.toml` for one boot-time
  `log::debug!("Opened store at {} with profile {:?}", ...)` line (operationally useful
  to confirm which profile is actually active; not requested by the spec but cheap and
  low-risk).

---

## Corrections found during the gate runs

### §1: `use_optimistic_tips` needed a serde default too, not just `protocol`

First `fab local` failed immediately: every primary/worker exited with `Error: Failed
to load the node's parameters ... missing field \`use_optimistic_tips\``. Cause:
§1's own instruction ("Remove `use_optimistic_tips` from fabfile node_params") does
exactly that, but `Parameters::use_optimistic_tips` had no `#[serde(default)]` (only
`protocol` got one) — so a generated `.parameters.json` that no longer contains the
key fails to deserialize at all. This is an internal inconsistency between two clauses
of the same §1 instruction, not a new protocol decision: the fix is the identical
mechanism already used for `protocol`, applied to the field the harness now omits.
Added `#[serde(default = "default_use_optimistic_tips")]` (→ `true`, matching
`Parameters::default()`) to `config/src/lib.rs`. Inert at runtime either way:
`reconcile_protocol()` overwrites `use_optimistic_tips` from `protocol` on every
reachable Autobahn path regardless of what it deserialized to; the only thing this
changes is whether parsing *succeeds*. Judged this narrow and low-risk enough to fix
directly (it blocks 100% of the remaining gate items and doesn't touch protocol
semantics), rather than stopping the run over it, but flagging prominently here since
it required a design choice not literally spelled out in the spec.

### §5: cross-node aggregation must not *sum* `count`/`misses`

The first successful run reported "Real transaction latency: ... (50,351,172 txs, 0
misses)" against a true committed-transaction count of ~14.4M (239,786 tx/s × 60s) --
roughly 3.5x too many. Cause: every node's `Committer` processes the *entire*
replicated commit sequence (that's the point of BFT consensus -- every correct
replica commits the same log), so every worker's `Committed` notifications, and hence
its `transaction_committed_latency` count, cover the *same* global set of
transactions, not a `1/n` partition of it. Summing `count` across the 4 worker
scrapes was summing 4 (near-)identical countings of the same underlying set.

Checked this directly against starfish's own orchestrator
(`crates/orchestrator/src/measurements.rs`) rather than guessing: its
`aggregate_rate()` (used for TPS) reduces the analogous per-scraper `count` field with
**max**, not sum -- confirming starfish's architecture has the same "every validator
observes the full replicated stream" property, and its own convention for a
cardinality like `count` is "take the most complete single reading," not "sum
partitions." I could not find anywhere in starfish's real aggregation code that sums
`count` across scrapers for a global total, despite the spec text saying "summed
count/sum/sum²." Fixed `count` and `misses` to take the **max** across worker scrapes
(matching starfish's convention) instead of the sum.

`avg`/`stddev` were *not* changed -- they stay sum-based, and that's actually correct,
not merely convenient: since every node's `(count, sum, squared_sum)` triple scales by
the same near-constant factor (all observing the same set, modulo a lagging node not
yet caught up), `sum/count` and `squared_sum/count` are invariant to summing across
nodes first -- verified both algebraically and by re-parsing the same saved run's
files before and after the fix (avg/stddev/percentiles identical; only `count` changed
from 50,351,172 to 14,387,793). Summing first is arguably *better* than reading one
node, since it blends every node's independent measurement of the same distribution
rather than trusting a single (possibly less-complete) one.

Net: the spec's literal "summed count/sum/sum²" is right for `sum`/`squared_sum` (as
inputs to the avg/stddev ratio) but not for `count` itself as a reported figure, and
"misses" follows `count`'s convention for the same reason. Flagging this explicitly —
it's a real, verified divergence from the spec's literal words, resolved by checking
starfish's actual behavior rather than the prose description of it.

## Verification & gate

**Environment**: `tmux ls` confirmed no sessions before every `fab local` run below.
Fresh `fab local` invocation (`CommandMaker.clean_logs()` wipes `logs/` at the start of
every run, so no cross-run contamination of the metrics/log files between the four
gate runs below).

### Gate 1 — build + test, fully green

`cargo build --workspace --all-targets` (debug and `--release`) and `cargo test
--workspace`: clean, 0 errors, 0 failures, 0 hangs. Also verified with every
`benchmark` feature enabled (`--features "primary/benchmark worker/benchmark
node/benchmark"`, debug and release) and the exact fab compile command (`cargo build
--quiet --release --features benchmark`, run from `node/`). Per-crate test counts:
config 0, crypto 7, network 6, node 0, primary 11 (+6 `#[ignore]`d, the documented
set), store 4, worker 6, metrics 0 (infrastructure ported from a working reference;
exercised end-to-end by the fab runs below rather than via synthetic unit tests).

### Gate 2 & 3 — `fab local`, `autobahn-optimistic`, `all-zero`

```
Protocol: AutobahnOptimistic
Consensus TPS: 239,786 tx/s        (Phase-1: 240,040)
Consensus latency: 5 ms            (Phase-1: 5 ms)
End-to-end TPS: 239,762 tx/s       (Phase-1: 240,004)
End-to-end latency: 13 ms          (Phase-1: 14 ms)
Real transaction latency: avg 20.24 ms (stddev 13.87),
  p50/p90/p99 17.00/38.00/56.00 ms (14,387,793 txs, 0 misses)
```

Throughput/latency reproduce Phase-1 numbers closely (input rate fully sustained;
consensus latency identical; end-to-end latency very slightly *better*, consistent
with "blake3 should only help"). Scrape succeeded on all 8 endpoints (4 primary + 4
worker; 0 misses on every worker).

**Real-latency vs legacy-sample comparison (gate item 3), reported side by side above.**
Real mean 20.24 ms vs. legacy sample mean 13 ms -- these do *not* closely agree, and I
want to flag why rather than let the gap pass quietly. The two metrics don't actually
share the same clock path end-to-end the way the spec's "same clock path" framing
assumes: the legacy sample measures client-send → *primary* commit-log timestamp;
the real metric measures client-send → the moment the *worker's* `Synchronizer` task
gets around to processing that commit's `Committed` notification (store read +
bincode decode + per-tx extraction), which is one additional hop and a
single-consumer queue behind the primary's commit instant. Under a sustained 240k tx/s
load, that queue is a plausible source of several milliseconds of added, systematic
(not random) latency in the real metric -- which would explain a real mean sitting
above the sample mean by roughly this margin without indicating a correctness bug in
either measurement. I did not attempt to add queue-priority tuning or otherwise close
this gap; nothing in the spec asks for it, and it risks scope creep into
performance work the spec parks for later phases. Flagging for Fable's judgment on
whether ~7ms of added observation-path latency is acceptable "noise" for Phase 2 or
warrants a Phase 3+ look (e.g., prioritizing `Committed` messages, or measuring at
store-write time instead of at notification-processing time).

### Gate 4 — `fab local`, `autobahn-seamless`

Ran with `benchmark/fabfile.py`'s `local` task's `protocol` temporarily switched to
`'autobahn-seamless'` (the only way to select a protocol — the fabfile hardcodes the
dict literal per task, no CLI override exists), then reverted back to
`'autobahn-optimistic'` immediately after (diffed to confirm both `local` and `remote`
tasks ended at the spec's own example default, `protocol: 'autobahn-optimistic'` /
`tx_mode: 'all-zero'`, before moving on).

```
Protocol: AutobahnSeamless
Consensus TPS: 239,839 tx/s        Consensus latency: 18 ms   (optimistic: 5 ms)
End-to-end TPS: 239,815 tx/s       End-to-end latency: 34 ms  (optimistic: 13 ms)
Real transaction latency: avg 45.81 ms (stddev 30.04),
  p50/p90/p99 42.00/72.50/117.50 ms (11,986,610 txs, 0 misses)
WARN: Clients missed their target rate 31 time(s)
```

Live and sustained (239,839 / 240,000 target = 99.9%). Latency delta vs optimistic is
consistently positive across every measurement (consensus +13 ms, end-to-end +21 ms,
real +25.6 ms) — the expected direction and rough order of magnitude for "one extra
car-certification round trip." The §2 debug_assert (compiled only under
`debug_assertions`, off by default in the release build `fab local` uses — see the §2
note above) could not have fired either way; nothing else panicked, and the run
produced correct, non-degenerate output for the entire 60s window, which is the best
available evidence the certified-tips path is functioning, short of a debug-mode
multi-node run (out of scope per §6's single-Core-harness limitation). "Clients missed
their target rate 31 times" is a client-side warning (transient submission burst vs.
target pacing), not a node-side failure — the achieved rate confirms it didn't matter.

### Gate 5 — `fab local`, `random` mode

Same dance: `local` task's `tx_mode` switched to `'random'` (protocol left at
`autobahn-optimistic`), run, reverted.

```
Protocol: AutobahnOptimistic, tx_mode=random
Consensus TPS: 239,873 tx/s        (all-zero: 239,786 tx/s)
End-to-end TPS: 239,813 tx/s       (all-zero: 239,762 tx/s)
Real transaction latency: avg 36.49 ms (stddev 28.63),
  p50/p90/p99 31.00/62.00/117.50 ms (11,990,789 txs, 0 misses)
WARN: Clients missed their target rate 218 time(s)
```

Verified end-to-end, not just at the config layer: `client-0-0.log` shows `Transaction
mode: Random`, confirming the `--mode` flag actually reached `benchmark_client`'s
`TransactionMode::parse`. Throughput parity with all-zero, as expected (239,873 vs
239,786 tx/s, 239,813 vs 239,762 tx/s — differences are noise): the consensus/network
layer doesn't inspect payload bytes, so random vs all-zero payload content has no
compression/dedup advantage to lose. This is the "honesty check" gate item 5 asks for.

Two honest observations, reported rather than smoothed over: (1) "Clients missed their
target rate" is far more frequent under random (218×) than all-zero (0× in the gate-2
run) or seamless (31×, itself running all-zero) — generating a true-random 495 B
payload per transaction via `rand::thread_rng()` is measurably more CPU-expensive
client-side than `BytesMut::resize`'s memset, occasionally pushing a client past its
per-tick budget. It didn't stop the target rate from being sustained in aggregate
(99.95% of target achieved), consistent with the gate's own wording ("rate sustained").
(2) Real/consensus/end-to-end latency all run somewhat higher under random than
all-zero (e.g. real mean 36.49 ms vs 20.24 ms) despite identical protocol — plausibly
downstream of the same client-side generation cost making submission burstier rather
than perfectly smooth, not a consensus-layer effect. Gate item 5 only asks for rate
parity and honest reporting, both satisfied; flagging the latency observation for
visibility rather than silence.

### Gate 6 — simplification pass

Single-pass review (reuse / simplification / efficiency / altitude) over every file
touched this phase, applied directly (not deferred to the audit):

- **Efficiency (real fix):** `worker/src/synchronizer.rs::observe_committed` was
  calling `IntCounter::inc()`/`inc_by()` once *per transaction* for
  `transaction_committed_latency_squared_micros`, `committed_transactions`, and
  `committed_bytes` — three atomic ops per transaction, at up to ~240k tx/s. Changed
  to accumulate all three locally across the whole `Committed` notification (every
  digest, every transaction) and flush with exactly one `inc_by` each at the end.
  Only `transaction_committed_latency.observe(latency)` stays per-transaction (each
  observation is a distinct value; it's already a lock-free channel push, not an
  atomic op, so there was nothing to batch there).
- **Simplification (real fix):** `primary/src/committer.rs`'s new by-worker grouping
  used `.or_insert_with(Vec::new)`; changed to `.or_default()`.
- **Efficiency (considered, skipped):** `HistogramReporter::report()` calls
  `histogram.pcts([250,500,750,900,990])` (sorts `points` once) then `histogram.max()`
  (sorts again). A second sort of an already-sorted `Vec` is cheap under Rust's
  adaptive sort and this runs once per 10s reporter tick, not in any hot path —
  not worth the added indirection of exposing an "already sorted" fast path for.
- **Altitude / reuse (considered, skipped):** `Synchronizer`'s `metrics: Arc<Metrics>`
  field is unconditional (`#[allow(dead_code)]`) while `observed_commits` is
  `#[cfg(feature = "benchmark")]`-only, unlike `Committer`'s fully-symmetric
  cfg-gating of both its new fields. Deliberate, not an inconsistency: `Committer`'s
  fields are *computed* fresh from data (`committee`/`name`) that's cheap to skip
  computing under cfg; `Synchronizer`'s `metrics` is a cheap `Arc` clone of a value
  `worker.rs` already unconditionally owns (the metrics server is always on), and
  gating it would force a `#[cfg]`-duplicated call site in `worker.rs` for no benefit.
  local.py/remote.py's near-duplicate metrics-scrape loops likewise left alone —
  matches this codebase's existing convention of not sharing logic between the two
  files (their primary/worker spawn loops, `kill()`, etc. are equally duplicated,
  not reused, because the execution models — local subprocess vs. remote SSH —
  already differ enough that a shared helper wouldn't simplify much).

Re-ran `cargo build --workspace --all-targets` (default features and every
`benchmark` feature enabled) and `cargo test --workspace` after the fixes: clean,
same green set as gate 1.

**Environment note:** the harness host process restarted mid-phase (orphaning the
in-progress session; this agent's transcript survived and the work resumed from it).
The restart wiped the session-scratchpad `fabenv` virtualenv used for every `fab
local` run above; it was rebuilt from `benchmark/requirements.txt` (same package
versions: fabric 3.2.3, boto3 1.43.53, matplotlib 3.11.1, google-cloud-compute
1.50.0) before continuing. Re-ran `fab local` once more (`autobahn-optimistic`,
`all-zero`) after the counter-batching change specifically, to confirm it didn't
silently change *correctness*:

```
Consensus TPS: 239,680 tx/s      (gate-2 run: 239,786)
End-to-end latency: 79 ms        (gate-2 run: 13 ms)
Real transaction latency: avg 94.45 ms (stddev 83.73),
  p50/p90/p99 71.00/208.50/375.00 ms (14,387,822 txs, 0 misses)
WARN: Clients missed their target rate 861 time(s)  (gate-2 run: 0)
```

Throughput and correctness reproduce (TPS within noise; count matches the expected
~14.39M exactly as it did in gate 2, confirming the count/misses aggregation fix and
the counter-batching change together are still correct; 0 misses). Latency is
markedly worse across every measure, and "clients missed target rate" jumped from 0
to 861 -- both symptoms of a client-side/host CPU contention problem, not a
consensus-layer regression: same code (modulo the two simplification-pass changes,
one proven a no-op for correctness, the other a pure `HashMap` API swap), same
committee/parameters, same machine. This machine had just gone through a process
restart and a from-scratch venv rebuild immediately before this run; some contended
resource (CPU scheduling, thermal throttling, or background load from whatever
triggered the restart) is the far more likely explanation than a code-path
regression, given throughput held and only latency and client-side pacing degraded.
Treating gates 2-5's original numbers (run consecutively, earlier, presumably under
less contention) as the authoritative Phase-2 baseline, and this run as a correctness
re-confirmation (throughput/count/misses all check out) rather than a fresh
performance baseline. `git status`/`git diff` are otherwise untouched (still zero
git commands beyond read-only status/diff/log throughout the phase).

---

## Post-audit amendments (F1, F2)

Fable's audit accepted open items (2), (3), (4) as documented (spec §5 amended to
match the max-for-count/misses ruling) and ordered a fix for (1), the real-vs-sample
gap, plus one unrelated cleanup:

### F2 — dropped the dead `sha2` dependency (mostly as instructed, one correction)

The audit's claim of "zero references in crypto/src" was **not accurate** — verified
by grepping (a rule I've followed all phase: check the claim on disk before acting on
it) — `crypto/src/tests/crypto_tests.rs` had a live `impl Hash for &[u8]` test helper
built on `sha2::Sha512::digest`, a Phase-1-era artifact (see MODERNIZATION-NOTES.md
§2's "sha2 (new, crypto dev-dependency only)") that Phase 2's §3 blake3 sweep never
touched, because §3's own inventory only named `primary`/`worker` files, not
`crypto`'s own test. Rather than skip F2 or silently break `cargo test -p crypto`,
migrated the helper to `Blake3Hasher` (already exposed by `crypto`, already the
pattern used everywhere else in the codebase) — it hashes arbitrary test messages
(`b"Hello, world!"`) for signature round-trip tests, not testing SHA-512 as a
standard specifically, so nothing depends on the hash choice. Verified via
`grep -rln sha2 --include="*.rs" --include=Cargo.toml .`: after the migration, the
only references were the two dependency declarations themselves (workspace
`Cargo.toml`, `crypto/Cargo.toml`'s now-empty `[dev-dependencies]`), both removed.
`cargo build --workspace --all-targets` and `cargo test -p crypto` (7/7, including
the migrated test) both clean afterward.

### F1 — carry the commit instant in the notification (implemented; gap did not close)

Implemented exactly as directed: `PrimaryWorkerMessage::Committed` (still appended
last) is now `Committed(u64 /* commit UTC-millis */, Vec<Digest>)`;
`primary/src/committer.rs` takes `SystemTime::now()` once per committed header, at
the same point as the `"Committed {} -> {:?}"` log line, and sends it with the
digests; `worker/src/synchronizer.rs::observe_committed` takes `commit_millis` as a
parameter, dropped its own `SystemTime::now()` call entirely, and computes
`commit_millis.saturating_sub(submitted_millis)`. Builds and tests clean (with and
without the `benchmark` feature); re-ran `fab local` (`autobahn-optimistic`,
`all-zero` — the gate-2 config):

```
Consensus TPS: 239,943 tx/s        Consensus latency: 5 ms     (matches gate-2 exactly)
End-to-end TPS: 239,919 tx/s       End-to-end latency: 14 ms   (matches gate-2 exactly)
Real transaction latency: avg 20.13 ms (stddev 12.22),
  p50/p90/p99 17.00/37.00/56.00 ms (14,385,770 txs, 0 misses)
```

Two things this run settles: **(a)** throughput and the legacy sample metric both
reproduce the original gate-2 numbers essentially exactly (5 ms / 14 ms vs. 5 ms /
13 ms), confirming the degraded post-simplification run (21 ms / 79 ms) really was
transient machine contention around the host restart, not a code-path regression —
matching my documentation at the time. **(b)** the real-vs-sample gap did **not**
close: 20.13 ms vs. 14 ms is essentially unchanged from the pre-F1 measurement
(20.24 ms vs. 13 ms). F1's mechanism is verified correct (the notification-hop and
worker-queue delay it targeted are now provably excluded from the measurement — the
timestamp comes from the primary, not the worker), but that specific bias was
evidently not the (main) source of the gap. Reporting this rather than the outcome I
expected: the fix is real and worth keeping (it's a strictly more accurate
measurement point, matching starfish's own commit-handler observation point,
regardless of what it does to this particular gap), but it does not settle item (1).

**Revised hypothesis**, offered for judgment rather than acted on (out of the scope
actually assigned): the legacy sample metric's "commit" instant is not one primary's
timestamp — `LogParser._merge_results` explicitly keeps the **earliest** timestamp
across all 4 primaries' own "Committed ... -> digest" log lines for a given digest
(every correct primary logs the same commit event, on its own clock, at a slightly
different instant). The sample metric's per-transaction latency therefore uses
min(4 replicas' commit times) for that batch, an order statistic that is
systematically earlier than a typical single replica's reading. My real-latency
metric, by construction, uses whichever *one* primary is local to the worker doing
the observing — structurally an "any one of 4" reading, not a "min of 4" one. Min-of-
4 being persistently earlier than a-typical-one-of-4 is exactly the shape of gap
observed (a few milliseconds, present both before and after F1, unaffected by fixing
the notification-hop bias since it's a completely different mechanism). I did not
implement anything further here — reconciling this would mean changing either the
legacy sample metric's own cross-node semantics (touching the log-format/parsing
invariant this phase was told to leave alone as a stable cross-validation reference)
or accepting the real metric will run a bit hotter than the sample by design. Flagging
for a decision rather than picking one unilaterally.

Re-ran `cargo build --workspace --all-targets` (default and every `benchmark`
feature) and `cargo test --workspace -j 4 -- --test-threads=4` after F1/F2: clean,
same green set throughout (crypto's migrated test included). `tmux ls` checked
before the confirmation run; no git writes; `fabfile.py` left at the gate-2 config
(`autobahn-optimistic`/`all-zero`) it was already at.

---

## Summary

**Frontier reached:** PHASE2-SPEC.md §§1–7 all complete and gate-verified, plus the
post-audit F1/F2 amendments above. §3 required no source edits (already satisfied on
disk, corrected assumption documented above); §§1, 2, 4, 5, 6, 7 all have concrete
changes, verified by `cargo test --workspace` and five separate `fab local` runs
(optimistic/all-zero ×2, seamless, random, plus the post-simplification/contention
re-check). Three real bugs found and fixed only because the gate runs were actually
executed and claims verified against disk rather than assumed: the
`use_optimistic_tips` serde default (§1/harness interaction), the count/misses
cross-node aggregation (§5, accepted by audit), and the crypto/`sha2` "zero
references" claim (F2, corrected before acting on it).

**Per-crate test counts** (`cargo test --workspace`): config 0, crypto 7, network 6,
node 0, primary 11 passed + 6 `#[ignore]`d (the documented set — `process_header`,
`process_prepare`, `generate_confirm`, `generate_commit`,
`generate_pipelined_prepare`, `sync_missing_proposals` — no others), store 4, worker
6, metrics 0. Zero failures anywhere, zero hangs, zero unexpected ignores.

**Resolved by Fable's audit** (no further action, kept here for the record): (2) §5
max-for-count/misses — accepted, spec §5 amended to match; (3) §2's inert
release-profile debug_assert — accepted as documented, no profile change; (4) §1's
`use_optimistic_tips` serde-default fix — accepted.

**Still open — item (1), real vs. legacy-sample latency gap:** F1 (commit instant
carried in the notification, eliminating the primary→worker hop and worker-queue
delay from the measurement) is implemented and verified correct, but did **not**
close the gap (20.13 ms vs. 14 ms post-F1, essentially unchanged from 20.24 ms vs.
13 ms pre-F1). Revised hypothesis above (`_merge_results`' min-across-4-primaries
semantics for the legacy sample metric vs. my metric's any-one-primary reading) is
offered but not acted on — would mean touching the log-parsing cross-validation
metric's own semantics, which is a different, larger decision than F1 was. Needs a
ruling on whether to (a) accept the real metric running structurally ~6 ms above the
sample metric as expected/documented behavior given the two use different order
statistics across replicas, (b) change the real metric to also take a min-across-
primaries reading (would need the *worker* to learn about other primaries' commit
times, a real design change), or (c) leave both as independently-valid measurements
that aren't expected to match exactly and say so plainly in whatever documentation
eventually reports this metric.

No git commits made (working tree left dirty throughout, as instructed). No files
touched outside `/Users/nikitapolianskii/code/vantage`; `~/code/starfish` read only
(`rocks_store.rs`, `stat.rs`, `metrics.rs`, `prometheus.rs`, `validator.rs`,
`measurements.rs` — all read, none modified); `upstream/autobahn-blips` consulted
read-only per §2's own instruction. `BASE_PORT` stays 4000. `fabfile.py`'s `local`/
`remote` tasks both end this session at `protocol: 'autobahn-optimistic'` /
`tx_mode: 'all-zero'` (verified diffed back to that state after gates 4–5's temporary
edits) — the spec's own stated defaults.

---

## 8. `local-benchmark` + Grafana dashboard (user directive, added 2026-07-23)

Read `~/code/starfish/crates/starfish/src/main.rs`'s `local_benchmark()` (read-only) as
the reference: it spawns every validator in-process via `tokio::spawn`, keeps each
one's `Arc<Metrics>`/reporter handle, and calls `Metrics::aggregate_and_display` on them
directly at the end -- no HTTP scrape, no log parsing. Replicated the same shape here.

### `node/src/client.rs` (extraction)

`TransactionMode`/`Client` (struct + `send`/`wait`) moved out of `benchmark_client.rs`
verbatim, made `pub`. `benchmark_client.rs` is now a thin CLI wrapper: parses args,
constructs a `client::Client`, calls `.wait()`/`.send()` -- byte-identical behavior,
same flags, same log lines. Both `node`'s `main.rs` and its `benchmark_client` `[[bin]]`
declare `mod client;` (equivalently `#[path = "client.rs"] mod client;`) against the
same file -- no `lib.rs` restructure needed for two bins to share one module; this is
the standard idiom, not a new pattern.

### `config::Committee::local_benchmark`

New constructor, in-memory: fresh `KeyPair` per authority (`KeyPair::new()`, the
existing constructor, not a new keygen path), all addresses on 127.0.0.1, port layout
identical to `config.py`'s (1 consensus + 3 primary + 4 per worker). Returns
`(Committee, Vec<KeyPair>)` -- the constructor doesn't spawn anything itself, keeping
"build a committee" and "spawn nodes from one" separate, matching how the harness-driven
path already keeps `Committee::import` and `Primary::spawn`/`Worker::spawn` separate.

Added `Serialize` to `Parameters`/`Committee`/`Authority`/`ConsensusAddresses`/
`PrimaryAddresses`/`WorkerAddresses` (previously `Deserialize`-only -- nothing needed to
write these out before) and `impl Export for Parameters {}` / `impl Export for Committee
{}`, so `local-benchmark` can dump its generated committee/parameters/keys into
`--data-dir` for reference using the *existing* `Export` trait (same one `KeyPair`
already used), not a bespoke serialization path.

### `Primary::spawn` / `Worker::spawn` return their metrics handle

Both used to build a `Registry`/`Metrics`/`MetricReporter` internally and discard them
(nothing outside needed them when every node was a separate OS process, scraped over
HTTP). Changed both to return `(Arc<Metrics>, Arc<MetricReporter>, Registry)` instead of
`()`, so `local-benchmark` -- the first caller with several nodes *in the same process*
-- can read every node's metrics directly. The pre-existing call sites in `node/src/
main.rs::run` (the regular `primary`/`worker` subcommands) simply don't use the returned
tuple; Rust doesn't require consuming a return value, so this is a pure extension, no
existing behavior changed. `MetricReporter` gained a `pub fn force_report`, splitting the
periodic drain out of the reporter's own `run()` loop so `local-benchmark` can force one
final gauge update at the exact end of the run instead of waiting on the next `10s` tick
(which could otherwise miss up to 10s of the tail).

### `metrics::snapshot` (new module)

`read_latency_snapshot(&Registry) -> Option<LatencySnapshot>` reads a node's own
gathered metric families directly via `prometheus::Registry::gather()`'s typed protobuf
structs (`MetricFamily`/`Metric`/`LabelPair`) -- no text-format round trip, since
everything is already in the same process. `aggregate_latency_snapshots(&[LatencySnapshot])
-> Option<AggregatedLatency>` applies the *exact* audited rules from `logs.py`'s
`_real_transaction_latency` (max for count/misses, summed sum/sum² for the avg/stddev
ratio, median across nodes for percentiles) -- one aggregation rule set, expressed twice
(Python for `fab`, Rust for `local-benchmark`) because the two vehicles don't share a
runtime to share the code itself; kept deliberately identical rather than drifting.

### `node local-benchmark` subcommand

`node/src/local_benchmark.rs` (new, `#[cfg(feature = "benchmark")]`-gated module and
subcommand dispatch, matching how `benchmark_client` is already feature-gated): wipes
`--data-dir`, builds the in-memory committee/parameters, spawns every primary (Metadata
store profile) and every worker (Data profile) via the unmodified `Primary::spawn`/
`Worker::spawn`, spawns one `client::Client` task per worker (rate divided across
`nodes * workers`, matching `fab`'s own `ceil(rate / workers())`), generates
`<data-dir>/prometheus.yaml` targeting every metrics endpoint via `host.docker.internal`
with `node-<i>-primary` / `node-<i>-worker-<j>` labels, waits for `--duration` or
Ctrl-C, then force-reports and prints a RESULTS block computed entirely in-process.

### `monitoring/` (new directory)

`docker-compose.yml`: only `prometheus`/`grafana` (stock images) -- deliberately *not*
dockerizing the nodes themselves the way starfish's own compose does (nodes run natively
in `local-benchmark`; native means no image rebuild per code change, per the spec's own
stated rationale). Host ports grafana **3003**, prometheus **9095** (this machine holds
3001/3002 already; starfish's own 3002/9093 choices collide). `grafana/{datasource,
dashboard}.yaml` adapted near-verbatim from starfish's provisioning structure (only the
datasource UID/name changed for clarity). `grafana/grafana-dashboard.json` written from
scratch (not ported from starfish, which was correctly non-goaled here -- its 23 panels
reference DAG/BLS/shard-reconstruction metrics this artifact doesn't have): 5 panels for
exactly the metrics named in the spec (committed TPS per node + total, real-latency
p50/p90/p99/max, latency misses, committed bytes rate). `monitoring/README.md` documents
the cwd convention (`local-benchmark` run from the repo root, so `.local-bench/` sits
next to `monitoring/` the way the compose file's relative bind mount expects) and how to
adapt it if `--data-dir` is overridden. `.local-bench/` added to `.gitignore`.

### Verification

- Smoke test (4 nodes, 1 worker, 4,000 tx/s, 8s): correct end-to-end behavior --
  consensus committing, 0 misses, sensible latency numbers -- before committing to the
  full-scale run.
- Full-scale run (`--nodes 4 --workers 1 --rate 240000 --tx-size 512 --protocol
  autobahn-optimistic --mode all-zero --duration 60 --base-port 4000`), the gate-2
  config:
  ```
  Consensus TPS: 240,997 tx/s        (fab gate-2: 239,943 / 239,786)
  Real transaction latency: avg 115.67 ms (stddev 88.02),
    p50/p90/p99 107.00/203.00/395.00 ms (14,434,300 txs, 0 misses)
  ```
  **Throughput reproduces closely** (240,997 vs. ~240k target, matching fab's own
  numbers). **Count/correctness reproduce closely** (14,434,300 vs. fab's 14,387,793 --
  same order, 0 misses both times, confirming the aggregation code is correct when
  exercised a second, independent way in a different language). **Latency does not
  reproduce** -- 115.67 ms here vs. ~20 ms under `fab local`, a genuine, substantial gap,
  not a rounding difference.

  Root-caused, not hand-waved: `fab local` runs each of the 4 primaries + 4 workers + 4
  clients as **12 separate OS processes**, each with its own tokio runtime, scheduled
  independently by the OS across real cores. `local-benchmark` runs the same 12 logical
  actors' worth of tasks (and everything each one internally spawns -- Core, Committer,
  Proposer, several waiters, Synchronizer, BatchMaker, Processor ×2, network listeners,
  etc. -- likely 200+ concurrent tokio tasks in total) inside **one process's one tokio
  runtime** (default multi-threaded, sized to this machine's 14 cores). Throughput held
  because the workload is still fundamentally rate-limited by the configured input rate,
  not by available parallelism; latency did not, because packing ~3x as many
  independently-scheduled-in-`fab` actors into one shared thread pool measurably adds
  queueing/scheduling delay under sustained load. This is a structural property of "self-
  host everything in one process" (explicitly what the spec asked for, and starfish's own
  `local-benchmark` makes the identical trade-off for the identical reason -- convenience
  over representativeness), not a bug in the metric, the aggregation, or the spawn wiring.
  Did not attempt to change the architecture to compensate (e.g. spawning real child
  processes internally), since that would defeat the "one process, no rebuild" point of
  building this subcommand at all. Flagging plainly: `local-benchmark` is the right tool
  to check *correctness and liveness* quickly (and it does, cleanly); `fab local` (or a
  future multi-machine/Phase-7 harness) remains the right tool for *latency* numbers.

- Monitoring stack: `docker compose -f monitoring/docker-compose.yml config` validates
  clean. Live end-to-end check (a real `local-benchmark` run + `docker compose up -d`
  concurrently): `curl http://localhost:9095/-/healthy` → healthy; `curl http://localhost
  :3003/api/health` → ok; `curl http://localhost:9095/api/v1/targets` → **all 8 targets
  (4 primary + 4 worker) `up`**; `curl -u admin:admin http://localhost:3003/api/search
  ?query=vantage` → the provisioned dashboard (`vantage-local-benchmark`) present. Torn
  back down (`docker compose down`) after verifying -- the stack is optional and the
  spec says the run works fine without it.

---

## Audit rulings (Fable, 2026-07-23) — gate closed

One adversarial pass over §§1–7 diffs + F1/F2 + targeted §8 review (snapshot.rs
aggregation, Committee::local_benchmark port math, client extraction). No defects found.

- **R1 — real-vs-sample gap: definitional, accepted.** The legacy sample metric takes
  the earliest commit across all primaries (logs.py `_merge_results` min); the real
  metric is each replica's own commit instant (post-F1) aggregated per the §5 rules.
  Real ≥ sample by construction; the ~6 ms delta ≈ cross-replica commit spread. The
  legacy metric's semantics stay untouched (comparability anchor to upstream
  paper-results); the real metric is the headline. Gate item 3's criterion amended in
  PHASE2-SPEC.md accordingly. F1 itself stands: the commit-instant timestamp is the
  correct observation point regardless (removes hop/queue noise; starfish parity).
- **R2 — F2 correction accepted.** The auditor's "zero sha2 references" grep missed
  `crypto/src/tests/`; the agent's verify-then-migrate (test helper → Blake3Hasher, then
  drop the dep workspace-wide) was the right handling.
- **R3 — §8 latency infidelity accepted as documented.** `local-benchmark` (one process,
  one runtime, 200+ tasks) reproduces throughput/count but not latency (~116 ms vs
  ~20 ms); inherent to in-process self-hosting (starfish's LocalBenchmark makes the same
  trade). Division of labor: local-benchmark = functional checks + live dashboard +
  throughput; latency-faithful local numbers = fab local (kept functional); authoritative
  numbers = remote (Phase 7).

Phase-2 gate: **closed**. Working tree remains uncommitted for the user. Suggested
commit split: (1) §§3/6 test+dep cleanup, (2) §1 protocol enum + harness, (3) §4 tx
format, (4) §5 metrics crate + plumbing + F1, (5) §7 rocksdb profiles, (6) §8
local-benchmark + monitoring, (7) docs/specs.

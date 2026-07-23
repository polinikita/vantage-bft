# Phase 2 spec — multi-protocol layout + Starfish-parity substrate

Owner: Opus implementation agent. Author/auditor: Fable. Status: ACTIVE.
Prerequisite state: Phase 1 closed (see `MODERNIZATION-NOTES.md`); `fab local` green at
240k tx/s / 4 nodes; Phase-1 invariants 1–3 (CLI/log/wire freeze) are now RELAXED — but
every deviation from upstream behavior must be listed in `PHASE2-NOTES.md` (create it,
same style as MODERNIZATION-NOTES). Invariant 4 (no *semantic* changes to Autobahn
protocol logic beyond what this spec orders) holds.

Reuse-first: every item below names the existing module to modify. Do not create parallel
implementations. No git commits — leave the tree dirty for user review.

---

## 1. Protocol enum

`config/src/lib.rs`:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Protocol { AutobahnOptimistic, AutobahnSeamless, Vantage }
impl Default for Protocol { fn default() -> Self { Self::AutobahnOptimistic } }
```

- `Parameters` gains `#[serde(default)] pub protocol: Protocol` (old parameter files stay
  valid). On import, `protocol` **derives** `use_optimistic_tips`
  (optimistic→true, seamless→false, vantage→irrelevant); the raw
  `use_optimistic_tips` field stays in the struct (Autobahn code paths read it as today)
  but is no longer an independent knob — if the JSON sets it inconsistently with
  `protocol`, `protocol` wins and a warning is logged. Add `info!("Protocol: {:?}", ..)`
  next to the existing parameter log lines.
- `node/src/main.rs`: match on protocol — both autobahn variants use the existing
  assembly; `Vantage` bails with a clear "implemented in Phase 3" error.
- Harness: `fabfile.py` node_params gain `'protocol': 'autobahn-optimistic'`;
  `config.py::NodeParameters` already passes unknown keys through (verified) — only add
  the key to the two fabfile tasks and print it in the SUMMARY block of `logs.py`.
  Remove `use_optimistic_tips` from fabfile node_params (protocol subsumes it).

## 2. `autobahn-seamless` — activate the certified-tips path

Upstream marks non-optimistic tips as TODO (`config/src/lib.rs:86`), but
`primary/src/core.rs` already carries both branches: `current_certified_tips` is
maintained (`core.rs:270`, `core.rs:398`), consumed for proposals (`core.rs:872`) and for
coverage (`enough_coverage`, `core.rs:1503`). The work is to **complete and validate**
these branches, not to write new machinery:

- Audit every `use_optimistic_tips` read site (grep) plus the downstream consumers of
  `current_certified_tips` for gaps (validation of incoming proposals, sync/fetch of
  proposal payloads at commit, ticket/waiting-slot re-checks). Fix minimally.
- The authors' `-blips` branches (local refs `upstream/*-blips`) contain their
  seamlessness experiments — consult them read-only when a branch's intent is unclear.
- Add a `debug_assert!`/test-mode check for the defining invariant: under seamless, every
  proposal entering a cut references a height ≤ the author's last *certified* height
  (no uncertified tip ever enters consensus).

## 3. blake3 digests (Starfish parity)

- Workspace dep `blake3 = "1.5"`. `crypto` exposes `pub type Blake3Hasher = blake3::Hasher;`
  (Starfish naming, `crypto.rs:95` there). `crypto::Digest` stays `pub struct Digest(pub [u8; 32])`.
- Replace every SHA-512 hash site with blake3 — mechanical pattern: `Sha512::new()` →
  `Blake3Hasher::new()`; drop the 64→32-byte truncation (`finalize()` is 32 B; use
  `.into()`). Inventory (grep-verified): `primary/src/messages.rs` (~18 live sites, plus
  commented ones — leave comments alone), `primary/src/committer.rs:257`,
  `worker/src/batch_maker.rs`, `worker/src/processor.rs`, `worker/src/tests/common.rs`.
- Then remove `ed25519-dalek = "1.0.1"` from `primary/Cargo.toml` and `worker/Cargo.toml`
  (this completes F1 Path B — the legacy dalek/curve25519/sha2-0.9/rand-0.7 stack must
  disappear from `Cargo.lock`). `crypto` keeps dalek 2 for signatures.
- Gate: `grep -rn "Sha512\|ed25519_dalek" --include="*.rs" primary/src worker/src` → only
  hits allowed are inside comments; `Cargo.lock` contains exactly one `ed25519-dalek`.

## 4. Transaction format + payload modes (Starfish parity)

Current client format (`node/src/benchmark_client.rs:150–158`):
`[1 B marker (0=sample, 1=standard)][8 B id, BE][zero padding]`, min size 9.

New format — **keep the marker+id prefix byte-identical** (the sample machinery in
`worker/src/batch_maker.rs:123–150` reads bytes 0..9 and must keep working for the
legacy cross-validation metric):

```
[1 B marker][8 B id (BE, as today)][8 B UTC-millis timestamp (LE, starfish parity)][payload…]
```

- Timestamp: `SystemTime::now()` → millis since epoch, `put_u64_le` (starfish writes
  `to_le_bytes`; extraction uses `from_le_bytes`). Every tx gets one, samples included.
- Payload (bytes 17..size), mode-dependent — mirror starfish `TransactionMode`
  (`~/code/starfish` `config.rs:332`):
  - `all-zero` (default; upstream-equivalent): zeros via the existing `tx.resize`.
  - `random`: fill with `thread_rng` bytes — the honest mode; defeats accidental
    compression/dedup anywhere in the stack.
- Enforce `size >= 17` (client-side error, replacing the current `>= 9` check). Note in
  PHASE2-NOTES: our header is 17 B (starfish's is 16 — we keep the extra marker byte for
  the legacy sample metric).
- CLI: `benchmark_client` gains optional `--mode <all-zero|random>` (default `all-zero`).
  Plumb through `commands.py` and a `tx_mode` bench param in `fabfile.py` (additive,
  documented CLI change).

## 5. Real transaction latency — starfish-style prometheus metrics (headline metric)

USER DIRECTIVE (2026-07-22, supersedes the earlier log-line design): measurement works
like the starfish codebase — in-process prometheus metrics scraped over HTTP, no log
parsing for this metric.

Definition (starfish parity): commit-time minus the embedded submission timestamp, for
**every** committed transaction, full distribution. NTP-grade clock sync assumed. The
legacy sample metric stays untouched alongside (cross-validation).

Port the starfish pattern — reference files in `~/code/starfish/crates/starfish-core/src`:
`stat.rs` (`PreciseHistogram`, `histogram()`), `metrics.rs` (`Metrics`, `MetricReporter`,
`HistogramSender`/`HistogramReporter`, gauge labeling), `prometheus.rs`
(`start_prometheus_server`, axum), `validator.rs:56–91` (boot wiring); consumption format
in `crates/orchestrator/src/measurements.rs:832–838`.

- New workspace crate **`metrics`** (worker and primary both link it; the Phase-3+
  vantage core will reuse it), porting minimally from starfish:
  - `stat.rs`-style `PreciseHistogram<T>` + `histogram()` → (`HistogramSender<T>`,
    reporter side). Hot path = `sender.observe(x)` (channel push, no locks).
  - `HistogramReporter` periodic task drains into the histogram and publishes **exact**
    quantiles as labeled gauges — `name{v="count"|"sum"|"p25"|"p50"|"p75"|"p90"|"p99"|"max"}`
    — in **microseconds**, exactly starfish's exposition shape, into a
    `prometheus::Registry`.
  - Phase-2 `Metrics` struct: `transaction_committed_latency: HistogramSender<Duration>`,
    `transaction_committed_latency_squared_micros: IntCounter` (stddev),
    `committed_transactions: IntCounter`, `committed_bytes: IntCounter`,
    `latency_misses: IntCounter` (batch-not-in-store skips).
  - `start_prometheus_server(addr, &registry)` — axum, as starfish `prometheus.rs`.
    Dependency choices follow starfish: `prometheus = "0.13"`, `axum = "0.7"`.
- Data flow (unchanged in kind): committer → `PrimaryWorkerMessage::Committed(Vec<Digest>)`
  (`primary/src/primary.rs:59`, variant appended **last** — bincode indices must not
  shift), sent at the exact commit point where the `Committed B…` line is logged, to the
  local worker(s) by `WorkerId`; worker metrics task: dedup digests, read batch from
  store (miss → `latency_misses`, skip — never block), extract per-tx timestamp
  (bytes 9..17 LE, §4), `observe(now − ts)` + squared-micros counter. The send site and
  worker task are `#[cfg(feature = "benchmark")]`; the metrics server itself is always on
  (starfish parity — it also serves Phase-3+ protocol metrics).
- Addresses: the committee schema gains a `metrics` address per primary and per worker
  (primary's registry is near-empty in Phase 2; wired anyway so Phase 3+ only adds
  counters). `config.py::LocalCommittee` port allocation grows from 6 to 8 ports per
  authority — document the new layout in PHASE2-NOTES.md. committee.json is regenerated
  every run, so no back-compat concern.
- Harness consumption (fab plays the orchestrator's role): `local.py` (and `remote.py`
  symmetrically) scrapes every metrics endpoint via stdlib `urllib` right **before**
  killing the nodes, saving `metrics-primary-<i>.txt` / `metrics-worker-<i>-<j>.txt`
  beside the logs; `logs.py` parses the text exposition format (plain regex — starfish's
  own measurements.rs does the same) and adds to RESULTS:
  `Real transaction latency: avg X ms (stddev Y), p50/p90/p99 … ms (N txs, M misses)`.
  Cross-node aggregation starfish-style: exact global avg/stddev from summed
  count/sum/sum²; percentiles reported as the **median across nodes** of per-node
  percentiles (what starfish's orchestrator reports). Saved snapshot files keep results
  re-analyzable offline.
- Semantics note for PHASE2-NOTES: quantiles are cumulative over the whole run (warm-up
  included) — the same property as starfish's end-of-run summaries.

### §5 amendments (Fable audit, 2026-07-23)

- **Observation instant**: `PrimaryWorkerMessage::Committed` carries the committer's
  commit timestamp — `Committed(u64 /* commit UTC-millis */, Vec<Digest>)` — taken once
  per committed header at the "Committed" log site. The worker computes
  `commit_millis − tx_submit_millis` instead of `now − tx_submit_millis`, so the metric
  measures submission→commit exactly (starfish observes at its commit handler, same
  instant) and is immune to the primary→worker notification hop and the worker
  synchronizer's queueing delay. This supersedes the original "observe(now − ts)" text.
- **Cross-node aggregation**: `count`/`misses` aggregate by **max** across nodes, not
  sum — every replica commits the entire log, so per-node counts are near-identical
  countings of the same set (starfish's `aggregate_rate` convention). `sum`/`sum²`
  stay summed, used only inside the avg/stddev ratios, where the common scale factor
  cancels. This supersedes the original "summed count/sum/sum²" wording; rationale in
  PHASE2-NOTES.md.

## 6. Dead code + pre-existing test rot (deferred here from Phase 1)

- `primary/src/tests/core_tests.rs`: fix the 12×E0061 (`Core::spawn` 24→27 args — supply
  the three missing `Parameters` values with the fabfile defaults).
  Decision (Fable, 2026-07-22): six of the twelve are deeper-rotted single-Core
  integration tests — five await a quorum a single spawned Core can never reach (hang),
  one asserts a stale first-message expectation (`process_header`). Mark exactly those
  six `#[ignore = "<factual reason>"]`, inventory them by name + symptom in
  PHASE2-NOTES.md, and do NOT build a multi-Core/mocked-quorum harness in this phase —
  nor delete them (they document intended Prepare/Confirm/Commit behavior for later
  phases). The six arg-fix-rescued tests plus proposer_tests must pass.
- `worker`: `QuorumWaiter` is never constructed (compiler warning) and
  `batch_maker_tests.rs` fails 2×E0308 expecting `QuorumWaiterMessage`. Verify
  unreferenced, then delete `quorum_waiter.rs` + `QuorumWaiterMessage` and rewrite the
  tests against the actual `Vec<u8>` channel.
- Remove only dead code that is *warning-evident or made dead by this spec*; broader
  sweeps belong to the simplification pass.
- `benchmark/requirements.txt`: refresh — `fabric>=3.2`, current `boto3`, current
  `matplotlib`, add the missing `google-cloud-compute` (fabric 3 runs this fabfile
  unmodified; verified 2026-07-22).

## 7. RocksDB tuning — starfish parity (user directive, added 2026-07-23)

Reference (READ-ONLY): `~/code/starfish/crates/starfish-core/src/rocks_store.rs`
(`open`, `block_options`, `metadata_cf_options`, `data_cf_options`). Port the *tuning*,
not the column-family layout: starfish splits one DB into metadata vs bulk-data CFs; the
artifact already separates the same concerns into per-component DBs (primary store =
headers/certs/payload markers; each worker store = batch bytes), so the two starfish
profiles map to store **instances**. Single default CF per DB stays; the channel +
`notify_read` Store API stays byte-identical — this is an Options-only change plus a
profile constructor.

- `store/src/lib.rs`: `pub enum StoreProfile { Metadata, Data }`;
  `Store::new_with_profile(path, profile)`; keep `Store::new(path)` ≡ Metadata so test
  fixtures stay unchanged.
- DB-wide options, both profiles (from starfish `open()`):
  - `create_if_missing(true)`;
  - fd limit: `fdlimit::raise_fd_limit()`, on `Outcome::LimitRaised { to, .. }` call
    `set_max_open_files((to / 8) as i32)` — new small dep `fdlimit` (workspace dep);
  - `set_table_cache_num_shard_bits(10)`;
  - `set_compression_type(Lz4)`, `set_bottommost_compression_type(Zstd)`,
    `set_bottommost_zstd_max_train_bytes(1024 * 1024, true)`;
  - `set_db_write_buffer_size(2 GiB)`, `set_write_buffer_size(256 MiB)`,
    `set_max_write_buffer_number(6)`;
  - `set_max_total_wal_size(2 GiB)`; `increase_parallelism(8)`; `set_use_fsync(false)`;
  - `set_writable_file_max_buffer_size(64 MiB)`; `set_target_file_size_base(128 MiB)`;
  - `set_enable_pipelined_write(true)`; `set_memtable_prefix_bloom_ratio(0.02)`.
- Profile deltas (starfish `metadata_cf_options` / `data_cf_options`):
  - **Metadata**: level compaction (default style), L0 triggers 4 / 48 / 64
    (trigger, ×12 slowdown, ×16 stop); block table 16 KiB blocks + 128 MiB LRU cache.
  - **Data**: `DBCompactionStyle::Universal`, L0 triggers 80 / 96 / 128; block table
    128 KiB blocks + 512 MiB LRU cache.
  - Both block tables: `set_bloom_filter(10.0, false)` and
    `set_pin_l0_filter_and_index_blocks_in_cache(true)` (starfish `block_options`).
- All writes use explicit `WriteOptions` with `set_sync(false)`.
- Wiring: primary store opens **Metadata**, worker stores open **Data** (wherever
  `Store::new` is called in `node/src` and the worker spawn path).
- Cargo: workspace `rocksdb = { version = "0.23", features = ["lz4", "zstd"] }` — the
  compression settings above require those features. Omit starfish's
  `multi-threaded-cf`: the store task solely owns the DB and no CF handles cross
  threads. starfish pins 0.22 — if 0.23 renamed any of these setters, adapt mechanically
  and note it in PHASE2-NOTES.md.

Land this **before** the gate runs so items 2–5 below measure the tuned build.

## 8. `local-benchmark` + Grafana dashboard — starfish-style local runs (user directive, added 2026-07-23)

The user does not want `fab local` as the local vehicle (Python/fab/tmux orchestration,
log-scrape summaries). Replace it for local use with what starfish does: a Rust
subcommand that self-hosts the whole benchmark, plus a dockerized
prometheus+grafana monitoring stack with a live dashboard. References (READ-ONLY):
`~/code/starfish/crates/starfish/src/main.rs` (`Operation::DryRun`/`LocalBenchmark`,
`dryrun()` — note `Committee::new_for_benchmarks`, `NodePublicConfig::new_for_benchmarks`,
in-memory config generation), `~/code/starfish/local-dryrun/data/{docker-compose.yml,
prometheus.yaml}`, `~/code/starfish/monitoring/grafana/*`.

Deviation from starfish, deliberate: starfish dockerizes the nodes themselves (one
`dry-run` container per authority); we run nodes **natively in one process** (starfish's
`LocalBenchmark` shape — the artifact has no Dockerfile, and native means no image
rebuild per code change) and dockerize **only** the stock monitoring containers.

- **`node local-benchmark` subcommand** (extends the existing clap `Command` in
  `node/src/main.rs` — reuse the existing spawn paths, do not fork them):
  `--nodes 4 --workers 1 --rate 240000 --tx-size 512 --protocol autobahn-optimistic
  --mode all-zero --duration 60 --base-port 4000 --data-dir .local-bench`.
  - Generates committee + parameters **in-memory** (new `Committee::local_benchmark(n,
    workers, base_port)` in `config`, port layout identical to config.py's: 3 primary +
    4 per-worker ports per authority; fresh keys via the existing crypto keygen). Writes
    them into `--data-dir` for reference/debugging, not as the source of truth.
  - Spawns, in this one process: every primary (Metadata store profile), every worker
    (Data profile), and one client task per worker. The client: extract the existing
    `benchmark_client.rs` logic into a reusable module (`node/src/client.rs` — the
    `benchmark_client` bin keeps working as a thin wrapper; reuse the type, don't
    duplicate it).
  - Per-node stores/logs under `--data-dir/node-<i>/` (wiped at start, like starfish's
    dryrun does); add the dir to `.gitignore`.
  - On `--duration` expiry or Ctrl-C: print the RESULTS block **computed in-process**
    from each node's own `Registry` (no log parsing, no scraping ourselves): consensus
    TPS from the existing committed counters, real-latency avg/stddev/p50/p90/p99
    aggregated exactly like logs.py's audited rules (max for count/misses, median across
    nodes for percentiles, summed sum/sum² for avg/stddev).
- **Monitoring stack**: new `monitoring/` dir in the repo —
  `docker-compose.yml` (prometheus + grafana only, stock images, adapted from starfish's
  local-dryrun compose: anonymous-admin grafana, provisioned datasource + dashboard),
  `prometheus.yaml` **generated by `local-benchmark` into `--data-dir`** (targets = every
  primary/worker metrics endpoint with `node: 'node-<i>-primary'` / `'node-<i>-worker-<j>'`
  labels, 1 s scrape interval, starfish-style). Containers reach native node endpoints
  via `host.docker.internal` (macOS). Host ports: grafana **3003**, prometheus **9095**
  (this machine holds 3001/3002; starfish's 3002/9093 choices collide — verify free at
  generation time and error clearly if not).
  - Dashboard: write our own minimal `grafana-dashboard.json` modeled on starfish's
    provisioning structure (datasource.yaml/dashboard.yaml verbatim-adapted): panels for
    committed TPS (`rate(committed_transactions[10s])`) per node + total, real-latency
    p50/p90/p99 + max (the `transaction_committed_latency{v=...}` gauges), latency
    misses, committed bytes rate. Do not port starfish's dashboard JSON wholesale — its
    panels reference metrics we don't have.
  - `local-benchmark` prints the grafana URL at boot; the monitoring stack is optional
    (`docker compose -f monitoring/docker-compose.yml up -d` documented in a short
    `monitoring/README.md`; the run works fine without it).
- The fab harness stays for **remote** runs (Phase 7, starfish-orchestrator-equivalent)
  and is otherwise untouched; `fab local` remains functional but is no longer the local
  vehicle.
- Resource caps apply (user): CARGO_BUILD_JOBS=4 for builds; the run itself uses
  whatever the configured `--rate` needs.
- Verification: one `local-benchmark` run (optimistic, all-zero, 240k, 60 s) must
  reproduce the fab-local gate-2 numbers (same consensus TPS ballpark; real-latency
  consistent with the §5-amended metric); results block prints; prometheus.yaml
  generated with correct targets; grafana dashboard renders live data during the run
  (verify by curling the prometheus targets + `api/health`; a human look at grafana is
  the user's part).

## Non-goals (parked)

- `set_nodelay` alignment with starfish (Nagle stays ON as upstream shipped; revisit
  before Phase 7 evaluation).
- Grafana dashboards / continuous scraping during the run (endpoint + end-scrape only;
  dashboards revisit in Phase 7 for WAN).
- Any Vantage protocol logic (Phase 3+). Any `signature-free.tex` / paper edits (never).

## Verification & gate

1. `cargo build --workspace --all-targets` (debug + release) and
   `cargo test --workspace` — **fully green**: 0 failures, 0 hangs; the only exceptions
   are the six `#[ignore]`d core_tests documented per §6.
2. `fab local`, `autobahn-optimistic`, `all-zero`: reproduces Phase-1 numbers
   (240k tx/s sustained; sample e2e latency same ballpark — blake3 should only help).
3. Same run: real-latency mean (scraped from the metrics endpoints) vs legacy sample mean
   (from logs) — report both side by side; scrape must succeed on all nodes.
   *Amended criterion (audit, 2026-07-23):* the two are **definitionally different** and
   will not coincide — the legacy sample metric takes the EARLIEST commit timestamp
   across all primaries (`logs.py::_merge_results`, a min order statistic), while the
   real metric is each replica's own commit instant aggregated per §5's rules. Pass =
   real ≥ sample with the delta consistent with cross-replica commit spread (a few ms),
   both reported. The legacy metric's semantics stay untouched — it is the
   comparability anchor to upstream's paper-results.
4. `fab local`, `autobahn-seamless`: live, sustained rate; the §2 certified-only
   assertion never fires; latency delta vs optimistic reported (expect roughly one extra
   car-certification round trip).
5. `fab local`, `random` mode: rate sustained; numbers reported (honesty check — no
   compression anywhere to defeat, so expect parity with all-zero).
6. Simplification pass (dedupe, collapse, delete newly-dead paths) → tests green again →
   **one** adversarial audit pass (Fable). Wire-format and log-format changes get
   line-by-line review against this spec; `PHASE2-NOTES.md` must list every deviation.

Before any `fab local`: `tmux ls` — if user tmux sessions exist, STOP and report (the
harness kills the tmux server). Reuse the session venv:
`/private/tmp/claude-501/-Users-nikitapolianskii-code-tex-projects-signature-free/e3d22549-28b8-43f2-b3e6-5dbd87e1cdde/scratchpad/fabenv/bin/fab`.
`BASE_PORT = 4000` is already set in `local.py` (Docker owns 3001 here) — do not revert.

# Metrics expansion + dashboard — work order (user-approved 2026-07-23)

Precondition: the cleanup/simplification pass (CLEANUP-NOTES.md) is merged — build on the
simplified structure. All additions metrics/harness-only; zero protocol-semantic change;
full throttled suite green per milestone. Starfish reference is READ-ONLY
(crates/starfish-core/src/{metrics.rs,network.rs} — hook sites at network.rs:614-691
send / 713-784 receive; we mirror the pattern).

## 1. Wire-layer counters (network crate hooks, all 3 protocols, both directions)
- `bytes_sent_total`, `bytes_received_total` (IntCounter; include the 4-byte length
  prefix — starfish-comparable).
- `network_messages_sent_total{type}`, `network_messages_received_total{type}`
  (IntCounterVec).
- `network_bytes_sent_total{type}`, `network_bytes_received_total{type}`
  (IntCounterVec — beyond starfish; serialized length is in hand at the same sites).
- `type` label = wire variant name for every PrimaryMessage / PrimaryWorkerMessage /
  WorkerPrimaryMessage / WorkerMessage variant. Tag where the variant is known:
  receiver dispatch (post-deserialize) for received; the send/broadcast call sites for
  sent (a small helper taking `&'static str` type + byte len; senders gain an optional
  metrics handle like the existing with_latency pattern, default None = zero overhead).
- No per-peer labels (matches starfish's omission; n is small).

## 2. Goodput / pipeline
- `submitted_transactions`, `submitted_transactions_bytes` (worker ingress:
  batch_maker's client-received txs).
- Existing `committed_transactions`/`committed_bytes` = the sequenced-goodput
  denominator (unchanged names).
- Derived in local-benchmark RESULTS + dashboard (not stored): overhead bytes per
  sequenced byte; messages per committed tx; bandwidth efficiency = bytes_sent per tx
  normalized to 512B (starfish's formula, metrics.rs:1077-1083); per-category shares.
- Category map (RESULTS + dashboard grouping, from the `type` label):
  - vantage: dissemination = Header(publish) + worker Batch; acks = VantageAck;
    agb = VantagePropose/Echo/EchoSkip/Ready/NoReady; pacemaker = VantageWish;
    repair = HeadersRequest + Header(serve) + PrimaryWorkerMessage::Synchronize +
    worker BatchRequest; control = CompReport + Control*(Init/Echo/Ready/TimeoutVote/
    TimeoutAccept/Commit/Fetch/Serve); metricsplumbing = Committed.
  - autobahn: dissemination = Header + Batch; votes-certs = Vote + Certificate;
    consensus = ConsensusMessage/Request/Vote + Timeout + TC; sync = *Request variants +
    Header(sync) + Synchronize + BatchRequest; metricsplumbing = Committed.
- Explicitly omitted (user-approved): per-peer wire counters, compression counters,
  block_committed_latency (would need an on-wire block timestamp — revisit on request).

## 3. Consensus quality / utilization
- Keep: seal-route vec, the six diagnosis gauges (entered view, frontier, cursor
  next_view, control round, delivered-log len, consume pos).
- Add: `proposed_block_size_bytes` (HistogramSender<usize>, vantage block serialized
  size at publish; report via the existing reporter pattern).
- Add starfish's utilization pattern (metrics.rs:1325-1376): a Drop-guard
  `utilization_timer{proc}` IntCounterVec accumulating busy-µs around VantageCore's
  major sections (inbound dispatch, effect execution, timer firing, payload sync) +
  `core_queue_length` gauge (rx_vantage depth via capacity arithmetic if obtainable
  cheaply; skip if not without code contortions — note the decision).

## 4. Dashboard (single JSON, both modes) — replaces the minimal Phase-2 one
- Rows: Overview (committed TPS, real-latency p50/p90/p99, seal-route rates);
  Consensus (view entry/seal/anchor rates, cursor lag = entered−cursor, control round);
  Network (stacked msg/s and bytes/s by category — the §2 map encoded as Grafana
  transformations or recording rules; overhead-per-goodput ratio; bandwidth
  efficiency); Data plane (blocks published/received, acks, repairs, batches,
  submitted vs sequenced); Node health (up/scrape status, utilization by proc, queue
  depth). Template variable: node. Works for both protocols (panels show what exists).
- Local mode: existing generated prometheus.yaml + compose (grafana 3003 / prom 9095).
- Orchestration mode: `fab remote` (or a `fab monitor` task) generates
  monitoring/prometheus-remote.yaml from the committee's public IPs + metrics ports so
  the SAME local compose watches an AWS run live. Document both flows in
  monitoring/README.md.

## 5. Local dryrun launcher (user-requested, starfish-style UX, Python)

Directory `local-dryrun/` mirroring starfish's layout (~/code/starfish/local-dryrun is
the read-only reference for the UX, not the mechanism):
- `config.yml` (template, committed): every run parameter in one file — protocol,
  nodes, workers, rate, tx_size, duration (`0` = run until Ctrl-C), delta_ms,
  max_batch_delay_ms, max_header_delay_ms, crash, latency_table (path or `none`),
  data_dir. Commented defaults = the n=10/1000tx/s latency experiment.
- `dryrun.py` (Python 3, stdlib + pyyaml only — runs from the session venv or any
  python with pyyaml): (1) read config; (2) `CARGO_BUILD_JOBS=4 cargo build --release
  --features benchmark` (skip via `--no-build`); (3) start the monitoring stack
  (`docker compose -f ../monitoring/docker-compose.yml up -d`, idempotent) and wait for
  grafana health; (4) generate the prometheus targets file for the configured node
  count; (5) `open http://localhost:3003/d/<dashboard-uid>` (macOS `open`, best-effort);
  (6) exec `node local-benchmark` with the config's parameters, streaming its output;
  (7) on exit/Ctrl-C: print RESULTS location; `--down` flag tears the monitoring stack
  down (default leaves it running for post-run inspection).
- `node local-benchmark`: add `--duration 0` = indefinite (clean Ctrl-C shutdown with
  final RESULTS) if not already supported — harness-only change.
- Nodes stay NATIVE processes (the deliberate Phase-2 §8 deviation from starfish's
  node containers: no Dockerfile, no image rebuild per code change); only
  prometheus+grafana are dockerized. Note this in local-dryrun/README.md and record
  that fully-dockerized nodes remain available as a future option if the user asks.

## 6. Verification
Full suite; one local-benchmark run per protocol (n=4 short) confirming: counters
populate for every category, RESULTS prints the taxonomy + ratios, dashboard JSON lints
(grafana API or jq schema sanity); Autobahn 240k regression unchanged (metrics hooks
must not cost measurable throughput — compare against the gate range); one full
`local-dryrun/dryrun.py` end-to-end run (config file → monitoring up → dashboard URL
printed/opened → benchmark streams → Ctrl-C clean) with the dashboard verified live
against a real run via the grafana API (panels return data); notes to METRICS-NOTES.md.
AWS live-dashboard validation deferred until the user okays an instance-hour.

## 7. Local scaling sweep (user question: max n without much degradation — 20? 30?)

Run LAST, after all builds/tests are done and the machine is otherwise idle (concurrent
compilation would corrupt the numbers). No code changes — flags exist.

- Sweep n ∈ {4, 10, 15, 20, 25, 30}, vantage AND autobahn-optimistic (seamless only at
  the final chosen n as a sanity point), 1 worker/node, tx-size 512, Δ=150, cadences
  20/50, loopback (no latency table — this measures MACHINE capacity, not geography).
- Per n, two measurements (30s runs suffice): (a) latency at fixed total offered
  1,000 tx/s — record sustained rate + real-latency avg/p50/p99 + any non-happy-path
  seal routes (a fallback route appearing = scheduling jitter = degradation signal);
  (b) coarse capacity: try total rates {50k, 100k, 240k} descending, record the
  highest fully-sustained one (no bisection — coarse is fine).
- Degradation criteria for the recommendation: offered 1k fully sustained AND
  latency-at-1k within ~1.5× of the n=10 value AND zero fallback seal routes AND
  capacity ≥ 50k. Report the largest conforming n, with the full table, and one
  confirmation run at that n WITH the latency table (extend the 10×10 by round-robin
  region assignment for n>10; note the construction).
- Deliverable: table + one-paragraph recommendation ("use up to n=X locally; beyond
  that use AWS") appended to METRICS-NOTES.md; also stamp the host spec (14 logical
  cores) since the answer is machine-relative.

## 8. Addenda (user, 2026-07-23, approved)

- **Protocol + tx-mode info gauges** (starfish pattern: `consensus_protocol_info`
  label `protocol`, `transaction_mode_info`): write-once at boot on every node's
  registry — `protocol_info{protocol="vantage|autobahn-optimistic|autobahn-seamless"}=1`
  and `transaction_mode_info{mode="all-zero|random"}=1`. Dashboard Overview row gets a
  stat panel showing the running protocol + mode (prominent, first panel).
- **Random tx generation becomes the DEFAULT** (all-zero stays available): flip the
  default in the client CLI (`--mode`), `local-benchmark`, `local-dryrun/config.yml`,
  fabfile remote task, and config.py BenchParameters. IMPORTANT for comparability: the
  §6/§7 guard benchmarks and the scaling sweep PIN `--mode all-zero` explicitly (all
  historical gate numbers are all-zero; random adds client-side CPU cost measured in
  Phase 2). Note the flip in METRICS-NOTES.
- **Network-level compression behind a flag, default OFF**: lz4 (pure-rust
  `lz4_flex`, workspace dep) applied uniformly in the network crate — compress the
  serialized payload before length-prefix framing on send, decompress after framing on
  receive, BOTH senders + Receiver, all protocols identically (fairness). Gate by a
  `compress_network: bool` `Parameters` field (`#[serde(default)]` = false) + flags on
  local-benchmark/dryrun config/fabfile — committee-wide consistent by construction
  (all nodes share Parameters; mixed settings would fail decode, note this). Add
  starfish's `bytes_uncompressed_sent_total` counter (§1's omission reverses — it's
  meaningful now); dashboard gets a compression-ratio panel (uncompressed/wire).
  Default-off must be byte-identical framing (regression guard: one 240k run, off).
  Reference for hook placement: starfish network.rs:614-691 (read-only).

Hard rules: no git writes (coordinator commits); no protocol semantics; tex-projects
and starfish read-only; CARGO_BUILD_JOBS=4 etc.; STOP on anything ambiguous.

// Copyright(C) Facebook, Inc. and its affiliates.
// `node local-benchmark` (PHASE2-SPEC.md #8): self-hosts a whole local run -- every
// primary, every worker, and one client task per worker -- in this single process,
// replacing `fab local` as the local vehicle (fab stays for remote/Phase-7 runs).
// Reuses the exact same spawn paths (`Primary::spawn`, `Worker::spawn`) and client
// logic (`crate::client::Client`) the standalone binaries use -- no parallel
// reimplementation of any of it -- plus generates a `prometheus.yaml` for the optional
// `monitoring/` docker-compose stack.

use crate::client::{Client, TransactionMode};
use crate::CHANNEL_CAPACITY;
use anyhow::{Context, Result};
use clap::ArgMatches;
use config::{Committee, Export as _, KeyPair, LatencyTable, Parameters, Protocol, WorkerId};
use crypto::{PublicKey, SignatureService};
use metrics::{
    aggregate_latency_snapshots, read_counter, read_counter_vec, read_latency_snapshot,
    read_materialised_latency_snapshot, read_vantage_progress, LatencySnapshot, MetricReporter,
};
use primary::Primary;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use store::{Store, StoreProfile};
use tokio::sync::mpsc::channel;
use worker::Worker;

/// clippy::type_complexity: named alias for `spawn_node_primary`/`spawn_node_workers`'s
/// shared return shape -- (this node's metrics registry, its periodic reporter, and
/// its own (label, address) scrape target for `prometheus.yaml`).
type NodeMetricsHandle = (
    prometheus::Registry,
    Arc<MetricReporter>,
    (String, SocketAddr),
);

pub async fn run(matches: &ArgMatches) -> Result<()> {
    let nodes: usize = matches
        .get_one::<String>("nodes")
        .unwrap()
        .parse()
        .context("--nodes must be a positive integer")?;
    let workers: usize = matches
        .get_one::<String>("workers")
        .unwrap()
        .parse()
        .context("--workers must be a positive integer")?;
    let rate: u64 = matches
        .get_one::<String>("rate")
        .unwrap()
        .parse()
        .context("--rate must be a non-negative integer")?;
    let tx_size: usize = matches
        .get_one::<String>("tx-size")
        .unwrap()
        .parse()
        .context("--tx-size must be a positive integer")?;
    let protocol_str = matches.get_one::<String>("protocol").unwrap().clone();
    let mode_str = matches.get_one::<String>("mode").unwrap().clone();
    let duration: u64 = matches
        .get_one::<String>("duration")
        .unwrap()
        .parse()
        .context("--duration must be a non-negative integer")?;
    let base_port: u16 = matches
        .get_one::<String>("base-port")
        .unwrap()
        .parse()
        .context("--base-port must be a valid port number")?;
    let data_dir = PathBuf::from(matches.get_one::<String>("data-dir").unwrap());
    let crash: usize = matches
        .get_one::<String>("crash")
        .unwrap()
        .parse()
        .context("--crash must be a non-negative integer")?;
    anyhow::ensure!(
        crash < nodes,
        "--crash ({}) must be strictly less than --nodes ({})",
        crash,
        nodes
    );
    let live_nodes = nodes - crash;
    // Load-skew: which of the LIVE nodes spawn client tasks. Absent, defaults to every
    // live node (today's behavior, byte-identical). The loaded set is always "the
    // first `load_nodes` live indices" -- since crashed nodes are already excluded by
    // being the trailing `crash` indices (R2, just above), this selection can never
    // overlap the crashed set without any extra bookkeeping here.
    let load_nodes: usize = match matches.get_one::<String>("load-nodes") {
        Some(s) => s
            .parse()
            .context("--load-nodes must be a positive integer")?,
        None => live_nodes,
    };
    anyhow::ensure!(
        load_nodes >= 1,
        "--load-nodes ({}) must be at least 1 (a run with zero clients measures nothing)",
        load_nodes
    );
    anyhow::ensure!(
        load_nodes <= live_nodes,
        "--load-nodes ({}) must be at most the number of live nodes ({})",
        load_nodes,
        live_nodes
    );
    // Data-plane withholding fault injector: indexed over the FULL committee (0-based,
    // sorted order), the same universe `--crash`/`--load-nodes` share -- not clamped to
    // `live_nodes`, since a withholding sender index that happens to fall in the
    // crashed (trailing) range is simply never spawned at all, a harmless no-op rather
    // than an error (see `withheld_destinations`'s own doc comment for the derivation
    // every node performs locally).
    let withhold: usize = matches
        .get_one::<String>("withhold")
        .unwrap()
        .parse()
        .context("--withhold must be a non-negative integer")?;
    anyhow::ensure!(
        withhold <= nodes,
        "--withhold ({}) must be at most --nodes ({})",
        withhold,
        nodes
    );
    // Time-windows the data-plane withholding fault injector above: absent (the
    // default) means WHOLE-RUN withholding, byte-identical to --withhold's original
    // (unwindowed) behavior. `--withhold-at 0` (withholding starts immediately at
    // measurement start) is just an ordinary value -- there is no "explicit 0 vs.
    // absent" ambiguity to resolve here.
    let withhold_at_secs: Option<u64> = matches
        .get_one::<String>("withhold-at")
        .map(|s| {
            s.parse()
                .context("--withhold-at must be a non-negative integer")
        })
        .transpose()?;
    let withhold_for_secs: u64 = matches
        .get_one::<String>("withhold-for")
        .unwrap()
        .parse()
        .context("--withhold-for must be a non-negative integer")?;
    anyhow::ensure!(
        withhold_at_secs.is_none() || withhold > 0,
        "--withhold-at requires --withhold > 0"
    );
    let delta_ms: u64 = matches
        .get_one::<String>("delta-ms")
        .unwrap()
        .parse()
        .context("--delta-ms must be a non-negative integer")?;
    let max_batch_delay_ms: u64 = matches
        .get_one::<String>("max-batch-delay-ms")
        .unwrap()
        .parse()
        .context("--max-batch-delay-ms must be a non-negative integer")?;
    let max_header_delay_ms: u64 = matches
        .get_one::<String>("max-header-delay-ms")
        .unwrap()
        .parse()
        .context("--max-header-delay-ms must be a non-negative integer")?;
    let batch_max_bytes: usize = matches
        .get_one::<String>("batch-max-bytes")
        .unwrap()
        .parse()
        .context("--batch-max-bytes must be a non-negative integer")?;
    let batch_max_delay_ms: u64 = matches
        .get_one::<String>("batch-max-delay-ms")
        .unwrap()
        .parse()
        .context("--batch-max-delay-ms must be a non-negative integer")?;
    let ack_watermark_period_ms: u64 = matches
        .get_one::<String>("ack-watermark-period-ms")
        .unwrap()
        .parse()
        .context("--ack-watermark-period-ms must be a non-negative integer")?;
    // PHASE7-PREP-NOTES.md Finding A: diagnostic-only, off by default. Auto-enabled
    // whenever a withholding window is configured (`--withhold-at`), since its whole
    // point is to make the committed-throughput dip/recovery shape visible (see the
    // `TIMELINE:` series below).
    let timeline: bool = matches.get_flag("timeline") || withhold_at_secs.is_some();
    // PHASE7-PREP-NOTES.md (WAN-shaped local runs): `--latency-table <csv>` (an n x n
    // RTT-ms matrix, node index = committee order) takes precedence over
    // `--mimic-latency-ms <u64>` (the uniform EXPLICIT OVERRIDE shorthand -- defined
    // as exactly the trivial table whose every cell is that value -- see
    // `LatencyTable::uniform`). If NEITHER is given, DEFAULT to the real 10-AWS-region
    // RTT matrix (`LatencyTable::aws_rtt`, ported VERBATIM from starfish) -- mirroring
    // starfish's own default for single-region AWS benchmarking; flags above override
    // this default.
    //
    // Fable audit FIX 1: the clap default for `--mimic-latency-ms` is the string "0",
    // which is indistinguishable from an EXPLICITLY PASSED `--mimic-latency-ms 0`
    // unless the two are told apart via clap's value-source API -- otherwise, once
    // "neither given" started defaulting to `aws_rtt` below, no flag combination could
    // ever request zero injected latency again (every existing zero-latency
    // invocation, e.g. local-dryrun's documented pure-loopback mode, would silently
    // become WAN-shaped). Resulting precedence:
    //   1. `--latency-table <csv>` given                   -> that CSV
    //   2. `--mimic-latency-ms <n>` explicitly given, n > 0 -> `LatencyTable::uniform(nodes, n)`
    //   3. `--mimic-latency-ms 0` explicitly given          -> `None` (zero injected latency, pure loopback)
    //   4. neither given                                    -> `LatencyTable::aws_rtt(nodes)` (default)
    let mimic_latency_ms: u64 = matches
        .get_one::<String>("mimic-latency-ms")
        .unwrap()
        .parse()
        .context("--mimic-latency-ms must be a non-negative integer")?;
    let mimic_latency_explicit =
        matches.value_source("mimic-latency-ms") == Some(clap::parser::ValueSource::CommandLine);
    let latency_table_path = matches.get_one::<String>("latency-table").cloned();
    let latency_table: Option<LatencyTable> = if let Some(path) = &latency_table_path {
        let table = LatencyTable::from_rtt_csv(path, nodes).with_context(|| {
            format!(
                "Failed to parse --latency-table '{}' as a {n}x{n} RTT-ms CSV matrix",
                path,
                n = nodes
            )
        })?;
        println!(
            "Latency table: loaded from {} ({}x{} RTT-ms matrix, node index = committee order)",
            path, nodes, nodes
        );
        Some(table)
    } else if mimic_latency_explicit && mimic_latency_ms > 0 {
        println!(
            "Latency table: uniform {} ms RTT ({} ms one-way) on every inter-authority link (--mimic-latency-ms)",
            mimic_latency_ms,
            mimic_latency_ms / 2
        );
        Some(LatencyTable::uniform(nodes, mimic_latency_ms as f64))
    } else if mimic_latency_explicit {
        println!(
            "Latency table: none -- zero injected latency, pure loopback (--mimic-latency-ms 0 explicitly given)"
        );
        None
    } else {
        println!(
            "Latency table: real 10-AWS-region RTT matrix (default, committee index i -> region i % 10)"
        );
        Some(LatencyTable::aws_rtt(nodes))
    };

    let protocol = match protocol_str.as_str() {
        "autobahn-optimistic" => Protocol::AutobahnOptimistic,
        "autobahn-seamless" => Protocol::AutobahnSeamless,
        "vantage" => Protocol::Vantage,
        "simple-it" => Protocol::SimpleIt,
        "simple-it-bracha" => Protocol::SimpleItBracha,
        other => anyhow::bail!(
            "Unknown --protocol '{}'; use autobahn-optimistic, autobahn-seamless, vantage, \
             simple-it, or simple-it-bracha",
            other
        ),
    };
    let mode = TransactionMode::parse(&mode_str).context("Invalid --mode")?;

    println!("\n=== local-benchmark configuration ===");
    println!(
        "Nodes: {}   Workers/node: {}   Rate: {} tx/s   Tx size: {} B",
        nodes, workers, rate, tx_size
    );
    println!(
        "Protocol: {:?}   Mode: {:?}   Duration: {} s   Base port: {}",
        protocol, mode, duration, base_port
    );
    println!(
        "Delta: {} ms   Max batch delay: {} ms   Max header delay: {} ms",
        delta_ms, max_batch_delay_ms, max_header_delay_ms
    );
    println!("Data dir: {}", data_dir.display());
    if !matches.get_flag("no-ack-watermarks") {
        println!(
            "Ack watermarks: ON (periodic per-lane availability broadcast every {} ms, replaces per-block acks)",
            ack_watermark_period_ms
        );
    }
    if !matches.get_flag("no-digest-statements") {
        println!(
            "Digest-named AGB statements: ON (Vantage ECHO/READY name their proposal by hash instead of by value)"
        );
    }
    if crash > 0 {
        println!(
            "Crash fault: {} of {} nodes never spawned (committee unchanged; live = {})",
            crash, nodes, live_nodes
        );
    }
    if load_nodes < live_nodes {
        println!(
            "Load: {} of {} live node(s) receive client transactions (aggregate rate unchanged)",
            load_nodes, live_nodes
        );
    }
    if withhold > 0 {
        // Stable, grep-parseable: the "only X of N nodes" count is the staggered
        // unblocked half INCLUDING the withholding sender itself (n - floor(n/2)),
        // matching `withheld_destinations`'s own derivation exactly. The line stays
        // BYTE-IDENTICAL to c35fc4a's own (grep-stable) wording when no window is
        // configured; `--withhold-at` appends one trailing clause rather than
        // altering anything already printed.
        let reachable = nodes - nodes / 2;
        match withhold_at_secs {
            Some(at) => println!(
                "Withhold: first {} node(s) disseminate payload to only {} of {} nodes \
                 (staggered halves; repair paths unaffected), active T+{}s..T+{}s",
                withhold,
                reachable,
                nodes,
                at,
                at + withhold_for_secs
            ),
            None => println!(
                "Withhold: first {} node(s) disseminate payload to only {} of {} nodes \
                 (staggered halves; repair paths unaffected)",
                withhold, reachable, nodes
            ),
        }
    }
    println!("======================================\n");

    // Wipe and recreate the data dir (starfish's own local-benchmark does the same).
    let _ = fs::remove_dir_all(&data_dir);
    fs::create_dir_all(&data_dir).context("Failed to create --data-dir")?;

    // Committee + parameters generated in-memory (config::Committee::local_benchmark);
    // written to --data-dir for reference/debugging only -- nothing re-reads them, they
    // are not the source of truth the way committee.json/parameters.json are for `fab`.
    let (committee, keypairs) = Committee::local_benchmark(nodes, workers, base_port);
    committee
        .export(data_dir.join("committee.json").to_str().unwrap())
        .context("Failed to write committee.json")?;

    // Data-plane withholding fault injector, time-windowed variant: the shared,
    // in-process "has the window opened yet" cell (see `Parameters::withhold_window`'s
    // own doc comment) -- created empty here, BEFORE any node spawns, and cloned into
    // every node's own `Parameters` below; armed (via `.set(..)`) once `run_start` is
    // known, further down. `None` whenever `--withhold-at` isn't given, so no cell is
    // even allocated on the default (unwindowed or disabled) path.
    let withhold_window_cell: Option<Arc<OnceLock<(std::time::Instant, std::time::Instant)>>> =
        withhold_at_secs.map(|_| Arc::new(OnceLock::new()));

    let mut parameters = Parameters {
        protocol,
        delta_ms,
        // FAIRNESS: derive Simple-IT's round timeout from the SAME Delta as Vantage's
        // theta_E/theta_R rather than leaving it on Autobahn's unrelated 5s default.
        // `CutEngine`'s timer is `parameters.timeout_delay` (simpleit/node.rs), so
        // without this the two signature-free protocols would be timed off different
        // bases and any timeout-path comparison would be meaningless. 8x matches the
        // paper's own Delta_to = (d_s + d_t)Delta for the Opt-RBC variant
        // (arXiv:2606.14404, Corollary 5: 8Delta + 4delta). Fault-free runs never fire
        // these timers, so this does not affect happy-path latency either way -- it
        // makes the comparison principled rather than accidental.
        timeout_delay: match protocol {
            Protocol::SimpleIt => delta_ms.saturating_mul(8),
            // arXiv:2606.14404 Corollary 5, variant S (Bracha-RBC): Delta_to = 5Delta
            // + 2delta -- same treatment as `SimpleIt`'s own 8Delta+4delta comment
            // just above (the +2delta/+4delta terms are dropped: this local-benchmark
            // model has no separate delta parameter of its own, only delta_ms).
            Protocol::SimpleItBracha => delta_ms.saturating_mul(5),
            _ => Parameters::default().timeout_delay,
        },
        max_batch_delay: max_batch_delay_ms,
        max_header_delay: max_header_delay_ms,
        // Transport-level batching: ON by default (5 ms / 64 KB per-destination
        // coalescing); --no-batch-messages restores unbatched framing.
        batch_messages: !matches.get_flag("no-batch-messages"),
        batch_max_bytes,
        batch_max_delay_ms,
        // Autobahn (Giridharan et al., SOSP'24) §5.5.3: off by default, byte-identical
        // behavior when off.
        all_to_all: matches.get_flag("all-to-all"),
        ack_watermarks: !matches.get_flag("no-ack-watermarks"),
        ack_watermark_period_ms,
        digest_statements: !matches.get_flag("no-digest-statements"),
        withhold_senders: withhold,
        // Data-plane withholding fault injector, time-windowed variant:
        // `withhold_at_ms`/`withhold_for_ms` are meaningless/unused whenever
        // `withhold_at_secs` is absent (whole-run withholding -- see
        // `config::withhold_active`'s own doc comment). `withhold_window` is the
        // shared cell created just above -- `#[serde(skip)]`, same
        // never-round-trips-through-JSON treatment as `latency_table` below.
        withhold_at_ms: withhold_at_secs.map(|secs| secs * 1000),
        withhold_for_ms: withhold_for_secs * 1000,
        withhold_window: withhold_window_cell.clone(),
        // PHASE7-PREP-NOTES.md (WAN-shaped local runs): `#[serde(skip)]` on this field
        // means it never round-trips through the `parameters.json` export just below --
        // set on the in-memory `Parameters` every node's `Primary::spawn` receives, which
        // is all `Core::spawn`/`vantage::node::VantageCore::spawn` ever read it from.
        latency_table: latency_table.map(Arc::new),
        ..Parameters::default()
    };
    parameters.reconcile_protocol();
    parameters
        .export(data_dir.join("parameters.json").to_str().unwrap())
        .context("Failed to write parameters.json")?;

    for (i, keypair) in keypairs.iter().enumerate() {
        keypair
            .export(data_dir.join(format!("node-{}.json", i)).to_str().unwrap())
            .context("Failed to write node keypair")?;
    }

    // R2 (PHASE6-SPEC.md): a true crash fault -- the committee above already covers all
    // `nodes` authorities unchanged (every live node still sees the full membership,
    // e.g. still counts the crashed node's absence as an ordinary faulty-party gap in
    // quorum thresholds); only the *spawning* below is restricted to the first
    // `live_nodes` keypairs, so the trailing `crash` nodes' primary/worker/client tasks
    // are simply never started -- no process to kill, no message ever sent as them.
    let live_keypairs = &keypairs[..live_nodes];

    // Every LIVE worker's client-facing address -- every client task waits for all of
    // these before sending (mirrors `benchmark_client --nodes`); a crashed node's
    // address must NOT appear here, or every client would wait forever on an address
    // nobody is listening on.
    let all_worker_addresses: Vec<SocketAddr> = live_keypairs
        .iter()
        .flat_map(|keypair| {
            let authority = &committee.authorities[&keypair.name];
            authority.workers.values().map(|w| w.transactions)
        })
        .collect();
    // Offered rate scaled to the LOADED clients only (R2 + load-skew): the same
    // aggregate --rate is divided among however many senders actually exist --
    // `load_nodes` of them, not `live_nodes` -- so each loaded client's own
    // per-client rate rises to compensate; the aggregate offered load is unchanged by
    // which nodes crashed OR by how many live nodes are unloaded. `load_nodes ==
    // live_nodes` (the default) recovers the original divisor exactly.
    let rate_share = rate.div_ceil((load_nodes * workers).max(1) as u64);

    // Spawn every primary and every worker natively, in this one process -- only for
    // the live nodes (R2).
    // Fable audit item 1: kept per NODE index (not a flat list) -- with `--workers >
    // 1`, each worker's own registry only ever observes the slice of the committed
    // stream tagged with ITS OWN worker id (`Synchronizer::observe_committed`), so
    // summing every worker belonging to the same node recovers that node's true
    // committed total; only THEN is it comparable across nodes (see `print_results`).
    let mut worker_metrics: Vec<(usize, prometheus::Registry, Arc<MetricReporter>)> = Vec::new();
    // PHASE6-SPEC.md §9 gate amendment: `vantage_seals` lives on each PRIMARY's own
    // registry (distinct from `worker_metrics` above) -- kept per node index so the
    // RESULTS block can print each node's own route breakdown (they can legitimately
    // differ across nodes) plus a summed total.
    let mut primary_metrics: Vec<(usize, prometheus::Registry, Arc<MetricReporter>)> = Vec::new();
    let mut metrics_targets: Vec<(String, SocketAddr)> = Vec::new(); // (label, addr) for prometheus.yaml
                                                                     // Fable audit item 5: every client task's handle, so they can be stopped (aborted)
                                                                     // before the final, drained re-read -- see the end of this fn.
    let mut client_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    for (i, keypair) in keypairs.into_iter().take(live_nodes).enumerate() {
        let name = keypair.name;
        let node_dir = data_dir.join(format!("node-{}", i));
        fs::create_dir_all(&node_dir)?;

        let (primary_registry, primary_reporter, primary_target) =
            spawn_node_primary(i, keypair, &node_dir, &committee, &parameters, mode)?;
        primary_metrics.push((i, primary_registry, primary_reporter));
        metrics_targets.push(primary_target);

        // Load-skew: `i` ranges over live node indices only (`.take(live_nodes)`
        // below), so "first `load_nodes` of these" is exactly the loaded set --
        // already disjoint from the crashed (trailing) indices by construction; see
        // the `load_nodes` validation above for the same interplay spelled out.
        let client_rate_share = (i < load_nodes).then_some(rate_share);
        let (workers_spawned, workers_client_handles) = spawn_node_workers(
            i,
            name,
            &node_dir,
            workers,
            &committee,
            &parameters,
            tx_size,
            client_rate_share,
            mode,
            &all_worker_addresses,
        )?;
        for (worker_registry, worker_reporter, worker_target) in workers_spawned {
            worker_metrics.push((i, worker_registry, worker_reporter));
            metrics_targets.push(worker_target);
        }
        client_handles.extend(workers_client_handles);
    }

    // Generate prometheus.yaml for the optional monitoring/ stack (PHASE2-SPEC.md #8):
    // native nodes reachable from the dockerized prometheus via host.docker.internal.
    write_prometheus_config(&data_dir, &metrics_targets)?;
    println!(
        "Prometheus target file: {}",
        data_dir.join("prometheus.yaml").display()
    );
    println!(
        "Bring up monitoring (optional): docker compose -f monitoring/docker-compose.yml up -d"
    );
    println!("Grafana (once up): http://localhost:3003\n");

    // METRICS-DASHBOARD-SPEC.md §5: `--duration 0` = run until Ctrl-C (clean shutdown,
    // final RESULTS still printed). Actual wall-clock elapsed time is tracked
    // separately from the configured `duration` so RESULTS' TPS/BPS figures are
    // correct whether the run went the full configured length, was interrupted
    // early, or (duration 0) has no configured length at all.
    if duration == 0 {
        println!("Running benchmark (until Ctrl-C)...");
    } else {
        println!("Running benchmark ({} sec)...", duration);
    }
    let run_start = tokio::time::Instant::now();
    // Data-plane withholding fault injector, time-windowed variant: the window is
    // anchored to THIS benchmark's own measurement start (`run_start`, just captured)
    // rather than to process-spawn time (which happened earlier, in the loop above) --
    // `--withhold-at` is defined as an offset from measurement start, and every node's
    // own `Parameters::withhold_window` clone (set at spawn time, before this point)
    // observes this `.set(..)` the instant it happens, since they all share the same
    // `Arc`.
    if let (Some(cell), Some(at)) = (&withhold_window_cell, withhold_at_secs) {
        let start = run_start.into_std() + std::time::Duration::from_secs(at);
        let end = start + std::time::Duration::from_secs(withhold_for_secs);
        cell.set((start, end))
            .expect("withhold window is set exactly once, right after run_start");
        println!(
            "TIMELINE-WITHHOLD: start={} end={} senders={}",
            at,
            at + withhold_for_secs,
            withhold
        );
    }
    if timeline {
        // PHASE7-PREP-NOTES.md Finding A: once/sec progress-gauge line per live
        // primary, for the whole run -- reads the same registries `print_results`
        // reads below, just every second instead of once at the end. Diagnostic only;
        // does not touch the client/committer/execute path in any way.
        println!(
            " [timeline] T+s   node       entered   a_i   cursor   round   delivered   consume   wish   target   omega_q"
        );
        let mut elapsed: u64 = 0;
        // Committed-throughput series (grep-parseable `TIMELINE:` lines): matches
        // `print_results`' own `max_committed_transactions` formula exactly -- sum
        // `committed_transactions` within each node's own workers (handles
        // `--workers > 1`, where each worker registry only sees its own worker-id
        // slice of the replicated commit stream), then take the MAX across live
        // nodes (every node counts ~the same replicated stream once summed, not a
        // disjoint partition of it). `committed_transactions` is a plain `IntCounter`
        // incremented directly at commit time (`worker::synchronizer::Synchronizer::
        // observe_committed`) -- reading it needs no `MetricReporter::force_report`
        // (that only flushes the separate, buffered histogram gauges) and doesn't
        // perturb anything: a lock-free atomic read, once per second, same source the
        // final summary's own "Sequenced (committed)" line uses.
        let mut prev_committed_total: u64 = 0;
        'timeline: loop {
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                    elapsed += 1;
                    for (i, registry, _reporter) in &primary_metrics {
                        if let Some(p) = read_vantage_progress(registry) {
                            println!(
                                " [timeline] T+{:<4} node-{:<3} entered={:<7} a_i={:<5} cursor={:<7} round={:<6} delivered={:<9} consume={:<6} wish={:<6} target={:<6} omega_q={}",
                                elapsed, i, p.entered_view, p.frontier_a_i, p.cursor_next_view, p.control_round,
                                p.control_delivered_len, p.control_consume_pos,
                                p.own_watermark, p.entry_target, p.omega_q
                            );
                        }
                    }
                    let mut committed_by_node: std::collections::BTreeMap<usize, u64> =
                        std::collections::BTreeMap::new();
                    for (node, registry, _reporter) in &worker_metrics {
                        *committed_by_node.entry(*node).or_insert(0) +=
                            read_counter(registry, "committed_transactions");
                    }
                    let committed_total = committed_by_node.values().copied().max().unwrap_or(0);
                    println!(
                        "TIMELINE: sec={} committed_total={} committed_delta={}",
                        elapsed,
                        committed_total,
                        committed_total.saturating_sub(prev_committed_total)
                    );
                    prev_committed_total = committed_total;
                    if duration != 0 && elapsed >= duration {
                        break 'timeline;
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    println!("\nInterrupted -- computing results from data observed so far.");
                    break 'timeline;
                }
            }
        }
    } else if duration == 0 {
        // No sleep branch at all -- the only way out is Ctrl-C.
        tokio::signal::ctrl_c().await.ok();
        println!("\nInterrupted -- computing results from data observed so far.");
    } else {
        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(duration)) => {}
            _ = tokio::signal::ctrl_c() => {
                println!("\nInterrupted -- computing results from data observed so far.");
            }
        }
    }

    let actual_secs = run_start.elapsed().as_secs().max(1);
    // Unchanged from before this fix: read + print exactly as always (byte-identical
    // numbers for every existing config, including the default gate config) --
    // client tasks are still running and the commit pipeline may still be advancing
    // underneath this read, same as before.
    print_results(&worker_metrics, &primary_metrics, actual_secs, protocol, "").await;

    // Fable audit item 5 (measurement window): the RESULTS above can under- or
    // over-count the tail of the run, since client tasks are never stopped and the
    // registries are read while commits are still non-atomically advancing. Rather
    // than changing the numbers above (which would move already-recorded headline
    // numbers for every config, including the default gate config), stop every
    // client task now (so no NEW transaction is submitted), give already-in-flight
    // transactions a bounded chance to actually land in the registries, and re-report
    // the same duration/numbers from that settled state as a clearly separate,
    // additional block. This is the "at least stop the client/benchmark tasks before
    // reading final RESULTS" alternative this item calls out; `duration` (the
    // denominator) is intentionally NOT extended by the drain below, only the
    // registry read is delayed.
    //
    // NOT done here (would change existing numbers, so out of scope without a STOP):
    // trimming `run_start`'s own ramp-up (ports/synchronization warm-up before the
    // first real commit) out of the measured window -- that would shrink the
    // denominator and raise every reported TPS/BPS, including the default gate
    // config's.
    for handle in &client_handles {
        handle.abort();
    }
    let drain_ms = max_batch_delay_ms
        .saturating_add(max_header_delay_ms)
        .saturating_add(parameters.timeout_delay)
        .saturating_add(delta_ms)
        .max(1_000);
    tokio::time::sleep(tokio::time::Duration::from_millis(drain_ms)).await;
    print_results(
        &worker_metrics,
        &primary_metrics,
        actual_secs,
        protocol,
        &format!(
            " -- STEADY-STATE (client tasks stopped, {} ms drain before re-read)",
            drain_ms
        ),
    )
    .await;

    Ok(())
}

/// METRICS-DASHBOARD-SPEC.md §2: the wire-`type` -> traffic-category map, one per
/// protocol family (Vantage's wire variants are entirely disjoint from Autobahn's,
/// but both protocols share a few names -- e.g. `Header`, `Synchronize`,
/// `BatchRequest`, `Committed` -- with different category meanings per protocol, so
/// the map is protocol-scoped rather than global).
///
/// Known simplification (documented, not a metrics bug): `Header` is a single wire
/// variant carrying a bool (publish vs. serve/sync) that the `type` label does not
/// distinguish (the label is the wire variant name, per §1) -- so `Header` is folded
/// entirely into `dissemination` here, even though a (typically small) share of it is
/// actually repair/sync traffic (Vantage `Header(serve)`, Autobahn `Header(sync)`).
/// Every wire type not named below (e.g. `OurBatch`/`OthersBatch`, worker-to-primary
/// digest notifications) falls into `other` -- this keeps totals conserved (every
/// sent message lands in exactly one bucket) rather than silently under-counting.
fn categorize(protocol: Protocol, msg_type: &str) -> &'static str {
    match protocol {
        Protocol::Vantage => match msg_type {
            "Header" | "Batch" => "dissemination",
            "VantageAck" | "VantageAvail" => "acks",
            "VantagePropose" | "VantageEcho" | "VantageEchoSkip" | "VantageReady"
            | "VantageNoReady" | "VantageSkipVote"
            // signature-free.tex sec.8.3 "Digest-named AGB statements": the same
            // logical ECHO/READY statements, just a compact encoding -- same
            // category as their by-value counterparts above.
            | "VantageEchoDigest" | "VantageReadyDigest" => "agb",
            "VantageWish" => "pacemaker",
            // "repair": recovers already-identified missing data given only a
            // reference/digest -- `HeadersRequest`/`Synchronize`/`BatchRequest`'s own
            // role (see this function's "known simplification" doc comment: the
            // ANSWER side of that pair is miscategorized as "dissemination" purely
            // because it shares the `Header` wire variant with the publish path --
            // `VantageBodyFetch`/`VantageBodyServe` have no such sharing accident, so
            // both ends of this pair land in "repair" cleanly).
            // Mechanism A (sender-side lane resume, `vantage::resume`): same "repair"
            // category as the fetch/serve pairs above -- it recovers already-
            // identified missing data (a peer's own ack-census gap) given only a
            // height, same role as `HeadersRequest`/`VantageBodyFetch` for their own
            // gap classes. The served answer rides the ordinary `Header` wire
            // variant (already categorized "dissemination" above), same accepted
            // miscategorization noted for `HeadersRequest`'s own answer.
            "HeadersRequest" | "Synchronize" | "BatchRequest" | "VantageBodyFetch"
            | "VantageBodyServe" | "VantageLaneResume" => "repair",
            "CompReport"
            | "ControlInit"
            | "ControlEcho"
            | "ControlReady"
            | "ControlTimeoutVote"
            | "ControlTimeoutAccept"
            | "ControlCommit"
            | "ControlFetch"
            | "ControlServe" => "control",
            "Committed" => "metricsplumbing",
            _ => "other",
        },
        // Simple-IT reuses Vantage's own data plane verbatim (same `LaneManager`/
        // `Repairer`, hence the identical `Header`/`VantageAck`/`HeadersRequest`/
        // `Synchronize`/`BatchRequest` wire names and categories below) and adds its
        // own cut-consensus message family on top, grouped under `consensus` -- the
        // same category name Autobahn uses for its own consensus-layer messages
        // below, since this is the same kind of traffic (round-based, leader/vote/
        // certificate/timeout messages), just from a different protocol. Kept as a
        // separate arm rather than folded into `Protocol::Vantage`'s (even though the
        // data-plane lines are identical) so neither protocol's dashboard ever shows
        // the other's message vocabulary as a valid `msg_type`.
        Protocol::SimpleIt => match msg_type {
            "Header" | "Batch" => "dissemination",
            "VantageAck" | "VantageAvail" => "acks",
            // Mechanism A: same data plane as `Protocol::Vantage`'s own identical
            // addition above (`SimpleItCore` reuses `LaneManager`/`Wire` verbatim).
            "HeadersRequest" | "Synchronize" | "BatchRequest" | "VantageLaneResume" => "repair",
            "SimpleItCutProposal"
            | "SimpleItCutVote"
            | "SimpleItDecide"
            | "SimpleItTimeout"
            | "SimpleItTimeoutAccept" => "consensus",
            "Committed" => "metricsplumbing",
            _ => "other",
        },
        // Bracha-RBC variant (`--protocol simple-it-bracha`, arXiv:2606.14404 Table
        // 1/2 + Corollary 5, variant S): identical wire vocabulary to `SimpleIt`
        // above, plus its own extra echo-round message (`SimpleItCutReady`) -- kept
        // as its own arm for the same reason `SimpleIt` is kept separate from
        // `Protocol::Vantage` just above (so neither protocol's dashboard ever shows
        // a msg_type the OTHER one can produce but it can't).
        Protocol::SimpleItBracha => match msg_type {
            "Header" | "Batch" => "dissemination",
            "VantageAck" | "VantageAvail" => "acks",
            // Mechanism A: same data plane as `Protocol::SimpleIt`'s own identical
            // addition above (`SimpleItCore` serves both variants -- see
            // `SimpleItCore::build`'s own `Variant::{Opt,Bracha}` selection).
            "HeadersRequest" | "Synchronize" | "BatchRequest" | "VantageLaneResume" => "repair",
            "SimpleItCutProposal"
            | "SimpleItCutVote"
            | "SimpleItDecide"
            | "SimpleItTimeout"
            | "SimpleItTimeoutAccept"
            | "SimpleItCutReady" => "consensus",
            "Committed" => "metricsplumbing",
            _ => "other",
        },
        Protocol::AutobahnOptimistic | Protocol::AutobahnSeamless => match msg_type {
            "Header" | "Batch" => "dissemination",
            "Vote" | "Certificate" => "votes-certs",
            "ConsensusMessage" | "ConsensusRequest" | "ConsensusVote" | "Timeout" | "TC" => {
                "consensus"
            }
            "CertificatesRequest"
            | "HeadersRequest"
            | "ProposalHeadersRequest"
            | "Synchronize"
            | "BatchRequest" => "sync",
            "Committed" => "metricsplumbing",
            _ => "other",
        },
    }
}

/// Spawns one live node's primary in-process -- the exact same `Primary::spawn`
/// wiring `node run ... primary` uses standalone (PHASE2-SPEC.md #8: "reuses the
/// exact same spawn paths ... no parallel reimplementation"). The `tx_output`/
/// `rx_output` channel is a same-process no-op consumer, matching
/// `node/src/main.rs::analyze` (there is no separate application here). Returns
/// the primary's metrics registry/reporter and its own metrics-scrape target
/// (label, address) for `prometheus.yaml`.
fn spawn_node_primary(
    i: usize,
    keypair: KeyPair,
    node_dir: &std::path::Path,
    committee: &Committee,
    parameters: &Parameters,
    mode: TransactionMode,
) -> Result<NodeMetricsHandle> {
    let name = keypair.name;
    let signature_service = SignatureService::new(keypair.secret);

    let primary_store = Store::new_with_profile(
        node_dir.join("primary-db").to_str().unwrap(),
        StoreProfile::Metadata,
    )
    .context("Failed to create primary store")?;

    let (tx_output, mut rx_output) = channel(CHANNEL_CAPACITY);
    let (tx_new_certificates, _rx_new_certificates) = channel(CHANNEL_CAPACITY);
    let (_tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);
    let (tx_committer, rx_committer) = channel(CHANNEL_CAPACITY);
    let (_tx_pushdown_cert, rx_pushdown_cert) = channel(CHANNEL_CAPACITY);
    let (_tx_request_header_sync, rx_request_header_sync) = channel(CHANNEL_CAPACITY);
    let (tx_sailfish, _rx_sailfish) = channel(CHANNEL_CAPACITY);

    let (primary_metrics, primary_reporter, primary_registry) = Primary::spawn(
        name,
        committee.clone(),
        parameters.clone(),
        signature_service,
        primary_store,
        tx_new_certificates,
        tx_committer,
        rx_committer,
        rx_feedback,
        tx_sailfish,
        rx_pushdown_cert,
        rx_request_header_sync,
        tx_output,
    );
    // METRICS-DASHBOARD-SPEC.md §8: write-once (see `spawn_node_workers`'s identical
    // comment -- primary/worker both know `mode` here since `local-benchmark` builds
    // both).
    primary_metrics.set_transaction_mode_info(mode.label());
    // Application logic no-op, matching node/src/main.rs::analyze.
    tokio::spawn(async move { while rx_output.recv().await.is_some() {} });
    let target = (
        format!("node-{}-primary", i),
        committee.primary(&name).unwrap().metrics,
    );
    Ok((primary_registry, primary_reporter, target))
}

/// Spawns one live node's `workers` workers in-process -- the exact same
/// `Worker::spawn` wiring `node run ... worker` uses standalone -- plus, when
/// `rate_share` is `Some` (this node is in the `--load-nodes` loaded set), one
/// client task per worker, waiting for every live node's worker addresses before
/// sending (mirrors `benchmark_client --nodes`); when `None`, this node's workers
/// still spawn and still listen, they just get no client task. Returns each
/// worker's metrics registry/reporter alongside its own metrics-scrape target for
/// `prometheus.yaml` (in worker-id order), plus every spawned client task's own
/// `JoinHandle` (Fable audit item 5: so the caller can stop it before a final,
/// drained re-read -- see `run`; empty when `rate_share` is `None`).
// clippy::too_many_arguments: see primary/src/committer.rs's identical justification
// (this local helper mirrors Worker::spawn's own wiring one-for-one).
#[allow(clippy::too_many_arguments)]
fn spawn_node_workers(
    i: usize,
    name: PublicKey,
    node_dir: &std::path::Path,
    workers: usize,
    committee: &Committee,
    parameters: &Parameters,
    tx_size: usize,
    rate_share: Option<u64>,
    mode: TransactionMode,
    all_worker_addresses: &[SocketAddr],
) -> Result<(Vec<NodeMetricsHandle>, Vec<tokio::task::JoinHandle<()>>)> {
    let mut spawned = Vec::with_capacity(workers);
    let mut client_handles = Vec::with_capacity(workers);
    for j in 0..workers {
        let worker_id = j as WorkerId;
        let worker_store = Store::new_with_profile(
            node_dir.join(format!("worker-{}-db", j)).to_str().unwrap(),
            StoreProfile::Data,
        )
        .context("Failed to create worker store")?;

        let (metrics, reporter, registry) = Worker::spawn(
            name,
            worker_id,
            committee.clone(),
            parameters.clone(),
            worker_store,
        );
        // METRICS-DASHBOARD-SPEC.md §8: write-once -- only `local-benchmark` has the
        // client's tx-mode in scope at registry-construction time (see `Metrics::
        // set_transaction_mode_info`'s doc for why the standalone `node run` path
        // doesn't set this).
        metrics.set_transaction_mode_info(mode.label());
        let target = (
            format!("node-{}-worker-{}", i, j),
            committee.worker(&name, &worker_id).unwrap().metrics,
        );
        spawned.push((registry, reporter, target));

        // --load-nodes: unloaded live nodes (`rate_share == None`) already ran
        // `Worker::spawn` above and already listen on their transactions port --
        // still included in every client's `all_worker_addresses` wait-list -- they
        // just never get a client task of their own below, so their lane carries no
        // payload.
        if let Some(rate_share) = rate_share {
            let target_addr = committee.worker(&name, &worker_id).unwrap().transactions;
            let client = Client {
                target: target_addr,
                size: tx_size,
                rate: rate_share,
                nodes: all_worker_addresses.to_vec(),
                mode,
                // Every node in a local benchmark boots in ONE process, so there is no
                // multi-second deploy spread to ride out and no window to align to.
                // The AWS harness is what sets this (see `Client::activate_at_ms`).
                activate_at_ms: None,
            };
            client_handles.push(tokio::spawn(async move {
                client.wait().await;
                if let Err(e) = client.send().await {
                    log::warn!("Client for node {} worker {} exited: {}", i, j, e);
                }
            }));
        }
    }
    Ok((spawned, client_handles))
}

/// Computed in-process from each worker's own `Registry` -- no scraping, no log
/// parsing -- aggregated with the same rules `logs.py`'s audited `_real_transaction_
/// latency` uses (PHASE2-SPEC.md #5 amendments): max for count/misses (every node
/// counts the same replicated commit stream), summed sum/sum-of-squares for the exact
/// avg/stddev ratio, median across nodes for percentiles.
async fn print_results(
    worker_metrics: &[(usize, prometheus::Registry, Arc<MetricReporter>)],
    primary_metrics: &[(usize, prometheus::Registry, Arc<MetricReporter>)],
    duration: u64,
    protocol: Protocol,
    label: &str,
) {
    let mut snapshots: Vec<LatencySnapshot> = Vec::new();
    // Fable audit item 1: with `--workers > 1`, each worker's registry only ever
    // observes the slice of the committed stream tagged with ITS OWN worker id
    // (`Synchronizer::observe_committed` routes a `Committed` notification to the
    // local worker whose id matches the header author's payload entries) -- so
    // `committed_transactions`/`committed_bytes` on any ONE worker's registry is only
    // that worker-id's partition, not the node's full committed total. Summed within
    // each NODE first (below), so the two committed counters reflect that node's true
    // total BEFORE taking `.max()` ACROSS nodes -- every node counts ~the same
    // replicated stream once summed, the same invariant `aggregate_latency_snapshots`
    // already relies on for the latency histogram's own `count`/`misses`. With
    // `--workers 1` (the default) each node contributes exactly one worker, so this
    // sum is a no-op and the reported numbers are unchanged.
    let mut committed_by_node: std::collections::BTreeMap<usize, (u64, u64)> =
        std::collections::BTreeMap::new();
    // The materialised-latency series, read from the same registries in the same pass.
    // Kept in its own vector rather than folded into `LatencySnapshot` because the two
    // series can legitimately have DIFFERENT counts: a batch still being fetched has
    // contributed to neither yet, and a deferred-then-resolved batch contributes to both
    // but with different values. Aggregated with the identical cross-node rules.
    let mut materialised: Vec<LatencySnapshot> = Vec::new();
    for (node, registry, reporter) in worker_metrics {
        // Force a final drain so the gauges reflect every observation up to now, not
        // whatever the last periodic (every-10s) tick happened to see.
        reporter.force_report();
        if let Some(snapshot) = read_latency_snapshot(registry) {
            let entry = committed_by_node.entry(*node).or_insert((0, 0));
            entry.0 += snapshot.committed_transactions;
            entry.1 += snapshot.committed_bytes;
            snapshots.push(snapshot);
        }
        if let Some(snapshot) = read_materialised_latency_snapshot(registry) {
            materialised.push(snapshot);
        }
    }

    let max_committed_transactions = committed_by_node
        .values()
        .map(|(t, _)| *t)
        .max()
        .unwrap_or(0);
    let max_committed_bytes = committed_by_node
        .values()
        .map(|(_, b)| *b)
        .max()
        .unwrap_or(0);
    let consensus_tps = max_committed_transactions as f64 / duration.max(1) as f64;
    let consensus_bps = max_committed_bytes as f64 / duration.max(1) as f64;

    println!("\n-----------------------------------------");
    println!(" SUMMARY{}:", label);
    println!("-----------------------------------------");
    println!(" + RESULTS:");
    println!(" Consensus TPS: {:.0} tx/s", consensus_tps);
    println!(" Consensus BPS: {:.0} B/s", consensus_bps);
    println!();

    match aggregate_latency_snapshots(&snapshots) {
        Some(agg) => {
            let scrape_note = if agg.nodes_reporting < worker_metrics.len() {
                format!(
                    " [WARNING: only {}/{} worker(s) reporting]",
                    agg.nodes_reporting,
                    worker_metrics.len()
                )
            } else {
                String::new()
            };
            println!(
                " Real transaction latency: avg {:.2} ms (stddev {:.2}), p50/p90/p99 {:.2}/{:.2}/{:.2} ms ({} txs, {} misses){}",
                agg.avg_micros / 1_000.0,
                agg.stddev_micros / 1_000.0,
                agg.p50_micros as f64 / 1_000.0,
                agg.p90_micros as f64 / 1_000.0,
                agg.p99_micros as f64 / 1_000.0,
                agg.count,
                agg.misses,
                scrape_note,
            );
        }
        None => {
            println!(
                " Real transaction latency: no metrics observed (0/{} worker(s) reporting)",
                worker_metrics.len()
            );
        }
    }

    // The starfish-comparable series: submit -> ordered AND materialised. The line above
    // stops at the primary's ordering decision; this one stops when the batch's bytes are
    // actually in hand locally, so the gap between them IS the payload-availability cost.
    match aggregate_latency_snapshots(&materialised) {
        Some(agg) => {
            println!(
                " Materialised transaction latency: avg {:.2} ms (stddev {:.2}), p50/p90/p99 {:.2}/{:.2}/{:.2} ms ({} txs)",
                agg.avg_micros / 1_000.0,
                agg.stddev_micros / 1_000.0,
                agg.p50_micros as f64 / 1_000.0,
                agg.p90_micros as f64 / 1_000.0,
                agg.p99_micros as f64 / 1_000.0,
                agg.count,
            );
        }
        None => {
            println!(
                " Materialised transaction latency: no metrics observed (0/{} worker(s) reporting)",
                worker_metrics.len()
            );
        }
    }

    // METRICS-DASHBOARD-SPEC.md §2: goodput (submitted vs. sequenced) + wire-taxonomy
    // breakdown, computed here (not stored) from the §1 counters. Submitted/wire
    // counters are SUMMED across every node (each node's own independent traffic,
    // additive) -- unlike the replicated-commit-stream latency snapshot above, which
    // uses max/median across nodes because every node counts the same global stream.
    let submitted_transactions: u64 = worker_metrics
        .iter()
        .map(|(_, r, _)| read_counter(r, "submitted_transactions"))
        .sum();
    let submitted_bytes: u64 = worker_metrics
        .iter()
        .map(|(_, r, _)| read_counter(r, "submitted_transactions_bytes"))
        .sum();

    let mut sent_by_type: std::collections::BTreeMap<String, u64> =
        std::collections::BTreeMap::new();
    let mut sent_bytes_by_type: std::collections::BTreeMap<String, u64> =
        std::collections::BTreeMap::new();
    let mut total_bytes_sent: u64 = 0;
    let mut total_bytes_received: u64 = 0;
    let all_registries = worker_metrics
        .iter()
        .map(|(_, r, _)| r)
        .chain(primary_metrics.iter().map(|(_, r, _)| r));
    for registry in all_registries {
        total_bytes_sent += read_counter(registry, "bytes_sent_total");
        total_bytes_received += read_counter(registry, "bytes_received_total");
        for (t, c) in read_counter_vec(registry, "network_messages_sent_total", "type") {
            *sent_by_type.entry(t).or_insert(0) += c;
        }
        for (t, b) in read_counter_vec(registry, "network_bytes_sent_total", "type") {
            *sent_bytes_by_type.entry(t).or_insert(0) += b;
        }
    }
    let total_messages_sent: u64 = sent_by_type.values().sum();

    println!(" + GOODPUT / NETWORK:");
    println!(
        " Submitted: {} tx(s), {} B  |  Sequenced (committed): {} tx(s), {} B",
        submitted_transactions, submitted_bytes, max_committed_transactions, max_committed_bytes
    );
    println!(
        " Wire: {} B sent, {} B received ({} messages sent)",
        total_bytes_sent, total_bytes_received, total_messages_sent
    );
    if max_committed_bytes > 0 {
        println!(
            " Overhead bytes per sequenced byte: {:.3}",
            total_bytes_sent as f64 / max_committed_bytes as f64
        );
    }
    if max_committed_transactions > 0 {
        println!(
            " Messages per committed tx: {:.3}",
            total_messages_sent as f64 / max_committed_transactions as f64
        );
        // Starfish's own formula (metrics.rs:1077-1083): bytes sent per committed tx,
        // normalized to a 512 B transaction -- 1.0 means exactly 512 B of wire traffic
        // per committed tx at this run's tx size; higher means more relative overhead.
        println!(
            " Bandwidth efficiency (512B-normalized): {:.3}",
            total_bytes_sent as f64 / max_committed_transactions as f64 / 512.0
        );
    }
    if !sent_bytes_by_type.is_empty() {
        let mut by_category: std::collections::BTreeMap<&'static str, (u64, u64)> =
            std::collections::BTreeMap::new();
        for (t, bytes) in &sent_bytes_by_type {
            let count = sent_by_type.get(t).copied().unwrap_or(0);
            let entry = by_category.entry(categorize(protocol, t)).or_insert((0, 0));
            entry.0 += count;
            entry.1 += *bytes;
        }
        let total_category_bytes: u64 = by_category.values().map(|(_, b)| b).sum();
        println!(" Traffic by category (messages, bytes, % of sent bytes):");
        for (category, (count, bytes)) in &by_category {
            let pct = if total_category_bytes > 0 {
                100.0 * *bytes as f64 / total_category_bytes as f64
            } else {
                0.0
            };
            println!(
                "   {:<16} {:>10} msgs  {:>12} B  ({:.1}%)",
                category, count, bytes, pct
            );
        }
    }

    // Mechanism A (sender-side lane resume, `vantage::resume`) diagnostic: read
    // straight from each primary's own registry -- not derived from the wire-type
    // counters above (which only ever see what actually reached the wire).
    // `vantage_lane_resume_send_drops` in particular never appears on the wire at
    // all by construction (a dropped enqueue never becomes a send), so this is the
    // only way to observe it post-run.
    let resume_requests_sent: u64 = primary_metrics
        .iter()
        .map(|(_, r, _)| read_counter(r, "vantage_lane_resume_requests_sent"))
        .sum();
    let resume_blocks_served: u64 = primary_metrics
        .iter()
        .map(|(_, r, _)| read_counter(r, "vantage_lane_resume_blocks_served"))
        .sum();
    let resume_send_drops: u64 = primary_metrics
        .iter()
        .map(|(_, r, _)| read_counter(r, "vantage_lane_resume_send_drops"))
        .sum();
    println!(
        " Mechanism A (lane resume): {} requests sent, {} blocks served, {} sends dropped (channel full/closed)",
        resume_requests_sent, resume_blocks_served, resume_send_drops
    );
    println!();

    // PHASE6-SPEC.md §9 gate amendment: per-node seal-route breakdown (near-idle/absent
    // on the two Autobahn paths, which never observe into `vantage_seals` at all).
    let mut per_route_totals: std::collections::BTreeMap<String, u64> =
        std::collections::BTreeMap::new();
    let mut any_route_observed = false;
    for (i, registry, _reporter) in primary_metrics {
        // No `force_report()` needed here (unlike the latency histogram above):
        // `vantage_seals` is a plain counter, always current, with no periodic-report
        // buffering to flush.
        let routes = metrics::read_seal_route_counts(registry);
        if routes.is_empty() {
            continue;
        }
        any_route_observed = true;
        let breakdown: Vec<String> = routes
            .iter()
            .map(|(route, count)| format!("{}={}", route, count))
            .collect();
        println!(" Node {} seal routes: {}", i, breakdown.join(", "));
        for (route, count) in &routes {
            *per_route_totals.entry(route.clone()).or_insert(0) += count;
        }
    }
    if any_route_observed {
        let total: Vec<String> = per_route_totals
            .iter()
            .map(|(route, count)| format!("{}={}", route, count))
            .collect();
        println!(
            " Total seal routes (summed across nodes): {}",
            total.join(", ")
        );
    }
    println!("-----------------------------------------");
}

/// Generates `prometheus.yaml` targeting every primary/worker metrics endpoint, for
/// the optional `monitoring/docker-compose.yml` stack. Containers reach native node
/// endpoints via `host.docker.internal` (macOS Docker Desktop's host-loopback name).
fn write_prometheus_config(
    data_dir: &std::path::Path,
    targets: &[(String, SocketAddr)],
) -> Result<()> {
    let mut yaml = String::new();
    yaml.push_str("global:\n  scrape_interval: 1s\n");
    yaml.push_str("scrape_configs:\n");
    yaml.push_str("  - job_name: 'vantage-local-benchmark'\n");
    yaml.push_str("    static_configs:\n");
    for (label, addr) in targets {
        yaml.push_str(&format!(
            "      - targets: ['host.docker.internal:{}']\n        labels:\n          node: '{}'\n",
            addr.port(),
            label
        ));
    }
    fs::write(data_dir.join("prometheus.yaml"), yaml).context("Failed to write prometheus.yaml")
}

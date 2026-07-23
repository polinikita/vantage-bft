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
use crypto::SignatureService;
use metrics::{aggregate_latency_snapshots, read_latency_snapshot, read_vantage_progress, LatencySnapshot, MetricReporter};
use primary::Primary;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use store::{Store, StoreProfile};
use tokio::sync::mpsc::channel;
use worker::Worker;

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
    anyhow::ensure!(crash < nodes, "--crash ({}) must be strictly less than --nodes ({})", crash, nodes);
    let live_nodes = nodes - crash;
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
    // PHASE7-PREP-NOTES.md Finding A: diagnostic-only, off by default.
    let timeline: bool = matches.get_flag("timeline");
    // PHASE7-PREP-NOTES.md (WAN-shaped local runs): `--latency-table <csv>` (an n x n
    // RTT-ms matrix, node index = committee order) takes precedence; `--mimic-latency
    // -ms <u64>` is the uniform shorthand (defined as exactly the trivial table whose
    // every cell is that value -- see `LatencyTable::uniform`). Neither set (both
    // default/0) => `None`, i.e. zero injected delay, current behavior unchanged.
    let mimic_latency_ms: u64 = matches
        .get_one::<String>("mimic-latency-ms")
        .unwrap()
        .parse()
        .context("--mimic-latency-ms must be a non-negative integer")?;
    let latency_table_path = matches.get_one::<String>("latency-table").cloned();
    let latency_table: Option<LatencyTable> = if let Some(path) = &latency_table_path {
        let table = LatencyTable::from_rtt_csv(path, nodes)
            .with_context(|| format!("Failed to parse --latency-table '{}' as a {n}x{n} RTT-ms CSV matrix", path, n = nodes))?;
        println!("Latency table: loaded from {} ({}x{} RTT-ms matrix, node index = committee order)", path, nodes, nodes);
        Some(table)
    } else if mimic_latency_ms > 0 {
        println!(
            "Latency table: uniform {} ms RTT ({} ms one-way) on every inter-authority link (--mimic-latency-ms)",
            mimic_latency_ms,
            mimic_latency_ms / 2
        );
        Some(LatencyTable::uniform(nodes, mimic_latency_ms as f64))
    } else {
        None
    };

    let protocol = match protocol_str.as_str() {
        "autobahn-optimistic" => Protocol::AutobahnOptimistic,
        "autobahn-seamless" => Protocol::AutobahnSeamless,
        "vantage" => Protocol::Vantage,
        other => anyhow::bail!(
            "Unknown --protocol '{}'; use autobahn-optimistic, autobahn-seamless, or vantage",
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
    if crash > 0 {
        println!(
            "Crash fault: {} of {} nodes never spawned (committee unchanged; live = {})",
            crash, nodes, live_nodes
        );
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

    let mut parameters = Parameters::default();
    parameters.protocol = protocol;
    parameters.delta_ms = delta_ms;
    parameters.max_batch_delay = max_batch_delay_ms;
    parameters.max_header_delay = max_header_delay_ms;
    // PHASE7-PREP-NOTES.md (WAN-shaped local runs): `#[serde(skip)]` on this field
    // means it never round-trips through the `parameters.json` export just below --
    // set on the in-memory `Parameters` every node's `Primary::spawn` receives, which
    // is all `Core::spawn`/`vantage::node::VantageCore::spawn` ever read it from.
    parameters.latency_table = latency_table.map(Arc::new);
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
    // Offered rate scaled to the live clients only (R2): the same aggregate --rate is
    // now divided among fewer senders, so each live client's own per-client rate rises
    // to compensate -- the aggregate offered load is unchanged by which nodes crashed.
    let rate_share = rate.div_ceil((live_nodes * workers).max(1) as u64);

    // Spawn every primary and every worker natively, in this one process -- only for
    // the live nodes (R2).
    let mut worker_metrics: Vec<(prometheus::Registry, Arc<MetricReporter>)> = Vec::new();
    // PHASE6-SPEC.md §9 gate amendment: `vantage_seals` lives on each PRIMARY's own
    // registry (distinct from `worker_metrics` above) -- kept per node index so the
    // RESULTS block can print each node's own route breakdown (they can legitimately
    // differ across nodes) plus a summed total.
    let mut primary_metrics: Vec<(usize, prometheus::Registry, Arc<MetricReporter>)> = Vec::new();
    let mut metrics_targets: Vec<(String, SocketAddr)> = Vec::new(); // (label, addr) for prometheus.yaml

    for (i, keypair) in keypairs.into_iter().take(live_nodes).enumerate() {
        let name = keypair.name;
        let node_dir = data_dir.join(format!("node-{}", i));
        fs::create_dir_all(&node_dir)?;

        let signature_service = SignatureService::new(keypair.secret);

        let primary_store = Store::new_with_profile(
            node_dir.join("primary-db").to_str().unwrap(),
            StoreProfile::Metadata,
        )
        .context("Failed to create primary store")?;

        let (tx_output, mut rx_output) = channel(CHANNEL_CAPACITY);
        let (tx_new_certificates, _rx_new_certificates) = channel(CHANNEL_CAPACITY);
        let (tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);
        let (tx_committer, rx_committer) = channel(CHANNEL_CAPACITY);
        let (_tx_pushdown_cert, rx_pushdown_cert) = channel(CHANNEL_CAPACITY);
        let (_tx_request_header_sync, rx_request_header_sync) = channel(CHANNEL_CAPACITY);
        let (tx_sailfish, _rx_sailfish) = channel(CHANNEL_CAPACITY);

        let (_primary_metrics, primary_reporter, primary_registry) = Primary::spawn(
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
        primary_metrics.push((i, primary_registry, primary_reporter));
        // Application logic no-op, matching node/src/main.rs::analyze.
        tokio::spawn(async move { while rx_output.recv().await.is_some() {} });
        metrics_targets.push((
            format!("node-{}-primary", i),
            committee.primary(&name).unwrap().metrics,
        ));

        for j in 0..workers {
            let worker_id = j as WorkerId;
            let worker_store = Store::new_with_profile(
                node_dir
                    .join(format!("worker-{}-db", j))
                    .to_str()
                    .unwrap(),
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
            let _ = &metrics; // kept alive by `reporter`/`registry`; not read directly here
            worker_metrics.push((registry, reporter));
            metrics_targets.push((
                format!("node-{}-worker-{}", i, j),
                committee.worker(&name, &worker_id).unwrap().metrics,
            ));

            let target = committee.worker(&name, &worker_id).unwrap().transactions;
            let client = Client {
                target,
                size: tx_size,
                rate: rate_share,
                nodes: all_worker_addresses.clone(),
                mode,
            };
            tokio::spawn(async move {
                client.wait().await;
                if let Err(e) = client.send().await {
                    log::warn!("Client for node {} worker {} exited: {}", i, j, e);
                }
            });
        }
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

    println!("Running benchmark ({} sec)...", duration);
    if timeline {
        // PHASE7-PREP-NOTES.md Finding A: once/sec progress-gauge line per live
        // primary, for the whole run -- reads the same registries `print_results`
        // reads below, just every second instead of once at the end. Diagnostic only;
        // does not touch the client/committer/execute path in any way.
        println!(" [timeline] T+s   node       entered   a_i   cursor   round   delivered   consume");
        let mut elapsed: u64 = 0;
        'timeline: loop {
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                    elapsed += 1;
                    for (i, registry, _reporter) in &primary_metrics {
                        if let Some(p) = read_vantage_progress(registry) {
                            println!(
                                " [timeline] T+{:<4} node-{:<3} entered={:<7} a_i={:<5} cursor={:<7} round={:<6} delivered={:<9} consume={}",
                                elapsed, i, p.entered_view, p.frontier_a_i, p.cursor_next_view, p.control_round,
                                p.control_delivered_len, p.control_consume_pos
                            );
                        }
                    }
                    if elapsed >= duration {
                        break 'timeline;
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    println!("\nInterrupted -- computing results from data observed so far.");
                    break 'timeline;
                }
            }
        }
    } else {
        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(duration)) => {}
            _ = tokio::signal::ctrl_c() => {
                println!("\nInterrupted -- computing results from data observed so far.");
            }
        }
    }

    print_results(&worker_metrics, &primary_metrics, duration).await;
    Ok(())
}

/// Computed in-process from each worker's own `Registry` -- no scraping, no log
/// parsing -- aggregated with the same rules `logs.py`'s audited `_real_transaction_
/// latency` uses (PHASE2-SPEC.md #5 amendments): max for count/misses (every node
/// counts the same replicated commit stream), summed sum/sum-of-squares for the exact
/// avg/stddev ratio, median across nodes for percentiles.
async fn print_results(
    worker_metrics: &[(prometheus::Registry, Arc<MetricReporter>)],
    primary_metrics: &[(usize, prometheus::Registry, Arc<MetricReporter>)],
    duration: u64,
) {
    let mut snapshots: Vec<LatencySnapshot> = Vec::new();
    for (registry, reporter) in worker_metrics {
        // Force a final drain so the gauges reflect every observation up to now, not
        // whatever the last periodic (every-10s) tick happened to see.
        reporter.force_report();
        if let Some(snapshot) = read_latency_snapshot(registry) {
            snapshots.push(snapshot);
        }
    }

    let max_committed_transactions = snapshots.iter().map(|s| s.committed_transactions).max().unwrap_or(0);
    let max_committed_bytes = snapshots.iter().map(|s| s.committed_bytes).max().unwrap_or(0);
    let consensus_tps = max_committed_transactions as f64 / duration.max(1) as f64;
    let consensus_bps = max_committed_bytes as f64 / duration.max(1) as f64;

    println!("\n-----------------------------------------");
    println!(" SUMMARY:");
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

    // PHASE6-SPEC.md §9 gate amendment: per-node seal-route breakdown (near-idle/absent
    // on the two Autobahn paths, which never observe into `vantage_seals` at all).
    let mut per_route_totals: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
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
        let breakdown: Vec<String> = routes.iter().map(|(route, count)| format!("{}={}", route, count)).collect();
        println!(" Node {} seal routes: {}", i, breakdown.join(", "));
        for (route, count) in &routes {
            *per_route_totals.entry(route.clone()).or_insert(0) += count;
        }
    }
    if any_route_observed {
        let total: Vec<String> = per_route_totals.iter().map(|(route, count)| format!("{}={}", route, count)).collect();
        println!(" Total seal routes (summed across nodes): {}", total.join(", "));
    }
    println!("-----------------------------------------");
}

/// Generates `prometheus.yaml` targeting every primary/worker metrics endpoint, for
/// the optional `monitoring/docker-compose.yml` stack. Containers reach native node
/// endpoints via `host.docker.internal` (macOS Docker Desktop's host-loopback name).
fn write_prometheus_config(data_dir: &std::path::Path, targets: &[(String, SocketAddr)]) -> Result<()> {
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

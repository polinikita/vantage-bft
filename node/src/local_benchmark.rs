// Copyright(C) Facebook, Inc. and its affiliates.
// Run a complete local benchmark in one process.

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

/// Metrics registry, reporter, and Prometheus target for one node component.
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
    let ack_watermarks = !matches.get_flag("no-ack-watermarks");
    let echo_avail_claims = ack_watermarks && !matches.get_flag("no-echo-avail-claims");
    let timeline: bool = matches.get_flag("timeline") || withhold_at_secs.is_some();
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
    if echo_avail_claims {
        println!("Availability acknowledgments: positional claims on AGB echoes (default)");
    } else if ack_watermarks {
        println!(
            "Availability acknowledgments: periodic VantageAvail every {} ms",
            ack_watermark_period_ms
        );
    }
    if !matches.get_flag("no-digest-statements") {
        println!(
            "Digest-named AGB statements: ON (Vantage ECHO/READY name their proposal by hash instead of by value)"
        );
    }
    if protocol == Protocol::Vantage && !matches.get_flag("no-compact-ids") {
        println!("Vantage committee identifiers: one-byte indices (default)");
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

    let _ = fs::remove_dir_all(&data_dir);
    fs::create_dir_all(&data_dir).context("Failed to create --data-dir")?;

    let (committee, keypairs) = Committee::local_benchmark(nodes, workers, base_port);
    committee
        .export(data_dir.join("committee.json").to_str().unwrap())
        .context("Failed to write committee.json")?;

    let withhold_window_cell: Option<Arc<OnceLock<(std::time::Instant, std::time::Instant)>>> =
        withhold_at_secs.map(|_| Arc::new(OnceLock::new()));

    let mut parameters = Parameters {
        protocol,
        delta_ms,
        timeout_delay: match protocol {
            Protocol::SimpleIt => delta_ms.saturating_mul(8),
            Protocol::SimpleItBracha => delta_ms.saturating_mul(5),
            _ => Parameters::default().timeout_delay,
        },
        max_batch_delay: max_batch_delay_ms,
        max_header_delay: max_header_delay_ms,
        batch_messages: !matches.get_flag("no-batch-messages"),
        batch_max_bytes,
        batch_max_delay_ms,
        all_to_all: matches.get_flag("all-to-all"),
        ack_watermarks,
        echo_avail_claims,
        ack_watermark_period_ms,
        digest_statements: !matches.get_flag("no-digest-statements"),
        vantage_compact_ids: !matches.get_flag("no-compact-ids"),
        withhold_senders: withhold,
        withhold_at_ms: withhold_at_secs.map(|secs| secs * 1000),
        withhold_for_ms: withhold_for_secs * 1000,
        withhold_window: withhold_window_cell.clone(),
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

    let live_keypairs = &keypairs[..live_nodes];

    let all_worker_addresses: Vec<SocketAddr> = live_keypairs
        .iter()
        .flat_map(|keypair| {
            let authority = &committee.authorities[&keypair.name];
            authority.workers.values().map(|w| w.transactions)
        })
        .collect();
    let rate_share = rate.div_ceil((load_nodes * workers).max(1) as u64);

    let mut worker_metrics: Vec<(usize, prometheus::Registry, Arc<MetricReporter>)> = Vec::new();
    let mut primary_metrics: Vec<(usize, prometheus::Registry, Arc<MetricReporter>)> = Vec::new();
    let mut metrics_targets: Vec<(String, SocketAddr)> = Vec::new();
    let mut client_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    for (i, keypair) in keypairs.into_iter().take(live_nodes).enumerate() {
        let name = keypair.name;
        let node_dir = data_dir.join(format!("node-{}", i));
        fs::create_dir_all(&node_dir)?;

        let (primary_registry, primary_reporter, primary_target) =
            spawn_node_primary(i, keypair, &node_dir, &committee, &parameters, mode)?;
        primary_metrics.push((i, primary_registry, primary_reporter));
        metrics_targets.push(primary_target);

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

    write_prometheus_config(&data_dir, &metrics_targets)?;
    println!(
        "Prometheus target file: {}",
        data_dir.join("prometheus.yaml").display()
    );
    println!(
        "Bring up monitoring (optional): docker compose -f monitoring/docker-compose.yml up -d"
    );
    println!("Grafana (once up): http://localhost:3003\n");

    if duration == 0 {
        println!("Running benchmark (until Ctrl-C)...");
    } else {
        println!("Running benchmark ({} sec)...", duration);
    }
    let run_start = tokio::time::Instant::now();
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
        // Print one diagnostic line per live primary each second.
        println!(
            " [timeline] T+s   node       entered   a_i   cursor   round   delivered   consume   wish   target   omega_q"
        );
        let mut elapsed: u64 = 0;
        // Sum each node's worker counters, then use the highest replicated total.
        let mut prev_committed_total: u64 = 0;
        'timeline: loop {
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                    elapsed += 1;
                    for (i, registry, _reporter) in &primary_metrics {
                        if let Some(p) = read_vantage_progress(registry) {
                            println!(
                                " [timeline] T+{:<4} node-{:<3} entered={:<7} a_i={:<5} cursor={:<7} round={:<6} delivered={:<9} consume={:<6} wish={:<6} target={:<6} omega_q={:<6} cache={}",
                                elapsed, i, p.entered_view, p.frontier_a_i, p.cursor_next_view, p.control_round,
                                p.control_delivered_len, p.control_consume_pos,
                                p.own_watermark, p.entry_target, p.omega_q, p.block_cache_len
                            );
                            // Keep walk counters on a separate parseable line.
                            let w = read_counter_vec(registry, "vantage_walk_steps_total", "family");
                            println!(
                                "WALK: sec={} node={} chain={} direct={} settle={} blocks={}",
                                elapsed, i,
                                w.get("chain").copied().unwrap_or(0),
                                w.get("direct").copied().unwrap_or(0),
                                w.get("settle").copied().unwrap_or(0),
                                read_counter(registry, "vantage_blocks_received"),
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
    // Read results while clients and commits may still be progressing.
    print_results(&worker_metrics, &primary_metrics, actual_secs, protocol, "").await;

    // Stop clients, allow in-flight work to settle, and print a final snapshot.
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

/// Map each protocol's wire types to dashboard traffic categories.
fn categorize(protocol: Protocol, msg_type: &str) -> &'static str {
    match protocol {
        Protocol::Vantage => match msg_type {
            "Header" | "Batch" => "dissemination",
            "VantageAck" | "VantageAvail" => "acks",
            "VantagePropose" | "VantageEcho" | "VantageEchoSkip" | "VantageReady"
            | "VantageNoReady" | "VantageSkipVote"
            // Digest-named AGB statements use the same category as their by-value forms.
            | "VantageEchoDigest" | "VantageReadyDigest" => "agb",
            "VantageWish" => "pacemaker",
            // Repair messages recover data identified by a reference or height.
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
        // Simple-IT shares the Vantage data plane and adds cut-consensus messages.
        Protocol::SimpleIt => match msg_type {
            "Header" | "Batch" => "dissemination",
            "VantageAck" | "VantageAvail" => "acks",
            // Lane resume uses the shared data plane.
            "HeadersRequest" | "Synchronize" | "BatchRequest" | "VantageLaneResume" => "repair",
            "SimpleItCutProposal"
            | "SimpleItCutVote"
            | "SimpleItDecide"
            | "SimpleItTimeout"
            | "SimpleItTimeoutAccept" => "consensus",
            "Committed" => "metricsplumbing",
            _ => "other",
        },
        // Bracha-RBC adds the `SimpleItCutReady` message.
        Protocol::SimpleItBracha => match msg_type {
            "Header" | "Batch" => "dissemination",
            "VantageAck" | "VantageAvail" => "acks",
            // Lane resume uses the shared data plane for both variants.
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

/// Spawn one live primary in-process and return its metrics target.
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
    // Record the transaction mode once.
    primary_metrics.set_transaction_mode_info(mode.label());
    // Application logic no-op, matching node/src/main.rs::analyze.
    tokio::spawn(async move { while rx_output.recv().await.is_some() {} });
    let target = (
        format!("node-{}-primary", i),
        committee.primary(&name).unwrap().metrics,
    );
    Ok((primary_registry, primary_reporter, target))
}

/// Spawn one node's workers and optional client tasks.
// The explicit arguments mirror `Worker::spawn`.
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
        // Record the transaction mode once.
        metrics.set_transaction_mode_info(mode.label());
        let target = (
            format!("node-{}-worker-{}", i, j),
            committee.worker(&name, &worker_id).unwrap().metrics,
        );
        spawned.push((registry, reporter, target));

        // Unloaded nodes still listen but do not run a client task.
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
/// parsing. Aggregate counts consistently across worker registries: max for count/misses,
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
    // With `--workers > 1`, each worker's registry only ever
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

    // Report latency after ordered payloads are available locally.
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

    // Sum submitted traffic and network usage across nodes.
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
        // Normalize bytes sent per committed transaction to 512 bytes.
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

    // Sum lane-resume counters across primaries.
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
        " Lane resume: {} requests sent, {} blocks served, {} sends dropped",
        resume_requests_sent, resume_blocks_served, resume_send_drops
    );
    println!();

    // Report Vantage seal routes by node.
    let mut per_route_totals: std::collections::BTreeMap<String, u64> =
        std::collections::BTreeMap::new();
    let mut any_route_observed = false;
    for (i, registry, _reporter) in primary_metrics {
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

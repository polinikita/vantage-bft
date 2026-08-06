// Copyright(C) Facebook, Inc. and its affiliates.
// Thin CLI wrapper around `client::Client` (PHASE2-SPEC.md §8 -- extracted so
// `local-benchmark` can reuse the exact same transaction-generation logic in-process,
// instead of a parallel reimplementation).
use anyhow::{Context, Result};
use clap::{crate_name, crate_version, Arg, ArgAction, Command};
use env_logger::Env;
use log::info;
use std::net::SocketAddr;

#[path = "client.rs"]
mod client;
use client::{Client, TransactionMode};

#[tokio::main]
async fn main() -> Result<()> {
    let matches = Command::new(crate_name!())
        .version(crate_version!())
        .about("Benchmark client for Sailfish.")
        .arg(
            Arg::new("ADDR")
                .required(true)
                .value_name("ADDR")
                .action(ArgAction::Set)
                .help("The network address of the node where to send txs"),
        )
        .arg(
            Arg::new("size")
                .long("size")
                .value_name("INT")
                .required(true)
                .action(ArgAction::Set)
                .help("The size of each transaction in bytes"),
        )
        .arg(
            Arg::new("rate")
                .long("rate")
                .value_name("INT")
                .required(true)
                .action(ArgAction::Set)
                .help("The rate (txs/s) at which to send the transactions"),
        )
        .arg(
            Arg::new("nodes")
                .long("nodes")
                .value_name("ADDR")
                .num_args(1..)
                .required(false)
                .action(ArgAction::Set)
                .help("Network addresses that must be reachable before starting the benchmark."),
        )
        .arg(
            Arg::new("mode")
                .long("mode")
                .value_name("MODE")
                .required(false)
                .default_value("random")
                // "all-zero" (hyphen) kept as a legacy alias; "all_zero" (snake_case) is
                // the starfish-aligned canonical spelling normalized in
                // `TransactionMode::parse`.
                .value_parser(["all_zero", "all-zero", "random"])
                .action(ArgAction::Set)
                .help(
                    "Transaction payload mode: 'all_zero' or 'random' (default, as of \
                    METRICS-DASHBOARD-SPEC.md §8; legacy 'all-zero' spelling still accepted)",
                ),
        )
        .arg(
            Arg::new("activate-at-ms")
                .long("activate-at-ms")
                .value_name("EPOCH_MS")
                .required(false)
                .action(ArgAction::Set)
                .help(
                    "Absolute epoch-millisecond instant before which no transaction is \
                     submitted. Pass the SAME value the nodes were given as \
                     parameters.json's `metrics_active_at_ms`, so the first transaction \
                     submitted is also the first one counted. Omit to submit immediately \
                     (which includes the committee-formation transient in the run's \
                     latency distribution)",
                ),
        )
        .arg_required_else_help(true)
        .get_matches();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let target = matches
        .get_one::<String>("ADDR")
        .unwrap()
        .parse::<SocketAddr>()
        .context("Invalid socket address format")?;
    let size = matches
        .get_one::<String>("size")
        .unwrap()
        .parse::<usize>()
        .context("The size of transactions must be a non-negative integer")?;
    let rate = matches
        .get_one::<String>("rate")
        .unwrap()
        .parse::<u64>()
        .context("The rate of transactions must be a non-negative integer")?;
    let nodes = matches
        .get_many::<String>("nodes")
        .unwrap_or_default()
        .map(|x| x.parse::<SocketAddr>())
        .collect::<Result<Vec<_>, _>>()
        .context("Invalid socket address format")?;
    let mode = TransactionMode::parse(matches.get_one::<String>("mode").unwrap())
        .context("Invalid transaction mode")?;

    info!("Node address: {}", target);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions size: {} B", size);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions rate: {} tx/s", rate);

    info!("Transaction mode: {:?}", mode);

    let activate_at_ms = matches
        .get_one::<String>("activate-at-ms")
        .map(|x| x.parse::<u64>())
        .transpose()
        .context("--activate-at-ms must be a non-negative integer (epoch milliseconds)")?;

    if let Some(at) = activate_at_ms {
        info!("Metrics-active window opens at: {} (epoch ms)", at);
    }

    let client = Client {
        target,
        size,
        rate,
        nodes,
        mode,
        activate_at_ms,
    };

    // Wait for all nodes to be online and synchronized.
    client.wait().await;

    // Start the benchmark.
    client.send().await.context("Failed to submit transactions")
}

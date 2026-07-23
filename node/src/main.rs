#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use clap::{crate_name, crate_version, Arg, ArgAction, ArgMatches, Command};
use config::Export as _;
use config::Import as _;
use config::{Committee, KeyPair, Parameters, Protocol, WorkerId};
use crypto::SignatureService;
use env_logger::Env;
use primary::Header;
use primary::Primary;
use store::{Store, StoreProfile};
use tokio::sync::mpsc::{channel, Receiver};
use worker::Worker;

#[cfg(feature = "benchmark")]
mod client;
#[cfg(feature = "benchmark")]
mod local_benchmark;

/// The default channel capacity.
pub const CHANNEL_CAPACITY: usize = 1_000;

#[tokio::main]
async fn main() -> Result<()> {
    //std::env::set_var("RUST_BACKTRACE", "1");
    
    let matches = Command::new(crate_name!())
        .version(crate_version!())
        .about("A research implementation of Sailfish.")
        .arg(
            Arg::new("v")
                .short('v')
                .action(ArgAction::Count)
                .help("Sets the level of verbosity"),
        )
        .subcommand(
            Command::new("generate_keys")
                .about("Print a fresh key pair to file")
                .arg(
                    Arg::new("filename")
                        .long("filename")
                        .value_name("FILE")
                        .required(true)
                        .action(ArgAction::Set)
                        .help("The file where to print the new key pair"),
                ),
        )
        .subcommand(
            Command::new("run")
                .about("Run a node")
                .arg(
                    Arg::new("keys")
                        .long("keys")
                        .value_name("FILE")
                        .required(true)
                        .action(ArgAction::Set)
                        .help("The file containing the node keys"),
                )
                .arg(
                    Arg::new("committee")
                        .long("committee")
                        .value_name("FILE")
                        .required(true)
                        .action(ArgAction::Set)
                        .help("The file containing committee information"),
                )
                .arg(
                    Arg::new("parameters")
                        .long("parameters")
                        .value_name("FILE")
                        .required(false)
                        .action(ArgAction::Set)
                        .help("The file containing the node parameters"),
                )
                .arg(
                    Arg::new("store")
                        .long("store")
                        .value_name("PATH")
                        .required(true)
                        .action(ArgAction::Set)
                        .help("The path where to create the data store"),
                )
                .subcommand(Command::new("primary").about("Run a single primary"))
                .subcommand(
                    Command::new("worker")
                        .about("Run a single worker")
                        .arg(
                            Arg::new("id")
                                .long("id")
                                .value_name("INT")
                                .required(true)
                                .action(ArgAction::Set)
                                .help("The worker id"),
                        ),
                )
                .subcommand_required(true)
                .arg_required_else_help(true),
        )
        .subcommand(
            Command::new("local-benchmark")
                .about("Self-host a whole local benchmark run (every primary, every \
                    worker, and one client per worker) in this one process \
                    (PHASE2-SPEC.md #8) -- the local replacement for `fab local`")
                .arg(Arg::new("nodes").long("nodes").value_name("INT").default_value("4")
                    .action(ArgAction::Set).help("Number of authorities"))
                .arg(Arg::new("workers").long("workers").value_name("INT").default_value("1")
                    .action(ArgAction::Set).help("Workers per authority"))
                .arg(Arg::new("rate").long("rate").value_name("INT").default_value("240000")
                    .action(ArgAction::Set).help("Aggregate input rate (tx/s)"))
                .arg(Arg::new("tx-size").long("tx-size").value_name("INT").default_value("512")
                    .action(ArgAction::Set).help("Transaction size in bytes"))
                .arg(Arg::new("protocol").long("protocol").value_name("PROTOCOL")
                    .default_value("autobahn-optimistic")
                    .value_parser(["autobahn-optimistic", "autobahn-seamless", "vantage"])
                    .action(ArgAction::Set).help("Consensus protocol"))
                .arg(Arg::new("mode").long("mode").value_name("MODE").default_value("all-zero")
                    .value_parser(["all-zero", "random"])
                    .action(ArgAction::Set).help("Transaction payload mode"))
                .arg(Arg::new("duration").long("duration").value_name("INT").default_value("60")
                    .action(ArgAction::Set).help("Benchmark duration in seconds"))
                .arg(Arg::new("base-port").long("base-port").value_name("INT").default_value("4000")
                    .action(ArgAction::Set).help("First port allocated (127.0.0.1)"))
                .arg(Arg::new("data-dir").long("data-dir").value_name("PATH").default_value(".local-bench")
                    .action(ArgAction::Set).help("Directory for per-node stores/reference config"))
                .arg(Arg::new("crash").long("crash").value_name("INT").default_value("0")
                    .action(ArgAction::Set).help("PHASE6-SPEC.md R2: number of trailing nodes \
                        to leave unspawned (true crash fault -- committee membership is \
                        unchanged, only the last k nodes' primary/worker/client tasks are \
                        never started); offered rate is scaled to the live clients only"))
                .arg(Arg::new("delta-ms").long("delta-ms").value_name("INT").default_value("1000")
                    .action(ArgAction::Set).help("Vantage AGB base delay unit Δ, ms \
                        (theta_E=5Δ, theta_R=6Δ, control-round=6Δ derive from this \
                        automatically; irrelevant to the two Autobahn paths)"))
                .arg(Arg::new("max-batch-delay-ms").long("max-batch-delay-ms").value_name("INT").default_value("20")
                    .action(ArgAction::Set).help("Worker max batch seal delay, ms"))
                .arg(Arg::new("max-header-delay-ms").long("max-header-delay-ms").value_name("INT").default_value("50")
                    .action(ArgAction::Set).help("Primary max header/car creation delay, ms"))
                .arg(Arg::new("timeline").long("timeline").action(ArgAction::SetTrue)
                    .help("PHASE7-PREP-NOTES.md Finding A: print a once/sec progress-gauge \
                        line per live node (entered view / frontier a_i / cursor next_view / \
                        control round / delivered-log len / consume pos) for the duration of \
                        the run -- diagnostic only, off by default (verbose)"))
                .arg(Arg::new("mimic-latency-ms").long("mimic-latency-ms").value_name("INT")
                    .default_value("0").action(ArgAction::Set)
                    .help("PHASE7-PREP-NOTES.md (optional): starfish-style fixed artificial \
                        delay (ms) injected on every inter-node send, both primary-to-primary \
                        and primary-to-worker/worker-to-worker (network crate, process-wide); \
                        0 = off (default, current behavior)")),
        )
        .subcommand_required(true)
        .arg_required_else_help(true)
        .get_matches();

    let log_level = match matches.get_count("v") {
        0 => "error",
        1 => "warn",
        2 => "info",
        3 => "debug",
        _ => "trace",
    };
    let mut logger = env_logger::Builder::from_env(Env::default().default_filter_or(log_level));
    #[cfg(feature = "benchmark")]
    logger.format_timestamp_millis();
    logger.init();

    match matches.subcommand() {
        Some(("generate_keys", sub_matches)) => KeyPair::new()
            .export(sub_matches.get_one::<String>("filename").unwrap())
            .context("Failed to generate key pair")?,
        Some(("run", sub_matches)) => run(sub_matches).await?,
        Some(("local-benchmark", sub_matches)) => {
            #[cfg(feature = "benchmark")]
            {
                local_benchmark::run(sub_matches).await?;
            }
            #[cfg(not(feature = "benchmark"))]
            {
                let _ = sub_matches;
                anyhow::bail!(
                    "local-benchmark requires building with --features benchmark"
                );
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}

// Runs either a worker or a primary.
async fn run(matches: &ArgMatches) -> Result<()> {
    let key_file = matches.get_one::<String>("keys").unwrap();
    let committee_file = matches.get_one::<String>("committee").unwrap();
    let parameters_file = matches.get_one::<String>("parameters").map(|s| s.as_str());
    let store_path = matches.get_one::<String>("store").unwrap();

    // Read the committee and node's keypair from file.
    let keypair = KeyPair::import(key_file).context("Failed to load the node's keypair")?;
    let name = keypair.name;
    let committee =
        Committee::import(committee_file).context("Failed to load the committee information")?;

    // Load default parameters if none are specified.
    let mut parameters = match parameters_file {
        Some(filename) => {
            Parameters::import(filename).context("Failed to load the node's parameters")?
        }
        None => Parameters::default(),
    };

    // `protocol` is authoritative over the legacy `use_optimistic_tips` knob.
    parameters.reconcile_protocol();

    // Select the node assembly by protocol. Both Autobahn variants share the
    // existing primary/worker assembly (the seamless path is activated inside
    // the primary Core via `use_optimistic_tips = false`); Vantage spawns a single
    // `VantageCore` task instead (PHASE4-SPEC.md §1 -- D3 lifted).

    // The `SignatureService` provides signatures on input digests.
    let signature_service = SignatureService::new(keypair.secret);

    // Make the data store, tuned per Phase-2's RocksDB profile (PHASE2-SPEC.md #7):
    // the primary's store holds small, point-lookup metadata (headers/certs/payload
    // markers), workers' stores hold large, append-heavy batch bytes.
    let store_profile = match matches.subcommand_name() {
        Some("worker") => StoreProfile::Data,
        _ => StoreProfile::Metadata,
    };
    let store = Store::new_with_profile(store_path, store_profile)
        .context("Failed to create a store")?;

    // Channels the sequence of certificates.
    let (tx_output, rx_output) = channel(CHANNEL_CAPACITY);

    // Channel for sending headers between DAG and Consensus
    let (tx_sailfish, rx_sailfish) = channel(CHANNEL_CAPACITY);

    // Channel for sending loopback headerds that completed validation between DAG and Consensus
    //let (tx_validation, rx_validation) = channel(CHANNEL_CAPACITY);

    // Channel for indicating commit and that new header should be proposed
    //let (tx_ticket, rx_ticket) = channel(CHANNEL_CAPACITY);

    // Check whether to run a primary, a worker, or an entire authority.
    //Note: Each node has at most one worker. Workers that don't include a primary (e.g. are not an entire authority) use PrimaryConnector to connect to a designated primary.
    match matches.subcommand() {
        // Spawn the primary and consensus core.
        Some(("primary", _)) => {
            let (tx_new_certificates, rx_new_certificates) = channel(CHANNEL_CAPACITY);
            let (tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);
            let (tx_committer, rx_committer) = channel(CHANNEL_CAPACITY);
            let (tx_pushdown_cert, rx_pushdown_cert) = channel(CHANNEL_CAPACITY);
            let(tx_request_header_sync, rx_request_header_sync) = channel(CHANNEL_CAPACITY);

            Primary::spawn(
                name,
                committee.clone(),
                parameters.clone(),
                signature_service.clone(),
                store.clone(),
                /* tx_consensus */ tx_new_certificates,
                tx_committer,
                rx_committer,
                /* rx_consensus */ rx_feedback,
                tx_sailfish,
                //rx_ticket,
                rx_pushdown_cert,
                rx_request_header_sync,
                tx_output,
            );
            /*Consensus::spawn(
                name,
                committee,
                parameters,
                signature_service,
                store,
                /* rx_consensus */ rx_new_certificates,
                rx_committer,
                /* tx_mempool */ tx_feedback,
                tx_output,
                tx_ticket,
                tx_validation,
                rx_sailfish,
                tx_pushdown_cert,
                tx_request_header_sync,
            );*/
        }

        // Spawn a single worker.
        Some(("worker", sub_matches)) => {
            let id = sub_matches
                .get_one::<String>("id")
                .unwrap()
                .parse::<WorkerId>()
                .context("The worker id must be a positive integer")?;
            Worker::spawn(keypair.name, id, committee, parameters, store);
        }
        _ => unreachable!(),
    }

    // Analyze the consensus' output.
    analyze(rx_output).await;

    // If this expression is reached, the program ends and all other tasks terminate.
    unreachable!();
}

/// Receives an ordered list of certificates and apply any application-specific logic.
async fn analyze(mut rx_output: Receiver<Header>) {
    while let Some(_header) = rx_output.recv().await {
        // NOTE: Here goes the application logic.
    }
}

// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use clap::{crate_name, crate_version, Arg, ArgAction, ArgMatches, Command};
use config::Export as _;
use config::Import as _;
use config::{Committee, KeyPair, LatencyTable, Parameters, WorkerId};
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

/// Default channel capacity.
pub const CHANNEL_CAPACITY: usize = 1_000;

#[tokio::main]
async fn main() -> Result<()> {
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
                    Command::new("worker").about("Run a single worker").arg(
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
                .about("Run a complete local benchmark in one process")
                .arg(
                    Arg::new("nodes")
                        .long("nodes")
                        .value_name("INT")
                        .default_value("4")
                        .action(ArgAction::Set)
                        .help("Number of authorities"),
                )
                .arg(
                    Arg::new("workers")
                        .long("workers")
                        .value_name("INT")
                        .default_value("1")
                        .action(ArgAction::Set)
                        .help("Workers per authority"),
                )
                .arg(
                    Arg::new("rate")
                        .long("rate")
                        .value_name("INT")
                        .default_value("240000")
                        .action(ArgAction::Set)
                        .help("Aggregate input rate (tx/s)"),
                )
                .arg(
                    Arg::new("tx-size")
                        .long("tx-size")
                        .value_name("INT")
                        .default_value("512")
                        .action(ArgAction::Set)
                        .help("Transaction size in bytes"),
                )
                .arg(
                    Arg::new("protocol")
                        .long("protocol")
                        .value_name("PROTOCOL")
                        .default_value("autobahn-optimistic")
                        .value_parser([
                            "autobahn-optimistic",
                            "autobahn-seamless",
                            "vantage",
                            "simple-it",
                            "simple-it-bracha",
                        ])
                        .action(ArgAction::Set)
                        .help("Consensus protocol"),
                )
                .arg(
                    Arg::new("mode")
                        .long("mode")
                        .value_name("MODE")
                        .default_value("random")
                        .value_parser(["all_zero", "all-zero", "random"])
                        .action(ArgAction::Set)
                        .help(
                            "Transaction payload mode (default: random; all-zero is also accepted)",
                        ),
                )
                .arg(
                    Arg::new("duration")
                        .long("duration")
                        .value_name("INT")
                        .default_value("60")
                        .action(ArgAction::Set)
                        .help("Measured benchmark duration in seconds (0 = run until Ctrl-C)"),
                )
                .arg(
                    Arg::new("warmup")
                        .long("warmup")
                        .value_name("SEC")
                        .default_value("0")
                        .action(ArgAction::Set)
                        .help("Warmup under load before counters and latency measurements begin"),
                )
                .arg(
                    Arg::new("base-port")
                        .long("base-port")
                        .value_name("INT")
                        .default_value("4000")
                        .action(ArgAction::Set)
                        .help("First port allocated (127.0.0.1)"),
                )
                .arg(
                    Arg::new("data-dir")
                        .long("data-dir")
                        .value_name("PATH")
                        .default_value(".local-bench")
                        .action(ArgAction::Set)
                        .help("Directory for per-node stores and configuration"),
                )
                .arg(
                    Arg::new("crash")
                        .long("crash")
                        .value_name("INT")
                        .default_value("0")
                        .action(ArgAction::Set)
                        .help("Number of trailing nodes to leave unspawned"),
                )
                .arg(
                    Arg::new("load-nodes")
                        .long("load-nodes")
                        .value_name("INT")
                        .action(ArgAction::Set)
                        .help("Number of live nodes that submit the aggregate transaction load"),
                )
                .arg(
                    Arg::new("withhold")
                        .long("withhold")
                        .value_name("INT")
                        .default_value("0")
                        .action(ArgAction::Set)
                        .help("Number of leading nodes that withhold payload broadcasts from half the committee"),
                )
                .arg(
                    Arg::new("withhold-count")
                        .long("withhold-count")
                        .value_name("INT")
                        .action(ArgAction::Set)
                        .help("Peers each withholding node excludes (default: half the committee)"),
                )
                .arg(
                    Arg::new("withhold-stride")
                        .long("withhold-stride")
                        .value_name("INT")
                        .default_value("1")
                        .action(ArgAction::Set)
                        .help("Coprime committee-index stride used to spread omitted payloads"),
                )
                .arg(
                    Arg::new("withhold-fixed-receivers")
                        .long("withhold-fixed-receivers")
                        .action(ArgAction::SetTrue)
                        .help(
                            "Make every withholding sender exclude the same following receiver group",
                        ),
                )
                .arg(
                    Arg::new("withhold-batches-only")
                        .long("withhold-batches-only")
                        .action(ArgAction::SetTrue)
                        .help("Drop heavy worker batches while continuing original lane headers"),
                )
                .arg(
                    Arg::new("withhold-repair")
                        .long("withhold-repair")
                        .action(ArgAction::SetTrue)
                        .help("Make selected Byzantine publishers ignore all lane repair requests"),
                )
                .arg(
                    Arg::new("late-header-publishers")
                        .long("late-header-publishers")
                        .value_name("INT")
                        .default_value("0")
                        .action(ArgAction::Set)
                        .help("Leading Byzantine nodes that delay original header publication"),
                )
                .arg(
                    Arg::new("late-header-receivers")
                        .long("late-header-receivers")
                        .value_name("INT")
                        .default_value("0")
                        .action(ArgAction::Set)
                        .help("Following nodes that receive selected original headers late"),
                )
                .arg(
                    Arg::new("late-header-delay-ms")
                        .long("late-header-delay-ms")
                        .value_name("INT")
                        .default_value("1000")
                        .action(ArgAction::Set)
                        .help("Additional one-way delay for selected original headers"),
                )
                .arg(
                    Arg::new("withhold-at")
                        .long("withhold-at")
                        .value_name("SEC")
                        .action(ArgAction::Set)
                        .help("Seconds after measurement starts before withholding begins"),
                )
                .arg(
                    Arg::new("withhold-for")
                        .long("withhold-for")
                        .value_name("SEC")
                        .default_value("30")
                        .action(ArgAction::Set)
                        .help("Withholding duration in seconds"),
                )
                .arg(
                    Arg::new("delta-ms")
                        .long("delta-ms")
                        .value_name("INT")
                        .default_value("200")
                        .action(ArgAction::Set)
                        .help("Vantage AGB base delay in milliseconds"),
                )
                .arg(
                    Arg::new("timeout-delay-ms")
                        .long("timeout-delay-ms")
                        .value_name("INT")
                        .action(ArgAction::Set)
                        .help(
                            "Override the proof-calibrated round timeout in milliseconds \
                             (defaults: Autobahn 10*Delta, Simple-IT Opt 8*Delta, Bracha 5*Delta)",
                        ),
                )
                .arg(
                    Arg::new("fast-path-timeout-ms")
                        .long("fast-path-timeout-ms")
                        .value_name("INT")
                        .default_value("500")
                        .action(ArgAction::Set)
                        .help("Autobahn fast-path wait in milliseconds"),
                )
                .arg(
                    Arg::new("max-batch-delay-ms")
                        .long("max-batch-delay-ms")
                        .value_name("INT")
                        .default_value("20")
                        .action(ArgAction::Set)
                        .help("Worker max batch seal delay, ms"),
                )
                .arg(
                    Arg::new("max-header-delay-ms")
                        .long("max-header-delay-ms")
                        .value_name("INT")
                        .default_value("100")
                        .action(ArgAction::Set)
                        .help("Primary max header/car creation delay, ms"),
                )
                .arg(
                    Arg::new("timeline")
                        .long("timeline")
                        .action(ArgAction::SetTrue)
                        .help("Print per-node progress once per second"),
                )
                .arg(
                    Arg::new("mimic-latency-ms")
                        .long("mimic-latency-ms")
                        .value_name("INT")
                        .default_value("0")
                        .action(ArgAction::Set)
                        .help("Uniform inter-authority RTT in milliseconds; overridden by --latency-table"),
                )
                .arg(
                    Arg::new("latency-table")
                        .long("latency-table")
                        .value_name("PATH")
                        .action(ArgAction::Set)
                        .help("Path to a headerless NxN RTT matrix in milliseconds"),
                )
                .arg(
                    Arg::new("no-batch-messages")
                        .long("no-batch-messages")
                        .action(ArgAction::SetTrue)
                        .help("Disable transport-level outbound message batching"),
                )
                .arg(
                    Arg::new("batch-max-bytes")
                        .long("batch-max-bytes")
                        .value_name("INT")
                        .default_value("65536")
                        .action(ArgAction::Set)
                        .help("Maximum bundled frame size in bytes"),
                )
                .arg(
                    Arg::new("batch-max-delay-ms")
                        .long("batch-max-delay-ms")
                        .value_name("INT")
                        .default_value("5")
                        .action(ArgAction::Set)
                        .help("Maximum bundle delay in milliseconds"),
                )
                .arg(
                    Arg::new("all-to-all")
                        .long("all-to-all")
                        .action(ArgAction::SetTrue)
                        .help(
                            "Use all-to-all Autobahn vote and acknowledgement exchange \
                             (implied by autobahn-optimistic)",
                        ),
                )
                .arg(
                    Arg::new("echo-avail-claims")
                        .long("echo-avail-claims")
                        .action(ArgAction::SetTrue)
                        .conflicts_with("no-echo-avail-claims")
                        .help(
                            "Compatibility flag. Echo availability claims are enabled by default.",
                        ),
                )
                .arg(
                    Arg::new("no-echo-avail-claims")
                        .long("no-echo-avail-claims")
                        .action(ArgAction::SetTrue)
                        .help(
                            "Use periodic VantageAvail watermarks instead of positional \
                             availability bits on AGB echoes.",
                        ),
                )
                .arg(
                    Arg::new("no-ack-watermarks")
                        .long("no-ack-watermarks")
                        .action(ArgAction::SetTrue)
                        .help("Disable compact availability claims and send per-block acknowledgements"),
                )
                .arg(
                    Arg::new("ack-watermark-period-ms")
                        .long("ack-watermark-period-ms")
                        .value_name("INT")
                        .default_value("50")
                        .action(ArgAction::Set)
                        .help("Periodic availability watermark interval in milliseconds"),
                )
                .arg(
                    Arg::new("no-digest-statements")
                        .long("no-digest-statements")
                        .action(ArgAction::SetTrue)
                        .help("Send full proposals in AGB ECHO and READY messages"),
                )
                .arg(
                    Arg::new("no-compact-ids")
                        .long("no-compact-ids")
                        .action(ArgAction::SetTrue)
                        .help("Send full committee identifiers on the Vantage primary wire"),
                ),
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
                anyhow::bail!("local-benchmark requires building with --features benchmark");
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}

// Runs a primary or worker.
async fn run(matches: &ArgMatches) -> Result<()> {
    let key_file = matches.get_one::<String>("keys").unwrap();
    let committee_file = matches.get_one::<String>("committee").unwrap();
    let parameters_file = matches.get_one::<String>("parameters").map(|s| s.as_str());
    let store_path = matches.get_one::<String>("store").unwrap();

    let keypair = KeyPair::import(key_file).context("Failed to load the node's keypair")?;
    let name = keypair.name;
    let committee =
        Committee::import(committee_file).context("Failed to load the committee information")?;

    let mut parameters = match parameters_file {
        Some(filename) => {
            Parameters::import(filename).context("Failed to load the node's parameters")?
        }
        None => Parameters::default(),
    };

    parameters.reconcile_protocol();

    if parameters.latency_table.is_none() {
        parameters.latency_table = Some(std::sync::Arc::new(match parameters.mimic_latency_ms {
            Some(rtt_ms) => LatencyTable::uniform(committee.size(), rtt_ms as f64),
            None => LatencyTable::aws_rtt(committee.size()),
        }));
    }

    let signature_service = SignatureService::new(keypair.secret);

    let store_profile = match matches.subcommand_name() {
        Some("worker") => StoreProfile::Data,
        _ => StoreProfile::Metadata,
    };
    let store =
        Store::new_with_profile(store_path, store_profile).context("Failed to create a store")?;

    let (tx_output, rx_output) = channel(CHANNEL_CAPACITY);

    let (tx_sailfish, _rx_sailfish) = channel(CHANNEL_CAPACITY);

    match matches.subcommand() {
        Some(("primary", _)) => {
            let (tx_new_certificates, _rx_new_certificates) = channel(CHANNEL_CAPACITY);
            let (_tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);
            let (tx_committer, rx_committer) = channel(CHANNEL_CAPACITY);
            let (_tx_pushdown_cert, rx_pushdown_cert) = channel(CHANNEL_CAPACITY);
            let (_tx_request_header_sync, rx_request_header_sync) = channel(CHANNEL_CAPACITY);

            let (_, _, registry) = Primary::spawn(
                name,
                committee.clone(),
                parameters.clone(),
                signature_service.clone(),
                store.clone(),
                tx_new_certificates,
                tx_committer,
                rx_committer,
                rx_feedback,
                tx_sailfish,
                rx_pushdown_cert,
                rx_request_header_sync,
                tx_output,
            );
            metrics::register_process_collector(&registry)
                .context("Failed to register primary process metrics")?;
        }

        Some(("worker", sub_matches)) => {
            let id = sub_matches
                .get_one::<String>("id")
                .unwrap()
                .parse::<WorkerId>()
                .context("The worker id must be a positive integer")?;
            let (_, _, registry) = Worker::spawn(keypair.name, id, committee, parameters, store);
            metrics::register_process_collector(&registry)
                .context("Failed to register worker process metrics")?;
        }
        _ => unreachable!(),
    }

    analyze(rx_output).await;

    unreachable!();
}

/// Receives ordered headers.
async fn analyze(mut rx_output: Receiver<Header>) {
    while let Some(_header) = rx_output.recv().await {}
}

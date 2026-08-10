// Copyright(C) Facebook, Inc. and its affiliates.
use crypto::{generate_production_keypair, PublicKey, SecretKey};
use log::{info, warn};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::BufWriter;
use std::io::Write as _;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Node {0} is not in the committee")]
    NotInCommittee(PublicKey),

    #[error("Unknown worker id {0}")]
    UnknownWorker(WorkerId),

    #[error("Failed to read config file '{file}': {message}")]
    ImportError { file: String, message: String },

    #[error("Failed to write config file '{file}': {message}")]
    ExportError { file: String, message: String },
}

pub trait Import: DeserializeOwned {
    fn import(path: &str) -> Result<Self, ConfigError> {
        let reader = || -> Result<Self, std::io::Error> {
            let data = fs::read(path)?;
            Ok(serde_json::from_slice(data.as_slice())?)
        };
        reader().map_err(|e| ConfigError::ImportError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }
}

pub trait Export: Serialize {
    fn export(&self, path: &str) -> Result<(), ConfigError> {
        let writer = || -> Result<(), std::io::Error> {
            // Truncate the file so a shorter serialization cannot leave trailing data.
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(path)?;
            let mut writer = BufWriter::new(file);
            let data = serde_json::to_string_pretty(self).unwrap();
            writer.write_all(data.as_ref())?;
            writer.write_all(b"\n")?;
            Ok(())
        };
        writer().map_err(|e| ConfigError::ExportError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }
}

pub type Stake = u32;
pub type WorkerId = u32;

/// Consensus protocol selected for this node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[derive(Default)]
pub enum Protocol {
    /// Autobahn as shipped/evaluated (`use_optimistic_tips = true`).
    #[default]
    AutobahnOptimistic,
    /// Autobahn with certified-tips-only cut formation (`use_optimistic_tips = false`).
    AutobahnSeamless,
    /// Signature-free AGB protocol.
    Vantage,
    /// Simple-IT cut consensus using `simpleit::CutEngine`.
    SimpleIt,
    /// Simple-IT using the Bracha RBC variant.
    SimpleItBracha,
}

impl Protocol {
    /// Returns the optimistic-tip setting for protocols that use Autobahn.
    /// Returns `None` for Vantage and Simple-IT.
    pub fn implied_optimistic_tips(&self) -> Option<bool> {
        match self {
            Protocol::AutobahnOptimistic => Some(true),
            Protocol::AutobahnSeamless => Some(false),
            Protocol::Vantage => None,
            Protocol::SimpleIt => None,
            Protocol::SimpleItBracha => None,
        }
    }

    /// Canonical label for `protocol_info` and the `--protocol` CLI value.
    pub fn label(&self) -> &'static str {
        match self {
            Protocol::AutobahnOptimistic => "autobahn-optimistic",
            Protocol::AutobahnSeamless => "autobahn-seamless",
            Protocol::Vantage => "vantage",
            Protocol::SimpleIt => "simple-it",
            Protocol::SimpleItBracha => "simple-it-bracha",
        }
    }
}

/// Default value for `use_optimistic_tips` when a parameter file omits it.
/// `reconcile_protocol` derives the effective value from `protocol`.
fn default_use_optimistic_tips() -> bool {
    true
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Parameters {
    /// The timeout delay of the consensus protocol.
    pub timeout_delay: u64,
    /// The preferred header size. The primary creates a new header when it has enough parents and
    /// enough batches' digests to reach `header_size`. Denominated in bytes.
    pub header_size: usize,
    /// The maximum delay that the primary waits between generating two headers, even if the header
    /// did not reach `max_header_size`. Denominated in ms.
    pub max_header_delay: u64,
    /// The depth of the garbage collection (Denominated in number of rounds).
    pub gc_depth: u64,
    /// The delay after which the synchronizer retries to send sync requests. Denominated in ms.
    pub sync_retry_delay: u64,
    /// Determine with how many nodes to sync when re-trying to send sync-request. These nodes
    /// are picked at random from the committee.
    pub sync_retry_nodes: usize,
    /// The preferred batch size. The workers seal a batch of transactions when it reaches this size.
    /// Denominated in bytes.
    pub batch_size: usize,
    /// The delay after which the workers seal a batch of transactions, even if `max_batch_size`
    /// is not reached. Denominated in ms.
    pub max_batch_delay: u64,

    // Autobahn protocol parameters.
    #[serde(default = "default_use_optimistic_tips")]
    pub use_optimistic_tips: bool, // Default true.

    pub use_parallel_proposals: bool, // Default true.
    pub k: u64,                       // Maximum open consensus instances.

    pub use_fast_path: bool, // Autobahn only; default true.
    pub fast_path_timeout: u64,

    pub use_ride_share: bool,
    pub car_timeout: u64,

    /// Autobahn (Giridharan et al., SOSP'24) §5.5.3 "All-to-all communication":
    /// On the external-consensus path, broadcast prepare and confirm votes and
    /// assemble the corresponding certificates locally.
    #[serde(default)]
    pub all_to_all: bool,

    // Asynchrony simulation.
    pub simulate_asynchrony: bool,
    pub asynchrony_start: u64,
    pub asynchrony_duration: u64,

    /// The consensus protocol assembly to run. Authoritative over
    /// `use_optimistic_tips` (see `reconcile_protocol`).
    #[serde(default)]
    pub protocol: Protocol,

    /// Transaction-generation mode supplied by the load generator, when known.
    /// `None` means the mode is unknown.
    #[serde(default)]
    pub tx_mode: Option<String>,

    /// Vantage-only maximum number of payload entries in one data block.
    /// The value is part of `BlockOK`.
    #[serde(default = "default_max_block_payload")]
    pub max_block_payload: usize,

    /// Vantage-only AGB base delay unit in milliseconds. Fallback deadlines are
    /// derived from this value.
    #[serde(default = "default_delta_ms")]
    pub delta_ms: u64,

    /// Benchmark-only epoch-millisecond instant at which commit metrics become active.
    /// Transactions submitted before this instant are excluded from rate and latency
    /// metrics. `None` disables the gate.
    #[serde(default)]
    pub metrics_active_at_ms: Option<u64>,

    /// Vantage-only number of views retained behind the resolved prefix before internal
    /// garbage collection. Carrier bodies remain available for the configured service
    /// margin. The value is clamped to at least 1.
    #[serde(default = "default_vantage_gc_window_views")]
    pub vantage_gc_window_views: u64,

    /// Simple-IT-only number of rounds retained before `CutEngine::prune_below` runs.
    /// The value is clamped to at least 1.
    #[serde(default = "default_simpleit_gc_window_rounds")]
    pub simpleit_gc_window_rounds: u64,

    /// Enables periodic per-author digest-bound availability watermarks. Each watermark
    /// identifies a verified lane prefix by height and digest before aggregation.
    #[serde(default = "default_ack_watermarks")]
    pub ack_watermarks: bool,
    /// Ack-watermark broadcast period in milliseconds.
    #[serde(default = "default_ack_watermark_period_ms")]
    pub ack_watermark_period_ms: u64,

    /// Enables digest-named AGB statements. Proposals are still sent by value;
    /// digest statements identify them and use the body-fetch path when needed.
    #[serde(default = "default_digest_statements")]
    pub digest_statements: bool,

    /// Builds the local hash-chained sequence log and checkpoint heads. State sync can
    /// announce, certify, download, verify, and install remote sequence data.
    #[serde(default = "default_sequence_checkpoints")]
    pub sequence_checkpoints: bool,

    /// Checkpoint boundary interval in terminally processed views. Zero is treated as 1.
    #[serde(default = "default_sequence_checkpoint_interval_views")]
    pub sequence_checkpoint_interval_views: u64,

    /// How often the announce timer fires. An announcement
    /// is sent when the local boundary has advanced, or when
    /// `sequence_announce_repeat_ms` has elapsed for the current one.
    #[serde(default = "default_sequence_announce_period_ms")]
    pub sequence_announce_period_ms: u64,

    /// Re-send interval for an unchanged boundary. Repetition lets a late joiner collect
    /// the required `f+1` announcements.
    #[serde(default = "default_sequence_announce_repeat_ms")]
    pub sequence_announce_repeat_ms: u64,

    /// Records per served chunk. Chunked by ITEM COUNT rather than bytes because records
    /// are fixed-width, so an item cap is already an exact byte cap.
    #[serde(default = "default_sequence_sync_chunk_records")]
    pub sequence_sync_chunk_records: usize,

    /// Maximum terminal outcome views per served range. This bounds runs of `Skip`
    /// outcomes; manifest-carrying outcomes use `sequence_sync_chunk_outcome_items`.
    #[serde(default = "default_sequence_sync_chunk_outcomes")]
    pub sequence_sync_chunk_outcomes: usize,

    /// Maximum manifest references in one served outcome range.
    #[serde(default = "default_sequence_sync_chunk_outcome_items")]
    pub sequence_sync_chunk_outcome_items: usize,

    /// Delta digests per served chunk (32 B each).
    #[serde(default = "default_sequence_sync_chunk_digests")]
    pub sequence_sync_chunk_digests: usize,

    /// Run state sync while a certified checkpoint is at least this many terminal views
    /// beyond the local sequence cursor. A staged install may continue to drain below it.
    #[serde(default = "default_sequence_sync_min_gap_views")]
    pub sequence_sync_min_gap_views: u64,

    /// Gap above which ordinary consensus/control traffic is dropped before entering the
    /// core queue. This is independent of the sync threshold.
    #[serde(default = "default_sequence_sync_shed_gap_views")]
    pub sequence_sync_shed_gap_views: u64,

    /// Gap at which a recovered node re-arms state sync. The gap must represent a new
    /// outage so normal participation remains active after recovery.
    #[serde(default = "default_sequence_sync_rearm_gap_views")]
    pub sequence_sync_rearm_gap_views: u64,

    /// Matching announcers queried concurrently for one outstanding chunk.
    #[serde(default = "default_sequence_sync_max_sources")]
    pub sequence_sync_max_sources: usize,

    /// Per-request deadline before failing over to the NEXT source.
    #[serde(default = "default_sequence_sync_request_timeout_ms")]
    pub sequence_sync_request_timeout_ms: u64,

    /// Bounded ingress for state-sync responses. Overflow drops the newest frame;
    /// responses are idempotent and can be requested again.
    #[serde(default = "default_sequence_sync_inbound_capacity")]
    pub sequence_sync_inbound_capacity: usize,

    /// Verified target views admitted into the block-fetch window at once.
    #[serde(default = "default_sequence_install_window_views")]
    pub sequence_install_window_views: usize,

    /// Retained for configuration compatibility. Install admission uses
    /// `sequence_install_window_views`.
    #[serde(default = "default_sequence_install_settle_ceiling")]
    pub sequence_install_settle_ceiling: usize,

    /// Apply verified checkpoint state to the cursor.
    #[serde(default = "default_sequence_install_enabled")]
    pub sequence_install_enabled: bool,

    /// Views applied per install pass. The limit prevents one target from monopolizing
    /// the consensus core.
    #[serde(default = "default_sequence_install_views_per_tick")]
    pub sequence_install_views_per_tick: usize,

    /// Block digests emitted per install pass.
    ///
    /// `Cursor::install` leaves a view open when the budget is exhausted.
    #[serde(default = "default_sequence_install_digests_per_tick")]
    pub sequence_install_digests_per_tick: usize,

    /// Carries positional availability bits on AGB echoes instead of periodic
    /// `VantageAvail` messages. Requires `ack_watermarks`.
    #[serde(default = "default_echo_avail_claims")]
    pub echo_avail_claims: bool,

    /// Optional local-run one-way latency table for primary-to-primary
    /// connections. CLI handlers populate it; library code does not.
    /// The field is skipped by serde and is unset by `Parameters::default()`.
    /// `node run` uses an explicit `mimic_latency_ms` value or AWS RTT.
    /// `node local-benchmark` gives the CSV table precedence, then the explicit
    /// mimic value, then AWS RTT. An explicit zero requests no injected latency.
    #[serde(skip)]
    pub latency_table: Option<Arc<LatencyTable>>,

    /// Deployable uniform-RTT setting corresponding to
    /// `node local-benchmark --mimic-latency-ms`. This field is serialized in
    /// `parameters.json`.
    ///
    /// `node run` treats this field as an explicit override to a uniform scalar:
    /// `Some(rtt)` (including `Some(0)`) always wins and expands to
    /// `LatencyTable::uniform(committee.size(), rtt)` at spawn time (one-way = rtt/2).
    /// When `None`, `node run` uses `LatencyTable::aws_rtt`.
    #[serde(default)]
    pub mimic_latency_ms: Option<u64>,

    /// Enables transport-level per-peer outbound message batching. Client-facing
    /// transaction traffic is not batched.
    #[serde(default = "default_batch_messages")]
    pub batch_messages: bool,
    /// Hybrid flush size cap in bytes (see `network::BatchConfig::max_bytes`).
    /// Irrelevant when `batch_messages` is off.
    #[serde(default = "default_batch_max_bytes")]
    pub batch_max_bytes: usize,
    /// Hybrid flush delay in milliseconds (see `network::BatchConfig::max_delay_ms`).
    /// Irrelevant when `batch_messages` is off.
    #[serde(default = "default_batch_max_delay_ms")]
    pub batch_max_delay_ms: u64,

    /// Data-plane withholding fault injector. The first `withhold_senders` committee
    /// indices withhold payload dissemination from a staggered half of the committee.
    /// Other message types are unaffected. Zero disables withholding.
    #[serde(default)]
    pub withhold_senders: usize,

    /// Start offset in milliseconds for the time-windowed withholding injector.
    /// `None` enables withholding for the whole run. Set by `node local-benchmark`.
    #[serde(default)]
    pub withhold_at_ms: Option<u64>,
    /// Withholding window duration in milliseconds when `withhold_at_ms` is set.
    #[serde(default = "default_withhold_for_ms")]
    pub withhold_for_ms: u64,
    /// Shared in-process window state used by the withholding filters. It is not
    /// serialized and is initialized by `node local-benchmark`.
    #[serde(skip)]
    pub withhold_window: Option<Arc<OnceLock<(Instant, Instant)>>>,

    /// Period for checking gaps between a local verified lane prefix and the highest
    /// attested height for another author. Shared by Vantage and Simple-IT.
    #[serde(default = "default_resume_check_period_ms")]
    pub resume_check_period_ms: u64,
    /// Minimum spacing between resume requests or resume batches for the same lane
    /// author and gap height.
    #[serde(default = "default_resume_backoff_ms")]
    pub resume_backoff_ms: u64,
    /// Maximum number of blocks served in one lane-resume batch. The requester asks
    /// for the next batch after its frontier advances. `resume_max_concurrent` limits
    /// concurrent resume episodes; zero means unlimited.
    #[serde(default = "default_resume_max_concurrent")]
    pub resume_max_concurrent: usize,

    #[serde(default = "default_resume_batch")]
    pub resume_batch: u64,

    /// Per-destination outbound queue depth at which a volatile send is shed and
    /// recorded for reconnect replay. Zero disables the limit. Used only when
    /// `reconnect_replay` is enabled.
    #[serde(default = "default_volatile_soft_cap")]
    pub volatile_soft_cap: usize,

    /// Enables reconnect replay for volatile one-shot messages. When disabled,
    /// broadcasts use the ordinary durable path and replay messages are ignored.
    #[serde(default = "default_reconnect_replay")]
    pub reconnect_replay: bool,

    /// Reconnect-waiter's exponential-backoff ceiling in milliseconds.
    /// Applied to every primary-to-primary `ReliableSender`.
    #[serde(default = "default_retry_backoff_max_ms")]
    pub retry_backoff_max_ms: u64,

    /// Number of views of one-shot messages retained by `Outbox` behind
    /// `own_watermark` before `prune_below` evicts them. `outbox_max_bytes` is an
    /// additional byte cap.
    #[serde(default = "default_replay_history_views")]
    pub replay_history_views: u64,
    /// Resume task payload size per chunk in bytes.
    #[serde(default = "default_replay_chunk_bytes")]
    pub replay_chunk_bytes: usize,
    /// Delay between resume-task round-robin rotations. Together with
    /// `replay_chunk_bytes`, this bounds replay throughput.
    #[serde(default = "default_replay_chunk_interval_ms")]
    pub replay_chunk_interval_ms: u64,
    /// Per-peer served-byte budget per `resume_backoff_ms` window. A single key larger
    /// than the budget is served as one unit.
    #[serde(default = "default_replay_serve_max_bytes")]
    pub replay_serve_max_bytes: usize,
    /// Outbox total byte cap. Eviction removes whole oldest views and preserves the
    /// newest key.
    #[serde(default = "default_outbox_max_bytes")]
    pub outbox_max_bytes: usize,
    /// Replay episode expiry and author-side in-flight stream TTL in milliseconds.
    #[serde(default = "default_replay_episode_max_ms")]
    pub replay_episode_max_ms: u64,
}

fn default_batch_messages() -> bool {
    true
}

/// Availability watermarks are enabled by default. `--no-ack-watermarks` disables them.
fn default_ack_watermarks() -> bool {
    true
}

fn default_echo_avail_claims() -> bool {
    true
}

/// Digest-named AGB statements are enabled by default.
/// `--no-digest-statements` disables them.
fn default_digest_statements() -> bool {
    true
}

/// On by default: sequence checkpoints and state sync provide Vantage recovery.
fn default_sequence_checkpoints() -> bool {
    true
}

/// Default checkpoint interval below the state-sync entry threshold.
fn default_sequence_checkpoint_interval_views() -> u64 {
    20
}

fn default_sequence_announce_period_ms() -> u64 {
    250
}

fn default_sequence_announce_repeat_ms() -> u64 {
    1_000
}

/// Default sequence records per served chunk.
fn default_sequence_sync_chunk_records() -> usize {
    256
}

/// Secondary bound for long runs of `Skip` outcomes, which consume no manifest items.
fn default_sequence_sync_chunk_outcomes() -> usize {
    256
}

/// Default manifest references per served outcome range.
fn default_sequence_sync_chunk_outcome_items() -> usize {
    1_600
}

/// Default delta digests per served chunk.
fn default_sequence_sync_chunk_digests() -> usize {
    1_024
}

/// State-sync entry threshold in views.
fn default_sequence_sync_min_gap_views() -> u64 {
    100
}

/// Inbound shedding threshold in views.
fn default_sequence_sync_shed_gap_views() -> u64 {
    300
}

/// Gap threshold for re-arming state sync after recovery.
fn default_sequence_sync_rearm_gap_views() -> u64 {
    800
}

/// `f+1` at the smallest committee this targets.
fn default_sequence_sync_max_sources() -> usize {
    3
}

fn default_sequence_sync_request_timeout_ms() -> u64 {
    1_000
}

fn default_sequence_sync_inbound_capacity() -> usize {
    1_024
}

/// Default verified views admitted into the install window.
fn default_sequence_install_window_views() -> usize {
    64
}

/// Default install settle ceiling.
fn default_sequence_install_settle_ceiling() -> usize {
    2_048
}

fn default_sequence_install_enabled() -> bool {
    true
}

fn default_sequence_install_views_per_tick() -> usize {
    16
}

/// Default digest budget per install pass.
fn default_sequence_install_digests_per_tick() -> usize {
    2_048
}

fn default_batch_max_bytes() -> usize {
    65_536
}

fn default_batch_max_delay_ms() -> u64 {
    5
}

fn default_max_block_payload() -> usize {
    16
}

fn default_delta_ms() -> u64 {
    // The default covers the largest one-way delay in the configured RTT matrix.
    200
}

/// Default Vantage internal-state retention window in views.
fn default_vantage_gc_window_views() -> u64 {
    200
}

/// Default Simple-IT internal-state retention window in rounds.
fn default_simpleit_gc_window_rounds() -> u64 {
    50
}

/// `ack_watermarks`'s own doc comment.
fn default_ack_watermark_period_ms() -> u64 {
    50
}

/// Default withholding window duration in milliseconds.
fn default_withhold_for_ms() -> u64 {
    30_000
}

/// `Parameters::resume_check_period_ms`'s own doc comment.
fn default_resume_check_period_ms() -> u64 {
    1_000
}

/// `Parameters::resume_backoff_ms`'s own doc comment.
fn default_resume_backoff_ms() -> u64 {
    4_000
}

/// `Parameters::resume_batch`'s own doc comment.
fn default_resume_batch() -> u64 {
    64
}

/// `Parameters::volatile_soft_cap`'s own doc comment.
fn default_volatile_soft_cap() -> usize {
    1_024
}

/// `Parameters::reconnect_replay`'s own doc comment.
fn default_reconnect_replay() -> bool {
    true
}

/// Default reconnect backoff ceiling in milliseconds.
fn default_retry_backoff_max_ms() -> u64 {
    2_000
}

/// Default maximum concurrent resume episodes.
fn default_resume_max_concurrent() -> usize {
    8
}

fn default_replay_history_views() -> u64 {
    512
}

/// `Parameters::replay_chunk_bytes`'s own doc comment.
fn default_replay_chunk_bytes() -> usize {
    65_536
}

/// `Parameters::replay_chunk_interval_ms`'s own doc comment.
fn default_replay_chunk_interval_ms() -> u64 {
    5
}

/// `Parameters::replay_serve_max_bytes`'s own doc comment.
fn default_replay_serve_max_bytes() -> usize {
    8 << 20
}

/// `Parameters::outbox_max_bytes`'s own doc comment.
fn default_outbox_max_bytes() -> usize {
    64 << 20
}

/// `Parameters::replay_episode_max_ms`'s own doc comment.
fn default_replay_episode_max_ms() -> u64 {
    60_000
}

/// AWS region names for the RTT matrix below.
#[allow(unused)]
const REGIONS: [&str; 10] = [
    "us-east-1",      // USE1
    "us-west-1",      // USW1
    "ca-central-1",   // CAC1
    "eu-west-1",      // EUW1
    "eu-south-1",     // EUS2
    "eu-north-1",     // EUN1
    "sa-east-1",      // SAE1
    "ap-south-1",     // APS1
    "ap-southeast-1", // APSE2
    "ap-northeast-1", // APNE1
];

/// RTT table for the AWS regions above, in milliseconds.
const RTT_LATENCY_TABLE: [[u32; 10]; 10] = [
    [1, 14, 104, 112, 198, 65, 68, 110, 201, 146],
    [14, 1, 106, 122, 196, 78, 67, 103, 189, 142],
    [104, 106, 1, 215, 281, 163, 29, 50, 143, 238],
    [112, 122, 215, 1, 309, 175, 176, 220, 299, 254],
    [198, 196, 281, 309, 1, 137, 254, 268, 150, 101],
    [65, 78, 163, 175, 137, 1, 127, 172, 226, 108],
    [68, 67, 29, 176, 254, 127, 1, 38, 125, 199],
    [110, 103, 50, 220, 268, 172, 38, 1, 148, 245],
    [201, 189, 143, 299, 150, 226, 125, 148, 1, 140],
    [146, 142, 238, 254, 101, 108, 199, 245, 140, 1],
];

/// Optional n x n one-way inter-authority latency table. Rows and columns use committee
/// order. The table is fixed; it has no per-call jitter.
#[derive(Clone, Debug)]
pub struct LatencyTable {
    /// `one_way_ms[i][j]` = one-way latency (ms) from committee-order index `i` to
    /// `j`. Symmetric by construction (halved from an RTT matrix), diagonal 0.
    one_way_ms: Vec<Vec<f64>>,
}

impl LatencyTable {
    /// Builds a uniform table from an RTT value. Off-diagonal entries are `rtt_ms / 2`;
    /// diagonal entries are zero.
    pub fn uniform(n: usize, rtt_ms: f64) -> Self {
        let one_way = rtt_ms / 2.0;
        let mut t = vec![vec![one_way; n]; n];
        for (i, row) in t.iter_mut().enumerate() {
            row[i] = 0.0;
        }
        Self { one_way_ms: t }
    }

    /// Parses an n x n comma-separated RTT matrix in committee order and stores half
    /// of each value as one-way latency. The matrix must have exactly n rows and columns.
    pub fn from_rtt_csv(path: &str, n: usize) -> Result<Self, ConfigError> {
        let err = |message: String| ConfigError::ImportError {
            file: path.to_string(),
            message,
        };
        let data = fs::read_to_string(path).map_err(|e| err(e.to_string()))?;
        let mut rows: Vec<Vec<f64>> = Vec::new();
        for line in data.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let row: Result<Vec<f64>, _> = line
                .split(',')
                .map(|cell| cell.trim().parse::<f64>())
                .collect();
            rows.push(
                row.map_err(|e| err(format!("non-numeric cell in row {}: {}", rows.len(), e)))?,
            );
        }
        if rows.len() != n || rows.iter().any(|r| r.len() != n) {
            return Err(err(format!(
                "expected a {n}x{n} RTT matrix, got {} data row(s) with lengths {:?}",
                rows.len(),
                rows.iter().map(|r| r.len()).collect::<Vec<_>>()
            )));
        }
        let one_way_ms = rows
            .into_iter()
            .map(|row| row.into_iter().map(|rtt_ms| rtt_ms / 2.0).collect())
            .collect();
        Ok(Self { one_way_ms })
    }

    /// Builds an n x n table by mapping committee indices cyclically to the ten regions
    /// in `RTT_LATENCY_TABLE`. Entries are halved for one-way latency; the diagonal is 0.
    pub fn aws_rtt(n: usize) -> Self {
        let mut t = vec![vec![0.0; n]; n];
        for (i, row) in t.iter_mut().enumerate() {
            for (j, cell) in row.iter_mut().enumerate() {
                *cell = RTT_LATENCY_TABLE[i % 10][j % 10] as f64 / 2.0;
            }
        }
        for (i, row) in t.iter_mut().enumerate() {
            row[i] = 0.0;
        }
        Self { one_way_ms: t }
    }

    /// Returns one-way latency between committee-order indices. Out-of-range indices
    /// return `Duration::ZERO`.
    pub fn one_way(&self, i: usize, j: usize) -> Duration {
        self.one_way_ms
            .get(i)
            .and_then(|row| row.get(j))
            .map_or(Duration::ZERO, |ms| {
                Duration::from_secs_f64(ms.max(0.0) / 1000.0)
            })
    }

    /// Returns the committee size used to build this table.
    pub fn dimension(&self) -> usize {
        self.one_way_ms.len()
    }

    /// Returns whether any off-diagonal entry injects delay.
    pub fn injects_delay(&self) -> bool {
        self.one_way_ms
            .iter()
            .enumerate()
            .any(|(i, row)| row.iter().enumerate().any(|(j, &ms)| i != j && ms > 0.0))
    }
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            timeout_delay: 1_000,
            header_size: 1_000,
            max_header_delay: 100,
            gc_depth: 50,
            vantage_gc_window_views: default_vantage_gc_window_views(),
            simpleit_gc_window_rounds: default_simpleit_gc_window_rounds(),
            ack_watermarks: default_ack_watermarks(),
            ack_watermark_period_ms: default_ack_watermark_period_ms(),
            digest_statements: default_digest_statements(),
            sequence_checkpoints: default_sequence_checkpoints(),
            sequence_checkpoint_interval_views: default_sequence_checkpoint_interval_views(),
            sequence_announce_period_ms: default_sequence_announce_period_ms(),
            sequence_announce_repeat_ms: default_sequence_announce_repeat_ms(),
            sequence_sync_chunk_records: default_sequence_sync_chunk_records(),
            sequence_sync_chunk_outcomes: default_sequence_sync_chunk_outcomes(),
            sequence_sync_chunk_outcome_items: default_sequence_sync_chunk_outcome_items(),
            sequence_sync_chunk_digests: default_sequence_sync_chunk_digests(),
            sequence_sync_min_gap_views: default_sequence_sync_min_gap_views(),
            sequence_sync_shed_gap_views: default_sequence_sync_shed_gap_views(),
            sequence_sync_rearm_gap_views: default_sequence_sync_rearm_gap_views(),
            sequence_sync_max_sources: default_sequence_sync_max_sources(),
            sequence_sync_request_timeout_ms: default_sequence_sync_request_timeout_ms(),
            sequence_sync_inbound_capacity: default_sequence_sync_inbound_capacity(),
            sequence_install_window_views: default_sequence_install_window_views(),
            sequence_install_settle_ceiling: default_sequence_install_settle_ceiling(),
            sequence_install_enabled: default_sequence_install_enabled(),
            sequence_install_views_per_tick: default_sequence_install_views_per_tick(),
            sequence_install_digests_per_tick: default_sequence_install_digests_per_tick(),
            echo_avail_claims: default_echo_avail_claims(),
            sync_retry_delay: 5_000,
            sync_retry_nodes: 3,
            batch_size: 500_000,
            max_batch_delay: 100,

            // Autobahn parameters.
            use_optimistic_tips: true,
            use_parallel_proposals: true,
            k: 4,
            use_fast_path: true,
            fast_path_timeout: 500,
            use_ride_share: false,
            car_timeout: 2000,
            all_to_all: false,

            // Asynchrony simulation.
            simulate_asynchrony: false,
            asynchrony_start: 20_000,    // Start after 20 seconds.
            asynchrony_duration: 10_000, // Run for 10 seconds.

            protocol: Protocol::default(),

            tx_mode: None,
            max_block_payload: default_max_block_payload(),
            delta_ms: default_delta_ms(),
            // No gate by default: every observation counts.
            metrics_active_at_ms: None,
            latency_table: None,
            mimic_latency_ms: None,
            batch_messages: true,
            batch_max_bytes: default_batch_max_bytes(),
            batch_max_delay_ms: default_batch_max_delay_ms(),
            withhold_senders: 0,
            withhold_at_ms: None,
            withhold_for_ms: default_withhold_for_ms(),
            withhold_window: None,
            resume_check_period_ms: default_resume_check_period_ms(),
            resume_backoff_ms: default_resume_backoff_ms(),
            resume_max_concurrent: default_resume_max_concurrent(),
            resume_batch: default_resume_batch(),
            volatile_soft_cap: default_volatile_soft_cap(),
            reconnect_replay: default_reconnect_replay(),
            retry_backoff_max_ms: default_retry_backoff_max_ms(),
            replay_history_views: default_replay_history_views(),
            replay_chunk_bytes: default_replay_chunk_bytes(),
            replay_chunk_interval_ms: default_replay_chunk_interval_ms(),
            replay_serve_max_bytes: default_replay_serve_max_bytes(),
            outbox_max_bytes: default_outbox_max_bytes(),
            replay_episode_max_ms: default_replay_episode_max_ms(),
        }
    }
}

impl Import for Parameters {}
impl Export for Parameters {}

impl Parameters {
    /// Reconcile `use_optimistic_tips` with `protocol`. `protocol` is authoritative.
    pub fn reconcile_protocol(&mut self) {
        if let Some(implied) = self.protocol.implied_optimistic_tips() {
            if self.use_optimistic_tips != implied {
                warn!(
                    "use_optimistic_tips={} is inconsistent with protocol {:?}; \
                     protocol wins, using use_optimistic_tips={}",
                    self.use_optimistic_tips, self.protocol, implied
                );
                self.use_optimistic_tips = implied;
            }
        }
    }

    pub fn log(&self) {
        info!("Protocol: {:?}", self.protocol);
        info!("Timeout delay set to {} ms", self.timeout_delay);
        info!("Header size set to {} B", self.header_size);
        info!("Max header delay set to {} ms", self.max_header_delay);
        info!("Garbage collection depth set to {} rounds", self.gc_depth);
        info!(
            "Vantage internal GC window set to {} views",
            self.vantage_gc_window_views
        );
        info!(
            "Simple-IT internal GC window set to {} rounds",
            self.simpleit_gc_window_rounds
        );
        info!("Sync retry delay set to {} ms", self.sync_retry_delay);
        info!("Sync retry nodes set to {} nodes", self.sync_retry_nodes);
        info!("Batch size set to {} B", self.batch_size);
        info!("Max batch delay set to {} ms", self.max_batch_delay);

        info!(
            "Fast path enabled? {}. Fast timeout: {}",
            self.use_fast_path, self.fast_path_timeout
        );
        info!("Optimistic tips enabled? {}", self.use_optimistic_tips);
        info!(
            "Parallel Proposals enabled? {}. K: {}",
            self.use_parallel_proposals, self.k
        );
        info!(
            "Ride share enabled? {}. Car timeout: {}",
            self.use_ride_share, self.car_timeout
        );
        info!("All-to-all (Autobahn §5.5.3) enabled? {}", self.all_to_all);
        info!(
            "Max block payload set to {} entries",
            self.max_block_payload
        );
        info!("Vantage delta set to {} ms", self.delta_ms);
        match &self.latency_table {
            Some(table) if table.injects_delay() => {
                info!(
                    "Mimic latency table active: {0}x{0} table, injecting delay",
                    table.dimension()
                );
            }
            Some(table) => {
                info!(
                    "Mimic latency table present but all-zero: {0}x{0} table, no delay injected",
                    table.dimension()
                );
            }
            None => {}
        }
        info!(
            "Network batching enabled? {}. Max bytes: {}. Max delay: {} ms",
            self.batch_messages, self.batch_max_bytes, self.batch_max_delay_ms
        );
        info!(
            "Availability acknowledgments: watermarks={}, echo claims={}, period={} ms",
            self.ack_watermarks, self.echo_avail_claims, self.ack_watermark_period_ms
        );
        info!(
            "Digest-named AGB statements (ECHO/READY name their proposal by hash \
             instead of by value) enabled? {}",
            self.digest_statements
        );
        info!(
            "Lane-resume (Mechanism A: sender-side resume triggered by an ack-census \
             gap) check period {} ms, backoff {} ms, batch {} blocks",
            self.resume_check_period_ms, self.resume_backoff_ms, self.resume_batch
        );
        info!(
            "Reconnect replay (server-floored volatile one-shot replay) {}: outbox {} views / \
             {} B, replay chunk {} B / {} ms, per-peer serve budget {} B, episode/in-flight TTL \
             {} ms, retry backoff cap {} ms, volatile soft cap {} msgs",
            if self.reconnect_replay {
                "ENABLED"
            } else {
                "DISABLED"
            },
            self.replay_history_views,
            self.outbox_max_bytes,
            self.replay_chunk_bytes,
            self.replay_chunk_interval_ms,
            self.replay_serve_max_bytes,
            self.replay_episode_max_ms,
            self.retry_backoff_max_ms,
            self.volatile_soft_cap
        );
        if self.withhold_senders > 0 {
            match self.withhold_at_ms {
                Some(at) => info!(
                    "Data-plane withholding: first {} node(s) withhold payload dissemination \
                     from a staggered half of the committee, active [{}, {}) ms after start",
                    self.withhold_senders,
                    at,
                    at + self.withhold_for_ms
                ),
                None => info!(
                    "Data-plane withholding: first {} node(s) withhold payload dissemination \
                     from a staggered half of the committee",
                    self.withhold_senders
                ),
            }
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ConsensusAddresses {
    /// Address to receive messages from other consensus nodes (WAN).
    pub consensus_to_consensus: SocketAddr,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PrimaryAddresses {
    /// Address to receive messages from other primaries (WAN).
    pub primary_to_primary: SocketAddr,
    /// Address to receive messages from our workers (LAN).
    pub worker_to_primary: SocketAddr,
    /// Address serving this primary's Prometheus metrics (LAN; scraped by the
    /// benchmark harness at run end).
    pub metrics: SocketAddr,
}

#[derive(Clone, Serialize, Deserialize, Eq, Hash, PartialEq)]
pub struct WorkerAddresses {
    /// Address to receive client transactions (WAN).
    pub transactions: SocketAddr,
    /// Address to receive messages from other workers (WAN).
    pub worker_to_worker: SocketAddr,
    /// Address to receive messages from our primary (LAN).
    pub primary_to_worker: SocketAddr,
    /// Address serving this worker's Prometheus metrics (LAN; scraped by the
    /// benchmark harness at run end).
    pub metrics: SocketAddr,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Authority {
    /// The voting power of this authority.
    pub stake: Stake,
    /// The network addresses of the consensus protocol.
    pub consensus: ConsensusAddresses,
    /// The network addresses of the primary.
    pub primary: PrimaryAddresses,
    /// Map of workers' id and their network addresses.
    pub workers: HashMap<WorkerId, WorkerAddresses>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Committee {
    pub authorities: BTreeMap<PublicKey, Authority>,
    //pub id_map: HashMap<PublicKey, u64>, //position
}

impl Import for Committee {}
impl Export for Committee {}

impl Committee {
    pub fn new(info: Vec<(PublicKey, Stake, SocketAddr)>) -> Self {
        Self {
            authorities: info
                .into_iter()
                .map(|(name, stake, address)| {
                    let authority = Authority {
                        stake,
                        consensus: ConsensusAddresses {
                            consensus_to_consensus: address,
                        },
                        primary: PrimaryAddresses {
                            primary_to_primary: address,
                            worker_to_primary: address,
                            metrics: address,
                        },
                        workers: HashMap::new(),
                    };
                    (name, authority)
                })
                .collect(),
        }
    }

    /// Generates an in-memory committee for `node local-benchmark`. Keys are sorted by
    /// public key so vector indices match committee order and latency-table rows.
    pub fn local_benchmark(nodes: usize, workers: usize, base_port: u16) -> (Self, Vec<KeyPair>) {
        let mut authorities = BTreeMap::new();
        let mut keypairs = Vec::with_capacity(nodes);
        let mut port = base_port;

        for _ in 0..nodes {
            let keypair = KeyPair::new();

            let consensus = ConsensusAddresses {
                consensus_to_consensus: format!("127.0.0.1:{}", port).parse().unwrap(),
            };
            port += 1;

            let primary = PrimaryAddresses {
                primary_to_primary: format!("127.0.0.1:{}", port).parse().unwrap(),
                worker_to_primary: format!("127.0.0.1:{}", port + 1).parse().unwrap(),
                metrics: format!("127.0.0.1:{}", port + 2).parse().unwrap(),
            };
            port += 3;

            let mut worker_addresses = HashMap::new();
            for j in 0..workers {
                worker_addresses.insert(
                    j as WorkerId,
                    WorkerAddresses {
                        primary_to_worker: format!("127.0.0.1:{}", port).parse().unwrap(),
                        transactions: format!("127.0.0.1:{}", port + 1).parse().unwrap(),
                        worker_to_worker: format!("127.0.0.1:{}", port + 2).parse().unwrap(),
                        metrics: format!("127.0.0.1:{}", port + 3).parse().unwrap(),
                    },
                );
                port += 4;
            }

            authorities.insert(
                keypair.name,
                Authority {
                    stake: 1,
                    consensus,
                    primary,
                    workers: worker_addresses,
                },
            );
            keypairs.push(keypair);
        }

        // Match the BTreeMap order used by committee indices.
        keypairs.sort_by_key(|k| k.name);

        (Self { authorities }, keypairs)
    }

    /// Returns the number of authorities.
    pub fn size(&self) -> usize {
        self.authorities.len()
    }

    /// Return the stake of a specific authority.
    pub fn stake(&self, name: &PublicKey) -> Stake {
        self.authorities.get(name).map_or_else(|| 0, |x| x.stake)
    }

    /// Returns the stake of all authorities except `myself`.
    pub fn others_stake(&self, myself: &PublicKey) -> Vec<(PublicKey, Stake)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, authority)| (*name, authority.stake))
            .collect()
    }

    /// Returns the stake required to reach a quorum (2f+1).
    pub fn quorum_threshold(&self) -> Stake {
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        2 * total_votes / 3 + 1
    }

    /// Returns the stake required to reach availability (f+1).
    pub fn validity_threshold(&self) -> Stake {
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        total_votes.div_ceil(3)
    }

    pub fn fast_threshold(&self) -> Stake {
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        total_votes
    }

    /// Returns the consensus addresses of the target consensus node.
    pub fn consensus(&self, to: &PublicKey) -> Result<ConsensusAddresses, ConfigError> {
        self.authorities
            .get(to)
            .map(|x| x.consensus.clone())
            .ok_or(ConfigError::NotInCommittee(*to))
    }

    /// Returns the addresses of all consensus nodes except `myself`.
    pub fn others_consensus(&self, myself: &PublicKey) -> Vec<(PublicKey, ConsensusAddresses)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, authority)| (*name, authority.consensus.clone()))
            .collect()
    }

    /// Returns the primary addresses of the target primary.
    pub fn primary(&self, to: &PublicKey) -> Result<PrimaryAddresses, ConfigError> {
        self.authorities
            .get(to)
            .map(|x| x.primary.clone())
            .ok_or(ConfigError::NotInCommittee(*to))
    }

    /// Returns the addresses of all primaries except `myself`.
    pub fn others_primaries(&self, myself: &PublicKey) -> Vec<(PublicKey, PrimaryAddresses)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, authority)| (*name, authority.primary.clone()))
            .collect()
    }

    /// Returns `name`'s position in the committee's deterministic BTreeMap order.
    pub fn index_of(&self, name: &PublicKey) -> Option<usize> {
        self.authorities.keys().position(|k| k == name)
    }

    /// Returns every socket address used by `name`'s authority.
    pub fn addresses_of(&self, name: &PublicKey) -> Vec<SocketAddr> {
        let Some(a) = self.authorities.get(name) else {
            return Vec::new();
        };
        let mut out = vec![
            a.primary.primary_to_primary,
            a.primary.worker_to_primary,
            a.primary.metrics,
            a.consensus.consensus_to_consensus,
        ];
        for w in a.workers.values() {
            out.extend([
                w.primary_to_worker,
                w.transactions,
                w.worker_to_worker,
                w.metrics,
            ]);
        }
        out
    }

    /// Builds per-destination one-way latency entries for all other authorities.
    /// Returns an empty map when `myself` is not a committee member.
    pub fn latency_map(
        &self,
        myself: &PublicKey,
        table: &LatencyTable,
    ) -> HashMap<SocketAddr, Duration> {
        let mut out = HashMap::new();
        let Some(i) = self.index_of(myself) else {
            return out;
        };
        for (j, other) in self.authorities.keys().enumerate() {
            if other == myself {
                continue;
            }
            let delay = table.one_way(i, j);
            for addr in self.addresses_of(other) {
                out.insert(addr, delay);
            }
        }
        out
    }

    /// Returns the addresses of a specific worker (`id`) of a specific authority (`to`).
    pub fn worker(&self, to: &PublicKey, id: &WorkerId) -> Result<WorkerAddresses, ConfigError> {
        self.authorities
            .iter()
            .find(|(name, _)| name == &to)
            .map(|(_, authority)| authority)
            .ok_or(ConfigError::NotInCommittee(*to))?
            .workers
            .iter()
            .find(|(worker_id, _)| worker_id == &id)
            .map(|(_, worker)| worker.clone())
            .ok_or(ConfigError::NotInCommittee(*to))
    }

    /// Returns the addresses of all our workers.
    pub fn our_workers(&self, myself: &PublicKey) -> Result<Vec<WorkerAddresses>, ConfigError> {
        self.authorities
            .iter()
            .find(|(name, _)| name == &myself)
            .map(|(_, authority)| authority)
            .ok_or(ConfigError::NotInCommittee(*myself))?
            .workers
            .values()
            .cloned()
            .map(Ok)
            .collect()
    }

    /// Returns the addresses of all our workers, keyed by `WorkerId` (unlike
    /// `our_workers`, which discards the id). Used to route a message to a specific
    /// local worker.
    pub fn our_workers_by_id(
        &self,
        myself: &PublicKey,
    ) -> Result<HashMap<WorkerId, WorkerAddresses>, ConfigError> {
        self.authorities
            .get(myself)
            .map(|x| x.workers.clone())
            .ok_or(ConfigError::NotInCommittee(*myself))
    }

    /// Returns the addresses of all workers with a specific id except the ones of the authority
    /// specified by `myself`.
    pub fn others_workers(
        &self,
        myself: &PublicKey,
        id: &WorkerId,
    ) -> Vec<(PublicKey, WorkerAddresses)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .filter_map(|(name, authority)| {
                authority
                    .workers
                    .iter()
                    .find(|(worker_id, _)| worker_id == &id)
                    .map(|(_, addresses)| (*name, addresses.clone()))
            })
            .collect()
    }

    pub fn address(&self, name: &PublicKey) -> Option<SocketAddr> {
        self.authorities
            .get(name)
            .map(|x| x.consensus.consensus_to_consensus)
    }

    pub fn broadcast_addresses(&self, myself: &PublicKey) -> Vec<(PublicKey, SocketAddr)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, x)| (*name, x.consensus.consensus_to_consensus))
            .collect()
    }
}

/// Returns the destinations withheld by a local data-plane fault injector.
///
/// The first `withhold_senders` committee indices are withholding senders. Sender `i`
/// withholds from destinations `(i + 1)..=(i + n/2)` modulo `n`.
///
/// Returns `None` when withholding is disabled, `self_pk` is not a withholding sender,
/// or `self_pk` is not a committee member.
pub fn withheld_destinations(
    committee: &Committee,
    self_pk: &PublicKey,
    withhold_senders: usize,
) -> Option<HashSet<PublicKey>> {
    let n = committee.size();
    if withhold_senders == 0 || n == 0 {
        return None;
    }
    let i = committee.index_of(self_pk)?;
    if i >= withhold_senders {
        return None;
    }
    let order: Vec<PublicKey> = committee.authorities.keys().copied().collect();
    let half = n / 2;
    Some((1..=half).map(|offset| order[(i + offset) % n]).collect())
}

/// Returns whether withholding is active at `now`. An unset window means active for
/// the whole run. A set window is active on `[start, end)`.
pub fn withhold_active(window: Option<&OnceLock<(Instant, Instant)>>, now: Instant) -> bool {
    match window {
        None => true,
        Some(cell) => cell
            .get()
            .is_some_and(|&(start, end)| now >= start && now < end),
    }
}

#[derive(Serialize, Deserialize)]
pub struct KeyPair {
    /// The node's public key (and identifier).
    pub name: PublicKey,
    /// The node's secret key.
    pub secret: SecretKey,
}

impl Import for KeyPair {}
impl Export for KeyPair {}

impl KeyPair {
    pub fn new() -> Self {
        let (name, secret) = generate_production_keypair();
        Self { name, secret }
    }
}

impl Default for KeyPair {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A generated Vantage parameter file must deserialize and retain the protocol,
    /// timing, and latency settings.
    #[test]
    fn deserializes_fab_vantage_parameters_json_with_mimic_latency() {
        let json = r#"{
            "timeout_delay": 5000,
            "header_size": 32,
            "max_header_delay": 5000,
            "gc_depth": 50,
            "sync_retry_delay": 5000,
            "sync_retry_nodes": 3,
            "batch_size": 500000,
            "max_batch_delay": 20,
            "protocol": "vantage",
            "use_parallel_proposals": true,
            "k": 4,
            "use_fast_path": true,
            "fast_path_timeout": 5000,
            "use_ride_share": false,
            "car_timeout": 5000,
            "delta_ms": 150,
            "mimic_latency_ms": 100,
            "simulate_asynchrony": false,
            "asynchrony_start": 15000,
            "asynchrony_duration": 3000
        }"#;

        let mut params: Parameters =
            serde_json::from_str(json).expect("fab-generated Vantage parameters.json must parse");
        // Imported parameters are reconciled before use.
        params.reconcile_protocol();

        assert_eq!(params.protocol, Protocol::Vantage);
        assert_eq!(params.delta_ms, 150);
        assert_eq!(params.mimic_latency_ms, Some(100));
        // Omitted batching settings use the enabled default.
        assert!(params.batch_messages);
        // An omitted Vantage GC window uses its default.
        assert_eq!(params.vantage_gc_window_views, 200);
        // Vantage views and Autobahn rounds use separate parameters.
        assert_ne!(params.vantage_gc_window_views, params.gc_depth);
        assert_eq!(
            params.gc_depth, 50,
            "the Autobahn GC parameter is unchanged"
        );
        // `latency_table` is not serialized; `node run` builds it at spawn.
        assert!(params.latency_table.is_none());
        // Omitted replay settings use their defaults.
        assert!(params.reconnect_replay);
        assert_eq!(params.retry_backoff_max_ms, 2000);

        // Prove the spawn-time expansion `node run` performs yields a well-formed
        // uniform NxN table with the RTT/2 one-way convention.
        let n = 20;
        let table = LatencyTable::uniform(n, params.mimic_latency_ms.unwrap() as f64);
        assert_eq!(table.one_way(0, 0), Duration::ZERO); // diagonal
        assert_eq!(table.one_way(0, 1), Duration::from_millis(50)); // 100ms RTT / 2
        assert_eq!(table.one_way(19, 3), Duration::from_millis(50));
    }

    /// Withholding wraps at the end of committee order.
    #[test]
    fn withheld_destinations_stagger_wraps_around() {
        let (committee, keypairs) = Committee::local_benchmark(20, 1, 9000);
        let blocked = withheld_destinations(&committee, &keypairs[15].name, 16)
            .expect("index 15 is one of the first 16 withholding senders");
        let expected: HashSet<PublicKey> = [16, 17, 18, 19, 0, 1, 2, 3, 4, 5]
            .into_iter()
            .map(|idx| keypairs[idx].name)
            .collect();
        assert_eq!(blocked, expected);
    }

    /// `--withhold 0` (the default): every node gets `None`, regardless of committee
    /// size or its own position.
    #[test]
    fn withheld_destinations_zero_is_none_for_everyone() {
        let (committee, keypairs) = Committee::local_benchmark(20, 1, 9000);
        for keypair in &keypairs {
            assert!(withheld_destinations(&committee, &keypair.name, 0).is_none());
        }
    }

    /// `--withhold <nodes>`: every single node is a withholding sender.
    #[test]
    fn withheld_destinations_k_equals_n_every_sender_withholds() {
        let (committee, keypairs) = Committee::local_benchmark(20, 1, 9000);
        for keypair in &keypairs {
            assert!(withheld_destinations(&committee, &keypair.name, 20).is_some());
        }
    }

    /// Odd `n` uses `floor(n/2)`, not a rounded-up half: n=7 blocks exactly 3.
    #[test]
    fn withheld_destinations_odd_committee_floors() {
        let (committee, keypairs) = Committee::local_benchmark(7, 1, 9000);
        let blocked = withheld_destinations(&committee, &keypairs[0].name, 1)
            .expect("index 0 is the sole withholding sender");
        let expected: HashSet<PublicKey> = [1, 2, 3]
            .into_iter()
            .map(|idx| keypairs[idx].name)
            .collect();
        assert_eq!(blocked, expected);
    }

    /// A node past the first `withhold_senders` indices does not withhold at all, even
    /// though other nodes do.
    #[test]
    fn withheld_destinations_non_sender_index_is_none() {
        let (committee, keypairs) = Committee::local_benchmark(20, 1, 9000);
        assert!(withheld_destinations(&committee, &keypairs[2].name, 3).is_some());
        assert!(withheld_destinations(&committee, &keypairs[3].name, 3).is_none());
    }

    /// No configured window keeps withholding active.
    #[test]
    fn withhold_active_no_window_is_always_active() {
        let now = Instant::now();
        assert!(withhold_active(None, now));
        assert!(withhold_active(None, now + Duration::from_secs(1_000_000)));
    }

    /// A configured but unarmed window is inactive.
    #[test]
    fn withhold_active_configured_unarmed_is_inactive() {
        let cell: OnceLock<(Instant, Instant)> = OnceLock::new();
        assert!(!withhold_active(Some(&cell), Instant::now()));
    }

    /// A configured window is active on `[start, end)`.
    #[test]
    fn withhold_active_armed_inside_and_outside_window() {
        let cell: OnceLock<(Instant, Instant)> = OnceLock::new();
        let base = Instant::now();
        let start = base + Duration::from_secs(10);
        let end = base + Duration::from_secs(20);
        cell.set((start, end)).unwrap();

        assert!(!withhold_active(Some(&cell), base + Duration::from_secs(5))); // before start
        assert!(withhold_active(Some(&cell), start)); // at start (inclusive)
        assert!(withhold_active(Some(&cell), base + Duration::from_secs(15))); // inside
        assert!(!withhold_active(Some(&cell), end)); // at end (exclusive)
        assert!(!withhold_active(
            Some(&cell),
            base + Duration::from_secs(25)
        )); // after end
    }

    /// Replay is enabled and uses the configured default backoff ceiling.
    #[test]
    fn reconnect_replay_and_retry_backoff_default_to_todays_behavior() {
        let params = Parameters::default();
        assert!(params.reconnect_replay);
        assert_eq!(params.retry_backoff_max_ms, 2000);
    }

    #[test]
    fn sequence_recovery_defaults_allow_bounded_bursts() {
        let params = Parameters::default();
        assert_eq!(params.sequence_sync_min_gap_views, 100);
        assert_eq!(params.sequence_sync_shed_gap_views, 300);
        assert_eq!(params.sequence_sync_request_timeout_ms, 1_000);
        assert_eq!(params.sequence_sync_inbound_capacity, 1_024);
    }

    #[test]
    fn echo_availability_claims_default_on() {
        let defaults = Parameters::default();
        assert!(defaults.ack_watermarks);
        assert!(defaults.echo_avail_claims);

        let encoded = serde_json::to_value(defaults).expect("parameters serialize");
        let mut object = encoded
            .as_object()
            .expect("parameters are an object")
            .clone();
        object.remove("echo_avail_claims");
        let decoded: Parameters =
            serde_json::from_value(object.into()).expect("legacy parameters deserialize");
        assert!(decoded.echo_avail_claims);
    }
}

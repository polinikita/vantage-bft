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
            // Truncate before writing the new serialization.
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
    /// Autobahn with optimistic tips.
    #[default]
    AutobahnOptimistic,
    /// Autobahn with certified tips only.
    AutobahnSeamless,
    /// Signature-free AGB.
    Vantage,
    /// Simple-IT cut consensus.
    SimpleIt,
    /// Simple-IT with Bracha RBC.
    SimpleItBracha,
}

impl Protocol {
    /// Returns the Autobahn optimistic-tip setting, or `None` otherwise.
    pub fn implied_optimistic_tips(&self) -> Option<bool> {
        match self {
            Protocol::AutobahnOptimistic => Some(true),
            Protocol::AutobahnSeamless => Some(false),
            Protocol::Vantage => None,
            Protocol::SimpleIt => None,
            Protocol::SimpleItBracha => None,
        }
    }

    /// Autobahn's optimistic variant uses all-to-all vote dissemination.
    pub fn implies_all_to_all(&self) -> bool {
        matches!(self, Protocol::AutobahnOptimistic)
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

/// Default for `use_optimistic_tips` when omitted.
fn default_use_optimistic_tips() -> bool {
    true
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Parameters {
    /// Consensus timeout, in milliseconds.
    pub timeout_delay: u64,
    /// Preferred header size, in bytes.
    pub header_size: usize,
    /// Maximum header delay, in milliseconds.
    pub max_header_delay: u64,
    /// Garbage-collection depth, in rounds.
    pub gc_depth: u64,
    /// Sync retry delay, in milliseconds.
    pub sync_retry_delay: u64,
    /// Number of random peers used for sync retries.
    pub sync_retry_nodes: usize,
    /// Preferred worker batch size, in bytes.
    pub batch_size: usize,
    /// Maximum worker batch delay, in milliseconds.
    pub max_batch_delay: u64,

    // Autobahn parameters.
    #[serde(default = "default_use_optimistic_tips")]
    pub use_optimistic_tips: bool, // Default true.

    pub use_parallel_proposals: bool, // Default true.
    pub k: u64,                       // Maximum open consensus instances.

    pub use_fast_path: bool, // Autobahn only; default true.
    pub fast_path_timeout: u64,

    pub use_ride_share: bool,
    pub car_timeout: u64,

    /// Use all-to-all vote dissemination on the external-consensus path.
    #[serde(default)]
    pub all_to_all: bool,

    // Asynchrony parameters.
    pub simulate_asynchrony: bool,
    pub asynchrony_start: u64,
    pub asynchrony_duration: u64,

    /// Consensus protocol. Overrides `use_optimistic_tips`.
    #[serde(default)]
    pub protocol: Protocol,

    /// Transaction-generation mode, when known.
    #[serde(default)]
    pub tx_mode: Option<String>,

    /// Maximum payload entries in one Vantage data block.
    #[serde(default = "default_max_block_payload")]
    pub max_block_payload: usize,

    /// Vantage AGB base delay unit, in milliseconds.
    #[serde(default = "default_delta_ms")]
    pub delta_ms: u64,

    /// Epoch-millisecond start time for commit metrics. `None` counts all transactions.
    #[serde(default)]
    pub metrics_active_at_ms: Option<u64>,

    /// Vantage views retained behind the resolved prefix before garbage collection.
    #[serde(default = "default_vantage_gc_window_views")]
    pub vantage_gc_window_views: u64,

    /// Simple-IT rounds retained before pruning.
    #[serde(default = "default_simpleit_gc_window_rounds")]
    pub simpleit_gc_window_rounds: u64,

    /// Enables per-author availability watermarks.
    #[serde(default = "default_ack_watermarks")]
    pub ack_watermarks: bool,
    /// Availability watermark period, in milliseconds.
    #[serde(default = "default_ack_watermark_period_ms")]
    pub ack_watermark_period_ms: u64,

    /// Enables digest-named AGB statements and body fetches.
    #[serde(default = "default_digest_statements")]
    pub digest_statements: bool,

    /// Uses one-byte committee indices on the Vantage primary wire.
    #[serde(default = "default_vantage_compact_ids")]
    pub vantage_compact_ids: bool,

    /// Enables sequence checkpoints and state synchronization.
    #[serde(default = "default_sequence_checkpoints")]
    pub sequence_checkpoints: bool,

    /// Checkpoint boundary interval in terminally processed views. Zero is treated as 1.
    #[serde(default = "default_sequence_checkpoint_interval_views")]
    pub sequence_checkpoint_interval_views: u64,

    /// Sequence announcement timer period, in milliseconds.
    #[serde(default = "default_sequence_announce_period_ms")]
    pub sequence_announce_period_ms: u64,

    /// Repeat interval for an unchanged sequence boundary, in milliseconds.
    #[serde(default = "default_sequence_announce_repeat_ms")]
    pub sequence_announce_repeat_ms: u64,

    /// Sequence records per served chunk.
    #[serde(default = "default_sequence_sync_chunk_records")]
    pub sequence_sync_chunk_records: usize,

    /// Maximum terminal outcomes per served range.
    #[serde(default = "default_sequence_sync_chunk_outcomes")]
    pub sequence_sync_chunk_outcomes: usize,

    /// Maximum manifest references per served outcome range.
    #[serde(default = "default_sequence_sync_chunk_outcome_items")]
    pub sequence_sync_chunk_outcome_items: usize,

    /// Delta digests per served chunk. Each digest is 32 bytes.
    #[serde(default = "default_sequence_sync_chunk_digests")]
    pub sequence_sync_chunk_digests: usize,

    /// Sequence gap that starts state synchronization, in views.
    #[serde(default = "default_sequence_sync_min_gap_views")]
    pub sequence_sync_min_gap_views: u64,

    /// Sequence gap that sheds ordinary consensus and control traffic.
    #[serde(default = "default_sequence_sync_shed_gap_views")]
    pub sequence_sync_shed_gap_views: u64,

    /// Sequence gap that re-arms state synchronization after recovery.
    #[serde(default = "default_sequence_sync_rearm_gap_views")]
    pub sequence_sync_rearm_gap_views: u64,

    /// Concurrent sources queried for one sequence chunk.
    #[serde(default = "default_sequence_sync_max_sources")]
    pub sequence_sync_max_sources: usize,

    /// Sequence source request timeout, in milliseconds.
    #[serde(default = "default_sequence_sync_request_timeout_ms")]
    pub sequence_sync_request_timeout_ms: u64,

    /// State-sync response queue capacity. Overflow drops the newest frame.
    #[serde(default = "default_sequence_sync_inbound_capacity")]
    pub sequence_sync_inbound_capacity: usize,

    /// Verified target views admitted to the block-fetch window.
    #[serde(default = "default_sequence_install_window_views")]
    pub sequence_install_window_views: usize,

    /// Deprecated compatibility setting; install admission uses
    /// `sequence_install_window_views`.
    #[serde(default = "default_sequence_install_settle_ceiling")]
    pub sequence_install_settle_ceiling: usize,

    /// Enables installation of verified checkpoint state.
    #[serde(default = "default_sequence_install_enabled")]
    pub sequence_install_enabled: bool,

    /// Views applied per install pass.
    #[serde(default = "default_sequence_install_views_per_tick")]
    pub sequence_install_views_per_tick: usize,

    /// Block digests emitted per install pass.
    #[serde(default = "default_sequence_install_digests_per_tick")]
    pub sequence_install_digests_per_tick: usize,

    /// Carries positional availability bits on AGB echoes. Requires `ack_watermarks`.
    #[serde(default = "default_echo_avail_claims")]
    pub echo_avail_claims: bool,

    /// Optional local-run one-way latency table. This field is not serialized.
    #[serde(skip)]
    pub latency_table: Option<Arc<LatencyTable>>,

    /// Optional uniform RTT override, in milliseconds. Serialized in parameters.
    #[serde(default)]
    pub mimic_latency_ms: Option<u64>,

    /// Enables per-peer outbound message batching. Client traffic is unbatched.
    #[serde(default = "default_batch_messages")]
    pub batch_messages: bool,
    /// Batch flush size cap, in bytes.
    #[serde(default = "default_batch_max_bytes")]
    pub batch_max_bytes: usize,
    /// Batch flush delay, in milliseconds.
    #[serde(default = "default_batch_max_delay_ms")]
    pub batch_max_delay_ms: u64,

    /// Number of staggered payload-withholding senders. Zero disables this
    /// legacy selector unless `withhold_publishers` names explicit senders.
    #[serde(default)]
    pub withhold_senders: usize,

    /// Optional explicit payload-withholding publishers. This is the remote-
    /// deployment form of `withhold_senders`: it binds the fault to validator
    /// identities instead of the committee's public-key sort order.
    #[serde(default)]
    pub withhold_publishers: Vec<PublicKey>,

    /// Destinations each withholding sender excludes. `None` keeps the legacy
    /// half-committee width; values below `n - quorum` keep the withheld blocks
    /// able to reach the availability quorum, exercising the mixed-grade path.
    #[serde(default)]
    pub withhold_count: Option<usize>,

    /// Committee-index stride between destinations omitted by consecutive
    /// withholding senders. A coprime stride spreads missing payloads across
    /// the committee so every correct leader holds some tips and lacks others.
    #[serde(default = "default_withhold_stride")]
    pub withhold_stride: usize,

    /// Optional fixed destination set excluded by every withholding sender.
    /// An empty set keeps the staggered mapping selected by `withhold_count`.
    /// Repair and control traffic are unaffected unless `withhold_repair` is
    /// set for the selected Byzantine publishers.
    #[serde(default)]
    pub withhold_receivers: Vec<PublicKey>,

    /// Benchmark-only Byzantine behavior: selected publishers ignore lane
    /// header, certificate, and batch repair requests after narrowcasting the
    /// original publication.
    #[serde(default)]
    pub withhold_repair: bool,

    /// Whether permanent withholding also suppresses original lane headers.
    /// Disable this to drop only the heavy worker batches while retaining the
    /// metadata path needed for a load-scaling repair experiment.
    #[serde(default = "default_withhold_headers")]
    pub withhold_headers: bool,

    /// Byzantine authors whose original header publication is delayed to a
    /// fixed receiver subset. Repair traffic is never delayed.
    #[serde(default)]
    pub late_header_publishers: Vec<PublicKey>,

    /// Honest receivers to which selected Byzantine authors publish headers late.
    #[serde(default)]
    pub late_header_receivers: Vec<PublicKey>,

    /// Additional one-way delay applied to the selected original publications.
    #[serde(default)]
    pub late_header_delay_ms: u64,

    /// Withholding start offset, in milliseconds. `None` enables it for the full run.
    #[serde(default)]
    pub withhold_at_ms: Option<u64>,
    /// Withholding duration, in milliseconds.
    #[serde(default = "default_withhold_for_ms")]
    pub withhold_for_ms: u64,
    /// In-process withholding window state. This field is not serialized.
    #[serde(skip)]
    pub withhold_window: Option<Arc<OnceLock<(Instant, Instant)>>>,

    /// Lane-resume gap check period, in milliseconds.
    #[serde(default = "default_resume_check_period_ms")]
    pub resume_check_period_ms: u64,
    /// Minimum spacing between resume requests, in milliseconds.
    #[serde(default = "default_resume_backoff_ms")]
    pub resume_backoff_ms: u64,
    /// Maximum blocks per lane-resume batch.
    #[serde(default = "default_resume_max_concurrent")]
    pub resume_max_concurrent: usize,

    #[serde(default = "default_resume_batch")]
    pub resume_batch: u64,

    /// Queue depth at which volatile sends are shed. Zero disables shedding.
    #[serde(default = "default_volatile_soft_cap")]
    pub volatile_soft_cap: usize,

    /// Enables reconnect replay for volatile messages.
    #[serde(default = "default_reconnect_replay")]
    pub reconnect_replay: bool,

    /// Reconnect backoff ceiling, in milliseconds.
    #[serde(default = "default_retry_backoff_max_ms")]
    pub retry_backoff_max_ms: u64,

    /// Views of one-shot messages retained behind the local watermark.
    #[serde(default = "default_replay_history_views")]
    pub replay_history_views: u64,
    /// Resume payload size per chunk, in bytes.
    #[serde(default = "default_replay_chunk_bytes")]
    pub replay_chunk_bytes: usize,
    /// Delay between replay rotations, in milliseconds.
    #[serde(default = "default_replay_chunk_interval_ms")]
    pub replay_chunk_interval_ms: u64,
    /// Per-peer replay byte budget per backoff window.
    #[serde(default = "default_replay_serve_max_bytes")]
    pub replay_serve_max_bytes: usize,
    /// Outbox byte cap. Eviction preserves the newest key.
    #[serde(default = "default_outbox_max_bytes")]
    pub outbox_max_bytes: usize,
    /// Replay episode and in-flight stream lifetime, in milliseconds.
    #[serde(default = "default_replay_episode_max_ms")]
    pub replay_episode_max_ms: u64,
}

fn default_batch_messages() -> bool {
    true
}

/// Enables compressed availability by default.
fn default_ack_watermarks() -> bool {
    true
}

fn default_echo_avail_claims() -> bool {
    true
}

/// Enables digest statements by default.
fn default_digest_statements() -> bool {
    true
}

fn default_vantage_compact_ids() -> bool {
    true
}

/// Default sequence-checkpoint setting.
fn default_sequence_checkpoints() -> bool {
    true
}

/// Default checkpoint interval, in views.
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

/// Secondary bound for `Skip` outcomes.
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
    300
}

/// Minimum number of sequence sources.
fn default_sequence_sync_max_sources() -> usize {
    3
}

fn default_sequence_sync_request_timeout_ms() -> u64 {
    1_000
}

fn default_sequence_sync_inbound_capacity() -> usize {
    1_024
}

/// Default install window, in views.
fn default_sequence_install_window_views() -> usize {
    64
}

/// Default install settle ceiling, in views.
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
    // Covers the largest configured one-way delay.
    200
}

/// Default Vantage retention window, in views.
fn default_vantage_gc_window_views() -> u64 {
    200
}

/// Default Simple-IT retention window, in rounds.
fn default_simpleit_gc_window_rounds() -> u64 {
    50
}

/// Default availability-watermark period, in milliseconds.
fn default_ack_watermark_period_ms() -> u64 {
    50
}

/// Default withholding duration, in milliseconds.
fn default_withhold_for_ms() -> u64 {
    30_000
}

fn default_withhold_headers() -> bool {
    true
}

fn default_withhold_stride() -> usize {
    1
}

/// Default lane-resume check period, in milliseconds.
fn default_resume_check_period_ms() -> u64 {
    1_000
}

/// Default lane-resume backoff, in milliseconds.
fn default_resume_backoff_ms() -> u64 {
    4_000
}

/// Default lane-resume batch size.
fn default_resume_batch() -> u64 {
    64
}

/// Default volatile queue soft cap.
fn default_volatile_soft_cap() -> usize {
    1_024
}

/// Default reconnect-replay setting.
fn default_reconnect_replay() -> bool {
    true
}

/// Default reconnect backoff ceiling in milliseconds.
fn default_retry_backoff_max_ms() -> u64 {
    2_000
}

/// Default concurrent resume episodes.
fn default_resume_max_concurrent() -> usize {
    8
}

fn default_replay_history_views() -> u64 {
    512
}

/// Default replay chunk size, in bytes.
fn default_replay_chunk_bytes() -> usize {
    65_536
}

/// Default replay rotation interval, in milliseconds.
fn default_replay_chunk_interval_ms() -> u64 {
    5
}

/// Default replay byte budget.
fn default_replay_serve_max_bytes() -> usize {
    8 << 20
}

/// Default outbox size, in bytes.
fn default_outbox_max_bytes() -> usize {
    64 << 20
}

/// Default replay lifetime, in milliseconds.
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

/// Optional fixed n × n one-way latency table in committee order.
#[derive(Clone, Debug)]
pub struct LatencyTable {
    /// `one_way_ms[i][j]` is the one-way latency from index `i` to `j`, in milliseconds.
    one_way_ms: Vec<Vec<f64>>,
}

impl LatencyTable {
    /// Builds a uniform table. Off-diagonal entries are `rtt_ms / 2`; diagonal entries are zero.
    pub fn uniform(n: usize, rtt_ms: f64) -> Self {
        let one_way = rtt_ms / 2.0;
        let mut t = vec![vec![one_way; n]; n];
        for (i, row) in t.iter_mut().enumerate() {
            row[i] = 0.0;
        }
        Self { one_way_ms: t }
    }

    /// Parses an n × n comma-separated RTT matrix and stores one-way latencies.
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

    /// Builds a table by mapping committee indices cyclically to the AWS RTT matrix.
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

    /// Returns one-way latency between committee-order indices, or zero if out of range.
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
            vantage_compact_ids: default_vantage_compact_ids(),
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

            // Autobahn defaults.
            use_optimistic_tips: true,
            use_parallel_proposals: true,
            k: 4,
            use_fast_path: true,
            fast_path_timeout: 500,
            use_ride_share: false,
            car_timeout: 2000,
            all_to_all: true,

            // Asynchrony defaults.
            simulate_asynchrony: false,
            asynchrony_start: 20_000,    // 20 seconds.
            asynchrony_duration: 10_000, // 10 seconds.

            protocol: Protocol::default(),

            tx_mode: None,
            max_block_payload: default_max_block_payload(),
            delta_ms: default_delta_ms(),
            // Count every observation by default.
            metrics_active_at_ms: None,
            latency_table: None,
            mimic_latency_ms: None,
            batch_messages: true,
            batch_max_bytes: default_batch_max_bytes(),
            batch_max_delay_ms: default_batch_max_delay_ms(),
            withhold_senders: 0,
            withhold_publishers: Vec::new(),
            withhold_count: None,
            withhold_stride: default_withhold_stride(),
            withhold_receivers: Vec::new(),
            withhold_repair: false,
            withhold_headers: default_withhold_headers(),
            late_header_publishers: Vec::new(),
            late_header_receivers: Vec::new(),
            late_header_delay_ms: 0,
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
    /// Reconcile protocol-derived mode flags with the selected protocol.
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
        if self.protocol.implies_all_to_all() && !self.all_to_all {
            warn!(
                "all_to_all=false is inconsistent with protocol {:?}; \
                 protocol wins, using all_to_all=true",
                self.protocol
            );
            self.all_to_all = true;
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
            "Vantage one-byte committee identifiers enabled? {}",
            self.vantage_compact_ids
        );
        info!(
            "Lane resume: check period {} ms, backoff {} ms, batch {} blocks",
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
        if self.withhold_senders > 0 || !self.withhold_publishers.is_empty() {
            let publishers = if self.withhold_publishers.is_empty() {
                format!("first {} node(s)", self.withhold_senders)
            } else {
                format!("{} fixed node(s)", self.withhold_publishers.len())
            };
            let traffic = if self.withhold_headers {
                "payload batches and lane headers"
            } else {
                "payload batches only"
            };
            let width = if self.withhold_receivers.is_empty() {
                match self.withhold_count {
                    Some(count) => format!(
                        "{count} staggered peer(s), committee stride {}",
                        self.withhold_stride
                    ),
                    None => format!(
                        "a staggered half of the committee, committee stride {}",
                        self.withhold_stride
                    ),
                }
            } else {
                format!("{} fixed peer(s)", self.withhold_receivers.len())
            };
            match self.withhold_at_ms {
                Some(at) => info!(
                    "Data-plane withholding: {} withhold {} from {}, active [{}, {}) ms \
                     after start",
                    publishers,
                    traffic,
                    width,
                    at,
                    at + self.withhold_for_ms
                ),
                None => info!(
                    "Data-plane withholding: {} withhold {} from {}",
                    publishers, traffic, width
                ),
            }
            if self.withhold_repair {
                info!("Selected Byzantine publishers suppress all lane repair responses");
            }
        }
        if !self.late_header_publishers.is_empty() {
            info!(
                "Late original-header publication: {} Byzantine publisher(s), {} receiver(s), \
                 additional one-way delay {} ms",
                self.late_header_publishers.len(),
                self.late_header_receivers.len(),
                self.late_header_delay_ms
            );
        }
    }

    /// Validate the finite-delay Byzantine header-publication experiment.
    pub fn validate_header_faults(&self, committee: &Committee) -> Result<(), String> {
        let n = committee.size();
        if self.withhold_senders > n {
            return Err(format!(
                "withholding sender count {} exceeds committee size {n}",
                self.withhold_senders
            ));
        }
        if self.withhold_count.is_some_and(|count| count >= n) {
            return Err("withholding destination count must be below committee size".to_string());
        }
        if self.withhold_stride == 0 {
            return Err("withholding stride must be greater than zero".to_string());
        }
        if self.withhold_receivers.is_empty()
            && (self.withhold_senders > 0 || !self.withhold_publishers.is_empty())
            && gcd(self.withhold_stride, n) != 1
        {
            return Err(format!(
                "withholding stride {} must be coprime with committee size {n}",
                self.withhold_stride
            ));
        }
        if self.withhold_repair && self.withhold_senders == 0 && self.withhold_publishers.is_empty()
        {
            return Err("repair suppression requires withholding publishers".to_string());
        }
        if self.withhold_senders > 0 && !self.withhold_publishers.is_empty() {
            return Err(
                "staggered withholding senders and fixed withholding publishers are mutually exclusive"
                    .to_string(),
            );
        }
        let fixed_publishers: HashSet<_> = self.withhold_publishers.iter().copied().collect();
        if fixed_publishers.len() != self.withhold_publishers.len() {
            return Err("fixed withholding publisher list contains duplicates".to_string());
        }
        if let Some(key) = fixed_publishers
            .iter()
            .find(|key| !committee.authorities.contains_key(key))
        {
            return Err(format!(
                "fixed withholding publisher {key} is not in the committee"
            ));
        }
        if !self.withhold_receivers.is_empty() {
            if self.withhold_senders == 0 && fixed_publishers.is_empty() {
                return Err(
                    "fixed withholding receivers require withholding publishers".to_string()
                );
            }
            let receivers: HashSet<_> = self.withhold_receivers.iter().copied().collect();
            if receivers.len() != self.withhold_receivers.len() {
                return Err("fixed withholding receiver list contains duplicates".to_string());
            }
            if let Some(key) = receivers
                .iter()
                .find(|key| !committee.authorities.contains_key(key))
            {
                return Err(format!(
                    "fixed withholding receiver {key} is not in the committee"
                ));
            }
            let publishers: HashSet<_> = if fixed_publishers.is_empty() {
                committee
                    .authorities
                    .keys()
                    .take(self.withhold_senders)
                    .copied()
                    .collect()
            } else {
                fixed_publishers.clone()
            };
            if !publishers.is_disjoint(&receivers) {
                return Err(
                    "fixed withholding publishers and receivers must be disjoint".to_string(),
                );
            }
        }
        let publishers_empty = self.late_header_publishers.is_empty();
        let receivers_empty = self.late_header_receivers.is_empty();
        if publishers_empty && receivers_empty {
            return Ok(());
        }
        if publishers_empty != receivers_empty {
            return Err(
                "late-header publishers and receivers must either both be empty or both be set"
                    .to_string(),
            );
        }
        if self.late_header_delay_ms == 0 {
            return Err("late-header delay must be greater than zero".to_string());
        }
        if self.withhold_senders > 0 || !self.withhold_publishers.is_empty() {
            return Err(
                "finite late-header publication cannot be combined with permanent withholding"
                    .to_string(),
            );
        }

        let publishers: HashSet<_> = self.late_header_publishers.iter().copied().collect();
        let receivers: HashSet<_> = self.late_header_receivers.iter().copied().collect();
        if publishers.len() != self.late_header_publishers.len() {
            return Err("late-header publisher list contains duplicates".to_string());
        }
        if receivers.len() != self.late_header_receivers.len() {
            return Err("late-header receiver list contains duplicates".to_string());
        }
        if !publishers.is_disjoint(&receivers) {
            return Err("late-header publishers and receivers must be disjoint".to_string());
        }
        if let Some(key) = publishers
            .iter()
            .chain(receivers.iter())
            .find(|key| !committee.authorities.contains_key(key))
        {
            return Err(format!("late-header node {key} is not in the committee"));
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ConsensusAddresses {
    /// Consensus listener address.
    pub consensus_to_consensus: SocketAddr,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PrimaryAddresses {
    /// Primary listener address.
    pub primary_to_primary: SocketAddr,
    /// Worker-to-primary listener address.
    pub worker_to_primary: SocketAddr,
    /// Prometheus metrics address.
    pub metrics: SocketAddr,
}

#[derive(Clone, Serialize, Deserialize, Eq, Hash, PartialEq)]
pub struct WorkerAddresses {
    /// Client transaction listener address.
    pub transactions: SocketAddr,
    /// Worker-to-worker listener address.
    pub worker_to_worker: SocketAddr,
    /// Primary-to-worker listener address.
    pub primary_to_worker: SocketAddr,
    /// Prometheus metrics address.
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

    /// Generates an in-memory committee for `node local-benchmark`.
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

        // Match committee index order.
        keypairs.sort_by_key(|k| k.name);

        (Self { authorities }, keypairs)
    }

    /// Returns the number of authorities.
    pub fn size(&self) -> usize {
        self.authorities.len()
    }

    /// Returns the stake of one authority.
    pub fn stake(&self, name: &PublicKey) -> Stake {
        self.authorities.get(name).map_or_else(|| 0, |x| x.stake)
    }

    /// Returns stakes excluding `myself`.
    pub fn others_stake(&self, myself: &PublicKey) -> Vec<(PublicKey, Stake)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, authority)| (*name, authority.stake))
            .collect()
    }

    /// Returns the quorum stake (2f+1).
    pub fn quorum_threshold(&self) -> Stake {
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        2 * total_votes / 3 + 1
    }

    /// Returns the availability stake (f+1).
    pub fn validity_threshold(&self) -> Stake {
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        total_votes.div_ceil(3)
    }

    pub fn fast_threshold(&self) -> Stake {
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        total_votes
    }

    /// Returns the target consensus address.
    pub fn consensus(&self, to: &PublicKey) -> Result<ConsensusAddresses, ConfigError> {
        self.authorities
            .get(to)
            .map(|x| x.consensus.clone())
            .ok_or(ConfigError::NotInCommittee(*to))
    }

    /// Returns consensus addresses excluding `myself`.
    pub fn others_consensus(&self, myself: &PublicKey) -> Vec<(PublicKey, ConsensusAddresses)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, authority)| (*name, authority.consensus.clone()))
            .collect()
    }

    /// Returns the target primary addresses.
    pub fn primary(&self, to: &PublicKey) -> Result<PrimaryAddresses, ConfigError> {
        self.authorities
            .get(to)
            .map(|x| x.primary.clone())
            .ok_or(ConfigError::NotInCommittee(*to))
    }

    /// Returns primary addresses excluding `myself`.
    pub fn others_primaries(&self, myself: &PublicKey) -> Vec<(PublicKey, PrimaryAddresses)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, authority)| (*name, authority.primary.clone()))
            .collect()
    }

    /// Returns `name`'s deterministic committee index.
    pub fn index_of(&self, name: &PublicKey) -> Option<usize> {
        self.authorities.keys().position(|k| k == name)
    }

    /// Returns every socket address for `name`.
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

    /// Builds one-way latency entries for every other authority.
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

    /// Returns worker `id`'s addresses for authority `to`.
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

    /// Returns all worker addresses for `myself`.
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

    /// Returns local worker addresses keyed by `WorkerId`.
    pub fn our_workers_by_id(
        &self,
        myself: &PublicKey,
    ) -> Result<HashMap<WorkerId, WorkerAddresses>, ConfigError> {
        self.authorities
            .get(myself)
            .map(|x| x.workers.clone())
            .ok_or(ConfigError::NotInCommittee(*myself))
    }

    /// Returns worker `id` addresses excluding `myself`.
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

/// Returns destinations withheld by the local data-plane injector.
///
/// Sender `i` withholds destinations `i + stride, ..., i + count*stride`
/// modulo `n`;
/// `count = None` keeps the legacy half-committee width `n/2`.
pub fn withheld_destinations(
    committee: &Committee,
    self_pk: &PublicKey,
    withhold_senders: usize,
    fixed_publishers: &[PublicKey],
    count: Option<usize>,
    stride: usize,
    fixed_receivers: &[PublicKey],
) -> Option<HashSet<PublicKey>> {
    let n = committee.size();
    if (withhold_senders == 0 && fixed_publishers.is_empty()) || n == 0 {
        return None;
    }
    let i = committee.index_of(self_pk)?;
    let selected = if fixed_publishers.is_empty() {
        i < withhold_senders
    } else {
        fixed_publishers.contains(self_pk)
    };
    if !selected {
        return None;
    }
    if !fixed_receivers.is_empty() {
        return Some(
            fixed_receivers
                .iter()
                .copied()
                .filter(|key| key != self_pk && committee.authorities.contains_key(key))
                .collect(),
        );
    }
    let order: Vec<PublicKey> = committee.authorities.keys().copied().collect();
    let width = count.unwrap_or(n / 2).min(n.saturating_sub(1));
    Some(
        (1..=width)
            .map(|offset| order[(i + offset * stride) % n])
            .collect(),
    )
}

fn gcd(mut a: usize, mut b: usize) -> usize {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

/// Returns the receivers whose original headers the local Byzantine publisher
/// delays. An honest/non-selected publisher returns `None`.
pub fn late_header_destinations(
    committee: &Committee,
    self_pk: &PublicKey,
    publishers: &[PublicKey],
    receivers: &[PublicKey],
) -> Option<HashSet<PublicKey>> {
    if !publishers.contains(self_pk) {
        return None;
    }
    let destinations: HashSet<_> = receivers
        .iter()
        .copied()
        .filter(|key| key != self_pk && committee.authorities.contains_key(key))
        .collect();
    (!destinations.is_empty()).then_some(destinations)
}

/// Returns whether withholding is active at `now`. An unset window is always active;
/// a set window is active on `[start, end)`.
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

    #[test]
    fn optimistic_autobahn_implies_all_to_all() {
        let mut optimistic = Parameters {
            protocol: Protocol::AutobahnOptimistic,
            all_to_all: false,
            ..Parameters::default()
        };
        optimistic.reconcile_protocol();
        assert!(optimistic.all_to_all);

        let mut seamless = Parameters {
            protocol: Protocol::AutobahnSeamless,
            all_to_all: false,
            ..Parameters::default()
        };
        seamless.reconcile_protocol();
        assert!(!seamless.all_to_all);
    }

    /// Generated Vantage parameters retain protocol and latency settings.
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
        params.reconcile_protocol();

        assert_eq!(params.protocol, Protocol::Vantage);
        assert_eq!(params.delta_ms, 150);
        assert_eq!(params.mimic_latency_ms, Some(100));
        assert!(params.batch_messages);
        assert_eq!(params.vantage_gc_window_views, 200);
        assert_ne!(params.vantage_gc_window_views, params.gc_depth);
        assert_eq!(
            params.gc_depth, 50,
            "the Autobahn GC parameter is unchanged"
        );
        assert!(params.latency_table.is_none());
        assert!(params.reconnect_replay);
        assert_eq!(params.retry_backoff_max_ms, 2000);
        assert!(params.withhold_headers);

        let n = 20;
        let table = LatencyTable::uniform(n, params.mimic_latency_ms.unwrap() as f64);
        assert_eq!(table.one_way(0, 0), Duration::ZERO);
        assert_eq!(table.one_way(0, 1), Duration::from_millis(50));
        assert_eq!(table.one_way(19, 3), Duration::from_millis(50));
    }

    /// Withholding wraps at the end of committee order.
    #[test]
    fn withheld_destinations_stagger_wraps_around() {
        let (committee, keypairs) = Committee::local_benchmark(20, 1, 9000);
        let blocked = withheld_destinations(&committee, &keypairs[15].name, 16, &[], None, 1, &[])
            .expect("index 15 is one of the first 16 withholding senders");
        let expected: HashSet<PublicKey> = [16, 17, 18, 19, 0, 1, 2, 3, 4, 5]
            .into_iter()
            .map(|idx| keypairs[idx].name)
            .collect();
        assert_eq!(blocked, expected);
    }

    /// Zero withholding disables the injector for every node.
    #[test]
    fn withheld_destinations_zero_is_none_for_everyone() {
        let (committee, keypairs) = Committee::local_benchmark(20, 1, 9000);
        for keypair in &keypairs {
            assert!(
                withheld_destinations(&committee, &keypair.name, 0, &[], None, 1, &[]).is_none()
            );
        }
    }

    /// Withholding count equal to committee size makes every node a sender.
    #[test]
    fn withheld_destinations_k_equals_n_every_sender_withholds() {
        let (committee, keypairs) = Committee::local_benchmark(20, 1, 9000);
        for keypair in &keypairs {
            assert!(
                withheld_destinations(&committee, &keypair.name, 20, &[], None, 1, &[]).is_some()
            );
        }
    }

    /// Odd committees withhold from `floor(n/2)` destinations.
    #[test]
    fn withheld_destinations_odd_committee_floors() {
        let (committee, keypairs) = Committee::local_benchmark(7, 1, 9000);
        let blocked = withheld_destinations(&committee, &keypairs[0].name, 1, &[], None, 1, &[])
            .expect("index 0 is the sole withholding sender");
        let expected: HashSet<PublicKey> = [1, 2, 3]
            .into_iter()
            .map(|idx| keypairs[idx].name)
            .collect();
        assert_eq!(blocked, expected);
    }

    /// Nodes outside the sender prefix do not withhold.
    #[test]
    fn withheld_destinations_non_sender_index_is_none() {
        let (committee, keypairs) = Committee::local_benchmark(20, 1, 9000);
        assert!(
            withheld_destinations(&committee, &keypairs[2].name, 3, &[], None, 1, &[]).is_some()
        );
        assert!(
            withheld_destinations(&committee, &keypairs[3].name, 3, &[], None, 1, &[]).is_none()
        );
    }

    #[test]
    fn fixed_withholding_concentrates_every_sender_on_one_receiver_set() {
        let (committee, keypairs) = Committee::local_benchmark(10, 1, 9000);
        let receivers: Vec<_> = keypairs[3..6].iter().map(|keypair| keypair.name).collect();
        let expected: HashSet<_> = receivers.iter().copied().collect();

        for sender in &keypairs[..3] {
            assert_eq!(
                withheld_destinations(&committee, &sender.name, 3, &[], Some(3), 1, &receivers),
                Some(expected.clone())
            );
        }
        assert!(withheld_destinations(
            &committee,
            &keypairs[3].name,
            3,
            &[],
            Some(3),
            1,
            &receivers,
        )
        .is_none());
    }

    #[test]
    fn fixed_withholding_publishers_bind_the_fault_to_explicit_identities() {
        let (committee, keypairs) = Committee::local_benchmark(10, 1, 9000);
        let publishers: Vec<_> = keypairs[..3].iter().map(|keypair| keypair.name).collect();
        let receivers: Vec<_> = keypairs[3..6].iter().map(|keypair| keypair.name).collect();
        let expected: HashSet<_> = receivers.iter().copied().collect();

        for sender in &publishers {
            assert_eq!(
                withheld_destinations(&committee, sender, 0, &publishers, Some(3), 1, &receivers,),
                Some(expected.clone())
            );
        }
        assert!(withheld_destinations(
            &committee,
            &keypairs[6].name,
            0,
            &publishers,
            Some(3),
            1,
            &receivers,
        )
        .is_none());
    }

    #[test]
    fn coprime_stride_spreads_faulty_lanes_across_every_correct_leader() {
        let (committee, keypairs) = Committee::local_benchmark(40, 1, 9000);
        let publishers: Vec<PublicKey> = (0..13)
            .map(|offset| keypairs[(offset * 13) % 40].name)
            .collect();
        let blocked: Vec<HashSet<PublicKey>> = publishers
            .iter()
            .map(|sender| {
                withheld_destinations(&committee, sender, 0, &publishers, Some(13), 3, &[])
                    .expect("the selected authority is a withholding publisher")
            })
            .collect();

        for leader in keypairs
            .iter()
            .filter(|leader| !publishers.contains(&leader.name))
        {
            let missing = blocked
                .iter()
                .filter(|destinations| destinations.contains(&leader.name))
                .count();
            let held = 13 - missing;
            assert!((4..=5).contains(&missing), "missing {missing} faulty lanes");
            assert!((8..=9).contains(&held), "holds {held} faulty lanes");
        }
    }

    #[test]
    fn n20_leader_burden_mapping_hits_every_correct_leader() {
        let (committee, keypairs) = Committee::local_benchmark(20, 1, 9000);
        let publishers: Vec<PublicKey> = (0..6)
            .map(|offset| keypairs[(offset * 7) % 20].name)
            .collect();
        let blocked: Vec<HashSet<PublicKey>> = publishers
            .iter()
            .map(|sender| {
                withheld_destinations(&committee, sender, 0, &publishers, Some(6), 19, &[])
                    .expect("the selected authority is a withholding publisher")
            })
            .collect();

        for leader in keypairs
            .iter()
            .filter(|leader| !publishers.contains(&leader.name))
        {
            let missing = blocked
                .iter()
                .filter(|destinations| destinations.contains(&leader.name))
                .count();
            assert_eq!(
                missing, 2,
                "every correct leader must miss two faulty lanes"
            );
            assert_eq!(publishers.len() - missing, 4);
        }
    }

    #[test]
    fn repair_suppression_requires_publishers_and_stride_must_be_coprime() {
        let (committee, _) = Committee::local_benchmark(20, 1, 9000);
        let mut params = Parameters {
            withhold_repair: true,
            ..Parameters::default()
        };
        assert!(params.validate_header_faults(&committee).is_err());

        params.withhold_senders = 6;
        params.withhold_stride = 2;
        assert!(params.validate_header_faults(&committee).is_err());

        params.withhold_stride = 3;
        assert!(params.validate_header_faults(&committee).is_ok());
    }

    #[test]
    fn late_header_destinations_select_only_configured_publishers_and_receivers() {
        let (committee, keypairs) = Committee::local_benchmark(10, 1, 9000);
        let publishers: Vec<_> = keypairs[..3].iter().map(|keypair| keypair.name).collect();
        let receivers: Vec<_> = keypairs[3..6].iter().map(|keypair| keypair.name).collect();
        let expected: HashSet<_> = receivers.iter().copied().collect();

        assert_eq!(
            late_header_destinations(&committee, &publishers[0], &publishers, &receivers),
            Some(expected)
        );
        assert!(
            late_header_destinations(&committee, &keypairs[6].name, &publishers, &receivers)
                .is_none()
        );
    }

    #[test]
    fn late_header_configuration_requires_disjoint_groups_and_excludes_withholding() {
        let (committee, keypairs) = Committee::local_benchmark(10, 1, 9000);
        let mut params = Parameters {
            late_header_publishers: keypairs[..3].iter().map(|keypair| keypair.name).collect(),
            late_header_receivers: keypairs[3..6].iter().map(|keypair| keypair.name).collect(),
            late_header_delay_ms: 1_000,
            ..Parameters::default()
        };
        assert!(params.validate_header_faults(&committee).is_ok());

        params.late_header_receivers[0] = params.late_header_publishers[0];
        assert!(params.validate_header_faults(&committee).is_err());
        params.late_header_receivers[0] = keypairs[3].name;
        params.withhold_senders = 1;
        assert!(params.validate_header_faults(&committee).is_err());
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

        assert!(!withhold_active(Some(&cell), base + Duration::from_secs(5)));
        assert!(withhold_active(Some(&cell), start));
        assert!(withhold_active(Some(&cell), base + Duration::from_secs(15)));
        assert!(!withhold_active(Some(&cell), end));
        assert!(!withhold_active(
            Some(&cell),
            base + Duration::from_secs(25)
        ));
    }

    /// Reconnect replay defaults are enabled with a 2-second backoff ceiling.
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
        assert_eq!(params.sequence_sync_rearm_gap_views, 300);
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

    #[test]
    fn vantage_compact_ids_default_on() {
        let defaults = Parameters::default();
        assert!(defaults.vantage_compact_ids);

        let encoded = serde_json::to_value(defaults).expect("parameters serialize");
        let mut object = encoded
            .as_object()
            .expect("parameters are an object")
            .clone();
        object.remove("vantage_compact_ids");
        let decoded: Parameters =
            serde_json::from_value(object.into()).expect("legacy parameters deserialize");
        assert!(decoded.vantage_compact_ids);
    }

    #[test]
    fn withheld_headers_default_on_for_legacy_parameters() {
        let defaults = Parameters::default();
        assert!(defaults.withhold_headers);

        let encoded = serde_json::to_value(defaults).expect("parameters serialize");
        let mut object = encoded
            .as_object()
            .expect("parameters are an object")
            .clone();
        object.remove("withhold_headers");
        let decoded: Parameters =
            serde_json::from_value(object.into()).expect("legacy parameters deserialize");
        assert!(decoded.withhold_headers);
    }
}

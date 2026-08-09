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
            // clippy::suspicious_open_options: explicit truncate (this always writes
            // the WHOLE serialized struct, so a shorter new write must not leave
            // trailing bytes from a longer previous file -- without this, a stale
            // longer parameters.json/committee.json from an earlier run could corrupt
            // the JSON a later, shorter write produces).
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

/// The consensus protocol selected for this node's assembly. One binary, three
/// protocols; the fab harness picks one via the `protocol` parameter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[derive(Default)]
pub enum Protocol {
    /// Autobahn as shipped/evaluated (`use_optimistic_tips = true`).
    #[default]
    AutobahnOptimistic,
    /// Autobahn with certified-tips-only cut formation (`use_optimistic_tips = false`).
    AutobahnSeamless,
    /// Signature-free AGB protocol (implemented in Phase 3+).
    Vantage,
    /// Simple-IT cut-consensus (Fig. 4), driving `simpleit::CutEngine` over the same
    /// data plane Vantage uses (`simpleit::node::SimpleItCore`).
    SimpleIt,
    /// Simple-IT's Bracha-RBC variant (arXiv:2606.14404 Table 1/2 + Corollary 5,
    /// variant S) -- the same `simpleit::node::SimpleItCore`/`simpleit::CutEngine`
    /// assembly as `SimpleIt` above, with `CutEngine`'s own `Variant::Bracha`
    /// selected instead of the default `Variant::Opt`: an extra RBC echo round (own
    /// `CutVote` census at `quorum_threshold` broadcasts a `CutReady`; a `CutReady`
    /// census at `quorum_threshold` marks the round safe) in exchange for never
    /// needing more than `quorum_threshold`-many live authors to make progress,
    /// unlike `SimpleIt`'s own (larger, at big committees) `mint_threshold`.
    SimpleItBracha,
}

impl Protocol {
    /// The `use_optimistic_tips` value implied by this protocol when the
    /// Autobahn code paths run. `None` for Vantage and both Simple-IT variants (the
    /// flag is irrelevant on all three paths -- none of them ever runs Autobahn's
    /// `Core`).
    ///
    /// Open question (see the accompanying task report): Simple-IT has no
    /// optimistic-tip notion of its own, exactly like Vantage -- `None` mirrors
    /// Vantage's own treatment (the only existing precedent for a protocol that
    /// never runs the Autobahn code path this method's own doc comment scopes
    /// itself to), NOT `AutobahnSeamless`'s `Some(false)`: seamless's value is
    /// meaningful precisely because that protocol still runs Autobahn's `Core`
    /// with the flag pinned off, which Simple-IT never does either.
    pub fn implied_optimistic_tips(&self) -> Option<bool> {
        match self {
            Protocol::AutobahnOptimistic => Some(true),
            Protocol::AutobahnSeamless => Some(false),
            Protocol::Vantage => None,
            Protocol::SimpleIt => None,
            // Same reasoning as `SimpleIt` immediately above -- `SimpleItBracha`
            // never runs Autobahn's `Core` either.
            Protocol::SimpleItBracha => None,
        }
    }

    /// METRICS-DASHBOARD-SPEC.md §8: canonical string label for `protocol_info`
    /// (dashboard) and the `--protocol` CLI value -- the exact strings already used by
    /// `node local-benchmark --protocol`/`fab remote --protocol`.
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

/// `protocol` now subsumes `use_optimistic_tips` (see `reconcile_protocol`), so the
/// fab harness no longer writes the raw flag into generated parameter files (Phase-2
/// §1: "Remove `use_optimistic_tips` from fabfile node_params"). The field itself
/// stays required-shaped everywhere else in this struct's docs/semantics, but needs a
/// `#[serde(default)]` fallback to keep deserializing those now-`protocol`-only files;
/// `true` matches `Parameters::default()` and is inert either way, since
/// `reconcile_protocol` overwrites it from `protocol` whenever the latter is present
/// (every reachable Autobahn path always sets `protocol`).
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

    //Autobahn protocol config parameters
    #[serde(default = "default_use_optimistic_tips")]
    pub use_optimistic_tips: bool, //default = true (TODO: implement non optimistic tip option)

    pub use_parallel_proposals: bool, //default = true (TODO: implement sequential slot option)
    pub k: u64,                       //Max open conensus instances at a time.

    pub use_fast_path: bool, // Autobahn only; default = true (Vantage fast seal is unconditional)
    pub fast_path_timeout: u64,

    pub use_ride_share: bool,
    pub car_timeout: u64,

    /// Autobahn (Giridharan et al., SOSP'24) §5.5.3 "All-to-all communication":
    /// on the external-consensus path (`use_ride_share = false`), replicas
    /// broadcast Prepare-Votes/Confirm-Acks and assemble PrepareQC/ConfirmQC
    /// locally instead of unicasting votes to the leader for it to assemble and
    /// re-broadcast -- 3 message exchanges / 2 message-delays on the fast path
    /// instead of the leader-collected regime's 5/4. Off by default
    /// (`#[serde(default)]`) -- byte-identical to today when off; every new
    /// branch this flag guards is inert unless set. Orthogonal to
    /// `use_optimistic_tips`, so it composes with both autobahn-optimistic and
    /// autobahn-seamless automatically. Irrelevant to the ride-share and Vantage
    /// paths (untouched).
    #[serde(default)]
    pub all_to_all: bool,

    //asynchrony simulation:
    pub simulate_asynchrony: bool,
    pub asynchrony_start: u64,
    pub asynchrony_duration: u64,

    /// The consensus protocol assembly to run. Authoritative over
    /// `use_optimistic_tips` (see `reconcile_protocol`). `#[serde(default)]`
    /// keeps pre-Phase-2 parameter files valid.
    #[serde(default)]
    pub protocol: Protocol,

    /// The load generator's transaction-generation mode ("random"/"all_zero") when
    /// the HARNESS knows it. Exists purely to give the standalone `node run
    /// primary`/`node run worker` path a channel for a fact it otherwise cannot
    /// see: `--mode` belongs to the separate `benchmark_client` process, so without
    /// this the `transaction_mode_info` gauge stays absent on every deployed
    /// (docker/fab) run and the dashboard's mode field renders blank -- see
    /// `Metrics::set_transaction_mode_info`. `None` = genuinely unknown; library and
    /// production callers leave it unset so the gauge stays absent rather than
    /// reporting a fabricated mode.
    #[serde(default)]
    pub tx_mode: Option<String>,

    /// Vantage only (PHASE3-SPEC.md §3.1): the maximum number of payload entries
    /// (worker-batch digests) a single data block may carry -- part of `BlockOK`.
    /// Irrelevant on the two Autobahn paths. Rust-side default is a conservative
    /// constant; the harness/config generator is expected to size it as
    /// `workers_per_authority * 4` per the spec note, the same way other int
    /// parameters are sized by `config.py`. `#[serde(default)]` keeps pre-Phase-3
    /// parameter files valid.
    #[serde(default = "default_max_block_payload")]
    pub max_block_payload: usize,

    /// Vantage only (PHASE4-SPEC.md §10): the AGB base delay unit "Δ" (milliseconds).
    /// The fallback deadlines θE = 5Δ, θR = 6Δ are paper-fixed constants derived from
    /// this. Irrelevant on the two Autobahn paths. `#[serde(default)]` (D4-5: 1000ms)
    /// keeps pre-Phase-4 parameter files valid.
    #[serde(default = "default_delta_ms")]
    pub delta_ms: u64,

    /// Benchmark only: absolute wall-clock instant (epoch milliseconds) from which
    /// commit-time observations count toward the rate-relevant metrics. A
    /// transaction whose EMBEDDED submission timestamp predates this instant is
    /// skipped in `worker::synchronizer::read_and_observe_batch`, so the startup
    /// transient never enters the latency histograms or the committed counters.
    ///
    /// Why an absolute instant rather than a per-node uptime offset: nodes do not
    /// boot together (a 50-node `deploy` spans ~40s), so "N ms after my own start"
    /// means a different moment on every machine, and an early-booting node would
    /// still capture the committee-formation transient. Cross-machine clock sync is
    /// already load-bearing for this metric -- the latency is `commit_millis` minus a
    /// timestamp stamped by a DIFFERENT machine's client (see
    /// `read_and_observe_batch`'s own "NTP-grade sync is assumed, not enforced"),
    /// so keying the window off a shared absolute instant introduces no new
    /// assumption.
    ///
    /// The harness must set this EARLIER than its first metrics scrape, or the
    /// baseline scrape reads a partly-gated counter and the windowed TPS it derives
    /// is overstated. `None` (the default, and absent from every pre-existing
    /// parameters file) disables the gate entirely: every observation counts, which
    /// is byte-identical to the behaviour before this field existed.
    #[serde(default)]
    pub metrics_active_at_ms: Option<u64>,

    /// Vantage only: how many VIEWS of per-view internal state `VantageCore` retains
    /// behind its resolved prefix before `collect_internal_garbage` prunes
    /// (`AgbEngine`/`Frontier`/`ControlLog`/`Resolver`). Carrier bodies are additionally
    /// kept `ControlLog::SERVE_MARGIN_WINDOWS` further windows back so a lagging peer can
    /// still fetch them.
    ///
    /// This is deliberately SEPARATE from `gc_depth`, which the Vantage GC originally
    /// reused. `gc_depth` is documented and consumed as a depth in Autobahn ROUNDS
    /// (`Core`/`garbage_collector`); a Vantage view and an Autobahn round are different
    /// counters with different cadence, so one integer cannot correctly serve both, and an
    /// operator tuning `gc_depth` for Autobahn was silently resizing Vantage's retention
    /// window. `#[serde(default)]` keeps every pre-existing parameter file valid. The
    /// default was originally `gc_depth`'s 50 for exactly that reason; it is now 200 -- see
    /// `default_vantage_gc_window_views` for the measurement that motivated raising it.
    ///
    /// `VantageCore::build` clamps this to >= 1: a window of 0 would put the GC floor at
    /// the resolved watermark itself and prune state for the view being resolved.
    #[serde(default = "default_vantage_gc_window_views")]
    pub vantage_gc_window_views: u64,

    /// Simple-IT only: how many ROUNDS of per-round `CutEngine` state
    /// (`SimpleItCore`'s own analogue of `collect_internal_garbage`) retains behind
    /// its current round before `CutEngine::prune_below` is called.
    ///
    /// Deliberately SEPARATE from both `gc_depth` (Autobahn rounds) and
    /// `vantage_gc_window_views` (Vantage views) for the identical reason
    /// `vantage_gc_window_views`'s own doc comment gives: a Simple-IT cut round is yet
    /// another counter with its own cadence, distinct from either. `#[serde(default)]`
    /// keeps every pre-existing parameter file valid. NOTE this default no longer matches
    /// `vantage_gc_window_views`, which was raised to 200 on evidence specific to Vantage's
    /// strictly-serial output cursor (see `default_vantage_gc_window_views`); Simple-IT has
    /// not been measured for the same failure, so its window is left where it was rather
    /// than moved on someone else's evidence.
    ///
    /// `SimpleItCore::build` clamps this to >= 1, matching `vantage_gc_window_views`'s
    /// own clamp for the identical reason (a window of 0 would prune the round
    /// currently being resolved).
    #[serde(default = "default_simpleit_gc_window_rounds")]
    pub simpleit_gc_window_rounds: u64,

    /// Optional, flag-gated replacement for per-block ACK broadcasts (N3): instead of
    /// one ack per (block, acker, recipient), each party periodically broadcasts one
    /// compact watermark per author it holds -- "for author a, I hold a's lane
    /// through (height h, head digest d)". Lanes are hash chains with prefix
    /// verification, so one (h, d) pair covers a's whole verified prefix through h;
    /// this replaces O(n) messages/period/author with O(1). Digest-bound (never
    /// height-only), so crediting still resolves to an exact `BlockRef` before
    /// touching the shared `AckAggregator` -- the same soundness invariant a per-block
    /// ack already satisfies (see `vantage::lanes::LaneManager::resolve_watermark`'s
    /// doc comment for why a height-only watermark would be unsound under an
    /// equivocating author). Shared by both Vantage and Simple-IT (same `LaneManager`/
    /// `AckAggregator` data plane). `#[serde(default)]` = `false` -- byte-identical
    /// wire/behavior when off: the per-block ack broadcast is unchanged, no periodic
    /// watermark tick is even scheduled, and no `VantageAvail` message is ever sent.
    #[serde(default = "default_ack_watermarks")]
    pub ack_watermarks: bool,
    /// The ack-watermark broadcast period, in ms -- irrelevant when `ack_watermarks`
    /// is off. `#[serde(default)]` (50ms) keeps every pre-existing parameter file
    /// valid.
    #[serde(default = "default_ack_watermark_period_ms")]
    pub ack_watermark_period_ms: u64,

    /// Vantage only, optional, flag-gated (signature-free.tex §8.3's "Digest-named
    /// AGB statements"): every ECHO/READY (the `Single`, non-batch shape) names its
    /// proposal by `hash(B_v)` instead of carrying it by value -- the proposal
    /// itself still travels by value only in `VantagePropose`. Reception handles
    /// both encodings unconditionally regardless of this flag, on every party
    /// (`vantage::agb::DigestStatements`); the flag gates EMISSION only.
    /// `#[serde(default)]` = `false` -- byte-identical wire/behavior when off: no
    /// `VantageEchoDigest`/`VantageReadyDigest`/`VantageBodyFetch`/
    /// `VantageBodyServe` message is ever constructed or sent, and
    /// `VantageCore`/`SimpleItCore` (irrelevant to the latter, which has no AGB
    /// engine) never touch `DigestStatements`'s buffering/fetch bookkeeping in any
    /// observable way. Committee-wide consistent by construction, same reasoning as
    /// `ack_watermarks`: every node's `Parameters` comes from the
    /// same generated config.
    #[serde(default = "default_digest_statements")]
    pub digest_statements: bool,

    /// SEQUENCE-CHECKPOINT-SYNC-PLAN.md. Build the local hash-chained sequence log and
    /// checkpoint-boundary heads. Phase B also announces, certifies, downloads, and
    /// verifies remote history; installation remains disabled until Phase C.
    ///
    /// `#[serde(default)]` = `false`. Off, the store is never constructed and the only
    /// residue is one `Vec<Digest>` for the current cursor view, which the cursor
    /// accumulates unconditionally so its output rules cannot depend on configuration.
    #[serde(default = "default_sequence_checkpoints")]
    pub sequence_checkpoints: bool,

    /// Checkpoint boundary interval `K` in terminally processed views.
    ///
    /// Fixed boundaries rather than "whatever head I hold": correct cursors sit a few
    /// views apart at any instant, so announcing current heads would rarely produce the
    /// `f+1` EXACT matches the recovery rule needs (§4.4). 0 is treated as 1.
    #[serde(default = "default_sequence_checkpoint_interval_views")]
    pub sequence_checkpoint_interval_views: u64,

    /// Phase B (plan section 6.2). How often the announce timer fires. An announcement
    /// is sent when the local boundary has advanced, or when
    /// `sequence_announce_repeat_ms` has elapsed for the current one.
    #[serde(default = "default_sequence_announce_period_ms")]
    pub sequence_announce_period_ms: u64,

    /// Re-send interval for an UNCHANGED boundary. Repetition is required, not an
    /// optimization: a node that starts late must still be able to collect `f+1`
    /// announcements for a boundary the fleet passed before it existed, and a strictly
    /// edge-triggered announcement would never reach it.
    #[serde(default = "default_sequence_announce_repeat_ms")]
    pub sequence_announce_repeat_ms: u64,

    /// Records per served chunk. Chunked by ITEM COUNT rather than bytes because records
    /// are fixed-width, so an item cap is already an exact byte cap.
    #[serde(default = "default_sequence_sync_chunk_records")]
    pub sequence_sync_chunk_records: usize,

    /// Consecutive terminal outcome bodies per served chunk. Outcomes contain manifests
    /// and are therefore variable-sized; the conservative default keeps an n=100 Full
    /// batch near the existing 64 KiB frame norm.
    #[serde(default = "default_sequence_sync_chunk_outcomes")]
    pub sequence_sync_chunk_outcomes: usize,

    /// Delta digests per served chunk (32 B each).
    #[serde(default = "default_sequence_sync_chunk_digests")]
    pub sequence_sync_chunk_digests: usize,

    /// Start state sync only when a certified checkpoint is at least this many terminal
    /// views beyond the local sequence cursor. Smaller gaps use normal dissemination and
    /// parked-message recovery. 50 views is roughly 3--5 seconds in the n=100 runs.
    #[serde(default = "default_sequence_sync_min_gap_views")]
    pub sequence_sync_min_gap_views: u64,

    /// Matching announcers queried concurrently for one outstanding chunk. Up to `f` of
    /// the `f+1` may withhold every response, so SERIAL failover would multiply
    /// worst-case recovery time by `f`; concurrency bounds it by the one correct
    /// announcer's latency at a bandwidth cost of at most this factor.
    #[serde(default = "default_sequence_sync_max_sources")]
    pub sequence_sync_max_sources: usize,

    /// Per-request deadline before failing over to the NEXT source.
    #[serde(default = "default_sequence_sync_request_timeout_ms")]
    pub sequence_sync_request_timeout_ms: u64,

    /// Bounded ingress for state-sync responses, in frames. Separate from the main
    /// Vantage inbound queue by design: this mechanism exists to relieve a node whose
    /// main queue is ALREADY saturated, so sharing that queue would deepen the exact
    /// congestion it is meant to drain. Overflow drops the newest frame -- responses are
    /// idempotent and re-requestable, so a drop costs one retry while blocking would
    /// propagate backpressure into the transport and stall live consensus.
    #[serde(default = "default_sequence_sync_inbound_capacity")]
    pub sequence_sync_inbound_capacity: usize,

    /// Views of a verified target admitted into the block-fetch window at once.
    ///
    /// Installation applies views in order, so fetching far ahead of the install point
    /// buys nothing; the window exists only so that one slow lane does not serialize the
    /// whole range behind it.
    #[serde(default = "default_sequence_install_window_views")]
    pub sequence_install_window_views: usize,

    /// `Repairer::pending_settle_len()` above which the install fetch admits no further
    /// view. Unsettled refs are the set whose unbounded growth turned 60,262 received
    /// blocks into 612M settle calls on the 2026-08-07 n=100 run, so an installer that
    /// ignored repair's backlog would recreate that regime on exactly the nodes least able
    /// to absorb it.
    #[serde(default = "default_sequence_install_settle_ceiling")]
    pub sequence_install_settle_ceiling: usize,

    /// Apply verified checkpoint state to the cursor, rather than only fetching and
    /// verifying it.
    ///
    /// OFF by default and separate from `sequence_checkpoints` on purpose: staging is
    /// observation and costs a node nothing it would not otherwise spend on repair, while
    /// installation is the only path in the system that produces committed output from
    /// bytes another party derived. The plan enables it first on deliberately
    /// delayed/restarted nodes, not fleet-wide.
    #[serde(default = "default_sequence_install_enabled")]
    pub sequence_install_enabled: bool,

    /// Views applied per install pass, so a large target cannot monopolize the core loop.
    /// Each view costs a header lookup per delivered block plus one `NotifyCommitted`, and
    /// the loop it runs on is the same single-threaded core that serves consensus.
    #[serde(default = "default_sequence_install_views_per_tick")]
    pub sequence_install_views_per_tick: usize,

    /// Block digests emitted per install pass.
    ///
    /// The real bound. A view's delta is the whole accumulated lane suffix since the last
    /// emitted watermark, so a multi-second gap at n=100 puts thousands of headers behind a
    /// single view and a views-only cap would still hand the core an unbounded turn.
    /// `Cursor::install` honours this by leaving a view OPEN when the budget runs out, so
    /// the bound costs latency, never correctness.
    #[serde(default = "default_sequence_install_digests_per_tick")]
    pub sequence_install_digests_per_tick: usize,

    /// AVAIL-ECHO-SPEC.md: carry availability acknowledgments POSITIONALLY on the AGB
    /// echo -- a bit per lane against the echoed proposal's own reference vector --
    /// instead of `VantageAvail`'s explicit `(a,k,h)` tuples.
    ///
    /// Same statements, different encoding: a set bit denotes exactly the reference at
    /// that index in `claim::manifest_refs(proposal)`, counted first-hand from the
    /// echo's channel sender at the unchanged thresholds. `Definition (Availability)`
    /// is untouched, and the claim rides OUTSIDE the echo's counting identity exactly
    /// like `Echo::wish` and `Echo::origin` already do.
    ///
    /// Why: measured per node on the 2026-08-07 n=100 run at `e46f6e1`,
    /// `network_bytes_sent_total{type="VantageAvail"}` was 18.330 of 19.880 MB/s --
    /// **92.2% of the node's entire wire budget**, at 9,258 B per message against a
    /// 2,203 B average, while `Header` was 1.4%. Autobahn at the same throughput pushes
    /// 4.34 MB/s with no acknowledgment layer at all.
    ///
    /// `#[serde(default)]` = `false`, and when off NOTHING changes: no claim is ever
    /// constructed, the field serializes as `None`, and `avail_tick` keeps flushing
    /// `VantageAvail` as before. When ON, the periodic flush is not scheduled and the
    /// claims replace it. Committee-wide consistent by construction, same reasoning as
    /// `ack_watermarks`/`digest_statements`: every node's `Parameters` comes from the
    /// same generated config.
    #[serde(default)]
    pub echo_avail_claims: bool,

    /// PHASE7-PREP-NOTES.md (optional, WAN-shaped local runs): an optional
    /// per-authority-pair one-way latency table, applied to THIS node's own
    /// primary-to-primary connections at spawn time via `Committee::latency_map`
    /// (`Core::spawn`/`vantage::node::VantageCore::spawn`, both protocols
    /// identically). Never round-trips through `parameters.json`/`fab`
    /// (`#[serde(skip)]`) -- always `None` immediately after `Parameters::default()`
    /// or `Parameters::import(..)`, since the field is entirely absent from every
    /// JSON file (existing or new).
    ///
    /// Fable audit FIX 3: this field is populated ONLY by the two CLI entry handlers
    /// below, never by library code, and both of them now DEFAULT to injecting the
    /// real 10-AWS-region RTT matrix rather than to zero injected delay -- so, as of
    /// this changeset, `None` no longer means "zero injected delay" / "byte-identical
    /// current behavior" for a running node; it only means "the CLI entry hasn't
    /// substituted a table yet". Precedence (see each handler's own doc comment for
    /// the exact derivation):
    ///   - `node run` (`node/src/main.rs::run`): expands the deployable
    ///     `mimic_latency_ms` knob just below -- `Some(rtt)` (including `Some(0)`) is
    ///     an EXPLICIT override to `LatencyTable::uniform(n, rtt)`; `None` (absent
    ///     from `parameters.json`, true of every pre-Phase-7 file) DEFAULTS to
    ///     `LatencyTable::aws_rtt(n)`. After this expansion the field is always
    ///     `Some(..)` for a running node -- an all-zero uniform table (e.g. `fab
    ///     remote`'s current `mimic_latency_ms: Some(0)` default) injects no delay,
    ///     but is still a `Some`, never a `None`.
    ///   - `node local-benchmark` (`node/src/local_benchmark.rs::run`):
    ///     `--latency-table <csv>` wins; else an EXPLICITLY passed
    ///     `--mimic-latency-ms <n>` (n > 0) gives `uniform(n, ..)`; else an EXPLICITLY
    ///     passed `--mimic-latency-ms 0` gives `None` (the only remaining way to
    ///     genuinely request zero injected latency, i.e. pure loopback); else
    ///     (neither flag given) defaults to `aws_rtt(n)`.
    ///
    /// Neither handler's substitution touches `Parameters::default()` itself, which
    /// still yields `latency_table: None` / `mimic_latency_ms: None` -- library
    /// defaults and existing unit tests are unaffected.
    #[serde(skip)]
    pub latency_table: Option<Arc<LatencyTable>>,

    /// PHASE7 (AWS/distributed WAN-shaped runs): the DEPLOYABLE uniform-RTT mimic
    /// latency knob -- the parameters.json counterpart of `node local-benchmark
    /// --mimic-latency-ms`. Unlike `latency_table` (which is `#[serde(skip)]`: an
    /// in-process value built only by `local-benchmark`, never carried in a config
    /// file), THIS field DOES round-trip through `parameters.json`/`fab`, so the fab
    /// harness can inject a WAN-like RTT into a co-located AWS committee where every
    /// node reads its config from a deployed file rather than a CLI flag.
    ///
    /// `node run` treats this field as an EXPLICIT OVERRIDE to a uniform scalar:
    /// `Some(rtt)` (including `Some(0)`) always wins and expands to
    /// `LatencyTable::uniform(committee.size(), rtt)` at spawn time (one-way = rtt/2).
    /// When this field is `None` (absent from `parameters.json` -- true of every
    /// pre-Phase-7 file), `node run` instead DEFAULTS to the real 10-AWS-region RTT
    /// matrix (`LatencyTable::aws_rtt(committee.size())`, ported VERBATIM from
    /// starfish), mirroring starfish's own default for single-region AWS
    /// benchmarking. Either way the existing `Committee::latency_map` path applies
    /// the resulting table identically to both protocols -- no primary/worker/Vantage
    /// code changes needed, only this one expansion in `node`'s `run`. This
    /// default-substitution logic lives ONLY in the `node run`/`node local-benchmark`
    /// CLI entry handlers, never in `Parameters::default()` (which keeps
    /// `mimic_latency_ms: None` and, transitively, `latency_table: None` --
    /// library defaults and existing unit tests are unaffected).
    #[serde(default)]
    pub mimic_latency_ms: Option<u64>,

    /// Transport-level per-peer outbound message batching (coalescing), on by
    /// default (`default_batch_messages()` = `true`). When explicitly turned off it
    /// restores byte-identical unbatched wire behavior (see `network` crate's `batch`
    /// module doc). Protocol-transparent: applied
    /// uniformly by the `network` crate to every sender/receiver this node spawns
    /// EXCEPT the client-facing transaction port (clients never batch). Committee-wide
    /// consistent by construction: every node's `Parameters` comes from the same
    /// generated config.
    #[serde(default = "default_batch_messages")]
    pub batch_messages: bool,
    /// Hybrid flush size cap in bytes (see `network::BatchConfig::max_bytes`).
    /// Irrelevant when `batch_messages` is off. `#[serde(default)]` (65536) keeps
    /// pre-batching parameter files valid.
    #[serde(default = "default_batch_max_bytes")]
    pub batch_max_bytes: usize,
    /// Hybrid flush delay in milliseconds (see `network::BatchConfig::max_delay_ms`).
    /// Irrelevant when `batch_messages` is off. `#[serde(default)]` (5) keeps
    /// pre-batching parameter files valid. 5 ms costs only ~2.5 ms average added
    /// latency (a message waits ~window/2) -- negligible next to a WAN's ~400 ms
    /// p50 -- while coalescing substantially more per flush than a 1 ms window
    /// would, which matters more as n grows (n~50/100). `batch_max_bytes`'s size
    /// cap still short-circuits this window the moment a burst fills it.
    #[serde(default = "default_batch_max_delay_ms")]
    pub batch_max_delay_ms: u64,

    /// Data-plane withholding fault injector (`node local-benchmark --withhold`): the
    /// first `withhold_senders` committee indices (0-based, sorted order -- the same
    /// convention `--crash`/`--load-nodes` already use) withhold their payload-
    /// dissemination broadcasts (worker `Batch`, primary `Header`/lane-block publish)
    /// from a staggered half of the committee -- see `withheld_destinations`. Every
    /// other message (consensus, acks, and every repair/request-response path) is
    /// unaffected. `0` (default) means no node withholds anything -- `#[serde(default)]`
    /// keeps every pre-existing parameter file valid, and `withheld_destinations`
    /// returns `None` for every node when this is `0`, so the filter never allocates or
    /// perturbs a send path in that case.
    #[serde(default)]
    pub withhold_senders: usize,

    /// Time-windows the data-plane withholding fault injector above (`node
    /// local-benchmark --withhold-at`): offset from measurement start (ms) when
    /// withholding begins. `None` (default, and the ONLY value reachable when
    /// `--withhold-at` is absent) means WHOLE-RUN withholding -- `withhold_active`
    /// (this crate) then returns `true` unconditionally for every withholding
    /// sender's whole lifetime, reproducing c35fc4a's original (pre-window) behavior
    /// exactly. `#[serde(default)]` keeps every pre-existing parameter file valid.
    ///
    /// Only ever populated by `node local-benchmark`'s CLI entry handler -- library
    /// code/`node run` never sets this.
    #[serde(default)]
    pub withhold_at_ms: Option<u64>,
    /// Withholding window duration (ms). Only consulted when `withhold_at_ms` is
    /// `Some`. `#[serde(default = "default_withhold_for_ms")]` (30_000 ms,
    /// `--withhold-for`'s own CLI default) keeps every pre-existing parameter file
    /// valid.
    #[serde(default = "default_withhold_for_ms")]
    pub withhold_for_ms: u64,
    /// The shared, in-process "has the window opened yet" cell every withholding
    /// sender's own filter site (`worker::BatchMaker::seal`, `primary::Core::
    /// process_own_header`, `primary::vantage::wire::Wire::broadcast_message`)
    /// consults via `withhold_active` (this crate). `node local-benchmark::run` arms
    /// this cell right after `run_start` is captured. Stores `std::time::Instant`
    /// so this crate never needs a `tokio` dependency just for this one
    /// skip-serialized field. `#[serde(skip)]` -- always `None` immediately after
    /// `Parameters::default()`/`Parameters::import(..)`.
    ///
    /// `None` here makes `withhold_active` treat withholding as WHOLE-RUN (see that
    /// fn's own doc comment) -- it does NOT disable withholding; disabling
    /// withholding entirely is `withhold_senders: 0`'s job, a completely separate
    /// knob this field never touches.
    #[serde(skip)]
    pub withhold_window: Option<Arc<OnceLock<(Instant, Instant)>>>,

    /// Mechanism A (sender-side lane resume, modeled on Starfish's subscription
    /// resume but ack-census-gap-triggered instead of reconnection-triggered --
    /// motivated by the windowed `--withhold` experiment, where a fire-and-forget
    /// broadcast publish never gets replayed for whatever half of the committee
    /// missed it during the window). The periodic tick period at which
    /// `VantageCore`/`SimpleItCore` check each OTHER lane author for a persistent gap
    /// between this party's own held contiguous direct-verified prefix
    /// (`vantage::lanes::LaneManager::own_direct_frontier`) and the highest
    /// (f+1)-attested height for that author's lane
    /// (`vantage::lanes::LaneManager::avail_high`) -- see `vantage::resume`'s own
    /// module doc comment for the trigger/serve design. Shared by both Vantage and
    /// Simple-IT (same `LaneManager`/`Wire` data plane). `#[serde(default)]`
    /// (1000 ms) keeps every pre-existing parameter file valid.
    #[serde(default = "default_resume_check_period_ms")]
    pub resume_check_period_ms: u64,
    /// Mechanism A: the minimum spacing between two `VantageLaneResume` requests this
    /// party sends for the SAME (lane author, gap height), and independently the
    /// minimum spacing between two resume batches a lane author serves for the SAME
    /// (requester, gap height) -- a resend/re-serve rate limit, not an absolute
    /// one-shot (a request/serve for a DIFFERENT height is never held back by this).
    /// `#[serde(default)]` (4000 ms) keeps every pre-existing parameter file valid.
    #[serde(default = "default_resume_backoff_ms")]
    pub resume_backoff_ms: u64,
    /// Mechanism A: the maximum number of own blocks a lane author serves in a
    /// single resume batch. Requester-paced rather than a server-side cursor
    /// looping to the author's own tip in one shot: the requester's own frontier
    /// advances on receipt of a batch, and its NEXT REQUEST follows immediately
    /// (`VantageCore::try_resume_request`'s receipt-continuation call sites,
    /// `Inbound::Publish`/`on_payload_ready` -- not just the periodic
    /// `resume_check_period_ms` tick) -- this deliberately simplifies Starfish's
    /// server-side park-on-notify serving loop (no per-requester cursor state to
    /// clean up) while still draining at receipt pace, not tick pace.
    ///
    /// NOT a direct copy of Starfish's own `batch_own_block_size` default (8,
    /// crates/starfish-core/src/dag_state.rs) despite the shared name and role:
    /// Starfish's 8 is sized per iteration of a server-side loop that keeps
    /// streaming batch after batch with no round trip in between (bounded only by
    /// the transport's own flow control); ours is sized per REQUEST-RESPONSE ROUND
    /// TRIP (one batch, then wait for the next ask). At Starfish's cadence 8/
    /// iteration over a tight loop is already fast; at ours, 8/RTT over a
    /// (bursty, contended-WAN-mimicked) round trip is not -- 64/RTT
    /// (~150-300 ms under this repo's own AWS-RTT latency mimic) is roughly
    /// 200-400 blocks/s/lane, enough to clear a several-hundred-block gap (this
    /// repo's own `max_header_delay` default publishes roughly one block every
    /// tens of ms per author, so even a short fault window backs up a lane by
    /// hundreds of blocks) in low single-digit seconds instead of tens of seconds.
    /// `#[serde(default)]` (64) keeps every pre-existing parameter file valid.
    /// GLOBAL cap on how many lane-resume EPISODES one node may have established at
    /// once (`vantage::resume::ResumeTrigger::max_concurrent`); 0 = unlimited, the
    /// behaviour before this field existed.
    ///
    /// The pre-existing in-flight cap is per author -- one outstanding request each --
    /// so at n=100 up to 99 episodes stream simultaneously at receipt pace. That
    /// ignited the 2026-08-07 n=100 congestion collapse: 122,736 blocks re-served per
    /// node (zero at n=50) put the single-threaded core at 87.2% of one core executing
    /// effects, pinning its inbound queue and cutting organic delivery to ~5%, which
    /// produced further gaps. Bounding concurrency breaks that loop; deferred episodes
    /// stay `pending` and are promoted as earlier ones close, so recovery still
    /// completes.
    #[serde(default = "default_resume_max_concurrent")]
    pub resume_max_concurrent: usize,

    #[serde(default = "default_resume_batch")]
    pub resume_batch: u64,

    /// n=100 straggler fix (2026-08-08): per-destination outbound queue depth at
    /// which the MAIN pool sheds a volatile send at enqueue -- min-merging its
    /// filing key into the drop map exactly like a session-death discard, so the
    /// reconnect-replay nudge/Hello path recovers it -- instead of blocking the
    /// consensus core behind the slowest peer. This is the trigger the 2026-08-07
    /// investigation found missing: every replay path required a session DEATH, so
    /// a connected-but-slow straggler (291 established sessions, zero drops) never
    /// earned a replay episode and could not recover missed one-shots. `0` disables
    /// (the pre-existing blocking behavior). Only consulted when `reconnect_replay`
    /// is on -- shedding without the outbox+replay mechanism behind it would be a
    /// hidden loss, so `VantageCore` gates the attach on that flag. Sized so a
    /// healthy peer's queue (depth ~ rate x RTT, single digits) never grazes it,
    /// while a peer draining slower than organic broadcast volume crosses it within
    /// tens of seconds. `#[serde(default)]` keeps every pre-existing parameter file
    /// valid.
    #[serde(default = "default_volatile_soft_cap")]
    pub volatile_soft_cap: usize,

    /// KNOB 1 (measurement ablation): master on/off switch for the newer,
    /// server-floored volatile one-shot replay mechanism (`vantage::outbox::Outbox`
    /// plus the Hello/Done exchange -- `replay_history_views`/`replay_chunk_bytes`/
    /// `replay_chunk_interval_ms`/`replay_serve_max_bytes`/`outbox_max_bytes`/
    /// `replay_episode_max_ms` below). Vantage only.
    ///
    /// Motivation: this mechanism and `retry_backoff_max_ms`'s cap change (60s ->
    /// 2s) landed in the same commits, and an adversarial review of a before/after
    /// benchmark figure found the cap alone explains most of the measured
    /// improvement -- with no build able to disable the replay mechanism
    /// independently, it could not be attributed any effect at all. This flag,
    /// together with `retry_backoff_max_ms`, creates three cleanly separable
    /// measurement arms: (A) "true before" (`reconnect_replay=false`,
    /// `retry_backoff_max_ms=60000`), (B) "cap only" (`false`, `2000`), (C) "full"
    /// (`true`, `2000`) -- A->B isolates the backoff cap, B->C isolates the replay.
    ///
    /// `#[serde(default = "default_reconnect_replay")]` (`true`) keeps every
    /// pre-existing parameter file's behavior unchanged. When `false`,
    /// `VantageCore::broadcast_recorded` (the single choke point every one-shot
    /// AGB/consensus broadcast passes through) records nothing into the outbox and
    /// sends via the ordinary DURABLE path instead of the volatile one, no Hello is
    /// ever sent or reciprocated (the reconnect-event arm, the tick re-ask, and the
    /// `pending_low` nudge are all inert), and an incoming `ResumeHello`/
    /// `ReplayDone` is ignored -- i.e. the node behaves exactly as it did before
    /// this mechanism existed. Mechanism A (the PRE-EXISTING sender-side lane
    /// resume, `vantage::resume`'s `ResumeTrigger`/`ResumeServe`/
    /// `Inbound::LaneResume`) is a SEPARATE mechanism and is never gated by this
    /// flag -- see that module's own doc comment.
    #[serde(default = "default_reconnect_replay")]
    pub reconnect_replay: bool,

    /// KNOB 2 (measurement ablation, paired with `reconnect_replay` above): the
    /// reconnect-waiter's exponential-backoff CEILING, in ms
    /// (`network::reliable_sender::Connection::run`'s `delay = min(2*delay,
    /// retry_backoff_max_ms)`). Transport-level, so unlike every other field in
    /// this group it is NOT Vantage-specific -- it applies uniformly to every
    /// `ReliableSender` this workspace's three protocols (Autobahn, Vantage,
    /// Simple-IT) construct for primary-to-primary traffic, including the
    /// reconnect-replay pool's own task-owned sender (`vantage::wire::
    /// spawn_resume_sender`). The initial per-connection retry delay (200ms) and
    /// the doubling between attempts are unaffected by this knob -- only the
    /// ceiling the doubling saturates at.
    ///
    /// `#[serde(default = "default_retry_backoff_max_ms")]` (`2000`) matches the
    /// value this cap was hardcoded to before this field existed, so every
    /// pre-existing parameter file's behavior is unchanged.
    #[serde(default = "default_retry_backoff_max_ms")]
    pub retry_backoff_max_ms: u64,

    /// reconnect-replay plan §5/§9: how many VIEWS of one-shot-message history
    /// `vantage::outbox::Outbox` retains behind the current `own_watermark` before
    /// `prune_below` evicts a whole view's worth (a ceiling; `outbox_max_bytes`
    /// below is the byte cap that actually binds in practice at typical Δ). Kept
    /// SEPARATE from `vantage_gc_window_views` for the identical reason that field's
    /// own doc comment gives for being separate from `gc_depth`: the outbox is keyed
    /// by `Pacemaker::own_watermark` (this party's own wish), a different, generally
    /// faster-advancing counter than the resolver's `resolved_watermark`
    /// `vantage_gc_window_views` is sized against -- reusing either existing window
    /// would silently mis-size this one. `#[serde(default)]` keeps every
    /// pre-existing parameter file valid.
    #[serde(default = "default_replay_history_views")]
    pub replay_history_views: u64,
    /// reconnect-replay plan §5/§9: the resume task's per-chunk send size, in bytes
    /// of pre-bundle-header replay payload -- one `ResumeSend::Replay` chunk per
    /// stream per round-robin rotation. `#[serde(default)]` keeps every pre-existing
    /// parameter file valid.
    #[serde(default = "default_replay_chunk_bytes")]
    pub replay_chunk_bytes: usize,
    /// reconnect-replay plan §5/§9: the resume task's pacing delay between
    /// round-robin rotations -- together with `replay_chunk_bytes` this bounds the
    /// GLOBAL replay ceiling to `replay_chunk_bytes / replay_chunk_interval_ms`
    /// bytes/s by construction (one task, one bucket, shared by every concurrent
    /// replay stream). `#[serde(default)]` keeps every pre-existing parameter file
    /// valid.
    #[serde(default = "default_replay_chunk_interval_ms")]
    pub replay_chunk_interval_ms: u64,
    /// reconnect-replay plan §6/§9: the per-peer served-bytes budget per rolling
    /// `resume_backoff_ms` window -- bounds per-peer extraction to roughly
    /// `replay_serve_max_bytes / resume_backoff_ms` bytes/s; an over-budget Hello is
    /// deferred to the next window rather than served partially past the cap (a
    /// single key larger than the whole budget is still served whole -- see
    /// `vantage::outbox`'s module doc). `#[serde(default)]` keeps every pre-existing
    /// parameter file valid.
    #[serde(default = "default_replay_serve_max_bytes")]
    pub replay_serve_max_bytes: usize,
    /// reconnect-replay plan §5/§9: the outbox's total byte cap, evicting whole
    /// oldest views (never the newest key) once crossed -- the bound that actually
    /// binds in practice at typical Δ (`replay_history_views` above is a ceiling on
    /// top of it). `#[serde(default)]` keeps every pre-existing parameter file valid.
    #[serde(default = "default_outbox_max_bytes")]
    pub outbox_max_bytes: usize,
    /// reconnect-replay plan §6/§9/§14 A6: the requester-side replay episode's
    /// expiry valve (re-opened by the next reconnect/Hello/nudge event), AND (A6)
    /// the author-side in-flight-replay-stream TTL -- the two are deliberately the
    /// SAME constant: A6's own rationale is that strict `Message`-priority
    /// scheduling means replay throughput is not guaranteed, so a shorter in-flight
    /// TTL could expire mid-drain and cause a duplicate re-serve; sizing it to the
    /// requester's own episode lifetime avoids that by construction. `#[serde(default)]`
    /// keeps every pre-existing parameter file valid.
    #[serde(default = "default_replay_episode_max_ms")]
    pub replay_episode_max_ms: u64,
}

fn default_batch_messages() -> bool {
    true
}

/// ON by default since the n=20 / 1000 tx/s measurement (HANDOFF section 27):
/// watermarks cut wire messages ~3.8x and ~8 pp CPU per node at no p50 cost
/// (they cost ~+45 ms only at very low load, where the 50 ms tick dominates).
/// `--no-ack-watermarks` restores per-block acks.
fn default_ack_watermarks() -> bool {
    true
}

/// ON by default since the same measurement: digest-named AGB statements halve
/// wire bytes (76.8 -> 38.3 MB/s at n=20 / 1000 tx/s) with p50 unchanged.
/// `--no-digest-statements` restores value-carrying statements.
fn default_digest_statements() -> bool {
    true
}

/// Off until guarded installation and its adversarial audit complete (plan Phase C).
fn default_sequence_checkpoints() -> bool {
    false
}

/// 100 views. Frequent enough that a straggler's recovery anchor is never far behind,
/// rare enough that boundary bookkeeping is negligible next to per-view records. The
/// plan defers a profiled value, so this is deliberately a round starting point rather
/// than a tuned one.
fn default_sequence_checkpoint_interval_views() -> u64 {
    100
}

fn default_sequence_announce_period_ms() -> u64 {
    2_000
}

fn default_sequence_announce_repeat_ms() -> u64 {
    10_000
}

/// 256 records is ~24 KB at 96 B/record.
fn default_sequence_sync_chunk_records() -> usize {
    256
}

/// A Full outcome at n=100 is roughly 7 KiB; eight stay near the 64 KiB frame norm.
fn default_sequence_sync_chunk_outcomes() -> usize {
    8
}

/// 1024 digests is 32 KB, comfortably below the 64 KB frame norm.
fn default_sequence_sync_chunk_digests() -> usize {
    1_024
}

fn default_sequence_sync_min_gap_views() -> u64 {
    50
}

/// `f+1` at the smallest committee this targets.
fn default_sequence_sync_max_sources() -> usize {
    3
}

fn default_sequence_sync_request_timeout_ms() -> u64 {
    5_000
}

fn default_sequence_sync_inbound_capacity() -> usize {
    256
}

/// Mirrors `vantage::install::DEFAULT_WINDOW_VIEWS` (that crate depends on this one, not
/// the other way round). Eight views overlaps progress across a slow lane while keeping
/// the authorized set on the order of `8 * n` refs rather than `range * n`.
fn default_sequence_install_window_views() -> usize {
    8
}

/// Mirrors `vantage::install::DEFAULT_SETTLE_CEILING`. Well below the 4,967 unsettled refs
/// measured on a straggler, so installation backs off before reaching that regime.
fn default_sequence_install_settle_ceiling() -> usize {
    2_048
}

fn default_sequence_install_enabled() -> bool {
    false
}

fn default_sequence_install_views_per_tick() -> usize {
    16
}

/// Roughly one second of fleet output at the n=100 rates this mechanism targets (~2,000
/// blocks/s), so a pass costs about as much as one tick of ordinary committing.
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
    // Delta's contract is "bounds every one-way message delay after GST".  The
    // ten-region AWS matrix tops out at 154.5 ms one-way (309 ms RTT), so 200 ms
    // is the tightest defensible default for the shapes this harness emulates;
    // fault-path latencies scale linearly in Delta (HANDOFF sections 25-26), so
    // an overly conservative default directly inflates them.
    200
}

/// 200 views, 4x the 50 it was originally set to (which matched `gc_depth`'s default, so
/// that splitting the two knobs apart changed nothing at the time).
///
/// Raised because 50 views is a very short retention horizon in wall-clock terms. Measured
/// view rates: ~8.7 views/s locally at n=20 and ~11.5 views/s on AWS at n=100, so 50 views
/// is only **4-6 SECONDS** of history, and a node that falls further behind than that can
/// never obtain the prefix its strictly-serial output cursor needs -- every peer has already
/// pruned it. Demonstrated directly on 2026-08-08: a validator started 60s late at n=20
/// caught its AGB view up completely (`entered_view` 1 -> 2,870 in 23s, tracking the
/// committee median exactly) while its output cursor never left view 1 and it committed
/// ZERO in 181s, at two different load levels. Its header requests plateaued within 23s --
/// it had stopped asking, because nothing could answer.
///
/// 200 views is ~17-23s of history at those rates. That is a real widening of the
/// straggler-recovery window, NOT a fix for late joining: the same measurement shows
/// stragglers on the AWS n=100 runs sitting 177-882 views behind the median, so only the
/// mildest of them come back inside this horizon. Catching up from an arbitrary lag needs a
/// state-sync/snapshot path, which this codebase does not have.
///
/// Cost is retained per-view component state (`collect_internal_garbage` prunes below
/// `resolved_watermark - gc_window`), so this trades RSS for recovery headroom.
fn default_vantage_gc_window_views() -> u64 {
    200
}

/// Left at 50 while `vantage_gc_window_views` moved to 200 -- see
/// `simpleit_gc_window_rounds`'s own doc comment for why the two are not the same counter,
/// and `default_vantage_gc_window_views` for what motivated raising only the Vantage one.
fn default_simpleit_gc_window_rounds() -> u64 {
    50
}

/// `ack_watermarks`'s own doc comment.
fn default_ack_watermark_period_ms() -> u64 {
    50
}

/// `Parameters::withhold_for_ms`'s own doc comment -- matches `--withhold-for`'s own
/// CLI default.
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

/// `Parameters::retry_backoff_max_ms`'s own doc comment -- matches
/// `network::reliable_sender`'s own previously-hardcoded cap exactly.
fn default_retry_backoff_max_ms() -> u64 {
    2_000
}

/// `Parameters::replay_history_views`'s own doc comment.
/// 8 concurrent resume episodes: enough that recovery is not serialised to a crawl,
/// small enough that the single-threaded core keeps servicing consensus alongside it.
/// n=50 -- which never collapsed -- ran with ZERO episodes open, so this bound is far
/// above anything a healthy run needs.
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

/// AWS region names for the 10-region RTT matrix below. Ported VERBATIM from
/// `~/code/starfish/crates/starfish-core/src/network.rs` lines 51-62.
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

/// RTT table for the 10 AWS regions above, in milliseconds. Ported VERBATIM from
/// `~/code/starfish/crates/starfish-core/src/network.rs` lines 65-76 (base,
/// non-adversarial matrix only -- the adversarial-latency ramp in starfish's
/// `generate_latency_table` is out of scope here).
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

/// PHASE7-PREP-NOTES.md (WAN-shaped local runs, optional item): an n x n one-way
/// inter-authority latency table, indexed by committee order (`Committee::index_of`
/// -- the same deterministic `BTreeMap<PublicKey, _>` order `Pacemaker`/
/// `ControlLog::control_leader`/`Resolver` already rely on for their own
/// party-indexed arrays/rotations, so a CSV's rows/columns line up with committee.json
/// the same way every node in a run sees it). Reference (read-only):
/// `~/code/starfish/crates/starfish-core/src/network.rs`'s `generate_latency_table` +
/// per-connection `extra_connection_latency` application build an analogous per-pair
/// table for starfish's own injection point; this is the much smaller subset this
/// workspace's harness needs -- a fixed table (no adversarial ramp/per-call jitter).
#[derive(Clone, Debug)]
pub struct LatencyTable {
    /// `one_way_ms[i][j]` = one-way latency (ms) from committee-order index `i` to
    /// `j`. Symmetric by construction (halved from an RTT matrix), diagonal 0.
    one_way_ms: Vec<Vec<f64>>,
}

impl LatencyTable {
    /// The trivial uniform table `--mimic-latency-ms` builds: same RTT-ms/halving
    /// convention as `from_rtt_csv` (every off-diagonal pair gets the same one-way
    /// delay, `rtt_ms / 2`) so both flags are governed by the identical construction --
    /// `--mimic-latency-ms X` is defined as exactly equivalent to a uniform `--latency
    /// -table` CSV whose every cell is `X`. Diagonal (self-to-self, never looked up by
    /// `Committee::latency_map`, which always skips `other == myself`) is 0.
    pub fn uniform(n: usize, rtt_ms: f64) -> Self {
        let one_way = rtt_ms / 2.0;
        let mut t = vec![vec![one_way; n]; n];
        for (i, row) in t.iter_mut().enumerate() {
            row[i] = 0.0;
        }
        Self { one_way_ms: t }
    }

    /// Parses an n x n ROUND-TRIP-ms CSV matrix (rows/columns in committee order, no
    /// header row, comma-separated, blank lines skipped), halving every entry on load
    /// to the one-way latency this table stores (RTT is the natural unit to
    /// measure/specify a link in; the network layer only ever needs to delay one
    /// direction of a send, hence the one-way half). `n` must match the parsed
    /// matrix's own row/column count exactly (checked, not assumed).
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

    /// PHASE7 (AWS/distributed WAN-shaped runs): the real 10-AWS-region RTT matrix
    /// (`RTT_LATENCY_TABLE` above), expanded to `n x n` by mapping committee index `i`
    /// to region `i % 10` -- so `n > 10` reuses the 10 regions cyclically, matching
    /// starfish's `generate_latency_table` (`~/code/starfish/crates/starfish-core/
    /// src/network.rs` lines ~813-844). Same RTT/2 one-way halving convention as
    /// `uniform`/`from_rtt_csv`. Every node builds the identical full `n x n` table;
    /// `Committee::latency_map` picks out the row for `index_of(self)`.
    ///
    /// Fable audit FIX 2: the diagonal is forced to 0 (matching `uniform` and this
    /// struct's own `one_way_ms` doc, which states the diagonal is 0), a DELIBERATE
    /// deviation from `RTT_LATENCY_TABLE`'s own diagonal (1 ms, i.e. 0.5 ms one-way).
    /// Starfish's 1 ms diagonal is a same-region RTT placeholder (two distinct
    /// authorities that happen to land in the same region, `i % 10 == j % 10` for
    /// `i != j`) -- it is NOT a self-send delay, and that off-diagonal same-region
    /// case is deliberately left untouched below (still 0.5 ms one-way). Zeroing only
    /// `[i][i]` cannot change any injected delay either way: `Committee::latency_map`
    /// never emits a self entry (it always skips `other == myself`), so `[i][i]` is
    /// dead weight in this table regardless of its value.
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

    /// The one-way latency between committee-order indices `i` and `j` (`Duration::
    /// ZERO` for an out-of-range index -- defensive; unreachable given callers always
    /// build `i`/`j` from `Committee::index_of` over the SAME committee this table was
    /// sized against).
    pub fn one_way(&self, i: usize, j: usize) -> Duration {
        self.one_way_ms
            .get(i)
            .and_then(|row| row.get(j))
            .map_or(Duration::ZERO, |ms| {
                Duration::from_secs_f64(ms.max(0.0) / 1000.0)
            })
    }

    /// `n`, the committee size this table was built for (its row/column count).
    /// Named `dimension` rather than `len` so clippy's `len_without_is_empty` doesn't
    /// expect an `is_empty` counterpart that would make no sense here (an empty
    /// `LatencyTable` is not a meaningful state -- every constructor above always
    /// sizes it to the committee).
    pub fn dimension(&self) -> usize {
        self.one_way_ms.len()
    }

    /// Fable audit FIX 4: true if every off-diagonal entry is exactly 0, i.e. this
    /// table injects no delay at all on any inter-authority link. Distinguishes a
    /// genuinely WAN-shaping table (`uniform` with a positive RTT, `aws_rtt`, or a
    /// non-trivial `from_rtt_csv`) from an all-zero `uniform(n, 0.0)` table -- the
    /// latter is still `Some(LatencyTable)` on `Parameters::latency_table` (see that
    /// field's doc comment), e.g. from `node run`'s EXPLICIT `mimic_latency_ms:
    /// Some(0)` (`fab remote`'s current default), even though it delays nothing.
    /// Diagonal entries are ignored (never looked up by `Committee::latency_map`,
    /// which always skips `other == myself`) since they carry no injected delay
    /// regardless of their value (see `aws_rtt`'s doc comment).
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
            sequence_sync_chunk_digests: default_sequence_sync_chunk_digests(),
            sequence_sync_min_gap_views: default_sequence_sync_min_gap_views(),
            sequence_sync_max_sources: default_sequence_sync_max_sources(),
            sequence_sync_request_timeout_ms: default_sequence_sync_request_timeout_ms(),
            sequence_sync_inbound_capacity: default_sequence_sync_inbound_capacity(),
            sequence_install_window_views: default_sequence_install_window_views(),
            sequence_install_settle_ceiling: default_sequence_install_settle_ceiling(),
            sequence_install_enabled: default_sequence_install_enabled(),
            sequence_install_views_per_tick: default_sequence_install_views_per_tick(),
            sequence_install_digests_per_tick: default_sequence_install_digests_per_tick(),
            // AVAIL-ECHO-SPEC.md: off by default, so `Parameters::default()` and every
            // config predating the field keep the explicit-tuple `VantageAvail` path.
            echo_avail_claims: false,
            sync_retry_delay: 5_000,
            sync_retry_nodes: 3,
            batch_size: 500_000,
            max_batch_delay: 100,

            //Autobahn microbench configs
            use_optimistic_tips: true,
            use_parallel_proposals: true,
            k: 4,
            use_fast_path: true,
            fast_path_timeout: 500,
            use_ride_share: false,
            car_timeout: 2000,
            all_to_all: false,

            //Async simulation:
            simulate_asynchrony: false,
            asynchrony_start: 20_000,    //20 second in
            asynchrony_duration: 10_000, //10 seconds

            protocol: Protocol::default(),

            tx_mode: None,
            max_block_payload: default_max_block_payload(),
            delta_ms: default_delta_ms(),
            // No gate by default: every observation counts, as before this field.
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
    /// Reconcile the legacy `use_optimistic_tips` knob with `protocol`.
    /// `protocol` is authoritative: if an imported parameter file set the raw
    /// flag inconsistently with `protocol`, `protocol` wins and we warn. Called
    /// once after import, before the Core reads `use_optimistic_tips`.
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
        // NOTE: These log entries are needed to compute performance.
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
        // Fable audit FIX 4: `node run` now populates `latency_table` unconditionally
        // (see that field's doc comment) -- including for e.g. `fab remote`'s current
        // `mimic_latency_ms: Some(0)` default, whose expanded table is `Some` but
        // all-zero. Reporting `is_some()` alone would therefore claim latency was
        // "active" even when the table injects no delay at all; report the table's
        // dimension and whether it actually injects delay instead.
        match &self.latency_table {
            Some(table) if table.injects_delay() => {
                info!(
                    "Mimic latency table active (PHASE7-PREP-NOTES.md): {0}x{0} table, injecting delay",
                    table.dimension()
                );
            }
            Some(table) => {
                info!(
                    "Mimic latency table present but all-zero (PHASE7-PREP-NOTES.md): {0}x{0} table, no delay injected",
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
            "Ack watermarks (periodic per-lane availability broadcast, replaces \
             per-block acks) enabled? {}. Period: {} ms",
            self.ack_watermarks, self.ack_watermark_period_ms
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
            // KNOB 1/2 (measurement ablation): every run's log must self-document
            // which of the three arms it is -- see `reconnect_replay`'s own doc
            // comment for the arm definitions. The two flags are otherwise
            // indistinguishable from the rest of a run's log.
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

    /// Generates an in-memory committee (fresh keys, all addresses on 127.0.0.1) for
    /// `node local-benchmark` (PHASE2-SPEC.md §8). Port layout matches `fab remote`'s
    /// `config.py::Committee`: per authority, one `consensus_to_consensus` port, three
    /// primary ports
    /// (`primary_to_primary`, `worker_to_primary`, `metrics`), then four ports per
    /// worker (`primary_to_worker`, `transactions`, `worker_to_worker`, `metrics`).
    ///
    /// Fable audit (harness reproducibility): keys are generated at random, but the
    /// returned `keypairs` are sorted by public key before being handed back -- the
    /// exact same order `authorities`' `BTreeMap<PublicKey, _>` iterates in (i.e.
    /// `Committee::index_of` order). This guarantees `keypairs[i]` is always committee
    /// index `i`, so the harness's own node numbering (the printed `node-i` label,
    /// `node-i.json`, a `--latency-table` CSV's row `i`, and which nodes `--crash k`
    /// selects -- all of which index into this `Vec` positionally) stays aligned with
    /// `index_of`/`latency_map`'s committee order on every run, instead of the two
    /// orderings being a fresh random permutation of each other every time the process
    /// happens to draw different random keys. Without this, `node-i` in one run and
    /// `node-i` in the next could land on two different committee indices, silently
    /// misaligning any asymmetric `--latency-table` or targeted `--crash` across
    /// repeated runs. Purely a re-labeling of which (still-random) key occupies which
    /// slot -- committee membership, stakes, and every address/port assignment are
    /// unaffected; aggregate latency stats are invariant under relabeling of a fixed
    /// matrix, so this does not change any already-recorded headline number.
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

        // See this fn's doc comment: sort into the same order `authorities`' own
        // `BTreeMap<PublicKey, _>` iterates in, so `keypairs[i]` is always committee
        // index `i` (`Committee::index_of`), deterministically and reproducibly across
        // runs -- not a fresh random permutation of it every time.
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
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (2 N + 3) / 3 = 2f + 1 + (2k + 2)/3 = 2f + 1 + k = N - f
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        2 * total_votes / 3 + 1
    }

    /// Returns the stake required to reach availability (f+1).
    pub fn validity_threshold(&self) -> Stake {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (N + 2) / 3 = f + 1 + k/3 = f + 1
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

    /// PHASE7-PREP-NOTES.md (WAN-shaped local runs): `name`'s position in the
    /// committee's own deterministic order (`authorities`' `BTreeMap<PublicKey, _>`
    /// iteration order -- the same canonical ordering `Pacemaker`/`ControlLog::
    /// control_leader`/`Resolver` already rely on internally for their own
    /// party-indexed arrays/rotations, now exposed for `LatencyTable` indexing).
    /// `None` if `name` isn't a committee member.
    pub fn index_of(&self, name: &PublicKey) -> Option<usize> {
        self.authorities.keys().position(|k| k == name)
    }

    /// Every socket address `name`'s authority listens on -- its primary's three
    /// addresses, its consensus address, and every one of its workers' four
    /// addresses. The full set of endpoints one `LatencyTable` pair-entry should
    /// cover, since latency is modeled per AUTHORITY pair, not per individual service
    /// port. Empty if `name` isn't a committee member.
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

    /// PHASE7-PREP-NOTES.md (WAN-shaped local runs): builds `myself`'s own
    /// per-destination one-way latency map from `table` -- every socket address
    /// belonging to every OTHER authority maps to `table.one_way(index_of(myself),
    /// index_of(other))`. Feeds `ReliableSender`/`SimpleSender::with_latency(..)` at
    /// each protocol's own primary-to-primary spawn site (`Core::spawn`/
    /// `vantage::node::VantageCore::spawn`) -- the SAME table, resolved relative to
    /// whichever node calls this, applied identically to both protocols (the
    /// fairness point: a WAN-shaped run models the same network for either
    /// assembly). Empty (no entries -- equivalent to zero injected delay everywhere)
    /// if `myself` isn't a committee member.
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

/// Data-plane withholding fault injector (`node local-benchmark --withhold`,
/// `Parameters::withhold_senders`). Every node derives this PURELY locally, from data
/// it already has (its own identity, `committee`'s own sorted order, and the
/// configured sender count) -- never sent over the wire, and meant to be called once
/// at node startup and cached, not per send.
///
/// The first `withhold_senders` committee indices (0-based, `Committee::index_of`
/// order -- the same convention `--crash` (trailing) and `--load-nodes` (leading)
/// already use) are withholding senders. A withholding sender at index `i` withholds
/// its payload-dissemination broadcasts from exactly the staggered half `{(i+1),
/// (i+2), ..., (i + n/2)} mod n` (integer division, so an odd `n` rounds down) --
/// every other node, INCLUDING `i` itself, still receives normally.
///
/// Returns `None` when `self_pk` is not withholding: always the case when
/// `withhold_senders` is 0 (the default -- every node gets `None`, so the caller's
/// filter is skipped entirely and the send path is byte-identical to before this
/// feature existed), when `self_pk`'s own committee index is `>= withhold_senders`, or
/// when `self_pk` is not a committee member at all.
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

/// Data-plane withholding fault injector, TIME-WINDOWED variant (`node local-benchmark
/// --withhold-at`/`--withhold-for`, `Parameters::withhold_at_ms`/`withhold_for_ms`/
/// `withhold_window`): whether withholding is ACTIVE at instant `now`, i.e. whether a
/// withholding sender's own `withheld_destinations` filter should actually be applied
/// right now. `window` is `Parameters::withhold_window.as_deref()` -- the SAME shared,
/// in-process "has the window opened yet" cell every withholding node's own filter
/// site consults.
///
///   - `window` is `None` (`--withhold-at` was never given): WHOLE-RUN withholding,
///     exactly c35fc4a's original behavior -- ALWAYS active. This is the only case
///     reached when `--withhold-at` is absent, so a withholding sender with no window
///     configured filters for the entire run, byte-identical to before this feature
///     existed.
///   - `window` is `Some(cell)` and UNARMED (`cell.get()` is `None`, i.e. `node
///     local-benchmark::run` hasn't yet reached measurement start): NOT active -- the
///     window's start-to-be is necessarily still in the future, so there is nothing to
///     withhold from yet.
///   - `window` is `Some(cell)` and ARMED (`Some((start, end))`): active iff `now` is
///     in the half-open interval `[start, end)`.
///
/// Callers consult this ONCE per send (never cached) -- time-windowed withholding can
/// turn on/off mid-run, unlike the spatial `withheld_destinations` filter (which is
/// fixed for a node's whole lifetime and IS resolved once at spawn).
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

    /// PHASE7 (AWS PREP): the real contract the distributed `node run` must honor --
    /// the exact `parameters.json` the fab campaign task emits (via
    /// `benchmark/config.py`'s `NodeParameters(...).print()`) must deserialize into
    /// `config::Parameters`, select Vantage, carry `delta_ms`, and carry the
    /// deployable `mimic_latency_ms` mimic-latency knob (the only mechanism able to
    /// inject WAN-shaped latency on the distributed path, since `latency_table` is
    /// `#[serde(skip)]`). This JSON is byte-identical in KEYS/VALUES to the campaign
    /// `node_params` dict in `benchmark/fabfile.py::campaign`.
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
        // `node run` calls this immediately after import.
        params.reconcile_protocol();

        assert_eq!(params.protocol, Protocol::Vantage);
        assert_eq!(params.delta_ms, 150);
        assert_eq!(params.mimic_latency_ms, Some(100));
        // Transport batching is ON by default (5 ms / 64 KiB per-destination
        // coalescing) -- a parameters file that omits the key gets it enabled.
        assert!(params.batch_messages);
        // `vantage_gc_window_views` is absent from this (pre-existing shape) file, so the
        // property under test is that it still deserializes and picks up the default. That
        // default is now 200, NOT `gc_depth`'s 50: at the measured 8.7-11.5 views/s, 50
        // views is only 4-6 seconds of retained history, which is less than the lag a
        // straggler routinely accumulates -- see `default_vantage_gc_window_views`.
        assert_eq!(params.vantage_gc_window_views, 200);
        // Deliberately DECOUPLED from `gc_depth` now. This used to assert equality to show
        // that splitting the two knobs changed nothing; the split has since been used, and
        // asserting equality again would silently re-tie a Vantage view window to an
        // Autobahn round count.
        assert_ne!(params.vantage_gc_window_views, params.gc_depth);
        assert_eq!(
            params.gc_depth, 50,
            "the Autobahn knob is untouched by that change"
        );
        // `latency_table` is `#[serde(skip)]`: never present in the file, always
        // `None` after deserialization -- `node run` builds it from
        // `mimic_latency_ms` at spawn.
        assert!(params.latency_table.is_none());
        // KNOB 1/2 (measurement ablation): absent from this (pre-existing shape)
        // file -- must default to today's behavior exactly (replay enabled, 2s
        // backoff cap), same "splitting a knob apart changes nothing for an
        // existing parameter file" guarantee `vantage_gc_window_views` is pinned
        // against just above.
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

    /// `withheld_destinations`: the exact stagger + wraparound example the feature's
    /// own spec walks through -- n=20, sender index 15 blocks {16..19, 0..5}, exactly
    /// 10 (= floor(20/2)) destinations.
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

    /// `withhold_active`: no window configured (`--withhold-at` absent) is ALWAYS
    /// active, regardless of `now` -- whole-run withholding, matching c35fc4a's
    /// original (pre-window) behavior exactly.
    #[test]
    fn withhold_active_no_window_is_always_active() {
        let now = Instant::now();
        assert!(withhold_active(None, now));
        assert!(withhold_active(None, now + Duration::from_secs(1_000_000)));
    }

    /// A configured-but-UNARMED window (`node local-benchmark::run` hasn't yet
    /// reached measurement start) is NOT active -- the window's start-to-be is
    /// necessarily still in the future.
    #[test]
    fn withhold_active_configured_unarmed_is_inactive() {
        let cell: OnceLock<(Instant, Instant)> = OnceLock::new();
        assert!(!withhold_active(Some(&cell), Instant::now()));
    }

    /// An ARMED window is active strictly inside `[start, end)`, and inactive
    /// everywhere else (before `start`, at/after `end`).
    #[test]
    fn withhold_active_armed_inside_and_outside_window() {
        let cell: OnceLock<(Instant, Instant)> = OnceLock::new();
        let base = Instant::now();
        let start = base + Duration::from_secs(10);
        let end = base + Duration::from_secs(20);
        cell.set((start, end)).unwrap();

        assert!(!withhold_active(Some(&cell), base + Duration::from_secs(5))); // before
        assert!(withhold_active(Some(&cell), start)); // at start (inclusive)
        assert!(withhold_active(Some(&cell), base + Duration::from_secs(15))); // inside
        assert!(!withhold_active(Some(&cell), end)); // at end (exclusive)
        assert!(!withhold_active(
            Some(&cell),
            base + Duration::from_secs(25)
        )); // after
    }

    /// KNOB 1/2 (measurement ablation): `Parameters::default()` reproduces today's
    /// existing behavior exactly -- the reconnect-replay mechanism enabled and the
    /// 2s retry-backoff cap, both previously hardcoded/unconditional.
    #[test]
    fn reconnect_replay_and_retry_backoff_default_to_todays_behavior() {
        let params = Parameters::default();
        assert!(params.reconnect_replay);
        assert_eq!(params.retry_backoff_max_ms, 2000);
    }
}

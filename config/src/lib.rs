// Copyright(C) Facebook, Inc. and its affiliates.
use crypto::{generate_production_keypair, PublicKey, SecretKey};
use log::{info, warn};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, OpenOptions};
use std::io::BufWriter;
use std::io::Write as _;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
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
            let file = OpenOptions::new().create(true).write(true).truncate(true).open(path)?;
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
}


impl Protocol {
    /// The `use_optimistic_tips` value implied by this protocol when the
    /// Autobahn code paths run. `None` for Vantage (the flag is irrelevant on
    /// that path).
    pub fn implied_optimistic_tips(&self) -> Option<bool> {
        match self {
            Protocol::AutobahnOptimistic => Some(true),
            Protocol::AutobahnSeamless => Some(false),
            Protocol::Vantage => None,
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
    pub use_optimistic_tips: bool,     //default = true (TODO: implement non optimistic tip option)

    pub use_parallel_proposals: bool,  //default = true (TODO: implement sequential slot option)
    pub k: u64, //Max open conensus instances at a time.

    pub use_fast_path: bool,           //default = false
    pub fast_path_timeout: u64,

    pub use_ride_share: bool,
    pub car_timeout: u64,

    //asynchrony simulation:
    pub simulate_asynchrony: bool,
    pub asynchrony_start: u64,
    pub asynchrony_duration: u64,

    /// The consensus protocol assembly to run. Authoritative over
    /// `use_optimistic_tips` (see `reconcile_protocol`). `#[serde(default)]`
    /// keeps pre-Phase-2 parameter files valid.
    #[serde(default)]
    pub protocol: Protocol,

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

    /// PHASE7-PREP-NOTES.md (optional, WAN-shaped local runs): an optional
    /// per-authority-pair one-way latency table, applied to THIS node's own
    /// primary-to-primary connections at spawn time via `Committee::latency_map`
    /// (`Core::spawn`/`vantage::node::VantageCore::spawn`, both protocols
    /// identically). Never round-trips through `parameters.json`/`fab`
    /// (`#[serde(skip)]`) -- it is a benchmark-diagnostic-only, in-process value built
    /// by `node local-benchmark` from `--latency-table`/`--mimic-latency-ms`, not a
    /// deployable setting. `None` (the default -- also what every EXISTING
    /// `parameters.json` deserializes to, since the field is entirely absent from
    /// their JSON) means zero injected delay, i.e. byte-identical current behavior --
    /// required for invariant 4 (both Autobahn paths) and for Vantage's own
    /// already-recorded fault-free/crash-fault gate numbers to stay reproducible.
    #[serde(skip)]
    pub latency_table: Option<Arc<LatencyTable>>,

    /// METRICS-DASHBOARD-SPEC.md §8: network-level lz4 compression, off by default
    /// (`#[serde(default)]` = `false`) -- byte-identical framing when off (no
    /// compress/decompress call is even made, see `network` crate). Applied uniformly
    /// by the `network` crate to every sender/receiver, all three protocols
    /// identically. Committee-wide consistent by construction: every node's
    /// `Parameters` comes from the same generated config, so a mixed on/off committee
    /// isn't a supported configuration (a node expecting compressed frames would fail
    /// to decode an uncompressed peer's traffic, and vice versa).
    #[serde(default)]
    pub compress_network: bool,
}

fn default_max_block_payload() -> usize {
    16
}

fn default_delta_ms() -> u64 {
    1000
}

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
        let err = |message: String| ConfigError::ImportError { file: path.to_string(), message };
        let data = fs::read_to_string(path).map_err(|e| err(e.to_string()))?;
        let mut rows: Vec<Vec<f64>> = Vec::new();
        for line in data.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let row: Result<Vec<f64>, _> = line.split(',').map(|cell| cell.trim().parse::<f64>()).collect();
            rows.push(row.map_err(|e| err(format!("non-numeric cell in row {}: {}", rows.len(), e)))?);
        }
        if rows.len() != n || rows.iter().any(|r| r.len() != n) {
            return Err(err(format!(
                "expected a {n}x{n} RTT matrix, got {} data row(s) with lengths {:?}",
                rows.len(),
                rows.iter().map(|r| r.len()).collect::<Vec<_>>()
            )));
        }
        let one_way_ms = rows.into_iter().map(|row| row.into_iter().map(|rtt_ms| rtt_ms / 2.0).collect()).collect();
        Ok(Self { one_way_ms })
    }

    /// The one-way latency between committee-order indices `i` and `j` (`Duration::
    /// ZERO` for an out-of-range index -- defensive; unreachable given callers always
    /// build `i`/`j` from `Committee::index_of` over the SAME committee this table was
    /// sized against).
    pub fn one_way(&self, i: usize, j: usize) -> Duration {
        self.one_way_ms
            .get(i)
            .and_then(|row| row.get(j))
            .map_or(Duration::ZERO, |ms| Duration::from_secs_f64(ms.max(0.0) / 1000.0))
    }
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            timeout_delay: 1_000,
            header_size: 1_000,
            max_header_delay: 100,
            gc_depth: 50,
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

            //Async simulation:
            simulate_asynchrony: false,
            asynchrony_start: 20_000, //20 second in
            asynchrony_duration: 10_000, //10 seconds

            protocol: Protocol::default(),
            max_block_payload: default_max_block_payload(),
            delta_ms: default_delta_ms(),
            latency_table: None,
            compress_network: false,
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
        info!("Sync retry delay set to {} ms", self.sync_retry_delay);
        info!("Sync retry nodes set to {} nodes", self.sync_retry_nodes);
        info!("Batch size set to {} B", self.batch_size);
        info!("Max batch delay set to {} ms", self.max_batch_delay);

        info!("Fast path enabled? {}. Fast timeout: {}", self.use_fast_path, self.fast_path_timeout);
        info!("Optimistic tips enabled? {}", self.use_optimistic_tips);
        info!("Parallel Proposals enabled? {}. K: {}", self.use_parallel_proposals, self.k);
        info!("Ride share enabled? {}. Car timeout: {}", self.use_ride_share, self.car_timeout);
        info!("Max block payload set to {} entries", self.max_block_payload);
        info!("Vantage delta set to {} ms", self.delta_ms);
        if self.latency_table.is_some() {
            info!("Mimic latency table active (PHASE7-PREP-NOTES.md)");
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
                    let authority = Authority { stake, consensus: ConsensusAddresses { consensus_to_consensus: address }, primary: PrimaryAddresses { primary_to_primary: address, worker_to_primary: address, metrics: address }, workers: HashMap::new() };
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
            out.extend([w.primary_to_worker, w.transactions, w.worker_to_worker, w.metrics]);
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
    pub fn latency_map(&self, myself: &PublicKey, table: &LatencyTable) -> HashMap<SocketAddr, Duration> {
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
        self.authorities.get(name).map(|x| x.consensus.consensus_to_consensus)
    }

    pub fn broadcast_addresses(&self, myself: &PublicKey) -> Vec<(PublicKey, SocketAddr)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, x)| (*name, x.consensus.consensus_to_consensus))
            .collect()
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

// Copyright(C) Facebook, Inc. and its affiliates.
use crypto::{generate_production_keypair, PublicKey, SecretKey, Hash};
use log::{info, warn};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, OpenOptions};
use std::io::BufWriter;
use std::io::Write as _;
use std::net::SocketAddr;
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
            let file = OpenOptions::new().create(true).write(true).open(path)?;
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
pub enum Protocol {
    /// Autobahn as shipped/evaluated (`use_optimistic_tips = true`).
    AutobahnOptimistic,
    /// Autobahn with certified-tips-only cut formation (`use_optimistic_tips = false`).
    AutobahnSeamless,
    /// Signature-free AGB protocol (implemented in Phase 3+).
    Vantage,
}

impl Default for Protocol {
    fn default() -> Self {
        Self::AutobahnOptimistic
    }
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
}

fn default_max_block_payload() -> usize {
    16
}

fn default_delta_ms() -> u64 {
    1000
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
    /// `node local-benchmark` (PHASE2-SPEC.md §8) -- the in-process analog of
    /// `config.py::LocalCommittee`. Port layout is identical to that harness's: per
    /// authority, one `consensus_to_consensus` port, three primary ports
    /// (`primary_to_primary`, `worker_to_primary`, `metrics`), then four ports per
    /// worker (`primary_to_worker`, `transactions`, `worker_to_worker`, `metrics`).
    /// Returns the committee alongside each authority's freshly generated keypair, in
    /// the same order, since the caller (not this constructor) owns spawning nodes.
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

        (Self { authorities }, keypairs)
    }

    /// Returns the number of authorities.
    pub fn size(&self) -> usize {
        self.authorities.len()
    }

    /// Return the stake of a specific authority.
    pub fn stake(&self, name: &PublicKey) -> Stake {
        self.authorities.get(&name).map_or_else(|| 0, |x| x.stake)
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
        (total_votes + 2) / 3
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
            .ok_or_else(|| ConfigError::NotInCommittee(*to))
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
            .ok_or_else(|| ConfigError::NotInCommittee(*to))
    }

    /// Returns the addresses of all primaries except `myself`.
    pub fn others_primaries(&self, myself: &PublicKey) -> Vec<(PublicKey, PrimaryAddresses)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, authority)| (*name, authority.primary.clone()))
            .collect()
    }

    /// Returns the addresses of a specific worker (`id`) of a specific authority (`to`).
    pub fn worker(&self, to: &PublicKey, id: &WorkerId) -> Result<WorkerAddresses, ConfigError> {
        self.authorities
            .iter()
            .find(|(name, _)| name == &to)
            .map(|(_, authority)| authority)
            .ok_or_else(|| ConfigError::NotInCommittee(*to))?
            .workers
            .iter()
            .find(|(worker_id, _)| worker_id == &id)
            .map(|(_, worker)| worker.clone())
            .ok_or_else(|| ConfigError::NotInCommittee(*to))
    }

    /// Returns the addresses of all our workers.
    pub fn our_workers(&self, myself: &PublicKey) -> Result<Vec<WorkerAddresses>, ConfigError> {
        self.authorities
            .iter()
            .find(|(name, _)| name == &myself)
            .map(|(_, authority)| authority)
            .ok_or_else(|| ConfigError::NotInCommittee(*myself))?
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
            .ok_or_else(|| ConfigError::NotInCommittee(*myself))
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

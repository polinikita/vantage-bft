// Copyright(C) Facebook, Inc. and its affiliates.
use crate::certificate_waiter::CertificateWaiter;
use crate::committer::Committer;
use crate::core::Core;
use crate::error::DagError;
use crate::garbage_collector::GarbageCollector;
use crate::header_waiter::HeaderWaiter;
use crate::helper::Helper;
use crate::leader::LeaderElector;
use crate::messages::{
    Ack, Certificate, ConsensusMessage, ConsensusRequest, ConsensusVote, Header, Proposal, Timeout,
    Vote, TC,
};
use crate::payload_receiver::PayloadReceiver;
use crate::proposer::Proposer;
use crate::synchronizer::Synchronizer;
use crate::vantage::agb::{Echo, Ready, ViewProposal};
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, Parameters, Protocol, WorkerId};
use crypto::{Digest, PublicKey, SignatureService};
use log::info;
use metrics::{start_prometheus_server, MetricReporter, Metrics};
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use prometheus::Registry;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

/// The default channel capacity for each channel of the primary.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The round number.
pub type Height = u64;
/// The view number (of consensus)
pub type View = u64;
// The slot (sequence) number of consensus
pub type Slot = u64;

// clippy::large_enum_variant: `Timeout(Timeout)` (~560 B) makes this enum large --
// boxing it is wire-compatible (serde's `Box<T>` impl serializes identically to `T`)
// but would still touch every `PrimaryMessage::X(...)` construction and `match`/
// `if let` destructuring site across the audited dispatch code in core.rs,
// vantage/node.rs, primary.rs, committer.rs, header_waiter.rs, helper.rs for a pure
// stack-size optimization; not done.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryMessage {
    Header(Header, bool),
    Vote(Vote),
    Certificate(Certificate),
    Timeout(Timeout),
    TC(TC),
    ConsensusMessage(ConsensusMessage),
    ConsensusRequest(ConsensusRequest),
    ConsensusVote(ConsensusVote),
    CertificatesRequest(Vec<Digest>, /* requestor */ PublicKey),
    HeadersRequest(Vec<Digest>, /* requestor */ PublicKey),
    ProposalHeadersRequest(Proposal, Height, /* requestor */ PublicKey),
    // Vantage only (PHASE3-SPEC.md §5). Appended last -- bincode wire compat, same rule
    // as `PrimaryWorkerMessage::Committed` above: this is a variant index, inserting it
    // anywhere else would shift every following discriminant.
    VantageAck(Ack),
    // Vantage AGB engine (PHASE4-SPEC.md §2). Appended after `VantageAck`, in the
    // spec's declared order -- same bincode wire-compat rule as above.
    VantagePropose(ViewProposal),
    VantageEcho(Echo),
    // PHASE5-SPEC.md §2 (D5-2): `EchoSkip`/`NoReady` gain a trailing `View` wish
    // piggyback field, same convention as `Echo`/`Ready`'s own new `wish` field.
    VantageEchoSkip(View, /* sender */ PublicKey, /* wish */ View),
    VantageReady(Ready),
    VantageNoReady(View, /* sender */ PublicKey, /* wish */ View),
    // Vantage WISH pacemaker (PHASE5-SPEC.md §2). Appended after `VantageNoReady`, last
    // -- same bincode wire-compat rule as every other Vantage-only variant above.
    VantageWish(View, /* sender */ PublicKey),
    // Vantage resolution + control log (PHASE6-SPEC.md §5). Appended after
    // `VantageWish`, in the spec's declared order plus D6-6's necessary commit-vote
    // addition -- same bincode wire-compat rule as above.
    CompReport(View, Digest, /* sender */ PublicKey),
    ControlInit(crate::vantage::ControlProposal, Option<ViewProposal>),
    ControlEcho(crate::vantage::ControlProposal, /* sender */ PublicKey),
    ControlReady(crate::vantage::ControlProposal, /* sender */ PublicKey),
    ControlFetch(View, Digest, /* requester */ PublicKey),
    ControlServe(View, ViewProposal),
    // D6-6 (PHASE6-NOTES.md): the paper's Vote step ("send <commit, curr_round> to all
    // parties") has no listed wire message in the spec's §5 enumeration but is
    // load-bearing for the Commit rule -- added here, appended last.
    ControlCommit(crate::vantage::Round, /* sender */ PublicKey),
    // Simple-IT's reliable-notification round-timeout messages (Fig. 4) -- appended
    // last per the spec's "whatever round-timeout notification message the reference
    // requires (append last)".
    ControlTimeoutVote(crate::vantage::Round, /* sender */ PublicKey),
    ControlTimeoutAccept(crate::vantage::Round, /* sender */ PublicKey),
    // Simple-IT cut-consensus (a fourth, separate protocol assembly -- primary/src/
    // simpleit/**), reusing Vantage's own data plane (`Header`/`HeadersRequest`/
    // `VantageAck` above) for dissemination and adding these five for its own
    // cut-consensus layer. Appended last -- same bincode wire-compat rule as every
    // other protocol-specific variant above. `SimpleItTimeout`/`SimpleItTimeoutAccept`
    // are deliberately distinct types from the pre-existing `Timeout` (Autobahn's,
    // above) and from `ControlTimeoutVote`/`ControlTimeoutAccept` (Vantage's own
    // resolution-layer notifications) -- three unrelated protocols' round-timeout
    // messages, never unified into one wire type. NOTE: this used to be six variants,
    // including `SimpleItCutCertificate` -- removed (arXiv:2606.14404 Fig.-2 rewrite,
    // see `primary/src/simpleit/engine.rs`'s module doc comment): that message let a
    // party assert a signature-free "notarization" for any `cut_id`, checked only for
    // committee membership, never for whether its listed voters actually voted. Each
    // party now marks a round safe by counting `SimpleItCutVote`s itself. Removing a
    // non-last variant shifts every later discriminant by one, which the bincode
    // wire-compat rule above would normally forbid -- safe here only because every
    // node in a run is the same compiled binary (this codebase's actual deployment
    // model; see `node/src/local_benchmark.rs` and the nightly-binary release flow),
    // so there is no old/new version pair that ever needs to decode each other's
    // bytes.
    SimpleItCutProposal(crate::simpleit::CutProposal),
    SimpleItCutVote(crate::simpleit::CutVote),
    SimpleItDecide(crate::simpleit::Decide),
    SimpleItTimeout(crate::simpleit::Timeout),
    SimpleItTimeoutAccept(crate::simpleit::TimeoutAccept),
    // Simple-IT cut-proposal repair: mirrors `ControlFetch`/`ControlServe` above
    // exactly (see `vantage::control::ControlLog::on_control_fetch`/
    // `on_control_serve`'s identical role for Vantage's own carrier bodies) -- closes
    // a liveness gap where a party locally marks round r safe (via vote-counting,
    // naming only a `cut_id`) without ever receiving round r's own `CutProposal`.
    // Appended last -- same bincode wire-compat rule as every other
    // protocol-specific variant above.
    SimpleItCutFetch(
        crate::simpleit::CutRound,
        Digest,
        /* requester */ PublicKey,
    ),
    SimpleItCutServe(crate::simpleit::CutProposal),
    // Optional, flag-gated periodic per-lane availability watermark
    // (`Parameters::ack_watermarks`) -- replaces per-block acks with one compact
    // broadcast per period naming, for each author this party holds, the greatest
    // DirectPub (height, head digest) pair (lanes are hash chains, so this one pair
    // covers the whole verified prefix through that height). Shared by both Vantage
    // and Simple-IT (same `LaneManager`/`AckAggregator` data plane). Appended last --
    // same bincode wire-compat rule as every other protocol-specific variant above.
    VantageAvail(Vec<crate::vantage::AvailEntry>, /* sender */ PublicKey),
    // Vantage only, flag-gated (`Parameters::batched_anchors`, signature-free.tex's
    // "Batched resolution entries"): the vector-`M` counterparts of
    // `VantagePropose`/`VantageEcho`/`VantageReady`/`ControlInit`/`ControlServe`.
    // Deliberately NEVER on the pre-existing variants above (see `agb::ViewProposal`'s
    // doc comment) -- constructed only when the flag is on AND the proposer's
    // recovery-turn prefix has `>= 2` entries; a run with the flag off never
    // constructs or sends any of these five, so the flag-off wire format is
    // untouched. Appended last -- same bincode wire-compat rule as every other
    // protocol-specific variant above.
    VantageProposeBatch(crate::vantage::BatchViewProposal),
    VantageEchoBatch(crate::vantage::EchoBatch),
    VantageReadyBatch(crate::vantage::ReadyBatch),
    ControlInitBatch(
        crate::vantage::ControlProposal,
        Option<crate::vantage::BatchViewProposal>,
    ),
    ControlServeBatch(View, crate::vantage::BatchViewProposal),
    // Vantage only, signature-free.tex's "Grounded post-ready skip" (par:skip-seal).
    // Unconditional protocol behavior (no flag, unlike the variants above): the
    // one-shot statement `<skip-vote, u>`, sent by a correct party after its own
    // durably-emitted no-ready, a first-hand 2f+1 echo-skip census, and a free
    // resolution stance. Appended last -- same bincode wire-compat rule as every
    // other protocol-specific variant above.
    VantageSkipVote(View, /* sender */ PublicKey),
    // Vantage only, flag-gated (`Parameters::digest_statements`, signature-free.tex
    // §8.3's "Digest-named AGB statements"): the digest-named counterparts of
    // `VantageEcho`/`VantageReady` (naming the proposal by `hash(B_v)` instead of
    // carrying it), plus fetch/serve for the `ViewProposal` body itself. Constructed
    // only when the flag is on; a run with the flag off never sends any of these
    // four -- reception handles them unconditionally either way (see
    // `vantage::agb::DigestStatements`'s own module doc comment). Appended last --
    // same bincode wire-compat rule as every other protocol-specific variant above.
    VantageEchoDigest(crate::vantage::EchoDigest),
    VantageReadyDigest(crate::vantage::ReadyDigest),
    VantageBodyFetch(View, Digest, /* requester */ PublicKey),
    VantageBodyServe(View, ViewProposal),
}

impl PrimaryMessage {
    /// METRICS-DASHBOARD-SPEC.md §1: the wire variant name used as the `type` label
    /// for `network_messages_received_total`/`network_bytes_received_total` at
    /// receiver-dispatch sites whose match has a catch-all arm (so a literal string
    /// per arm isn't available the way it is at send call sites, which construct one
    /// specific variant at a time).
    pub fn type_name(&self) -> &'static str {
        match self {
            PrimaryMessage::Header(..) => "Header",
            PrimaryMessage::Vote(..) => "Vote",
            PrimaryMessage::Certificate(..) => "Certificate",
            PrimaryMessage::Timeout(..) => "Timeout",
            PrimaryMessage::TC(..) => "TC",
            PrimaryMessage::ConsensusMessage(..) => "ConsensusMessage",
            PrimaryMessage::ConsensusRequest(..) => "ConsensusRequest",
            PrimaryMessage::ConsensusVote(..) => "ConsensusVote",
            PrimaryMessage::CertificatesRequest(..) => "CertificatesRequest",
            PrimaryMessage::HeadersRequest(..) => "HeadersRequest",
            PrimaryMessage::ProposalHeadersRequest(..) => "ProposalHeadersRequest",
            PrimaryMessage::VantageAck(..) => "VantageAck",
            PrimaryMessage::VantagePropose(..) => "VantagePropose",
            PrimaryMessage::VantageEcho(..) => "VantageEcho",
            PrimaryMessage::VantageEchoSkip(..) => "VantageEchoSkip",
            PrimaryMessage::VantageReady(..) => "VantageReady",
            PrimaryMessage::VantageNoReady(..) => "VantageNoReady",
            PrimaryMessage::VantageWish(..) => "VantageWish",
            PrimaryMessage::CompReport(..) => "CompReport",
            PrimaryMessage::ControlInit(..) => "ControlInit",
            PrimaryMessage::ControlEcho(..) => "ControlEcho",
            PrimaryMessage::ControlReady(..) => "ControlReady",
            PrimaryMessage::ControlFetch(..) => "ControlFetch",
            PrimaryMessage::ControlServe(..) => "ControlServe",
            PrimaryMessage::ControlCommit(..) => "ControlCommit",
            PrimaryMessage::ControlTimeoutVote(..) => "ControlTimeoutVote",
            PrimaryMessage::ControlTimeoutAccept(..) => "ControlTimeoutAccept",
            PrimaryMessage::SimpleItCutProposal(..) => "SimpleItCutProposal",
            PrimaryMessage::SimpleItCutVote(..) => "SimpleItCutVote",
            PrimaryMessage::SimpleItDecide(..) => "SimpleItDecide",
            PrimaryMessage::SimpleItTimeout(..) => "SimpleItTimeout",
            PrimaryMessage::SimpleItTimeoutAccept(..) => "SimpleItTimeoutAccept",
            PrimaryMessage::SimpleItCutFetch(..) => "SimpleItCutFetch",
            PrimaryMessage::SimpleItCutServe(..) => "SimpleItCutServe",
            PrimaryMessage::VantageAvail(..) => "VantageAvail",
            PrimaryMessage::VantageProposeBatch(..) => "VantageProposeBatch",
            PrimaryMessage::VantageEchoBatch(..) => "VantageEchoBatch",
            PrimaryMessage::VantageReadyBatch(..) => "VantageReadyBatch",
            PrimaryMessage::ControlInitBatch(..) => "ControlInitBatch",
            PrimaryMessage::ControlServeBatch(..) => "ControlServeBatch",
            PrimaryMessage::VantageSkipVote(..) => "VantageSkipVote",
            PrimaryMessage::VantageEchoDigest(..) => "VantageEchoDigest",
            PrimaryMessage::VantageReadyDigest(..) => "VantageReadyDigest",
            PrimaryMessage::VantageBodyFetch(..) => "VantageBodyFetch",
            PrimaryMessage::VantageBodyServe(..) => "VantageBodyServe",
        }
    }
}

/// METRICS-DASHBOARD-SPEC.md §1: shared by every `MessageHandler::dispatch` impl in
/// this crate (a no-op if `metrics` is `None`). `len` is the serialized payload size
/// (no frame prefix -- `network_bytes_received_total` is "beyond starfish", the
/// serialized-length-is-in-hand convention noted in the spec).
pub(crate) fn record_typed_received(metrics: &Arc<Metrics>, msg_type: &'static str, len: usize) {
    metrics
        .network_messages_received_total
        .with_label_values(&[msg_type])
        .inc();
    metrics
        .network_bytes_received_total
        .with_label_values(&[msg_type])
        .inc_by(len as u64);
}

/// The messages sent by the primary to its workers.
// bincode wire compat: `Committed` must stay appended LAST -- it is a variant index,
// not a named field, so inserting it anywhere else would shift every discriminant that
// follows it and break decoding against any node running different source.
#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryWorkerMessage {
    /// The primary indicates that the worker need to sync the target missing batches.
    Synchronize(Vec<Digest>, /* target */ PublicKey),
    /// The primary indicates a round update.
    Cleanup(Height),
    /// Benchmark-only (PHASE2-SPEC.md #5, amended): the primary indicates these batch
    /// digests were just committed, carrying the commit instant itself (UTC millis,
    /// taken once at the "Committed B..." log site) so the worker measures
    /// submission -> commit exactly -- not submission -> whenever this notification
    /// happened to reach the front of the worker's queue. Starfish parity: starfish
    /// observes latency at its own commit handler, not at a downstream consumer.
    Committed(u64 /* commit UTC-millis */, Vec<Digest>),
}

impl PrimaryWorkerMessage {
    /// METRICS-DASHBOARD-SPEC.md §1: see `PrimaryMessage::type_name`.
    pub fn type_name(&self) -> &'static str {
        match self {
            PrimaryWorkerMessage::Synchronize(..) => "Synchronize",
            PrimaryWorkerMessage::Cleanup(..) => "Cleanup",
            PrimaryWorkerMessage::Committed(..) => "Committed",
        }
    }
}

/// The messages sent by the workers to their primary.
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerPrimaryMessage {
    /// The worker indicates it sealed a new batch.
    OurBatch(Digest, WorkerId),
    /// The worker indicates it received a batch's digest from another authority.
    OthersBatch(Digest, WorkerId),
}

impl WorkerPrimaryMessage {
    /// METRICS-DASHBOARD-SPEC.md §1: see `PrimaryMessage::type_name`.
    pub fn type_name(&self) -> &'static str {
        match self {
            WorkerPrimaryMessage::OurBatch(..) => "OurBatch",
            WorkerPrimaryMessage::OthersBatch(..) => "OthersBatch",
        }
    }
}

pub struct Primary;

impl Primary {
    // clippy::too_many_arguments: see `Committer::spawn`'s identical justification --
    // this is the top-level assembly constructor wiring every channel between the
    // two protocol families; a params struct would only add indirection, not reduce
    // the audited call site's actual argument count.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        signature_service: SignatureService,
        store: Store,
        _tx_consensus: Sender<Certificate>,
        _tx_committer: Sender<Certificate>,
        rx_committer: Receiver<Certificate>,
        rx_consensus: Receiver<Certificate>,
        _tx_sailfish: Sender<Header>,
        _rx_pushdown_cert: Receiver<Certificate>,
        rx_request_header_sync: Receiver<Digest>,
        tx_output: Sender<Header>,
    ) -> (Arc<Metrics>, Arc<MetricReporter>, Registry) {
        let (tx_others_digests, rx_others_digests) = channel(CHANNEL_CAPACITY);
        let (tx_our_digests, rx_our_digests) = channel(CHANNEL_CAPACITY);
        let (tx_parents, rx_parents) = channel(CHANNEL_CAPACITY);
        let (tx_headers, rx_headers) = channel(CHANNEL_CAPACITY);
        let (tx_sync_headers, rx_sync_headers) = channel(CHANNEL_CAPACITY);
        let (tx_sync_certificates, rx_sync_certificates) = channel(CHANNEL_CAPACITY);
        let (tx_headers_loopback, rx_headers_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_certificates_loopback, _rx_certificates_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_primary_messages, rx_primary_messages) = channel(CHANNEL_CAPACITY);
        let (tx_cert_requests, rx_cert_requests) = channel(CHANNEL_CAPACITY);
        let (tx_header_requests, rx_header_requests) = channel(CHANNEL_CAPACITY);
        let (tx_instance, rx_instance) = channel(CHANNEL_CAPACITY);
        let (tx_header_waiter_instances, rx_header_waiter_instances) = channel(CHANNEL_CAPACITY);
        let (tx_commit, rx_commit) = channel(CHANNEL_CAPACITY);
        let (_tx_mempool, rx_mempool) = channel(CHANNEL_CAPACITY);

        // Write the parameters to the logs.
        // NOTE: These log entries are needed to compute performance.
        parameters.log();

        // Boot the (always-on, starfish-parity) Prometheus metrics server. Primary's
        // registry is near-empty in Phase 2 -- nothing here observes into `metrics` yet
        // -- but it is wired up now so Phase 3+ only has to add counters, not plumbing.
        let metrics_address = committee
            .primary(&name)
            .expect("Our public key or worker id is not in the committee")
            .metrics;
        let mut binding_metrics_address = metrics_address;
        binding_metrics_address.set_ip("0.0.0.0".parse().unwrap());
        let registry = Registry::new();
        let (metrics, reporter) = Metrics::new(&registry);
        // METRICS-DASHBOARD-SPEC.md §8: write-once at boot.
        metrics.set_protocol_info(parameters.protocol.label());
        reporter.clone().start();
        start_prometheus_server(binding_metrics_address, &registry);
        info!("Primary {} metrics listening on {}", name, metrics_address);

        // Atomic variable use to synchronizer all tasks with the latest consensus round. This is only
        // used for cleanup. The only tasks that write into this variable is `GarbageCollector`.
        let consensus_round = Arc::new(AtomicU64::new(0));

        // Transport-level batching, resolved once here (mirrors `compress_network`'s
        // own plumbing -- a single `Parameters`-derived value threaded into every
        // `Reliable`/`SimpleSender` and `network::Receiver` this primary spawns,
        // both protocols identically).
        let batch = network::BatchConfig {
            enabled: parameters.batch_messages,
            max_bytes: parameters.batch_max_bytes,
            max_delay_ms: parameters.batch_max_delay_ms,
        };

        // SECURITY (Fable audit): symmetric pairwise-MAC authenticated channels
        // (`Parameters::authenticate_channels`), resolved once here -- shared by
        // every handler/sender below, both protocol branches, exactly like `batch`
        // above. `authenticate_channels` on with no `mac_secret` is a misconfiguration
        // (would otherwise silently run unauthenticated); panic loudly rather than
        // let it pass.
        let channel_auth: Option<Arc<crypto::PairwiseKeys>> = if parameters.authenticate_channels {
            let secret = parameters
                .mac_secret
                .expect("authenticate_channels is set but mac_secret is None (misconfiguration)");
            Some(Arc::new(committee.pairwise_keys(&name, &secret)))
        } else {
            None
        };

        match parameters.protocol {
            Protocol::Vantage => {
                // PHASE4-SPEC.md §1: a single `VantageCore` task replaces
                // Core/Proposer/HeaderWaiter/Helper/consensus entirely. Only the
                // worker-facing receiver and the metrics server (already booted above)
                // are shared with Autobahn.
                let (tx_vantage, ack_aggregator) = crate::vantage::VantageCore::spawn(
                    name,
                    committee.clone(),
                    parameters.clone(),
                    store.clone(),
                    Some(metrics.clone()),
                    rx_our_digests,
                    tx_output,
                );

                // Spawn the network receiver listening to messages from the other
                // primaries, routed into `VantageCore` (not `Core`).
                let mut address = committee
                    .primary(&name)
                    .expect("Our public key or worker id is not in the committee")
                    .primary_to_primary;
                address.set_ip("0.0.0.0".parse().unwrap());
                NetworkReceiver::spawn_full(
                    address,
                    /* handler */
                    crate::vantage::node::VantageReceiverHandler {
                        tx: tx_vantage,
                        ack_aggregator,
                        metrics: Some(metrics.clone()),
                        channel_auth: channel_auth.clone(),
                        committee: committee.clone(),
                    },
                    Some(metrics.clone()),
                    parameters.compress_network,
                    // Acks every received frame (moved out of `dispatch` -- see
                    // `VantageReceiverHandler`'s doc comment).
                    /* acks */
                    true,
                    parameters.batch_messages,
                );
                info!(
                    "Primary {} listening to primary messages on {}",
                    name, address
                );

                // Spawn the network receiver listening to messages from our workers
                // (unchanged handler/shape): `OurBatch` feeds `VantageCore`'s own-lane
                // publication cadence via `rx_our_digests`; `OthersBatch` still feeds
                // `PayloadReceiver`'s D1 payload-presence markers.
                let mut address = committee
                    .primary(&name)
                    .expect("Our public key or worker id is not in the committee")
                    .worker_to_primary;
                address.set_ip("0.0.0.0".parse().unwrap());
                NetworkReceiver::spawn_full(
                    address,
                    /* handler */
                    WorkerReceiverHandler {
                        tx_our_digests,
                        tx_others_digests,
                        metrics: metrics.clone(),
                        name,
                        channel_auth: channel_auth.clone(),
                    },
                    Some(metrics.clone()),
                    parameters.compress_network,
                    // This handler never acked (see its `dispatch`).
                    /* acks */
                    false,
                    parameters.batch_messages,
                );
                info!(
                    "Primary {} listening to workers messages on {}",
                    name, address
                );

                // Receives batch digests from other workers -- reused as-is (D1's
                // payload-presence key shape is identical on both assemblies).
                PayloadReceiver::spawn(store, /* rx_workers */ rx_others_digests);

                info!(
                    "Primary {} successfully booted on {}",
                    name,
                    committee
                        .primary(&name)
                        .expect("Our public key or worker id is not in the committee")
                        .primary_to_primary
                        .ip()
                );
            }
            Protocol::SimpleIt => {
                // Simple-IT cut-consensus: a single `SimpleItCore` task, mirroring
                // `Protocol::Vantage`'s assembly exactly (same address setup, same
                // `acks: true`, same compress/batch parameters) -- it drives
                // `simpleit::CutEngine` over the identical data plane (`LaneManager`/
                // `Repairer`/`Wire`/`PayloadIo`) Vantage uses, as its own separate
                // instances (deliberately not shared mutable state -- see
                // `simpleit::node::SimpleItCore`'s own doc comment).
                let (tx_simpleit, ack_aggregator) = crate::simpleit::SimpleItCore::spawn(
                    name,
                    committee.clone(),
                    parameters.clone(),
                    store.clone(),
                    Some(metrics.clone()),
                    rx_our_digests,
                    tx_output,
                );

                // Spawn the network receiver listening to messages from the other
                // primaries, routed into `SimpleItCore` (not `Core`/`VantageCore`).
                let mut address = committee
                    .primary(&name)
                    .expect("Our public key or worker id is not in the committee")
                    .primary_to_primary;
                address.set_ip("0.0.0.0".parse().unwrap());
                NetworkReceiver::spawn_full(
                    address,
                    /* handler */
                    crate::simpleit::node::SimpleItReceiverHandler {
                        tx: tx_simpleit,
                        ack_aggregator,
                        metrics: Some(metrics.clone()),
                        channel_auth: channel_auth.clone(),
                    },
                    Some(metrics.clone()),
                    parameters.compress_network,
                    // Acks every received frame (moved out of `dispatch` -- see
                    // `SimpleItReceiverHandler`'s doc comment).
                    /* acks */
                    true,
                    parameters.batch_messages,
                );
                info!(
                    "Primary {} listening to primary messages on {}",
                    name, address
                );

                // Spawn the network receiver listening to messages from our workers
                // (unchanged handler/shape): `OurBatch` feeds `SimpleItCore`'s own-lane
                // publication cadence via `rx_our_digests`; `OthersBatch` still feeds
                // `PayloadReceiver`'s D1 payload-presence markers.
                let mut address = committee
                    .primary(&name)
                    .expect("Our public key or worker id is not in the committee")
                    .worker_to_primary;
                address.set_ip("0.0.0.0".parse().unwrap());
                NetworkReceiver::spawn_full(
                    address,
                    /* handler */
                    WorkerReceiverHandler {
                        tx_our_digests,
                        tx_others_digests,
                        metrics: metrics.clone(),
                        name,
                        channel_auth: channel_auth.clone(),
                    },
                    Some(metrics.clone()),
                    parameters.compress_network,
                    // This handler never acked (see its `dispatch`).
                    /* acks */
                    false,
                    parameters.batch_messages,
                );
                info!(
                    "Primary {} listening to workers messages on {}",
                    name, address
                );

                // Receives batch digests from other workers -- reused as-is (D1's
                // payload-presence key shape is identical across every data-plane-
                // sharing assembly).
                PayloadReceiver::spawn(store, /* rx_workers */ rx_others_digests);

                info!(
                    "Primary {} successfully booted on {}",
                    name,
                    committee
                        .primary(&name)
                        .expect("Our public key or worker id is not in the committee")
                        .primary_to_primary
                        .ip()
                );
            }
            Protocol::AutobahnOptimistic | Protocol::AutobahnSeamless => {
                // Spawn the network receiver listening to messages from the other primaries.
                let mut address = committee
                    .primary(&name)
                    .expect("Our public key or worker id is not in the committee")
                    .primary_to_primary;
                address.set_ip("0.0.0.0".parse().unwrap());
                NetworkReceiver::spawn_full(
                    address,
                    /* handler */
                    PrimaryReceiverHandler {
                        tx_primary_messages,
                        tx_cert_requests,
                        tx_header_requests,
                        metrics: metrics.clone(),
                    },
                    Some(metrics.clone()),
                    parameters.compress_network,
                    // Acks every received frame (moved out of `dispatch` -- see
                    // `PrimaryReceiverHandler`'s doc comment).
                    /* acks */
                    true,
                    parameters.batch_messages,
                );
                info!(
                    "Primary {} listening to primary messages on {}",
                    name, address
                );

                // Spawn the network receiver listening to messages from our workers.
                let mut address = committee
                    .primary(&name)
                    .expect("Our public key or worker id is not in the committee")
                    .worker_to_primary;
                address.set_ip("0.0.0.0".parse().unwrap());
                NetworkReceiver::spawn_full(
                    address,
                    /* handler */
                    WorkerReceiverHandler {
                        tx_our_digests,
                        tx_others_digests,
                        metrics: metrics.clone(),
                        name,
                        channel_auth: channel_auth.clone(),
                    },
                    Some(metrics.clone()),
                    parameters.compress_network,
                    // This handler never acked (see its `dispatch`).
                    /* acks */
                    false,
                    parameters.batch_messages,
                );
                info!(
                    "Primary {} listening to workers messages on {}",
                    name, address
                );

                // The `Synchronizer` provides auxiliary methods helping to `Core` to sync.
                let synchronizer = Synchronizer::new(
                    name,
                    &committee,
                    store.clone(),
                    /* tx_header_waiter */ tx_sync_headers,
                    /* tx_certificate_waiter */ tx_sync_certificates,
                );

                // use_optimistic_tips: bool,     //default = true (TODO: implement non optimistic tip option)

                // use_parallel_proposals: bool,  //default = true (TODO: implement sequential slot option)
                // let k = 1; //Max open conensus instances at a time.

                // use_fast_path: bool,           //default = false
                // fast_path_timeout: u64,

                // use_ride_share: bool,
                // car_timeout: u64,

                // The `Core` receives and handles headers, votes, and certificates from the other primaries.
                Core::spawn(
                    name,
                    committee.clone(),
                    store.clone(),
                    synchronizer.clone(),
                    signature_service.clone(),
                    consensus_round.clone(),
                    parameters.gc_depth,
                    /* rx_primaries */ rx_primary_messages,
                    /* rx_header_waiter */ rx_headers_loopback,
                    rx_header_waiter_instances,
                    /* rx_proposer */ rx_headers,
                    tx_commit,
                    /* tx_proposer */ tx_parents,
                    rx_request_header_sync,
                    /*tx info */ tx_instance,
                    LeaderElector::new(committee.clone()),
                    parameters.timeout_delay,
                    parameters.use_optimistic_tips,
                    parameters.use_parallel_proposals,
                    parameters.k,
                    parameters.use_fast_path,
                    parameters.fast_path_timeout,
                    parameters.use_ride_share,
                    parameters.all_to_all,
                    parameters.simulate_asynchrony,
                    parameters.asynchrony_start,
                    parameters.asynchrony_duration,
                    // PHASE7-PREP-NOTES.md (WAN-shaped local runs, optional item):
                    // resolved once here (empty == current behavior, byte-identical,
                    // unless `--latency-table`/`--mimic-latency-ms` set
                    // `parameters.latency_table`) -- the fairness point: the exact
                    // same `Committee::latency_map` call `Protocol::Vantage`'s arm
                    // above makes for `VantageCore::spawn`.
                    parameters
                        .latency_table
                        .as_deref()
                        .map(|table| committee.latency_map(&name, table))
                        .unwrap_or_default(),
                    metrics.clone(),
                    parameters.compress_network,
                    batch,
                );

                Committer::spawn(
                    name,
                    committee.clone(),
                    store.clone(),
                    parameters.gc_depth,
                    rx_mempool,
                    rx_committer,
                    rx_commit,
                    tx_output,
                    synchronizer,
                    metrics.clone(),
                    parameters.compress_network,
                    batch,
                    channel_auth.clone(),
                );

                // Keeps track of the latest consensus round and allows other tasks to clean up their their internal state
                GarbageCollector::spawn(
                    &name,
                    &committee,
                    store.clone(),
                    consensus_round.clone(),
                    rx_consensus,
                    tx_certificates_loopback.clone(),
                    metrics.clone(),
                    parameters.compress_network,
                    batch,
                    channel_auth.clone(),
                );

                // Receives batch digests from other workers. They are only used to validate headers.
                PayloadReceiver::spawn(store.clone(), /* rx_workers */ rx_others_digests);

                // Whenever the `Synchronizer` does not manage to validate a header due to missing parent certificates of
                // batch digests, it commands the `HeaderWaiter` to synchronizer with other nodes, wait for their reply, and
                // re-schedule execution of the header once we have all missing data.
                HeaderWaiter::spawn(
                    name,
                    committee.clone(),
                    store.clone(),
                    consensus_round,
                    parameters.gc_depth,
                    parameters.sync_retry_delay,
                    parameters.sync_retry_nodes,
                    /* rx_synchronizer */ rx_sync_headers,
                    /* tx_core */ tx_headers_loopback,
                    tx_header_waiter_instances,
                    metrics.clone(),
                    parameters.compress_network,
                    batch,
                    channel_auth.clone(),
                );

                // The `CertificateWaiter` waits to receive all the ancestors of a certificate before looping it back to the
                // `Core` for further processing.
                CertificateWaiter::spawn(
                    store.clone(),
                    /* rx_synchronizer */ rx_sync_certificates,
                    /* tx_core */ tx_certificates_loopback,
                );

                // When the `Core` collects enough parent certificates, the `Proposer` generates a new header with new batch
                // digests from our workers and it back to the `Core`.
                Proposer::spawn(
                    name,
                    committee.clone(),
                    signature_service,
                    parameters.header_size,
                    parameters.max_header_delay,
                    /* rx_core */ rx_parents,
                    /* rx_workers */ rx_our_digests,
                    /* rx_ticket */ rx_instance,
                    /* tx_core */ tx_headers,
                );

                // The `Helper` is dedicated to reply to certificates requests from other primaries.
                Helper::spawn(
                    committee.clone(),
                    store,
                    rx_cert_requests,
                    rx_header_requests,
                    metrics.clone(),
                    parameters.compress_network,
                    batch,
                );

                // NOTE: This log entry is used to compute performance.
                info!(
                    "Primary {} successfully booted on {}",
                    name,
                    committee
                        .primary(&name)
                        .expect("Our public key or worker id is not in the committee")
                        .primary_to_primary
                        .ip()
                );
            }
        }

        (metrics, reporter, registry)
    }
}

/// Defines how the network receiver handles incoming primary messages.
#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_primary_messages: Sender<PrimaryMessage>,
    tx_cert_requests: Sender<(Vec<Digest>, PublicKey)>,
    tx_header_requests: Sender<(Vec<Digest>, PublicKey)>,
    metrics: Arc<Metrics>,
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        // The ack is now sent by `network::Receiver` itself, once per received FRAME
        // rather than once per `dispatch` call -- required for batching (several
        // logical messages can share one frame, and only one ack may be sent per
        // frame). See `Receiver::acks`'s doc comment.

        // Deserialize and parse the message.
        let message: PrimaryMessage =
            bincode::deserialize(&serialized).map_err(DagError::SerializationError)?;
        record_typed_received(&self.metrics, message.type_name(), serialized.len());
        match message {
            PrimaryMessage::CertificatesRequest(missing, requestor) => self
                .tx_cert_requests
                .send((missing, requestor))
                .await
                .expect("Failed to send primary message"),
            PrimaryMessage::HeadersRequest(missing, requestor) => self
                .tx_header_requests
                .send((missing, requestor))
                .await
                .expect("Failed to send primary message"),
            request => {
                ////println!("Made it to dispatch");
                self.tx_primary_messages
                    .send(request)
                    .await
                    .expect("Failed to send certificate")
            }
        }
        Ok(())
    }
}

/// Defines how the network receiver handles incoming workers messages.
#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_our_digests: Sender<(Digest, WorkerId)>,
    tx_others_digests: Sender<(Digest, WorkerId)>,
    metrics: Arc<Metrics>,
    /// SECURITY (Fable audit): this authority's own public key -- the worker<->primary
    /// channel is intra-authority (our own worker shares our own public key), so the
    /// MAC candidate sender for every message on this port is always `name` itself
    /// (`k_{name,name}`, the degenerate self-pair key -- see `PairwiseKeys::build`'s
    /// doc comment). Unused when `channel_auth` is `None`.
    name: PublicKey,
    /// `Parameters::authenticate_channels`; `None` is byte-identical to pre-MAC
    /// behavior -- every received frame is deserialized and routed exactly as
    /// received, no trailing bytes stripped or checked.
    channel_auth: Option<Arc<crypto::PairwiseKeys>>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        // SECURITY (Fable audit): strip and verify the trailing MAC tag before
        // deserializing -- see `crate::vantage::node::VantageReceiverHandler::
        // dispatch`'s identical contract/doc comment.
        let (payload, tag): (&[u8], Option<[u8; crypto::mac::TAG_LEN]>) = match &self.channel_auth {
            Some(_) => match crypto::mac::split_tag(&serialized) {
                Some((payload, tag)) => (payload, Some(tag)),
                None => return Ok(()),
            },
            None => (&serialized[..], None),
        };

        // Deserialize and parse the message.
        let message: WorkerPrimaryMessage =
            bincode::deserialize(payload).map_err(DagError::SerializationError)?;

        if let (Some(auth), Some(tag)) = (&self.channel_auth, tag) {
            if !auth.verify(&self.name, payload, &tag) {
                self.metrics.authenticated_channel_rejected_total.inc();
                return Ok(());
            }
        }

        match message {
            WorkerPrimaryMessage::OurBatch(digest, worker_id) => {
                record_typed_received(&self.metrics, "OurBatch", payload.len());
                self.tx_our_digests //sender channel to Proposer
                    .send((digest, worker_id))
                    .await
                    .expect("Failed to send workers' digests")
            }
            WorkerPrimaryMessage::OthersBatch(digest, worker_id) => {
                record_typed_received(&self.metrics, "OthersBatch", payload.len());
                self.tx_others_digests //sender channel to PayloadReceiver
                    .send((digest, worker_id))
                    .await
                    .expect("Failed to send workers' digests")
            }
        }
        Ok(())
    }
}

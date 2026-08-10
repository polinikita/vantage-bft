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
use crate::vantage::sequence::{
    SequenceAnnouncement, SequenceDeltaChunk, SequenceDeltaRangeChunk, SequenceDeltaRangeRequest,
    SequenceDeltaRequest, SequenceOutcomeRequest, SequenceOutcomeServe, SequenceRecordChunk,
    SequenceRequest, SequenceUnavailable,
};
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, Parameters, Protocol, WorkerId};
use crypto::{Digest, PublicKey, SignatureService};
use log::info;
use metrics::{spawn_queue_sampler, start_prometheus_server, MetricReporter, Metrics, StoreProbe};
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
// `if let` destructuring site across the dispatch code in core.rs,
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
    // Vantage only. Appended last for bincode wire compatibility; the same rule
    // as `PrimaryWorkerMessage::Committed` above: this is a variant index, inserting it
    // anywhere else would shift every following discriminant.
    VantageAck(Ack),
    // Vantage AGB engine. Keep new variants appended for bincode compatibility.
    VantagePropose(ViewProposal),
    VantageEcho(Echo),
    // `EchoSkip` and `NoReady` carry a trailing `View` wish
    // piggyback field, same convention as `Echo`/`Ready`'s own new `wish` field.
    VantageEchoSkip(View, /* sender */ PublicKey, /* wish */ View),
    VantageReady(Ready),
    VantageNoReady(View, /* sender */ PublicKey, /* wish */ View),
    // Vantage WISH pacemaker. Appended after `VantageNoReady`, last
    // -- same bincode wire-compat rule as every other Vantage-only variant above.
    VantageWish(View, /* sender */ PublicKey),
    // Vantage resolution and control-log messages. Keep new variants appended.
    CompReport(View, Digest, /* sender */ PublicKey),
    ControlInit(crate::vantage::ControlProposal, Option<ViewProposal>),
    ControlEcho(crate::vantage::ControlProposal, /* sender */ PublicKey),
    ControlReady(crate::vantage::ControlProposal, /* sender */ PublicKey),
    ControlFetch(View, Digest, /* requester */ PublicKey),
    ControlServe(View, ViewProposal),
    // Control commit messages are appended to preserve existing wire indices.
    ControlCommit(crate::vantage::Round, /* sender */ PublicKey),
    // Simple-IT timeout messages are appended to preserve existing wire indices.
    ControlTimeoutVote(crate::vantage::Round, /* sender */ PublicKey),
    ControlTimeoutAccept(crate::vantage::Round, /* sender */ PublicKey),
    // Simple-IT cut-consensus messages use Vantage's data plane and keep their
    // existing variants distinct. New variants are appended for wire compatibility.
    SimpleItCutProposal(crate::simpleit::CutProposal),
    SimpleItCutVote(crate::simpleit::CutVote),
    SimpleItDecide(crate::simpleit::Decide),
    SimpleItTimeout(crate::simpleit::Timeout),
    SimpleItTimeoutAccept(crate::simpleit::TimeoutAccept),
    // Simple-IT cut-proposal repair messages. New variants are appended.
    SimpleItCutFetch(
        crate::simpleit::CutRound,
        Digest,
        /* requester */ PublicKey,
    ),
    SimpleItCutServe(crate::simpleit::CutProposal),
    // Optional periodic per-lane availability watermark. Appended for wire
    // compatibility and shared by Vantage and Simple-IT.
    VantageAvail(Vec<crate::vantage::AvailEntry>, /* sender */ PublicKey),
    // Vantage-only batched resolution entries. New variants are appended.
    // `VantagePropose`/`VantageEcho`/`VantageReady`/`ControlInit`/`ControlServe`.
    // Never use these on the variants above (see `agb::ViewProposal`'s
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
    // Vantage-only skip vote. It is unconditional protocol behavior:
    // one-shot statement `<skip-vote, u>`, sent by a correct party after its own
    // durably-emitted no-ready, a first-hand 2f+1 echo-skip census, and a free
    // resolution stance. Appended last -- same bincode wire-compat rule as every
    // other protocol-specific variant above.
    VantageSkipVote(View, /* sender */ PublicKey),
    // Vantage-only digest-named AGB statements. The digest variants are enabled by
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
    // Simple-IT's Bracha-RBC variant:
    // Bracha-RBC's own second echo round ("ready"), sent once a party's own
    // `SimpleItCutVote` census crosses `quorum_threshold` (see
    // `simpleit::engine::CutEngine::broadcast_cut_ready`). Appended last -- same
    // bincode wire-compat rule as every other protocol-specific variant above.
    SimpleItCutReady(crate::simpleit::CutReady),
    // Lane resume. A requester asks the named
    // lane AUTHOR to re-publish its own lane from `from` (inclusive) upward --
    // unicast to the author only, never broadcast. Shared by both Vantage and
    // Simple-IT (same `LaneManager`/`Wire` data plane); see `vantage::resume`'s own
    // module doc comment for the full trigger/serve design. No separate reply
    // variant: the author answers with ordinary `Header(_, false)` messages,
    // unicast (`vantage::wire::Wire::enqueue_resume_header`, a non-blocking
    // hand-off onto a dedicated resume-sender task) -- receipt is therefore
    // DirectPub/ack-eligible through the existing publish path, exactly as a
    // broadcast publish would be, and still subject to the `--withhold` filter
    // during an active window (required for experiment fidelity: a withholding
    // sender must not resume-serve its own blocked half mid-window either).
    // Appended last -- same bincode wire-compat rule as every other
    // protocol-specific variant above.
    VantageLaneResume(
        /* lane author */ PublicKey,
        /* from height, inclusive */ Height,
        /* requester */ PublicKey,
    ),
    // Replay of durable messages lost during a connection reset. This is separate
    // from `VantageLaneResume`:
    // doc comment) -- resumes ONE-SHOT AGB/consensus broadcasts lost to a volatile
    // session death, rather than lane content. `VantageResumeHello(floor hint,
    // sender)` is sent unicast (i) on this node's own reconnect event, (ii) by the
    // tick for any open episode past `resume_backoff_ms`, (iii) reciprocally on
    // Hello receipt, and (iv) by the server-side nudge loop when `pending_low` is
    // set and no serve has been enqueued since (§14 A3) -- (iv) rides the VOLATILE
    // send class (§14 A7); (i)-(iii) ride the ordinary durable unicast path. Both
    // variants appended last -- same bincode wire-compat rule as every other
    // protocol-specific variant above.
    VantageResumeHello(View, /* sender */ PublicKey),
    // Sent by the resume task after a replay stream's last chunk (`end_key`, always
    // durable -- rides the SAME `ReplaySend` frame sequence as the chunks it
    // terminates). `complete` is `false` iff the per-peer serve budget truncated the
    // span before the requester's known need was fully covered (a continuation
    // Hello follows immediately); `clamped` is `true` iff `outbox_floor` truncated
    // the requested span below what was actually asked for (a recovered-with-gap
    // signal, `vantage_replay_done_clamped_total`).
    VantageReplayDone(
        View,      /* end_key: last fully served key + 1 */
        bool,      /* complete */
        bool,      /* clamped */
        PublicKey, /* sender */
    ),

    // Sequence messages are appended and must not be reordered:
    // reordered: bincode encodes an enum by its variant INDEX, so inserting anywhere
    // above would silently reinterpret every existing message on a mixed-version fleet.
    //
    // Announcements are first-hand only and are never forwarded as evidence -- the
    // receiver derives the authoritative sender from the authenticated connection and
    // rejects a payload whose encoded sender differs. None of these carry a live-view
    // vote, availability acknowledgment, or resolution stance.
    VantageSequenceAnnounce(SequenceAnnouncement),
    VantageSequenceRequest(SequenceRequest),
    VantageSequenceRecords(SequenceRecordChunk),
    VantageSequenceDeltaRequest(SequenceDeltaRequest),
    VantageSequenceDelta(SequenceDeltaChunk),
    VantageSequenceOutcomeRequest(SequenceOutcomeRequest),
    VantageSequenceOutcome(SequenceOutcomeServe),
    VantageSequenceUnavailable(SequenceUnavailable),
    VantageSequenceDeltaRangeRequest(SequenceDeltaRangeRequest),
    VantageSequenceDeltaRange(SequenceDeltaRangeChunk),
    // These materialization messages are separate from `HeadersRequest`/`Header(_, true)`
    // so a late joiner's committed responses stay on the dedicated sequence
    // transport and ingress queue instead of refilling its saturated consensus queue.
    VantageSequenceHeadersRequest(Vec<Digest>, /* requester */ PublicKey),
    VantageSequenceHeaders(Vec<Header>, /* sender */ PublicKey),
    VantageSequenceAnnounceBatch(Vec<SequenceAnnouncement>, /* sender */ PublicKey),
}

impl PrimaryMessage {
    /// Returns the wire variant name used as the `type` label
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
            PrimaryMessage::SimpleItCutReady(..) => "SimpleItCutReady",
            PrimaryMessage::VantageLaneResume(..) => "VantageLaneResume",
            PrimaryMessage::VantageResumeHello(..) => "VantageResumeHello",
            // Keep the completion message label distinct from replay chunks.
            PrimaryMessage::VantageReplayDone(..) => "VantageReplayDone",
            PrimaryMessage::VantageSequenceAnnounce(..) => "VantageSequenceAnnounce",
            PrimaryMessage::VantageSequenceRequest(..) => "VantageSequenceRequest",
            PrimaryMessage::VantageSequenceRecords(..) => "VantageSequenceRecords",
            PrimaryMessage::VantageSequenceDeltaRequest(..) => "VantageSequenceDeltaRequest",
            PrimaryMessage::VantageSequenceDelta(..) => "VantageSequenceDelta",
            PrimaryMessage::VantageSequenceOutcomeRequest(..) => "VantageSequenceOutcomeRequest",
            PrimaryMessage::VantageSequenceOutcome(..) => "VantageSequenceOutcome",
            PrimaryMessage::VantageSequenceUnavailable(..) => "VantageSequenceUnavailable",
            PrimaryMessage::VantageSequenceDeltaRangeRequest(..) => {
                "VantageSequenceDeltaRangeRequest"
            }
            PrimaryMessage::VantageSequenceDeltaRange(..) => "VantageSequenceDeltaRange",
            PrimaryMessage::VantageSequenceHeadersRequest(..) => "VantageSequenceHeadersRequest",
            PrimaryMessage::VantageSequenceHeaders(..) => "VantageSequenceHeaders",
            PrimaryMessage::VantageSequenceAnnounceBatch(..) => "VantageSequenceAnnounceBatch",
        }
    }
}

/// Records a typed receive for every `MessageHandler::dispatch` implementation in
/// this crate (a no-op if `metrics` is `None`). `len` is the serialized payload size
/// `len` is the serialized payload length without a frame prefix.
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
    /// Benchmark-only: the primary indicates these batch
    /// digests were just committed, carrying the commit instant itself (UTC millis,
    /// taken once at the "Committed B..." log site) so the worker measures
    /// submission -> commit exactly -- not submission -> whenever this notification
    /// happened to reach the front of the worker's queue. Starfish parity: starfish
    /// observes latency at its own commit handler, not at a downstream consumer.
    Committed(u64 /* commit UTC-millis */, Vec<Digest>),
}

impl PrimaryWorkerMessage {
    /// Returns the same wire type label as `PrimaryMessage::type_name`.
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
    /// Returns the same wire type label as `PrimaryMessage::type_name`.
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
    // the call site's actual argument count.
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
        // The registry starts with no primary-specific counters.
        let metrics_address = committee
            .primary(&name)
            .expect("Our public key or worker id is not in the committee")
            .metrics;
        let mut binding_metrics_address = metrics_address;
        binding_metrics_address.set_ip("0.0.0.0".parse().unwrap());
        let registry = Registry::new();
        let (metrics, reporter) = Metrics::new(&registry);
        // Set protocol metrics once at boot.
        metrics.set_protocol_info(parameters.protocol.label());
        if let Some(mode) = parameters.tx_mode.as_deref() {
            metrics.set_transaction_mode_info(mode);
        }
        // Arm the metrics-active window here too, matching `Worker::spawn`. The primary
        // does not gate anything on it (commit-time observation lives in the worker's
        // `synchronizer`), but `ActiveWindowCollector` publishes
        // `metrics_active_seconds` from this same value -- so leaving it unset made the
        // primary's series read a permanent 0 in every scrape, an active false lead
        // during startup (it looks exactly like "the
        // measurement window never opened").
        metrics.set_active_from_millis(parameters.metrics_active_at_ms);
        // Task panics are otherwise dropped on the floor -- nothing here awaits a
        // `JoinHandle`, so a dead subsystem leaves the process serving metrics and every
        // unrelated counter advancing. See `Metrics::install_panic_hook`.
        Metrics::install_panic_hook(metrics.clone());
        // The primary's OWN store, which had no observability at all: the first cut of this
        // instrument sampled only the worker's, so `store_actor_heartbeat_age_ms` was
        // registered in this process and never written -- it read 0 forever, which is
        // indistinguishable from perfect health. The primary runs its own store actor and
        // its own per-key `notify_read` waiters (`vantage::payload::sync_batches`), so it is
        // subject to the same permit-starvation class the worker wedged on. No pipeline
        // channels here, hence the empty probe list.
        spawn_queue_sampler(
            Vec::new(),
            {
                let depth = store.clone();
                let beat = store.clone();
                let drained = store.clone();
                StoreProbe {
                    occupancy: Box::new(move || (depth.queue_depth(), depth.queue_capacity())),
                    heartbeat_millis: Box::new(move || beat.heartbeat_millis()),
                    commands_drained: Box::new(move || drained.commands_drained()),
                }
            },
            metrics.clone(),
        );
        reporter.clone().start();
        start_prometheus_server(binding_metrics_address, &registry);
        info!("Primary {} metrics listening on {}", name, metrics_address);

        // Atomic variable use to synchronizer all tasks with the latest consensus round. This is only
        // used for cleanup. The only tasks that write into this variable is `GarbageCollector`.
        let consensus_round = Arc::new(AtomicU64::new(0));

        // Transport-level batching, resolved once here -- a single `Parameters`-
        // derived value threaded into every `Reliable`/`SimpleSender` and
        // `network::Receiver` this primary spawns, both protocols identically.
        let batch = network::BatchConfig {
            enabled: parameters.batch_messages,
            max_bytes: parameters.batch_max_bytes,
            max_delay_ms: parameters.batch_max_delay_ms,
        };

        match parameters.protocol {
            Protocol::Vantage => {
                // A single `VantageCore` task replaces
                // Core/Proposer/HeaderWaiter/Helper/consensus entirely. Only the
                // worker-facing receiver and the metrics server (already booted above)
                // are shared with Autobahn.
                let (
                    tx_vantage,
                    tx_vantage_bulk,
                    tx_vantage_sequence,
                    ack_aggregator,
                    sequence_large_gap_drop,
                ) = crate::vantage::VantageCore::spawn(
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
                        tx_bulk: tx_vantage_bulk,
                        tx_sequence: tx_vantage_sequence,
                        sequence_large_gap_drop,
                        ack_aggregator,
                        metrics: Some(metrics.clone()),
                    },
                    Some(metrics.clone()),
                    // Acks every received frame (moved out of `dispatch` -- see
                    // `VantageReceiverHandler`'s doc comment).
                    /* acks */
                    true,
                    parameters.batch_messages,
                    "primary_to_primary",
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
                    },
                    Some(metrics.clone()),
                    // This handler never acked (see its `dispatch`).
                    /* acks */
                    false,
                    parameters.batch_messages,
                    "worker_to_primary",
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
            Protocol::SimpleIt | Protocol::SimpleItBracha => {
                // Simple-IT cut-consensus: a single `SimpleItCore` task, mirroring
                // `Protocol::Vantage`'s assembly exactly (same address setup, same
                // `acks: true`, same batch parameters) -- it drives
                // `simpleit::CutEngine` over the shared data plane (`LaneManager`/
                // `Repairer`/`Wire`/`PayloadIo`) Vantage uses, as its own separate
                // instances (deliberately not shared mutable state -- see
                // `simpleit::node::SimpleItCore`'s own doc comment). One shared arm
                // 2606.14404 Table 1/2 + Corollary 5, variant S) -- `SimpleItCore::
                // build` reads `parameters.protocol` (already threaded through below)
                // to select `simpleit::engine::Variant::{Opt,Bracha}`.
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
                    },
                    Some(metrics.clone()),
                    // Acks every received frame (moved out of `dispatch` -- see
                    // `SimpleItReceiverHandler`'s doc comment).
                    /* acks */
                    true,
                    parameters.batch_messages,
                    "primary_to_primary",
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
                    },
                    Some(metrics.clone()),
                    // This handler never acked (see its `dispatch`).
                    /* acks */
                    false,
                    parameters.batch_messages,
                    "worker_to_primary",
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
                    // Acks every received frame (moved out of `dispatch` -- see
                    // `PrimaryReceiverHandler`'s doc comment).
                    /* acks */
                    true,
                    parameters.batch_messages,
                    "primary_to_primary",
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
                    },
                    Some(metrics.clone()),
                    // This handler never acked (see its `dispatch`).
                    /* acks */
                    false,
                    parameters.batch_messages,
                    "worker_to_primary",
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



                // use_fast_path: bool,           // Autobahn only; default = true
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
                    // Optional per-destination latency map:
                    // resolved once here (empty keeps the default behavior, unless
                    // `--latency-table`/`--mimic-latency-ms` set
                    // `parameters.latency_table`) -- the fairness point: the exact
                    // same `Committee::latency_map` call `Protocol::Vantage`'s arm
                    // above makes for `VantageCore::spawn`.
                    parameters
                        .latency_table
                        .as_deref()
                        .map(|table| committee.latency_map(&name, table))
                        .unwrap_or_default(),
                    // Data-plane withholding fault injector (`--withhold`): resolved
                    // once here (`None` keeps the default behavior, unless
                    // `--withhold` selects this node as a withholding sender) -- same
                    // "doesn't otherwise take a `Parameters`" reasoning as
                    // `latency_map` just above.
                    config::withheld_destinations(&committee, &name, parameters.withhold_senders),
                    // Data-plane withholding fault injector, time-windowed variant:
                    // resolved once here, a plain clone of the shared cell (`None`
                    // whenever `--withhold-at` isn't given).
                    parameters.withhold_window.clone(),
                    metrics.clone(),
                    batch,
                    // KNOB 2 (measurement ablation): applies to Autobahn's own
                    // primary-to-primary pool too -- see `Parameters::
                    // retry_backoff_max_ms`'s own doc comment.
                    parameters.retry_backoff_max_ms,
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
                    batch,
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
                    batch,
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
                    batch,
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
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        // Deserialize and parse the message.
        let message: WorkerPrimaryMessage =
            bincode::deserialize(&serialized).map_err(DagError::SerializationError)?;

        match message {
            WorkerPrimaryMessage::OurBatch(digest, worker_id) => {
                record_typed_received(&self.metrics, "OurBatch", serialized.len());
                self.tx_our_digests //sender channel to Proposer
                    .send((digest, worker_id))
                    .await
                    .expect("Failed to send workers' digests")
            }
            WorkerPrimaryMessage::OthersBatch(digest, worker_id) => {
                record_typed_received(&self.metrics, "OthersBatch", serialized.len());
                self.tx_others_digests //sender channel to PayloadReceiver
                    .send((digest, worker_id))
                    .await
                    .expect("Failed to send workers' digests")
            }
        }
        Ok(())
    }
}

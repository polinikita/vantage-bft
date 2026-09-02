// Copyright(C) Facebook, Inc. and its affiliates.
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
use crate::verified::VerifiedCache;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, Parameters, Protocol, WorkerId};
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, info, warn};
use metrics::{spawn_queue_sampler, start_prometheus_server, MetricReporter, Metrics, StoreProbe};
use network::{ChannelAuth, MessageHandler, Receiver as NetworkReceiver, Writer};
use prometheus::Registry;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

/// The default channel capacity for each channel of the primary.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// Header height.
pub type Height = u64;
/// Consensus view number.
pub type View = u64;
/// Consensus slot number.
pub type Slot = u64;

/// Returns whether this node is one of the Byzantine publishers in the
/// optimistic leader-relay experiment. Those publishers may avoid helping as
/// consensus leaders, but their lane cars retain the protocol's ordinary
/// payload capacity.
fn is_leader_relay_publisher(
    name: &PublicKey,
    committee: &Committee,
    parameters: &Parameters,
) -> bool {
    parameters.leader_relay_attack
        && config::withholding_publishers(
            committee,
            parameters.withhold_senders,
            &parameters.withhold_publishers,
        )
        .contains(name)
}

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
    // Keep appended for bincode compatibility.
    VantageAck(Ack),
    // Keep Vantage AGB variants appended for bincode compatibility.
    VantagePropose(ViewProposal),
    VantageEcho(Echo),
    // These variants carry a trailing wish view.
    VantageEchoSkip(View, /* sender */ PublicKey, /* wish */ View),
    VantageReady(Ready),
    VantageNoReady(View, /* sender */ PublicKey, /* wish */ View),
    // Keep WISH variants appended for bincode compatibility.
    VantageWish(View, /* sender */ PublicKey),
    // Obsolete Vantage control-log variants retained at their original indices.
    CompReport(View, Digest, /* sender */ PublicKey),
    ControlInit(crate::vantage::LegacyControlProposal, Option<ViewProposal>),
    ControlEcho(
        crate::vantage::LegacyControlProposal,
        /* sender */ PublicKey,
    ),
    ControlReady(
        crate::vantage::LegacyControlProposal,
        /* sender */ PublicKey,
    ),
    ControlFetch(View, Digest, /* requester */ PublicKey),
    ControlServe(View, ViewProposal),
    // Keep control commit messages appended for wire compatibility.
    ControlCommit(u64, /* sender */ PublicKey),
    // Keep Simple-IT timeout messages appended for wire compatibility.
    ControlTimeoutVote(u64, /* sender */ PublicKey),
    ControlTimeoutAccept(u64, /* sender */ PublicKey),
    // Keep Simple-IT cut messages appended for wire compatibility.
    SimpleItCutProposal(crate::simpleit::CutProposal),
    SimpleItCutVote(crate::simpleit::CutVote),
    SimpleItDecide(crate::simpleit::Decide),
    SimpleItTimeout(crate::simpleit::Timeout),
    SimpleItTimeoutAccept(crate::simpleit::TimeoutAccept),
    // Simple-IT cut-proposal repair messages.
    SimpleItCutFetch(
        crate::simpleit::CutRound,
        Digest,
        /* requester */ PublicKey,
    ),
    SimpleItCutServe(crate::simpleit::CutProposal),
    // Optional per-lane availability watermark; keep appended for compatibility.
    VantageAvail(Vec<crate::vantage::AvailEntry>, /* sender */ PublicKey),
    // Vantage batched resolution entries; keep appended for compatibility.
    VantageProposeBatch(crate::vantage::BatchViewProposal),
    VantageEchoBatch(crate::vantage::EchoBatch),
    VantageReadyBatch(crate::vantage::ReadyBatch),
    ControlInitBatch(
        crate::vantage::LegacyControlProposal,
        Option<crate::vantage::BatchViewProposal>,
    ),
    ControlServeBatch(View, crate::vantage::BatchViewProposal),
    // Vantage skip vote; keep appended for compatibility.
    VantageSkipVote(View, /* sender */ PublicKey),
    // Vantage digest-named AGB and body-fetch messages; keep appended.
    VantageEchoDigest(crate::vantage::EchoDigest),
    VantageReadyDigest(crate::vantage::ReadyDigest),
    VantageBodyFetch(View, Digest, /* requester */ PublicKey),
    VantageBodyServe(View, ViewProposal),
    // Simple-IT Bracha ready messages; keep appended for compatibility.
    SimpleItCutReady(crate::simpleit::CutReady),
    // Lane resume request; keep appended for compatibility.
    VantageLaneResume(
        /* lane author */ PublicKey,
        /* from height, inclusive */ Height,
        /* requester */ PublicKey,
    ),
    // Replay messages; keep appended for compatibility.
    VantageResumeHello(View, /* sender */ PublicKey),
    // Replay completion: end key, completion flag, clamp flag, sender.
    VantageReplayDone(
        View,      /* end_key: last fully served key + 1 */
        bool,      /* complete */
        bool,      /* clamped */
        PublicKey, /* sender */
    ),

    // Sequence messages use appended bincode variants.
    // Announcements are first-hand and are not forwarded as evidence.
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
    // Keep materialization messages separate from consensus requests.
    VantageSequenceHeadersRequest(Vec<Digest>, /* requester */ PublicKey),
    VantageSequenceHeaders(Vec<Header>, /* sender */ PublicKey),
    VantageSequenceAnnounceBatch(Vec<SequenceAnnouncement>, /* sender */ PublicKey),
    // Autobahn prepare-time tip repair; appended for bincode compatibility.
    PrepareHeadersRequest(Vec<Digest>, /* requestor */ PublicKey),
    // Autobahn whole-suffix response; appended for bincode compatibility.
    ProposalHeaders(Vec<Header>),

    // Direct per-target resolver messages. Keep appended for bincode compatibility.
    VantageDirectResolutionWish(crate::vantage::DirectResolutionWish),
    VantageDirectResolutionSuggest(crate::vantage::DirectResolutionSuggest),
    VantageDirectResolutionProof(crate::vantage::DirectResolutionProof),
    VantageDirectResolutionProposal(crate::vantage::DirectResolutionProposal),
    VantageDirectResolutionStatement(crate::vantage::DirectResolutionStatement),
    VantageDirectResolutionDone(crate::vantage::DirectResolutionDone),
    VantageDirectResolutionValueFetch(crate::vantage::DirectResolutionValueFetch),
    VantageDirectResolutionValueServe(crate::vantage::DirectResolutionValueServe),
    VantageDirectResolutionWitness(crate::vantage::DirectResolutionWitness),
}

impl PrimaryMessage {
    /// Returns the wire variant name used for metrics.
    pub fn type_name(&self) -> &'static str {
        match self {
            // The serve flag gets its own label so repair serves are separable from
            // broadcast publishes in the typed wire counters.
            PrimaryMessage::Header(_, true) => "HeaderServe",
            PrimaryMessage::Header(_, false) => "Header",
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
            PrimaryMessage::PrepareHeadersRequest(..) => "PrepareHeadersRequest",
            PrimaryMessage::ProposalHeaders(..) => "ProposalHeaders",
            PrimaryMessage::VantageDirectResolutionWish(..) => "VantageDirectResolutionWish",
            PrimaryMessage::VantageDirectResolutionSuggest(..) => "VantageDirectResolutionSuggest",
            PrimaryMessage::VantageDirectResolutionProof(..) => "VantageDirectResolutionProof",
            PrimaryMessage::VantageDirectResolutionProposal(..) => {
                "VantageDirectResolutionProposal"
            }
            PrimaryMessage::VantageDirectResolutionStatement(..) => {
                "VantageDirectResolutionStatement"
            }
            PrimaryMessage::VantageDirectResolutionDone(..) => "VantageDirectResolutionDone",
            PrimaryMessage::VantageDirectResolutionValueFetch(..) => {
                "VantageDirectResolutionValueFetch"
            }
            PrimaryMessage::VantageDirectResolutionValueServe(..) => {
                "VantageDirectResolutionValueServe"
            }
            PrimaryMessage::VantageDirectResolutionWitness(..) => "VantageDirectResolutionWitness",
        }
    }
}

/// Records a received message type and serialized payload length.
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
// New variants must be appended: bincode encodes enum variant indices.
#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryWorkerMessage {
    /// Legacy author-targeted batch request. Kept at its original wire index;
    /// new code should use `SynchronizeAuthor`.
    Synchronize(Vec<Digest>, /* target */ PublicKey),
    /// Notifies the worker of a committed height.
    Cleanup(Height),
    /// Commit notification used for post-decision materialization; the
    /// UTC-millisecond timestamp also feeds benchmark latency accounting.
    Committed(u64 /* commit UTC-millis */, Vec<Digest>),
    /// Requests batches specifically from the current optimistic proposal
    /// leader, which must relay the tips it chose to propose.
    SynchronizeOptimistic(Vec<Digest>, /* proposal leader */ PublicKey),
    /// Requests batches only from holders named by a PoA, QC, or TC.
    SynchronizeProofSources(Vec<Digest>, Vec<PublicKey>),
    /// Pre-commit lane repair: retry only against the lane author.
    SynchronizeAuthor(Vec<Digest>, /* lane author */ PublicKey),
}

impl PrimaryWorkerMessage {
    /// Returns the wire type label.
    pub fn type_name(&self) -> &'static str {
        match self {
            PrimaryWorkerMessage::Synchronize(..) => "Synchronize",
            PrimaryWorkerMessage::Cleanup(..) => "Cleanup",
            PrimaryWorkerMessage::Committed(..) => "Committed",
            PrimaryWorkerMessage::SynchronizeOptimistic(..) => "SynchronizeOptimistic",
            PrimaryWorkerMessage::SynchronizeProofSources(..) => "SynchronizeProofSources",
            PrimaryWorkerMessage::SynchronizeAuthor(..) => "SynchronizeAuthor",
        }
    }
}

/// The messages sent by the workers to their primary.
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerPrimaryMessage {
    /// Reports a newly sealed batch.
    OurBatch(Digest, WorkerId),
    /// Reports a remote batch digest.
    OthersBatch(Digest, WorkerId),
}

impl WorkerPrimaryMessage {
    /// Returns the wire type label.
    pub fn type_name(&self) -> &'static str {
        match self {
            WorkerPrimaryMessage::OurBatch(..) => "OurBatch",
            WorkerPrimaryMessage::OthersBatch(..) => "OthersBatch",
        }
    }
}

/// Builds the process-wide channel-authentication context, or `None` when disabled.
///
/// Shared by the primary and the worker: both hold a committee, their own identity, and
/// the parameter document that carries the seed.
pub fn channel_auth(
    name: &PublicKey,
    committee: &Committee,
    parameters: &Parameters,
) -> Option<Arc<ChannelAuth>> {
    let seed = parameters
        .channel_auth_seed_bytes()
        .unwrap_or_else(|error| panic!("invalid channel-authentication configuration: {error}"))?;
    let index = committee
        .index_of(name)
        .expect("Our public key is not in the committee") as u8;
    let peers = committee.peer_channel_indices(name);
    info!(
        "Channel authentication enabled: {} peer address(es) authenticated",
        peers.len()
    );
    Some(Arc::new(ChannelAuth::new(
        &seed,
        index,
        committee.size(),
        peers,
    )))
}

pub struct Primary;

impl Primary {
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
        let (tx_headers_loopback, rx_headers_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_certificates_loopback, _rx_certificates_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_primary_messages, rx_primary_messages) = channel(CHANNEL_CAPACITY);
        let (tx_cert_requests, rx_cert_requests) = channel(CHANNEL_CAPACITY);
        let (tx_header_requests, rx_header_requests) = channel(CHANNEL_CAPACITY);
        let (tx_proposal_header_requests, rx_proposal_header_requests) = channel(CHANNEL_CAPACITY);
        let (tx_instance, rx_instance) = channel(CHANNEL_CAPACITY);
        let (tx_header_waiter_instances, rx_header_waiter_instances) = channel(CHANNEL_CAPACITY);
        let (tx_commit, rx_commit) = channel(CHANNEL_CAPACITY);
        let (_tx_mempool, rx_mempool) = channel(CHANNEL_CAPACITY);

        parameters
            .validate_header_faults(&committee)
            .unwrap_or_else(|error| panic!("invalid header fault configuration: {error}"));

        // Parsed by benchmark tooling.
        parameters.log();

        // Start the Prometheus metrics server.
        let metrics_address = committee
            .primary(&name)
            .expect("Our public key or worker id is not in the committee")
            .metrics;
        let mut binding_metrics_address = metrics_address;
        binding_metrics_address.set_ip("0.0.0.0".parse().unwrap());
        let registry = Registry::new();
        let (metrics, reporter) = Metrics::new(&registry);
        // Set protocol metrics.
        metrics.set_protocol_info(parameters.protocol.label());
        if let Some(mode) = parameters.tx_mode.as_deref() {
            metrics.set_transaction_mode_info(mode);
        }
        // Set the metrics activation time.
        metrics.set_active_from_millis(parameters.metrics_active_at_ms);
        // Report panics from spawned tasks.
        Metrics::install_panic_hook(metrics.clone());
        // Sample the primary store.
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
        reporter.clone().start(Duration::from_millis(
            parameters.metrics_report_interval_ms.max(1),
        ));
        start_prometheus_server(binding_metrics_address, &registry);
        info!("Primary {} metrics listening on {}", name, metrics_address);

        // Share the latest consensus round with cleanup tasks.
        let consensus_round = Arc::new(AtomicU64::new(0));

        // Resolve transport batching once for all primary components.
        let batch = network::BatchConfig {
            enabled: parameters.batch_messages,
            max_bytes: parameters.batch_max_bytes,
            max_delay_ms: parameters.batch_max_delay_ms,
        };

        // Derive the pairwise channel keys once. Senders authenticate the peer addresses
        // this covers and leave every other destination alone; peer listeners are handed
        // the same value and refuse a connection that cannot produce a valid tag.
        let channel_auth = channel_auth(&name, &committee, &parameters);

        match parameters.protocol {
            Protocol::Vantage => {
                // Start the Vantage core and worker-facing receiver.
                let (
                    tx_vantage,
                    tx_vantage_bulk,
                    tx_vantage_sequence,
                    ack_aggregator,
                    sequence_large_gap_drop,
                    sequence_install_drop_through,
                ) = crate::vantage::VantageCore::spawn(
                    name,
                    committee.clone(),
                    parameters.clone(),
                    store.clone(),
                    Some(metrics.clone()),
                    rx_our_digests,
                    tx_output,
                    channel_auth.clone(),
                );

                // Route primary messages to Vantage.
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
                        codec: crate::vantage::wire::VantageWireCodec::new(
                            &committee,
                            parameters.vantage_compact_ids,
                        )
                        .unwrap_or_else(|error| {
                            panic!("invalid Vantage wire configuration: {error}")
                        }),
                        committee: committee.clone(),
                        tx_bulk: tx_vantage_bulk,
                        tx_sequence: tx_vantage_sequence,
                        sequence_large_gap_drop,
                        sequence_install_drop_through,
                        ack_aggregator,
                        metrics: Some(metrics.clone()),
                    },
                    Some(metrics.clone()),
                    // Acknowledge each received frame.
                    /* acks */
                    true,
                    parameters.batch_messages,
                    "primary_to_primary",
                    channel_auth.clone(),
                );
                info!(
                    "Primary {} listening to primary messages on {}",
                    name, address
                );

                // Route local and remote worker batches.
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
                    // Worker frames are not acknowledged.
                    /* acks */
                    false,
                    parameters.batch_messages,
                    "worker_to_primary",
                    // Our own workers reach us over a same-host link, which the model does not cover.
                    None,
                );
                info!(
                    "Primary {} listening to workers messages on {}",
                    name, address
                );

                // Store remote worker digests.
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
                // Start Simple-IT with separate data-plane instances.
                let (tx_simpleit, ack_aggregator) = crate::simpleit::SimpleItCore::spawn(
                    name,
                    committee.clone(),
                    parameters.clone(),
                    store.clone(),
                    Some(metrics.clone()),
                    rx_our_digests,
                    tx_output,
                    channel_auth.clone(),
                );

                // Route primary messages to Simple-IT.
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
                    // Acknowledge each received frame.
                    /* acks */
                    true,
                    parameters.batch_messages,
                    "primary_to_primary",
                    channel_auth.clone(),
                );
                info!(
                    "Primary {} listening to primary messages on {}",
                    name, address
                );

                // Route local and remote worker batches.
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
                    // Worker frames are not acknowledged here.
                    /* acks */
                    false,
                    parameters.batch_messages,
                    "worker_to_primary",
                    // Our own workers reach us over a same-host link, which the model does not cover.
                    None,
                );
                info!(
                    "Primary {} listening to workers messages on {}",
                    name, address
                );

                // Receive batch digests from other workers.
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
                let certified_only_leader =
                    is_leader_relay_publisher(&name, &committee, &parameters);
                let withholding_dests = config::withheld_destinations(
                    &committee,
                    &name,
                    parameters.withhold_senders,
                    &parameters.withhold_publishers,
                    parameters.withhold_count,
                    parameters.withhold_stride,
                    &parameters.withhold_receivers,
                );
                let withheld_header_dests = parameters
                    .withhold_headers
                    .then(|| withholding_dests.clone())
                    .flatten();
                let suppressed_repair_destinations = parameters
                    .withhold_repair
                    .then_some(withholding_dests)
                    .flatten();

                // Build the dependency synchronizer (and the verified-object
                // cache it owns) before the receiver, which shares the cache.
                let synchronizer = Synchronizer::new(
                    name,
                    &committee,
                    store.clone(),
                    /* tx_header_waiter */ tx_sync_headers,
                );
                let verified = synchronizer.verified();

                // Receive messages from other primaries.
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
                        tx_proposal_header_requests,
                        metrics: metrics.clone(),
                        committee: committee.clone(),
                        verified,
                    },
                    Some(metrics.clone()),
                    // Acknowledge each received frame.
                    /* acks */
                    true,
                    parameters.batch_messages,
                    "primary_to_primary",
                    channel_auth.clone(),
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
                    // Worker frames are not acknowledged here.
                    /* acks */
                    false,
                    parameters.batch_messages,
                    "worker_to_primary",
                    // Our own workers reach us over a same-host link, which the model does not cover.
                    None,
                );
                info!(
                    "Primary {} listening to workers messages on {}",
                    name, address
                );

                let verified = synchronizer.verified();

                // Core handles headers, votes, and certificates.
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
                    /* tx info */ tx_instance,
                    LeaderElector::new(committee.clone()),
                    parameters.timeout_delay,
                    parameters.use_optimistic_tips,
                    parameters.use_parallel_proposals,
                    parameters.k,
                    parameters.use_fast_path,
                    parameters.fast_path_timeout,
                    parameters.use_ride_share,
                    parameters.car_timeout,
                    parameters.all_to_all,
                    certified_only_leader,
                    parameters.simulate_asynchrony,
                    parameters.asynchrony_start,
                    parameters.asynchrony_duration,
                    // Optional per-destination latency map.
                    parameters
                        .latency_table
                        .as_deref()
                        .map(|table| committee.latency_map(&name, table))
                        .unwrap_or_default(),
                    // Data-plane withholding destinations.
                    withheld_header_dests,
                    // Finite-delay original-header destinations.
                    config::late_header_destinations(
                        &committee,
                        &name,
                        &parameters.late_header_publishers,
                        &parameters.late_header_receivers,
                    ),
                    parameters.late_header_delay_ms,
                    // Time-windowed withholding.
                    parameters.withhold_window.clone(),
                    metrics.clone(),
                    batch,
                    channel_auth.clone(),
                    // Reconnect backoff for the primary network.
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
                    channel_auth.clone(),
                );

                // Track the latest round for garbage collection.
                GarbageCollector::spawn(
                    &name,
                    &committee,
                    store.clone(),
                    consensus_round.clone(),
                    rx_consensus,
                    tx_certificates_loopback.clone(),
                    metrics.clone(),
                    batch,
                    channel_auth.clone(),
                );

                // Store batch digests used to validate headers.
                PayloadReceiver::spawn(store.clone(), /* rx_workers */ rx_others_digests);

                // Fetch missing header dependencies and retry validation.
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
                    channel_auth.clone(),
                );

                // Build headers from parent certificates and worker batches.
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

                // Serve certificate and header requests.
                Helper::spawn(
                    committee.clone(),
                    store,
                    rx_cert_requests,
                    rx_header_requests,
                    rx_proposal_header_requests,
                    metrics.clone(),
                    batch,
                    channel_auth.clone(),
                    suppressed_repair_destinations,
                    parameters.withhold_window.clone(),
                    verified,
                );

                // Parsed by benchmark tooling.
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
    tx_header_requests: Sender<(Vec<Digest>, PublicKey, bool)>,
    tx_proposal_header_requests: Sender<(Proposal, Height, PublicKey)>,
    metrics: Arc<Metrics>,
    /// The committee and shared verified-object cache: signatures are checked
    /// here, on the per-peer connection task, so the single Core task only
    /// performs cache lookups on its hot path.
    committee: Committee,
    verified: VerifiedCache,
}

impl PrimaryReceiverHandler {
    /// Verifies a message on the connection task and marks it in the shared
    /// cache. Returns false (after counting the drop) when verification
    /// fails; the Core would reject the message identically, so dropping it
    /// here changes no acceptance decision.
    /// A header is admitted with its own signature and its parent PoA both
    /// verified (or already cached).
    fn header_verified(&self, header: &Header) -> bool {
        self.verified
            .check_header(header, &self.committee)
            .and_then(|_| {
                self.verified
                    .check_certificate(&header.parent_cert, &self.committee)
            })
            .is_ok()
    }

    fn ingress_verified(&self, message: &PrimaryMessage) -> bool {
        let verdict = match message {
            PrimaryMessage::Header(header, _) => self.header_verified(header),
            PrimaryMessage::Vote(vote) => self.verified.check_vote(vote, &self.committee).is_ok(),
            PrimaryMessage::Certificate(certificate) => self
                .verified
                .check_certificate(certificate, &self.committee)
                .is_ok(),
            PrimaryMessage::Timeout(timeout) => self
                .verified
                .check_timeout(timeout, &self.committee)
                .is_ok(),
            PrimaryMessage::TC(tc) => self.verified.check_tc(tc, &self.committee).is_ok(),
            PrimaryMessage::ConsensusRequest(request) => self
                .verified
                .check_consensus_request(request, &self.committee)
                .is_ok(),
            PrimaryMessage::ConsensusVote(vote) => self
                .verified
                .check_consensus_vote(vote, &self.committee)
                .is_ok(),
            _ => true,
        };
        if !verdict {
            warn!(
                "Dropping {} that failed ingress verification",
                message.type_name()
            );
            self.metrics
                .primary_ingress_verify_failures_total
                .with_label_values(&[message.type_name()])
                .inc();
        }
        verdict
    }
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        _authenticated_peer: Option<u8>,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        // The receiver sends one acknowledgement per frame.

        // Deserialize and parse the message.
        let message: PrimaryMessage =
            bincode::deserialize(&serialized).map_err(DagError::SerializationError)?;
        record_typed_received(&self.metrics, message.type_name(), serialized.len());
        // A closed channel below means the primary is shutting down while
        // this connection task still holds a frame; drop the message exactly
        // as a stopped node would, instead of panicking mid-teardown.
        match message {
            PrimaryMessage::CertificatesRequest(missing, requestor) => {
                let _ = self.tx_cert_requests.send((missing, requestor)).await;
            }
            PrimaryMessage::HeadersRequest(missing, requestor) => {
                let _ = self
                    .tx_header_requests
                    .send((missing, requestor, false))
                    .await;
            }
            PrimaryMessage::PrepareHeadersRequest(missing, requestor) => {
                let _ = self
                    .tx_header_requests
                    .send((missing, requestor, true))
                    .await;
            }
            PrimaryMessage::ProposalHeadersRequest(proposal, stop_height, requestor) => {
                let _ = self
                    .tx_proposal_header_requests
                    .send((proposal, stop_height, requestor))
                    .await;
            }
            PrimaryMessage::ProposalHeaders(headers) => {
                // Verify here (mostly cache hits for headers already seen on
                // the wire), drop what fails, and hand the suffix to the Core
                // as one unit instead of one message per header.
                let verified: Vec<_> = headers
                    .into_iter()
                    .filter(|header| {
                        let admitted = self.header_verified(header);
                        if !admitted {
                            warn!("Dropping a suffix header that failed ingress verification");
                            self.metrics
                                .primary_ingress_verify_failures_total
                                .with_label_values(&["ProposalHeaders"])
                                .inc();
                        }
                        admitted
                    })
                    .collect();
                if !verified.is_empty()
                    && self
                        .tx_primary_messages
                        .send(PrimaryMessage::ProposalHeaders(verified))
                        .await
                        .is_err()
                {
                    debug!("Dropping a repaired suffix received during shutdown");
                }
            }
            request => {
                if !self.ingress_verified(&request) {
                    return Ok(());
                }
                if self.tx_primary_messages.send(request).await.is_err() {
                    debug!("Dropping a wire message received during shutdown");
                }
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
        _authenticated_peer: Option<u8>,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        // Deserialize and parse the message.
        let message: WorkerPrimaryMessage =
            bincode::deserialize(&serialized).map_err(DagError::SerializationError)?;

        match message {
            WorkerPrimaryMessage::OurBatch(digest, worker_id) => {
                record_typed_received(&self.metrics, "OurBatch", serialized.len());
                self.tx_our_digests
                    .send((digest, worker_id))
                    .await
                    .expect("Failed to send workers' digests")
            }
            WorkerPrimaryMessage::OthersBatch(digest, worker_id) => {
                record_typed_received(&self.metrics, "OthersBatch", serialized.len());
                self.tx_others_digests
                    .send((digest, worker_id))
                    .await
                    .expect("Failed to send workers' digests")
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leader_relay_marks_only_byzantine_autobahn_publishers() {
        let (committee, _) = Committee::local_benchmark(7, 1, 18_000);
        let mut parameters = Parameters {
            leader_relay_attack: true,
            withhold_senders: 2,
            ..Parameters::default()
        };
        let authors: Vec<_> = committee.authorities.keys().copied().collect();

        assert!(is_leader_relay_publisher(
            &authors[0],
            &committee,
            &parameters
        ));
        assert!(is_leader_relay_publisher(
            &authors[1],
            &committee,
            &parameters
        ));
        assert!(!is_leader_relay_publisher(
            &authors[2],
            &committee,
            &parameters
        ));

        parameters.leader_relay_attack = false;
        assert!(!is_leader_relay_publisher(
            &authors[0],
            &committee,
            &parameters
        ));
    }
}

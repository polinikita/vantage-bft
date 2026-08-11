// Simple-IT wiring supplies cut snapshots and tip availability to `CutEngine`.

use crate::messages::{Ack, Header, Proposal};
use crate::primary::{Height, PrimaryMessage, CHANNEL_CAPACITY};
use crate::simpleit::effects::{CutEffect, CutOut};
use crate::simpleit::engine::{self, CutEngine, TipOracle};
use crate::simpleit::messages::{Cut, CutRound};
use crate::vantage::block;
use crate::vantage::lanes::{
    aggregate_received_ack, AckAggregator, AckAvailability, AvailEntry, BlockCache, LaneManager,
    SharedAckAggregator, SharedBlocks,
};
use crate::vantage::payload::{append_missing_payload_sync, PayloadIo};
use crate::vantage::repair::Repairer;
use crate::vantage::resume::{ResumeServe, ResumeTrigger};
use crate::vantage::wire::{self, DeclaredSender, Wire};
use crate::vantage::{BlockRef, Effect};
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, Parameters, Protocol, WorkerId};
use crypto::{Digest, PublicKey};
use futures::stream::{FuturesUnordered, StreamExt};
use metrics::Metrics;
use network::{BatchConfig, MessageHandler, ReliableSender, SimpleSender, Writer};
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::error::Error;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

/// Returns the declared sender. Explicit senders require committee membership;
/// served proposals require a pending fetch authorization.
impl DeclaredSender for engine::Inbound {
    fn declared_sender(&self) -> Option<PublicKey> {
        match self {
            engine::Inbound::CutProposal(p) => Some(p.proposer),
            engine::Inbound::CutVote(v) => Some(v.author),
            engine::Inbound::CutReady(r) => Some(r.author),
            engine::Inbound::Decide(d) => Some(d.author),
            engine::Inbound::Timeout(t) => Some(t.author),
            engine::Inbound::TimeoutAccept(a) => Some(a.author),
            engine::Inbound::TimerFired(_) => None,
            engine::Inbound::CutFetch(_, _, requester) => Some(*requester),
            engine::Inbound::CutServe(_) => None,
        }
    }
}

/// Data-plane and cut-consensus messages routed to `SimpleItCore`.
#[derive(Debug, Clone)]
pub enum Inbound {
    Publish(PublicKey, Header),
    Serve(Header),
    HeadersRequest(Vec<Digest>, PublicKey),
    /// Availability threshold emitted by the shared ACK aggregator.
    AckAvailability(AckAvailability),
    Avail(Vec<AvailEntry>, PublicKey),
    Cut(engine::Inbound),
    LaneResume(PublicKey, Height, PublicKey),
}

impl DeclaredSender for Inbound {
    fn declared_sender(&self) -> Option<PublicKey> {
        match self {
            Inbound::Publish(sender, _) => Some(*sender),
            Inbound::HeadersRequest(_, requestor) => Some(*requestor),
            Inbound::Serve(_) | Inbound::AckAvailability(_) => None,
            Inbound::Avail(_, s) => Some(*s),
            Inbound::Cut(cut_inbound) => cut_inbound.declared_sender(),
            Inbound::LaneResume(_, _, requester) => Some(*requester),
        }
    }
}

/// Handles Simple-IT primary-to-primary messages.
#[derive(Clone)]
pub struct SimpleItReceiverHandler {
    pub tx: Sender<Inbound>,
    pub ack_aggregator: SharedAckAggregator,
    /// Optional metrics for direct unit tests.
    pub metrics: Option<Arc<Metrics>>,
}

#[async_trait]
impl MessageHandler for SimpleItReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        let message: PrimaryMessage = bincode::deserialize(&serialized)?;

        if let Some(metrics) = &self.metrics {
            crate::primary::record_typed_received(metrics, message.type_name(), serialized.len());
        }

        let inbound = match message {
            PrimaryMessage::Header(h, false) => Inbound::Publish(h.author, h),
            PrimaryMessage::Header(h, true) => Inbound::Serve(h),
            PrimaryMessage::HeadersRequest(digests, requestor) => {
                Inbound::HeadersRequest(digests, requestor)
            }
            PrimaryMessage::VantageAck(a) => {
                let Some(availability) =
                    aggregate_received_ack(&self.ack_aggregator, self.metrics.as_deref(), &a)
                else {
                    return Ok(());
                };
                Inbound::AckAvailability(availability)
            }
            PrimaryMessage::VantageAvail(entries, sender) => Inbound::Avail(entries, sender),
            PrimaryMessage::SimpleItCutProposal(p) => Inbound::Cut(engine::Inbound::CutProposal(p)),
            PrimaryMessage::SimpleItCutVote(v) => Inbound::Cut(engine::Inbound::CutVote(v)),
            PrimaryMessage::SimpleItDecide(d) => Inbound::Cut(engine::Inbound::Decide(d)),
            PrimaryMessage::SimpleItTimeout(t) => Inbound::Cut(engine::Inbound::Timeout(t)),
            PrimaryMessage::SimpleItTimeoutAccept(a) => {
                Inbound::Cut(engine::Inbound::TimeoutAccept(a))
            }
            PrimaryMessage::SimpleItCutReady(r) => Inbound::Cut(engine::Inbound::CutReady(r)),
            PrimaryMessage::SimpleItCutFetch(round, digest, requester) => {
                Inbound::Cut(engine::Inbound::CutFetch(round, digest, requester))
            }
            PrimaryMessage::SimpleItCutServe(p) => Inbound::Cut(engine::Inbound::CutServe(p)),
            PrimaryMessage::VantageLaneResume(author, from, requester) => {
                Inbound::LaneResume(author, from, requester)
            }
            _ => return Ok(()),
        };
        self.tx
            .send(inbound)
            .await
            .expect("Failed to send simpleit message");
        Ok(())
    }
}

/// Reads tip availability without borrowing the entire core.
struct TipOracleAdapter<'a> {
    lm: &'a LaneManager,
    committee: &'a Committee,
}

impl TipOracle for TipOracleAdapter<'_> {
    fn available_at_validity(&self, author: &PublicKey, tip: &Proposal) -> bool {
        let r: BlockRef = (*author, tip.height, tip.header_digest.clone());
        self.lm
            .is_q_available(&r, self.committee.validity_threshold())
    }
}

/// One-shot cut timers; stale firings are rejected by `CutEngine`.
type PendingTimers = FuturesUnordered<Pin<Box<dyn Future<Output = CutRound> + Send>>>;

/// Materialized worker batches and headers for one commit.
type CommitBatch = (Vec<(WorkerId, Vec<Digest>)>, Vec<Header>);

pub struct SimpleItCore {
    name: PublicKey,
    /// Trusted wire senders.
    members: HashSet<PublicKey>,
    committee: Committee,
    lm: LaneManager,
    ack_aggregator: SharedAckAggregator,
    rep: Repairer,
    wire: Wire,
    payload: PayloadIo,
    cut: CutEngine,
    pending_timers: PendingTimers,

    header_size: usize,
    max_header_delay: u64,
    digests: Vec<(Digest, WorkerId)>,
    payload_size: usize,

    /// Suppresses per-block ACK broadcasts while retaining local ACKs.
    ack_watermarks: bool,
    ack_watermark_period_ms: u64,

    resume_trigger: ResumeTrigger,
    resume_serve: ResumeServe,
    resume_check_period_ms: u64,
    resume_backoff_ms: u64,
    resume_batch: u64,

    sid: Digest,
    genesis: Digest,
    max_block_payload: usize,
    /// Last materialized block per author.
    committed_watermark: HashMap<PublicKey, (Height, Digest)>,
    /// Committed rounds awaiting local materialization, ordered by round.
    commit_queue: BTreeMap<CutRound, Cut>,
    gc_window: CutRound,
    last_gc_floor: CutRound,

    metrics: Option<Arc<Metrics>>,
}

type BuildOutput = (
    SimpleItCore,
    Receiver<Inbound>,
    Receiver<(Digest, Digest, WorkerId)>,
    Sender<Inbound>,
    SharedAckAggregator,
);

impl SimpleItCore {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        metrics: Option<Arc<Metrics>>,
        rx_our_digests: Receiver<(Digest, WorkerId)>,
        tx_output: Sender<Header>,
    ) -> (Sender<Inbound>, SharedAckAggregator) {
        let (core, rx_simpleit, rx_payload_ready, tx_simpleit, ack_aggregator) =
            Self::build(name, committee, parameters, store, metrics, tx_output);
        tokio::spawn(core.run(rx_simpleit, rx_our_digests, rx_payload_ready));
        (tx_simpleit, ack_aggregator)
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        metrics: Option<Arc<Metrics>>,
        tx_output: Sender<Header>,
    ) -> BuildOutput {
        let (tx_simpleit, rx_simpleit) = channel(CHANNEL_CAPACITY);
        let (tx_payload_ready, rx_payload_ready) = channel(CHANNEL_CAPACITY);

        let members: HashSet<PublicKey> = committee.authorities.keys().cloned().collect();

        let sid = block::session_id(&committee);
        let genesis = block::genesis_digest(&sid);
        let blocks: SharedBlocks = Arc::new(Mutex::new(BlockCache::new()));
        let ack_aggregator: SharedAckAggregator =
            Arc::new(Mutex::new(AckAggregator::new(committee.clone())));

        let mut lm = LaneManager::with_shared_blocks(
            name,
            committee.clone(),
            parameters.max_block_payload,
            store.clone(),
            blocks.clone(),
        );
        let mut rep = Repairer::new(
            name,
            committee.clone(),
            sid.clone(),
            genesis.clone(),
            parameters.max_block_payload,
            blocks,
        );
        let core_metrics = metrics.clone();
        if let Some(m) = metrics {
            lm = lm.with_metrics(m.clone());
            rep = rep.with_metrics(m);
        }

        let variant = match parameters.protocol {
            Protocol::SimpleItBracha => engine::Variant::Bracha,
            Protocol::SimpleIt
            | Protocol::AutobahnOptimistic
            | Protocol::AutobahnSeamless
            | Protocol::Vantage => engine::Variant::Opt,
        };
        let cut =
            CutEngine::new(name, committee.clone(), parameters.timeout_delay).with_variant(variant);

        let other_primaries: Vec<(PublicKey, SocketAddr)> = committee
            .others_primaries(&name)
            .into_iter()
            .map(|(pk, addr)| (pk, addr.primary_to_primary))
            .collect();
        let other_primary_addrs: Vec<SocketAddr> =
            other_primaries.iter().map(|(_, a)| *a).collect();

        let withheld_header_dests: wire::WithheldHeaderDests =
            config::withheld_destinations(&committee, &name, parameters.withhold_senders).map(
                |blocked| {
                    let full: Vec<(PublicKey, SocketAddr)> = other_primaries
                        .iter()
                        .filter(|(pk, _)| !blocked.contains(pk))
                        .copied()
                        .collect();
                    let addrs: Vec<SocketAddr> = full.iter().map(|(_, a)| *a).collect();
                    (addrs, full)
                },
            );

        let worker_addresses: HashMap<WorkerId, SocketAddr> = committee
            .our_workers_by_id(&name)
            .expect("Our public key is not in the committee")
            .into_iter()
            .map(|(id, addr)| (id, addr.primary_to_worker))
            .collect();

        let latency_map = parameters
            .latency_table
            .as_deref()
            .map(|table| committee.latency_map(&name, table))
            .unwrap_or_default();

        let batch = BatchConfig {
            enabled: parameters.batch_messages,
            max_bytes: parameters.batch_max_bytes,
            max_delay_ms: parameters.batch_max_delay_ms,
        };

        let in_flight: wire::InFlightMap = Arc::new(Mutex::new(HashMap::new()));
        let wire_codec = wire::VantageWireCodec::new(&committee, false)
            .expect("legacy primary wire supports any committee size");
        let resume_senders = wire::spawn_resume_sender(
            latency_map.clone(),
            batch,
            core_metrics.clone(),
            in_flight.clone(),
            wire_codec.clone(),
            parameters.replay_chunk_bytes,
            parameters.replay_chunk_interval_ms,
            parameters.replay_serve_max_bytes,
            parameters.retry_backoff_max_ms,
        );

        let core = Self {
            name,
            members,
            committee,
            lm,
            ack_aggregator: ack_aggregator.clone(),
            rep,
            wire: Wire {
                codec: wire_codec,
                network: {
                    let mut s = ReliableSender::new()
                        .with_latency(latency_map.clone())
                        .with_batching(batch)
                        .with_retry_backoff_max_ms(parameters.retry_backoff_max_ms);
                    if let Some(m) = &core_metrics {
                        s = s.with_metrics(m.clone());
                    }
                    s
                },
                worker_network: {
                    let mut s = SimpleSender::new()
                        .with_latency(latency_map)
                        .with_batching(batch);
                    if let Some(m) = &core_metrics {
                        s = s.with_metrics(m.clone());
                    }
                    s
                },
                resume_lane_tx: resume_senders.lane,
                replay_tx: resume_senders.replay,
                sequence_tx: resume_senders.sequence,
                replay_generation: resume_senders.generation,
                cancel_handlers: Vec::new(),
                last_prune_len: 0,
                other_primaries: other_primaries.clone(),
                other_primary_addrs,
                worker_addresses,
                withheld_header_dests,
                withhold_window: parameters.withhold_window.clone(),
                metrics: core_metrics.clone(),
                addr_to_peer: other_primaries
                    .iter()
                    .map(|(pk, addr)| (*addr, *pk))
                    .collect(),
                dirty_map: Arc::new(Mutex::new(HashMap::new())),
                in_flight,
            },
            payload: PayloadIo::new(store, tx_payload_ready, tx_output, core_metrics.clone()),
            cut,
            pending_timers: FuturesUnordered::new(),
            header_size: parameters.header_size,
            max_header_delay: parameters.max_header_delay,
            digests: Vec::new(),
            payload_size: 0,
            ack_watermarks: parameters.ack_watermarks,
            ack_watermark_period_ms: parameters.ack_watermark_period_ms,
            resume_trigger: ResumeTrigger::with_max_concurrent(parameters.resume_max_concurrent),
            resume_serve: ResumeServe::new(),
            resume_check_period_ms: parameters.resume_check_period_ms,
            resume_backoff_ms: parameters.resume_backoff_ms,
            resume_batch: parameters.resume_batch,
            sid,
            genesis,
            max_block_payload: parameters.max_block_payload,
            committed_watermark: HashMap::new(),
            commit_queue: BTreeMap::new(),
            gc_window: parameters.simpleit_gc_window_rounds.max(1),
            last_gc_floor: 1,
            metrics: core_metrics,
        };
        (
            core,
            rx_simpleit,
            rx_payload_ready,
            tx_simpleit,
            ack_aggregator,
        )
    }

    async fn run(
        mut self,
        mut rx_simpleit: Receiver<Inbound>,
        mut rx_our_digests: Receiver<(Digest, WorkerId)>,
        mut rx_payload_ready: Receiver<(Digest, Digest, WorkerId)>,
    ) {
        self.lm.restore_own_frontier().await;
        // Start the round-1 proposal and fallback timer.
        let effects = {
            let tips = self.build_cut();
            let oracle = TipOracleAdapter {
                lm: &self.lm,
                committee: &self.committee,
            };
            let mut effects = self.cut.try_propose_cut_for_current_round(&tips, &oracle);
            effects.extend(self.cut.schedule_cut_timer(1));
            effects
        };
        self.execute_cut(effects).await;

        let header_timer = tokio::time::sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(header_timer);

        let mut prune_tick = tokio::time::interval(Duration::from_secs(1));
        prune_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut avail_tick = if self.ack_watermarks {
            let mut interval =
                tokio::time::interval(Duration::from_millis(self.ack_watermark_period_ms));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            Some(interval)
        } else {
            None
        };

        let mut resume_tick =
            tokio::time::interval(Duration::from_millis(self.resume_check_period_ms));
        resume_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            self.wire.maybe_prune_cancel_handlers();

            tokio::select! {
                biased;

                Some(inbound) = rx_simpleit.recv() => {
                    self.dispatch_inbound(inbound).await;
                }

                Some((header_digest, digest, worker_id)) = rx_payload_ready.recv() => {
                    self.on_payload_ready(header_digest, digest, worker_id).await;
                }

                Some((digest, worker_id)) = rx_our_digests.recv() => {
                    self.payload_size += digest.size();
                    self.digests.push((digest, worker_id));
                    if self.payload_size >= self.header_size {
                        self.seal_own_header().await;
                        header_timer.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(self.max_header_delay));
                    }
                }

                () = &mut header_timer => {
                    self.seal_own_header().await;
                    header_timer.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(self.max_header_delay));
                }

                Some(round) = self.pending_timers.next(), if !self.pending_timers.is_empty() => {
                    let effects = {
                        let tips = self.build_cut();
                        let oracle = TipOracleAdapter { lm: &self.lm, committee: &self.committee };
                        self.cut.handle(engine::Inbound::TimerFired(round), &tips, &oracle)
                    };
                    self.execute_cut(effects).await;
                }

                () = async {
                    match avail_tick.as_mut() {
                        Some(t) => { t.tick().await; }
                        None => std::future::pending::<()>().await,
                    }
                }, if avail_tick.is_some() => {
                    if let Some(entries) = self.lm.take_avail_flush() {
                        if let Some(metrics) = &self.metrics {
                            metrics.vantage_avail_sent.inc();
                        }
                        self.wire
                            .broadcast_message(PrimaryMessage::VantageAvail(entries, self.name))
                            .await;
                    }
                }

                _ = resume_tick.tick() => {
                    let now = Instant::now();
                    let authors: Vec<PublicKey> =
                        self.wire.other_primaries.iter().map(|(pk, _)| *pk).collect();
                    for author in authors {
                        self.try_resume_request(author, now);
                    }
                }

                _ = prune_tick.tick() => {
                    self.wire.prune_cancel_handlers();
                    self.collect_internal_garbage();
                }
            }
        }
    }

    /// Returns one certified tip per available committee lane.
    fn build_cut(&self) -> Cut {
        self.committee
            .authorities
            .keys()
            .filter_map(|author| {
                self.lm.c_candidate(author).map(|(_, height, digest)| {
                    (
                        *author,
                        Proposal {
                            header_digest: digest,
                            height,
                        },
                    )
                })
            })
            .collect()
    }

    async fn dispatch_inbound(&mut self, inbound: Inbound) {
        if !wire::sender_is_member(&inbound, &self.members) {
            if let Some(metrics) = &self.metrics {
                metrics.vantage_rejected_nonmember_total.inc();
            }
            return;
        }
        match inbound {
            Inbound::Publish(sender, header) => {
                let author = header.author;
                let before = self.lm.own_direct_frontier(&author);
                let effects = self.lm.process_publish(sender, header).await;
                self.execute(effects).await;
                if self.lm.own_direct_frontier(&author) > before {
                    self.try_resume_request(author, Instant::now());
                }
            }
            Inbound::Serve(header) => {
                let effects = self.serve_effects(header).await;
                self.execute(effects).await;
            }
            Inbound::HeadersRequest(digests, requestor) => {
                let mut effects = Vec::new();
                for d in digests {
                    effects.extend(self.rep.on_request(requestor, d));
                }
                self.execute(effects).await;
            }
            Inbound::AckAvailability(availability) => {
                let effects = self.on_ack_availability(availability);
                self.execute(effects).await;
            }
            Inbound::Avail(entries, sender) => {
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_avail_received.inc();
                }
                let refs = self.lm.resolve_watermark(sender, &entries);
                let effects = self.credit_refs(sender, refs);
                self.execute(effects).await;
            }
            Inbound::Cut(cut_inbound) => {
                let effects = {
                    let tips = self.build_cut();
                    let oracle = TipOracleAdapter {
                        lm: &self.lm,
                        committee: &self.committee,
                    };
                    self.cut.handle(cut_inbound, &tips, &oracle)
                };
                self.execute_cut(effects).await;
            }
            Inbound::LaneResume(author, from, requester) => {
                if author != self.name {
                    return;
                }
                let floor = self.lm.earliest_authored_height(&author);
                let from = from.max(floor);
                let tip = self.lm.own_tip_height();
                if from > tip {
                    return;
                }
                let now = Instant::now();
                let backoff = Duration::from_millis(self.resume_backoff_ms);
                if !self
                    .resume_serve
                    .should_serve(requester, from, now, backoff)
                {
                    return;
                }
                let to = (from + self.resume_batch - 1).min(tip);
                let mut effects = Vec::with_capacity((to - from + 1) as usize);
                for height in from..=to {
                    if let Some(header) = self.lm.author_block_at(&author, height) {
                        if let Some(metrics) = &self.metrics {
                            metrics.vantage_lane_resume_blocks_served.inc();
                        }
                        effects.push(Effect::ResumeServeTo(requester, header));
                    }
                }
                self.execute(effects).await;
            }
        }
    }

    async fn serve_effects(&mut self, header: Header) -> Vec<Effect> {
        let mut effects = self.rep.on_serve(header.clone());
        append_missing_payload_sync(&mut self.lm, &header, &mut effects).await;
        effects
    }

    fn on_ack_availability(&mut self, availability: AckAvailability) -> Vec<Effect> {
        self.lm.process_ack_availability(availability)
    }

    fn record_local_ack(&mut self, ack: &Ack) -> Vec<Effect> {
        let availability = {
            let mut aggregator = self.ack_aggregator.lock();
            aggregator
                .record_ack(self.name, ack.reference())
                .availability
        };
        availability
            .map(|availability| self.on_ack_availability(availability))
            .unwrap_or_default()
    }

    /// Credits watermark references through the shared ACK aggregator.
    fn credit_refs(&mut self, sender: PublicKey, refs: Vec<BlockRef>) -> Vec<Effect> {
        let mut effects = Vec::new();
        for r in refs {
            let result = {
                let mut aggregator = self.ack_aggregator.lock();
                aggregator.record_ack(sender, r)
            };
            if !result.accepted {
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_rejected_nonmember_total.inc();
                }
                continue;
            }
            if let Some(metrics) = &self.metrics {
                metrics.vantage_avail_credited_refs.inc();
            }
            if let Some(availability) = result.availability {
                effects.extend(self.on_ack_availability(availability));
            }
        }
        effects
    }

    /// Executes data-plane effects and rejects consensus-only variants.
    async fn execute(&mut self, initial: Vec<Effect>) {
        let mut queue: VecDeque<Effect> = initial.into();
        while let Some(effect) = queue.pop_front() {
            match effect {
                Effect::BroadcastPublish(header) => {
                    self.wire
                        .broadcast_message(PrimaryMessage::Header(header, false))
                        .await
                }
                Effect::BroadcastAck(ack) => {
                    queue.extend(self.record_local_ack(&ack));
                    if !self.ack_watermarks {
                        self.wire
                            .broadcast_message(PrimaryMessage::VantageAck(ack))
                            .await
                    }
                }
                Effect::SyncBatches(author, header_digest, missing) => {
                    self.payload
                        .sync_batches(&mut self.wire, author, header_digest, missing)
                        .await;
                }
                Effect::RequestTo(peer, digest) => {
                    self.wire
                        .send_message(
                            peer,
                            PrimaryMessage::HeadersRequest(vec![digest], self.name),
                        )
                        .await;
                }
                Effect::ServeTo(peer, header) => {
                    self.wire
                        .send_message(peer, PrimaryMessage::Header(header, true))
                        .await
                }
                Effect::BlockCached(digest) => {
                    // Retry work waiting on this block.
                    for (sender, r) in self.lm.retry_pending_avail(&digest) {
                        queue.extend(self.credit_refs(sender, vec![r]));
                    }
                    queue.extend(self.rep.on_block_available(digest));
                    // Retry commit materialization.
                    self.drain_commit_queue().await;
                }

                Effect::ResumeServeTo(requester, header) => {
                    self.wire.enqueue_resume_header(requester, header);
                }
                other @ (Effect::BroadcastPropose(_)
                | Effect::BroadcastEcho(_)
                | Effect::BroadcastEchoSkip(_)
                | Effect::BroadcastReady(_)
                | Effect::BroadcastNoReady(_)
                | Effect::BroadcastSkipVote(_)
                | Effect::Fixed(_, _)
                | Effect::Completed(_, _, _)
                | Effect::Sealed(_, _)
                | Effect::ArmTimer(_, _, _)
                | Effect::NotifyCommitted(_, _, _)
                | Effect::BroadcastWish(_)
                | Effect::Enter(_)
                | Effect::RaiseWish(_)
                | Effect::SequenceFinalized { .. }
                | Effect::RecoverOwnLane(_)
                | Effect::CompletionReportable(_, _)
                | Effect::BroadcastCompReport(_, _)
                | Effect::BroadcastControlInit(_, _)
                | Effect::BroadcastControlEcho(_)
                | Effect::BroadcastControlReady(_)
                | Effect::BroadcastControlCommit(_)
                | Effect::BroadcastControlTimeoutVote(_)
                | Effect::BroadcastControlTimeoutAccept(_)
                | Effect::ControlFetchTo(_, _, _)
                | Effect::ControlServeTo(_, _, _)
                | Effect::ArmControlTimer(_, _)
                | Effect::ApplyAnchor(_, _, _)
                | Effect::BodyFetchTo(_, _, _)
                | Effect::BodyServeTo(_, _, _)
                // Vantage handles availability claims.
                | Effect::AvailClaimed(_, _)) => {
                    debug_assert!(
                        false,
                        "SimpleItCore's data-plane effect loop received a Vantage-\
                         consensus-only Effect variant that lm/rep can never \
                         construct: {:?}",
                        other
                    );
                    if let Some(metrics) = &self.metrics {
                        metrics.simpleit_unexpected_effect_total.inc();
                    }
                }
            }
        }
    }

    /// Executes cut-consensus effects.
    async fn execute_cut(&mut self, effects: Vec<CutEffect>) {
        for effect in effects {
            match effect {
                CutEffect::Broadcast(out) => {
                    let message = match out {
                        CutOut::CutProposal(p) => PrimaryMessage::SimpleItCutProposal(p),
                        CutOut::CutVote(v) => PrimaryMessage::SimpleItCutVote(v),
                        CutOut::Decide(d) => PrimaryMessage::SimpleItDecide(d),
                        CutOut::Timeout(t) => PrimaryMessage::SimpleItTimeout(t),
                        CutOut::TimeoutAccept(a) => PrimaryMessage::SimpleItTimeoutAccept(a),
                        CutOut::CutReady(r) => PrimaryMessage::SimpleItCutReady(r),
                    };
                    self.wire.broadcast_message(message).await;
                }
                CutEffect::ArmTimer { round, deadline } => {
                    let deadline = tokio::time::Instant::from_std(deadline);
                    let fut: Pin<Box<dyn Future<Output = CutRound> + Send>> =
                        Box::pin(async move {
                            tokio::time::sleep_until(deadline).await;
                            round
                        });
                    self.pending_timers.push(fut);
                }
                CutEffect::Commit { round, proposals } => {
                    // Preserve round order while data is repaired.
                    self.commit_queue.insert(round, proposals);
                    self.drain_commit_queue().await;
                }
                CutEffect::FetchTo {
                    peer,
                    round,
                    cut_id,
                } => {
                    self.wire
                        .send_message(
                            peer,
                            PrimaryMessage::SimpleItCutFetch(round, cut_id, self.name),
                        )
                        .await;
                }
                CutEffect::ServeTo { peer, proposal } => {
                    self.wire
                        .send_message(peer, PrimaryMessage::SimpleItCutServe(proposal))
                        .await;
                }
            }
        }
    }

    async fn drain_commit_queue(&mut self) {
        while let Some(&round) = self.commit_queue.keys().next() {
            // Release the map borrow before materialization.
            let proposals = self.commit_queue[&round].clone();
            let Some((by_worker, headers)) = self.try_expand_commit(&proposals) else {
                break;
            };
            self.commit_queue.remove(&round);
            let commit_millis = now_millis();
            self.payload
                .notify_committed(&mut self.wire, commit_millis, by_worker, headers)
                .await;
        }
        if let Some(metrics) = &self.metrics {
            metrics
                .simpleit_commit_queue_len
                .set(self.commit_queue.len() as i64);
        }
    }

    fn try_expand_commit(&mut self, proposals: &Cut) -> Option<CommitBatch> {
        let blocks = self.lm.blocks_handle();
        let blocks = blocks.lock();

        // Resolve every suffix before updating any watermark.
        let mut resolved: Vec<(PublicKey, Height, Digest, Vec<Digest>)> =
            Vec::with_capacity(proposals.len());
        for (author, proposal) in proposals {
            let (stop_height, stop_digest) = self
                .committed_watermark
                .get(author)
                .cloned()
                .unwrap_or_else(|| (0, self.genesis.clone()));
            if proposal.height <= stop_height {
                continue;
            }
            // A missing suffix leaves this and later rounds queued.
            let suffix = blocks.collect_verified_suffix(
                &self.committee,
                &self.sid,
                self.max_block_payload,
                stop_height,
                &stop_digest,
                &proposal.header_digest,
            )?;
            resolved.push((
                *author,
                proposal.height,
                proposal.header_digest.clone(),
                suffix,
            ));
        }

        let mut by_worker: HashMap<WorkerId, Vec<Digest>> = HashMap::new();
        let mut headers = Vec::new();
        for (author, height, tip_digest, suffix) in resolved {
            for digest in &suffix {
                if let Some(entry) = blocks.get(digest) {
                    for (batch_digest, worker_id) in &entry.block.payload {
                        by_worker
                            .entry(*worker_id)
                            .or_default()
                            .push(batch_digest.clone());
                    }
                    headers.push(entry.block.clone());
                }
            }
            // Pass one verification for the exact tip.
            self.committed_watermark
                .insert(author, (height, tip_digest));
        }
        Some((by_worker.into_iter().collect(), headers))
    }

    /// Prunes cut-engine state below the configured round window.
    fn collect_internal_garbage(&mut self) {
        let floor = self.cut.cut_round().saturating_sub(self.gc_window);
        if floor <= self.last_gc_floor {
            return;
        }
        self.cut.prune_below(floor);
        self.last_gc_floor = floor;
    }

    /// Seals accumulated worker digests into a local header.
    async fn seal_own_header(&mut self) {
        let payload = self.digests.drain(..).collect();
        self.payload_size = 0;
        let (_, effects) = self.lm.publish_own(payload).await;
        self.execute(effects).await;
    }

    /// Payload-sync bookkeeping, including lane-resume continuation.
    async fn on_payload_ready(
        &mut self,
        header_digest: Digest,
        digest: Digest,
        worker_id: WorkerId,
    ) {
        let mut resolved = false;
        if let Some(set) = self.payload.pending_payload.get_mut(&header_digest) {
            set.remove(&(digest, worker_id));
            resolved = set.is_empty();
        }
        self.payload.publish_sizes();
        if resolved {
            self.payload.pending_payload.remove(&header_digest);
            let author = self.lm.author_of(&header_digest);
            let before = author.map(|a| self.lm.own_direct_frontier(&a));
            let effects = self.lm.set_payload_ready(&header_digest);
            self.execute(effects).await;
            // Retry commits after completing payload synchronization.
            self.drain_commit_queue().await;
            if let (Some(author), Some(before)) = (author, before) {
                if self.lm.own_direct_frontier(&author) > before {
                    self.try_resume_request(author, Instant::now());
                }
            }
        }
    }

    fn try_resume_request(&mut self, author: PublicKey, now: Instant) {
        let frontier = self.lm.own_direct_frontier(&author);
        let avail = self.lm.avail_high(&author);
        let backoff = Duration::from_millis(self.resume_backoff_ms);
        if let Some(from) =
            self.resume_trigger
                .check(author, frontier, avail, now, backoff, self.resume_batch)
        {
            if let Some(metrics) = &self.metrics {
                metrics.vantage_lane_resume_requests_sent.inc();
            }
            self.wire.enqueue_resume(
                author,
                PrimaryMessage::VantageLaneResume(author, from, self.name),
            );
        }
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    // Commit rounds materialize atomically in ascending order.
    use super::*;
    use crypto::Hash as _;

    fn key(byte: u8) -> PublicKey {
        PublicKey([byte; 32])
    }

    fn committee_of(n: u8) -> Committee {
        let keys: Vec<PublicKey> = (1..=n).map(key).collect();
        let info = keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                (
                    *k,
                    1u32,
                    format!("127.0.0.1:{}", 9800 + i as u16).parse().unwrap(),
                )
            })
            .collect();
        Committee::new(info)
    }

    fn test_core(
        name: PublicKey,
        committee: Committee,
        path_suffix: &str,
    ) -> (SimpleItCore, Receiver<Header>) {
        let path = format!(".db_test_simpleit_commit_queue_{}", path_suffix);
        let _ = std::fs::remove_dir_all(&path);
        let store = Store::new(&path).expect("store opens");
        let registry = prometheus::Registry::new();
        let (metrics, _reporter) = Metrics::new(&registry);
        let (tx_output, rx_output) = channel(16);
        let (core, _rx_simpleit, _rx_payload_ready, _tx_simpleit, _ack_aggregator) =
            SimpleItCore::build(
                name,
                committee,
                Parameters::default(),
                store,
                Some(metrics),
                tx_output,
            );
        (core, rx_output)
    }

    async fn mark_payload(store: &mut Store, digest: &Digest, worker_id: WorkerId) {
        let key = [digest.as_ref(), &worker_id.to_le_bytes()].concat();
        store.write(key, Vec::new()).await;
    }

    fn served_payload_header(core: &SimpleItCore, author: PublicKey) -> (Header, Digest, Digest) {
        let missing = Digest([0x31; 32]);
        let present = Digest([0x32; 32]);
        let mut payload = BTreeMap::new();
        payload.insert(missing.clone(), 0);
        payload.insert(present.clone(), 0);
        (
            Header::new_vantage(author, 1, payload, core.genesis.clone(), core.sid.clone()),
            missing,
            present,
        )
    }

    fn sync_effect(effects: &[Effect]) -> Option<&Effect> {
        effects
            .iter()
            .find(|effect| matches!(effect, Effect::SyncBatches(..)))
    }

    #[tokio::test]
    async fn accepted_served_header_syncs_only_missing_payloads() {
        let committee = crate::common::committee();
        let keys: Vec<PublicKey> = committee.authorities.keys().cloned().collect();
        let (mut core, _) = test_core(keys[0], committee, "serve_missing");
        let (header, missing, present) = served_payload_header(&core, keys[1]);
        let mut store = core.lm.store_for_test();
        mark_payload(&mut store, &present, 0).await;
        core.rep.authorize((keys[1], 1, header.id.clone()));

        let effects = core.serve_effects(header.clone()).await;
        assert!(matches!(
            sync_effect(&effects),
            Some(Effect::SyncBatches(a, h, entries))
                if *a == keys[1] && h == &header.id && entries == &vec![(missing, 0)]
        ));
        assert!(
            !core
                .lm
                .blocks_handle()
                .lock()
                .get(&header.id)
                .unwrap()
                .payload_ok
        );
    }

    #[tokio::test]
    async fn duplicate_accepted_serve_merges_pending_payload_without_duplicate_waiters() {
        let committee = crate::common::committee();
        let keys: Vec<PublicKey> = committee.authorities.keys().cloned().collect();
        let (mut core, _) = test_core(keys[0], committee, "serve_duplicate");
        let (header, missing, first_arrival) = served_payload_header(&core, keys[1]);
        let mut store = core.lm.store_for_test();
        let (payload_tx, mut payload_rx) = channel(4);
        core.payload.tx_payload_ready = payload_tx;
        core.wire.worker_addresses.clear();
        core.rep.authorize((keys[1], 1, header.id.clone()));

        let effects = core.serve_effects(header.clone()).await;
        core.execute(effects).await;

        mark_payload(&mut store, &first_arrival, 0).await;
        let arrived = tokio::time::timeout(Duration::from_secs(1), payload_rx.recv())
            .await
            .expect("first payload waiter resolves")
            .expect("payload-ready channel stays open");
        assert_eq!(arrived, (header.id.clone(), first_arrival.clone(), 0));

        // Repeat the serve while one payload remains missing.
        let effects = core.serve_effects(header.clone()).await;
        assert!(matches!(
            sync_effect(&effects),
            Some(Effect::SyncBatches(a, h, entries))
                if *a == keys[1]
                    && h == &header.id
                    && entries == &vec![(missing.clone(), 0)]
        ));
        core.execute(effects).await;

        {
            let pending = core
                .payload
                .pending_payload
                .get(&header.id)
                .expect("header remains pending");
            assert_eq!(pending.len(), 2);
            assert!(pending.contains(&(first_arrival, 0)));
            assert!(pending.contains(&(missing.clone(), 0)));
        }

        mark_payload(&mut store, &missing, 0).await;
        let arrived = tokio::time::timeout(Duration::from_secs(1), payload_rx.recv())
            .await
            .expect("last payload waiter resolves")
            .expect("payload-ready channel stays open");
        assert_eq!(arrived, (header.id, missing, 0));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), payload_rx.recv())
                .await
                .is_err(),
            "a duplicate serve must not spawn a second waiter for the same payload"
        );
    }

    #[tokio::test]
    async fn rejected_served_header_emits_no_payload_sync() {
        let committee = crate::common::committee();
        let keys: Vec<PublicKey> = committee.authorities.keys().cloned().collect();
        let (mut core, _) = test_core(keys[0], committee, "serve_rejected");
        let mut header = Header::new_vantage(
            keys[1],
            1,
            BTreeMap::new(),
            core.genesis.clone(),
            core.sid.clone(),
        );
        header.id = Digest([0xee; 32]);
        core.rep.authorize((keys[1], 1, header.id.clone()));

        let effects = core.serve_effects(header).await;
        assert!(effects.is_empty());
    }

    #[tokio::test]
    async fn fully_present_served_header_emits_no_payload_sync() {
        let committee = crate::common::committee();
        let keys: Vec<PublicKey> = committee.authorities.keys().cloned().collect();
        let (mut core, _) = test_core(keys[0], committee, "serve_present");
        let (mut header, _missing, present) = served_payload_header(&core, keys[1]);
        header.payload.retain(|digest, _| digest == &present);
        header.id = header.digest();
        let mut store = core.lm.store_for_test();
        mark_payload(&mut store, &present, 0).await;
        core.rep.authorize((keys[1], 1, header.id.clone()));

        let effects = core.serve_effects(header.clone()).await;
        assert!(sync_effect(&effects).is_none());
        assert!(
            !core
                .lm
                .blocks_handle()
                .lock()
                .get(&header.id)
                .unwrap()
                .payload_ok
        );
    }

    /// Builds an uninserted chain of `n` headers.
    fn chain(author: PublicKey, n: Height, sid: &Digest, genesis: &Digest) -> Vec<Header> {
        let mut headers = Vec::new();
        let mut prev = genesis.clone();
        for height in 1..=n {
            let header = Header::new_vantage(author, height, BTreeMap::new(), prev, sid.clone());
            prev = header.id.clone();
            headers.push(header);
        }
        headers
    }

    /// Inserts a fully verified test header.
    fn insert(core: &SimpleItCore, header: &Header) {
        core.lm
            .blocks_handle()
            .lock()
            .upsert(header.clone(), true, true, true, true);
    }

    fn cut(entries: &[(PublicKey, &Header)]) -> Cut {
        entries
            .iter()
            .map(|(author, header)| {
                (
                    *author,
                    Proposal {
                        header_digest: header.id.clone(),
                        height: header.height,
                    },
                )
            })
            .collect()
    }

    /// A round whose author's suffix is unavailable emits nothing and stays queued.
    #[tokio::test]
    async fn commit_queue_blocks_on_unavailable_author_suffix_and_stays_queued() {
        let committee = committee_of(4);
        let keys: Vec<PublicKey> = committee.authorities.keys().cloned().collect();
        let (mut core, mut rx_output) = test_core(keys[0], committee, "unavailable");

        let author = keys[1];
        let chain_a = chain(author, 1, &core.sid.clone(), &core.genesis.clone());
        core.commit_queue.insert(1, cut(&[(author, &chain_a[0])]));
        core.drain_commit_queue().await;

        assert_eq!(core.commit_queue.len(), 1, "the round must stay queued");
        assert!(
            core.committed_watermark.is_empty(),
            "nothing should have advanced"
        );
        assert!(
            rx_output.try_recv().is_err(),
            "nothing should have been emitted"
        );
        assert_eq!(
            core.metrics
                .as_ref()
                .unwrap()
                .simpleit_commit_queue_len
                .get(),
            1,
            "the queue-depth gauge must reflect the stuck round"
        );
    }

    /// Unblocking the first round drains later ready rounds in order.
    #[tokio::test]
    async fn commit_queue_drains_in_round_order_once_unblocked_batching_authors_together() {
        let committee = committee_of(4);
        let keys: Vec<PublicKey> = committee.authorities.keys().cloned().collect();
        let (mut core, mut rx_output) = test_core(keys[0], committee, "drains_in_order");

        let sid = core.sid.clone();
        let genesis = core.genesis.clone();
        let author_a = keys[1];
        let author_b = keys[2];
        let chain_a = chain(author_a, 2, &sid, &genesis);
        let chain_b = chain(author_b, 1, &sid, &genesis);

        core.commit_queue
            .insert(1, cut(&[(author_a, &chain_a[0]), (author_b, &chain_b[0])]));
        core.commit_queue
            .insert(2, cut(&[(author_a, &chain_a[1]), (author_b, &chain_b[0])]));

        // Both rounds remain queued until their blocks arrive.
        core.drain_commit_queue().await;
        assert_eq!(core.commit_queue.len(), 2);
        assert!(rx_output.try_recv().is_err());

        insert(&core, &chain_a[0]);
        insert(&core, &chain_a[1]);
        insert(&core, &chain_b[0]);
        core.drain_commit_queue().await;

        assert!(
            core.commit_queue.is_empty(),
            "both rounds should have drained"
        );
        assert_eq!(
            core.committed_watermark.get(&author_a),
            Some(&(2, chain_a[1].id.clone()))
        );
        assert_eq!(
            core.committed_watermark.get(&author_b),
            Some(&(1, chain_b[0].id.clone()))
        );

        // Round 1 emits both authors together.
        let mut round1 = vec![
            rx_output.try_recv().expect("round 1's first header").id,
            rx_output.try_recv().expect("round 1's second header").id,
        ];
        round1.sort();
        let mut expected1 = vec![chain_a[0].id.clone(), chain_b[0].id.clone()];
        expected1.sort();
        assert_eq!(
            round1, expected1,
            "round 1's two authors must be emitted together"
        );

        // Round 2: only A@2 (B had nothing new that round).
        let round2 = rx_output.try_recv().expect("round 2's header");
        assert_eq!(round2.id, chain_a[1].id);

        assert!(
            rx_output.try_recv().is_err(),
            "nothing else should have been emitted"
        );
    }

    /// Payload arrival order does not change commit output order.
    #[tokio::test]
    async fn commit_queue_emits_same_sequence_regardless_of_payload_arrival_order() {
        let committee = committee_of(4);
        let keys: Vec<PublicKey> = committee.authorities.keys().cloned().collect();

        let (mut node_x, mut rx_x) = test_core(keys[0], committee.clone(), "arrival_order_x");
        let (mut node_y, mut rx_y) = test_core(keys[0], committee, "arrival_order_y");

        let sid = node_x.sid.clone();
        let genesis = node_x.genesis.clone();
        let author_a = keys[1];
        let author_b = keys[2];
        let chain_a = chain(author_a, 2, &sid, &genesis);
        let chain_b = chain(author_b, 2, &sid, &genesis);

        let cut1 = cut(&[(author_a, &chain_a[0])]);
        let cut2 = cut(&[(author_a, &chain_a[1]), (author_b, &chain_b[0])]);
        let cut3 = cut(&[(author_b, &chain_b[1])]);

        for (round, c) in [(1, cut1.clone()), (2, cut2.clone()), (3, cut3.clone())] {
            node_x.commit_queue.insert(round, c.clone());
            node_y.commit_queue.insert(round, c);
        }

        // Node X receives blocks in round order.
        node_x.drain_commit_queue().await;
        insert(&node_x, &chain_a[0]);
        node_x.drain_commit_queue().await;
        insert(&node_x, &chain_a[1]);
        insert(&node_x, &chain_b[0]);
        node_x.drain_commit_queue().await;
        insert(&node_x, &chain_b[1]);
        node_x.drain_commit_queue().await;

        node_y.drain_commit_queue().await;
        insert(&node_y, &chain_b[1]);
        insert(&node_y, &chain_b[0]);
        node_y.drain_commit_queue().await;
        // Node Y cannot skip unresolved round 1.
        assert!(
            rx_y.try_recv().is_err(),
            "round 1 blocking must suppress round 3 too, even though B is fully available"
        );
        assert_eq!(
            node_y.commit_queue.len(),
            3,
            "all three rounds must still be queued"
        );

        insert(&node_y, &chain_a[0]);
        insert(&node_y, &chain_a[1]);
        node_y.drain_commit_queue().await;

        assert!(node_x.commit_queue.is_empty());
        assert!(node_y.commit_queue.is_empty());

        let seq_x: Vec<Digest> =
            std::iter::from_fn(|| rx_x.try_recv().ok().map(|h| h.id)).collect();
        let seq_y: Vec<Digest> =
            std::iter::from_fn(|| rx_y.try_recv().ok().map(|h| h.id)).collect();
        let expected = vec![
            chain_a[0].id.clone(),
            chain_a[1].id.clone(),
            chain_b[0].id.clone(),
            chain_b[1].id.clone(),
        ];
        assert_eq!(seq_x, expected, "node X's emitted sequence");
        assert_eq!(
            seq_x, seq_y,
            "emitted sequence must be identical regardless of payload arrival order"
        );
    }
}

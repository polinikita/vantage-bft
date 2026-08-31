// Copyright(C) Facebook, Inc. and its affiliates.
use crate::aggregators::{QCMaker, TCMaker, VotesAggregator};
use crate::delayed_header::DelayedHeaderSender;
use crate::error::{DagError, DagResult};
use crate::leader::LeaderElector;
use crate::messages::{
    transform_commit_qc, Certificate, CommitQC, ConsensusMessage, ConsensusRequest, ConsensusVote,
    Header, Proposal, ProposalKind, Timeout, Vote, QC, TC,
};
use crate::primary::{Height, PrimaryMessage, Slot, View};
use crate::synchronizer::Synchronizer;
use crate::timer::{CarTimer, FastTimer, Timer};
use crate::verified::VerifiedCache;
use async_recursion::async_recursion;
use bytes::Bytes;
use config::Committee;
use core::panic;
use crypto::consensus_auth::ConsensusSignature;
use crypto::{Digest, PublicKey, SignatureService};
use crypto::Hash as _;
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt};
use log::{debug, error, warn};
use metrics::Metrics;
use network::{BatchConfig, CancelHandler, ChannelAuth, ReliableSender};
use std::cmp::max;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

/// Timer future shared by normal and injected-asynchrony paths.
type SlotViewTimerFuture = Pin<Box<dyn Future<Output = (Slot, View)> + Send>>;

pub(crate) fn keep_after_slot_period_gc(candidate: Slot, committed: Slot, k: Slot) -> bool {
    debug_assert!(k > 0);
    candidate > committed || candidate % k != committed % k
}

fn keep_all_to_all_delivery(candidate: Slot, committed: Slot, gc_depth: Slot) -> bool {
    candidate >= committed.saturating_sub(gc_depth)
}

fn latest_pipeline_tickets<I>(slots: I, k: Slot) -> HashSet<Slot>
where
    I: IntoIterator<Item = Slot>,
{
    debug_assert!(k > 0);
    let mut latest = HashMap::<Slot, Slot>::new();
    for slot in slots {
        latest
            .entry(slot % k)
            .and_modify(|current| *current = (*current).max(slot))
            .or_insert(slot);
    }
    latest.into_values().collect()
}

/// A valid Prepare(s, _) is the timer ticket for s+1 in parallel mode.
fn parallel_timer_slot(prepare_slot: Slot, k: Slot) -> Option<Slot> {
    (k > 1).then(|| prepare_slot.checked_add(1)).flatten()
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ParentVoteState {
    Exact,
    Conflicting,
    Missing,
}

fn parent_vote_state(
    votes: &HashMap<Height, HashMap<PublicKey, Digest>>,
    parent: &Header,
) -> ParentVoteState {
    match votes
        .get(&parent.height)
        .and_then(|lanes| lanes.get(&parent.author))
    {
        Some(digest) if *digest == parent.id => ParentVoteState::Exact,
        Some(_) => ParentVoteState::Conflicting,
        None => ParentVoteState::Missing,
    }
}

fn record_car_vote(
    votes: &mut HashMap<Height, HashMap<PublicKey, Digest>>,
    header: &Header,
) -> bool {
    match votes.entry(header.height).or_default().entry(header.author) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(header.id.clone());
            true
        }
        std::collections::hash_map::Entry::Occupied(_) => false,
    }
}

fn autobahn_own_tip_is_admissible(
    committee: &Committee,
    lane: &PublicKey,
    allow_optimistic_tip: bool,
    proposal: &Proposal,
) -> bool {
    match proposal.verify(lane, committee) {
        Ok(ProposalKind::Genesis | ProposalKind::Certified) => true,
        Ok(ProposalKind::Optimistic) => allow_optimistic_tip,
        Err(_) => false,
    }
}

fn allow_optimistic_leader_cut(
    use_optimistic_tips: bool,
    certified_only_leader: bool,
    has_tc: bool,
) -> bool {
    use_optimistic_tips && !certified_only_leader && !has_tc
}

pub struct Core {
    name: PublicKey,
    committee: Committee,
    store: Store,
    synchronizer: Synchronizer,
    /// Shared memo of objects already verified against the committee.
    verified: VerifiedCache,
    signature_service: SignatureService,
    consensus_round: Arc<AtomicU64>,
    gc_depth: Height,

    rx_primaries: Receiver<PrimaryMessage>,
    rx_header_waiter: Receiver<Header>,
    rx_header_waiter_instances: Receiver<(ConsensusMessage, Header)>,
    rx_proposer: Receiver<Header>,
    tx_committer: Sender<ConsensusMessage>,
    tx_proposer: Sender<Certificate>,
    rx_request_header_sync: Receiver<Digest>,

    gc_round: Height,
    /// Authors voted for in each height.
    /// Exact car digest voted at each `(lane, height)` coordinate.
    last_voted: HashMap<Height, HashMap<PublicKey, Digest>>,
    current_header: Header,
    sent_cert_to_proposer: bool,
    votes_aggregator: VotesAggregator,

    network: ReliableSender,
    /// Header recipients blocked by fault injection.
    withheld_header_dests: Option<HashSet<PublicKey>>,
    /// Receiver subset and sender for finite-delay original publication.
    late_header: Option<(HashSet<PublicKey>, DelayedHeaderSender)>,
    /// Optional active window for withholding.
    withhold_window: Option<Arc<OnceLock<(Instant, Instant)>>>,
    metrics: Arc<Metrics>,
    cancel_handlers: HashMap<Height, Vec<CancelHandler>>,
    consensus_cancel_handlers: HashMap<Slot, Vec<CancelHandler>>,

    current_proposal_tips: HashMap<PublicKey, Proposal>,
    current_certified_tips: HashMap<PublicKey, Proposal>,

    consensus_instances: HashMap<(Slot, Digest), ConsensusMessage>,
    views: HashMap<Slot, View>,
    timers: HashSet<(Slot, View)>,
    last_voted_consensus: HashSet<(Slot, View)>,
    last_confirmed_consensus: HashSet<(Slot, View)>,
    timer_futures: FuturesUnordered<SlotViewTimerFuture>,
    high_proposals: HashMap<Slot, ConsensusMessage>,
    /// Latest quorum certificate per slot.
    high_qcs: HashMap<Slot, ConsensusMessage>,
    qc_makers: HashMap<(Slot, Digest), QCMaker>,
    current_qcs_formed: usize,
    tc_makers: HashMap<(Slot, View), TCMaker>,
    /// Views for which this replica already broadcast its own Timeout.
    sent_timeouts: HashSet<(Slot, View)>,
    prepare_tickets: VecDeque<ConsensusMessage>,
    already_proposed_slots: HashSet<Slot>,
    /// Consensus views for which this leader has already emitted a Prepare.
    proposed_consensus_views: HashSet<(Slot, View)>,
    tx_info: Sender<ConsensusMessage>,
    leader_elector: LeaderElector,
    timeout_delay: u64,
    committed_slots: HashMap<Slot, CommitQC>,
    last_committed_slot: u64,

    use_fast_path: bool,
    use_optimistic_tips: bool,
    use_parallel_proposals: bool,
    /// Maximum number of open honest instances.
    k: u64,
    fast_path_timeout: u64,

    use_ride_share: bool,
    car_timeout: u64,
    car_timer_futures: FuturesUnordered<Pin<Box<dyn Future<Output = Vote> + Send>>>,
    fast_timer_futures: FuturesUnordered<Pin<Box<dyn Future<Output = ConsensusVote> + Send>>>,

    /// Whether replicas broadcast votes and assemble certificates locally.
    all_to_all: bool,
    /// Benchmark adversary: a Byzantine lane publisher avoids self-stalling
    /// its own consensus-leader views by proposing only its certified cut.
    certified_only_leader: bool,
    /// Early all-to-all votes, bounded by slot, digest, and committee author.
    /// Entries are drained when the matching consensus instance is registered.
    pending_consensus_votes: HashMap<Slot, HashMap<Digest, HashMap<PublicKey, ConsensusVote>>>,

    /// Asynchrony fault-injection settings.
    simulate_asynchrony: bool,
    asynchrony_start: u64,
    asynchrony_duration: u64,
    during_simulated_asynchrony: bool,
    async_timer_futures: FuturesUnordered<SlotViewTimerFuture>,
    current_time: Instant,
    async_delayed_prepare: Option<ConsensusMessage>,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        synchronizer: Synchronizer,
        signature_service: SignatureService,
        consensus_round: Arc<AtomicU64>,
        gc_depth: Height,
        rx_primaries: Receiver<PrimaryMessage>,
        rx_header_waiter: Receiver<Header>,
        rx_header_waiter_instances: Receiver<(ConsensusMessage, Header)>,
        rx_proposer: Receiver<Header>,
        tx_committer: Sender<ConsensusMessage>,
        tx_proposer: Sender<Certificate>,
        rx_request_header_sync: Receiver<Digest>,
        tx_info: Sender<ConsensusMessage>,
        leader_elector: LeaderElector,
        timeout_delay: u64,
        use_optimistic_tips: bool,
        use_parallel_proposals: bool,
        k: u64,
        use_fast_path: bool,
        fast_path_timeout: u64,
        use_ride_share: bool,
        car_timeout: u64,
        all_to_all: bool,
        certified_only_leader: bool,

        simulate_asynchrony: bool,
        asynchrony_start: u64,
        asynchrony_duration: u64,

        latency_map: HashMap<SocketAddr, Duration>,
        withheld_header_dests: Option<HashSet<PublicKey>>,
        late_header_dests: Option<HashSet<PublicKey>>,
        late_header_delay_ms: u64,
        withhold_window: Option<Arc<OnceLock<(Instant, Instant)>>>,
        metrics: Arc<Metrics>,
        batch: BatchConfig,
        auth: Option<Arc<ChannelAuth>>,
        retry_backoff_max_ms: u64,
    ) {
        assert!(k > 0, "Autobahn parallel window k must be positive");
        tokio::spawn(async move {
            let late_header = late_header_dests.map(|late| {
                let addresses = committee
                    .others_primaries(&name)
                    .iter()
                    .filter(|(peer, _)| late.contains(peer))
                    .map(|(_, value)| value.primary_to_primary)
                    .collect();
                let sender = DelayedHeaderSender::new(
                    addresses,
                    &latency_map,
                    late_header_delay_ms,
                    batch,
                    auth.clone(),
                    retry_backoff_max_ms,
                    Some(metrics.clone()),
                )
                .expect("validated late-header configuration has receivers");
                (late, sender)
            });
            Self {
                name,
                committee,
                store,
                verified: synchronizer.verified(),
                synchronizer,
                signature_service,
                consensus_round,
                gc_depth,
                rx_primaries,
                rx_header_waiter,
                rx_header_waiter_instances,
                rx_proposer,
                tx_committer,
                tx_proposer,
                rx_request_header_sync,
                tx_info,
                leader_elector,
                gc_round: 0,
                current_qcs_formed: 0,
                sent_cert_to_proposer: false,
                last_voted: HashMap::with_capacity(2 * gc_depth as usize),
                current_header: Header::default(),
                votes_aggregator: VotesAggregator::new(),
                metrics: metrics.clone(),
                network: ReliableSender::new()
                    .with_queue_role("core")
                    .with_latency(latency_map)
                    .with_metrics(metrics)
                    .with_batching(batch)
                    .with_channel_auth(auth)
                    .with_retry_backoff_max_ms(retry_backoff_max_ms),
                withheld_header_dests,
                late_header,
                withhold_window,
                cancel_handlers: HashMap::with_capacity(2 * gc_depth as usize),
                consensus_cancel_handlers: HashMap::with_capacity(2 * gc_depth as usize),
                already_proposed_slots: HashSet::new(),
                current_proposal_tips: HashMap::with_capacity(2 * gc_depth as usize),
                current_certified_tips: HashMap::with_capacity(2 * gc_depth as usize),
                consensus_instances: HashMap::with_capacity(2 * gc_depth as usize),
                views: HashMap::with_capacity(2 * gc_depth as usize),
                timers: HashSet::with_capacity(2 * gc_depth as usize),
                last_voted_consensus: HashSet::with_capacity(2 * gc_depth as usize),
                last_confirmed_consensus: HashSet::with_capacity(2 * gc_depth as usize),
                high_qcs: HashMap::with_capacity(2 * gc_depth as usize),
                high_proposals: HashMap::with_capacity(2 * gc_depth as usize),
                qc_makers: HashMap::with_capacity(2 * gc_depth as usize),
                tc_makers: HashMap::with_capacity(2 * gc_depth as usize),
                sent_timeouts: HashSet::with_capacity(2 * gc_depth as usize),
                prepare_tickets: VecDeque::with_capacity(2 * gc_depth as usize),
                proposed_consensus_views: HashSet::with_capacity(2 * gc_depth as usize),
                timeout_delay,
                timer_futures: FuturesUnordered::new(),
                committed_slots: HashMap::with_capacity(2 * gc_depth as usize),
                last_committed_slot: 0,

                use_fast_path,
                use_optimistic_tips,
                use_parallel_proposals,
                k,
                fast_path_timeout,
                use_ride_share,
                car_timeout,
                car_timer_futures: FuturesUnordered::new(),
                fast_timer_futures: FuturesUnordered::new(),
                all_to_all,
                certified_only_leader,
                pending_consensus_votes: HashMap::with_capacity(2 * gc_depth as usize),

                simulate_asynchrony,
                asynchrony_start,
                asynchrony_duration,
                during_simulated_asynchrony: false,
                async_timer_futures: FuturesUnordered::new(),
                current_time: Instant::now(),
                async_delayed_prepare: None,
            }
            .run()
            .await;
        });
    }

    async fn process_own_header(&mut self, mut header: Header) -> DagResult<()> {
        debug!(
            "Processing own header with {:?} consensus messages",
            header.consensus_messages.len()
        );

        // A carrier cannot name itself without making its signed digest
        // self-referential. Its predecessor has just become certified and is
        // the freshest valid own-lane coordinate for a ride-shared Prepare.
        let ride_share_own_tip = Proposal::certified(header.parent_cert.clone());
        let mut bound_messages = HashMap::new();
        for mut consensus in std::mem::take(&mut header.consensus_messages).into_values() {
            self.set_consensus_proposal_with_own(&mut consensus, ride_share_own_tip.clone());
            bound_messages.insert(consensus.digest(), consensus);
        }
        header.consensus_messages = bound_messages;

        // `set_consensus_proposal` fills the cut after the proposer constructs
        // the car. Recompute and re-sign so ride-shared values cannot be added,
        // removed, or altered without invalidating the car signature.
        if !header.consensus_messages.is_empty() {
            header.id = header.digest();
            header.signature = Some(
                self.signature_service
                    .request_consensus_signature(header.id.clone())
                    .await,
            );
        }

        self.current_certified_tips.insert(
            header.origin(),
            Proposal::certified(header.parent_cert.clone()),
        );
        if self.use_optimistic_tips {
            self.current_proposal_tips
                .insert(header.origin(), Proposal::optimistic(&header));
        }

        self.current_header = header.clone();
        self.sent_cert_to_proposer = false;
        self.votes_aggregator = VotesAggregator::new();

        for consensus in header.consensus_messages.values() {
            let dig = consensus.digest();
            match consensus {
                ConsensusMessage::Prepare {
                    slot,
                    view: _,
                    tc: _,
                    qc_ticket: _,
                    proposals: _,
                } => {
                    self.consensus_instances
                        .insert((*slot, dig), consensus.clone());
                }
                ConsensusMessage::Confirm {
                    slot,
                    view: _,
                    qc: _,
                    proposals: _,
                } => {
                    self.consensus_instances
                        .insert((*slot, dig), consensus.clone());
                }
                _ => {}
            };
        }

        // Exclude fault-injection targets only while withholding is active.
        let withhold_active =
            config::withhold_active(self.withhold_window.as_deref(), Instant::now());
        let addresses = self
            .committee
            .others_primaries(&self.name)
            .iter()
            .filter(|(pk, _)| {
                let withheld = self
                    .withheld_header_dests
                    .as_ref()
                    .is_some_and(|blocked| withhold_active && blocked.contains(pk));
                let late = self
                    .late_header
                    .as_ref()
                    .is_some_and(|(blocked, _)| blocked.contains(pk));
                !withheld && !late
            })
            .map(|(_, x)| x.primary_to_primary)
            .collect();
        let bytes = bincode::serialize(&PrimaryMessage::Header(header.clone(), false))
            .expect("Failed to serialize our own header");
        let payload = Bytes::from(bytes);
        let mut handlers = self
            .network
            .broadcast_typed(addresses, payload.clone(), "Header")
            .await;
        if let Some((_, sender)) = &mut self.late_header {
            handlers.extend(sender.broadcast(payload).await);
        }
        self.cancel_handlers
            .entry(header.height)
            .or_default()
            .extend(handlers);

        // Measure the header without its wire envelope.
        let header_bytes = bincode::serialize(&header).expect("Failed to serialize header");
        self.metrics
            .proposed_header_size_bytes
            .observe(header_bytes.len());

        self.process_header(header, false).await
    }

    /// Ingests one repaired lane suffix as a unit, oldest header first, so
    /// each car finds its parent already processed and vote continuity holds
    /// without a loopback round-trip per header. One malformed or stale
    /// header does not discard the rest of the suffix, matching the previous
    /// one-message-per-header behavior.
    async fn process_header_suffix(&mut self, mut headers: Vec<Header>) -> DagResult<()> {
        headers.sort_by_key(|header| header.height);
        for header in headers {
            let outcome = match self.sanitize_header(&header) {
                Ok(()) => self.process_header(header, true).await,
                error => error,
            };
            if let Err(error) = outcome {
                debug!("Skipping one repaired suffix header: {}", error);
            }
        }
        Ok(())
    }

    #[async_recursion]
    async fn process_header(&mut self, header: Header, sync: bool) -> DagResult<()> {
        debug!("Processing Header:  {:?}", header);
        debug!("Processing the header with height {:?}", header.height);

        // A car already voted at this exact coordinate has been fully
        // processed and stored; duplicate deliveries (rebroadcasts, repeated
        // suffix replies) end here before any store round-trip.
        if self
            .last_voted
            .get(&header.height)
            .and_then(|lane| lane.get(&header.author))
            .is_some_and(|voted| *voted == header.id)
        {
            return Ok(());
        }

        // Every car names its lane predecessor and carries an f+1 PoA for it.
        self.verified
            .check_certificate(&header.parent_cert, &self.committee)?;
        ensure!(
            header.parent_cert.author == header.author
                && header.parent_cert.height().checked_add(1) == Some(header.height()),
            DagError::MalformedHeader(header.id.clone())
        );

        // Retry the header after its payload arrives.
        if self.synchronizer.missing_payload(&header, sync).await? {
            debug!("Processing of {} suspended: missing payload", header);
            return Ok(());
        }

        // Possession of an optimistic tip is enough for Prepare. Store the
        // signed car as soon as its own payload is present; parent-chain repair
        // remains asynchronous until voting for the car or executing a Commit.
        let bytes = bincode::serialize(&header).expect("Failed to serialize header");
        self.store.write(header.digest().to_vec(), bytes).await;

        // Retry the car vote after its parent arrives.
        let Some(parent) = self.synchronizer.get_parent_header(&header).await? else {
            debug!("The parent is missing, suspending processing");
            return Ok(());
        };

        // A correct replica votes for lane position h only after it has
        // received and voted for position h-1, as required by the car protocol.
        if parent.height > 0 {
            match parent_vote_state(&self.last_voted, &parent) {
                ParentVoteState::Exact => {}
                ParentVoteState::Conflicting => {
                    debug!("Refusing a child of a conflicting parent car");
                    return Ok(());
                }
                ParentVoteState::Missing => {
                    self.verified.check_header(&parent, &self.committee)?;
                    ensure!(
                        parent.author == header.author
                            && parent.height == header.parent_cert.height
                            && parent.id == header.parent_cert.header_digest,
                        DagError::MalformedHeader(header.id.clone())
                    );
                    // The child's verified PoA identifies f+1 holders of the
                    // parent. Use those proof sources for parent/suffix repair;
                    // a Byzantine lane author is not a liveness dependency.
                    self.synchronizer
                        .register_parent_poa_sources(&header.parent_cert)
                        .await;
                    self.process_header(parent.clone(), true).await?;
                    if parent_vote_state(&self.last_voted, &parent) != ParentVoteState::Exact {
                        debug!("The parent is present but not yet vote-eligible");
                        return Ok(());
                    }
                }
            }
        }

        // Update local tips and proposals for a higher header.
        if self.use_optimistic_tips
            && header.height()
                > self
                    .current_proposal_tips
                    .get(&header.origin())
                    .unwrap()
                    .height
        {
            self.current_proposal_tips
                .insert(header.origin(), Proposal::optimistic(&header));
            debug!("updating tip");

            // Recheck pending tickets after receiving a tip.
            self.try_prepare_waiting_slots().await?;
        }

        if header.parent_cert.height
            > self
                .current_certified_tips
                .get(&header.origin())
                .unwrap()
                .height
        {
            self.current_certified_tips.insert(
                header.origin(),
                Proposal::certified(header.parent_cert.clone()),
            );
            debug!("updating tip");

            // Recheck pending tickets after receiving a tip.
            self.try_prepare_waiting_slots().await?;
        }

        debug!("after tip height check");

        self.process_certificate(header.clone().parent_cert).await?;

        let should_vote = record_car_vote(&mut self.last_voted, &header);
        if should_vote {
            let consensus_votes = self.process_consensus_messages(&header).await?;

            debug!("Consensus sigs length {:?}", consensus_votes.len());

            let vote = Vote::new(
                &header,
                &self.name,
                &mut self.signature_service,
                consensus_votes,
            )
            .await;
            debug!("Created Vote {:?}", vote);

            if vote.origin == self.name {
                self.process_vote(vote, false)
                    .await
                    .expect("Failed to process our own vote");
            } else {
                let address = self
                    .committee
                    .primary(&header.author)
                    .expect("Author of valid header is not in the committee")
                    .primary_to_primary;
                let bytes = bincode::serialize(&PrimaryMessage::Vote(vote))
                    .expect("Failed to serialize our own vote");
                let handler = self
                    .network
                    .send_typed(address, Bytes::from(bytes), "Vote")
                    .await;
                self.cancel_handlers
                    .entry(header.height())
                    .or_default()
                    .push(handler);
            }
        }
        Ok(())
    }

    #[async_recursion]
    async fn process_vote(&mut self, vote: Vote, is_loopback: bool) -> DagResult<()> {
        debug!("Processing Vote {:?}", vote);

        let current_car_vote = vote.id == self.current_header.id
            && vote.height == self.current_header.height
            && vote.origin == self.current_header.author;
        let consensus_loopback = is_loopback && !vote.consensus_votes.is_empty();
        let car_timeout =
            is_loopback && vote.id == self.current_header.id && vote.consensus_votes.is_empty();
        let late_consensus_vote = !current_car_vote && !vote.consensus_votes.is_empty();

        // Process only current-header votes and consensus loopbacks.
        if !current_car_vote && !car_timeout && !consensus_loopback && !late_consensus_vote {
            return Ok(());
        }

        let num_active_consensus_messages = self.current_header.num_active_instances;
        debug!("num active instances {:?}", num_active_consensus_messages);

        let mut advanced_current_consensus = false;
        for (slot, digest, sig) in vote.consensus_votes.iter() {
            debug!("current header {:?}", self.current_header);
            debug!("digest is {:?}", digest);

            let opt_curr_instance = self.consensus_instances.get(&(*slot, digest.clone()));
            if opt_curr_instance.is_none() {
                debug!("consensus instance slot has committed, skip processing vote");
                continue;
            }
            let current_instance = opt_curr_instance.unwrap();

            if !is_loopback && vote.author != self.name {
                sig.verify(
                    &current_instance.digest(),
                    &vote.author,
                    self.committee.consensus_signature_scheme,
                    self.committee.consensus_public_key(&vote.author),
                )?;
            }

            let qc_maker = self
                .qc_makers
                .entry((*slot, digest.clone()))
                .or_insert(QCMaker::new());

            // Configure fast-path quorum collection.
            qc_maker.try_fast = match current_instance {
                ConsensusMessage::Prepare {
                    slot: _,
                    view: _,
                    tc: _,
                    qc_ticket: _,
                    proposals: _,
                } => self.use_fast_path,
                _ => false,
            };

            let (qc_ready, qc_opt) = match is_loopback {
                false => {
                    qc_maker.append(vote.author, (digest.clone(), sig.clone()), &self.committee)?
                }
                true => {
                    qc_maker.try_fast = false;
                    qc_maker.get_qc()?
                }
            };

            if qc_ready {
                if qc_opt.is_none() && self.use_fast_path {
                    // Recheck the slow quorum after the fast-path timeout.
                    let t_vote = Vote {
                        id: Digest::default(),
                        height: 0,
                        origin: PublicKey::default(),
                        author: PublicKey::default(),
                        signature: ConsensusSignature::default(),
                        consensus_votes: vec![(
                            *slot,
                            digest.clone(),
                            ConsensusSignature::default(),
                        )],
                    };
                    let fast_timer = CarTimer::new(t_vote, self.fast_path_timeout);
                    self.car_timer_futures.push(Box::pin(fast_timer));
                } else if let Some(qc) = qc_opt {
                    if self.current_header.consensus_messages.contains_key(digest) {
                        self.current_qcs_formed += 1;
                        advanced_current_consensus = true;
                    }

                    match current_instance {
                        ConsensusMessage::Prepare {
                            slot,
                            view,
                            tc: _,
                            qc_ticket: _,
                            proposals,
                        } => {
                            debug!("Prepare QC formed in slot {:?}", slot);
                            debug!(
                                "Prepare has slot: {}, view: {}, digest: {}",
                                slot,
                                view,
                                current_instance.digest()
                            );
                            let new_consensus_message = match qc_maker.try_fast {
                                true => {
                                    debug!("taking fast path!");
                                    ConsensusMessage::Commit {
                                        slot: *slot,
                                        view: *view,
                                        qc,
                                        proposals: proposals.clone(),
                                    }
                                }
                                false => ConsensusMessage::Confirm {
                                    slot: *slot,
                                    view: *view,
                                    qc,
                                    proposals: proposals.clone(),
                                },
                            };

                            self.tx_info
                                .send(new_consensus_message)
                                .await
                                .expect("Failed to send info");
                        }
                        ConsensusMessage::Confirm {
                            slot,
                            view,
                            qc: _,
                            proposals,
                        } => {
                            debug!("Commit QC formed in slot {:?}", slot);
                            let new_consensus_message = ConsensusMessage::Commit {
                                slot: *slot,
                                view: *view,
                                qc,
                                proposals: proposals.clone(),
                            };

                            self.tx_info
                                .send(new_consensus_message)
                                .await
                                .expect("Failed to send info");
                        }
                        ConsensusMessage::Commit {
                            slot: _,
                            view: _,
                            qc: _,
                            proposals: _,
                        } => {}
                    };
                }
            }
        }

        // Wait for every active consensus instance.
        let consensus_ready: bool = self.current_header.consensus_messages.is_empty()
            || self.current_qcs_formed == num_active_consensus_messages;

        // A fast-path timer or a late ambassador-car reply exists only to
        // advance embedded consensus. It must never be counted as a vote for
        // the current car. If the car PoA was already ready, release it as soon
        // as the last embedded QC becomes available.
        if consensus_loopback || late_consensus_vote {
            if advanced_current_consensus
                && consensus_ready
                && self.votes_aggregator.complete
                && !self.sent_cert_to_proposer
            {
                if let Some(certificate) = self.votes_aggregator.get()? {
                    self.tx_proposer
                        .send(certificate)
                        .await
                        .expect("Failed to send certificate");
                    self.sent_cert_to_proposer = true;
                    self.current_qcs_formed = 0;
                }
            }
            return Ok(());
        }

        let vote_id = vote.id.clone();

        // Add the vote to the header certificate.
        let (car_cert_ready, first) =
            self.votes_aggregator
                .append(vote, &self.committee, &self.current_header)?;

        // A timeout stops waiting for embedded consensus.
        let consensus_ready = consensus_ready || car_timeout;
        debug!(
            "sentToProposer {:?}, poa_ready {:?}, consensus_ready {:?}",
            self.sent_cert_to_proposer, car_cert_ready, consensus_ready
        );

        // Start one timeout while the dissemination certificate waits for consensus.
        if car_cert_ready && !consensus_ready && first {
            let t_vote = Vote {
                id: vote_id,
                height: 0,
                origin: PublicKey::default(),
                author: PublicKey::default(),
                signature: ConsensusSignature::default(),
                consensus_votes: vec![],
            };
            let fast_timer = CarTimer::new(t_vote, self.car_timeout);
            self.car_timer_futures.push(Box::pin(fast_timer));
        }

        if !self.sent_cert_to_proposer && car_cert_ready && consensus_ready {
            if let Some(certificate) = self.votes_aggregator.get()? {
                self.tx_proposer
                    .send(certificate)
                    .await
                    .expect("Failed to send certificate");
                self.sent_cert_to_proposer = true;
                self.current_qcs_formed = 0;
            }
        }

        Ok(())
    }

    // The recursive all-to-all call is boxed at its call site.
    async fn process_consensus_vote(
        &mut self,
        vote: ConsensusVote,
        is_loopback: bool,
    ) -> DagResult<()> {
        debug!("Receive consensus vote for dig {}", &vote.digest);

        let opt_curr_instance = self
            .consensus_instances
            .get(&(vote.slot, vote.digest.clone()));
        if opt_curr_instance.is_none() {
            // All-to-all votes may arrive before local instance registration.
            if self.all_to_all && !is_loopback {
                // Authenticate before buffering. In particular, a wire sender
                // cannot bypass verification by claiming our own public key.
                self.verified.check_consensus_vote(&vote, &self.committee)?;
                if self.is_sane_pending_vote_slot(vote.slot) {
                    self.buffer_pending_consensus_vote(vote);
                }
                return Ok(());
            }
            debug!("consensus instance slot has committed, skip processing vote");
            return Ok(());
        }

        if !is_loopback {
            self.verified.check_consensus_vote(&vote, &self.committee)?;
        }

        let current_instance = opt_curr_instance.unwrap();
        let qc_maker = self
            .qc_makers
            .entry((vote.slot, vote.digest.clone()))
            .or_insert(QCMaker::new());

        // Configure fast-path quorum collection.
        qc_maker.try_fast = match current_instance {
            ConsensusMessage::Prepare {
                slot: _,
                view: _,
                tc: _,
                qc_ticket: _,
                proposals: _,
            } => self.use_fast_path,
            _ => false,
        };

        // A missing QC means the fast-path timer must complete first.
        let (qc_ready, qc_opt) = match is_loopback {
            false => qc_maker.append(
                vote.author,
                (vote.digest.clone(), vote.sig.clone()),
                &self.committee,
            )?,
            true => {
                qc_maker.try_fast = false;
                qc_maker.get_qc()?
            }
        };

        debug!("qc maker weight {:?}", qc_maker.votes.len());

        if qc_ready {
            if qc_opt.is_none() && self.use_fast_path {
                // Wait for the fast-path timer before using the slow QC.
                let fast_timer = FastTimer::new(vote.clone(), self.fast_path_timeout);
                self.fast_timer_futures.push(Box::pin(fast_timer));
            } else if let Some(qc) = qc_opt {
                match current_instance {
                    ConsensusMessage::Prepare {
                        slot,
                        view,
                        tc: _,
                        qc_ticket: _,
                        proposals,
                    } => {
                        debug!("Prepare QC formed in slot {:?}", slot);
                        debug!(
                            "Prepare has slot: {}, view: {}, digest: {}",
                            slot,
                            view,
                            current_instance.digest()
                        );

                        // Advance locally in all-to-all mode.
                        if self.all_to_all {
                            let slot = *slot;
                            let view = *view;
                            let proposals = proposals.clone();
                            let try_fast = qc_maker.try_fast;

                            if try_fast {
                                // A fast quorum commits locally.
                                debug!("taking fast path! (all-to-all)");
                                let commit_message = ConsensusMessage::Commit {
                                    slot,
                                    view,
                                    qc,
                                    proposals,
                                };
                                let header = Header {
                                    author: self.name,
                                    ..Header::default()
                                };
                                self.process_commit_message(commit_message, &header).await?;
                            } else {
                                // A slow quorum advances to Confirm.
                                Box::pin(
                                    self.all_to_all_synthesize_confirm(slot, view, qc, proposals),
                                )
                                .await?;
                            }
                        } else {
                            let new_consensus_message = match qc_maker.try_fast {
                                true => {
                                    debug!("taking fast path!");
                                    ConsensusMessage::Commit {
                                        slot: *slot,
                                        view: *view,
                                        qc,
                                        proposals: proposals.clone(),
                                    }
                                }
                                false => ConsensusMessage::Confirm {
                                    slot: *slot,
                                    view: *view,
                                    qc,
                                    proposals: proposals.clone(),
                                },
                            };

                            self.send_consensus_req(new_consensus_message).await?;
                        }
                    }
                    ConsensusMessage::Confirm {
                        slot,
                        view,
                        qc: _,
                        proposals,
                    } => {
                        debug!("Commit QC formed in slot {:?}", slot);
                        let new_consensus_message = ConsensusMessage::Commit {
                            slot: *slot,
                            view: *view,
                            qc,
                            proposals: proposals.clone(),
                        };

                        if self.all_to_all {
                            // All-to-all replicas commit their local Confirm quorum.
                            let header = Header {
                                author: self.name,
                                ..Header::default()
                            };
                            self.process_commit_message(new_consensus_message, &header)
                                .await?;
                        } else {
                            self.send_consensus_req(new_consensus_message).await?;
                        }
                    }
                    ConsensusMessage::Commit {
                        slot: _,
                        view: _,
                        qc: _,
                        proposals: _,
                    } => {
                        panic!("Should never receive Vote for Commit")
                    }
                };
            }
        }

        Ok(())
    }

    /// Registers and broadcasts an all-to-all Confirm after a slow Prepare quorum.
    async fn all_to_all_synthesize_confirm(
        &mut self,
        slot: Slot,
        view: View,
        qc: QC,
        proposals: HashMap<PublicKey, Proposal>,
    ) -> DagResult<()> {
        let confirm_message = ConsensusMessage::Confirm {
            slot,
            view,
            qc,
            proposals,
        };
        let confirm_digest = confirm_message.digest();

        self.consensus_instances
            .insert((slot, confirm_digest.clone()), confirm_message.clone());
        self.high_qcs.insert(slot, confirm_message.clone());
        self.last_confirmed_consensus.insert((slot, view));

        let sig = self
            .signature_service
            .request_consensus_signature(confirm_digest.clone())
            .await;
        let confirm_vote = ConsensusVote {
            author: self.name,
            slot,
            digest: confirm_digest.clone(),
            sig,
        };

        let addresses = self
            .committee
            .others_primaries(&self.name)
            .iter()
            .map(|(_, x)| x.primary_to_primary)
            .collect();
        let bytes = bincode::serialize(&PrimaryMessage::ConsensusVote(confirm_vote.clone()))
            .expect("Failed to serialize consensus vote");
        let handlers = self
            .network
            .broadcast_typed(addresses, Bytes::from(bytes), "ConsensusVote")
            .await;
        self.consensus_cancel_handlers
            .entry(slot)
            .or_default()
            .extend(handlers);

        self.process_consensus_vote(confirm_vote, false).await?;

        // Process Confirm votes that arrived before registration.
        self.drain_pending_consensus_votes(slot, confirm_digest)
            .await;

        Ok(())
    }

    /// Accepts pending votes only within the open-instance window.
    fn is_sane_pending_vote_slot(&self, slot: Slot) -> bool {
        !self.committed_slots.contains_key(&slot)
            && (slot <= self.k
                || self.views.contains_key(&slot)
                || slot
                    .checked_sub(self.k)
                    .is_some_and(|ticket_slot| self.committed_slots.contains_key(&ticket_slot)))
    }

    /// Bounds distinct pending digests per slot to O(n).
    fn pending_vote_digest_cap(&self) -> usize {
        2 * self.committee.size()
    }

    /// Buffers one vote per committee author and caps digests per slot.
    fn buffer_pending_consensus_vote(&mut self, vote: ConsensusVote) {
        let slot = vote.slot;
        let digest = vote.digest.clone();
        let author = vote.author;
        let cap = self.pending_vote_digest_cap();

        let by_digest = self.pending_consensus_votes.entry(slot).or_default();
        if !by_digest.contains_key(&digest) && by_digest.len() >= cap {
            debug!(
                "dropping all-to-all pending vote for slot {}: at distinct-digest cap {}",
                slot, cap
            );
            return;
        }
        by_digest
            .entry(digest)
            .or_default()
            .entry(author)
            .or_insert(vote);
    }

    /// Processes votes buffered before instance registration.
    /// Invalid buffered votes are ignored.
    async fn drain_pending_consensus_votes(&mut self, slot: Slot, digest: Digest) {
        let votes = self
            .pending_consensus_votes
            .get_mut(&slot)
            .and_then(|by_digest| by_digest.remove(&digest));
        if let Some(votes) = votes {
            for (_, vote) in votes {
                if let Err(e) = self.process_consensus_vote(vote, false).await {
                    warn!(
                        "Failed to process buffered all-to-all consensus vote: {}",
                        e
                    );
                }
            }
        }
        if self
            .pending_consensus_votes
            .get(&slot)
            .is_some_and(HashMap::is_empty)
        {
            self.pending_consensus_votes.remove(&slot);
        }
    }

    fn set_consensus_proposal_with_own(
        &mut self,
        consensus_message: &mut ConsensusMessage,
        own_tip: Proposal,
    ) {
        if let ConsensusMessage::Prepare {
            slot,
            view: _,
            tc,
            qc_ticket: _,
            proposals,
        } = consensus_message
        {
            let set_proposal = tc.is_none() || proposals.is_empty();
            if set_proposal {
                debug!("UPDATING HEADER for slot {}", slot);
                let allow_optimistic_cut = allow_optimistic_leader_cut(
                    self.use_optimistic_tips,
                    self.certified_only_leader,
                    tc.is_some(),
                );
                *proposals = if allow_optimistic_cut {
                    self.current_proposal_tips.clone()
                } else {
                    // A later-view proposal with no TC winner, every seamless
                    // proposal, and the benchmark's Byzantine leader strategy
                    // start from the local certified cut.
                    self.current_certified_tips.clone()
                };

                // View 1 of the optimistic protocol may use the leader's own
                // current car. A no-winner TC, like every seamless Prepare,
                // must keep a fully PoA-certified cut; replacing its own-lane
                // entry with an optimistic car would make the fallback
                // structurally invalid at every replica.
                let allow_optimistic_own = allow_optimistic_cut;
                if autobahn_own_tip_is_admissible(
                    &self.committee,
                    &self.name,
                    allow_optimistic_own,
                    &own_tip,
                ) {
                    proposals.insert(self.name, own_tip);
                }

                // A seamless Prepare is data-independent: every lane entry,
                // including the leader's, is Genesis or PoA-certified.
                if !self.use_optimistic_tips {
                    debug_assert!(
                        proposals.iter().all(|(lane, proposal)| {
                            matches!(
                                proposal.verify(lane, &self.committee),
                                Ok(ProposalKind::Genesis | ProposalKind::Certified)
                            )
                        }),
                        "seamless invariant violated: a Prepare contains an optimistic tip"
                    );
                }

                for proposal in proposals.values_mut() {
                    debug!("new proposal height is {:?}", proposal.height);
                }
            }
        }
    }

    fn set_consensus_proposal(&mut self, consensus_message: &mut ConsensusMessage) {
        let own_tip = if self.current_header.author == self.name && self.current_header.height > 0 {
            Proposal::optimistic(&self.current_header)
        } else {
            self.current_certified_tips
                .get(&self.name)
                .cloned()
                .unwrap_or_else(|| Proposal::genesis(self.name, &self.committee))
        };
        self.set_consensus_proposal_with_own(consensus_message, own_tip);
    }

    #[async_recursion]
    async fn send_consensus_req(
        &mut self,
        mut consensus_message: ConsensusMessage,
    ) -> DagResult<()> {
        self.set_consensus_proposal(&mut consensus_message);

        match &consensus_message {
            ConsensusMessage::Prepare {
                slot,
                view,
                tc: _,
                qc_ticket: _,
                proposals: _,
            } => {
                if self.during_simulated_asynchrony {
                    debug!("Simulating Asynchrony: skip sending Prepare for slot {} view {}. This will trigger a view change", slot, view);
                    self.async_delayed_prepare = Some(consensus_message);
                    return Ok(());
                }
            }
            ConsensusMessage::Confirm {
                slot: _,
                view: _,
                qc: _,
                proposals: _,
            } => {}
            ConsensusMessage::Commit {
                slot: _,
                view: _,
                qc: _,
                proposals: _,
            } => {}
        };

        debug!("Send req for Consensus message {}", consensus_message);

        let consensus_req =
            ConsensusRequest::new(self.name, consensus_message, &mut self.signature_service).await;

        let addresses = self
            .committee
            .others_primaries(&self.name)
            .iter()
            .map(|(_, x)| x.primary_to_primary)
            .collect();
        let message = bincode::serialize(&PrimaryMessage::ConsensusRequest(consensus_req.clone()))
            .expect("Failed to serialize timeout message");
        let handlers = self
            .network
            .broadcast_typed(addresses, Bytes::from(message), "ConsensusRequest")
            .await;

        self.cancel_handlers
            .entry(self.current_header.height())
            .or_default()
            .extend(handlers);

        self.process_consensus_request(consensus_req).await?;

        Ok(())
    }

    #[async_recursion]
    async fn process_certificate(&mut self, certificate: Certificate) -> DagResult<()> {
        debug!("Processing {:?}", certificate);

        let bytes = bincode::serialize(&certificate).expect("Failed to serialize certificate");
        self.store.write(certificate.digest().to_vec(), bytes).await;

        Ok(())
    }

    #[async_recursion]
    async fn try_prepare_waiting_slots(&mut self) -> DagResult<()> {
        for _i in 0..self.prepare_tickets.len() {
            let prepare_msg = self.prepare_tickets.pop_front().unwrap();
            self.is_prepare_ticket_ready(&prepare_msg).await?;
        }

        Ok(())
    }

    async fn is_prepare_ticket_ready(
        &mut self,
        prepare_message: &ConsensusMessage,
    ) -> DagResult<()> {
        match prepare_message {
            ConsensusMessage::Prepare {
                slot,
                view: _,
                tc: _,
                qc_ticket: _,
                proposals,
            } => {
                let Some(next_slot) = slot.checked_add(1) else {
                    return Ok(());
                };
                let next_leader = self.leader_elector.get_leader(next_slot, 1);

                if self.name != next_leader {
                    return Ok(());
                }

                // Ignore tickets for a slot already proposed locally.
                if self.already_proposed_slots.contains(&next_slot)
                    || self.committed_slots.contains_key(&next_slot)
                {
                    return Ok(());
                }

                // Wait for slot s-k before opening another instance.
                if next_slot > self.k {
                    debug!("beyond init k");
                    if !self.committed_slots.contains_key(&(next_slot - self.k)) {
                        debug!("too many instances open");
                        self.prepare_tickets.push_back(prepare_message.clone());
                        return Ok(());
                    }
                }

                // Open the next slot after its proposals have enough coverage.
                if self.enough_coverage(proposals) {
                    debug!("have enough coverage to start slot {}", next_slot);

                    let qc_ticket = match next_slot > self.k {
                        true => Some(
                            self.committed_slots
                                .get(&(next_slot - self.k))
                                .unwrap()
                                .clone(),
                        ),
                        false => None,
                    };

                    let new_prepare_instance = ConsensusMessage::Prepare {
                        slot: next_slot,
                        view: 1,
                        tc: None,
                        qc_ticket,
                        proposals: HashMap::new(),
                    };

                    self.already_proposed_slots.insert(next_slot);
                    self.proposed_consensus_views.insert((next_slot, 1));

                    if self.use_ride_share {
                        self.tx_info
                            .send(new_prepare_instance)
                            .await
                            .expect("failed to send info to proposer");
                    } else {
                        debug!("enough coverage!");
                        self.send_consensus_req(new_prepare_instance).await?;
                    }

                    Ok(())
                } else {
                    // Retry when more proposals arrive.
                    self.prepare_tickets.push_back(prepare_message.clone());
                    Ok(())
                }
            }
            _ => Ok(()),
        }
    }

    fn validate_cut(
        &self,
        proposals: &HashMap<PublicKey, Proposal>,
        _proposal_leader: PublicKey,
    ) -> bool {
        self.verified
            .cut_is_valid(&self.committee, self.use_optimistic_tips, proposals)
    }

    /// Validates the self-contained shape and proofs of a cut, independently
    /// of which view originally admitted its optimistic tips. A QC or TC
    /// supplies that missing admission evidence.
    fn validate_proven_cut(&self, proposals: &HashMap<PublicKey, Proposal>) -> bool {
        self.verified.cut_is_valid(&self.committee, true, proposals)
    }

    /// A TC without a winner starts from a fully certified cut.
    fn validate_no_winner_cut(
        &self,
        proposals: &HashMap<PublicKey, Proposal>,
        _proposal_leader: PublicKey,
    ) -> bool {
        self.verified
            .cut_is_valid(&self.committee, false, proposals)
    }

    fn validate_timeout_evidence(&self, timeout: &Timeout) -> bool {
        [&timeout.high_qc, &timeout.high_prop]
            .into_iter()
            .flatten()
            .all(|message| {
                let (slot, view, proposals) = match message {
                    ConsensusMessage::Prepare {
                        slot,
                        view,
                        proposals,
                        ..
                    }
                    | ConsensusMessage::Confirm {
                        slot,
                        view,
                        proposals,
                        ..
                    }
                    | ConsensusMessage::Commit {
                        slot,
                        view,
                        proposals,
                        ..
                    } => (*slot, *view, proposals),
                };
                slot == timeout.slot && view <= timeout.view && self.validate_proven_cut(proposals)
            })
    }

    /// A view-1 ticket is also transferable evidence that slot `s-k`
    /// committed. Adopt an unseen ticket locally instead of merely using it
    /// as an admission check for the newer slot.
    async fn adopt_qc_ticket(
        &mut self,
        consensus_message: &ConsensusMessage,
        carrier: &Header,
    ) -> DagResult<()> {
        let ConsensusMessage::Prepare {
            qc_ticket: Some(ticket),
            ..
        } = consensus_message
        else {
            return Ok(());
        };
        if self.committed_slots.contains_key(&ticket.slot) {
            return Ok(());
        }
        self.process_commit_message(transform_commit_qc(ticket.clone()), carrier)
            .await
    }

    #[async_recursion]
    async fn is_valid(&mut self, consensus_message: &ConsensusMessage, author: PublicKey) -> bool {
        let (slot, view, proposals) = match consensus_message {
            ConsensusMessage::Prepare {
                slot,
                view,
                proposals,
                ..
            }
            | ConsensusMessage::Confirm {
                slot,
                view,
                proposals,
                ..
            }
            | ConsensusMessage::Commit {
                slot,
                view,
                proposals,
                ..
            } => (*slot, *view, proposals),
        };
        let proposal_leader = self.leader_elector.get_leader(slot, view);
        if author != proposal_leader || !self.validate_proven_cut(proposals) {
            return false;
        }
        if !matches!(consensus_message, ConsensusMessage::Commit { .. })
            && self.committed_slots.contains_key(&slot)
        {
            return false;
        }

        match consensus_message {
            ConsensusMessage::Prepare {
                slot,
                view,
                tc,
                qc_ticket,
                proposals,
            } => {
                // View 1 uses a QC ticket; later views require exactly the
                // previous view's TC.
                let ticket_valid = match tc {
                    Some(tc) => {
                        if qc_ticket.is_some()
                            || *view <= 1
                            || tc.slot != *slot
                            || tc.view.checked_add(1) != Some(*view)
                        {
                            return false;
                        }
                        if self.verified.check_tc(tc, &self.committee).is_err()
                            || !tc
                                .timeouts
                                .iter()
                                .all(|timeout| self.validate_timeout_evidence(timeout))
                        {
                            return false;
                        }

                        let winning_proposals = tc.get_winning_proposals(&self.committee);
                        if !winning_proposals.is_empty() {
                            winning_proposals == *proposals
                        } else {
                            self.validate_no_winner_cut(proposals, proposal_leader)
                        }
                    }
                    None => {
                        if !self.validate_cut(proposals, proposal_leader) {
                            return false;
                        }
                        if *view != 1 || !self.use_parallel_proposals {
                            return false;
                        }
                        if *slot > self.k {
                            let Some(commit_qc) = qc_ticket else {
                                return false;
                            };
                            if commit_qc.slot.checked_add(self.k) != Some(*slot) {
                                return false;
                            }
                            self.verified.check_commit(
                                &transform_commit_qc(commit_qc.clone()),
                                &self.committee,
                            )
                        } else {
                            qc_ticket.is_none()
                        }
                    }
                };

                let curr_view = self.views.get(slot).copied().unwrap_or(0);
                if !ticket_valid
                    || curr_view > *view
                    || self.last_voted_consensus.contains(&(*slot, *view))
                    || self.sent_timeouts.contains(&(*slot, *view))
                {
                    return false;
                }
                self.enter_consensus_view(*slot, *view);
                // Section 5.4 starts the next parallel slot's timer upon the
                // first valid Prepare(s, _), independently of payload sync and
                // CommitQC(s+1-k). The latter remains the view-1 leader's
                // bounded-concurrency proposal ticket.
                if let Some(next_slot) = parallel_timer_slot(*slot, self.k) {
                    if !self.views.contains_key(&next_slot)
                        && !self.committed_slots.contains_key(&next_slot)
                    {
                        debug!("start timer for slot {}", next_slot);
                        self.enter_consensus_view(next_slot, 1);
                    }
                }
                true
            }
            ConsensusMessage::Confirm {
                slot,
                view,
                qc: _,
                proposals: _,
            } => {
                debug!("try to unwrap slot");

                let curr_view = self.views.get(slot).copied().unwrap_or(0);
                if curr_view <= *view
                    && !self.sent_timeouts.contains(&(*slot, *view))
                    && !self.last_confirmed_consensus.contains(&(*slot, *view))
                    && self
                        .verified
                        .check_confirm(consensus_message, &self.committee)
                {
                    self.enter_consensus_view(*slot, *view);
                    return true;
                }
                false
            }
            ConsensusMessage::Commit {
                slot: _,
                view: _,
                qc: _,
                proposals: _,
            } => self
                .verified
                .check_commit(consensus_message, &self.committee),
        }
    }

    #[async_recursion]
    async fn process_consensus_messages(
        &mut self,
        header: &Header,
    ) -> DagResult<Vec<(Slot, Digest, ConsensusSignature)>> {
        let mut consensus_votes: Vec<(Slot, Digest, ConsensusSignature)> = Vec::new();

        for consensus_message in header.consensus_messages.values() {
            debug!("processing instance");
            if self.is_valid(consensus_message, header.author).await {
                self.adopt_qc_ticket(consensus_message, header).await?;
                match consensus_message {
                    ConsensusMessage::Prepare {
                        slot,
                        view: _,
                        tc: _,
                        qc_ticket: _,
                        proposals,
                    } => {
                        debug!(
                            "processing prepare in slot {:?} with proposal {:?}",
                            slot, proposals
                        );
                        // An ambassador car always receives its availability
                        // vote. If the consensus value needs a blocking tip
                        // fetch, omit only this passenger vote and resume it
                        // independently when synchronization completes.
                        match self
                            .synchronizer
                            .get_proposals(consensus_message, header)
                            .await
                        {
                            Ok(true) => {
                                self.process_prepare_message(
                                    consensus_message,
                                    consensus_votes.as_mut(),
                                )
                                .await;
                            }
                            Ok(false) => {}
                            Err(error) => {
                                warn!("Omitting invalid ride-shared Prepare: {}", error);
                            }
                        }
                    }
                    ConsensusMessage::Confirm {
                        slot,
                        view: _,
                        qc: _,
                        proposals,
                    } => {
                        debug!(
                            "processing confirm in slot {:?} with proposal {:?}",
                            slot, proposals
                        );
                        match self
                            .synchronizer
                            .get_proposals(consensus_message, header)
                            .await
                        {
                            Ok(_) => {
                                self.process_confirm_message(
                                    consensus_message,
                                    consensus_votes.as_mut(),
                                )
                                .await;
                            }
                            Err(error) => {
                                warn!("Omitting invalid ride-shared Confirm: {}", error);
                            }
                        }
                    }
                    ConsensusMessage::Commit {
                        slot,
                        view: _,
                        qc: _,
                        proposals: _,
                    } => {
                        debug!("processing commit in slot {:?}", slot);
                        if let Err(error) = self
                            .process_commit_message(consensus_message.clone(), header)
                            .await
                        {
                            warn!("Omitting invalid ride-shared Commit: {}", error);
                        }
                    }
                }
            }
        }

        Ok(consensus_votes)
    }

    async fn process_consensus_request(
        &mut self,
        consensus_req: ConsensusRequest,
    ) -> DagResult<()> {
        let consensus_message = &consensus_req.message;
        debug!("received consensus request for slot");

        match consensus_message {
            ConsensusMessage::Prepare {
                slot,
                view: _,
                tc: _,
                qc_ticket: _,
                proposals,
            } => {
                debug!(
                    "processing prepare in slot {:?} with proposal {:?}",
                    slot, proposals
                );
            }
            ConsensusMessage::Confirm {
                slot,
                view: _,
                qc: _,
                proposals,
            } => {
                debug!(
                    "processing confirm in slot {:?} with proposal {:?}",
                    slot, proposals
                );
            }
            ConsensusMessage::Commit {
                slot,
                view: _,
                qc: _,
                proposals: _,
            } => {
                debug!("processing commit in slot {:?}", slot);
            }
        }
        debug!("try to verify");
        self.verified
            .check_consensus_request(&consensus_req, &self.committee)?;
        debug!("check validity");
        if !self.is_valid(consensus_message, consensus_req.author).await {
            return Ok(());
        }

        let carrier = Header {
            author: consensus_req.author,
            ..Header::default()
        };
        self.adopt_qc_ticket(consensus_message, &carrier).await?;

        // Register only a verified, leader-authored value. Otherwise a forged
        // request could poison the digest-to-instance table before validation.
        let dig = consensus_message.digest();
        match consensus_message {
            ConsensusMessage::Prepare { slot, .. } | ConsensusMessage::Confirm { slot, .. } => {
                self.consensus_instances
                    .insert((*slot, dig.clone()), consensus_message.clone());
                if self.all_to_all {
                    self.drain_pending_consensus_votes(*slot, dig).await;
                }
            }
            ConsensusMessage::Commit { .. } => {}
        }

        self.process_consensus_message(consensus_req.message, consensus_req.author)
            .await
    }

    async fn process_consensus_message(
        &mut self,
        consensus_message: ConsensusMessage,
        author: PublicKey,
    ) -> DagResult<()> {
        let mut consensus_votes: Vec<(Slot, Digest, ConsensusSignature)> = Vec::new();

        debug!("processing consensus msg");

        let header = Header {
            author,
            ..Header::default()
        };

        match &consensus_message {
            ConsensusMessage::Prepare {
                slot,
                view: _,
                tc: _,
                qc_ticket: _,
                proposals,
            } => {
                debug!(
                    "processing prepare in slot {:?} with proposal {:?}",
                    slot, proposals
                );
                if !self
                    .synchronizer
                    .get_proposals(&consensus_message, &header)
                    .await?
                {
                    debug!(
                        "proposals of prepare in slot {:?} with proposal {:?} are not ready",
                        slot, proposals
                    );
                    return Ok(());
                }
                self.process_prepare_message(&consensus_message, consensus_votes.as_mut())
                    .await;
            }
            ConsensusMessage::Confirm {
                slot,
                view: _,
                qc: _,
                proposals,
            } => {
                debug!(
                    "processing confirm in slot {:?} with proposal {:?}",
                    slot, proposals
                );
                self.synchronizer
                    .get_proposals(&consensus_message, &header)
                    .await?;
                self.process_confirm_message(&consensus_message, consensus_votes.as_mut())
                    .await;
            }
            ConsensusMessage::Commit {
                slot,
                view: _,
                qc: _,
                proposals: _,
            } => {
                debug!("processing commit in slot {:?}", slot);
                self.process_commit_message(consensus_message.clone(), &header)
                    .await?;
            }
        }

        debug!(
            "Returning from process consensus size of consensus sigs {:?}",
            consensus_votes.len()
        );

        if consensus_votes.is_empty() {
            return Ok(());
        }

        let (slot, digest, sig) = consensus_votes.pop().unwrap();
        let vote = ConsensusVote {
            author: self.name,
            slot,
            digest,
            sig,
        };

        if self.all_to_all {
            // All-to-all replicas process and broadcast their own vote.
            debug!("Process own consensus vote (all-to-all)");
            self.process_consensus_vote(vote.clone(), false)
                .await
                .expect("Failed to process our own vote");

            let addresses = self
                .committee
                .others_primaries(&self.name)
                .iter()
                .map(|(_, x)| x.primary_to_primary)
                .collect();
            let bytes = bincode::serialize(&PrimaryMessage::ConsensusVote(vote))
                .expect("Failed to serialize our own vote");
            let handlers = self
                .network
                .broadcast_typed(addresses, Bytes::from(bytes), "ConsensusVote")
                .await;
            self.consensus_cancel_handlers
                .entry(slot)
                .or_default()
                .extend(handlers);
        } else if author == self.name {
            debug!("Process own consensus vote");
            self.process_consensus_vote(vote, false)
                .await
                .expect("Failed to process our own vote");
        } else {
            debug!("Send consensus vote to replica {}", author);

            let address = self
                .committee
                .primary(&author)
                .expect("Author of valid header is not in the committee")
                .primary_to_primary;
            let bytes = bincode::serialize(&PrimaryMessage::ConsensusVote(vote))
                .expect("Failed to serialize our own vote");
            let handler = self
                .network
                .send_typed(address, Bytes::from(bytes), "ConsensusVote")
                .await;
            self.consensus_cancel_handlers
                .entry(slot)
                .or_default()
                .push(handler);
        }

        Ok(())
    }

    async fn process_prepare_message(
        &mut self,
        prepare_message: &ConsensusMessage,
        consensus_sigs: &mut Vec<(Slot, Digest, ConsensusSignature)>,
    ) {
        if let ConsensusMessage::Prepare {
            slot,
            view,
            tc: _,
            qc_ticket: _,
            proposals,
        } = prepare_message
        {
            // A leader skips `is_valid` for its own message, so record the
            // view here as well: the slot-period timer arming depends on it.
            let curr_view = self.views.get(slot).copied().unwrap_or(0);
            if curr_view < *view {
                self.views.insert(*slot, *view);
            }

            let _ = self.is_prepare_ticket_ready(prepare_message).await;

            for proposal in proposals.values() {
                debug!(
                    "prepare slot {:?}, proposal height {:?}",
                    slot, proposal.height
                );
            }
            debug!("prepare vote in slot {:?}", slot);

            self.last_voted_consensus.insert((*slot, *view));

            // Every Prepare vote is view-change evidence, independently of
            // whether the optional fast path is enabled.
            self.high_proposals.insert(
                *slot,
                ConsensusMessage::Prepare {
                    slot: *slot,
                    view: *view,
                    tc: None,
                    qc_ticket: None,
                    proposals: proposals.clone(),
                },
            );

            let sig = self
                .signature_service
                .request_consensus_signature(prepare_message.digest())
                .await;
            consensus_sigs.push((*slot, prepare_message.digest(), sig));
            debug!(
                "Prepare-Vote for slot: {}, view: {},has digest: {}",
                slot,
                view,
                prepare_message.digest()
            );
        }
    }

    async fn process_confirm_message(
        &mut self,
        confirm_message: &ConsensusMessage,
        consensus_sigs: &mut Vec<(Slot, Digest, ConsensusSignature)>,
    ) {
        if let ConsensusMessage::Confirm {
            slot,
            view,
            qc,
            proposals: _,
        } = confirm_message
        {
            // Same own-message gap as in `process_prepare_message`.
            let curr_view = self.views.get(slot).copied().unwrap_or(0);
            if curr_view < *view {
                self.views.insert(*slot, *view);
            }

            self.high_qcs.insert(*slot, confirm_message.clone());
            self.last_confirmed_consensus.insert((*slot, *view));

            let sig = self
                .signature_service
                .request_consensus_signature(confirm_message.digest())
                .await;
            consensus_sigs.push((*slot, confirm_message.digest(), sig));
            debug!(
                "Confirm-Vote for slot: {}, view: {}, qc_dig {:?} -> has digest: {}",
                slot,
                view,
                qc.id,
                confirm_message.digest()
            );
        }
    }

    fn enough_coverage(&mut self, prepare_proposals: &HashMap<PublicKey, Proposal>) -> bool {
        let current_proposals = match self.use_optimistic_tips {
            true => &self.current_proposal_tips,
            false => &self.current_certified_tips,
        };

        let new_tips: HashMap<&PublicKey, &Proposal> = current_proposals
            .iter()
            .filter(|(pk, proposal)| proposal.height > prepare_proposals.get(pk).unwrap().height)
            .collect();

        new_tips.len() as u32 >= self.committee.quorum_threshold()
    }

    #[async_recursion]
    async fn process_commit_message(
        &mut self,
        commit_message: ConsensusMessage,
        header: &Header,
    ) -> DagResult<()> {
        debug!("Called process commit");
        if let ConsensusMessage::Commit {
            slot,
            view,
            qc,
            proposals,
        } = &commit_message
        {
            debug!("Try to commit slot {}", slot);
            if self.committed_slots.contains_key(slot) {
                debug!("Ignoring duplicate CommitQC for slot {}", slot);
                return Ok(());
            }
            if self.simulate_asynchrony && *slot == 1 {
                debug!("added async timers");
                let async_start = Timer::new(0, 0, self.asynchrony_start);
                let async_end = Timer::new(0, 0, self.asynchrony_start + self.asynchrony_duration);
                self.async_timer_futures.push(Box::pin(async_start));
                self.async_timer_futures.push(Box::pin(async_end));
            }

            self.timers.retain(|(timer_slot, _)| timer_slot != slot);

            let sl = *slot;
            self.last_committed_slot = max(sl, self.last_committed_slot);
            self.committed_slots.insert(
                sl,
                CommitQC::new(*slot, *view, qc.clone(), proposals.clone()).await,
            );

            // Sequential mode retains the ordinary CommitQC(s-1) slot ticket.
            // Parallel mode starts slot timers from Prepare(s-1, _) instead.
            if self.k == 1 {
                if let Some(next_slot) = slot.checked_add(1) {
                    debug!("start timer for slot {}", next_slot);
                    self.enter_consensus_view(next_slot, 1);
                }
            }

            // Defer output until every proposal ancestor is available.
            if self
                .synchronizer
                .get_proposals(&commit_message, header)
                .await?
            {
                debug!("sending to committer");
                self.tx_committer
                    .send(commit_message)
                    .await
                    .expect("Failed to send headers");
            }

            self.try_prepare_waiting_slots().await?;

            self.clean_slot_periods(sl);
        }

        Ok(())
    }

    fn clean_slot_periods(&mut self, slot: Slot) {
        let k = self.k;

        // Bound the verified-object memo even during long quiet stretches.
        self.verified.advance_if_full();
        self.synchronizer.gc_implicit_cut_sources(slot, k);

        self.consensus_instances
            .retain(|(s, _), _| keep_after_slot_period_gc(*s, slot, k));
        if self.all_to_all {
            // Local instance state for a pipeline lane can be discarded once the
            // lane advances, but an outbound all-to-all vote is still needed by
            // an honest replica that has not committed that slot yet. Dropping
            // its CancelHandler here cancels reliable delivery and can strand
            // that replica permanently. Keep delivery alive for the same bounded
            // recovery window used by the rest of the primary state.
            let gc_depth = self.gc_depth;
            self.consensus_cancel_handlers
                .retain(|s, _| keep_all_to_all_delivery(*s, slot, gc_depth));
        } else {
            self.consensus_cancel_handlers
                .retain(|s, _| keep_after_slot_period_gc(*s, slot, k));
        }

        self.qc_makers
            .retain(|(s, _), _| keep_after_slot_period_gc(*s, slot, k));
        self.tc_makers
            .retain(|(s, _), _| keep_after_slot_period_gc(*s, slot, k));
        self.sent_timeouts
            .retain(|(s, _)| keep_after_slot_period_gc(*s, slot, k));

        self.pending_consensus_votes
            .retain(|s, _| keep_after_slot_period_gc(*s, slot, k));

        self.views
            .retain(|s, _| keep_after_slot_period_gc(*s, slot, k));
        self.timers
            .retain(|(s, _)| keep_after_slot_period_gc(*s, slot, k));
        self.last_voted_consensus
            .retain(|(s, _)| keep_after_slot_period_gc(*s, slot, k));
        self.last_confirmed_consensus
            .retain(|(s, _)| keep_after_slot_period_gc(*s, slot, k));
        self.high_proposals
            .retain(|s, _| keep_after_slot_period_gc(*s, slot, k));
        self.high_qcs
            .retain(|s, _| keep_after_slot_period_gc(*s, slot, k));
        self.already_proposed_slots
            .retain(|s| keep_after_slot_period_gc(*s, slot, k));
        self.proposed_consensus_views
            .retain(|(s, _)| keep_after_slot_period_gc(*s, slot, k));
        let committed = self.last_committed_slot;
        let gc_depth = max(self.gc_depth, k);
        // Parallel slots form k independent ticket chains. One chain may lag
        // arbitrarily far behind the maximum committed slot while the others
        // continue; retain the newest CommitQC in every chain so that global
        // age-based GC cannot permanently retire the lagging chain.
        let pipeline_tickets = latest_pipeline_tickets(self.committed_slots.keys().copied(), k);
        self.committed_slots.retain(|s, _| {
            pipeline_tickets.contains(s) || keep_all_to_all_delivery(*s, committed, gc_depth)
        });
    }

    #[async_recursion]
    async fn process_loopback(
        &mut self,
        consensus_message: ConsensusMessage,
        header: Header,
    ) -> DagResult<()> {
        debug!("Can reprocess a header/commit message");
        match &consensus_message {
            ConsensusMessage::Prepare {
                slot: _,
                view: _,
                tc: _,
                qc_ticket: _,
                proposals: _,
            } => {
                // Revalidate after a blocking tip fetch. In particular, a
                // timeout may have fired while synchronization was in flight;
                // replicas must not vote in that view afterward.
                if self.is_valid(&consensus_message, header.author).await {
                    self.process_consensus_message(consensus_message, header.author)
                        .await?;
                }
            }
            ConsensusMessage::Confirm {
                slot: _,
                view: _,
                qc: _,
                proposals: _,
            } => {}
            ConsensusMessage::Commit {
                slot: _,
                view: _,
                qc: _,
                proposals: _,
            } => {
                // A suffix reply may materialize its tip before every payload
                // in the prefix. Re-walk the cut on each loopback and release
                // execution only when the whole suffix is local.
                if self
                    .synchronizer
                    .get_proposals(&consensus_message, &header)
                    .await?
                {
                    self.tx_committer
                        .send(consensus_message)
                        .await
                        .expect("Failed to send to committer");
                }
            }
        };
        Ok(())
    }

    async fn process_forwarded_message(
        &mut self,
        consensus_message: ConsensusMessage,
    ) -> DagResult<()> {
        // This legacy envelope has no sender signature. Only a transferable
        // CommitQC is self-authenticating; Prepare and Confirm must arrive in
        // a signed ConsensusRequest (or a signed ride-sharing car).
        if let ConsensusMessage::Commit {
            slot: _,
            view: _,
            proposals,
            ..
        } = &consensus_message
        {
            if self
                .verified
                .check_commit(&consensus_message, &self.committee)
                && self.validate_proven_cut(proposals)
            {
                let header = self.current_header.clone();
                self.process_commit_message(consensus_message, &header)
                    .await?;
            }
        }
        Ok(())
    }

    /// Enters a ticket-certified view, cancels older logical timers for the
    /// slot, and starts the new view timer exactly once.
    fn enter_consensus_view(&mut self, slot: Slot, view: View) {
        let current = self.views.entry(slot).or_default();
        *current = (*current).max(view);
        self.timers
            .retain(|(timer_slot, timer_view)| *timer_slot != slot || *timer_view >= view);
        if !self.committed_slots.contains_key(&slot) && self.timers.insert((slot, view)) {
            self.timer_futures
                .push(Box::pin(Timer::new(slot, view, self.timeout_delay)));
        }
    }

    async fn broadcast_own_timeout(&mut self, slot: Slot, view: View) -> Option<Timeout> {
        if !self.sent_timeouts.insert((slot, view)) {
            return None;
        }

        let timeout = Timeout::new(
            slot,
            view,
            self.high_qcs.get(&slot).cloned(),
            self.high_proposals.get(&slot).cloned(),
            self.name,
            self.signature_service.clone(),
        )
        .await;
        let addresses = self
            .committee
            .others_primaries(&self.name)
            .iter()
            .map(|(_, x)| x.primary_to_primary)
            .collect();
        let message = bincode::serialize(&PrimaryMessage::Timeout(timeout.clone()))
            .expect("Failed to serialize timeout message");
        let handlers = self
            .network
            .broadcast_typed(addresses, Bytes::from(message), "Timeout")
            .await;
        self.consensus_cancel_handlers
            .entry(slot)
            .or_default()
            .extend(handlers);
        Some(timeout)
    }

    async fn local_timeout_round(&mut self, slot: Slot, view: View) -> DagResult<()> {
        if !self.timers.contains(&(slot, view)) {
            debug!(
                "Timer for slot {}, view {} is obsolete. Has been cancelled",
                slot, view
            );
            return Ok(());
        }

        if let Some(v) = self.views.get(&slot) {
            if *v > view {
                debug!(
                    "Timer for slot {}, view {} is obsolete. Have moved to view {}",
                    slot, view, *v
                );
                return Ok(());
            }
        };

        if self.committed_slots.contains_key(&slot) {
            return Ok(());
        }

        // Only a timer that outlives every moot check is a real view timeout.
        warn!("Timeout reached for slot {}, view {}", slot, view);

        debug!("Sending Timeout for slot {}, view {}", slot, view);
        let Some(timeout) = self.broadcast_own_timeout(slot, view).await else {
            return Ok(());
        };
        debug!("Created Timeout: {:?}", timeout);

        self.handle_timeout(&timeout).await
    }

    async fn handle_timeout(&mut self, timeout: &Timeout) -> DagResult<()> {
        debug!("Processing timeout {:?}", timeout);

        self.verified.check_timeout(timeout, &self.committee)?;
        ensure!(
            self.validate_timeout_evidence(timeout),
            DagError::MalformedTimeout(timeout.digest())
        );

        if let Some(commit_qc) = self.committed_slots.get(&timeout.slot).cloned() {
            // A committed replica helps a lagging mutineer instead of silently
            // discarding its complaint (Section 5.3).
            if timeout.author != self.name {
                let commit = transform_commit_qc(commit_qc);
                let address = self
                    .committee
                    .primary(&timeout.author)
                    .expect("Verified timeout author is in the committee")
                    .primary_to_primary;
                let message = PrimaryMessage::ConsensusMessage(commit);
                let bytes =
                    bincode::serialize(&message).expect("Failed to serialize forwarded CommitQC");
                let handler = self
                    .network
                    .send_typed(address, Bytes::from(bytes), message.type_name())
                    .await;
                self.consensus_cancel_handlers
                    .entry(timeout.slot)
                    .or_default()
                    .push(handler);
            }
            return Ok(());
        }

        if let Some(view) = self.views.get(&timeout.slot) {
            if timeout.view < *view {
                return Ok(());
            }
        };

        self.tc_makers
            .entry((timeout.slot, timeout.view))
            .or_insert_with(TCMaker::new);

        let (mut tc, amplify) = {
            let tc_maker = self
                .tc_makers
                .get_mut(&(timeout.slot, timeout.view))
                .unwrap();
            let tc = tc_maker.append(timeout.clone(), &self.committee)?;
            let amplify = tc.is_none()
                && tc_maker.weight() >= self.committee.validity_threshold()
                && !self.sent_timeouts.contains(&(timeout.slot, timeout.view));
            (tc, amplify)
        };

        // f+1 complaints prove that at least one correct replica timed out.
        // Join the mutiny once, which ensures every correct replica eventually
        // obtains the 2f+1 timeouts needed for a TC.
        if amplify {
            if let Some(own_timeout) = self.broadcast_own_timeout(timeout.slot, timeout.view).await
            {
                tc = self
                    .tc_makers
                    .get_mut(&(timeout.slot, timeout.view))
                    .expect("timeout maker was initialized")
                    .append(own_timeout, &self.committee)?;
            }
        }

        if let Some(tc) = tc {
            debug!("Assembled TimeoutCertificate {:?}", tc);

            let next_view = tc.view.checked_add(1).ok_or(DagError::InvalidQCTicket)?;
            self.enter_consensus_view(tc.slot, next_view);

            self.generate_prepare_from_tc(&tc).await?;
        }
        Ok(())
    }

    async fn generate_prepare_from_tc(&mut self, tc: &TC) -> DagResult<()> {
        let next_view = tc.view.checked_add(1).ok_or(DagError::InvalidQCTicket)?;
        if self.name == self.leader_elector.get_leader(tc.slot, next_view)
            && self.proposed_consensus_views.insert((tc.slot, next_view))
        {
            debug!("IsLeader. Start prepare from TC");
            let winning_proposals = tc.get_winning_proposals(&self.committee);

            debug!("winning proposals: {:?}", winning_proposals);

            let prepare_message: ConsensusMessage = ConsensusMessage::Prepare {
                slot: tc.slot,
                view: next_view,
                tc: Some(tc.clone()),
                qc_ticket: None,
                proposals: winning_proposals.clone(),
            };
            if self.use_ride_share {
                self.tx_info
                    .send(prepare_message.clone())
                    .await
                    .expect("Failed to send consensus instance");
            } else {
                self.send_consensus_req(prepare_message).await?;
            }
        }
        Ok(())
    }

    async fn handle_tc(&mut self, tc: &TC) -> DagResult<()> {
        debug!("Processing TC {:?}", tc);
        ensure!(
            self.verified.check_tc(tc, &self.committee).is_ok(),
            DagError::InvalidQCTicket
        );
        ensure!(
            tc.timeouts
                .iter()
                .all(|timeout| self.validate_timeout_evidence(timeout)),
            DagError::InvalidQCTicket
        );
        let next_view = tc.view.checked_add(1).ok_or(DagError::InvalidQCTicket)?;
        if self
            .views
            .get(&tc.slot)
            .is_some_and(|current| *current > next_view)
        {
            return Ok(());
        }
        self.enter_consensus_view(tc.slot, next_view);
        self.generate_prepare_from_tc(tc).await?;

        Ok(())
    }

    fn sanitize_header(&mut self, header: &Header) -> DagResult<()> {
        ensure!(
            self.gc_round <= header.height,
            DagError::HeaderTooOld(header.id.clone(), header.height)
        );

        self.verified.check_header(header, &self.committee)?;
        Ok(())
    }

    fn sanitize_vote(&mut self, vote: &Vote) -> DagResult<()> {
        let current_car = vote.id == self.current_header.id
            && vote.height == self.current_header.height
            && vote.origin == self.current_header.author;
        // Ambassador cars may move on after the short ride-sharing timeout.
        // Late replies can still carry useful consensus votes, but cannot be
        // counted toward the new car's PoA.
        ensure!(
            current_car || !vote.consensus_votes.is_empty(),
            DagError::UnexpectedVote(vote.id.clone())
        );
        // A completed header vote still accepts consensus votes carried by the
        // message, but the outer car vote must remain authenticated: otherwise
        // arbitrary public keys could populate a live QC maker at zero stake.
        if current_car && self.votes_aggregator.complete && vote.consensus_votes.is_empty() {
            return Err(DagError::CarAlreadySatisfied);
        }

        self.verified.check_vote(vote, &self.committee)
    }

    fn sanitize_certificate(&mut self, certificate: &Certificate) -> DagResult<()> {
        ensure!(
            self.gc_round <= certificate.height(),
            DagError::CertificateTooOld(certificate.digest(), certificate.height())
        );
        self.verified
            .check_certificate(certificate, &self.committee)
    }

    /// Processes primary events.
    pub async fn run(&mut self) {
        self.current_proposal_tips = Header::genesis_proposals(&self.committee);
        self.current_certified_tips = Header::genesis_proposals(&self.committee);
        debug!("genesis tips are {:?}", self.current_proposal_tips);

        debug!("start timer for slot {}", 1);
        let first_timer = Timer::new(1, 1, self.timeout_delay);
        self.timer_futures.push(Box::pin(first_timer));
        self.timers.insert((1, 1));
        self.views.insert(1, 1);

        if self.name == self.leader_elector.get_leader(1, 1) {
            let new_prepare_instance = ConsensusMessage::Prepare {
                slot: 0,
                view: 0,
                tc: None,
                qc_ticket: None,
                proposals: Header::genesis_proposals(&self.committee),
            };
            self.prepare_tickets.push_back(new_prepare_instance);
            self.already_proposed_slots.insert(0);
        }

        let genesis_cert = Certificate::genesis_certs(&self.committee)
            .get(&self.name)
            .unwrap()
            .clone();
        self.tx_proposer
            .send(genesis_cert)
            .await
            .expect("failed to send cert to proposer");

        loop {
            let result = tokio::select! {
                Some(message) = self.rx_primaries.recv() => {
                    match message {
                        PrimaryMessage::Header(header, sync) => {
                            match self.sanitize_header(&header) {
                                Ok(()) => self.process_header(header, sync).await,
                                error => error
                            }

                        },
                        PrimaryMessage::Vote(vote) => {
                            match self.sanitize_vote(&vote) {
                                Ok(()) => {
                                    self.process_vote(vote, false).await
                                },
                                error => {
                                    error
                                }
                            }
                        },
                        PrimaryMessage::Certificate(certificate) => {
                            match self.sanitize_certificate(&certificate) {
                                Ok(()) => self.process_certificate(certificate).await,
                                error => {
                                    error
                                }
                            }
                        },
                        PrimaryMessage::Timeout(timeout) => self.handle_timeout(&timeout).await,
                        PrimaryMessage::TC(tc) => self.handle_tc(&tc).await,

                        PrimaryMessage::ConsensusMessage(consensus_message) => self.process_forwarded_message(consensus_message).await,
                        PrimaryMessage::ConsensusRequest(consensus_req) => self.process_consensus_request(consensus_req).await,
                        PrimaryMessage::ConsensusVote(consensus_vote) => self.process_consensus_vote(consensus_vote, false).await,
                        PrimaryMessage::ProposalHeaders(headers) => self.process_header_suffix(headers).await,
                        _ => panic!("Unexpected core message")
                    }
                },

                // Process locally proposed headers.
                Some(header) = self.rx_proposer.recv() => self.process_own_header(header).await,

                // Resume headers after their dependencies arrive.
                Some(header) = self.rx_header_waiter.recv() => {
                    debug!("normal loopback for header");
                    self.process_header(header, true).await
                },

                // Resume committed instances after their ancestors arrive.
                Some((consensus_message, header)) = self.rx_header_waiter_instances.recv() => self.process_loopback(consensus_message, header).await,

                Some(header_digest) = self.rx_request_header_sync.recv() => self.synchronizer.fetch_header(header_digest).await,

                // Process expired timers.
                Some((slot, view)) = self.timer_futures.next() => self.local_timeout_round(slot, view).await,

                Some(vote) = self.car_timer_futures.next() => self.process_vote(vote, true).await,

                // Process delayed fast-path votes.
                Some(vote) = self.fast_timer_futures.next() => self.process_consensus_vote(vote, true).await,

                Some((_slot, _view)) = self.async_timer_futures.next() => {
                    self.during_simulated_asynchrony = !self.during_simulated_asynchrony;

                    debug!("Time elapsed is {:?}", self.current_time.elapsed());
                    self.current_time = Instant::now();

                    if !self.during_simulated_asynchrony {
                        let async_start = Timer::new(0, 0, self.asynchrony_start);
                        let async_end = Timer::new(0, 0, self.asynchrony_start + self.asynchrony_duration);

                        self.async_timer_futures.push(Box::pin(async_start));
                        self.async_timer_futures.push(Box::pin(async_end));

                        if self.async_delayed_prepare.is_some() {
                            let last_prop = self.async_delayed_prepare.clone().unwrap();
                            let still_relevant = match &last_prop {
                                ConsensusMessage::Prepare {slot, view, tc: _, qc_ticket: _, proposals: _} => view == self.views.get(slot).unwrap_or(&0),
                                _ => false,
                            };
                            if still_relevant {
                                let _ = self.send_consensus_req(last_prop).await;
                            }
                            self.async_delayed_prepare = None;
                        }

                    }
                    Ok(())
                },

            };
            match result {
                Ok(()) => (),
                Err(DagError::StoreError(e)) => {
                    error!("{}", e);
                    panic!("Storage failure: killing node.");
                }
                Err(e @ DagError::HeaderTooOld(..)) => debug!("{}", e),
                Err(e @ DagError::VoteTooOld(..)) => debug!("{}", e),
                Err(e @ DagError::CertificateTooOld(..)) => debug!("{}", e),
                // A car vote that arrives after its lane advanced, and a vote for
                // a car whose PoA is already complete, are the ordinary outcome of
                // a WAN round trip longer than the header delay: at n=20 they were
                // 93% of the primary log.
                Err(e @ DagError::UnexpectedVote(..)) => debug!("{}", e),
                Err(e @ DagError::CarAlreadySatisfied) => debug!("{}", e),
                Err(e) => warn!("{}", e),
            }

            let round = self.consensus_round.load(Ordering::Relaxed);
            if round > self.gc_depth {
                let gc_round = round - self.gc_depth;
                self.last_voted.retain(|k, _| k >= &gc_round);
                self.cancel_handlers.retain(|k, _| k >= &gc_round);
                self.gc_round = gc_round;
                debug!("GC round moved to {}", self.gc_round);
            }
        }
    }
}

#[cfg(test)]
mod slot_gc_tests {
    use super::{
        allow_optimistic_leader_cut, autobahn_own_tip_is_admissible, keep_after_slot_period_gc,
        keep_all_to_all_delivery, latest_pipeline_tickets, parallel_timer_slot, parent_vote_state,
        record_car_vote, ParentVoteState,
    };
    use crate::messages::{Header, Proposal};
    use crypto::Digest;
    use std::collections::HashMap;

    #[test]
    fn all_to_all_delivery_outlives_the_pipeline_lane() {
        assert!(!keep_after_slot_period_gc(11, 15, 4));
        assert!(keep_all_to_all_delivery(11, 15, 50));
        assert!(keep_all_to_all_delivery(50, 100, 50));
        assert!(!keep_all_to_all_delivery(49, 100, 50));
    }

    #[test]
    fn parallel_prepare_is_the_next_slot_timer_ticket() {
        assert_eq!(parallel_timer_slot(15, 4), Some(16));
        assert_eq!(parallel_timer_slot(15, 1), None);
        assert_eq!(parallel_timer_slot(u64::MAX, 4), None);
    }

    #[test]
    fn commit_ticket_gc_keeps_the_latest_ticket_in_each_pipeline_lane() {
        let retained = latest_pipeline_tickets([1, 2, 5, 6, 7, 9, 50, 99, 100], 4);
        assert_eq!(retained.len(), 4);
        assert!(retained.contains(&9));
        assert!(retained.contains(&50));
        assert!(retained.contains(&100));
        assert!(retained.contains(&99));
    }

    #[test]
    fn car_votes_bind_the_exact_parent_branch() {
        let author = crate::common::keys()[0].0;
        let first = Header {
            author,
            height: 5,
            id: Digest([21; 32]),
            ..Header::default()
        };
        let conflicting = Header {
            id: Digest([22; 32]),
            ..first.clone()
        };
        let mut votes = HashMap::new();

        assert!(record_car_vote(&mut votes, &first));
        assert!(!record_car_vote(&mut votes, &conflicting));
        assert_eq!(parent_vote_state(&votes, &first), ParentVoteState::Exact);
        assert_eq!(
            parent_vote_state(&votes, &conflicting),
            ParentVoteState::Conflicting
        );
    }

    #[test]
    fn seamless_rejects_every_optimistic_tip_including_the_leaders() {
        let committee = crate::common::committee();
        let mut certificate = crate::common::certificate(&crate::common::header());
        certificate
            .votes
            .truncate(committee.validity_threshold() as usize);
        let lane = certificate.author;
        let leader = *committee
            .authorities
            .keys()
            .find(|author| **author != lane)
            .unwrap();
        let optimistic = Proposal {
            header_digest: Digest([99; 32]),
            height: certificate.height + 1,
            poa: Some(certificate),
            ..Default::default()
        };
        let mut cut = Header::genesis_proposals(&committee);
        cut.insert(lane, optimistic.clone());

        let verified = crate::verified::VerifiedCache::for_committee(&committee);
        assert!(!verified.cut_is_valid(&committee, false, &cut));
        assert!(verified.cut_is_valid(&committee, true, &cut));

        let mut leader_cut = Header::genesis_proposals(&committee);
        leader_cut.insert(
            leader,
            Proposal {
                poa: Some(crate::messages::Certificate::genesis_for(
                    leader, &committee,
                )),
                header_digest: Digest([100; 32]),
                height: 1,
                ..Default::default()
            },
        );
        assert!(!verified.cut_is_valid(&committee, false, &leader_cut));
        assert!(verified.cut_is_valid(&committee, true, &leader_cut));
    }

    #[test]
    fn no_winner_fallback_never_replaces_its_own_certified_lane_with_an_optimistic_tip() {
        let committee = crate::common::committee();
        let lane = *committee.authorities.keys().next().unwrap();
        let optimistic = Proposal {
            poa: Some(crate::messages::Certificate::genesis_for(lane, &committee)),
            header_digest: Digest([101; 32]),
            height: 1,
            ..Default::default()
        };

        assert!(autobahn_own_tip_is_admissible(
            &committee,
            &lane,
            true,
            &optimistic,
        ));
        assert!(!autobahn_own_tip_is_admissible(
            &committee,
            &lane,
            false,
            &optimistic,
        ));
        assert!(autobahn_own_tip_is_admissible(
            &committee,
            &lane,
            false,
            &Proposal::genesis(lane, &committee),
        ));
    }

    #[test]
    fn benchmark_byzantine_leader_uses_a_certified_cut_without_changing_honest_leaders() {
        assert!(allow_optimistic_leader_cut(true, false, false));
        assert!(!allow_optimistic_leader_cut(true, true, false));
        assert!(!allow_optimistic_leader_cut(true, false, true));
        assert!(!allow_optimistic_leader_cut(false, false, false));
    }
}

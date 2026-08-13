// Copyright(C) Facebook, Inc. and its affiliates.
use crate::aggregators::{QCMaker, TCMaker, VotesAggregator};
use crate::delayed_header::DelayedHeaderSender;
use crate::error::{DagError, DagResult};
use crate::leader::LeaderElector;
use crate::messages::{
    transform_commit_qc, verify_commit, verify_confirm, Certificate, CommitQC, ConsensusMessage,
    ConsensusRequest, ConsensusVote, Header, Proposal, Timeout, Vote, QC, TC,
};
use crate::primary::{Height, PrimaryMessage, Slot, View};
use crate::synchronizer::Synchronizer;
use crate::timer::{CarTimer, FastTimer, Timer};
use async_recursion::async_recursion;
use bytes::Bytes;
use config::{Committee, Stake};
use core::panic;
use crypto::{Digest, PublicKey, SignatureService};
use crypto::{Hash as _, Signature};
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt};
use log::{debug, error, warn};
use metrics::Metrics;
use network::{BatchConfig, CancelHandler, ReliableSender};
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

fn keep_after_slot_period_gc(candidate: Slot, committed: Slot, k: Slot) -> bool {
    debug_assert!(k > 0);
    candidate > committed || candidate % k != committed % k
}

pub struct Core {
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

    gc_round: Height,
    /// Authors voted for in each height.
    last_voted: HashMap<Height, HashSet<PublicKey>>,
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
    timer_futures: FuturesUnordered<SlotViewTimerFuture>,
    high_proposals: HashMap<Slot, ConsensusMessage>,
    /// Latest quorum certificate per slot.
    high_qcs: HashMap<Slot, ConsensusMessage>,
    qc_makers: HashMap<(Slot, Digest), QCMaker>,
    current_qcs_formed: usize,
    tc_makers: HashMap<(Slot, View), TCMaker>,
    prepare_tickets: VecDeque<ConsensusMessage>,
    already_proposed_slots: HashSet<Slot>,
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
    car_timer_futures: FuturesUnordered<Pin<Box<dyn Future<Output = Vote> + Send>>>,
    fast_timer_futures: FuturesUnordered<Pin<Box<dyn Future<Output = ConsensusVote> + Send>>>,

    /// Whether replicas broadcast votes and assemble certificates locally.
    all_to_all: bool,
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
        all_to_all: bool,

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
        retry_backoff_max_ms: u64,
    ) {
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
                    .with_latency(latency_map)
                    .with_metrics(metrics)
                    .with_batching(batch)
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
                high_qcs: HashMap::with_capacity(2 * gc_depth as usize),
                high_proposals: HashMap::with_capacity(2 * gc_depth as usize),
                qc_makers: HashMap::with_capacity(2 * gc_depth as usize),
                tc_makers: HashMap::with_capacity(2 * gc_depth as usize),
                prepare_tickets: VecDeque::with_capacity(2 * gc_depth as usize),
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
                car_timer_futures: FuturesUnordered::new(),
                fast_timer_futures: FuturesUnordered::new(),
                all_to_all,
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

        self.current_header = header.clone();
        self.sent_cert_to_proposer = false;
        self.votes_aggregator = VotesAggregator::new();

        match self.use_optimistic_tips {
            // Include the leader tip in the coverage check.
            true => self.current_proposal_tips.insert(
                header.origin(),
                Proposal {
                    header_digest: header.digest(),
                    height: header.height(),
                },
            ),
            false => self.current_certified_tips.insert(
                header.origin(),
                Proposal {
                    header_digest: header.digest(),
                    height: header.height(),
                },
            ),
        };

        for consensus in header.consensus_messages.values_mut() {
            self.set_consensus_proposal(consensus);
        }

        for (dig, consensus) in &header.consensus_messages {
            match consensus {
                ConsensusMessage::Prepare {
                    slot,
                    view: _,
                    tc: _,
                    qc_ticket: _,
                    proposals: _,
                } => {
                    self.consensus_instances
                        .insert((*slot, dig.clone()), consensus.clone());
                }
                ConsensusMessage::Confirm {
                    slot,
                    view: _,
                    qc: _,
                    proposals: _,
                } => {
                    self.consensus_instances
                        .insert((*slot, dig.clone()), consensus.clone());
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

    #[async_recursion]
    async fn process_header(&mut self, header: Header, sync: bool) -> DagResult<()> {
        debug!("Processing Header:  {:?}", header);
        debug!("Processing the header with height {:?}", header.height);

        // The parent certificate must precede this header and meet quorum.
        let stake: Stake = header
            .parent_cert
            .votes
            .iter()
            .map(|(pk, _)| self.committee.stake(pk))
            .sum();
        debug!("Past header parent cert stake check");
        ensure!(
            header.parent_cert.height() + 1 == header.height(),
            DagError::MalformedHeader(header.id.clone())
        );
        debug!("Past header parent cert height check");

        ensure!(
            stake >= self.committee.validity_threshold() || header.parent_cert.height() == 0,
            DagError::HeaderRequiresQuorum(header.id.clone())
        );
        debug!("Past header parent cert stake check");

        // Retry the header after its payload arrives.
        if self.synchronizer.missing_payload(&header, sync).await? {
            debug!("Processing of {} suspended: missing payload", header);
            return Ok(());
        }

        // Retry the header after its parent arrives.
        if self
            .synchronizer
            .get_parent_header(&header)
            .await?
            .is_none()
        {
            debug!("The parent is missing, suspending processing");
            return Ok(());
        }

        // Wait until every embedded consensus message is ready.
        if !self.is_consensus_ready(&header).await {
            debug!("Can't vote for prepare, need to sync on missing tips, suspending processing");
            return Ok(());
        }

        debug!("storing the header");

        // Store the header since we have the parents (recursively).
        let bytes = bincode::serialize(&header).expect("Failed to serialize header");
        self.store.write(header.digest().to_vec(), bytes).await;

        // Update local tips and proposals for a higher header.
        if self.use_optimistic_tips
            && header.height()
                > self
                    .current_proposal_tips
                    .get(&header.origin())
                    .unwrap()
                    .height
        {
            self.current_proposal_tips.insert(
                header.origin(),
                Proposal {
                    header_digest: header.digest(),
                    height: header.height(),
                },
            );
            debug!("updating tip");

            // Recheck pending tickets after receiving a tip.
            self.try_prepare_waiting_slots().await?;
        }

        if !self.use_optimistic_tips
            && header.parent_cert.height
                > self
                    .current_certified_tips
                    .get(&header.origin())
                    .unwrap()
                    .height
        {
            self.current_certified_tips.insert(
                header.origin(),
                Proposal {
                    header_digest: header.parent_cert.header_digest.clone(),
                    height: header.parent_cert.height,
                },
            );
            debug!("updating tip");

            // Recheck pending tickets after receiving a tip.
            self.try_prepare_waiting_slots().await?;
        }

        debug!("after tip height check");

        self.process_certificate(header.clone().parent_cert).await?;

        // Pure dissemination headers require only the selected quorum.
        if header.consensus_messages.is_empty() && !self.check_cast_vote(&header) {
            return Ok(());
        }

        if self
            .last_voted
            .entry(header.height())
            .or_default()
            .insert(header.author)
        {
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

    fn check_cast_vote(&self, header: &Header) -> bool {
        // Select 2f+1 voters following the header author in committee order.
        let mut start = false;
        let mut count = 1;

        let mut iter = self.committee.authorities.iter();

        // Find the author's position.
        while count < self.committee.validity_threshold() {
            let x = iter.next();
            if x.is_none() {
                iter = self.committee.authorities.iter();
                continue;
            }
            let (id, _) = x.unwrap();
            if header.author.eq(id) {
                start = true;
                continue;
            }
            if start {
                if self.name.eq(id) {
                    debug!("DO NOT CAST VOTE for header: {}", header.id);
                    return false;
                }
                count += 1;
            }
        }
        debug!("CAST VOTE for header: {}", header.id);
        true
    }

    #[async_recursion]
    async fn process_vote(&mut self, vote: Vote, is_loopback: bool) -> DagResult<()> {
        debug!("Processing Vote {:?}", vote);

        let consensus_loopback = is_loopback && !vote.consensus_votes.is_empty();

        // Process only current-header votes and consensus loopbacks.
        if vote.id != self.current_header.id || consensus_loopback {
            return Ok(());
        }

        let num_active_consensus_messages = self.current_header.num_active_instances;
        debug!("num active instances {:?}", num_active_consensus_messages);

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
                sig.verify(&current_instance.digest(), &vote.author)?;
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
                        signature: Signature::default(),
                        consensus_votes: vec![(*slot, digest.clone(), Signature::default())],
                    };
                    let fast_timer = CarTimer::new(t_vote, self.fast_path_timeout);
                    self.car_timer_futures.push(Box::pin(fast_timer));
                } else if let Some(qc) = qc_opt {
                    self.current_qcs_formed += 1;

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

        let vote_id = vote.id.clone();
        let car_timeout = is_loopback && vote.consensus_votes.is_empty();

        // Add the vote to the header certificate.
        let (car_cert_ready, first) =
            self.votes_aggregator
                .append(vote, &self.committee, &self.current_header)?;

        // A timeout stops waiting for embedded consensus.
        let consensus_ready = consensus_ready || car_timeout;
        let dissemination_cert = match car_cert_ready && consensus_ready {
            true => self.votes_aggregator.get()?,
            false => None,
        };

        let dissemination_ready: bool = car_cert_ready && dissemination_cert.is_some();

        debug!(
            "sentToProposer {:?}, diss_ready {:?}, consensus_ready {:?}",
            self.sent_cert_to_proposer, dissemination_ready, consensus_ready
        );

        // Start one timeout while the dissemination certificate waits for consensus.
        if dissemination_ready && !consensus_ready && first {
            let t_vote = Vote {
                id: vote_id,
                height: 0,
                origin: PublicKey::default(),
                author: PublicKey::default(),
                signature: Signature::default(),
                consensus_votes: vec![],
            };
            let fast_timer = CarTimer::new(t_vote, self.fast_path_timeout);
            self.car_timer_futures.push(Box::pin(fast_timer));
        }

        if !self.sent_cert_to_proposer && (dissemination_ready && consensus_ready) {
            self.tx_proposer
                .send(dissemination_cert.unwrap())
                .await
                .expect("Failed to send certificate");

            self.sent_cert_to_proposer = true;
            self.current_qcs_formed = 0;
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
                // Membership bounds the buffered author set.
                if self.committee.stake(&vote.author) == 0 {
                    debug!(
                        "dropping all-to-all pending vote from unknown authority {}",
                        vote.author
                    );
                    return Ok(());
                }
                if vote.author != self.name {
                    vote.sig.verify(&vote.digest, &vote.author)?;
                }
                if self.is_sane_pending_vote_slot(vote.slot) {
                    self.buffer_pending_consensus_vote(vote);
                }
                return Ok(());
            }
            debug!("consensus instance slot has committed, skip processing vote");
            return Ok(());
        }

        if !is_loopback && vote.author != self.name {
            vote.sig.verify(&vote.digest, &vote.author)?;
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

        let sig = self
            .signature_service
            .request_signature(confirm_digest.clone())
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
        slot > self.last_committed_slot.saturating_sub(self.k)
            && slot <= self.last_committed_slot + self.k + 1
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

    fn set_consensus_proposal(&mut self, consensus_message: &mut ConsensusMessage) {
        let header = &self.current_header;
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
                *proposals = match self.use_optimistic_tips {
                    true => self.current_proposal_tips.clone(),
                    false => self.current_certified_tips.clone(),
                };

                proposals.insert(
                    self.name,
                    Proposal {
                        header_digest: header.id.clone(),
                        height: header.height,
                    },
                );

                // Seamless cuts contain only certified peer tips.
                if !self.use_optimistic_tips {
                    debug_assert!(
                        proposals.iter().all(|(pk, proposal)| {
                            *pk == self.name
                                || self.current_certified_tips.get(pk)
                                    .is_some_and(|certified| proposal.height <= certified.height)
                        }),
                        "seamless invariant violated: a cut proposal exceeds its author's last certified height"
                    );
                }

                for proposal in proposals.values_mut() {
                    debug!("new proposal height is {:?}", proposal.height);
                }
            }
        }
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
                let next_leader = self.leader_elector.get_leader(slot + 1, 1);

                if self.name != next_leader {
                    return Ok(());
                }

                // Ignore tickets for a slot already proposed locally.
                if self.already_proposed_slots.contains(&(slot + 1)) {
                    return Ok(());
                }

                // Wait for slot s-k before opening another instance.
                if *slot + 1 > self.k {
                    debug!("beyond init k");
                    if !self.committed_slots.contains_key(&(slot + 1 - self.k)) {
                        debug!("too many instances open");
                        self.prepare_tickets.push_back(prepare_message.clone());
                        return Ok(());
                    }
                }

                // Open the next slot after its proposals have enough coverage.
                if self.enough_coverage(proposals) {
                    debug!("have enough coverage to start slot {}", slot + 1);

                    let qc_ticket = match *slot + 1 > self.k {
                        true => Some(
                            self.committed_slots
                                .get(&(slot + 1 - self.k))
                                .unwrap()
                                .clone(),
                        ),
                        false => None,
                    };

                    let new_prepare_instance = ConsensusMessage::Prepare {
                        slot: slot + 1,
                        view: 1,
                        tc: None,
                        qc_ticket,
                        proposals: HashMap::new(),
                    };

                    self.already_proposed_slots.insert(slot + 1);

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

    #[async_recursion]
    async fn is_valid(&mut self, consensus_message: &ConsensusMessage) -> bool {
        match consensus_message {
            ConsensusMessage::Prepare {
                slot,
                view,
                tc,
                qc_ticket,
                proposals,
            } => {
                // View 1 uses a QC ticket; later views require the previous TC.
                let mut ticket_valid: bool = true;
                match tc {
                    Some(tc) => {
                        if tc.view + 1 != *view {
                            return false;
                        }
                        ticket_valid = tc.verify(&self.committee).is_ok();

                        let winning_proposals = tc.get_winning_proposals(&self.committee);
                        if !winning_proposals.is_empty() {
                            for (pk, proposal) in proposals {
                                ticket_valid =
                                    ticket_valid && proposal.eq(winning_proposals.get(pk).unwrap());
                            }
                        }
                    }
                    None => {
                        if !self.use_parallel_proposals {
                            panic!("Parallel proposals should be true");
                        }
                        if *slot > self.k {
                            debug!("Checking QC Ticket");
                            if !self.committed_slots.contains_key(&(slot - self.k)) {
                                debug!("Verify QC Ticket");
                                let commit_qc = qc_ticket.as_ref().unwrap();
                                let commit_message = transform_commit_qc(commit_qc.clone());
                                if commit_qc.slot + self.k != *slot {
                                    return false;
                                }
                                ticket_valid = self.is_valid(&commit_message).await;
                                debug!("Verify QC Ticket: {}", ticket_valid);
                            }
                        }
                        ticket_valid = ticket_valid && *view == 1;
                    }
                };

                let curr_view = self.views.get(slot).unwrap_or(&0);
                if curr_view < view {
                    self.views.insert(*slot, *view);
                }

                !self.last_voted_consensus.contains(&(*slot, *view))
                    && ticket_valid
                    && self.views.get(slot).unwrap() == view
            }
            ConsensusMessage::Confirm {
                slot,
                view,
                qc: _,
                proposals: _,
            } => {
                debug!("try to unwrap slot");

                let curr_view = self.views.get(slot).unwrap_or(&0);
                if curr_view <= view && verify_confirm(consensus_message, &self.committee) {
                    self.views.insert(*slot, *view);
                    return true;
                }
                false
            }
            ConsensusMessage::Commit {
                slot: _,
                view: _,
                qc: _,
                proposals: _,
            } => verify_commit(consensus_message, &self.committee),
        }
    }

    async fn is_consensus_ready(&mut self, header: &Header) -> bool {
        let mut is_ready = true;
        for consensus_message in header.consensus_messages.values() {
            match consensus_message {
                ConsensusMessage::Prepare {
                    slot: _,
                    view: _,
                    tc: _,
                    qc_ticket: _,
                    proposals: _,
                } => {
                    // Prepare proposals must be available before voting.
                    is_ready = is_ready
                        && !self
                            .synchronizer
                            .get_proposals(consensus_message, header)
                            .await
                            .unwrap()
                            .is_empty();
                }
                ConsensusMessage::Commit {
                    slot: _,
                    view: _,
                    qc: _,
                    proposals: _,
                } => {}
                _ => {}
            };
        }
        is_ready
    }

    #[async_recursion]
    async fn process_consensus_messages(
        &mut self,
        header: &Header,
    ) -> DagResult<Vec<(Slot, Digest, Signature)>> {
        let mut consensus_votes: Vec<(Slot, Digest, Signature)> = Vec::new();

        for consensus_message in header.consensus_messages.values() {
            debug!("processing instance");
            if self.is_valid(consensus_message).await {
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
                        self.process_prepare_message(consensus_message, consensus_votes.as_mut())
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
                            .get_proposals(consensus_message, header)
                            .await?;
                        self.process_confirm_message(consensus_message, consensus_votes.as_mut())
                            .await;
                    }
                    ConsensusMessage::Commit {
                        slot,
                        view: _,
                        qc: _,
                        proposals: _,
                    } => {
                        debug!("processing commit in slot {:?}", slot);
                        self.process_commit_message(consensus_message.clone(), header)
                            .await?;
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
        let dig = consensus_message.digest();
        match &consensus_message {
            ConsensusMessage::Prepare {
                slot,
                view: _,
                tc: _,
                qc_ticket: _,
                proposals: _,
            } => {
                self.consensus_instances
                    .insert((*slot, dig.clone()), consensus_message.clone());
                // Process votes that arrived before this Prepare.
                if self.all_to_all {
                    self.drain_pending_consensus_votes(*slot, dig.clone()).await;
                }
            }
            ConsensusMessage::Confirm {
                slot,
                view: _,
                qc: _,
                proposals: _,
            } => {
                self.consensus_instances
                    .insert((*slot, dig.clone()), consensus_message.clone());
                if self.all_to_all {
                    self.drain_pending_consensus_votes(*slot, dig.clone()).await;
                }
            }
            _ => {}
        };

        debug!("try to verify");
        let mut valid = true;
        if consensus_req.author != self.name {
            consensus_req.verify(&self.committee)?;
            debug!("check validity");
            valid = self.is_valid(consensus_message).await;
        }

        if !valid {
            return Ok(());
        }

        self.process_consensus_message(consensus_req.message, consensus_req.author)
            .await
    }

    async fn process_consensus_message(
        &mut self,
        consensus_message: ConsensusMessage,
        author: PublicKey,
    ) -> DagResult<()> {
        let mut consensus_votes: Vec<(Slot, Digest, Signature)> = Vec::new();

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
                if self
                    .synchronizer
                    .get_proposals(&consensus_message, &header)
                    .await
                    .unwrap()
                    .is_empty()
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
        consensus_sigs: &mut Vec<(Slot, Digest, Signature)>,
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

            if self.k > 1
                && !self.committed_slots.contains_key(&(slot + 1))
                && !self.timers.contains(&(slot + 1, 1))
                && self.committed_slots.contains_key(&(slot + 1 - self.k))
            {
                debug!("start timer for slot {}", slot + 1);
                let timer = Timer::new(slot + 1, 1, self.timeout_delay);
                self.timer_futures.push(Box::pin(timer));
                self.timers.insert((slot + 1, 1));
            }

            for proposal in proposals.values() {
                debug!(
                    "prepare slot {:?}, proposal height {:?}",
                    slot, proposal.height
                );
            }
            debug!("prepare vote in slot {:?}", slot);

            self.last_voted_consensus.insert((*slot, *view));

            if self.use_fast_path {
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
            }

            let sig = self
                .signature_service
                .request_signature(prepare_message.digest())
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
        consensus_sigs: &mut Vec<(Slot, Digest, Signature)>,
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

            let sig = self
                .signature_service
                .request_signature(confirm_message.digest())
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
            if self.simulate_asynchrony && *slot == 1 {
                debug!("added async timers");
                let async_start = Timer::new(0, 0, self.asynchrony_start);
                let async_end = Timer::new(0, 0, self.asynchrony_start + self.asynchrony_duration);
                self.async_timer_futures.push(Box::pin(async_start));
                self.async_timer_futures.push(Box::pin(async_end));
            }

            self.timers.remove(&(*slot, *view));

            let sl = *slot;
            self.last_committed_slot = max(sl, self.last_committed_slot);
            self.committed_slots.insert(
                sl,
                CommitQC::new(*slot, *view, qc.clone(), proposals.clone()).await,
            );

            if self.k == 1 {
                if !self.timers.contains(&(slot + self.k, 1)) {
                    debug!("start timer for slot {}", slot + 1);
                    let timer = Timer::new(slot + self.k, 1, self.timeout_delay);
                    self.timer_futures.push(Box::pin(timer));
                    self.timers.insert((slot + self.k, 1));
                }
            } else {
                if !self.timers.contains(&(slot + self.k, 1))
                    && self.views.contains_key(&(slot + self.k - 1))
                {
                    debug!("start timer for slot {}", slot + 1);
                    let timer = Timer::new(slot + self.k, 1, self.timeout_delay);
                    self.timer_futures.push(Box::pin(timer));
                    self.timers.insert((slot + self.k, 1));
                }
            }

            // Defer output until every proposal ancestor is available.
            if !self
                .synchronizer
                .get_proposals(&commit_message, header)
                .await
                .unwrap()
                .is_empty()
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

        self.consensus_instances
            .retain(|(s, _), _| keep_after_slot_period_gc(*s, slot, k));
        self.consensus_cancel_handlers
            .retain(|s, _| keep_after_slot_period_gc(*s, slot, k));

        self.qc_makers
            .retain(|(s, _), _| keep_after_slot_period_gc(*s, slot, k));

        self.pending_consensus_votes
            .retain(|s, _| keep_after_slot_period_gc(*s, slot, k));
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
                slot,
                view,
                tc: _,
                qc_ticket: _,
                proposals: _,
            } => {
                if self.use_ride_share {
                    self.process_header(header, false).await?;
                } else {
                    if self.last_voted_consensus.contains(&(*slot, *view)) {
                        return Ok(());
                    }
                    self.process_consensus_message(consensus_message, header.author)
                        .await?
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
                self.tx_committer
                    .send(consensus_message)
                    .await
                    .expect("Failed to send to committer");
            }
        };
        Ok(())
    }

    async fn process_forwarded_message(
        &mut self,
        consensus_message: ConsensusMessage,
    ) -> DagResult<()> {
        match &consensus_message {
            ConsensusMessage::Prepare {
                slot: _,
                view: _,
                tc: _,
                qc_ticket: _,
                proposals: _,
            } => {
                self.is_prepare_ticket_ready(&consensus_message).await?;
            }
            ConsensusMessage::Commit {
                slot: _,
                view: _,
                qc: _,
                proposals: _,
            } => {
                let header = self.current_header.clone();
                self.process_commit_message(consensus_message.clone(), &header)
                    .await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn local_timeout_round(&mut self, slot: Slot, view: View) -> DagResult<()> {
        warn!("Timeout reached for slot {}, view {}", slot, view);

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

        if let Some(ConsensusMessage::Commit {
            slot: _,
            view: _,
            qc: _,
            proposals: _,
        }) = self.high_qcs.get(&slot)
        {
            return Ok(());
        }

        debug!("Sending Timeout for slot {}, view {}", slot, view);
        let timeout = Timeout::new(
            slot,
            view,
            self.high_qcs.get(&slot).cloned(),
            self.high_proposals.get(&slot).cloned(),
            self.name,
            self.signature_service.clone(),
        )
        .await;
        debug!("Created Timeout: {:?}", timeout);

        debug!("Broadcasting Timeout: {:?}", timeout);
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

        self.handle_timeout(&timeout).await
    }

    async fn handle_timeout(&mut self, timeout: &Timeout) -> DagResult<()> {
        debug!("Processing timeout {:?}", timeout);

        if let Some(view) = self.views.get(&timeout.slot) {
            if timeout.view < *view {
                return Ok(());
            }
        };

        if self.committed_slots.contains_key(&timeout.slot) {
            return Ok(());
        }

        timeout.verify(&self.committee)?;

        self.tc_makers
            .entry((timeout.slot, timeout.view))
            .or_insert_with(TCMaker::new);

        let tc_maker = self
            .tc_makers
            .get_mut(&(timeout.slot, timeout.view))
            .unwrap();

        if let Some(tc) = tc_maker.append(timeout.clone(), &self.committee)? {
            debug!("Assembled TimeoutCertificate {:?}", tc);

            self.views.insert(timeout.slot, timeout.view + 1);

            let timer = Timer::new(tc.slot, tc.view + 1, self.timeout_delay);
            self.timer_futures.push(Box::pin(timer));
            self.timers.insert((tc.slot, tc.view + 1));

            self.generate_prepare_from_tc(&tc).await?;
        }
        Ok(())
    }

    async fn generate_prepare_from_tc(&mut self, tc: &TC) -> DagResult<()> {
        if self.name == self.leader_elector.get_leader(tc.slot, tc.view + 1) {
            debug!("IsLeader. Start prepare from TC");
            let winning_proposals = tc.get_winning_proposals(&self.committee);

            debug!("winning proposals: {:?}", winning_proposals);

            let prepare_message: ConsensusMessage = ConsensusMessage::Prepare {
                slot: tc.slot,
                view: tc.view + 1,
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
        self.generate_prepare_from_tc(tc).await?;

        Ok(())
    }

    fn sanitize_header(&mut self, header: &Header) -> DagResult<()> {
        ensure!(
            self.gc_round <= header.height,
            DagError::HeaderTooOld(header.id.clone(), header.height)
        );

        header.verify(&self.committee)?;
        Ok(())
    }

    fn sanitize_vote(&mut self, vote: &Vote) -> DagResult<()> {
        // A completed header vote still accepts consensus votes carried by the message.
        if self.current_header.id.eq(&vote.id) && self.votes_aggregator.complete {
            if vote.consensus_votes.is_empty() {
                return Err(DagError::CarAlreadySatisfied);
            } else {
                return Ok(());
            }
        }

        vote.verify(&self.committee)
    }

    fn sanitize_certificate(&mut self, certificate: &Certificate) -> DagResult<()> {
        ensure!(
            self.gc_round <= certificate.height(),
            DagError::CertificateTooOld(certificate.digest(), certificate.height())
        );
        certificate.verify(&self.committee)
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

#![allow(dead_code)]
#![allow(unused_variables)]
// Copyright(C) Facebook, Inc. and its affiliates.
use crate::aggregators::{QCMaker, TCMaker, VotesAggregator};
//use crate::common::special_header;
use crate::error::{DagError, DagResult};
use crate::leader::LeaderElector;
use crate::messages::{
    Certificate, ConsensusMessage, Header, Proposal, Timeout, Vote, TC, ConsensusType, QC, verify_confirm, verify_commit, CommitQC, transform_commitQC, ConsensusRequest, ConsensusVote,
};
use crate::primary::{Height, PrimaryMessage, Slot, View};
use crate::synchronizer::{Synchronizer, self};
use crate::timer::{Timer, CarTimer, FastTimer};
use async_recursion::async_recursion;
use bytes::Bytes;
use config::{Committee, Stake};
use crypto::{Digest, PublicKey, SignatureService};
use crypto::{Hash as _, Signature};
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt};
use log::{debug, error, warn};
use network::{CancelHandler, ReliableSender};
use core::panic;
use std::borrow::BorrowMut;
//use tokio::time::error::Elapsed;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
//use std::task::Poll;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use std::cmp::max;
//use tokio::time::{sleep, Duration, Instant};

//use crate::messages_consensus::{QC, TC};
#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

pub struct Core {
    /// The public key of this primary.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Handles synchronization with other nodes and our workers.
    synchronizer: Synchronizer,
    /// Service to sign headers.
    signature_service: SignatureService,
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// The depth of the garbage collector.
    gc_depth: Height,

    /// Receiver for dag messages (headers, votes, certificates).
    rx_primaries: Receiver<PrimaryMessage>,
    /// Receives loopback headers from the `HeaderWaiter`.
    rx_header_waiter: Receiver<Header>,
    /// Receives loopback instances from the 'HeaderWaiter'
    rx_header_waiter_instances: Receiver<(ConsensusMessage, Header)>,
    /// Receives our newly created headers from the `Proposer`.
    rx_proposer: Receiver<Header>,
    // Output all certificates to the consensus Dag view
    tx_committer: Sender<ConsensusMessage>,

    /// Send a valid parent certificate to the `Proposer` 
    tx_proposer: Sender<Certificate>,
    // Receive sync requests for headers required at the consensus layer
    rx_request_header_sync: Receiver<Digest>,

    /// The last garbage collected round.
    gc_round: Height,

    /// The authors of the last voted headers. (Ensures only voting for one header per round)
    last_voted: HashMap<Height, HashSet<PublicKey>>,
    /// The last header we proposed (for which we are waiting votes).
    current_header: Header,
    // Whether we have already sent certificate to proposer
    sent_cert_to_proposer: bool,

    // /// Aggregates votes into a certificate.
    votes_aggregator: VotesAggregator,

    network: ReliableSender,
    /// Keeps the cancel handlers of the messages we sent.
    cancel_handlers: HashMap<Height, Vec<CancelHandler>>,
    consensus_cancel_handlers: HashMap<Slot, Vec<CancelHandler>>,

    current_proposal_tips: HashMap<PublicKey, Proposal>,
    current_certified_tips: HashMap<PublicKey, Proposal>,

    consensus_instances: HashMap<(Slot, Digest), ConsensusMessage>,
    views: HashMap<Slot, View>,
    timers: HashSet<(Slot, View)>,
    last_voted_consensus: HashSet<(Slot, View)>,
    timer_futures: FuturesUnordered<Pin<Box<dyn Future<Output = (Slot, View)> + Send>>>,
    // TODO: Add garbage collection, related to how deep pipeline (parameter k)
    high_proposals: HashMap<Slot, ConsensusMessage>,
    high_qcs: HashMap<Slot, ConsensusMessage>, // NOTE: Store the latest QC for each slot
    qc_makers: HashMap<(Slot, Digest), QCMaker>,
    // pqc_makers: HashMap<(Slot, View), QCMaker>,
    // cqc_makers: HashMap<(Slot, View), QCMaker>,
    current_qcs_formed: usize,
    tc_makers: HashMap<(Slot, View), TCMaker>,
    prepare_tickets: VecDeque<ConsensusMessage>,
    already_proposed_slots: HashSet<Slot>,
    tx_info: Sender<ConsensusMessage>,
    leader_elector: LeaderElector,
    timeout_delay: u64,
    // GC the vote aggregators and current headers
    // gc_map: HashMap<Round, Digest>,
  
    committed_slots: HashMap<Slot, CommitQC>,
    last_committed_slot: u64, 
    //TODO: if we are not enforcing a ticket, then only start when we committed all instances < s-k.
    // If we just check that s-k is committed, but all it's predecessors are not, then we may still open an arbitrary number of instances in the absolute worst case
                                                                                // E.g. s-1 has not committed, but s has, so we can open s+k 

    //Configuration options: //TODO: Move to Primary level -> make configurable from main.rs
    use_fast_path: bool,           //default = false
    use_optimistic_tips: bool,     //default = true (TODO: implement non optimistic tip option)
    use_parallel_proposals: bool,  //default = true (TODO: implement sequential slot option)
    k: u64, //limit k on number of open honest instances (k+f instances can be open) => if require QC, then hard limit to k.
    fast_path_timeout: u64,

    use_ride_share: bool,
    car_timeout: u64,
    car_timer_futures: FuturesUnordered<Pin<Box<dyn Future<Output = Vote> + Send>>>,
    fast_timer_futures: FuturesUnordered<Pin<Box<dyn Future<Output = ConsensusVote> + Send>>>, // Use this one for Fast Path on external Consensus case

    //asynchrony simulation,
    simulate_asynchrony: bool,
    asynchrony_start: u64,
    asynchrony_duration: u64,
    during_simulated_asynchrony: bool,
    async_timer_futures: FuturesUnordered<Pin<Box<dyn Future<Output = (Slot, View)> + Send>>>,
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

        simulate_asynchrony: bool,
        asynchrony_start: u64,
        asynchrony_duration: u64,

        // PHASE7-PREP-NOTES.md (WAN-shaped local runs, optional item): this node's own
        // per-destination artificial latency map, already resolved by the caller
        // (`Primary::spawn`, via `Committee::latency_map`) -- empty (== current
        // behavior, byte-identical) unless `--latency-table`/`--mimic-latency-ms` was
        // passed. Resolved by the caller rather than here since `Core::spawn` (unlike
        // `vantage::node::VantageCore::spawn`) doesn't otherwise take a `Parameters`.
        latency_map: HashMap<SocketAddr, Duration>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                //current_header: Header::genesis(&committee),
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
                network: ReliableSender::new().with_latency(latency_map),
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
                // pqc_makers: HashMap::with_capacity(2 * gc_depth as usize),
                // cqc_makers: HashMap::with_capacity(2 * gc_depth as usize),
                tc_makers: HashMap::with_capacity(2 * gc_depth as usize),
                prepare_tickets: VecDeque::with_capacity(2 * gc_depth as usize),
                timeout_delay,
                timer_futures: FuturesUnordered::new(),
                //gc_map: HashMap::with_capacity(2 * gc_depth as usize),
                
                committed_slots: HashMap::with_capacity(2 * gc_depth as usize),
                last_committed_slot: 0,
                
                use_fast_path,           //default = true
                use_optimistic_tips,     //default = true (TODO: implement non optimistic tip option)
                use_parallel_proposals,    //default = true (TODO: implement sequential slot option)
                k,
                fast_path_timeout,
                use_ride_share,
                car_timeout,
                car_timer_futures: FuturesUnordered::new(),
                fast_timer_futures: FuturesUnordered::new(),

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
        //println!("Received own header");
        debug!("Processing own header with {:?} consensus messages", header.consensus_messages.len());
        // for (dig, consensus) in &header.consensus_messages {
        //     match consensus { //TODO: Re-factor ConsensusMessages to all have slot/view, option for TC/QC, and a type.
        //         ConsensusMessage::Prepare {slot, view, tc: _, qc_ticket: _, proposals: _, } => {debug!("Prepare instance for slot {}", slot);},  
        //         ConsensusMessage::Confirm {slot, view, qc: _, proposals: _, } => {debug!("Confirm instance for slot {}", slot);},  
        //         ConsensusMessage::Commit {slot, view, qc: _, proposals: _, } => {debug!("Commit instance for slot {}", slot);},  
        //     };
        // }

        //GC all obsolete qc_makers //WARNING: FIXME: Can only do this here if Votes are piggybacked on cars (i.e. not external and never delayed)
        //self.qc_makers.clear();

        // Update the current header we are collecting votes for
        self.current_header = header.clone();
        // Indicate that we haven't sent a cert yet for this header
        self.sent_cert_to_proposer = false;

        // Reset the votes aggregator.
        self.votes_aggregator = VotesAggregator::new();

        match self.use_optimistic_tips { //Add early here, so that enough coverage will include leader tip.
            true => self.current_proposal_tips.insert(header.origin(), Proposal {header_digest: header.digest(), height: header.height(),}),
            false => self.current_certified_tips.insert(header.origin(), Proposal {header_digest: header.digest(), height: header.height(),}),
        };

        // Augment consensus messages with latest prepares
        for consensus in header.consensus_messages.values_mut() {
            self.set_consensus_proposal(consensus);
        }


        //Set all consensus instances
        for (dig, consensus) in &header.consensus_messages {
            match consensus { //TODO: Re-factor ConsensusMessages to all have slot/view, option for TC/QC, and a type.
                ConsensusMessage::Prepare {slot, view, tc: _, qc_ticket: _, proposals: _, } => {self.consensus_instances.insert((*slot, dig.clone()), consensus.clone());},  
                ConsensusMessage::Confirm {slot, view, qc: _, proposals: _, } => {self.consensus_instances.insert((*slot, dig.clone()), consensus.clone());},  
                _ => {},
            };
            //self.consensus_instances.insert(dig.clone(), consensus.clone());
        }

        // Broadcast the new header in a reliable manner.
        let addresses = self
            .committee
            .others_primaries(&self.name)
            .iter()
            .map(|(_, x)| x.primary_to_primary)
            .collect();
        let bytes = bincode::serialize(&PrimaryMessage::Header(header.clone(), false))
            .expect("Failed to serialize our own header");
        let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
        self.cancel_handlers
            .entry(header.height)
            .or_insert_with(Vec::new)
            .extend(handlers);

        // Process the header.
        self.process_header(header, false).await
    }

    #[async_recursion]
    async fn process_header(&mut self, header: Header, sync: bool) -> DagResult<()> {
        debug!("Processing Header:  {:?}", header);
        debug!("Processing the header with height {:?}", header.height);

        // Check the parent certificate. Ensure the certificate contains a quorum of votes and is
        // at the preivous height
        let stake: Stake = header
            .parent_cert
            .votes
            .iter()
            .map(|(pk, _)| self.committee.stake(pk))
            .sum();
        //println!("Before first ensure");
        debug!("Past header parent cert stake check");
        ensure!(
            header.parent_cert.height() + 1 == header.height(),
            DagError::MalformedHeader(header.id.clone())
        );
        debug!("Past header parent cert height check");

        //println!("Before second ensure");
        ensure!(
            stake >= self.committee.validity_threshold() || header.parent_cert.height() == 0,
            DagError::HeaderRequiresQuorum(header.id.clone())
        );
        debug!("Past header parent cert stake check");
        //println!("After second ensure");

        // Ensure we have the payload. If we don't, the synchronizer will ask our workers to get it, and then
        // reschedule processing of this header once we have it.
        if self.synchronizer.missing_payload(&header, sync).await? {
            //println!("Missing payload");
            debug!("Processing of {} suspended: missing payload", header);
            return Ok(());
        }

        // By FIFO should have parent of this header (and recursively all ancestors), reschedule for processing if we don't
        if self
            .synchronizer
            .get_parent_header(&header)
            .await?
            .is_none()
        {
            //println!("The parent is missing");
            debug!("The parent is missing, suspending processing");
            return Ok(());
        }


        


        // Check whether we can seamlessly vote for all consensus messages, if not reschedule
        if !self.is_consensus_ready(&header).await {
            // TODO: Keep track of stats of sync
            // NOTE: This blocks if prepare tips are not available, the leader of the prepare takes
            // on the responsibility of possible blocking i.e. its lane won't continue
            // TODO: Use reputation
            //println!("Need to sync on missing tips, reschedule");
            debug!("Can't vote for prepare, need to sync on missing tips, suspending processing");
            return Ok(());
        }

        //println!("storing the header");
        debug!("storing the header");

        // Store the header since we have the parents (recursively).
        let bytes = bincode::serialize(&header).expect("Failed to serialize header");
        self.store.write(header.digest().to_vec(), bytes).await;

        // If the header received is at a greater height then add it to our local tips and proposals
        if self.use_optimistic_tips && header.height() > self.current_proposal_tips.get(&header.origin()).unwrap().height {
            self.current_proposal_tips.insert(
                header.origin(),
                Proposal {
                    header_digest: header.digest(),
                    height: header.height(),
                },
            );
            //println!("updating tip");
            debug!("updating tip");

            // Since we received a new tip, check if any of our pending tickets are ready
            self.try_prepare_waiting_slots().await?;
        }

        //TODO: Fix check coverage as well. (new proposals..)
        if !self.use_optimistic_tips && header.parent_cert.height > self.current_certified_tips.get(&header.origin()).unwrap().height {
            self.current_certified_tips.insert(
                header.origin(),
                Proposal {
                    header_digest: header.parent_cert.header_digest.clone(),
                    height: header.parent_cert.height,
                },
            );
            //println!("updating tip");
            debug!("updating tip");

            // Since we received a new tip, check if any of our pending tickets are ready
            self.try_prepare_waiting_slots().await?;
        }

        //println!("after height check");
        debug!("after tip height check");

        // Process the parent certificate
        self.process_certificate(header.clone().parent_cert).await?;

        //If Header has no consensus messages (i.e. is pure car) then only 2f+1 replicas need to vote and reply.
        if header.consensus_messages.is_empty() && !self.check_cast_vote(&header) {
            return Ok(());
        }

        // Check if we can vote for this header.
        if self
            .last_voted
            .entry(header.height())
            .or_insert_with(HashSet::new)
            .insert(header.author)
        {
            //println!("voting for header");
            // Process the consensus instances contained in the header (if any)
            let consensus_votes = self
                .process_consensus_messages(&header)
                .await?;

            //println!("Consensus sigs length {:?}", consensus_votes.len());
            debug!("Consensus sigs length {:?}", consensus_votes.len());

            // Create a vote for the header and any valid consensus instances
            let vote = Vote::new(
                &header,
                &self.name,
                &mut self.signature_service,
                consensus_votes,
            )
            .await;
            //println!("Created vote");
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
                let handler = self.network.send(address, Bytes::from(bytes)).await;
                self.cancel_handlers
                    .entry(header.height())
                    .or_insert_with(Vec::new)
                    .push(handler);
            }
        }
        Ok(())
    }

    fn check_cast_vote(&self, header: &Header) -> bool {
        //Only 2f+1 replicas need to vote for cars; i.e. skip f //Alternatively: Consider yourself a voter if name within 2f+1 after author 
        let mut start = false;
        let mut count = 1; //start at 1, f do not need to vote.

        let mut iter = self.committee.authorities.iter();
    
        //find origin position. After that identify first f that should not send.
        while count < self.committee.validity_threshold() {
            let x = iter.next();
            if x.is_none(){
                iter = self.committee.authorities.iter(); //wrap around
                continue; 
            }
            let (id, _) = x.unwrap();
            if header.author.eq(&id) {
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
        return true;

        //Alternatively: Count 2f+1 that should send.
        //let mut count = 0;
        // while count < self.committee.quorum_threshold() {
        //     let x = iter.next();
        //     if x.is_none(){
        //         iter = self.committee.authorities.iter(); //wrap around
        //         continue; 
        //     }
        //     let (id, _) = x.unwrap();
        //     if header.author.eq(&id) {
        //         start = true;
        //     }
        //     if start {
        //         if self.name.eq(id) {
        //             debug!("CAST VOTE for header: {}", header.id);
        //             return true;
        //         }
        //         count += 1;
        //     }
        // }
        // debug!("DO NOT CAST VOTE for header: {}", header.id);
        // return false;
    }


    #[async_recursion]
    async fn process_vote(&mut self, vote: Vote, is_loopback: bool) -> DagResult<()> {
        debug!("Processing Vote {:?}", vote);

        // NOTE: If sending externally then need map of open consensus instances

        //If consensus vote loopback => Look up digest directly instead of via current instance.
        let consensus_loopback = is_loopback && !vote.consensus_votes.is_empty();//vote.consensus_instance.is_some();

        // Only process votes for the current header (or loopbacks for consensus)
        if vote.id != self.current_header.id || consensus_loopback {
            //println!("Wrong header");
            return Ok(())
        }

        //Invariant: All votes contain the same content (i.e. it's not the case that some of them carry things like timeouts etc)
        //Wait to form num_active instance many QCs
       
        //TODO: continue earlier if timeouts expire!! Currently all our lanes will stop if consensus stops voting 
                //Car should still vote even if consensus says No.
        
        let num_active_consensus_messages = self.current_header.num_active_instances;
        debug!("num active instances {:?}", num_active_consensus_messages);

        // Iterate through vote for each consensus instance
        for (slot, digest, sig) in vote.consensus_votes.iter() {

            debug!("current header {:?}", self.current_header);
            debug!("digest is {:?}", digest);
            //Get vote type of the instance: Prepare/Confirm-vote

            let opt_curr_instance = self.consensus_instances.get(&(*slot, digest.clone()));
            if opt_curr_instance.is_none() {
                debug!("consensus instance slot has committed, skip processing vote");
                continue;
            }
            let current_instance = opt_curr_instance.unwrap();

            if !is_loopback && vote.author != self.name {
                //Verify signature. Could optimize performance by only verifying after forming a batch, and use parallel batch_verification
                sig.verify(&current_instance.digest(), &vote.author)?;
            }
            //Why does this code not work?
            //let current_instance = self.consensus_instances.get(&(*slot, digest.clone())).unwrap(); //todo: Throw a panic if it does not exist.

            // let current_instance = match consensus_loopback {
            //     true => &vote.consensus_instance.as_ref().unwrap(), //Just look it up from the buffered instance 
            //     false => self.current_header.consensus_messages.get(digest).unwrap(),
            // };
            
            let qc_maker = self.qc_makers.entry((*slot, digest.clone())).or_insert(QCMaker::new());
            // let qc_maker = match current_instance {
            //     ConsensusMessage::Prepare {slot, view, tc: _, proposals: _, } => self.qc_makers.entry((*slot, digest.clone())).or_insert(QCMaker::new()), //self.pqc_makers.entry((*slot, *view)).or_insert(QCMaker::new()), 
            //     ConsensusMessage::Confirm {slot, view, qc: _, proposals: _, } => self.qc_makers.entry((*slot, digest.clone())).or_insert(QCMaker::new()), //self.cqc_makers.entry((*slot, *view)).or_insert(QCMaker::new()),  
            //     _ => unreachable!("Should never try and fetch a qc_maker for Commit"),
            // };

        //    // If not already a qc maker for this consensus instance message, create one
        //     match self.qc_makers.get(&digest) {
        //         Some(_) => {
        //             //println!("QC Maker already exists");
        //         }
        //         None => {
        //             self.qc_makers.insert(digest.clone(), QCMaker::new());
        //         }
        //     }

        //     // Otherwise get the qc maker for this instance
        //     let qc_maker = self.qc_makers.get_mut(&digest).unwrap();

            //Configure qc_maker to try to use Fast Path
            qc_maker.try_fast = match current_instance {
                ConsensusMessage::Prepare {slot: _, view: _, tc: _, qc_ticket: _, proposals: _, } => self.use_fast_path,  //Only PrepareQC should try to compute a FastQC
                _ => false,
            };

            //println!("qc maker weight {:?}", qc_maker.votes.len());

            // Add vote to qc maker, if a QC forms then create a new consensus instance
            // TODO: Put fast path logic in qc maker (decide whether to wait timeout etc.), add
            // external messages

            //If qc_ready, but qc_opt = None => This is first Slow QC;
            //If qc_ready and qc_opt => This is FastQC or Consumption of Loopback to fetch SlowQC
            let (qc_ready, qc_opt) = match is_loopback {
                false => qc_maker.append(vote.author, (digest.clone(), sig.clone()), &self.committee)?,
                true => {
                    qc_maker.try_fast = false; //turn back to normal path handling
                    qc_maker.get_qc()?
                }
            };

            if qc_ready {
            // if let Some(qc) = qc_maker.append(vote.author, (digest.clone(), sig.clone()), &self.committee)?
            // {
                if qc_opt.is_none() && self.use_fast_path {
                    // Slow QC is available but we should wait for Fast
                    //Start timer for Fast:
                        //Creates a dummy vote with the same id as this vote, but only the waiting digest as consensus sigs
                        //Upon triggering timer, it will call loopback again, which will get the QC and proceed. 
                        //By including only the digest of the missing instance we avoid duplicates. 
                            //Alternatively could modify QCMaker such that it wipes the QC after first use

                    let t_vote = Vote {
                        id: Digest::default(),//vote.id.clone(), 
                        height: 0, 
                        origin: PublicKey::default(), 
                        author: PublicKey::default(), 
                        signature: Signature::default(), 
                        consensus_votes: vec![(*slot, digest.clone(), Signature::default())], 
                        //consensus_instance: Some(current_instance.clone()), //Buffer instance. Current header could've advanced in the meantime and thus no longer include this instance by the time timer triggers
                    };
                    let fast_timer = CarTimer::new(t_vote, self.fast_path_timeout);
                    self.car_timer_futures.push(Box::pin(fast_timer));
                    //self.timers.insert((tc.slot, tc.view + 1));
                }

                else if let Some(qc) = qc_opt { //If QC = some (i.e. FastPathQC succeed, or SlowPathQC suceed if running without FP)
                    //println!("QC formed");
                    self.current_qcs_formed += 1;

                    // let current_instance = self
                    //     .current_header
                    //     .consensus_messages
                    //     .get(&digest)
                    //     .unwrap();
                    match current_instance {
                        ConsensusMessage::Prepare {slot, view, tc: _, qc_ticket: _, proposals,} 
                        => {
                            debug!("Prepare QC formed in slot {:?}", slot);
                            debug!("Prepare has slot: {}, view: {}, digest: {}", slot, view, current_instance.digest());

                            //TODO: FIXME: (I assume this is the leader tip optimization): Re-factor this to be set at Header propose time already.
                            // Create a tip proposal for the header which contains the prepare message, so that it can be committed as part of the proposals
                            /*let leader_tip_proposal: Proposal = Proposal {header_digest: self.current_header.digest(), height: self.current_header.height(),};
                            // Add this cert to the proposals for this instance
                            let mut new_proposals = proposals.clone(); 
                            new_proposals.insert(self.name, leader_tip_proposal);*/
                            
                            let new_consensus_message = match qc_maker.try_fast {
                                true => {
                                    debug!("taking fast path!");
                                    ConsensusMessage::Commit {slot: *slot, view: *view,  qc, proposals: proposals.clone() }
                                    }, // Create Commit if we have FastPrepareQC
                                false => ConsensusMessage::Confirm {slot: *slot, view: *view,  qc, proposals: proposals.clone() },
                            };
                            //let new_consensus_message = ConsensusMessage::Confirm {slot: *slot, view: *view,  qc, proposals: new_proposals,};

                            // Send this new instance to the proposer
                            self.tx_info
                                .send(new_consensus_message)
                                .await
                                .expect("Failed to send info");
                        }
                        ConsensusMessage::Confirm {slot, view, qc: _,proposals,}
                        => {
                            debug!("Commit QC formed in slot {:?}", slot);
                            let new_consensus_message = ConsensusMessage::Commit {slot: *slot, view: *view, qc, proposals: proposals.clone(),};

                            // Send this new instance to the proposer
                            self.tx_info
                                .send(new_consensus_message)
                                .await
                                .expect("Failed to send info");
                        }
                        ConsensusMessage::Commit { slot: _, view: _, qc: _, proposals: _, } => {}
                    };
                }
            }
        }

        // If there are some consensus instances in the header then wait for 2f+1 votes to form QCs
                //let consensus_ready: bool = !self.current_header.consensus_messages.is_empty() && self.current_qcs_formed == num_active_consensus_messages;
        //NEW: Consider consensus ready if there is nothing we need to wait for either!
        let consensus_ready: bool = self.current_header.consensus_messages.is_empty() || self.current_qcs_formed == num_active_consensus_messages;

        //Next: Check whether Car is ready to go
        let vote_id = vote.id.clone();
        let car_timeout = is_loopback && vote.consensus_votes.is_empty();

        // Add the vote to the votes aggregator for the actual header
        //Note: car_cert_ready is true if QC exists (f+1 votes); first = true when QC is formed the first time (this starts timer only once)
        //=> aggregator will ignore new votes after (in particular it will ignore the fake loopback vote)
        let (car_cert_ready, first) = self.votes_aggregator.append(vote, &self.committee, &self.current_header)?;
        
        //Consider consensus "ready" if we timed out (i.e. just move on without waiting for consensus)
        let consensus_ready = consensus_ready || car_timeout;
        //only take the dissemination QC if consensus is ready, or we have timed out (this avoids needless copies)
        let dissemination_cert = match car_cert_ready && consensus_ready {
            true => self.votes_aggregator.get()?, //Get will only return Cert ONCE. I.e. if timer loopbacks after it's already been used, then nothing happens.
            false => None
        };
       
             //Old: If there are no consensus instances in the header then only wait for the dissemination cert (f+1) votes
             //let dissemination_ready: bool = self.current_header.consensus_messages.is_empty() && dissemination_cert.is_some();
        //New: dissemination ready as soon as 
        let dissemination_ready: bool = car_cert_ready && dissemination_cert.is_some();

        debug!("sentToProposer {:?}, diss_ready {:?}, consensus_ready {:?}", 
            self.sent_cert_to_proposer, dissemination_ready, consensus_ready);


        //If ready to disseminate car (dissemination cert exists) but waiting for consensus 
        if dissemination_ready && !consensus_ready && first { //first => start only one Timer
            let t_vote = Vote {
                id: vote_id, 
                height: 0, 
                origin: PublicKey::default(), 
                author: PublicKey::default(), 
                signature: Signature::default(), 
                consensus_votes: vec![], //Create dummy vote with no sigs => this indicates its the Car timeout
                //consensus_instance: None
            };
            let fast_timer = CarTimer::new(t_vote, self.fast_path_timeout);
            self.car_timer_futures.push(Box::pin(fast_timer));
        }

        //if !self.sent_cert_to_proposer && (dissemination_ready || consensus_ready) {
        if !self.sent_cert_to_proposer && (dissemination_ready && consensus_ready) {    
            //debug!("Assembled {:?}", dissemination_cert.unwrap());
            //println!("diss ready {:?}, consensus ready {:?}", dissemination_ready, consensus_ready);

            self.tx_proposer
                .send(dissemination_cert.unwrap())
                .await
                .expect("Failed to send certificate");

            self.sent_cert_to_proposer = true;
            //println!("after sending to proposer");
            self.current_qcs_formed = 0;
        }

        // TODO: Handle invalidated case where possibly want to send consensus message externally,
        // will add this when the fast path is added
        Ok(())
    }

     //TODO: Then work on Process Vote //TODO: Add a function: SendConsensus
    async fn process_consensus_vote(&mut self, vote: ConsensusVote, is_loopback: bool) -> DagResult<()> {

        debug!("Receive consensus vote for dig {}", &vote.digest);

        let opt_curr_instance = self.consensus_instances.get(&(vote.slot, vote.digest.clone()));
        if opt_curr_instance.is_none() {
            debug!("consensus instance slot has committed, skip processing vote");
            return Ok(());
        }

        if !is_loopback && vote.author != self.name {
            //Verify signature. Could optimize performance by only verifying after forming a batch, and use parallel batch_verification
            vote.sig.verify(&vote.digest, &vote.author)?;
        }

        let current_instance = opt_curr_instance.unwrap();
        //Invariant: All votes contain the same content (i.e. it's not the case that some of them carry things like timeouts etc)
        //Wait to form num_active instance many QCs
        
        let qc_maker = self.qc_makers.entry((vote.slot, vote.digest.clone())).or_insert(QCMaker::new());
    
        //Configure qc_maker to try to use Fast Path
        qc_maker.try_fast = match current_instance {
            ConsensusMessage::Prepare {slot: _, view: _, tc: _, qc_ticket: _, proposals: _, } => self.use_fast_path,  //Only PrepareQC should try to compute a FastQC
            _ => false,
        };
 
        
 
        // Add vote to qc maker, if a QC forms then create a new consensus instance
             
        //If qc_ready, but qc_opt = None => This is first Slow QC;
        //If qc_ready and qc_opt => This is FastQC or Consumption of Loopback to fetch SlowQC
        let (qc_ready, qc_opt) = match is_loopback {
            false => qc_maker.append(vote.author, (vote.digest.clone(), vote.sig.clone()), &self.committee)?,
            true => {
                qc_maker.try_fast = false; //turn back to normal path handling
                qc_maker.get_qc()?
            }
        };

        debug!("qc maker weight {:?}", qc_maker.votes.len());

        if qc_ready {
            if qc_opt.is_none() && self.use_fast_path {
                // Slow QC is available but we should wait for Fast
                //Start timer for Fast:
                    //Creates a dummy vote with the same id as this vote, but only the waiting digest as consensus sigs
                    //Upon triggering timer, it will call loopback again, which will get the QC and proceed. 
                    //By including only the digest of the missing instance we avoid duplicates. 
                        //Alternatively could modify QCMaker such that it wipes the QC after first use

                let fast_timer = FastTimer::new(vote.clone(), self.fast_path_timeout);
                self.fast_timer_futures.push(Box::pin(fast_timer));
                //self.timers.insert((tc.slot, tc.view + 1));
            }

            else if let Some(qc) = qc_opt { //If QC = some (i.e. FastPathQC succeed, or SlowPathQC suceed if running without FP)
                //println!("QC formed");
            
                match current_instance {
                    ConsensusMessage::Prepare {slot, view, tc: _, qc_ticket: _, proposals,} 
                    => {
                        debug!("Prepare QC formed in slot {:?}", slot);
                        debug!("Prepare has slot: {}, view: {}, digest: {}", slot, view, current_instance.digest());

                        let new_consensus_message = match qc_maker.try_fast {
                            true => {
                                debug!("taking fast path!");
                                ConsensusMessage::Commit {slot: *slot, view: *view,  qc, proposals: proposals.clone() }
                                }, // Create Commit if we have FastPrepareQC
                            false => ConsensusMessage::Confirm {slot: *slot, view: *view,  qc, proposals: proposals.clone() },
                        };
                        //let new_consensus_message = ConsensusMessage::Confirm {slot: *slot, view: *view,  qc, proposals: new_proposals,};

                        // continue with next consensus phase
                        self.send_consensus_req(new_consensus_message).await?;
                    }
                    ConsensusMessage::Confirm {slot, view, qc: _,proposals,}
                    => {
                        debug!("Commit QC formed in slot {:?}", slot);
                        let new_consensus_message = ConsensusMessage::Commit {slot: *slot, view: *view, qc, proposals: proposals.clone(),};

                        // continue with next consensus phase
                        self.send_consensus_req(new_consensus_message).await?;
                    }
                    ConsensusMessage::Commit { slot: _, view: _, qc: _, proposals: _, } => {
                        panic!("Should never receive Vote for Commit")
                    }
                };
            }
        }
         
        Ok(())
    }

    fn set_consensus_proposal(&mut self, consensus_message: &mut ConsensusMessage) {
        let header = &self.current_header;
        match consensus_message { //TODO: Re-factor ConsensusMessages to all have slot/view, option for TC/QC, and a type.
            ConsensusMessage::Prepare {slot, view, tc, qc_ticket: _, proposals} => {
                let set_proposal = tc.is_none() || proposals.is_empty(); 
                 //Set tips to propose if it is a new proposal (empty by default), or winning_proposal = empty. => if there is a winning prop proposals will not be empty
                if set_proposal {
                    debug!("UPDATING HEADER for slot {}", slot);
                    // Add new proposal tips

                    *proposals = match self.use_optimistic_tips {
                        true =>  self.current_proposal_tips.clone(),
                        false => self.current_certified_tips.clone(),
                    };
                   
                    // Leader tip proposal
                    proposals.insert(self.name, Proposal { header_digest: header.id.clone(), height: header.height });

                    // Defining seamless invariant (PHASE2-SPEC.md #2): under autobahn-seamless,
                    // every proposal entering a cut -- other than our own tip, which an author
                    // always trusts ahead of its own certificate (see the early insert in
                    // process_own_header) -- references a height no greater than that author's
                    // last *certified* height. No uncertified tip may ever enter consensus.
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

                    for (pk, proposal) in proposals {
                        debug!("new proposal height is {:?}", proposal.height);
                    }
                    
                    //TODO: If we want to hash also the proposals, then stored digest must change!! => have to remove entry from map and add it back with a new hash.
                    
                }
            },  
            _ => {},
        };
    }

    #[async_recursion]
    async fn send_consensus_req(&mut self, mut consensus_message: ConsensusMessage) -> DagResult<()> {

        self.set_consensus_proposal(&mut consensus_message);
       
        match &consensus_message {
            ConsensusMessage::Prepare {slot, view, tc, qc_ticket: _, proposals} => {
                if self.during_simulated_asynchrony {
                    debug!("Simulating Asynchrony: skip sending Prepare for slot {} view {}. This will trigger a view change", slot, view);
                    self.async_delayed_prepare = Some(consensus_message);
                    return Ok(());
                }
                // if *slot == 5 && *view == 1 {
                //     debug!("skip sending Prepare for slot 5 view 1. Trigger view change");
                //     return Ok(());
                // }
            },
            ConsensusMessage::Confirm {slot, view, qc: _, proposals} => {
                // if *slot == 5 && *view == 1 {
                //     debug!("skip sending Confirm for slot 5 view 1. Trigger view change");
                //     return Ok(());
                // }
            },
            ConsensusMessage::Commit {slot, view, qc: _, proposals} => {
                // if *slot == 5 && *view <3  {
                //     debug!("skip sending Commit for slot 5 view 1. Trigger view change");
                //     return Ok(());
                // }
            },
        };

        debug!("Send req for Consensus message {}", consensus_message);

        let consensus_req = ConsensusRequest::new(self.name, consensus_message, &mut self.signature_service).await;

        //send to all others
        let addresses = self
            .committee
            .others_primaries(&self.name)
            .iter()
            .map(|(_, x)| x.primary_to_primary)
            .collect();
        let message = bincode::serialize(&PrimaryMessage::ConsensusRequest(consensus_req.clone())).expect("Failed to serialize timeout message");
        let handlers = self.network.broadcast(addresses, Bytes::from(message)).await;

        self.cancel_handlers
            .entry(self.current_header.height())
            .or_insert_with(Vec::new)
            .extend(handlers);

        //process oneself
        self.process_consensus_request(consensus_req).await?;

        Ok(())
    }


    #[async_recursion]
    async fn process_certificate(&mut self, certificate: Certificate) -> DagResult<()> {
        debug!("Processing {:?}", certificate);

        // Store the certificate.
        let bytes = bincode::serialize(&certificate).expect("Failed to serialize certificate");
        self.store.write(certificate.digest().to_vec(), bytes).await;

        //println!("Stored the certificate: {:?}", certificate.digest());

        // If we receive a new certificate from ourself, then send to the proposer, so it can make
        // a new header
        // TODO: For certified tips need to keep this check and add to list of certified tips
        /*let latest_tip = self.current_proposal_tips.get(&certificate.origin()).unwrap();
        if certificate.origin() == self.name && certificate.height() == latest_tip.height - 1 {
            //println!("Sending to proposer");
            // Send it to the `Proposer`.
            self.tx_proposer
                .send(certificate.clone())
                .await
                .expect("Failed to send certificate");
        }*/

        //println!("Certificate is {:?}, {:?}", certificate.header_digest, certificate.height);
        Ok(())
    }

    #[async_recursion]
    async fn try_prepare_waiting_slots(&mut self) -> DagResult<()> {
        //Could there even be multiple prepares? Bounding l <= 4 should make it so that each replica can only be the original leader for one slot? VC leaders are not blocked on coverage (they just propose current tips)
    
        for i in 0..self.prepare_tickets.len() {
            //println!("checking prepare ticket");
            // Get the first buffered prepare ticket
            let prepare_msg = self.prepare_tickets.pop_front().unwrap();
            self.is_prepare_ticket_ready(&prepare_msg).await?;
        }

        Ok(())
    }

     // if !self.is_prepare_ticket_ready(prepare_message).await.unwrap() {
                //     //println!("prepare ticket not ready");
                //     self.prepare_tickets.push_back(prepare_message.clone());
                // }


    async fn is_prepare_ticket_ready(&mut self, prepare_message: &ConsensusMessage) -> DagResult<()> {
        match prepare_message {
            ConsensusMessage::Prepare { slot, view: _, tc: _, qc_ticket: _, proposals } => {
        
                let next_leader = self.leader_elector.get_leader(slot + 1, 1);
                
                // If not the next leader 
                if self.name != next_leader {
                    if false {
                        //forward the prepare message to the appropriate leader to ensure timeouts that respect honest leader  // TODO: Turn off to maximize perf in gracious intervals 
                        let address = self
                            .committee
                            .primary(&next_leader)
                            .expect("Author of valid header is not in the committee")
                            .primary_to_primary;
                        let bytes = bincode::serialize(&PrimaryMessage::ConsensusMessage(prepare_message.clone()))
                            .expect("Failed to serialize prepare message");
                        let handler = self.network.send(address, Bytes::from(bytes)).await;
                        self.cancel_handlers
                            .entry(self.current_header.height())
                            .or_insert_with(Vec::new)
                            .push(handler);
                        //println!("forwarding to the leader");
                    
                    }
                    return Ok(())
                }
                
                

                // If we are the leader of the next slot, view 1, and have already proposed in the next slot
                // then don't process the prepare ticket, just return true
                if self.already_proposed_slots.contains(&(slot + 1)) {
                    return Ok(())
                }

                //Check that we have bounded instances.
                        // => Wait for instance s - k to commit. This ensures that <= k consecutive instances are open at any time (since we also only start if have prepare ticket from s-1)
    

                if *slot + 1 > self.k {
                    debug!("beyond init k");
                    if !self.committed_slots.contains_key(&(slot + 1 - self.k)) {
                        debug!("too many instances open");
                        self.prepare_tickets.push_back(prepare_message.clone());
                        return Ok(())
                    }
                }
                // if slot + 1 > self.last_committed_slot + self.k {
                //     //println!("too many instances open");
                //     self.prepare_tickets.push_back(prepare_message.clone());
                //     return Ok(())
                // }
    

                // If there is enough coverage and we haven't already proposed in the next slot then create a new
                // prepare message if we are the leader of view 1 in the next slot
                //let new_proposals = self.current_proposal_tips.clone();
                if self.enough_coverage(&proposals) {//}, &new_proposals) {
                    debug!("have enough coverage to start slot {}", slot + 1);

                    let qc_ticket = match *slot + 1 > self.k {
                        true => Some(self.committed_slots.get(&(slot+1-self.k)).unwrap().clone()), //Validate this QC at recipient. Only necessary if not local available. Process if new!
                        false => None,
                    };

                    let new_prepare_instance = ConsensusMessage::Prepare {
                        slot: slot + 1,
                        view: 1,
                        tc: None,
                        qc_ticket,
                        proposals: HashMap::new(), //new_proposals,
                    };

                    //println!("The new slot is {:?}", slot + 1);
                    self.already_proposed_slots.insert(slot + 1);
                    //self.prepare_tickets.pop_front();

                    //TODO: Start measuring consensus latency from here. Measure latency for a slots commit
                    // #[cfg(feature = "benchmark")]
                    // // NOTE: This log entry is used to compute performance.
                    // info!("Started slot {}", slot + 1);
                    // 

                    if self.use_ride_share {
                        self.tx_info
                        .send(new_prepare_instance)
                        .await
                        .expect("failed to send info to proposer");
                    }
                    else{
                        debug!("enough coverage!");
                        self.send_consensus_req(new_prepare_instance).await?;
                    }
                    
                    return Ok(());
                } else {
                    // Not enough coverage, add this prepare ticket to the pending queue
                    // until enough new proposals have arrived
                    //println!("prepare ticket not ready");
                    self.prepare_tickets.push_back(prepare_message.clone());
                    return Ok(());
                }
            },
            _ => Ok(()),
        }
    }

    // TODO: Double check these checks are good enough
    #[async_recursion]
    async fn is_valid(&mut self, consensus_message: &ConsensusMessage) -> bool {
        match consensus_message {
            ConsensusMessage::Prepare { slot, view, tc, qc_ticket, proposals } => {
                
                // NOTE: There are two cases: view = 1, and view > 1
                // For view = 1 the leader can propose "anything", coverage is
                // enforced on a best effort basis
                // For view > 1, the leader must justify its prepare message with
                // a TC from the previous view, so that proposals that could have committed
                // are recovered
                let mut ticket_valid: bool = true;
                match tc {
                    Some(tc) => {
                        // Ensure tc is valid
                        if tc.view + 1 != *view {
                            return false;
                        }
                        ticket_valid = tc.verify(&self.committee).is_ok();
                        
                        let winning_proposals = tc.get_winning_proposals(&self.committee);
                        if !winning_proposals.is_empty() {
                            for (pk, proposal) in proposals {
                                ticket_valid = ticket_valid && proposal.eq(winning_proposals.get(&pk).unwrap());
                            }
                        }
                    },
                    None => {
                        // Any prepare is valid for view 1 //TODO: Add option for sequential ticket enforcement + bounding
                        if !self.use_parallel_proposals {
                            panic!("Parallel proposals should be true");
                        }
                        //Check if QC_ticket valid:
                        if *slot > self.k { 
                            debug!("Checking QC Ticket");
                            if !self.committed_slots.contains_key(&(slot-self.k)) { //If we have it locally don't need to verify
                                debug!("Verify QC Ticket");
                                //Process CommitMessage
                                let commit_qc = qc_ticket.as_ref().unwrap();
                                let commit_message = transform_commitQC(commit_qc.clone());
                                if commit_qc.slot + self.k != *slot {
                                    return false;
                                }
                                ticket_valid = self.is_valid(&commit_message).await;
                                debug!("Verify QC Ticket: {}", ticket_valid);
                                //self.process_commit_message(commit_message, &Header::default()).await.expect("QC Ticket valid"); //TODO: process if unseen..
                            }
                            //if locally committed, do nothing.
                        }
                        ticket_valid = ticket_valid && *view == 1;
                    },
                };

                let curr_view = self.views.get(slot).unwrap_or(&0);
                if curr_view < view {
                    self.views.insert(*slot, *view);
                }

                // Ensure that we haven't already voted in this slot, view, that the ticket is
                // valid, and we are in the same view
                !self.last_voted_consensus.contains(&(*slot, *view)) && ticket_valid && self.views.get(slot).unwrap() == view
            },
            ConsensusMessage::Confirm { slot, view, qc, proposals: _ } => {
                debug!("try to unwrap slot");
                
                let curr_view = self.views.get(slot).unwrap_or(&0);
                if curr_view <= view {
                    if verify_confirm(consensus_message, &self.committee){
                        self.views.insert(*slot, *view);
                        return true;
                    }
                    
                }
                return false;

                // Ensure that the QC is valid, and that we are in the same view
               //qc.verify(&self.committee).is_ok() && self.views.get(slot).unwrap() == view
                //self.views.get(slot).unwrap() == view && verify_confirm(consensus_message, &self.committee)
            },
            ConsensusMessage::Commit { slot, view, qc, proposals } => {
                verify_commit(consensus_message, &self.committee)
                
                // Ensure that the QC is valid, and that we are in the same view
                //qc.verify(&self.committee).is_ok() && self.views.get(slot).unwrap() == view
                //self.views.get(slot).unwrap() == view && verify_commit(consensus_message, &self.committee)
            },
        }
    }

    async fn is_consensus_ready(&mut self, header: &Header) -> bool {
        let mut is_ready = true;
        for (_, consensus_message) in &header.consensus_messages {
            match consensus_message {
                ConsensusMessage::Prepare { slot: _, view: _, tc: _, qc_ticket: _, proposals: _ } => {
                    // Consensus is ready if all proposals for all prepare messages in a car aren't
                    // missing
                    // NOTE: If view > 0 then don't have to call this, only the leader of first
                    // view takes responsibility, only check for view > 0 so you don't need to
                    // check whether winning proposals is correct
                    // TODO: For testing with faults make the certificate of tip syncing happen
                    // asynchronously, change synchronizer so that it write to the store without
                    // the history
                    is_ready = is_ready && !self.synchronizer.get_proposals(consensus_message, header).await.unwrap().is_empty();
                },
                ConsensusMessage::Commit { slot: _, view: _, qc: _, proposals: _ } => {
                    //TODO: If we'd like to process it earlier
                    //self.process_commit_message(consensus_message.clone(), &Header::default()).await?; 
                },
                _ => {},
            };
        }
        is_ready
    }

    #[async_recursion]
    async fn process_consensus_messages(
        &mut self,
        header: &Header,
    ) -> DagResult<Vec<(Slot, Digest, Signature)>> {
        // Map between consensus instance digest and a signature indicating a vote for that
        // instance
        let mut consensus_votes: Vec<(Slot, Digest, Signature)> = Vec::new();

        for (_, consensus_message) in &header.consensus_messages {
            //println!("processing instance");
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
                        //println!("processing prepare message");
                        debug!("processing prepare in slot {:?} with proposal {:?}", slot, proposals);
                        self.process_prepare_message(consensus_message, consensus_votes.as_mut()).await;
                    },
                    ConsensusMessage::Confirm {
                        slot,
                        view: _,
                        qc: _,
                        proposals,
                    } => {
                        //println!("processing confirm message");
                        debug!("processing confirm in slot {:?} with proposal {:?}", slot, proposals);
                        // Start syncing on the proposals if we haven't already
                        self.synchronizer.get_proposals(consensus_message, &header).await?;
                        self.process_confirm_message(consensus_message, consensus_votes.as_mut()).await;
                    },
                    ConsensusMessage::Commit {
                        slot,
                        view: _,
                        qc: _,
                        proposals: _,
                    } => {
                        //println!("processing commit message");
                        debug!("processing commit in slot {:?}", slot);
                        self.process_commit_message(consensus_message.clone(), &header).await?; //FIXME: Does this need to be a copy?
                    }
                }
            }
        }

        //println!("Returning from process consensus size of consensus sigs {:?}", consensus_votes.len());
        Ok(consensus_votes)
    }

    async fn process_consensus_request(&mut self, consensus_req: ConsensusRequest) -> DagResult<()> {
    
        
        let consensus_message = &consensus_req.message;
        debug!("received consensus request for slot");

        match consensus_message {
            ConsensusMessage::Prepare { slot, view: _, tc: _, qc_ticket: _, proposals,} 
            => {
                debug!("processing prepare in slot {:?} with proposal {:?}", slot, proposals);
            },
            ConsensusMessage::Confirm {
                slot,
                view: _,
                qc: _,
                proposals,
            } => {
                debug!("processing confirm in slot {:?} with proposal {:?}", slot, proposals);
            },
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
        match &consensus_message { //TODO: Re-factor ConsensusMessages to all have slot/view, option for TC/QC, and a type.
            ConsensusMessage::Prepare {slot, view, tc: _, qc_ticket: _, proposals: _, } => {self.consensus_instances.insert((*slot, dig.clone()), consensus_message.clone());},  
            ConsensusMessage::Confirm {slot, view, qc: _, proposals: _, } => {self.consensus_instances.insert((*slot, dig.clone()), consensus_message.clone());},  
            _ => {},
        };

        debug!("try to verify");
        let mut valid = true;
        if consensus_req.author != self.name {
            consensus_req.verify(&self.committee)?; 
            debug!("check validity");
            valid = self.is_valid(&consensus_message).await;
        }

        if !valid {
            return Ok(());
        }

        self.process_consensus_message(consensus_req.message, consensus_req.author).await
    }

    async fn process_consensus_message(&mut self, consensus_message: ConsensusMessage, author: PublicKey) -> DagResult<()> {

        let mut consensus_votes: Vec<(Slot, Digest, Signature)> = Vec::new();
       
        debug!("processing consensus msg");
       
        
        let mut header = Header::default();
        header.author = author;

        match &consensus_message {
            ConsensusMessage::Prepare { slot, view: _, tc: _, qc_ticket: _, proposals,} 
            => {
                debug!("processing prepare in slot {:?} with proposal {:?}", slot, proposals);
                if self.synchronizer.get_proposals(&consensus_message, &header).await.unwrap().is_empty() {
                    debug!("proposals of prepare in slot {:?} with proposal {:?} are not ready", slot, proposals);
                    return Ok(());
                }
                self.process_prepare_message(&consensus_message, consensus_votes.as_mut()).await;
            },
            ConsensusMessage::Confirm {
                slot,
                view: _,
                qc: _,
                proposals,
            } => {
                //println!("processing confirm message");
                debug!("processing confirm in slot {:?} with proposal {:?}", slot, proposals);
                // Start syncing on the proposals if we haven't already
                self.synchronizer.get_proposals(&consensus_message, &header).await?;
                self.process_confirm_message(&consensus_message, consensus_votes.as_mut()).await;
            },
            ConsensusMessage::Commit {
                slot,
                view: _,
                qc: _,
                proposals: _,
            } => {
                //println!("processing commit message");
                debug!("processing commit in slot {:?}", slot);
                self.process_commit_message(consensus_message.clone(), &header).await?; //FIXME: Does this need to be a copy?
            }
        }
        
        
        debug!("Returning from process consensus size of consensus sigs {:?}", consensus_votes.len());

        //TODO: Check that process_prepare isn't doing more than necessary for external.
        if consensus_votes.is_empty() { //E.g. if it was a Commit, or if messages were invalid.
            return Ok(());
        }

        //Broadcast the vote.
        let (slot, digest, sig) = consensus_votes.pop().unwrap();
        let vote = ConsensusVote {author: self.name, slot, digest, sig};

        if author == self.name {
            debug!("Process own consensus vote");
            self.process_consensus_vote(vote, false).await.expect("Failed to process our own vote"); //TODO: Don't need to sign...
        } 
        else {
            debug!("Send consensus vote to replica {}", author);
          
            let address = self
                    .committee
                    .primary(&author)
                    .expect("Author of valid header is not in the committee")
                    .primary_to_primary;
                let bytes = bincode::serialize(&PrimaryMessage::ConsensusVote(vote))
                    .expect("Failed to serialize our own vote");
                let handler = self.network.send(address, Bytes::from(bytes)).await;
                self.consensus_cancel_handlers
                    .entry(slot) 
                    .or_insert_with(Vec::new)
                    .push(handler);
        }
       
        
        Ok(())
    }

    //#[async_recursion]
    async fn process_prepare_message(
        &mut self,
        prepare_message: &ConsensusMessage,
        consensus_sigs: &mut Vec<(Slot, Digest, Signature)>,
    ) {
        match prepare_message {
            ConsensusMessage::Prepare {
                slot,
                view,
                tc: _,
                qc_ticket,
                proposals,
            } => {


                // Check if this prepare message can be used for a ticket to propose in the next slot
                // TODO: Remove from process_header
                let x = self.is_prepare_ticket_ready(prepare_message).await;
                // if !self.is_prepare_ticket_ready(prepare_message).await.unwrap() {
                //     //println!("prepare ticket not ready");
                //     self.prepare_tickets.push_back(prepare_message.clone());
                // }
                    //TODO: WE could start timers only locally after checking our local coverage as well.

                // If we haven't already started the timer for the next slot, start it
                // TODO:Can implement different forwarding methods (can be random, can forward to f+1, current one is the most pessimisstic)
              
                if self.k > 1 { //check whether a) we have already committed; and if not b) whether ticket is ready (prepare and QC)
                    if !self.committed_slots.contains_key(&(slot+1)) && !self.timers.contains(&(slot + 1, 1)) && self.committed_slots.contains_key(&(slot+1 - self.k))  { 
                        debug!("start timer for slot {}", slot +1);
                        let timer = Timer::new(slot + 1, 1, self.timeout_delay);
                        self.timer_futures.push(Box::pin(timer));
                        self.timers.insert((slot + 1, 1));
                    }
                }

                for (pk, proposal) in proposals {
                    debug!("prepare slot {:?}, proposal height {:?}", slot, proposal.height);
                }
                debug!("prepare vote in slot {:?}", slot);

                // Ensure that we don't vote for another prepare in this slot, view
                self.last_voted_consensus.insert((*slot, *view));

                if self.use_fast_path {
                      // Already checked that we were in the right view from validity checks, so just insert into our local high_proposals map
                    self.high_proposals.insert(*slot, ConsensusMessage::Prepare { slot: *slot, view: *view, tc: None, qc_ticket: None, proposals: proposals.clone()}); //Note: Don't need to store TC or QC's.
                }

                // Indicate that we vote for this instance's prepare message
                //let sig = Signature::default();
                let sig = self
                    .signature_service
                    .request_signature(prepare_message.digest())
                    .await;
                consensus_sigs.push((*slot, prepare_message.digest(), sig));
                debug!("Prepare-Vote for slot: {}, view: {},has digest: {}", slot, view, prepare_message.digest());
            }
            _ => {}
        }
    }

    //#[async_recursion]
    async fn process_confirm_message(
        &mut self,
        confirm_message: &ConsensusMessage,
        consensus_sigs: &mut Vec<(Slot, Digest, Signature)>,
    ) {
        match confirm_message {
            ConsensusMessage::Confirm {
                slot,
                view,
                qc,
                proposals: _,
            } => {
                // Already checked that we were in the right view from validity checks, so just
                // insert into our local high_qc map
                self.high_qcs.insert(*slot, confirm_message.clone());

                // Indicate that we vote for this instance's confirm message
                //let sig = Signature::default();
                let sig = self
                    .signature_service
                    .request_signature(confirm_message.digest())
                    .await;
                consensus_sigs.push((*slot, confirm_message.digest(), sig));
                debug!("Confirm-Vote for slot: {}, view: {}, qc_dig {:?} -> has digest: {}", slot, view, qc.id , confirm_message.digest());
            }
            _ => {}
        }
    }

    fn enough_coverage(
        &mut self,
        prepare_proposals: &HashMap<PublicKey, Proposal>,
        //current_proposals: &HashMap<PublicKey, Proposal>,
    ) -> bool {
        let current_proposals = match self.use_optimistic_tips {
            true => &self.current_proposal_tips, 
            false => &self.current_certified_tips,
        };

        // Checks whether there have been n-f new certs from the proposals from the ticket
        let new_tips: HashMap<&PublicKey, &Proposal> = current_proposals
            .iter()
            .filter(|(pk, proposal)| proposal.height > prepare_proposals.get(&pk).unwrap().height)
            .collect();

        new_tips.len() as u32 >= self.committee.quorum_threshold()
    }

    #[async_recursion]
    async fn process_commit_message(&mut self, commit_message: ConsensusMessage, header: &Header) -> DagResult<()> {
       debug!("Called process commit");
        match &commit_message {
            ConsensusMessage::Commit {
                slot,
                view,
                qc,
                proposals,
            } => {
                debug!("Try to commit slot {}", slot);
                // Start simulating async once slot 1 is committed
                if self.simulate_asynchrony && *slot == 1 {
                    debug!("added async timers");
                    let async_start = Timer::new(0, 0, self.asynchrony_start);
                    let async_end = Timer::new(0, 0, self.asynchrony_start + self.asynchrony_duration);
                    self.async_timer_futures.push(Box::pin(async_start));
                    self.async_timer_futures.push(Box::pin(async_end));
                }

                //Stop timer for this slot/view //Note: Ideally stop all timers for this slot, but timers for older views are obsolete anyways.
                self.timers.remove(&(*slot, *view));
                //self.high_qcs.insert(*slot, commit_message.clone());

                let sl = *slot;
                //update bounding heuristic
                self.last_committed_slot = max(sl, self.last_committed_slot);
                self.committed_slots.insert(sl, CommitQC::new(*slot, *view, qc.clone(), proposals.clone()).await);


                //self.begin_slot_from_commit(&commit_message).await.expect("Failed to start next consensus");

                if self.k == 1 { //Start timer for next slot
                    if !self.timers.contains(&(slot + self.k, 1)) {
                        debug!("start timer for slot {}", slot +1);
                        let timer = Timer::new(slot + self.k, 1, self.timeout_delay);
                        self.timer_futures.push(Box::pin(timer));
                        self.timers.insert((slot + self.k, 1));
                    }
                }
                else{ //If slot + k has ticket ready (Prepare from s+k-1 + QC in s)
                    if !self.timers.contains(&(slot + self.k, 1)) && self.views.contains_key(&(slot+self.k -1)) {
                        debug!("start timer for slot {}", slot +1);
                        let timer = Timer::new(slot + self.k, 1, self.timeout_delay);
                        self.timer_futures.push(Box::pin(timer));
                        self.timers.insert((slot + self.k, 1));
                    }
                }

                // Only send to committer if proposals and all ancestors are stored locally,
                // otherwise sync will be triggered, and this commit message will be reprocessed
                if !self.synchronizer.get_proposals(&commit_message, &header).await.unwrap().is_empty() {
                    //println!("Sent to committer");
                    debug!("sending to committer");
                    self.tx_committer
                        .send(commit_message)
                        .await
                        .expect("Failed to send headers");
                }

                //Try waking any prepares that are waiting for a QC ticket
                self.try_prepare_waiting_slots().await?;

                // Garbage collect (can be ascyn)
                //self.clean_slot(sl);
                self.clean_slot_periods(sl);
            }
            _ => {}
        }

        Ok(())
    }

    #[async_recursion]
    async fn clean_slot(&mut self, slot: Slot) -> DagResult<()> {

        //GC Consensus instances
        self.consensus_instances.retain(|(s, _), _| s != &slot); 
        self.consensus_cancel_handlers.retain(|s, _| s != &slot); 

        //GC QC_Makers
        self.qc_makers.retain(|(s, _), _| s != &slot); 
        // self.pqc_makers.retain(|(s, _), _| s != &sl); 
        // self.cqc_makers.retain(|(s, _), _| s != &sl); 
        Ok(())
    }

    #[async_recursion]
    async fn clean_slot_periods(&mut self, slot: Slot) -> DagResult<()> {

        //slot periodics
        let slot_period = slot % self.k;
        let k = self.k;

        //GC Consensus instances
        self.consensus_instances.retain(|(s, _), _| s % k != slot_period && s <= &slot); 
        self.consensus_cancel_handlers.retain(|s, _| s % k != slot_period && s <= &slot); 
        //self.committed_slots GC those that are older.

        //GC QC_Makers
        self.qc_makers.retain(|(s, _), _| s % k != slot_period && s <= &slot); 
     

        Ok(())
    }


    #[async_recursion]
    async fn process_loopback(&mut self, consensus_message: ConsensusMessage, header: Header) -> DagResult<()> {
        //println!("reprocessing a header/commit message");
        debug!("Can reprocess a header/commit message");
        match &consensus_message {
            ConsensusMessage::Prepare { slot, view, tc: _, qc_ticket: _, proposals: _ } => {
                if self.use_ride_share {
                    // Now that proposals are ready we can reprocess the header
                    self.process_header(header, false).await?;
                }
                else{
                    if self.last_voted_consensus.contains(&(*slot, *view)){ //Don't prepare twice
                        return Ok(());
                    }
                    self.process_consensus_message(consensus_message, header.author).await? 
                }
            },
            ConsensusMessage::Confirm { slot: _, view: _, qc: _, proposals: _ } => {
                // Don't need to do anything for the confirm case, since proposals will be
                // sent to the committer once a commit message is received
            },
            ConsensusMessage::Commit { slot: _, view: _, qc: _, proposals: _ } => {
                // Send the commit message to the committer to order everything
                self.tx_committer
                    .send(consensus_message)
                    .await
                    .expect("Failed to send to committer");
            },
        };
        Ok(())
    }

    async fn process_forwarded_message(&mut self, consensus_message: ConsensusMessage) -> DagResult<()> {
        match &consensus_message {
            ConsensusMessage::Prepare { slot: _, view: _, tc: _, qc_ticket: _, proposals: _ } => {
                // We have a ticket for instance (slot + 1, 1), so check if we have enough coverage
                // to send a prepare message, otherwise buffer it
                self.is_prepare_ticket_ready(&consensus_message).await?;
                 
            },
            ConsensusMessage::Commit { slot: _, view: _, qc: _, proposals: _ } => {
                // Process any forwarded commit messages
                // NOTE: Used "dummy header" for second argument for now, header doesn't matter since proposal syncing
                // does not block processing the header, only prepare messages do
                self.process_commit_message(consensus_message, &self.current_header.clone());
            },
            _ => {}
        }
        Ok(())
    }

    async fn qc_timeout() {

           //2 tier timeout:
           // wait up to timeout for normal QC to form. (Start this timer after receiving f+1 votes, e.g. enough to advance car)
           // when normal QC is ready, wait for timer (only for prepare) to see if fast QC is ready. 

        //This function is the callback for timer experiation: 

        //QCMaker should return two values, ReadyFast, and QC
        //If !ReadyFast, start a timer to continue here.
        //This timer calls QCMaker.get() which returns the ready QC with 2f+1

        // let timer = Timer::new(tc.slot, tc.view + 1, self.timeout_delay);
        // self.timer_futures.push(Box::pin(timer));
        // self.timers.insert((tc.slot, tc.view + 1));


    // -----------------------


        //If we fail to assemble QC within time => continue with car => ask 
        //

        //If we fail to assemble FastQC within time => continue with normal QC => just ask QC_maker again. : On second ask, qc maker returns QC if it has.
        
        //start waiting for timer only after forming normal QC
        //Note FastQC is only for Prepare.
    }


    async fn local_timeout_round(&mut self, slot: Slot, view: View) -> DagResult<()> {
        warn!("Timeout reached for slot {}, view {}", slot, view);
        //println!("timeout was triggered");

        //If timer was cancelled, ignore  -- Note: technically redundant with commit check below, but currently we do not insert CommitQC's... TODO: Need to insert these so we can avoid joining view change and just reply.
        if !self.timers.contains(&(slot, view)) {
            debug!("Timer for slot {}, view {} is obsolete. Has been cancelled", slot, view);
            return Ok(())
        }

        // If timing out a smaller view than the current view, ignore
        match self.views.get(&slot) {
            Some(v) => {
                if *v > view {
                    debug!("Timer for slot {}, view {} is obsolete. Have moved to view {}", slot, view, *v);
                    return Ok(());
                }
            },
            None => {},
        };

        // If we have already committed then ignore the timeout
        match self.high_qcs.get(&slot) {
            Some(message) => {
                match message {
                    ConsensusMessage::Commit { slot: _, view: _, qc: _, proposals: _ } => {
                        return Ok(())
                    },
                    _ => {},

                }
            }
            None => {},
        };

        debug!("Sending Timeout for slot {}, view {}", slot, view);
        // Make a timeout message.for the slot, view, containing the highest QC this replica has
        // seen
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

        // Broadcast the timeout message.
        debug!("Broadcasting Timeout: {:?}", timeout);
        let addresses = self
            .committee
            .others_primaries(&self.name)
            .iter()
            .map(|(_, x)| x.primary_to_primary)
            .collect();
        let message = bincode::serialize(&PrimaryMessage::Timeout(timeout.clone()))
            .expect("Failed to serialize timeout message");
        let handlers = self.network
            .broadcast(addresses, Bytes::from(message))
            .await;

        self.consensus_cancel_handlers
            .entry(slot)
            .or_insert_with(Vec::new)
            .extend(handlers);

        //println!("Processed our own timeout");
        // Process our message.
        self.handle_timeout(&timeout).await
    }

    async fn handle_timeout(&mut self, timeout: &Timeout) -> DagResult<()> {
        debug!("Processing timeout {:?}", timeout);

        // TODO: If already committed then don't need to verify, just forward commit

        // Don't process timeout messages for old views
        match self.views.get(&timeout.slot) {
            Some(view) => {
                if timeout.view < *view {
                    return Ok(());
                }
            }
            _ => {}
        };

        if self.committed_slots.contains_key(&timeout.slot) {
            //TODO: Forward CommitQC instead.
            return Ok(());
        }

        // Ensure the timeout is well formed.
        timeout.verify(&self.committee)?;

        // If we haven't seen a timeout for this slot, view, then create a new TC maker for it.
        if self.tc_makers.get(&(timeout.slot, timeout.view)).is_none() {
            self.tc_makers
                .insert((timeout.slot, timeout.view), TCMaker::new());
        }

        // Otherwise, get the TC maker for this slot, view.
        let tc_maker = self
            .tc_makers
            .get_mut(&(timeout.slot, timeout.view))
            .unwrap();

        //println!("got tc maker");

        // Add the new vote to our aggregator and see if we have a quorum.
        if let Some(tc) = tc_maker.append(timeout.clone(), &self.committee)? {
            debug!("Assembled TimeoutCertificate {:?}", tc);

            // Try to advance the view
            self.views.insert(timeout.slot, timeout.view + 1);

            // Start the new view timer
            let timer = Timer::new(tc.slot, tc.view + 1, self.timeout_delay);
            self.timer_futures.push(Box::pin(timer));
            self.timers.insert((tc.slot, tc.view + 1));

            // Broadcast the TC.
            // TODO: Low priority: If you see f+1 timeouts then join the mutiny

            //FIXME: Don't need to broadcast TC if we join mutiny upon seeing f+1 timeouts.
            // debug!("Broadcasting {:?}", tc);
            // let addresses = self
            //     .committee
            //     .others_primaries(&self.name)
            //     .iter()
            //     .map(|(_, x)| x.primary_to_primary)
            //     .collect();
            // let message = bincode::serialize(&PrimaryMessage::TC(tc.clone()))
            //     .expect("Failed to serialize timeout certificate");
            // let handlers = self.network
            //     .broadcast(addresses, Bytes::from(message))
            //     .await;

            // self.consensus_cancel_handlers
            //     .entry(slot)
            //     .or_insert_with(Vec::new)
            //     .extend(handlers);

            // Generate a new prepare if we are the next leader.
            self.generate_prepare_from_tc(&tc).await?;
        }
        //println!("return from handle timeout");
        Ok(())
    }

    async fn generate_prepare_from_tc(&mut self, tc: &TC) -> DagResult<()> {
        // Make a new prepare message if we are the next leader.
        if self.name == self.leader_elector.get_leader(tc.slot, tc.view + 1) {

            debug!("IsLeader. Start prepare from TC");
            let winning_proposals = tc.get_winning_proposals(&self.committee);

            debug!("winning proposals: {:?}", winning_proposals);
            

            // If there is no QC we have to propose, then use our current tips for our proposal => happens later
            // if winning_proposals.is_empty() {
            //     winning_proposals = self.current_proposal_tips.clone();
            // }

            // Create a prepare message for the next view, containing the ticket and proposals
            // TODO: Low priority can make winning proposals empty
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
            }
            else{
                self.send_consensus_req(prepare_message).await?;
            }
           

            // A TC could be a ticket for the next slot
            // NOTE: This is TC Ticket optimization code, commented out for now
            /*if !self.already_proposed_slots.contains(&(timeout.slot + 1))
                && self.enough_coverage(&ticket.proposals, &winning_proposals)
            {
                let new_prepare_instance = ConsensusMessage::Prepare {
                    slot: timeout.slot + 1,
                    view: 1,
                    tc: None,
                    proposals: winning_proposals,
                };
                self.already_proposed_slots.insert(timeout.slot + 1);
                self.tx_info
                    .send(new_prepare_instance)
                    .await
                    .expect("failed to send info to proposer");
            } else {
                // Otherwise add the ticket to the queue, and wait later until there
                // are enough new certificates to propose
                self.prepare_tickets.push_back(prepare_instance);
            }*/
        }
        Ok(())
    }

    async fn handle_tc(&mut self, tc: &TC) -> DagResult<()> {
        debug!("Processing TC {:?}", tc);
        // Generate a new prepare if we are the next leader.
        self.generate_prepare_from_tc(tc).await?;

        Ok(())
    }

    fn sanitize_header(&mut self, header: &Header) -> DagResult<()> {
        ensure!(
            self.gc_round <= header.height,
            DagError::HeaderTooOld(header.id.clone(), header.height)
        );

        // Verify the header's signature.
        header.verify(&self.committee)?;

        // TODO [issue #3]: Prevent bad nodes from sending junk headers with high round numbers.

        Ok(())
    }

    fn sanitize_vote(&mut self, vote: &Vote) -> DagResult<()> {
        ////println!("Received vote for origin: {}, header id {}, round {}. Vote sent by replica {}", vote.origin.clone(), vote.id.clone(), vote.round.clone(), vote.author.clone());
        /*ensure!(
            self.current_headers.get(&vote.height) != None,
            DagError::VoteTooOld(vote.digest(), vote.height)
        );*/
        // ensure!(
        //     self.current_header.round <= vote.round,
        //     DagError::VoteTooOld(vote.digest(), vote.round)
        // );

        // Ensure we receive a vote on the expected header.
        /*let current_header = self.current_headers.entry(vote.height).or_insert_with(HashMap::new).get(&vote.author);
        ensure!(
            current_header != None && current_header.unwrap().author == vote.origin,
            DagError::UnexpectedVote(vote.id.clone())
        );*/
        // ensure!(
        //     vote.id == self.current_header.id
        //         && vote.origin == self.current_header.author
        //         && vote.round == self.current_header.round,
        //     DagError::UnexpectedVote(vote.id.clone())
        // );

        //Deprecated code for Invalid vote proofs
        // if false && self.current_header.is_special && vote.special_valid == 0 {
        //     match &vote.tc {
        //         Some(tc) => { //invalidation proof = a TC that formed for the current view (or a future one). Implies one cannot vote in this view anymore.
        //             ensure!(
        //                 tc.view >= self.current_header.view,
        //                 DagError::InvalidVoteInvalidation
        //             );
        //             match tc.verify(&self.committee) {
        //                 Ok(()) => {},
        //                 _ => return Err(DagError::InvalidVoteInvalidation)
        //             }

        //          },
        //         None => {
        //             match &vote.qc {
        //                 Some(qc) => { //invalidation proof = a QC that formed for a future view (i.e. an extension of some TC in current view or future)
        //                     ensure!( //proof is actually showing a conflict.
        //                         qc.view > self.current_header.view,
        //                         DagError::InvalidVoteInvalidation
        //                     );
        //                     match qc.verify(&self.committee) {
        //                         Ok(()) => {},
        //                         _ => return Err(DagError::InvalidVoteInvalidation)
        //                     }
        //                 }
        //                 None => { return Err(DagError::InvalidVoteInvalidation)}
        //             }
        //         },
        //     }
        // }

        //Check: 
        //If vote has no consensus sigs and vote.aggregator already has QC => ignore vote.  
        if self.current_header.id.eq(&vote.id) && self.votes_aggregator.complete {
            if vote.consensus_votes.is_empty() {   //Note: If vote is empty, but self.current_header.consensus_messages is not we can still ignore processing this vote (since it requires no consensus processing)
                return Err(DagError::CarAlreadySatisfied);
            }
            else { //Don't need to check signature (won't use it), but do need to process vote for consensus contents
                return Ok(());
            } 
        }


        // Verify the vote.
        vote.verify(&self.committee).map_err(DagError::from)
    }

    fn sanitize_certificate(&mut self, certificate: &Certificate) -> DagResult<()> {
        ensure!(
            self.gc_round <= certificate.height(),
            DagError::CertificateTooOld(certificate.digest(), certificate.height())
        );

        //println!("Past first ensure");

        // Verify the certificate (and the embedded header).
        certificate.verify(&self.committee).map_err(DagError::from)
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {

        //Simulate asynchrony duration:
        /*if self.simulate_asynchrony {
            debug!("added async timers");
            let async_start = Timer::new(0, 0, self.asynchrony_start);
            let async_end = Timer::new(0, 0, self.asynchrony_start + self.asynchrony_duration);
            self.async_timer_futures.push(Box::pin(async_start));
            self.async_timer_futures.push(Box::pin(async_end));
        }*/

        // Initialize current proposals with the genesis tips
        self.current_proposal_tips = Header::genesis_proposals(&self.committee);
        self.current_certified_tips = Header::genesis_proposals(&self.committee);
        debug!("genesis tips are {:?}", self.current_proposal_tips);

        // Start the timeout for slot 1, view 1
        debug!("start timer for slot {}", 1);
        let first_timer = Timer::new(1, 1, self.timeout_delay);
        self.timer_futures.push(Box::pin(first_timer));
        self.timers.insert((1, 1));
        self.views.insert(1, 1);

        // If we are the first leader then create a prepare ticket for slot 1
        if self.name == self.leader_elector.get_leader(1, 1) {
            //println!("We are the first leader creating a prepare ticket");
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

        // Initiate the proposer with a genesis parent
        let genesis_cert = Certificate::genesis_certs(&self.committee).get(&self.name).unwrap().clone();
        self.tx_proposer
            .send(genesis_cert)
            .await
            .expect("failed to send cert to proposer");

        loop {
            let result = tokio::select! {
                // We receive here messages from other primaries.
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
                                Ok(()) => self.process_certificate(certificate).await, //self.receive_certificate(certificate).await,
                                error => {
                                    error
                                }
                            }
                        },
                        PrimaryMessage::Timeout(timeout) => self.handle_timeout(&timeout).await,
                        PrimaryMessage::TC(tc) => self.handle_tc(&tc).await,

                        // We receive a forwarded prepare or commit message from another replica
                        PrimaryMessage::ConsensusMessage(consensus_message) => self.process_forwarded_message(consensus_message).await,
                          
                    
                        // External Consensus implementation: Receive Consensus Requests (Prep/Confirm/Commit) or Votes (Prep-Vote/Confirm-Ack)
                        PrimaryMessage::ConsensusRequest(consensus_req) => self.process_consensus_request(consensus_req).await,
                        PrimaryMessage::ConsensusVote(consensus_vote) => self.process_consensus_vote(consensus_vote, false).await,
                        _ => panic!("Unexpected core message")
                    }
                },

                // We also receive here our new headers created by the `Proposer`.
                Some(header) = self.rx_proposer.recv() => self.process_own_header(header).await,

                // We receive here loopback headers from the `HeaderWaiter`. Those are headers for which we interrupted
                // execution (we were missing some of their dependencies) and we are now ready to resume processing.
                Some(header) = self.rx_header_waiter.recv() => {
                    debug!("normal loopback for header");
                    self.process_header(header, true).await
                },

                // Loopback for committed instance that hasn't had all of it ancestors yet
                Some((consensus_message, header)) = self.rx_header_waiter_instances.recv() => self.process_loopback(consensus_message, header).await,
                //Loopback for special headers that were validated by consensus layer.
                //Some((header, consensus_sigs)) = self.rx_validation.recv() => self.create_vote(header, consensus_sigs).await,
                //i.e. core requests validation from consensus (check if ticket valid; wait to receive ticket if we don't have it yet -- should arrive: using all to all or forwarding)

                Some(header_digest) = self.rx_request_header_sync.recv() => self.synchronizer.fetch_header(header_digest).await,

                // We receive here loopback certificates from the `CertificateWaiter`. Those are certificates for which
                // we interrupted execution (we were missing some of their ancestors) and we are now ready to resume
                // processing.
                //Some(certificate) = self.rx_certificate_waiter.recv() => self.process_certificate(certificate).await,

                // We receive an event that timer expired
                Some((slot, view)) = self.timer_futures.next() => self.local_timeout_round(slot, view).await,

                Some(vote) = self.car_timer_futures.next() => self.process_vote(vote, true).await,

                //Fast path loopback for external consensus
                Some(vote) = self.fast_timer_futures.next() => self.process_consensus_vote(vote, true).await,

                Some((slot, view)) = self.async_timer_futures.next() => {
                    self.during_simulated_asynchrony = !self.during_simulated_asynchrony; 

                    debug!("Time elapsed is {:?}", self.current_time.elapsed()); 
                    self.current_time = Instant::now();

                    if !self.during_simulated_asynchrony {
                        let async_start = Timer::new(0, 0, self.asynchrony_start);
                        let async_end = Timer::new(0, 0, self.asynchrony_start + self.asynchrony_duration);

                        self.async_timer_futures.push(Box::pin(async_start));
                        self.async_timer_futures.push(Box::pin(async_end));

                        //
                        if self.async_delayed_prepare.is_some() {
                            let last_prop = self.async_delayed_prepare.clone().unwrap();
                            let still_relevant = match &last_prop { //check whether we're still in a relevant view.
                                ConsensusMessage::Prepare {slot, view, tc: _, qc_ticket: _, proposals: _} => view == self.views.get(slot).unwrap_or(&0),
                                _ => false,
                            };
                            if still_relevant { //try sending it now.
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

            // Cleanup internal state.
            let round = self.consensus_round.load(Ordering::Relaxed);
            if round > self.gc_depth {
                let gc_round = round - self.gc_depth;
                self.last_voted.retain(|k, _| k >= &gc_round);
                //self.processing.retain(|k, _| k >= &gc_round);

                //self.current_headers.retain(|k, _| k >= &gc_round);
                //self.vote_aggregators.retain(|k, _| k >= &gc_round);

                //self.certificates_aggregators.retain(|k, _| k >= &gc_round);
                self.cancel_handlers.retain(|k, _| k >= &gc_round);
                self.gc_round = gc_round;
                debug!("GC round moved to {}", self.gc_round);
            }
        }
    }
}

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::leader::LeaderElector;
use crate::messages::{proposal_digest, ConsensusMessage, Header, Proposal};
use crate::primary::{Height, PrimaryMessage, PrimaryWorkerMessage};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Blake3Hasher, Digest, Hash as _, PublicKey};
use futures::future::try_join_all;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error};
use metrics::Metrics;
use network::{BatchConfig, SimpleSender};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

/// Timer resolution for retrying sync requests.
const TIMER_RESOLUTION: u64 = 1_000;

type BatchRequestGroups = HashMap<WorkerId, Vec<Digest>>;
type OptimisticTipSources = HashMap<Digest, (PublicKey, Height)>;
type OptimisticLaneSources = HashMap<PublicKey, (PublicKey, Height)>;

#[derive(Clone, Debug)]
struct PendingPayload {
    author: PublicKey,
    height: Height,
    missing: HashMap<Digest, WorkerId>,
}

type PendingPayloads = HashMap<Digest, PendingPayload>;

fn register_batch_requests(
    tracked: &mut HashMap<Digest, (Height, PublicKey)>,
    missing: &HashMap<Digest, WorkerId>,
    height: Height,
    target: PublicKey,
) -> BatchRequestGroups {
    let mut groups = HashMap::new();
    for (digest, worker_id) in missing {
        if tracked
            .get(digest)
            .is_some_and(|(_, previous_target)| *previous_target == target)
        {
            continue;
        }
        tracked.insert(digest.clone(), (height, target));
        groups
            .entry(*worker_id)
            .or_insert_with(Vec::new)
            .push(digest.clone());
    }
    groups
}

fn proposal_waiter_id(message: &ConsensusMessage) -> Digest {
    let phase = message.digest();
    let proposals = proposal_digest(message);
    let mut hasher = Blake3Hasher::new();
    hasher.update(b"autobahn-proposal-waiter");
    hasher.update(&phase.0);
    hasher.update(&proposals.0);
    Digest(hasher.finalize().into())
}

fn register_optimistic_tip_sources(
    sources: &mut OptimisticTipSources,
    missing: &[(PublicKey, Proposal)],
    proposal_leader: PublicKey,
) {
    for (_, proposal) in missing {
        sources.insert(
            proposal.header_digest.clone(),
            (proposal_leader, proposal.height),
        );
    }
}

fn inherit_optimistic_tip_source(
    sources: &mut OptimisticTipSources,
    child: &Digest,
    parent: Digest,
    parent_height: Height,
) -> Option<PublicKey> {
    let proposal_leader = sources.get(child).map(|(leader, _)| *leader);
    if let Some(proposal_leader) = proposal_leader {
        sources.insert(parent, (proposal_leader, parent_height));
    }
    proposal_leader
}

fn merge_batch_request_groups(destination: &mut BatchRequestGroups, source: BatchRequestGroups) {
    for (worker, mut digests) in source {
        destination.entry(worker).or_default().append(&mut digests);
    }
}

fn collect_optimistic_prefix_requests(
    pending_payloads: &PendingPayloads,
    tracked: &mut HashMap<Digest, (Height, PublicKey)>,
    sources: &mut OptimisticTipSources,
    author: PublicKey,
    tip_height: Height,
    proposal_leader: PublicKey,
) -> BatchRequestGroups {
    let mut requests = HashMap::new();
    for (header_id, pending) in pending_payloads {
        if pending.author != author || pending.height > tip_height {
            continue;
        }
        sources.insert(header_id.clone(), (proposal_leader, pending.height));
        merge_batch_request_groups(
            &mut requests,
            register_batch_requests(tracked, &pending.missing, pending.height, proposal_leader),
        );
    }
    requests
}

/// Commands sent to the waiter.
#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
pub enum WaiterMessage {
    SyncBatches(HashMap<Digest, WorkerId>, Header, bool),
    SyncProposals(Vec<(PublicKey, Proposal)>, ConsensusMessage, Header),
    SyncParent(Digest, Header),
    SyncHeader(Digest),
}

/// Waits for missing parent certificates and batches' digests.
pub struct HeaderWaiter {
    /// The name of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// Deterministic Autobahn leader schedule. Prepare repair must target the
    /// elected leader, not whichever header happened to carry the message.
    leader_elector: LeaderElector,
    /// The persistent storage.
    store: Store,
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// The depth of the garbage collector.
    gc_depth: Height,
    /// Delay between sync-request retries.
    sync_retry_delay: u64,
    /// Number of nodes contacted when retrying a sync request.
    sync_retry_nodes: usize,

    /// Receives sync commands from the `Synchronizer`.
    rx_synchronizer: Receiver<WaiterMessage>,
    /// Loops back to the core headers for which we got all parents and batches.
    tx_core: Sender<Header>,
    /// Returns commit messages to the committer for reprocessing.
    tx_consensus_loopback: Sender<(ConsensusMessage, Header)>,

    /// Network driver allowing to send messages.
    network: SimpleSender,
    metrics: Arc<Metrics>,

    /// Certificate requests and their retry timestamps.
    parent_requests: HashMap<Digest, (Height, u128)>,
    /// Requests for special parents.
    header_requests: HashMap<Digest, (Height, u128)>,
    /// Batch requests, their heights, and their current target. A Prepare
    /// leader may supersede an earlier request to the Byzantine lane author.
    batch_requests: HashMap<Digest, (Height, PublicKey)>,
    /// Missing payloads retained by lane so a Prepare can request the whole
    /// unavailable prefix concurrently rather than walking one block per RTT.
    pending_payloads: PendingPayloads,
    /// Prepare leader that can relay each missing optimistic tip. The lane
    /// author itself is not a valid liveness dependency when it is Byzantine.
    optimistic_tip_sources: OptimisticTipSources,
    /// Latest elected Prepare leader and covered height for each lane.
    optimistic_lane_sources: OptimisticLaneSources,
    /// Pending items and cancellation handles.
    pending: HashMap<Digest, (Height, Sender<()>)>,
}

impl HeaderWaiter {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        consensus_round: Arc<AtomicU64>,
        gc_depth: Height,
        sync_retry_delay: u64,
        sync_retry_nodes: usize,
        rx_synchronizer: Receiver<WaiterMessage>,
        tx_core: Sender<Header>,
        tx_consensus_loopback: Sender<(ConsensusMessage, Header)>,
        metrics: Arc<Metrics>,
        batch: BatchConfig,
    ) {
        tokio::spawn(async move {
            let leader_elector = LeaderElector::new(committee.clone());
            Self {
                name,
                committee,
                leader_elector,
                store,
                consensus_round,
                gc_depth,
                sync_retry_delay,
                sync_retry_nodes,
                rx_synchronizer,
                tx_core,
                tx_consensus_loopback,
                network: SimpleSender::new()
                    .with_metrics(metrics.clone())
                    .with_batching(batch),
                metrics,
                parent_requests: HashMap::new(),
                header_requests: HashMap::new(),
                batch_requests: HashMap::new(),
                pending_payloads: HashMap::new(),
                optimistic_tip_sources: HashMap::new(),
                optimistic_lane_sources: HashMap::new(),
                pending: HashMap::new(),
            }
            .run()
            .await;
        });
    }

    /// Waits for storage entries or cancellation.
    async fn waiter(
        mut missing: Vec<(Vec<u8>, Store)>,
        deliver: Header,
        mut handler: Receiver<()>,
    ) -> DagResult<Option<Header>> {
        let waiting: Vec<_> = missing
            .iter_mut()
            .map(|(x, y)| y.notify_read(x.to_vec()))
            .collect();
        tokio::select! {
            result = try_join_all(waiting) => {
                result.map(|_| Some(deliver)).map_err(DagError::from)
            }
            _ = handler.recv() => Ok(None),
        }
    }

    async fn proposal_waiter(
        mut missing: Vec<(Vec<u8>, Store)>,
        deliver: (ConsensusMessage, Header),
        mut handler: Receiver<()>,
        started: Instant,
    ) -> DagResult<Option<((ConsensusMessage, Header), Duration)>> {
        let waiting: Vec<_> = missing
            .iter_mut()
            .map(|(x, y)| y.notify_read(x.to_vec()))
            .collect();
        tokio::select! {
            result = try_join_all(waiting) => {
                result.map(|_| Some((deliver, started.elapsed()))).map_err(DagError::from)
            }
            _ = handler.recv() => Ok(None),
        }
    }

    async fn send_batch_requests(
        &mut self,
        target: PublicKey,
        requests: BatchRequestGroups,
        optimistic_leader_repair: bool,
    ) {
        for (worker_id, digests) in requests {
            let address = self
                .committee
                .worker(&self.name, &worker_id)
                .expect("Author of valid header is not in the committee")
                .primary_to_worker;
            debug!(
                "Requesting {} missing batch(es) from {} {}",
                digests.len(),
                if optimistic_leader_repair {
                    "optimistic proposal leader"
                } else {
                    "header author"
                },
                target
            );
            let message = if optimistic_leader_repair {
                PrimaryWorkerMessage::SynchronizeOptimistic(digests, target)
            } else {
                PrimaryWorkerMessage::Synchronize(digests, target)
            };
            let bytes =
                bincode::serialize(&message).expect("Failed to serialize batch sync request");
            self.network
                .send_typed(address, Bytes::from(bytes), message.type_name())
                .await;
        }
    }

    /// Main loop listening to the `Synchronizer` messages.
    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();
        let mut proposal_waiting = FuturesUnordered::new();

        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                Some(message) = self.rx_synchronizer.recv() => {
                    match message {
                        WaiterMessage::SyncBatches(missing, header, force_sync) => {
                            debug!("Synching the payload of {}", header);
                            let header_id = header.id.clone();
                            let round = header.height;
                            let author = header.author;

                            self.pending_payloads
                                .entry(header_id.clone())
                                .and_modify(|pending| {
                                    pending.missing.extend(missing.clone());
                                })
                                .or_insert_with(|| PendingPayload {
                                    author,
                                    height: round,
                                    missing: missing.clone(),
                                });

                            let lane_source = self
                                .optimistic_lane_sources
                                .get(&author)
                                .copied()
                                .filter(|(_, tip_height)| round <= *tip_height);
                            let proposal_source = lane_source
                                .or_else(|| {
                                    self.optimistic_tip_sources
                                        .get(&header_id)
                                        .copied()
                                });

                            // A served copy must be allowed to upgrade an
                            // existing direct-header wait into a fetch before
                            // the waiter itself is deduplicated below.
                            if force_sync || proposal_source.is_some() {
                                let (target, requests, optimistic) =
                                    if let Some((proposal_leader, tip_height)) = proposal_source {
                                        let requests = collect_optimistic_prefix_requests(
                                            &self.pending_payloads,
                                            &mut self.batch_requests,
                                            &mut self.optimistic_tip_sources,
                                            author,
                                            tip_height.max(round),
                                            proposal_leader,
                                        );
                                        (proposal_leader, requests, true)
                                    } else {
                                        let requests = register_batch_requests(
                                            &mut self.batch_requests,
                                            &missing,
                                            round,
                                            author,
                                        );
                                        (author, requests, false)
                                    };
                                if !requests.is_empty() {
                                    self.send_batch_requests(target, requests, optimistic).await;
                                }
                            }

                            // A served copy upgrades an existing direct wait to
                            // an immediate fetch above, but must not install a
                            // second waiter for the same header.
                            if self.pending.contains_key(&header_id) {
                                continue;
                            }

                            // Wait for all batches.
                            let wait_for = missing
                                .iter()
                                .map(|(digest, worker_id)| {
                                    let key = [digest.as_ref(), &worker_id.to_le_bytes()].concat();
                                    (key.to_vec(), self.store.clone())
                                })
                                .collect();
                            let (tx_cancel, rx_cancel) = channel(1);
                            self.pending.insert(header_id, (round, tx_cancel));
                            let fut = Self::waiter(wait_for, header, rx_cancel);
                            waiting.push(fut);
                        }

                        WaiterMessage::SyncHeader(missing) => {
                            debug!("Syncing on header with digest {}", missing);

                            let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Failed to measure time")
                            .as_millis();

                            let mut requires_sync = Vec::new();
                            self.header_requests.entry(missing.clone()).or_insert_with(|| {
                                requires_sync.push(missing);
                                (0, now)
                            });

                            if !requires_sync.is_empty() {
                                let addresses = self.committee
                                .others_primaries(&self.name)
                                .iter()
                                .map(|(_, x)| x.primary_to_primary)
                                .collect();

                                let message = PrimaryMessage::HeadersRequest(requires_sync, self.name);
                                let bytes = bincode::serialize(&message).expect("Failed to serialize cert request");
                                self.network.lucky_broadcast_typed(
                                    addresses,
                                    Bytes::from(bytes),
                                    self.sync_retry_nodes,
                                    "HeadersRequest",
                                ).await;
                            }
                        }

                        WaiterMessage::SyncParent(missing, header) => {
                            debug!("Synching the parents of {}", header);
                            let header_id = header.id.clone();
                            let height = header.height();
                            let author = header.author;
                            // The elected leader's relay obligation covers the
                            // whole missing prefix behind an optimistic tip,
                            // not only the tip header itself. Carry the source
                            // backward before the child is retried.
                            let proposal_leader = inherit_optimistic_tip_source(
                                &mut self.optimistic_tip_sources,
                                &header_id,
                                missing.clone(),
                                height.saturating_sub(1),
                            );

                            // Deduplicate requests for the same header.
                            if self.pending.contains_key(&header_id) {
                                continue;
                            }

                            // Wait for the missing parent.
                            let wait_for = vec![(missing.to_vec(), self.store.clone())];
                            let (tx_cancel, rx_cancel) = channel(1);
                            self.pending.insert(header_id, (height, tx_cancel));
                            let fut = Self::waiter(wait_for, header, rx_cancel);
                            waiting.push(fut);

                            // Contact the optimistic proposal leader first
                            // while walking its missing lane prefix. Generic
                            // recovery still contacts the lane author.
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("Failed to measure time")
                                .as_millis();
                            let mut requires_sync = Vec::new();
                            self.parent_requests.entry(missing.clone()).or_insert_with(|| {
                                requires_sync.push(missing);
                                (height, now)
                            });
                            if !requires_sync.is_empty() {
                                let address = self.committee
                                    .primary(&proposal_leader.unwrap_or(author))
                                    .expect("Author of valid header not in the committee")
                                    .primary_to_primary;
                                let message = if proposal_leader.is_some() {
                                    PrimaryMessage::PrepareHeadersRequest(requires_sync, self.name)
                                } else {
                                    PrimaryMessage::HeadersRequest(requires_sync, self.name)
                                };
                                let bytes = bincode::serialize(&message).expect("Failed to serialize cert request");
                                self.network.send_typed(address, Bytes::from(bytes), message.type_name()).await;
                            }
                        }


                        WaiterMessage::SyncProposals(missing, consensus_message, header) => {
                            let height = header.height();
                            // Every phase carries the slot and view of the
                            // elected Autobahn leader that chose this cut. A
                            // replica may receive Confirm or Commit before its
                            // Prepare, so those phases must recover from the
                            // same leader as well.
                            let (slot, view) = match &consensus_message {
                                ConsensusMessage::Prepare { slot, view, .. }
                                | ConsensusMessage::Confirm { slot, view, .. }
                                | ConsensusMessage::Commit { slot, view, .. } => (*slot, *view),
                            };
                            let proposal_leader = self.leader_elector.get_leader(slot, view);
                            // Prepare, Confirm, and Commit can reference the
                            // same proposal vector while it is unavailable.
                            // Keep one waiter per consensus message so a later
                            // phase is not discarded behind an earlier one.
                            let id = proposal_waiter_id(&consensus_message);

                            // Deduplicate requests for the same proposal.
                            if self.pending.contains_key(&id) {
                                continue;
                            }

                            // The elected Autobahn leader must relay any
                            // optimistic tip in its cut; a Byzantine lane
                            // author may remain silent after narrowcasting.
                            // `Core` admits a peer tip to `current_proposal_tips`
                            // only after `missing_payload` succeeds, so a
                            // correct leader necessarily possesses these bytes.
                            register_optimistic_tip_sources(
                                &mut self.optimistic_tip_sources,
                                &missing,
                                proposal_leader,
                            );

                            let mut prefix_requests = HashMap::new();
                            for (author, proposal) in &missing {
                                self.optimistic_lane_sources
                                    .insert(*author, (proposal_leader, proposal.height));
                                merge_batch_request_groups(
                                    &mut prefix_requests,
                                    collect_optimistic_prefix_requests(
                                        &self.pending_payloads,
                                        &mut self.batch_requests,
                                        &mut self.optimistic_tip_sources,
                                        *author,
                                        proposal.height,
                                        proposal_leader,
                                    ),
                                );
                            }
                            if !prefix_requests.is_empty() {
                                self.send_batch_requests(proposal_leader, prefix_requests, true)
                                    .await;
                            }

                            self.metrics.autobahn_prepare_sync_events_total.inc();
                            self.metrics
                                .autobahn_prepare_missing_headers_total
                                .inc_by(missing.len() as u64);

                            // Wait for all referenced headers.
                            let wait_for = missing
                                .iter()
                                .map(|(_, proposal)| {
                                    (proposal.header_digest.to_vec(), self.store.clone())
                                })
                                .collect();
                            let (tx_cancel, rx_cancel) = channel(1);
                            self.pending.insert(id, (height, tx_cancel));
                            let fut = Self::proposal_waiter(
                                wait_for,
                                (consensus_message, header),
                                rx_cancel,
                                Instant::now(),
                            );
                            proposal_waiting.push(fut);

                            // Contact the elected proposal leader. A lane
                            // author that narrowcast the tip may remain silent.
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("Failed to measure time")
                                .as_millis();
                            let mut requires_sync = Vec::new();
                            for (_, missing) in missing {
                                self.parent_requests.entry(missing.header_digest.clone()).or_insert_with(|| {
                                    requires_sync.push(missing.header_digest);
                                    (missing.height, now)
                                });
                            }
                            if !requires_sync.is_empty() {
                                let address = self.committee
                                    .primary(&proposal_leader)
                                    .expect("Author of valid header not in the committee")
                                    .primary_to_primary;
                                let message = PrimaryMessage::PrepareHeadersRequest(requires_sync, self.name);
                                let bytes = bincode::serialize(&message).expect("Failed to serialize cert request");
                                self.network.send_typed(address, Bytes::from(bytes), "PrepareHeadersRequest").await;
                            }
                        }
                    }
                },

                Some(result) = waiting.next() => match result {
                    Ok(Some(header)) => {
                        debug!("Finished synching {:?}", header);
                        let _ = self.pending.remove(&header.id);
                        let _ = self.pending_payloads.remove(&header.id);
                        for x in header.payload.keys() {
                            let _ = self.batch_requests.remove(x);
                        }
                        let _ = self.parent_requests.remove(&header.parent_cert.header_digest);

                        self.tx_core.send(header).await.expect("Failed to send header");
                    },
                    Ok(None) => {},
                    Err(e) => {
                        error!("{}", e);
                        panic!("Storage failure: killing node.");
                    }
                },

                Some(result) = proposal_waiting.next() => match result {
                    Ok(Some((deliver, elapsed))) => {
                        self.metrics.autobahn_prepare_sync_completed_total.inc();
                        self.metrics
                            .autobahn_prepare_sync_wait_micros_total
                            .inc_by(elapsed.as_micros().min(u64::MAX as u128) as u64);
                        let id = proposal_waiter_id(&deliver.0);
                        let _ = self.pending.remove(&id);
                        for x in deliver.1.payload.keys() {
                            let _ = self.batch_requests.remove(x);
                        }

                        let possibly_missing = match &deliver.0 {
                            ConsensusMessage::Prepare {view: _, slot: _, tc: _, qc_ticket: _, proposals} => proposals,
                            ConsensusMessage::Confirm {view: _, slot: _, qc: _, proposals} => proposals,
                            ConsensusMessage::Commit {view: _, slot: _, qc: _, proposals} => proposals,
                        };
                        for (_, prop) in possibly_missing.iter() {
                            let _ = self.parent_requests.remove(&prop.header_digest);
                            let _ = self.optimistic_tip_sources.remove(&prop.header_digest);
                        }

                        self.tx_consensus_loopback.send(deliver).await.expect("Failed to send header");
                    },
                    Ok(None) => {},
                    Err(e) => {
                        error!("{}", e);
                        panic!("Storage failure: killing node.");
                    }
                },

                () = &mut timer => {
                    // Broadcast requests whose targeted retry timed out.
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Failed to measure time")
                        .as_millis();

                    // Header requests use a separate retry path.
                    let mut retry = Vec::new();
                    for (digest, (_, timestamp)) in &self.parent_requests {
                        if timestamp + (self.sync_retry_delay as u128) < now {
                            debug!("Requesting retry sync for parent header {} (retry)", digest);
                            retry.push(digest.clone());
                        }
                    }
                    let addresses = self.committee.others_primaries(&self.name).iter().map(|(_, x)| x.primary_to_primary).collect();
                    let message = PrimaryMessage::HeadersRequest(retry, self.name);
                    let bytes = bincode::serialize(&message).expect("Failed to serialize cert request");
                    self.network.lucky_broadcast_typed(addresses, Bytes::from(bytes), self.sync_retry_nodes, "HeadersRequest").await;

                    // Reschedule the timer.
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                }
            }

            // Cleanup internal state.
            let round = self.consensus_round.load(Ordering::Relaxed);
            if round > self.gc_depth {
                let mut gc_round = round - self.gc_depth;

                for (r, handler) in self.pending.values() {
                    if r <= &gc_round {
                        let _ = handler.send(()).await;
                    }
                }
                self.pending.retain(|_, (r, _)| r > &mut gc_round);
                self.batch_requests.retain(|_, (r, _)| r > &mut gc_round);
                self.pending_payloads
                    .retain(|_, pending| pending.height > gc_round);
                self.optimistic_tip_sources
                    .retain(|_, (_, r)| r > &mut gc_round);
                self.optimistic_lane_sources
                    .retain(|_, (_, r)| r > &mut gc_round);
                self.parent_requests.retain(|_, (r, _)| r > &mut gc_round);
                self.header_requests.retain(|_, (r, _)| r > &mut gc_round);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_requests_group_by_worker_and_deduplicate() {
        let first_target = crate::common::keys()[0].0;
        let replacement_target = crate::common::keys()[1].0;
        let a = Digest([1; 32]);
        let b = Digest([2; 32]);
        let c = Digest([3; 32]);
        let missing = HashMap::from([(a.clone(), 0), (b.clone(), 0), (c.clone(), 1)]);
        let mut tracked = HashMap::from([(a.clone(), (4, first_target))]);

        let mut groups = register_batch_requests(&mut tracked, &missing, 7, first_target);
        let mut worker_zero = groups.remove(&0).unwrap();
        worker_zero.sort();
        assert_eq!(worker_zero, vec![b]);
        assert_eq!(groups.remove(&1), Some(vec![c]));
        assert!(groups.is_empty());
        assert!(register_batch_requests(&mut tracked, &missing, 8, first_target).is_empty());

        let replacement = register_batch_requests(&mut tracked, &missing, 8, replacement_target);
        assert_eq!(replacement.values().map(Vec::len).sum::<usize>(), 3);
        assert_eq!(tracked.get(&a), Some(&(8, replacement_target)));
    }

    #[test]
    fn proposal_waiters_do_not_merge_prepare_and_commit() {
        let proposals = HashMap::new();
        let prepare = ConsensusMessage::Prepare {
            slot: 4,
            view: 2,
            tc: None,
            qc_ticket: None,
            proposals: proposals.clone(),
        };
        let commit = ConsensusMessage::Commit {
            slot: 4,
            view: 2,
            qc: Default::default(),
            proposals,
        };

        assert_ne!(proposal_waiter_id(&prepare), proposal_waiter_id(&commit));
    }

    #[test]
    fn prepare_leader_is_recorded_for_every_missing_optimistic_tip() {
        let leader = crate::common::keys()[0].0;
        let first = Proposal {
            header_digest: Digest([7; 32]),
            height: 11,
        };
        let second = Proposal {
            header_digest: Digest([8; 32]),
            height: 13,
        };
        let mut sources = HashMap::new();

        register_optimistic_tip_sources(
            &mut sources,
            &[(leader, first.clone()), (leader, second.clone())],
            leader,
        );

        assert_eq!(sources.get(&first.header_digest), Some(&(leader, 11)));
        assert_eq!(sources.get(&second.header_digest), Some(&(leader, 13)));
    }

    #[test]
    fn optimistic_repair_source_is_inherited_by_the_missing_parent() {
        let leader = crate::common::keys()[0].0;
        let child = Digest([7; 32]);
        let parent = Digest([8; 32]);
        let mut sources = HashMap::from([(child.clone(), (leader, 11))]);

        let inherited = inherit_optimistic_tip_source(&mut sources, &child, parent.clone(), 10);

        assert_eq!(inherited, Some(leader));
        assert_eq!(sources.get(&parent), Some(&(leader, 10)));
    }

    #[test]
    fn prepare_collects_the_whole_known_missing_lane_prefix_for_its_leader() {
        let keys = crate::common::keys();
        let lane = keys[0].0;
        let other_lane = keys[1].0;
        let leader = keys[2].0;
        let old_target = keys[3].0;
        let low_header = Digest([1; 32]);
        let tip_header = Digest([2; 32]);
        let future_header = Digest([3; 32]);
        let other_header = Digest([4; 32]);
        let low_batch = Digest([11; 32]);
        let tip_batch = Digest([12; 32]);
        let future_batch = Digest([13; 32]);
        let other_batch = Digest([14; 32]);
        let pending = HashMap::from([
            (
                low_header.clone(),
                PendingPayload {
                    author: lane,
                    height: 4,
                    missing: HashMap::from([(low_batch.clone(), 0)]),
                },
            ),
            (
                tip_header.clone(),
                PendingPayload {
                    author: lane,
                    height: 5,
                    missing: HashMap::from([(tip_batch.clone(), 1)]),
                },
            ),
            (
                future_header,
                PendingPayload {
                    author: lane,
                    height: 6,
                    missing: HashMap::from([(future_batch, 0)]),
                },
            ),
            (
                other_header,
                PendingPayload {
                    author: other_lane,
                    height: 5,
                    missing: HashMap::from([(other_batch, 0)]),
                },
            ),
        ]);
        let mut tracked = HashMap::from([(low_batch.clone(), (4, old_target))]);
        let mut sources = HashMap::new();

        let groups = collect_optimistic_prefix_requests(
            &pending,
            &mut tracked,
            &mut sources,
            lane,
            5,
            leader,
        );
        let requested: usize = groups.values().map(Vec::len).sum();

        assert_eq!(requested, 2);
        assert_eq!(tracked.get(&low_batch), Some(&(4, leader)));
        assert_eq!(tracked.get(&tip_batch), Some(&(5, leader)));
        assert_eq!(sources.get(&low_header), Some(&(leader, 4)));
        assert_eq!(sources.get(&tip_header), Some(&(leader, 5)));
        assert_eq!(sources.len(), 2);
    }
}

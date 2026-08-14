use crate::messages::ConsensusMessage;
#[cfg(feature = "benchmark")]
use crate::primary::PrimaryWorkerMessage;
use crate::primary::{Slot, CHANNEL_CAPACITY};
use crate::synchronizer::Synchronizer;
use crate::{Certificate, Header, Height};
#[cfg(feature = "benchmark")]
use bytes::Bytes;
use config::Committee;
#[cfg(feature = "benchmark")]
use config::WorkerId;
#[cfg(feature = "benchmark")]
use crypto::Digest;
use crypto::Hash as _;
use crypto::PublicKey;
use log::debug;
use metrics::Metrics;
use network::BatchConfig;
#[cfg(feature = "benchmark")]
use network::SimpleSender;
use std::borrow::BorrowMut;
use std::collections::{HashMap, VecDeque};
#[cfg(feature = "benchmark")]
use std::net::SocketAddr;
use std::sync::Arc;
#[cfg(feature = "benchmark")]
use std::time::{SystemTime, UNIX_EPOCH};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

/// Commit state.
struct State {
    // Last executed height per authority.
    last_executed_heights: HashMap<PublicKey, Height>,
    // Committed certificates by slot.
    log: HashMap<Slot, ConsensusMessage>,
    // Last executed slot.
    last_executed_slot: Slot,
}

/// Deterministically interleaves per-lane suffixes while preserving the order
/// within every lane. The cut's `HashMap` iteration order must never influence
/// the replicated output log.
fn zip_lane_suffixes(mut suffixes: Vec<(PublicKey, Vec<Header>)>) -> Vec<Header> {
    suffixes.sort_unstable_by_key(|(author, _)| *author);
    let mut suffixes: Vec<_> = suffixes
        .into_iter()
        .map(|(author, headers)| (author, VecDeque::from(headers)))
        .collect();
    let total = suffixes.iter().map(|(_, headers)| headers.len()).sum();
    let mut output = Vec::with_capacity(total);
    while output.len() < total {
        for (_, headers) in &mut suffixes {
            if let Some(header) = headers.pop_front() {
                output.push(header);
            }
        }
    }
    output
}

impl State {
    fn new(genesis: Vec<Certificate>) -> Self {
        let genesis = genesis
            .into_iter()
            .map(|x| (x.origin(), (x.digest(), x)))
            .collect::<HashMap<_, _>>();

        Self {
            last_executed_heights: genesis.keys().map(|x| (*x, 0)).collect(),
            log: HashMap::new(),
            last_executed_slot: 0,
        }
    }
}

pub struct Committer {
    rx_mempool: Receiver<Certificate>,
    rx_deliver: Receiver<Certificate>,
    rx_commit_message: Receiver<ConsensusMessage>,
    tx_output: Sender<Header>,
    synchronizer: Synchronizer,
    genesis: Vec<Certificate>,
    /// Local worker `primary_to_worker` addresses, keyed by worker ID.
    #[cfg(feature = "benchmark")]
    worker_addresses: HashMap<WorkerId, SocketAddr>,
    #[cfg(feature = "benchmark")]
    network: SimpleSender,
}

impl Committer {
    #[allow(clippy::too_many_arguments)]
    #[allow(unused_variables)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        gc_depth: Height,
        rx_mempool: Receiver<Certificate>,
        rx_commit: Receiver<Certificate>,
        rx_commit_message: Receiver<ConsensusMessage>,
        tx_output: Sender<Header>,
        synchronizer: Synchronizer,
        metrics: Arc<Metrics>,
        batch: BatchConfig,
    ) {
        let (_tx_deliver, rx_deliver) = channel(CHANNEL_CAPACITY);

        let genesis = Certificate::genesis(&committee);

        #[cfg(feature = "benchmark")]
        let worker_addresses: HashMap<WorkerId, SocketAddr> = committee
            .our_workers_by_id(&name)
            .expect("Our public key or worker id is not in the committee")
            .into_iter()
            .map(|(id, address)| (id, address.primary_to_worker))
            .collect();

        tokio::spawn(async move {
            Self {
                rx_mempool,
                rx_deliver,
                rx_commit_message,
                tx_output,
                synchronizer,
                genesis,
                #[cfg(feature = "benchmark")]
                worker_addresses,
                #[cfg(feature = "benchmark")]
                network: SimpleSender::new()
                    .with_metrics(metrics)
                    .with_batching(batch),
            }
            .run()
            .await;
        });
    }

    async fn process_commit_message(
        &mut self,
        state: &mut State,
        commit_message: ConsensusMessage,
    ) {
        if let ConsensusMessage::Commit {
            slot,
            view: _,
            qc: _,
            proposals: _,
        } = commit_message.clone()
        {
            if slot <= state.last_executed_slot {
                debug!("Already committed slot {}", slot);
                return;
            }

            // Queue commits until slots are contiguous.
            state.log.insert(slot, commit_message);

            while let Some(current_commit_message) =
                state.log.remove(&(state.last_executed_slot + 1))
            {
                debug!(
                    "Currently executing slot {:?}",
                    state.last_executed_slot + 1
                );
                if let ConsensusMessage::Commit {
                    slot: _,
                    view: _,
                    qc: _,
                    proposals,
                } = current_commit_message
                {
                    let mut ordered_proposals: Vec<_> = proposals.into_iter().collect();
                    ordered_proposals.sort_unstable_by_key(|(author, _)| *author);
                    let mut suffixes = Vec::with_capacity(ordered_proposals.len());
                    for (pk, proposal) in ordered_proposals {
                        let stop_height = *state.last_executed_heights.get(&pk).unwrap();
                        // Skip already executed proposals.
                        if proposal.height <= stop_height {
                            debug!("skipping this proposal because it's too old");
                            continue;
                        }

                        let headers = self
                            .synchronizer
                            .get_all_headers_for_proposal(proposal.clone(), stop_height)
                            .await
                            .expect("should have ancestors by now");

                        if proposal.height > stop_height {
                            state.last_executed_heights.insert(pk, proposal.height);
                            self.synchronizer.mark_executed(pk, proposal.height);
                        }

                        suffixes.push((pk, headers));
                    }

                    for header in zip_lane_suffixes(suffixes) {
                        debug!("Committed {}", header);
                        #[cfg(feature = "benchmark")]
                        {
                            for digest in header.payload.keys() {
                                // Parsed by benchmark tooling.
                                debug!("Committed {} -> {:?}", header, digest);
                            }

                            // Use this commit instant for worker latency measurement.
                            let commit_millis = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("Failed to measure time")
                                .as_millis() as u64;

                            // Notify local workers of committed batches.
                            let mut by_worker: HashMap<WorkerId, Vec<Digest>> = HashMap::new();
                            for (digest, worker_id) in header.payload.iter() {
                                by_worker
                                    .entry(*worker_id)
                                    .or_default()
                                    .push(digest.clone());
                            }
                            for (worker_id, digests) in by_worker {
                                if let Some(address) = self.worker_addresses.get(&worker_id) {
                                    let bytes = bincode::serialize(
                                        &PrimaryWorkerMessage::Committed(commit_millis, digests),
                                    )
                                    .expect("Failed to serialize committed message");
                                    self.network
                                        .send_typed(*address, Bytes::from(bytes), "Committed")
                                        .await;
                                }
                            }
                        }
                        debug!("Finished Commit");
                        if let Err(e) = self.tx_output.send(header.clone()).await {
                            debug!("Failed to send block through the output channel: {}", e);
                        }
                        debug!("Finish upcall");
                    }
                    state.last_executed_slot += 1;
                }
            }
        };
    }

    async fn run(&mut self) {
        // Mutable commit state.
        let mut state = State::new(self.genesis.clone());

        loop {
            tokio::select! {
                Some(_) = self.rx_mempool.recv() => {},
                Some(commit_message) = self.rx_commit_message.recv() => {
                    self.process_commit_message(state.borrow_mut(), commit_message).await;
                },
                Some(_) = self.rx_deliver.recv() => {},
                // Every producer is owned by another in-process actor. During an
                // orderly runtime shutdown they may all disappear before this task;
                // an exhausted select is actor termination, not a protocol failure.
                else => return,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::zip_lane_suffixes;
    use crate::messages::Header;
    use crypto::Digest;

    #[test]
    fn lane_zip_is_deterministic_and_preserves_each_lane() {
        let mut authors: Vec<_> = crate::common::keys()
            .into_iter()
            .take(3)
            .map(|(author, _)| author)
            .collect();
        authors.sort_unstable();
        let lane = |author, heights: &[u64]| {
            heights
                .iter()
                .map(|height| Header {
                    author,
                    height: *height,
                    id: Digest([*height as u8; 32]),
                    ..Header::default()
                })
                .collect::<Vec<_>>()
        };
        let forward = vec![
            (authors[0], lane(authors[0], &[1, 2, 3])),
            (authors[1], lane(authors[1], &[4])),
            (authors[2], lane(authors[2], &[5, 6])),
        ];
        let mut reverse = forward.clone();
        reverse.reverse();

        let first = zip_lane_suffixes(forward);
        let second = zip_lane_suffixes(reverse);
        let coordinates = |headers: Vec<Header>| {
            headers
                .into_iter()
                .map(|header| (header.author, header.height))
                .collect::<Vec<_>>()
        };
        assert_eq!(coordinates(first.clone()), coordinates(second));
        assert_eq!(
            coordinates(first),
            vec![
                (authors[0], 1),
                (authors[1], 4),
                (authors[2], 5),
                (authors[0], 2),
                (authors[2], 6),
                (authors[0], 3),
            ]
        );
    }
}

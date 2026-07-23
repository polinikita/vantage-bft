#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
use crate::messages::ConsensusMessage;
use crate::primary::{PrimaryWorkerMessage, Slot, CHANNEL_CAPACITY};
use crate::synchronizer::Synchronizer;
use crate::{Certificate, Header, Height};
//use crate::error::{ConsensusError, ConsensusResult};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use log::{debug, info};
use network::SimpleSender;
use std::borrow::BorrowMut;
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

/// The representation of the DAG in memory.
type Dag = HashMap<Height, HashMap<PublicKey, (Digest, Certificate)>>;

/// The state that needs to be persisted for crash-recovery.
struct State {
    /// The last committed round.
    last_committed_round: Height,
    // Keeps the last committed height for each authority. This map is used to clean up the dag and
    // ensure we don't commit twice the same certificate.
    last_executed_heights: HashMap<PublicKey, Height>,
    /// Keeps the latest committed certificate (and its parents) for every authority. Anything older
    /// must be regularly cleaned up through the function `update`.
    dag: Dag,
    // Log containing slots and committed certificates
    log: HashMap<Slot, ConsensusMessage>,
    // The last executed slot
    last_executed_slot: Slot,
}

impl State {
    fn new(genesis: Vec<Certificate>) -> Self {
        let genesis = genesis
            .into_iter()
            .map(|x| (x.origin(), (x.digest(), x)))
            .collect::<HashMap<_, _>>();

        Self {
            last_committed_round: 0,
            last_executed_heights: genesis.iter().map(|(x, (_, y))| (*x, 0)).collect(),
            dag: [(0, genesis)].iter().cloned().collect(),
            log: HashMap::new(),
            last_executed_slot: 0,
        }
    }

    /// Update and clean up internal state base on committed certificates.
    fn update(&mut self, certificate: &Certificate, gc_depth: Height) {
        self.last_executed_heights
            .entry(certificate.origin())
            .and_modify(|r| *r = max(*r, certificate.height()))
            .or_insert_with(|| certificate.height());

        let last_committed_round = *self.last_executed_heights.values().max().unwrap();
        self.last_committed_round = last_committed_round;

        for (name, round) in &self.last_executed_heights {
            self.dag.retain(|r, authorities| {
                authorities.retain(|n, _| n != name || r >= round);
                !authorities.is_empty() && r + gc_depth >= last_committed_round
            });
        }
    }
}

pub struct Committer {
    gc_depth: Height,
    rx_mempool: Receiver<Certificate>,
    rx_deliver: Receiver<Certificate>,
    rx_commit_message: Receiver<ConsensusMessage>,
    tx_output: Sender<Header>,
    synchronizer: Synchronizer,
    genesis: Vec<Certificate>,
    /// Our own local workers' `primary_to_worker` addresses, keyed by id (benchmark-only:
    /// used to notify a worker of the batches it just saw committed, PHASE2-SPEC.md #5).
    #[cfg(feature = "benchmark")]
    worker_addresses: HashMap<WorkerId, SocketAddr>,
    #[cfg(feature = "benchmark")]
    network: SimpleSender,
}

impl Committer {
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
    ) {
        let (tx_deliver, rx_deliver) = channel(CHANNEL_CAPACITY);

        let genesis = Certificate::genesis(&committee);

        //special blocks from round >1 can also have genesis as parent!!! ==> Solution: Write genesis to store
        //Alternatively, just store genesis digests and compare against
        //let genesis_digests = genesis.clone().iter().map(|x| x.digest()).collect();

        #[cfg(feature = "benchmark")]
        let worker_addresses: HashMap<WorkerId, SocketAddr> = committee
            .our_workers_by_id(&name)
            .expect("Our public key or worker id is not in the committee")
            .into_iter()
            .map(|(id, address)| (id, address.primary_to_worker))
            .collect();

        tokio::spawn(async move {
            Self {
                gc_depth,
                rx_mempool,
                rx_deliver,
                rx_commit_message,
                tx_output,
                synchronizer,
                genesis,
                #[cfg(feature = "benchmark")]
                worker_addresses,
                #[cfg(feature = "benchmark")]
                network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn process_commit_message(&mut self, state: &mut State, commit_message: ConsensusMessage) {
        match commit_message.clone() {
            ConsensusMessage::Commit{slot, view: _, qc: _, proposals: _} => {
                if slot <= state.last_executed_slot {
                    debug!("Already committed slot {}", slot);
                    return;
                }

                // Store the commit message if all proposals are ready to be processed
                state.log.insert(slot, commit_message);

                while state.log.contains_key(&(state.last_executed_slot + 1)) {
                    let current_commit_message = state.log.get(&(state.last_executed_slot + 1)).unwrap();
                    debug!("Currently executing slot {:?}", state.last_executed_slot + 1);
                    match current_commit_message {
                        ConsensusMessage::Commit { slot: _, view: _, qc: _, proposals } => {
                            for (pk, proposal) in proposals {
                                let stop_height = *state.last_executed_heights.get(pk).unwrap();
                                // Don't execute proposals which are too old
                                if proposal.height <= stop_height {
                                    debug!("skipping this proposal because it's too old");
                                    continue;
                                }

                                let headers = self.synchronizer.get_all_headers_for_proposal(proposal.clone(), stop_height)
                                    .await
                                    .expect("should have ancestors by now");

                                // Update last executed height for the lane
                                if proposal.height > stop_height {
                                    state.last_executed_heights.insert(*pk, proposal.height);
                                }

                                // Commit all of the headers
                                for header in headers {
                                    info!("Committed {}", header);
                                    #[cfg(feature = "benchmark")]
                                    {
                                        for digest in header.payload.keys() {
                                            // NOTE: This log entry is used to compute performance.
                                            info!("Committed {} -> {:?}", header, digest);
                                        }

                                        // Commit instant (PHASE2-SPEC.md #5, amended): taken
                                        // once per header, right at the "Committed" log site,
                                        // and carried in the notification itself so the
                                        // worker's latency measurement is submission -> this
                                        // exact instant -- not submission -> whenever the
                                        // worker's queue got around to the notification.
                                        let commit_millis = SystemTime::now()
                                            .duration_since(UNIX_EPOCH)
                                            .expect("Failed to measure time")
                                            .as_millis() as u64;

                                        // Notify our own local worker(s), grouped by
                                        // WorkerId, of the batches just committed so they
                                        // can extract real transaction latency
                                        // (PHASE2-SPEC.md #5). Routed to *our* worker with
                                        // the same id as the header author's -- batches are
                                        // gossiped worker-to-worker by matching id, so our
                                        // local worker likely holds a replica even for a
                                        // remote author's batch; a store miss is fine
                                        // (worker-side `latency_misses`, never blocks).
                                        let mut by_worker: HashMap<WorkerId, Vec<Digest>> = HashMap::new();
                                        for (digest, worker_id) in header.payload.iter() {
                                            by_worker.entry(*worker_id).or_default().push(digest.clone());
                                        }
                                        for (worker_id, digests) in by_worker {
                                            if let Some(address) = self.worker_addresses.get(&worker_id) {
                                                let bytes = bincode::serialize(&PrimaryWorkerMessage::Committed(commit_millis, digests))
                                                    .expect("Failed to serialize committed message");
                                                self.network.send(*address, Bytes::from(bytes)).await;
                                            }
                                        }
                                    }
                                    debug!("Finished Commit");
                                    // Output the block to the top-level application.
                                    if let Err(e) = self.tx_output.send(header.clone()).await {
                                        debug!("Failed to send block through the output channel: {}", e);
                                    }
                                    debug!("Finish upcall");
                                }
                            }
                            state.last_executed_slot += 1;
                        },
                        _ => {}
                    }
                }

            },
            _ => {},
        };
    }

    async fn run(&mut self) {
        // The consensus state (everything else is immutable).
        let mut state = State::new(self.genesis.clone());

        loop {
            tokio::select! {
                Some(_) = self.rx_mempool.recv() => {
                    // Add the new certificate to the local storage.
                    /*state.dag.entry(certificate.height()).or_insert_with(HashMap::new).insert(
                        certificate.origin(),
                        (certificate.digest(), certificate.clone()),
                    );*/
                },
                Some(commit_message) = self.rx_commit_message.recv() => {
                    self.process_commit_message(state.borrow_mut(), commit_message).await;
                },
                Some(_) = self.rx_deliver.recv() => {}

            }
        }
    }

    /// Flatten the dag referenced by the input certificate. This is a classic depth-first search (pre-order):
    /// https://en.wikipedia.org/wiki/Tree_traversal#Pre-order
    fn order_dag(&self, tip: &Certificate, state: &State) -> Vec<Certificate> {
        debug!("Processing sub-dag of {:?}", tip);
        let ordered = Vec::new();
        /*let mut already_ordered = HashSet::new();

        let dummy = (Digest::default(), Certificate::default());


        let mut buffer = vec![tip];
        while let Some(x) = buffer.pop() {
            debug!("Sequencing {:?}", x);
            ordered.push(x.clone());

            for parent in &x.header.parents.clone() {

                let parent_digest;
                let round;

                parent_digest = parent;
                debug!("Trying to sequence normal Dag parent: {}", parent_digest);
                round = x.round() -1;

                let (digest, certificate) = match state
                    .dag
                    .get(&(round))                                           // returns Some(HashMap<key, value>)
                    .map(|x| x.values().find(|(x, _)| x == parent_digest))   // x := Some(key, value); where key = pubkey, value = (dig, cert) ==> maps to Some(value)
                    .flatten()                                               // result is something like Some(<Some(value)>)? => Flatten gets rid of outer Some
                {
                    Some(x) => x,
                    None => {
                        debug!("We already processed and cleaned up {}", parent_digest);
                        continue; // We already ordered or GC up to here.
                    }
                };

                // We skip the certificate if we (1) already processed it or (2) we reached a round that we already
                // committed for this authority.
                let mut skip = already_ordered.contains(&digest);
                skip |= state
                    .last_committed
                    .get(&certificate.origin())
                    .map_or_else(|| false, |r| r == &certificate.round());   //stop if last committed = the round we'd evaluate next
                if !skip {
                    buffer.push(certificate);
                    already_ordered.insert(digest);
                    debug!("Adding Dag parent to sequence: {}", parent_digest);
                }

            }

            if x.header.is_special && x.header.special_parent.is_some() { // i.e. is special edge ==> manually hack the digest (only works because of requirement that header is from same node in prev round)
                //Currently we can skip rounds. Header needs to include parent round to solve this.
                //Note: process_header verifies that author and rounds are correct.


                //generate digest of dummy cert
                let mut hasher = Sha512::new();
                hasher.update(&x.header.special_parent.as_ref().unwrap()); //== parent_header.id
                hasher.update(&x.header.special_parent_round.to_le_bytes());
                hasher.update(&x.header.origin()); //parent_header.origin = child_header_origin
                let parent_digest = Digest(hasher.finalize().as_slice()[..32].try_into().unwrap());
                debug!("Trying to sequence special parent header: {}, dummy cert digest {}", x.header.special_parent.as_ref().unwrap(), parent_digest);

                let round = x.header.special_parent_round;

                let mut skip: bool = false;



                let (digest, certificate) = match state
                    .dag
                    .get(&(round))                                           // returns Some(HashMap<key, value>)
                    .map(|x| x.values().find(|(x, _)| x == &parent_digest))   // x := Some(key, value); where key = pubkey, value = (dig, cert) ==> maps to Some(value)
                    .flatten()                                               // result is something like Some(<Some(value)>)? => Flatten gets rid of outer Some
                {
                    Some(x) => x,
                    None => {
                        debug!("We already processed and cleaned up {}", parent_digest);
                        skip = true; // We already ordered or GC up to here.
                        &dummy
                    }
                };

                // We skip the certificate if we (1) already processed it or (2) we reached a round that we already
                // committed for this authority.
                skip |= already_ordered.contains(&digest);
                skip |= state
                    .last_committed
                    .get(&certificate.origin())
                    .map_or_else(|| false, |r| r == &certificate.round());   //stop if last committed = the round we'd evaluate next
                if !skip {
                    buffer.push(certificate);
                    already_ordered.insert(digest);
                    debug!("Adding special Dag parent to sequence: {}", parent_digest);
                }
            }

        }

        // Ensure we do not commit garbage collected certificates.
        ordered.retain(|x| x.round() + self.gc_depth >= state.last_committed_round);

        // Ordering the output by round is not really necessary but it makes the commit sequence prettier.
        ordered.sort_by_key(|x| x.round());*/
        ordered
    }
}

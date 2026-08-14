use std::collections::HashMap;

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Certificate, ConsensusMessage, Header};
use crate::primary::Height;
use config::{Committee, WorkerId};
use crypto::{Digest, Hash, PublicKey, SignatureService};
use log::debug;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;

/// Creates headers and sends them to the core.
pub struct Proposer {
    /// The public key of this primary.
    name: PublicKey,
    /// Service to sign headers.
    signature_service: SignatureService,
    /// The size of the headers' payload.
    header_size: usize,
    /// The maximum delay to wait for batches' digests.
    max_header_delay: u64,

    /// Receives parent certificates for the next header.
    rx_core: Receiver<Certificate>,
    /// Receives the batches' digests from our workers.
    rx_workers: Receiver<(Digest, WorkerId)>,
    /// Receives consensus instances.
    rx_instance: Receiver<ConsensusMessage>,
    /// Sends newly created headers to the `Core`.
    tx_core: Sender<Header>,

    /// Current chain height.
    height: Height,
    /// Parent certificate for the next header.
    last_parent: Option<Certificate>,
    /// Consensus information for the current header.
    consensus_instances: HashMap<Digest, ConsensusMessage>,
    /// Holds the batches' digests waiting to be included in the next header.
    digests: Vec<(Digest, WorkerId)>,
    /// Keeps track of the size (in bytes) of batches' digests that we received so far.
    payload_size: usize,

    num_active_instances: usize,
    use_special_rule: bool,
    is_special: bool,
}

impl Proposer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        header_size: usize,
        max_header_delay: u64,
        rx_core: Receiver<Certificate>,
        rx_workers: Receiver<(Digest, WorkerId)>,
        rx_instance: Receiver<ConsensusMessage>,
        tx_core: Sender<Header>,
    ) {
        let genesis = Certificate::genesis_for(name, &committee);

        tokio::spawn(async move {
            Self {
                name,
                signature_service,
                header_size,
                max_header_delay,
                rx_core,
                rx_workers,
                rx_instance,
                tx_core,
                height: 0,
                last_parent: Some(genesis),
                consensus_instances: HashMap::new(),
                digests: Vec::with_capacity(2 * header_size),
                payload_size: 0,
                num_active_instances: 0,
                use_special_rule: false,
                is_special: false,
            }
            .run()
            .await;
        });
    }

    async fn make_header(&mut self) {
        // Build a new header.
        debug!("digests size before is {:?}", self.digests.len());

        let mut header = Header::new(
            self.name,
            self.height,
            self.digests.drain(..).collect(),
            self.last_parent.clone().unwrap(),
            &mut self.signature_service,
            self.consensus_instances.clone(),
            self.num_active_instances,
        )
        .await;

        if self.is_special {
            header.special = true;
        }

        debug!("Created {:?}", header);

        for digest in header.consensus_messages.keys() {
            debug!("Header has {:?}", digest);
        }

        #[cfg(feature = "benchmark")]
        for digest in header.payload.keys() {
            // Parsed by benchmark tooling.
            debug!("Created {} -> {:?}", header, digest);
        }

        self.last_parent = None;
        self.consensus_instances.clear();
        self.num_active_instances = 0;

        self.tx_core
            .send(header)
            .await
            .expect("Failed to send header");
    }

    /// Processes incoming messages.
    pub async fn run(&mut self) {
        debug!("Dag starting at round {}", self.height);

        let timer = sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(timer);
        let mut current_time = Instant::now();

        loop {
            // Propose when the parent, payload, timer, or special-block condition is ready.
            let enough_parent = self.last_parent.is_some();
            let enough_digests = self.payload_size >= self.header_size;
            let timer_expired = timer.is_elapsed();

            if (timer_expired || enough_digests) && (enough_parent || self.is_special) {
                if timer_expired {
                    debug!("Timer expired for height {}", self.height);
                }

                debug!(
                    "New car proposed after {:?} ms",
                    current_time.elapsed().as_millis()
                );
                debug!("is special is {:?}", self.is_special);
                current_time = Instant::now();

                self.make_header().await;
                self.payload_size = 0;

                let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                timer.as_mut().reset(deadline);
            }

            tokio::select! {
                // Receive consensus information.
                Some(info) = self.rx_instance.recv() => {
                    debug!("received consensus info");

                    let digest = info.digest();
                    if self.consensus_instances.contains_key(&digest) {
                        debug!("ignoring duplicate consensus info {}", digest);
                        continue;
                    }

                    match &info {
                        ConsensusMessage::Prepare { slot: _, view: _, tc: _, qc_ticket: _, proposals: _} => {
                            if self.use_special_rule {
                                self.is_special = true;
                            }
                            self.num_active_instances +=1;
                            debug!("prepare has digest: {}", info.digest());
                        },
                        ConsensusMessage::Confirm { slot: _, view: _, qc: _, proposals: _} => {
                            if self.use_special_rule {
                                self.is_special = true;
                            }
                            self.num_active_instances +=1;
                        },
                        _ => {},
                    }

                    self.consensus_instances.insert(digest, info);
                }

                // Receive the local parent certificate.
                Some(parent) = self.rx_core.recv() => {
                    debug!("   received parent from height {:?}", parent.height);

                    if parent.height < self.height {
                        continue;
                    }

                    self.height += 1;
                    debug!("Chain moved to height {}", self.height);

                    self.last_parent = Some(parent.clone());
                }

                Some((digest, worker_id)) = self.rx_workers.recv() => {
                    self.payload_size += digest.size();
                    self.digests.push((digest, worker_id));
                }
                () = &mut timer => {
                    // Timer expiration is handled at the top of the loop.
                }
            }
        }
    }
}

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::Certificate;
use crate::primary::PrimaryWorkerMessage;
use bytes::Bytes;
use config::Committee;
use crypto::Hash as _;
use crypto::PublicKey;
use metrics::Metrics;
use network::{BatchConfig, SimpleSender};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

/// Receives the highest round reached by consensus and update it for all tasks.
pub struct GarbageCollector {
    /// The persistent storage.
    store: Store,
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// Receives the ordered certificates from consensus.
    rx_consensus: Receiver<Certificate>,
    /// A loopback channel to the primary's core.
    tx_loopback: Sender<Certificate>,
    /// The network addresses of our workers.
    addresses: Vec<SocketAddr>,
    /// A network sender to notify our workers of cleanup events.
    network: SimpleSender,
    /// SECURITY (Fable audit): this authority's own public key -- the worker<->primary
    /// channel is intra-authority, so the tag every `Cleanup` notification carries is
    /// always keyed `k_{name,name}` (see `PairwiseKeys::build`'s doc comment),
    /// identical for every one of our own workers regardless of `WorkerId`.
    name: PublicKey,
    /// `Parameters::authenticate_channels`; `None` is byte-identical to pre-MAC
    /// behavior.
    channel_auth: Option<Arc<crypto::PairwiseKeys>>,
}

impl GarbageCollector {
    // clippy::too_many_arguments: see `Committer::spawn`'s identical justification.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: &PublicKey,
        committee: &Committee,
        store: Store,
        consensus_round: Arc<AtomicU64>,
        rx_consensus: Receiver<Certificate>,
        tx_loopback: Sender<Certificate>,
        // METRICS-DASHBOARD-SPEC.md §1: appended last, same convention as `Core::spawn`.
        metrics: Arc<Metrics>,
        // METRICS-DASHBOARD-SPEC.md §8: appended last, same convention.
        compress_network: bool,
        // Transport-level batching: appended last, same convention.
        batch: BatchConfig,
        // SECURITY (Fable audit): appended last, same convention as every other
        // MAC-consuming `::spawn`.
        channel_auth: Option<Arc<crypto::PairwiseKeys>>,
    ) {
        let addresses = committee
            .our_workers(name)
            .expect("Our public key or worker id is not in the committee")
            .iter()
            .map(|x| x.primary_to_worker)
            .collect();
        let name = *name;

        tokio::spawn(async move {
            Self {
                store,
                consensus_round,
                rx_consensus,
                tx_loopback,
                addresses,
                network: SimpleSender::new().with_metrics(metrics).with_compression(compress_network).with_batching(batch),
                name,
                channel_auth,
            }
            .run()
            .await;
        });
    }

    /// SECURITY (Fable audit): appends a tag keyed `k_{name,name}` (byte-identical,
    /// unappended, when `channel_auth` is off) before broadcasting to our own workers.
    fn tag(&self, payload: Vec<u8>) -> Bytes {
        match &self.channel_auth {
            None => Bytes::from(payload),
            Some(auth) => {
                let tag = auth.tag_for(&self.name, &payload).expect("self is a committee member");
                let mut tagged = payload;
                tagged.extend_from_slice(&tag);
                Bytes::from(tagged)
            }
        }
    }

    async fn run(&mut self) {
        let mut last_committed_round = 0;
        while let Some(certificate) = self.rx_consensus.recv().await {
            // TODO [issue #9]: Re-include batch digests that have not been sequenced into our next block.

            // Loop back the certificate from HotStuff in case we haven't seen it.
            if self
                .store
                .read(certificate.digest().to_vec())
                .await
                .expect("Failed to read from store")
                .is_none()
            {
                self.tx_loopback
                    .send(certificate.clone())
                    .await
                    .expect("Failed to loop back certificate to core");
            }

            // Cleanup all the modules.
            let round = certificate.height();
            if round > last_committed_round {
                last_committed_round = round;

                // Trigger cleanup on the primary.
                self.consensus_round.store(round, Ordering::Relaxed);

                // Trigger cleanup on the workers..
                let bytes = bincode::serialize(&PrimaryWorkerMessage::Cleanup(round))
                    .expect("Failed to serialize our own message");
                self.network
                    .broadcast_typed(self.addresses.clone(), self.tag(bytes), "Cleanup")
                    .await;
            }
        }
    }
}

// Copyright(C) Facebook, Inc. and its affiliates.
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use log::{error, warn};
use metrics::Metrics;
use network::{BatchConfig, BlipGate, SimpleSender};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use store::Store;
use tokio::sync::mpsc::Receiver;

#[cfg(test)]
#[path = "tests/helper_tests.rs"]
pub mod helper_tests;

/// A task dedicated to help other authorities by replying to their batch requests.
pub struct Helper {
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Input channel to receive batch requests.
    rx_request: Receiver<(Vec<Digest>, PublicKey)>,
    /// A network sender to send the batches to the other workers.
    network: SimpleSender,
}

impl Helper {
    // clippy::too_many_arguments: see `worker::batch_maker::BatchMaker::spawn`'s
    // identical justification (the new `batch` param pushed this over the threshold).
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        id: WorkerId,
        committee: Committee,
        store: Store,
        rx_request: Receiver<(Vec<Digest>, PublicKey)>,
        // Fable audit item 4 (WAN latency injection): this authority's own
        // per-destination artificial latency map (same contract/construction as
        // `BatchMaker::spawn`'s -- see its doc comment). Applied to this worker's
        // batch-request replies to other workers, previously undelayed even under a
        // WAN-shaped run.
        latency_map: HashMap<SocketAddr, Duration>,
        // Transient network-level "blip" fault injector: this authority's own gate
        // (same contract/construction as `BatchMaker::spawn`'s -- see its doc
        // comment). Applied to this worker's batch-request replies to other workers.
        blip_gate: Option<Arc<BlipGate>>,
        // METRICS-DASHBOARD-SPEC.md §1: appended last, same convention as primary-side
        // `::spawn` functions.
        metrics: Arc<Metrics>,
        // METRICS-DASHBOARD-SPEC.md §8: appended last, same convention.
        compress_network: bool,
        // Transport-level batching: appended last, same convention.
        batch: BatchConfig,
    ) {
        tokio::spawn(async move {
            Self {
                id,
                committee,
                store,
                rx_request,
                network: SimpleSender::new()
                    .with_latency(latency_map)
                    .with_blip(blip_gate)
                    .with_metrics(metrics)
                    .with_compression(compress_network)
                    .with_batching(batch),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some((digests, origin)) = self.rx_request.recv().await {
            // TODO [issue #7]: Do some accounting to prevent bad nodes from monopolizing our resources.

            // get the requestors address.
            let address = match self.committee.worker(&origin, &self.id) {
                Ok(x) => x.worker_to_worker,
                Err(e) => {
                    warn!("Unexpected batch request: {}", e);
                    continue;
                }
            };

            // Reply to the request (the best we can).
            for digest in digests {
                match self.store.read(digest.to_vec()).await {
                    Ok(Some(data)) => {
                        self.network
                            .send_typed(address, Bytes::from(data), "Batch")
                            .await
                    }
                    Ok(None) => (),
                    Err(e) => error!("{}", e),
                }
            }
        }
    }
}

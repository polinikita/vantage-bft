// Copyright(C) Facebook, Inc. and its affiliates.
use crate::processor::SerializedBatchMessage;
use crate::worker::WorkerMessage;
use bytes::Bytes;
use crypto::PublicKey;
#[cfg(feature = "benchmark")]
use crypto::{Blake3Hasher, Digest};
use log::debug;
#[cfg(feature = "benchmark")]
use log::info;
use metrics::Metrics;
use network::{BatchConfig, SimpleSender};
use std::collections::HashMap;
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

// The message type received by clients. `Bytes` (refcounted),
// not `Vec<u8>` -- the network receiver already hands `TxReceiverHandler::dispatch` an
// owned `Bytes` per transaction; keeping it as `Bytes` end-to-end through the batch
// avoids one full memcpy per transaction that `message.to_vec()` used to perform.
pub type Transaction = Bytes;
// The message type forwarded to quorum waiters.
pub type Batch = Vec<Transaction>;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,

    tx_batch: Sender<SerializedBatchMessage>, // channel to forward batch digest to processor in order for primary to propose.

    /// All worker addresses for this worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Worker addresses allowed by withholding configuration.
    withheld_workers_addresses: Option<Vec<(PublicKey, SocketAddr)>>,
    /// Optional withholding time window.
    withhold_window: Option<Arc<OnceLock<(std::time::Instant, std::time::Instant)>>>,
    /// Current batch.
    current_batch: Batch,
    /// Current batch size in bytes.
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other workers.
    network: SimpleSender,
    /// worker-ingress goodput counters (`submitted_
    /// transactions`/`submitted_transactions_bytes`), observed as each client
    /// transaction arrives, before batching.
    metrics: Arc<Metrics>,
    /// Counts loop iterations to limit explicit scheduling yields.
    loop_ticks: u64,
}

/// See `BatchMaker::loop_ticks`'s doc comment.
const YIELD_EVERY: u64 = 32;

impl BatchMaker {
    // The constructor has more arguments than Clippy's default limit.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>, //receiver channel from worker.TxReceiverHandler
        tx_batch: Sender<SerializedBatchMessage>, // sender channel to worker.Processor
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
        // Filtered worker addresses, if withholding is enabled.
        withheld_workers_addresses: Option<Vec<(PublicKey, SocketAddr)>>,
        // Optional withholding time window.
        withhold_window: Option<Arc<OnceLock<(std::time::Instant, std::time::Instant)>>>,
        // Per-destination network latency.
        latency_map: HashMap<SocketAddr, Duration>,
        // Metrics registry.
        metrics: Arc<Metrics>,
        // Transport-level batching: appended last, same convention.
        batch: BatchConfig,
    ) {
        tokio::spawn(async move {
            Self {
                batch_size,
                max_batch_delay,
                rx_transaction,
                tx_batch,
                workers_addresses,
                withheld_workers_addresses,
                withhold_window,
                current_batch: Batch::with_capacity(batch_size * 2),
                current_batch_size: 0,
                network: SimpleSender::new()
                    .with_latency(latency_map)
                    .with_metrics(metrics.clone())
                    .with_batching(batch),
                metrics,
                loop_ticks: 0,
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);
        let mut current_time = Instant::now();

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    self.metrics.submitted_transactions.inc();
                    self.metrics.submitted_transactions_bytes.inc_by(transaction.len() as u64);
                    self.current_batch_size += transaction.len();
                    self.current_batch.push(transaction);
                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;

                        debug!("batch ready it took {:?} ms", current_time.elapsed().as_millis());
                        current_time = Instant::now();

                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    debug!("BatchMaker: max batch delay timer triggered");
                    if !self.current_batch.is_empty() {
                        self.seal().await;
                    }

                    current_time = Instant::now();
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }

            // Yield periodically under sustained load.
            self.loop_ticks = self.loop_ticks.wrapping_add(1);
            if self.loop_ticks.is_multiple_of(YIELD_EVERY) {
                tokio::task::yield_now().await;
            }
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        let size = self.current_batch_size;

        // our own
        // proposed transactions' total payload size, sealed into this batch. This
        // repo's headers/proposals never carry transaction bytes inline -- only
        // batch digests (`primary::messages::Header::payload:
        // BTreeMap<Digest, WorkerId>`) -- so the batch, not the header, is the
        // closest analogue of reference's per-block `proposed_transaction_size_bytes`
        // observation (see that field's doc comment on `Metrics`). Own batches
        // only, matching `BatchMaker`'s scope: batches received from other workers
        // never pass through here (see `Processor`'s `own_batch` split in
        // `worker::Worker::handle_workers_messages`).
        self.metrics.proposed_transaction_size_bytes.observe(size);

        // Look for sample txs (they all start with 0) and gather their txs id (the next 8 bytes).
        #[cfg(feature = "benchmark")]
        let tx_ids: Vec<_> = self
            .current_batch
            .iter()
            .filter(|tx| tx[0] == 0u8 && tx.len() > 8)
            .filter_map(|tx| tx[1..9].try_into().ok())
            .collect();

        // Serialize the batch.
        self.current_batch_size = 0;
        let batch: Vec<_> = self.current_batch.drain(..).collect();
        let message = WorkerMessage::Batch(batch);
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");
        // wrap the freshly-serialized `Vec<u8>` into `Bytes`
        // once (a cheap pointer/len/cap move, not a copy -- `Bytes::from(Vec<u8>)`
        // takes ownership of the existing allocation). Both consumers below then just
        // clone this `Bytes` handle (a refcount bump, not a memcpy) instead of the
        // previous `Bytes::from(serialized.clone())`, which memcpy'd the whole batch
        // a second time on every single seal.
        let bytes = Bytes::from(serialized);

        #[cfg(feature = "benchmark")]
        {
            // Hash the batch for benchmark logs.
            let mut hasher = Blake3Hasher::new();
            hasher.update(&bytes);
            let digest = Digest(hasher.finalize().into());

            for id in tx_ids {
                info!(
                    "Batch {:?} contains sample tx {}",
                    digest,
                    u64::from_be_bytes(id)
                );
            }

            info!("Batch {:?} contains {} B", digest, size);
        }

        // Broadcast the batch through the network.
        // Apply withholding when the configured time window is active.
        let addresses: Vec<SocketAddr> = match &self.withheld_workers_addresses {
            Some(filtered)
                if config::withhold_active(
                    self.withhold_window.as_deref(),
                    std::time::Instant::now(),
                ) =>
            {
                filtered.iter().map(|(_, addr)| *addr).collect()
            }
            _ => self
                .workers_addresses
                .iter()
                .map(|(_, addr)| *addr)
                .collect(),
        };
        self.network
            .broadcast_typed(addresses, bytes.clone(), "Batch")
            .await;

        self.tx_batch
            .send(bytes)
            .await
            .expect("Failed to deliver batch");

    }
}

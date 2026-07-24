// Copyright(C) Facebook, Inc. and its affiliates.
use crate::processor::SerializedBatchMessage;
use crate::worker::WorkerMessage;
use bytes::Bytes;
#[cfg(feature = "benchmark")]
use crypto::{Blake3Hasher, Digest};
use crypto::{PairwiseKeys, PublicKey};
use log::debug;
#[cfg(feature = "benchmark")]
use log::info;
use metrics::Metrics;
use network::{BatchConfig, SimpleSender};
use std::collections::HashMap;
use std::sync::Arc;
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

// The message type received by clients. Fable perf audit item 1: `Bytes` (refcounted),
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
   
    //tx_message: Sender<QuorumWaiterMessage>,  /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_batch: Sender<SerializedBatchMessage>,   // channel to forward batch digest to processor in order for primary to propose.

    /// The network addresses of the other workers that share our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Holds the current batch.
    current_batch: Batch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other workers.
    network: SimpleSender,
    /// METRICS-DASHBOARD-SPEC.md §2: worker-ingress goodput counters (`submitted_
    /// transactions`/`submitted_transactions_bytes`), observed as each client
    /// transaction arrives, before batching.
    metrics: Arc<Metrics>,
    /// Fable perf audit item 1: counts `run`'s own loop iterations so the trailing
    /// `yield_now` only actually yields every `YIELD_EVERY`-th iteration instead of
    /// literally every one (every single received transaction/timer tick previously
    /// yielded unconditionally) -- purely a scheduling-fairness knob, no protocol
    /// effect either way.
    loop_ticks: u64,
    /// SECURITY (Fable audit): `Parameters::authenticate_channels`. `None` is
    /// byte-identical to pre-MAC behavior. `WorkerMessage::Batch` carries no sender
    /// claim to bind (see `worker::WorkerReceiverHandler::channel_auth`'s doc
    /// comment) -- the tag appended here is `PairwiseKeys::tag_unverified`'s
    /// destination-independent placeholder, computed ONCE per seal (not once per
    /// destination): correct, since no receiver ever checks it, and essential for
    /// performance, since this is the highest-volume message this worker sends.
    channel_auth: Option<Arc<PairwiseKeys>>,
}

/// See `BatchMaker::loop_ticks`'s doc comment.
const YIELD_EVERY: u64 = 32;

impl BatchMaker {
    // clippy::too_many_arguments: see primary/src/committer.rs's identical
    // justification (Fable audit item 4's new `latency_map` param pushed this over the
    // threshold).
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>, //receiver channel from worker.TxReceiverHandler
        //tx_message: Sender<QuorumWaiterMessage>, //sender channel to worker.QuorumWaiter
        tx_batch: Sender<SerializedBatchMessage>,   // sender channel to worker.Processor
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
        // Fable audit item 4 (WAN latency injection): this authority's own
        // per-destination artificial latency map (same contract as
        // `Core::spawn`/`vantage::node::VantageCore::spawn`'s `latency_map` --
        // resolved once by `Worker::spawn` via `Committee::latency_map`, empty ==
        // current behavior). Applied to worker-to-worker batch broadcast, the
        // dominant bandwidth path a WAN-shaped run previously left undelayed.
        latency_map: HashMap<SocketAddr, Duration>,
        // METRICS-DASHBOARD-SPEC.md §1/§2: appended last, same convention as
        // primary-side `::spawn` functions.
        metrics: Arc<Metrics>,
        // METRICS-DASHBOARD-SPEC.md §8: appended last, same convention.
        compress_network: bool,
        // Transport-level batching: appended last, same convention.
        batch: BatchConfig,
        // SECURITY (Fable audit): appended last, same convention as every other
        // MAC-consuming `::spawn`.
        channel_auth: Option<Arc<PairwiseKeys>>,
    ) {
        tokio::spawn(async move {
            Self {
                batch_size,
                max_batch_delay,
                rx_transaction,
                //tx_message, //previously forwarded batch to Quorum_waiter; now skipping this step.
                tx_batch,
                workers_addresses,
                current_batch: Batch::with_capacity(batch_size * 2),
                current_batch_size: 0,
                network: SimpleSender::new().with_latency(latency_map).with_metrics(metrics.clone()).with_compression(compress_network).with_batching(batch),
                metrics,
                loop_ticks: 0,
                channel_auth,
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

            // Give the chance to schedule other tasks, but not on literally every
            // single loop iteration (Fable perf audit item 1) -- see
            // `BatchMaker::loop_ticks`'s doc comment.
            self.loop_ticks = self.loop_ticks.wrapping_add(1);
            if self.loop_ticks.is_multiple_of(YIELD_EVERY) {
                tokio::task::yield_now().await;
            }
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        #[cfg(feature = "benchmark")]
        let size = self.current_batch_size;

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
        let mut serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");
        // SECURITY (Fable audit): `Batch` carries no sender claim to bind (see
        // `WorkerReceiverHandler::channel_auth`'s doc comment) -- append the
        // destination-independent placeholder tag (byte-identical, unappended, when
        // `channel_auth` is off) BEFORE wrapping in `Bytes`, so every downstream
        // consumer (the benchmark diagnostic hash just below, `Processor`'s real
        // content-addressed digest, and the broadcast itself) all see the exact same
        // final bytes.
        if let Some(auth) = &self.channel_auth {
            let tag = auth.tag_unverified(&serialized);
            serialized.extend_from_slice(&tag);
        }
        // Fable perf audit item 2: wrap the freshly-serialized `Vec<u8>` into `Bytes`
        // once (a cheap pointer/len/cap move, not a copy -- `Bytes::from(Vec<u8>)`
        // takes ownership of the existing allocation). Both consumers below then just
        // clone this `Bytes` handle (a refcount bump, not a memcpy) instead of the
        // previous `Bytes::from(serialized.clone())`, which memcpy'd the whole batch
        // a second time on every single seal.
        let bytes = Bytes::from(serialized);

        #[cfg(feature = "benchmark")]
        {
            // NOTE: This is one extra hash that is only needed to print the following log entries.
            let mut hasher = Blake3Hasher::new();
            hasher.update(&bytes);
            let digest = Digest(hasher.finalize().into());

            for id in tx_ids {
                // NOTE: This log entry is used to compute performance.
                info!(
                    "Batch {:?} contains sample tx {}",
                    digest,
                    u64::from_be_bytes(id)
                );
            }

            // NOTE: This log entry is used to compute performance.
            info!("Batch {:?} contains {} B", digest, size);
        }

        // Broadcast the batch through the network.

        //NEW:
        //Best-effort broadcast only. Any failure is correlated with the primary operating this node (running on same machine)
        let (_, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        self.network.broadcast_typed(addresses, bytes.clone(), "Batch").await;

        self.tx_batch.send(bytes).await.expect("Failed to deliver batch");

        //OLD:
        //This uses reliable sender. The receiver worker will reply with an ack. The Reply Handler is passed to Quorum Waiter.
        // let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        // let bytes = Bytes::from(serialized.clone());
        // let handlers = self.network.broadcast(addresses, bytes).await; 

        // // Send the batch through the deliver channel for further processing.
        // self.tx_message
        //     .send(QuorumWaiterMessage {
        //         batch: serialized,
        //         handlers: names.into_iter().zip(handlers.into_iter()).collect(),
        //     })
        //     .await
        //     .expect("Failed to deliver batch");
    }
}

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::processor::SerializedBatchMessage;
use crate::transaction_counts_toward_goodput;
use crate::worker::WorkerMessage;
use bytes::Bytes;
use crypto::PublicKey;
#[cfg(feature = "benchmark")]
use crypto::{Blake3Hasher, Digest};
use log::debug;
use metrics::Metrics;
use network::{BatchConfig, ChannelAuth, SimpleSender};
use std::collections::HashMap;
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
#[cfg(feature = "pipeline-tracing")]
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

/// Client transaction bytes.
pub type Transaction = Bytes;
/// Serialized batch forwarded to the processor.
pub type Batch = Vec<Transaction>;

/// Deterministic recipients for one Byzantine lane in the optimistic-leader
/// burden profile.
pub(crate) struct LeaderRelayRecipients {
    pub(crate) targets: Vec<(PublicKey, SocketAddr)>,
}

/// Assembles client transactions into batches.
pub struct BatchMaker {
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,

    /// Sends sealed batches to the processor.
    tx_batch: Sender<SerializedBatchMessage>,

    /// All worker addresses for this worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Worker addresses allowed by withholding configuration.
    withheld_workers_addresses: Option<Vec<(PublicKey, SocketAddr)>>,
    /// Fixed correct receivers for this Byzantine lane in the leader-relay
    /// attack.
    leader_relay_workers_addresses: Option<LeaderRelayRecipients>,
    /// Optional withholding time window.
    withhold_window: Option<Arc<OnceLock<(std::time::Instant, std::time::Instant)>>>,
    /// Current batch.
    current_batch: Batch,
    /// Current batch size in bytes.
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other workers.
    network: SimpleSender,
    /// Worker-ingress goodput counters, updated before batching.
    metrics: Arc<Metrics>,
    /// Counts loop iterations to limit explicit scheduling yields.
    loop_ticks: u64,
}

/// Explicit yield interval.
const YIELD_EVERY: u64 = 32;

fn leader_relay_batch_addresses(targets: &[(PublicKey, SocketAddr)]) -> Vec<SocketAddr> {
    targets.iter().map(|(_, address)| *address).collect()
}

impl BatchMaker {
    // The constructor has more arguments than Clippy's default limit.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        tx_batch: Sender<SerializedBatchMessage>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
        withheld_workers_addresses: Option<Vec<(PublicKey, SocketAddr)>>,
        leader_relay_workers_addresses: Option<LeaderRelayRecipients>,
        withhold_window: Option<Arc<OnceLock<(std::time::Instant, std::time::Instant)>>>,
        latency_map: HashMap<SocketAddr, Duration>,
        metrics: Arc<Metrics>,
        batch: BatchConfig,
        auth: Option<Arc<ChannelAuth>>,
    ) {
        tokio::spawn(async move {
            Self {
                batch_size,
                max_batch_delay,
                rx_transaction,
                tx_batch,
                workers_addresses,
                withheld_workers_addresses,
                leader_relay_workers_addresses,
                withhold_window,
                current_batch: Batch::new(),
                current_batch_size: 0,
                network: SimpleSender::new()
                    .with_queue_role("worker_batch")
                    .with_latency(latency_map)
                    .with_metrics(metrics.clone())
                    .with_batching(batch)
                    .with_channel_auth(auth),
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
                Some(transaction) = self.rx_transaction.recv() => {
                    if transaction_counts_toward_goodput(&transaction) {
                        self.metrics.submitted_transactions.inc();
                        self.metrics.submitted_transactions_bytes.inc_by(transaction.len() as u64);
                    }
                    self.current_batch_size += transaction.len();
                    self.current_batch.push(transaction);
                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;

                        debug!("batch ready it took {:?} ms", current_time.elapsed().as_millis());
                        current_time = Instant::now();

                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                () = &mut timer => {
                    debug!("BatchMaker: max batch delay timer triggered");
                    if !self.current_batch.is_empty() {
                        self.seal().await;
                    }

                    current_time = Instant::now();
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }

            self.loop_ticks = self.loop_ticks.wrapping_add(1);
            if self.loop_ticks.is_multiple_of(YIELD_EVERY) {
                tokio::task::yield_now().await;
            }
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        let size = self.current_batch_size;

        #[cfg(feature = "pipeline-tracing")]
        self.observe_transaction_to_batch_seal();

        // Record payload size for batches created by this worker.
        self.metrics.proposed_transaction_size_bytes.observe(size);

        // Extract sample IDs only when debug logging is active.
        #[cfg(feature = "benchmark")]
        let tx_ids = log::log_enabled!(log::Level::Debug).then(|| {
            self.current_batch
                .iter()
                .filter(|tx| tx.len() > 8 && tx[0] == 0u8)
                .filter_map(|tx| tx[1..9].try_into().ok())
                .collect::<Vec<_>>()
        });

        self.current_batch_size = 0;
        let message = WorkerMessage::Batch(std::mem::take(&mut self.current_batch));
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");
        let WorkerMessage::Batch(mut batch) = message else {
            unreachable!("constructed a batch message")
        };
        batch.clear();
        self.current_batch = batch;
        let bytes = Bytes::from(serialized);

        #[cfg(feature = "benchmark")]
        if let Some(tx_ids) = tx_ids {
            let mut hasher = Blake3Hasher::new();
            hasher.update(&bytes);
            let digest = Digest(hasher.finalize().into());

            for id in tx_ids {
                debug!(
                    "Batch {:?} contains sample tx {}",
                    digest,
                    u64::from_be_bytes(id)
                );
            }

            debug!("Batch {:?} contains {} B", digest, size);
        }

        // Apply withholding when its configured window is active.
        let withhold_active =
            config::withhold_active(self.withhold_window.as_deref(), std::time::Instant::now());
        let addresses: Vec<SocketAddr> = if withhold_active {
            match &self.leader_relay_workers_addresses {
                Some(profile) if !profile.targets.is_empty() => {
                    // A fixed group receives every batch in this lane and
                    // therefore holds its complete payload prefix. Groups are
                    // staggered across Byzantine publishers so every correct
                    // leader is burdened by at least one lane.
                    leader_relay_batch_addresses(&profile.targets)
                }
                _ => self
                    .withheld_workers_addresses
                    .as_ref()
                    .unwrap_or(&self.workers_addresses)
                    .iter()
                    .map(|(_, addr)| *addr)
                    .collect(),
            }
        } else {
            self.workers_addresses
                .iter()
                .map(|(_, addr)| *addr)
                .collect()
        };
        self.network
            .broadcast_typed(addresses, bytes.clone(), "Batch")
            .await;

        self.tx_batch
            .send(bytes)
            .await
            .expect("Failed to deliver batch");
    }

    #[cfg(feature = "pipeline-tracing")]
    fn observe_transaction_to_batch_seal(&self) {
        let sealed_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        for transaction in &self.current_batch {
            if transaction.len() < 17 || !transaction_counts_toward_goodput(transaction) {
                continue;
            }
            let submitted_millis =
                u64::from_le_bytes(transaction[9..17].try_into().expect("checked length"));
            if submitted_millis > 0 && self.metrics.counts_toward_metrics(submitted_millis) {
                self.metrics
                    .pipeline
                    .transaction_to_batch_seal_latency
                    .observe(Duration::from_millis(
                        sealed_millis.saturating_sub(submitted_millis),
                    ));
            }
        }
    }
}

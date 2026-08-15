// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::SerializedBatchDigestMessage;
use bytes::Bytes;
use config::WorkerId;
use crypto::{Blake3Hasher, Digest};
#[cfg(feature = "pipeline-tracing")]
use metrics::Metrics;
use primary::WorkerPrimaryMessage;
#[cfg(feature = "pipeline-tracing")]
use std::sync::Arc;
#[cfg(feature = "pipeline-tracing")]
use std::time::Instant;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

/// Serialized `WorkerMessage::Batch` bytes.
pub type SerializedBatchMessage = Bytes;

/// Selects whether storing a batch also advertises its digest to the primary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DigestNotification {
    Our,
    Others,
    /// Post-commit materialization is deliberately invisible to availability
    /// logic: repaired possession must not create first-hand provenance.
    None,
}

/// Hashes and stores batches, then sends their digests to the primary.
pub struct Processor;

/// Maximum batches handled per store submission.
const PROCESS_BATCH_LIMIT: usize = 64;

impl Processor {
    pub fn spawn(
        id: WorkerId,
        mut store: Store,
        mut rx_batch: Receiver<SerializedBatchMessage>,
        tx_digest: Sender<SerializedBatchDigestMessage>,
        digest_notification: DigestNotification,
        #[cfg(feature = "pipeline-tracing")] metrics: Arc<Metrics>,
    ) {
        tokio::spawn(async move {
            while let Some(first) = rx_batch.recv().await {
                let mut batches = Vec::with_capacity(PROCESS_BATCH_LIMIT);
                #[cfg(feature = "pipeline-tracing")]
                batches.push((first, Instant::now()));
                #[cfg(not(feature = "pipeline-tracing"))]
                batches.push(first);
                while batches.len() < PROCESS_BATCH_LIMIT {
                    match rx_batch.try_recv() {
                        #[cfg(feature = "pipeline-tracing")]
                        Ok(batch) => batches.push((batch, Instant::now())),
                        #[cfg(not(feature = "pipeline-tracing"))]
                        Ok(batch) => batches.push(batch),
                        Err(_) => break,
                    }
                }

                let mut writes = Vec::with_capacity(batches.len());
                let mut digests = Vec::with_capacity(batches.len());
                for batch in batches {
                    #[cfg(feature = "pipeline-tracing")]
                    let (batch, started) = batch;
                    let mut hasher = Blake3Hasher::new();
                    hasher.update(&batch);
                    let digest = Digest(hasher.finalize().into());
                    writes.push((digest.to_vec(), batch.to_vec()));
                    #[cfg(feature = "pipeline-tracing")]
                    digests.push((digest, started));
                    #[cfg(not(feature = "pipeline-tracing"))]
                    digests.push(digest);
                }
                store.write_many(writes).await;

                for digest in digests {
                    #[cfg(feature = "pipeline-tracing")]
                    let (digest, started) = digest;
                    #[cfg(feature = "pipeline-tracing")]
                    if digest_notification == DigestNotification::Our {
                        metrics
                            .pipeline
                            .batch_processing_latency
                            .observe(started.elapsed());
                    }
                    let message = match digest_notification {
                        DigestNotification::Our => WorkerPrimaryMessage::OurBatch(digest, id),
                        DigestNotification::Others => WorkerPrimaryMessage::OthersBatch(digest, id),
                        DigestNotification::None => continue,
                    };
                    let message = bincode::serialize(&message)
                        .expect("Failed to serialize our own worker-primary message");
                    tx_digest
                        .send(message)
                        .await
                        .expect("Failed to send digest");
                }
            }
        });
    }
}

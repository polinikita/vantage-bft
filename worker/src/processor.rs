// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::SerializedBatchDigestMessage;
use bytes::Bytes;
use config::WorkerId;
use crypto::{Blake3Hasher, Digest};
use primary::WorkerPrimaryMessage;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

/// Serialized `WorkerMessage::Batch` bytes.
pub type SerializedBatchMessage = Bytes;

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
        // Distinguish local batches from peer batches in the primary message.
        own_digest: bool,
    ) {
        tokio::spawn(async move {
            while let Some(first) = rx_batch.recv().await {
                let mut batches = Vec::with_capacity(PROCESS_BATCH_LIMIT);
                batches.push(first);
                while batches.len() < PROCESS_BATCH_LIMIT {
                    match rx_batch.try_recv() {
                        Ok(batch) => batches.push(batch),
                        Err(_) => break,
                    }
                }

                let mut writes = Vec::with_capacity(batches.len());
                let mut digests = Vec::with_capacity(batches.len());
                for batch in batches {
                    let mut hasher = Blake3Hasher::new();
                    hasher.update(&batch);
                    let digest = Digest(hasher.finalize().into());
                    writes.push((digest.to_vec(), batch.to_vec()));
                    digests.push(digest);
                }
                store.write_many(writes).await;

                for digest in digests {
                    let message = match own_digest {
                        true => WorkerPrimaryMessage::OurBatch(digest, id),
                        false => WorkerPrimaryMessage::OthersBatch(digest, id),
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

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

/// Indicates a serialized `WorkerMessage::Batch` message. Fable perf audit item 2/3:
/// `Bytes` (refcounted) rather than `Vec<u8>` -- both `BatchMaker::seal` (our own
/// batches) and `WorkerReceiverHandler::dispatch` (others' batches) already hold the
/// wire bytes as a `Bytes` and can hand it here without copying. `Store::write` still
/// needs an owned `Vec<u8>` (its fixed `Value = Vec<u8>` API, out of this audit's
/// scope) -- that one unavoidable copy now happens right here, at the point a write
/// actually needs `Vec`, instead of earlier on the sender's hot broadcast path.
pub type SerializedBatchMessage = Bytes;

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct Processor;

impl Processor {
    pub fn spawn(
        // Our worker's id.
        id: WorkerId,
        // The persistent storage.
        mut store: Store,
        // Input channel to receive batches.
        mut rx_batch: Receiver<SerializedBatchMessage>,
        // Output channel to send out batches' digests.
        tx_digest: Sender<SerializedBatchDigestMessage>,    //sender channel connects to PrimaryConnector
        // Whether we are processing our own batches or the batches of other nodes.
        own_digest: bool,
    ) {
        tokio::spawn(async move {
            while let Some(batch) = rx_batch.recv().await {
                // Hash the batch.
                let mut hasher = Blake3Hasher::new();
                hasher.update(&batch);
                let digest = Digest(hasher.finalize().into());

                // Store the batch. `Store::write` needs an owned `Vec<u8>` -- this is
                // the one owned copy `batch: Bytes` truly can't avoid (see
                // `SerializedBatchMessage`'s doc comment above).
                store.write(digest.to_vec(), batch.to_vec()).await;
                //store.write(digest.to_vec(), Vec::default()).await;

                // Deliver the batch's digest.
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
        });
    }
}

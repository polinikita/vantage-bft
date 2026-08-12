// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::batch;
use crate::worker::WorkerMessage;
use std::fs;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn hash_and_store() {
    let (tx_batch, rx_batch) = channel(1);
    let (tx_digest, mut rx_digest) = channel(1);

    let path = ".db_test_hash_and_store";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    let id = 0;
    Processor::spawn(
        id,
        store.clone(),
        rx_batch,
        tx_digest,
        true,
        #[cfg(feature = "pipeline-tracing")]
        Metrics::new(&prometheus::Registry::new()).0,
    );

    let message = WorkerMessage::Batch(batch());
    let serialized = bincode::serialize(&message).unwrap();
    let bytes = Bytes::from(serialized.clone());
    tx_batch.send(bytes).await.unwrap();

    let output = rx_digest.recv().await.unwrap();
    let mut hasher = Blake3Hasher::new();
    hasher.update(&serialized);
    let digest = Digest(hasher.finalize().into());
    let expected = bincode::serialize(&WorkerPrimaryMessage::OurBatch(digest.clone(), id)).unwrap();
    assert_eq!(output, expected);

    let stored_batch = store.read(digest.to_vec()).await.unwrap();
    assert!(stored_batch.is_some(), "The batch is not in the store");
    assert_eq!(stored_batch.unwrap(), serialized);
}

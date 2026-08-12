// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::transaction;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn make_batch() {
    let (tx_transaction, rx_transaction) = channel(1);
    let (tx_message, mut rx_message) = channel(1);
    let dummy_addresses = vec![(PublicKey::default(), "127.0.0.1:0".parse().unwrap())];

    BatchMaker::spawn(
        200,
        1_000_000,
        rx_transaction,
        tx_message,
        dummy_addresses,
        None,
        None,
        std::collections::HashMap::new(),
        Metrics::new(&prometheus::Registry::new()).0,
        BatchConfig::default(),
    );

    tx_transaction.send(transaction()).await.unwrap();
    tx_transaction.send(transaction()).await.unwrap();

    let expected_batch = vec![transaction(), transaction()];
    let batch = rx_message.recv().await.unwrap();
    match bincode::deserialize(&batch).unwrap() {
        WorkerMessage::Batch(batch) => assert_eq!(batch, expected_batch),
        _ => panic!("Unexpected message"),
    }
}

#[tokio::test]
async fn batch_timeout() {
    let (tx_transaction, rx_transaction) = channel(1);
    let (tx_message, mut rx_message) = channel(1);
    let dummy_addresses = vec![(PublicKey::default(), "127.0.0.1:0".parse().unwrap())];

    BatchMaker::spawn(
        200,
        50,
        rx_transaction,
        tx_message,
        dummy_addresses,
        None,
        None,
        std::collections::HashMap::new(),
        Metrics::new(&prometheus::Registry::new()).0,
        BatchConfig::default(),
    );

    tx_transaction.send(transaction()).await.unwrap();

    let expected_batch = vec![transaction()];
    let batch = rx_message.recv().await.unwrap();
    match bincode::deserialize(&batch).unwrap() {
        WorkerMessage::Batch(batch) => assert_eq!(batch, expected_batch),
        _ => panic!("Unexpected message"),
    }
}

#[cfg(feature = "pipeline-tracing")]
#[tokio::test]
async fn pipeline_metric_observes_every_transaction() {
    let (tx_transaction, rx_transaction) = channel(2);
    let (tx_message, mut rx_message) = channel(1);
    let registry = prometheus::Registry::new();
    let (metrics, reporter) = Metrics::new(&registry);
    let dummy_addresses = vec![(PublicKey::default(), "127.0.0.1:0".parse().unwrap())];

    BatchMaker::spawn(
        200,
        1_000_000,
        rx_transaction,
        tx_message,
        dummy_addresses,
        None,
        None,
        std::collections::HashMap::new(),
        metrics,
        BatchConfig::default(),
    );

    let submitted_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    for marker in [0, 1] {
        let mut transaction = vec![marker; 100];
        transaction[9..17].copy_from_slice(&submitted_millis.to_le_bytes());
        tx_transaction.send(Bytes::from(transaction)).await.unwrap();
    }
    rx_message.recv().await.unwrap();
    reporter.force_report();

    let snapshot = metrics::read_duration_snapshot(&registry, "transaction_to_batch_seal_latency")
        .expect("pipeline metric");
    assert_eq!(snapshot.count, 2);
}

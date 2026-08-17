// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{batch_digest, committee_with_base_port, keys, listener, serialized_batch};
use std::fs;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn batch_reply() {
    let (tx_request, rx_request) = channel(1);
    let (requestor, _) = keys().pop().unwrap();
    let id = 0;
    let committee = committee_with_base_port(8_000);

    let path = ".db_test_batch_reply";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    store
        .write(batch_digest().to_vec(), serialized_batch())
        .await;

    Helper::spawn(
        id,
        committee.clone(),
        store,
        rx_request,
        std::collections::HashMap::new(),
        Metrics::new(&prometheus::Registry::new()).0,
        BatchConfig::default(),
        None,
        None,
        None,
    );

    let address = committee.worker(&requestor, &id).unwrap().worker_to_worker;
    let expected = Bytes::from(serialized_batch());
    let handle = listener(address, Some(expected));

    let digests = vec![batch_digest()];
    tx_request
        .send((digests, requestor, false, false))
        .await
        .unwrap();

    assert!(handle.await.is_ok());
}

#[tokio::test]
async fn byzantine_author_does_not_reply_to_repair_requests() {
    let (tx_request, rx_request) = channel(1);
    let (requestor, _) = keys().pop().unwrap();
    let id = 0;
    let committee = committee_with_base_port(8_100);

    let path = ".db_test_silent_batch_reply";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();
    store
        .write(batch_digest().to_vec(), serialized_batch())
        .await;
    let suppressed = Some(std::iter::once(requestor).collect());

    Helper::spawn(
        id,
        committee.clone(),
        store,
        rx_request,
        std::collections::HashMap::new(),
        Metrics::new(&prometheus::Registry::new()).0,
        BatchConfig::default(),
        None,
        suppressed,
        None,
    );

    let address = committee.worker(&requestor, &id).unwrap().worker_to_worker;
    let handle = listener(address, None);
    tx_request
        .send((vec![batch_digest()], requestor, false, false))
        .await
        .unwrap();

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), handle)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn committed_batch_reply_wraps_original_batch_bytes() {
    let (tx_request, rx_request) = channel(1);
    let (requestor, _) = keys().pop().unwrap();
    let id = 0;
    let committee = committee_with_base_port(8_200);

    let path = ".db_test_committed_batch_reply";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();
    let original = serialized_batch();
    store.write(batch_digest().to_vec(), original.clone()).await;

    Helper::spawn(
        id,
        committee.clone(),
        store,
        rx_request,
        std::collections::HashMap::new(),
        Metrics::new(&prometheus::Registry::new()).0,
        BatchConfig::default(),
        None,
        None,
        None,
    );

    let expected = bincode::serialize(&WorkerMessage::CommittedBatch(original)).unwrap();
    let address = committee.worker(&requestor, &id).unwrap().worker_to_worker;
    let handle = listener(address, Some(Bytes::from(expected)));
    tx_request
        .send((vec![batch_digest()], requestor, false, true))
        .await
        .unwrap();

    assert!(handle.await.is_ok());
}

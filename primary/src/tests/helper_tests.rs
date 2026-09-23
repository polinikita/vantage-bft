// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{committee_with_base_port, header, keys, listener};
use serial_test::serial;
use std::{fs, time::Duration};
use tokio::sync::mpsc::channel;

fn test_metrics() -> Arc<Metrics> {
    Metrics::new(&prometheus::Registry::new()).0
}

async fn stored_header(path: &str) -> (Store, Header) {
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();
    let header = header();
    store
        .write(header.id.to_vec(), bincode::serialize(&header).unwrap())
        .await;
    (store, header)
}

#[tokio::test]
#[serial]
async fn honest_author_replies_to_header_repair_requests() {
    let committee = committee_with_base_port(24_000);
    let requestor = keys()[0].0;
    let (store, header) = stored_header(".db_test_primary_helper_reply").await;
    let (tx_certificates, rx_certificates) = channel(1);
    let (tx_headers, rx_headers) = channel(1);
    let (tx_proposals, rx_proposals) = channel(1);

    Helper::spawn(
        committee.clone(),
        store,
        rx_certificates,
        rx_headers,
        rx_proposals,
        test_metrics(),
        BatchConfig::default(),
        None,
        None,
        None,
        crate::verified::VerifiedCache::for_committee(&committee),
    );
    drop(tx_certificates);
    drop(tx_proposals);

    let address = committee.primary(&requestor).unwrap().primary_to_primary;
    let response = listener(address);
    tokio::task::yield_now().await;
    tx_headers
        .send((vec![header.id.clone()], requestor, true))
        .await
        .unwrap();

    let bytes = tokio::time::timeout(Duration::from_secs(2), response)
        .await
        .expect("honest helper did not answer")
        .expect("header listener failed");
    let message: PrimaryMessage = bincode::deserialize(&bytes).unwrap();
    assert!(matches!(
        message,
        PrimaryMessage::Header(received, true) if received == header
    ));
}

#[tokio::test]
#[serial]
async fn byzantine_author_does_not_reply_to_header_repair_requests() {
    let committee = committee_with_base_port(25_000);
    let requestor = keys()[0].0;
    let (store, header) = stored_header(".db_test_primary_helper_silent").await;
    let (_tx_certificates, rx_certificates) = channel(1);
    let (tx_headers, rx_headers) = channel(1);
    let (_tx_proposals, rx_proposals) = channel(1);
    let suppressed = Some(std::iter::once(requestor).collect());

    Helper::spawn(
        committee.clone(),
        store,
        rx_certificates,
        rx_headers,
        rx_proposals,
        test_metrics(),
        BatchConfig::default(),
        None,
        suppressed,
        None,
        crate::verified::VerifiedCache::for_committee(&committee),
    );

    let address = committee.primary(&requestor).unwrap().primary_to_primary;
    let mut response = listener(address);
    tokio::task::yield_now().await;
    tx_headers
        .send((vec![header.id], requestor, true))
        .await
        .unwrap();

    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut response)
            .await
            .is_err(),
        "Byzantine helper unexpectedly served a withheld header"
    );
    response.abort();
}

/// Spawns a helper over `store` and returns the suffix-request sender.
fn spawn_suffix_helper(
    committee: &Committee,
    store: Store,
) -> tokio::sync::mpsc::Sender<(Proposal, Height, PublicKey)> {
    let (_tx_certificates, rx_certificates) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_proposals, rx_proposals) = channel(1);
    Helper::spawn(
        committee.clone(),
        store,
        rx_certificates,
        rx_headers,
        rx_proposals,
        test_metrics(),
        BatchConfig::default(),
        None,
        None,
        None,
        crate::verified::VerifiedCache::for_committee(committee),
    );
    tx_proposals
}

#[tokio::test]
#[serial]
async fn serves_poa_free_optimistic_tip() {
    let committee = committee_with_base_port(26_000);
    let requestor = keys()[0].0;
    let (store, header) = stored_header(".db_test_primary_helper_optimistic_tip").await;
    let tx_proposals = spawn_suffix_helper(&committee, store);

    let address = committee.primary(&requestor).unwrap().primary_to_primary;
    let response = listener(address);
    tokio::task::yield_now().await;
    tx_proposals
        .send((Proposal::optimistic(&header), header.height - 1, requestor))
        .await
        .unwrap();

    let bytes = tokio::time::timeout(Duration::from_secs(2), response)
        .await
        .expect("helper did not serve the optimistic tip")
        .expect("proposal listener failed");
    let message: PrimaryMessage = bincode::deserialize(&bytes).unwrap();
    assert!(matches!(
        message,
        PrimaryMessage::ProposalHeaders(received) if received == vec![header]
    ));
}

#[tokio::test]
#[serial]
async fn ignores_poa_free_request_with_wrong_height() {
    let committee = committee_with_base_port(27_000);
    let requestor = keys()[0].0;
    let (store, header) = stored_header(".db_test_primary_helper_wrong_height").await;
    let tx_proposals = spawn_suffix_helper(&committee, store);

    let address = committee.primary(&requestor).unwrap().primary_to_primary;
    let mut response = listener(address);
    tokio::task::yield_now().await;
    let misplaced = Proposal {
        header_digest: header.id.clone(),
        height: header.height + 1,
        poa: None,
        ..Default::default()
    };
    tx_proposals
        .send((misplaced, header.height, requestor))
        .await
        .unwrap();

    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut response)
            .await
            .is_err(),
        "helper served a car at a height other than the requested one"
    );
    response.abort();
}

#[tokio::test]
#[serial]
async fn serves_certified_suffix_request() {
    let committee = committee_with_base_port(28_000);
    let requestor = keys()[0].0;
    let (store, header) = stored_header(".db_test_primary_helper_certified").await;
    let tx_proposals = spawn_suffix_helper(&committee, store);
    let mut poa = crate::common::certificate(&header);
    poa.votes.truncate(committee.validity_threshold() as usize);

    let address = committee.primary(&requestor).unwrap().primary_to_primary;
    let response = listener(address);
    tokio::task::yield_now().await;
    tx_proposals
        .send((Proposal::certified(poa), 0, requestor))
        .await
        .unwrap();

    let bytes = tokio::time::timeout(Duration::from_secs(2), response)
        .await
        .expect("helper did not serve the certified suffix")
        .expect("proposal listener failed");
    let message: PrimaryMessage = bincode::deserialize(&bytes).unwrap();
    assert!(matches!(
        message,
        PrimaryMessage::ProposalHeaders(received) if received == vec![header]
    ));
}

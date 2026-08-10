// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::{
    common::{committee, keys},
    messages::Vote,
};
use serial_test::serial;
use tokio::sync::mpsc::channel;

#[tokio::test]
#[serial]
async fn propose_empty() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_parents, rx_parents) = channel(1);
    let (_tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);
    let (_tx_ticket, rx_ticket) = channel(1);

    Proposer::spawn(
        name,
        committee(),
        signature_service,
        /* header_size */ 1_000,
        /* max_header_delay */ 20,
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        rx_ticket,
        /* tx_core */ tx_headers,
    );

    let genesis_cert = Certificate::genesis_certs(&committee())
        .get(&name)
        .unwrap()
        .clone();
    tx_parents
        .send(genesis_cert)
        .await
        .expect("failed to send cert to proposer");

    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.height, 1);
    assert!(header.payload.is_empty());
    assert!(header.verify(&committee()).is_ok());
}

#[tokio::test]
#[serial]
async fn propose_payload() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_parents, rx_parents) = channel(1);
    let (tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);
    let (_tx_ticket, rx_ticket) = channel(1);

    Proposer::spawn(
        name,
        committee(),
        signature_service,
        /* header_size */ 32,
        /* max_header_delay */ 1_000_000, // Ensure it is not triggered.
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        rx_ticket,
        /* tx_core */ tx_headers,
    );

    let genesis_cert = Certificate::genesis_certs(&committee())
        .get(&name)
        .unwrap()
        .clone();
    tx_parents
        .send(genesis_cert)
        .await
        .expect("failed to send cert to proposer");

    sleep(Duration::from_millis(500)).await;

    let digest = Digest(name.0);
    let worker_id = 0;
    tx_our_digests
        .send((digest.clone(), worker_id))
        .await
        .unwrap();

    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.height, 1);
    assert_eq!(header.payload.get(&digest), Some(&worker_id));
    assert!(header.verify(&committee()).is_ok());
}

#[tokio::test]
#[serial]
async fn propose_normal() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_parents, rx_parents) = channel(1);
    let (tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);
    let (_tx_ticket, rx_ticket) = channel(1);

    Proposer::spawn(
        name,
        committee(),
        signature_service,
        /* header_size */ 32,
        /* max_header_delay */ 1_000_000, // Ensure it is not triggered.
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        rx_ticket,
        /* tx_core */ tx_headers,
    );

    let genesis_cert = Certificate::genesis_certs(&committee())
        .get(&name)
        .unwrap()
        .clone();
    tx_parents
        .send(genesis_cert)
        .await
        .expect("failed to send cert to proposer");

    sleep(Duration::from_millis(500)).await;

    let digest = Digest(name.0);
    let worker_id = 0;
    tx_our_digests
        .send((digest.clone(), worker_id))
        .await
        .unwrap();

    let header = rx_headers.recv().await.unwrap();

    assert_eq!(header.height, 1);
    assert_eq!(header.payload.get(&digest), Some(&worker_id));
    assert!(header.verify(&committee()).is_ok());

    let votes: Vec<_> = keys()
        .iter()
        .take(1)
        .map(|(public_key, secret_key)| {
            Vote::new_from_key(header.clone(), Vec::new(), *public_key, secret_key)
        })
        .map(|x| (x.author, x.signature))
        .collect();

    let certificate = Certificate {
        author: header.origin(),
        header_digest: header.digest(),
        height: header.height,
        votes,
    };
    tx_parents.send(certificate).await.unwrap();

    tx_our_digests
        .send((digest.clone(), worker_id))
        .await
        .unwrap();

    let header1 = rx_headers.recv().await.unwrap();

    assert_eq!(header1.height, 2);
    assert_eq!(header1.payload.get(&digest), Some(&worker_id));
    assert!(header1.verify(&committee()).is_ok());
    assert_eq!(header1.parent_cert.header_digest, header.digest());
}

#[tokio::test]
#[serial]
async fn propose_special_ticket_first() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_parents, rx_parents) = channel(1);
    let (tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);
    let (_tx_ticket, rx_ticket) = channel(1);

    Proposer::spawn(
        name,
        committee(),
        signature_service,
        /* header_size */ 32,
        /* max_header_delay */ 1_000_000, // Ensure it is not triggered.
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        rx_ticket,
        /* tx_core */ tx_headers,
    );

    let genesis_cert = Certificate::genesis_certs(&committee())
        .get(&name)
        .unwrap()
        .clone();
    tx_parents
        .send(genesis_cert)
        .await
        .expect("failed to send cert to proposer");

    let _gen_header = Header::genesis(&committee());

    sleep(Duration::from_secs(1)).await;

    let digest = Digest(name.0);
    let worker_id = 0;
    tx_our_digests
        .send((digest.clone(), worker_id))
        .await
        .unwrap();

    let header = rx_headers.recv().await.unwrap();

    assert_eq!(header.height, 1);
    assert_eq!(header.payload.get(&digest), Some(&worker_id));
    assert!(header.verify(&committee()).is_ok());
}

#[tokio::test]
#[serial]
async fn propose_confirm_message() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_parents, rx_parents) = channel(1);
    let (tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);
    let (_tx_ticket, rx_ticket) = channel(1);

    Proposer::spawn(
        name,
        committee(),
        signature_service,
        /* header_size */ 32,
        /* max_header_delay */ 1_000_000, // Ensure it is not triggered.
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        rx_ticket,
        /* tx_core */ tx_headers,
    );

    let genesis_cert = Certificate::genesis_certs(&committee())
        .get(&name)
        .unwrap()
        .clone();
    tx_parents
        .send(genesis_cert)
        .await
        .expect("failed to send cert to proposer");

    let _gen_header = Header::genesis(&committee());

    sleep(Duration::from_secs(1)).await;

    let digest = Digest(name.0);
    let worker_id = 0;
    tx_our_digests
        .send((digest.clone(), worker_id))
        .await
        .unwrap();

    let header = rx_headers.recv().await.unwrap();

    assert_eq!(header.height, 1);
    assert_eq!(header.payload.get(&digest), Some(&worker_id));
    assert!(header.verify(&committee()).is_ok());
}

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Certificate, Header, Vote};
use bytes::Bytes;
use config::{Authority, Committee, ConsensusAddresses, PrimaryAddresses, WorkerAddresses};
use crypto::Hash as _;
use crypto::{generate_keypair, PublicKey, SecretKey, Signature};
use futures::sink::SinkExt as _;
use futures::stream::StreamExt as _;
use rand::rngs::StdRng;
use rand::SeedableRng as _;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub fn keys() -> Vec<(PublicKey, SecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| generate_keypair(&mut rng)).collect()
}

pub fn committee() -> Committee {
    Committee {
        consensus_signature_scheme: Default::default(),
        authorities: keys()
            .iter()
            .enumerate()
            .map(|(i, (id, _))| {
                let consensus = ConsensusAddresses {
                    consensus_to_consensus: format!("127.0.0.1:{}", i).parse().unwrap(),
                };
                let primary = PrimaryAddresses {
                    primary_to_primary: format!("127.0.0.1:{}", 100 + i).parse().unwrap(),
                    worker_to_primary: format!("127.0.0.1:{}", 200 + i).parse().unwrap(),
                    metrics: format!("127.0.0.1:{}", 600 + i).parse().unwrap(),
                };
                let workers = [(
                    0,
                    WorkerAddresses {
                        primary_to_worker: format!("127.0.0.1:{}", 300 + i).parse().unwrap(),
                        transactions: format!("127.0.0.1:{}", 400 + i).parse().unwrap(),
                        worker_to_worker: format!("127.0.0.1:{}", 500 + i).parse().unwrap(),
                        metrics: format!("127.0.0.1:{}", 700 + i).parse().unwrap(),
                    },
                )]
                .iter()
                .cloned()
                .collect();
                (
                    *id,
                    Authority {
                        stake: 1,
                        consensus_key: None,
                        consensus,
                        primary,
                        workers,
                    },
                )
            })
            .collect(),
    }
}

pub fn committee_with_base_port(base_port: u16) -> Committee {
    let mut committee = committee();
    for authority in committee.authorities.values_mut() {
        let primary = &mut authority.primary;

        let port = primary.primary_to_primary.port();
        primary.primary_to_primary.set_port(base_port + port);

        let port = primary.worker_to_primary.port();
        primary.worker_to_primary.set_port(base_port + port);

        let port = primary.metrics.port();
        primary.metrics.set_port(base_port + port);

        for worker in authority.workers.values_mut() {
            let port = worker.primary_to_worker.port();
            worker.primary_to_worker.set_port(base_port + port);

            let port = worker.transactions.port();
            worker.transactions.set_port(base_port + port);

            let port = worker.worker_to_worker.port();
            worker.worker_to_worker.set_port(base_port + port);

            let port = worker.metrics.port();
            worker.metrics.set_port(base_port + port);
        }
    }
    committee
}

pub fn header() -> Header {
    let (author, secret) = keys().pop().unwrap();
    let header = Header {
        author,
        height: 1,
        parent_cert: Certificate::genesis_certs(&committee())
            .get(&author)
            .unwrap()
            .clone(),
        ..Header::default()
    };
    Header {
        id: header.digest(),
        signature: Some(Signature::new(&header.digest(), &secret)),
        ..header
    }
}

pub fn headers() -> Vec<Header> {
    keys()
        .into_iter()
        .map(|(author, secret)| {
            let header = Header {
                author,
                height: 1,
                parent_cert: Certificate::genesis_certs(&committee())
                    .get(&author)
                    .unwrap()
                    .clone(),
                ..Header::default()
            };
            Header {
                id: header.digest(),
                signature: Some(Signature::new(&header.digest(), &secret)),
                ..header
            }
        })
        .collect()
}

pub fn header_from_cert(certificate: &Certificate) -> Header {
    let mut right_key: Vec<(PublicKey, SecretKey)> = keys()
        .into_iter()
        .filter(|(pk, _sk)| *pk == certificate.author)
        .collect();
    let secret = right_key.pop().unwrap().1;

    let header = Header {
        author: certificate.author,
        height: certificate.height + 1,
        parent_cert: certificate.clone(),
        ..Header::default()
    };

    Header {
        id: header.digest(),
        signature: Some(Signature::new(&header.digest(), &secret)),
        ..header
    }
}

pub fn votes(header: &Header) -> Vec<Vote> {
    keys()
        .into_iter()
        .map(|(author, secret)| {
            let vote = Vote {
                id: header.id.clone(),
                height: header.height,
                origin: header.author,
                author,
                signature: Signature::default(),
                consensus_votes: Vec::new(),
            };
            Vote {
                signature: Signature::new(&vote.digest(), &secret),
                ..vote
            }
        })
        .collect()
}

pub fn certificate(header: &Header) -> Certificate {
    Certificate {
        author: header.origin(),
        header_digest: header.digest(),
        height: header.height,
        votes: votes(header)
            .into_iter()
            .map(|x| (x.author, x.signature))
            .collect(),
        ..Default::default()
    }
}

pub fn listener(address: SocketAddr) -> JoinHandle<Bytes> {
    tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let transport = Framed::new(socket, LengthDelimitedCodec::new());
        let (mut writer, mut reader) = transport.split();
        match reader.next().await {
            Some(Ok(received)) => {
                writer.send(Bytes::from("Ack")).await.unwrap();
                received.freeze()
            }
            _ => panic!("Failed to receive network message"),
        }
    })
}

// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::listener;
use futures::future::try_join_all;

#[tokio::test]
async fn simple_send() {
    // Run a TCP server.
    let address = "127.0.0.1:6100".parse::<SocketAddr>().unwrap();
    let message = "Hello, world!";
    let handle = listener(address, message.to_string());

    // Make the network sender and send the message.
    let mut sender = SimpleSender::new();
    sender.send(address, Bytes::from(message)).await;

    // Ensure the server received the message (ie. it did not panic).
    assert!(handle.await.is_ok());
}

/// The sender reconnects when the peer appears. Messages queued while disconnected
/// are best-effort and may be discarded.
#[tokio::test]
async fn sender_reconnects_once_the_peer_appears() {
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);

    let mut sender = SimpleSender::new();
    sender
        .send(address, Bytes::from("shed while the peer is down"))
        .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let message = "delivered after the peer came up";
    let handle = listener(address, message.to_string());
    // Resend until the listener receives a frame.
    let delivered = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            sender.send(address, Bytes::from(message)).await;
            if handle.is_finished() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(delivered.is_ok(), "sender never reconnected to the peer");
    handle.await.unwrap();
}

/// An unreachable peer must not block the caller.
#[tokio::test]
async fn sends_to_an_unreachable_peer_do_not_block_the_caller() {
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);

    let mut sender = SimpleSender::new();
    let flood = async {
        // Exceed the channel capacity to exercise the drain path.
        for _ in 0..110_000 {
            sender.send(address, Bytes::from_static(b"x")).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(60), flood)
        .await
        .expect("an unreachable peer must not be able to block the sender");
}

#[tokio::test]
async fn broadcast() {
    // Run 3 TCP servers.
    let message = "Hello, world!";
    let (handles, addresses): (Vec<_>, Vec<_>) = (0..3)
        .map(|x| {
            let address = format!("127.0.0.1:{}", 6_200 + x)
                .parse::<SocketAddr>()
                .unwrap();
            (listener(address, message.to_string()), address)
        })
        .collect::<Vec<_>>()
        .into_iter()
        .unzip();

    // Make the network sender and send the message.
    let mut sender = SimpleSender::new();
    sender.broadcast(addresses, Bytes::from(message)).await;

    // Ensure all servers received the broadcast.
    assert!(try_join_all(handles).await.is_ok());
}

// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::listener;
use futures::future::try_join_all;
use tokio::net::TcpListener;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::test]
async fn send() {
    // Run a TCP server.
    let address = "127.0.0.1:5000".parse::<SocketAddr>().unwrap();
    let message = "Hello, world!";
    let handle = listener(address, message.to_string());

    // Make the network sender and send the message.
    let mut sender = ReliableSender::new();
    let cancel_handler = sender.send(address, Bytes::from(message)).await;

    // Ensure we get back an acknowledgement.
    assert!(cancel_handler.await.is_ok());

    // Ensure the server received the expected message (ie. it did not panic).
    assert!(handle.await.is_ok());
}

#[tokio::test]
async fn broadcast() {
    // Run 3 TCP servers.
    let message = "Hello, world!";
    let (handles, addresses): (Vec<_>, Vec<_>) = (0..3)
        .map(|x| {
            let address = format!("127.0.0.1:{}", 5_200 + x)
                .parse::<SocketAddr>()
                .unwrap();
            (listener(address, message.to_string()), address)
        })
        .collect::<Vec<_>>()
        .into_iter()
        .unzip();

    // Make the network sender and send the message.
    let mut sender = ReliableSender::new();
    let cancel_handlers = sender.broadcast(addresses, Bytes::from(message)).await;

    // Ensure we get back an acknowledgement for each message.
    assert!(try_join_all(cancel_handlers).await.is_ok());

    // Ensure all servers received the broadcast.
    assert!(try_join_all(handles).await.is_ok());
}

#[tokio::test]
async fn retry() {
    // Make the network sender and send the message  (no listeners are running).
    let address = "127.0.0.1:5300".parse::<SocketAddr>().unwrap();
    let message = "Hello, world!";
    let mut sender = ReliableSender::new();
    let cancel_handler = sender.send(address, Bytes::from(message)).await;

    // Run a TCP server.
    sleep(Duration::from_millis(50)).await;
    let handle = listener(address, message.to_string());

    // Ensure we get back an acknowledgement.
    assert!(cancel_handler.await.is_ok());

    // Ensure the server received the message (ie. it did not panic).
    assert!(handle.await.is_ok());
}

/// Adversarial-audit regression (FIX 1a): a message queued while the very first
/// TCP connect is still failing (the `run()` reconnect-waiter loop, BEFORE any
/// `Connection`-owned `Coalescer` exists) must still reach the wire bundle-framed
/// when batching is on -- otherwise the peer's `decode_bundle` either silently
/// mis-parses it (dropped message, sender still believes it was delivered) or
/// drops the connection on a truncated frame (permanent retransmit stall).
#[tokio::test]
async fn retry_with_batching_bundle_frames_the_reconnect_path_message() {
    let address = "127.0.0.1:5301".parse::<SocketAddr>().unwrap();
    let message = "Hello, world!";

    // No listener running yet -- this send is queued via the reconnect-waiter loop
    // (FIX 1a's exact code path), not the connected coalescer.
    let mut sender = ReliableSender::new().with_batching(BatchConfig {
        enabled: true,
        max_bytes: 65_536,
        max_delay_ms: 5,
    });
    let cancel_handler = sender.send(address, Bytes::from(message)).await;

    // Bring the peer up; it expects the PROPERLY bundle-framed message (one
    // sub-message), not the raw bytes.
    sleep(Duration::from_millis(50)).await;
    let expected_frame = encode_bundle(&[Bytes::from(message)]);
    let handle = tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let (mut writer, mut reader) = Framed::new(socket, LengthDelimitedCodec::new()).split();
        match reader.next().await {
            Some(Ok(received)) => {
                assert_eq!(received.freeze(), expected_frame);
                writer.send(Bytes::from("Ack")).await.unwrap();
            }
            _ => panic!("Failed to receive network message"),
        }
    });

    assert!(cancel_handler.await.is_ok());
    assert!(handle.await.is_ok());
}

/// Adversarial-audit regression (FIX 1b): a message still sitting UNFLUSHED in the
/// connected `Coalescer` when the peer disappears mid-flight must be re-encoded as a
/// bundle before it's requeued -- requeuing it raw would let a non-bundle-framed
/// entry into `Connection::buffer` while batching is on, breaking the same
/// "every buffered entry is bundle-framed" invariant FIX 1a restores for the
/// reconnect-waiter path.
#[tokio::test]
async fn reconnect_after_mid_coalesce_peer_drop_preserves_bundle_framing() {
    let address = "127.0.0.1:5302".parse::<SocketAddr>().unwrap();
    let message = "Hello, world!";
    let expected_frame = encode_bundle(&[Bytes::from(message)]);

    // A flaky peer: accepts once, waits long enough for the message to actually
    // reach the connected `Coalescer` (well within its 2s flush window), then drops
    // the connection without ever acking -- forcing `keep_alive_immediate` down its
    // error path with the message still unflushed. It then accepts a SECOND time
    // (the sender's reconnect) and expects the bundle-framed message there.
    let handle = tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let (first, _) = listener.accept().await.unwrap();
        sleep(Duration::from_millis(100)).await;
        drop(first);

        let (second, _) = listener.accept().await.unwrap();
        let (mut writer, mut reader) = Framed::new(second, LengthDelimitedCodec::new()).split();
        match reader.next().await {
            Some(Ok(received)) => {
                assert_eq!(received.freeze(), expected_frame);
                writer.send(Bytes::from("Ack")).await.unwrap();
            }
            _ => panic!("Failed to receive network message on the reconnect"),
        }
    });

    // A long flush delay so the message is still sitting in the coalescer (never
    // auto-flushed) when the peer drops the first connection.
    let mut sender = ReliableSender::new().with_batching(BatchConfig {
        enabled: true,
        max_bytes: 65_536,
        max_delay_ms: 2_000,
    });
    let cancel_handler = sender.send(address, Bytes::from(message)).await;

    assert!(cancel_handler.await.is_ok());
    assert!(handle.await.is_ok());
}

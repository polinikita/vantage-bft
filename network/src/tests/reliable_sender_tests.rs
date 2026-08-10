// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::listener;
use futures::future::try_join_all;
use std::collections::HashMap;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
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

/// A message queued before the first connection uses bundle framing.
#[tokio::test]
async fn retry_with_batching_bundle_frames_the_reconnect_path_message() {
    let address = "127.0.0.1:5301".parse::<SocketAddr>().unwrap();
    let message = "Hello, world!";

    // Queue the message before a listener exists.
    let mut sender = ReliableSender::new().with_batching(BatchConfig {
        enabled: true,
        max_bytes: 65_536,
        max_delay_ms: 5,
    });
    let cancel_handler = sender.send(address, Bytes::from(message)).await;

    // The peer expects a singleton bundle frame.
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

/// An unflushed message is re-encoded before retry after a connection loss.
#[tokio::test]
async fn reconnect_after_mid_coalesce_peer_drop_preserves_bundle_framing() {
    let address = "127.0.0.1:5302".parse::<SocketAddr>().unwrap();
    let message = "Hello, world!";
    let expected_frame = encode_bundle(&[Bytes::from(message)]);

    // Drop the first session before the coalescer flushes, then accept the retry.
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

    // Keep the message in the coalescer until the first session ends.
    let mut sender = ReliableSender::new().with_batching(BatchConfig {
        enabled: true,
        max_bytes: 65_536,
        max_delay_ms: 2_000,
    });
    let cancel_handler = sender.send(address, Bytes::from(message)).await;

    assert!(cancel_handler.await.is_ok());
    assert!(handle.await.is_ok());
}

/// A volatile entry with no reply target is still transmitted on a live session.
#[tokio::test]
async fn handler_less_volatile_entry_survives_pre_send_skip_on_a_live_session() {
    let address = "127.0.0.1:5303".parse::<SocketAddr>().unwrap();
    let handle = listener(address, "volatile".to_string());

    // Send over an established session.
    let mut sender = ReliableSender::new();
    sender
        .send_volatile(address, Bytes::from("volatile"), 42)
        .await;

    assert!(handle.await.is_ok());
}

/// Volatile messages queued while disconnected are dropped and their lowest key is
/// recorded.
#[tokio::test]
async fn volatile_arrival_while_disconnected_is_dropped_and_key_min_merged() {
    let address = "127.0.0.1:5308".parse::<SocketAddr>().unwrap();
    let drop_map: DirtyMap = Arc::new(parking_lot::Mutex::new(HashMap::new()));

    // Queue messages while disconnected.
    let mut sender = ReliableSender::new().with_drop_map(drop_map.clone());
    sender
        .send_volatile(address, Bytes::from("dropped-1"), 55)
        .await;
    sender
        .send_volatile(address, Bytes::from("dropped-2"), 60)
        .await;
    // Allow the disconnected path to process both messages.
    sleep(Duration::from_millis(100)).await;

    assert_eq!(
        drop_map.lock().get(&address).copied(),
        Some(55),
        "both volatile arrivals must be dropped and min-merged (55, the smaller key)"
    );

    // A later connection must receive neither dropped message.
    let listener = TcpListener::bind(&address).await.unwrap();
    let (socket, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .expect("the sender must eventually reconnect")
        .expect("accept must succeed");
    let (_writer, mut reader) = Framed::new(socket, LengthDelimitedCodec::new()).split();
    let frame = tokio::time::timeout(Duration::from_millis(300), reader.next()).await;
    assert!(
        frame.is_err(),
        "no dropped volatile arrival must ever reach the wire on a later reconnect"
    );
}

/// A volatile entry is also transmitted on the delayed path.
#[tokio::test]
async fn handler_less_volatile_entry_survives_delayed_pop_skip() {
    let address = "127.0.0.1:5304".parse::<SocketAddr>().unwrap();
    let handle = listener(address, "volatile-delayed".to_string());

    let mut latency = HashMap::new();
    latency.insert(address, Duration::from_millis(10));
    let mut sender = ReliableSender::new().with_latency(latency);
    sender
        .send_volatile(address, Bytes::from("volatile-delayed"), 7)
        .await;

    assert!(handle.await.is_ok());
}

/// A volatile entry awaiting acknowledgement is accounted when its session ends.
#[tokio::test]
async fn volatile_entry_dropped_at_session_death_is_reported_via_drop_map() {
    let address = "127.0.0.1:5305".parse::<SocketAddr>().unwrap();
    let drop_map: DirtyMap = Arc::new(parking_lot::Mutex::new(HashMap::new()));

    // Accept one frame, then close without acknowledging it.
    let handle = tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let (_writer, mut reader) = Framed::new(socket, LengthDelimitedCodec::new()).split();
        // Read the frame and close without an acknowledgement.
        let _ = reader.next().await;
    });

    let mut sender = ReliableSender::new().with_drop_map(drop_map.clone());
    sender
        .send_volatile(address, Bytes::from("will be dropped"), 99)
        .await;
    assert!(handle.await.is_ok());

    // Allow the session cleanup to record the key.
    sleep(Duration::from_millis(200)).await;

    let map = drop_map.lock();
    assert_eq!(map.get(&address), Some(&99));
}

/// An unflushed volatile message is accounted when its session ends.
#[tokio::test]
async fn volatile_entry_dropped_mid_coalesce_is_reported_via_drop_map() {
    let address = "127.0.0.1:5306".parse::<SocketAddr>().unwrap();
    let drop_map: DirtyMap = Arc::new(parking_lot::Mutex::new(HashMap::new()));

    let handle = tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let (first, _) = listener.accept().await.unwrap();
        // End the session before the coalescer flushes.
        sleep(Duration::from_millis(100)).await;
        drop(first);
    });

    let mut sender = ReliableSender::new()
        .with_batching(BatchConfig {
            enabled: true,
            max_bytes: 65_536,
            max_delay_ms: 2_000,
        })
        .with_drop_map(drop_map.clone());
    sender
        .send_volatile(address, Bytes::from("mid-coalesce volatile"), 13)
        .await;
    assert!(handle.await.is_ok());

    sleep(Duration::from_millis(200)).await;

    let map = drop_map.lock();
    assert_eq!(map.get(&address), Some(&13));
}

/// Recovery notifications fire after failure, not on the first connection.
#[tokio::test]
async fn reconnect_event_fires_only_after_a_failure() {
    let address = "127.0.0.1:5307".parse::<SocketAddr>().unwrap();
    let (tx, mut rx) = mpsc::channel(8);

    // The first connection succeeds without a prior failure.
    let first_listener = TcpListener::bind(&address).await.unwrap();
    let mut sender = ReliableSender::new().with_reconnect_events(tx);
    let _cancel = sender.send(address, Bytes::from("hello")).await;
    let (first_socket, _) = first_listener.accept().await.unwrap();

    // No recovery event is sent for the first connection.
    let none_yet = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await;
    assert!(
        none_yet.is_err(),
        "the very first clean connect must not fire a reconnect event"
    );

    // Force a reconnect.
    drop(first_socket);
    drop(first_listener);
    sleep(Duration::from_millis(100)).await;
    let second_listener = TcpListener::bind(&address).await.unwrap();
    // Keep the sender alive while it reconnects.
    let _cancel2 = sender.send(address, Bytes::from("again")).await;

    let event = tokio::time::timeout(Duration::from_secs(3), rx.recv()).await;
    assert_eq!(
        event.expect("reconnect event must fire after re-establishing post-failure"),
        Some(address)
    );
    drop(second_listener);
}

/// Durable messages queued while disconnected are delivered after reconnect.
#[tokio::test]
async fn durable_arrival_while_disconnected_is_buffered_and_delivered_on_reconnect() {
    let address = "127.0.0.1:5309".parse::<SocketAddr>().unwrap();
    let message = "durable while disconnected";

    // Queue a durable message while disconnected.
    let mut sender = ReliableSender::new();
    let cancel_handler = sender.send(address, Bytes::from(message)).await;

    sleep(Duration::from_millis(50)).await;
    let handle = listener(address, message.to_string());

    assert!(cancel_handler.await.is_ok());
    assert!(handle.await.is_ok());
}

/// When queued together while disconnected, durable messages survive and volatile
/// messages are dropped.
#[tokio::test]
async fn interleaved_arrivals_while_disconnected_only_durable_survives() {
    let address = "127.0.0.1:5310".parse::<SocketAddr>().unwrap();
    let drop_map: DirtyMap = Arc::new(parking_lot::Mutex::new(HashMap::new()));
    let message = "durable-survivor";

    // Queue volatile then durable messages while disconnected.
    let mut sender = ReliableSender::new().with_drop_map(drop_map.clone());
    sender
        .send_volatile(address, Bytes::from("volatile-casualty"), 77)
        .await;
    let cancel_handler = sender.send(address, Bytes::from(message)).await;

    sleep(Duration::from_millis(50)).await;
    assert_eq!(
        drop_map.lock().get(&address).copied(),
        Some(77),
        "the volatile arrival must already be dropped and min-merged before any \
         connection ever succeeds"
    );

    // The listener must receive only the durable frame.
    let handle = tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let (mut writer, mut reader) = Framed::new(socket, LengthDelimitedCodec::new()).split();
        match reader.next().await {
            Some(Ok(received)) => {
                assert_eq!(received.freeze(), Bytes::from(message));
                writer.send(Bytes::from("Ack")).await.unwrap();
            }
            _ => panic!("the durable survivor never reached the wire"),
        }
        // Confirm that no second frame arrives.
        let second = tokio::time::timeout(Duration::from_millis(300), reader.next()).await;
        assert!(
            second.is_err(),
            "the dropped volatile arrival must never resurrect"
        );
    });

    assert!(cancel_handler.await.is_ok());
    assert!(handle.await.is_ok());
}

// Detached durable sends have no reply target and must still be transmitted and retried.

/// A detached durable frame reaches the wire without a cancellation handler.
#[tokio::test]
async fn send_detached_has_no_handler_to_drop_and_still_transmits() {
    let address = "127.0.0.1:5311".parse::<SocketAddr>().unwrap();
    let message = "detached-durable";
    let handle = listener(address, message.to_string());

    let mut sender = ReliableSender::new();
    sender.send_detached(address, Bytes::from(message)).await;

    assert!(handle.await.is_ok());
}

/// A typed detached durable frame survives the sending call's scope.
#[tokio::test]
async fn send_detached_typed_done_frame_survives_past_the_send_call_scope() {
    let address = "127.0.0.1:5312".parse::<SocketAddr>().unwrap();
    let message = "VantageReplayDone-shaped";
    let handle = listener(address, message.to_string());

    let mut sender = ReliableSender::new();
    {
        sender
            .send_detached_typed(address, Bytes::from(message), "VantageReplayDone")
            .await;
    }

    assert!(
        handle.await.is_ok(),
        "the frame must still be received after the send call's scope ended"
    );
}

/// A detached durable entry survives the pre-send cancellation check.
#[tokio::test]
async fn detached_entry_survives_pre_send_skip_on_a_live_session() {
    let address = "127.0.0.1:5313".parse::<SocketAddr>().unwrap();
    let message = "detached-pre-send";
    let handle = listener(address, message.to_string());

    let mut sender = ReliableSender::new();
    sender.send_detached(address, Bytes::from(message)).await;

    assert!(handle.await.is_ok());
}

/// A detached durable entry queued while disconnected is delivered after reconnect.
#[tokio::test]
async fn detached_entry_survives_the_waiter_and_is_delivered_on_reconnect() {
    let address = "127.0.0.1:5314".parse::<SocketAddr>().unwrap();
    let message = "detached-waiter";

    // Queue the message while disconnected.
    let mut sender = ReliableSender::new();
    sender.send_detached(address, Bytes::from(message)).await;

    sleep(Duration::from_millis(50)).await;
    let handle = listener(address, message.to_string());

    assert!(handle.await.is_ok());
}

// Verify the configurable reconnect backoff limit.

/// The default backoff limit can be overridden.
#[test]
fn with_retry_backoff_max_ms_overrides_the_default() {
    let default_sender = ReliableSender::new();
    assert_eq!(
        default_sender.retry_backoff_max_ms,
        DEFAULT_RETRY_BACKOFF_MAX_MS
    );
    assert_eq!(DEFAULT_RETRY_BACKOFF_MAX_MS, 2_000);

    let overridden = ReliableSender::new().with_retry_backoff_max_ms(250);
    assert_eq!(overridden.retry_backoff_max_ms, 250);
}

/// A detached durable entry awaiting acknowledgement is requeued after session loss.
#[tokio::test]
async fn detached_entry_is_requeued_across_a_session_death_like_any_durable_entry() {
    let address = "127.0.0.1:5315".parse::<SocketAddr>().unwrap();
    let message = "detached-requeued";
    let expected_frame = Bytes::from(message);

    let handle = tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let (first, _) = listener.accept().await.unwrap();
        // Let the frame reach the peer before closing the session.
        sleep(Duration::from_millis(100)).await;
        drop(first);

        let (second, _) = listener.accept().await.unwrap();
        let (mut writer, mut reader) = Framed::new(second, LengthDelimitedCodec::new()).split();
        match reader.next().await {
            Some(Ok(received)) => {
                assert_eq!(received.freeze(), expected_frame);
                writer.send(Bytes::from("Ack")).await.unwrap();
            }
            _ => panic!("the detached entry was not requeued across the session death"),
        }
    });

    let mut sender = ReliableSender::new();
    sender.send_detached(address, Bytes::from(message)).await;

    assert!(handle.await.is_ok());
}

/// A full volatile queue records the lowest shed key instead of blocking.
#[tokio::test]
async fn volatile_soft_cap_sheds_into_the_drop_map_instead_of_blocking() {
    let address = "127.0.0.1:5320".parse::<SocketAddr>().unwrap();
    let drop_map: DirtyMap = Arc::new(parking_lot::Mutex::new(HashMap::new()));
    let mut sender = ReliableSender::new()
        .with_drop_map(drop_map.clone())
        .with_volatile_soft_cap(2);
    let (tx, _rx) = mpsc::channel(100);
    sender.connections.insert(address, tx);

    // Messages below the cap are queued.
    sender.send_volatile(address, Bytes::from("a"), 9).await;
    sender.send_volatile(address, Bytes::from("b"), 8).await;
    assert!(drop_map.lock().is_empty());

    // At the cap, the message is shed and recorded.
    sender.send_volatile(address, Bytes::from("c"), 7).await;
    assert_eq!(drop_map.lock().get(&address), Some(&7));

    // Keys are recorded as a minimum.
    sender.send_volatile(address, Bytes::from("d"), 3).await;
    assert_eq!(drop_map.lock().get(&address), Some(&3));
    sender.send_volatile(address, Bytes::from("e"), 5).await;
    assert_eq!(drop_map.lock().get(&address), Some(&3));
}

/// A shed without a drop map is not queued or recorded.
#[tokio::test]
async fn volatile_soft_cap_without_a_drop_map_sheds_silently() {
    let address = "127.0.0.1:5321".parse::<SocketAddr>().unwrap();
    let mut sender = ReliableSender::new().with_volatile_soft_cap(1);
    let (tx, mut rx) = mpsc::channel(100);
    sender.connections.insert(address, tx);

    sender.send_volatile(address, Bytes::from("kept"), 1).await;
    sender.send_volatile(address, Bytes::from("shed"), 2).await;

    // Only the first message was queued.
    assert!(rx.try_recv().is_ok());
    assert!(rx.try_recv().is_err());
}

/// A zero soft cap queues every volatile message.
#[tokio::test]
async fn volatile_soft_cap_zero_never_sheds() {
    let address = "127.0.0.1:5322".parse::<SocketAddr>().unwrap();
    let drop_map: DirtyMap = Arc::new(parking_lot::Mutex::new(HashMap::new()));
    let mut sender = ReliableSender::new().with_drop_map(drop_map.clone());
    let (tx, mut rx) = mpsc::channel(100);
    sender.connections.insert(address, tx);

    for key in 0..5 {
        sender.send_volatile(address, Bytes::from("m"), key).await;
    }
    for _ in 0..5 {
        assert!(rx.try_recv().is_ok());
    }
    assert!(drop_map.lock().is_empty());
}

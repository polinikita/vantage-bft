// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{authenticating_listener, listener};
use futures::future::try_join_all;
use std::collections::HashMap;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// A sender that authenticates to committee member 1 at `address`, and that member's own
/// view of the same pairwise key.
fn authenticating_pair(address: SocketAddr) -> (ReliableSender, Arc<ChannelAuth>) {
    let seed = [17u8; 32];
    let dialer = ChannelAuth::new(&seed, 0, 2, HashMap::from([(address, 1)]));
    let peer = Arc::new(ChannelAuth::new(&seed, 1, 2, HashMap::new()));
    let sender = ReliableSender::new().with_channel_auth(Some(Arc::new(dialer)));
    (sender, peer)
}

#[tokio::test]
async fn authenticated_send_is_delivered_and_acknowledged() {
    let address = "127.0.0.1:5900".parse::<SocketAddr>().unwrap();
    let message = "Hello, world!";
    let (mut sender, peer) = authenticating_pair(address);
    let handle = authenticating_listener(address, peer, message.to_string(), 1);

    let cancel_handler = sender.send(address, Bytes::from(message)).await;

    assert!(cancel_handler.await.is_ok());
    assert!(handle.await.is_ok());
}

/// The tag lives in the codec, so a payload requeued from a dead session is re-tagged under
/// the next session's key and counter. Tagging where the message is built would replay a
/// counter the peer has moved past, and no retry would ever verify.
#[tokio::test]
async fn a_requeued_payload_is_retagged_on_the_next_session() {
    let address = "127.0.0.1:5901".parse::<SocketAddr>().unwrap();
    let message = "Hello, world!";
    let (mut sender, peer) = authenticating_pair(address);
    // The first of the two sessions reads the frame and closes without acknowledging it.
    let handle = authenticating_listener(address, peer, message.to_string(), 2);

    let cancel_handler = sender.send(address, Bytes::from(message)).await;

    assert!(cancel_handler.await.is_ok());
    assert!(handle.await.is_ok());
}

/// `bytes_sent_total` must measure what crossed the wire. The codec appends the tag after
/// the sender records the payload length, so an authenticated frame costs its payload, the
/// length prefix, and the tag. Without this the bytes-per-sequenced-byte figure understates
/// an authenticated run by one tag per frame, which is ten percent of a small frame.
#[tokio::test]
async fn authenticated_frames_are_counted_with_their_tag() {
    let address = "127.0.0.1:5903".parse::<SocketAddr>().unwrap();
    let message = "Hello, world!";
    let registry = prometheus::Registry::new();
    let (metrics, _reporter) = Metrics::new(&registry);

    let seed = [17u8; 32];
    let dialer = ChannelAuth::new(&seed, 0, 2, HashMap::from([(address, 1)]));
    let peer = Arc::new(ChannelAuth::new(&seed, 1, 2, HashMap::new()));
    let mut sender = ReliableSender::new()
        .with_metrics(metrics.clone())
        .with_channel_auth(Some(Arc::new(dialer)));
    let handle = authenticating_listener(address, peer, message.to_string(), 1);

    let cancel_handler = sender.send(address, Bytes::from(message)).await;
    assert!(cancel_handler.await.is_ok());
    assert!(handle.await.is_ok());

    let expected = message.len() as u64 + 4 + crate::codec::TAG_LEN as u64;
    assert_eq!(
        metrics.bytes_sent_total.get(),
        expected,
        "an authenticated frame must be counted as payload + prefix + tag"
    );
    assert_eq!(
        metrics
            .channel_auth_bytes_total
            .with_label_values(&["sent"])
            .get(),
        message.len() as u64,
        "the auth counter tracks covered payload, not the tag itself"
    );
}

/// A destination outside the peer map stays on the plain path. This is what keeps client
/// and same-host connections unauthenticated without a decision at every call site.
#[tokio::test]
async fn an_unmapped_destination_is_not_authenticated() {
    let address = "127.0.0.1:5902".parse::<SocketAddr>().unwrap();
    let message = "Hello, world!";
    let auth = ChannelAuth::new(&[17u8; 32], 0, 2, HashMap::new());
    let mut sender = ReliableSender::new().with_channel_auth(Some(Arc::new(auth)));
    let handle = listener(address, message.to_string());

    let cancel_handler = sender.send(address, Bytes::from(message)).await;

    assert!(cancel_handler.await.is_ok());
    assert!(handle.await.is_ok());
}

#[tokio::test]
async fn send() {
    let address = "127.0.0.1:5000".parse::<SocketAddr>().unwrap();
    let message = "Hello, world!";
    let handle = listener(address, message.to_string());

    let mut sender = ReliableSender::new();
    let cancel_handler = sender.send(address, Bytes::from(message)).await;

    assert!(cancel_handler.await.is_ok());

    assert!(handle.await.is_ok());
}

#[tokio::test]
async fn broadcast() {
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

    let mut sender = ReliableSender::new();
    let cancel_handlers = sender.broadcast(addresses, Bytes::from(message)).await;

    assert!(try_join_all(cancel_handlers).await.is_ok());

    assert!(try_join_all(handles).await.is_ok());
}

#[tokio::test]
async fn retry() {
    let address = "127.0.0.1:5300".parse::<SocketAddr>().unwrap();
    let message = "Hello, world!";
    let mut sender = ReliableSender::new();
    let cancel_handler = sender.send(address, Bytes::from(message)).await;

    sleep(Duration::from_millis(50)).await;
    let handle = listener(address, message.to_string());

    assert!(cancel_handler.await.is_ok());

    assert!(handle.await.is_ok());
}

/// A message queued before the first connection uses bundle framing.
#[tokio::test]
async fn retry_with_batching_bundle_frames_the_reconnect_path_message() {
    let address = "127.0.0.1:5301".parse::<SocketAddr>().unwrap();
    let message = "Hello, world!";

    let mut sender = ReliableSender::new().with_batching(BatchConfig {
        enabled: true,
        max_bytes: 65_536,
        max_delay_ms: 5,
    });
    let cancel_handler = sender.send(address, Bytes::from(message)).await;

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

    let mut sender = ReliableSender::new().with_drop_map(drop_map.clone());
    sender
        .send_volatile(address, Bytes::from("dropped-1"), 55)
        .await;
    sender
        .send_volatile(address, Bytes::from("dropped-2"), 60)
        .await;
    sleep(Duration::from_millis(100)).await;

    assert_eq!(
        drop_map.lock().get(&address).copied(),
        Some(55),
        "both volatile arrivals must be dropped and min-merged (55, the smaller key)"
    );

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

    let handle = tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let (_writer, mut reader) = Framed::new(socket, LengthDelimitedCodec::new()).split();
        let _ = reader.next().await;
    });

    let mut sender = ReliableSender::new().with_drop_map(drop_map.clone());
    sender
        .send_volatile(address, Bytes::from("will be dropped"), 99)
        .await;
    assert!(handle.await.is_ok());

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

    let first_listener = TcpListener::bind(&address).await.unwrap();
    let mut sender = ReliableSender::new().with_reconnect_events(tx);
    let _cancel = sender.send(address, Bytes::from("hello")).await;
    let (first_socket, _) = first_listener.accept().await.unwrap();

    let none_yet = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await;
    assert!(
        none_yet.is_err(),
        "the very first clean connect must not fire a reconnect event"
    );

    drop(first_socket);
    drop(first_listener);
    sleep(Duration::from_millis(100)).await;
    let second_listener = TcpListener::bind(&address).await.unwrap();
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
        let second = tokio::time::timeout(Duration::from_millis(300), reader.next()).await;
        assert!(
            second.is_err(),
            "the dropped volatile arrival must never resurrect"
        );
    });

    assert!(cancel_handler.await.is_ok());
    assert!(handle.await.is_ok());
}

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

/// A detached send has no reply target, so its delivery is observable only through the
/// counter. The durable frame behind it is the barrier: acknowledgements are consumed in
/// transmission order, so its handler cannot resolve before the detached ack is counted.
#[tokio::test]
async fn detached_delivery_is_counted_by_message_type_when_its_ack_arrives() {
    let address = "127.0.0.1:5316".parse::<SocketAddr>().unwrap();
    let registry = prometheus::Registry::new();
    let (metrics, _reporter) = Metrics::new(&registry);
    let mut sender = ReliableSender::new().with_metrics(metrics.clone());

    let handle = tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let (mut writer, mut reader) = Framed::new(socket, LengthDelimitedCodec::new()).split();
        for _ in 0..2 {
            match reader.next().await {
                Some(Ok(_)) => writer.send(Bytes::from("Ack")).await.unwrap(),
                _ => panic!("a frame never reached the wire"),
            }
        }
    });

    sender
        .send_detached_typed(address, Bytes::from("detached"), "VantageReplayDone")
        .await;
    let barrier = sender.send(address, Bytes::from("durable")).await;
    assert!(barrier.await.is_ok());
    assert!(handle.await.is_ok());

    assert_eq!(
        metrics
            .network_detached_acked_total
            .with_label_values(&["VantageReplayDone"])
            .get(),
        1
    );
    assert_eq!(
        metrics
            .network_detached_acked_total
            .with_label_values(&["Header"])
            .get(),
        0,
        "the ack must be attributed to the type that was sent"
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

    let mut sender = ReliableSender::new();
    sender.send_detached(address, Bytes::from(message)).await;

    sleep(Duration::from_millis(50)).await;
    let handle = listener(address, message.to_string());

    assert!(handle.await.is_ok());
}

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
    let registry = prometheus::Registry::new();
    let (metrics, _reporter) = Metrics::new(&registry);
    let mut sender = ReliableSender::new()
        .with_metrics(metrics.clone())
        .with_drop_map(drop_map.clone())
        .with_volatile_soft_cap(2);
    let (tx, _rx) = mpsc::channel(100);
    sender.connections.insert(address, tx);

    sender.send_volatile(address, Bytes::from("a"), 9).await;
    sender.send_volatile(address, Bytes::from("b"), 8).await;
    assert!(drop_map.lock().is_empty());

    sender.send_volatile(address, Bytes::from("c"), 7).await;
    assert_eq!(drop_map.lock().get(&address), Some(&7));

    sender.send_volatile(address, Bytes::from("d"), 3).await;
    assert_eq!(drop_map.lock().get(&address), Some(&3));
    sender.send_volatile(address, Bytes::from("e"), 5).await;
    assert_eq!(drop_map.lock().get(&address), Some(&3));

    assert_eq!(
        metrics
            .network_sender_queue_peak
            .with_label_values(&["unlabeled"])
            .get(),
        2,
        "the watermark must report the two frames this undrained queue held under the \
         default role, not the depth of the last send"
    );
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

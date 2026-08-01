// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::listener;
use futures::future::try_join_all;
use std::collections::HashMap;
use std::sync::Mutex;
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

/// reconnect-replay plan §14 A1 (MAJOR, adversarial audit-3): a volatile send has NO
/// cancel handler at all (`ReplyTargets` empty). Before the fix, `all_closed` on an
/// empty vec was vacuously `true`, so a handler-less entry reaching the pre-send skip
/// (`keep_alive_immediate`'s main loop, the check right before `writer.send`) would
/// have been silently discarded before it was ever transmitted. Exercised on a LIVE
/// session (connect already up): the entry is queued, buffered by `on_arrival`, and
/// must still reach the wire on the very next pop -- this is the site's only
/// currently-reachable path post the waiter-arm fix below (a volatile arrival while
/// disconnected is now dropped at the WAITER, never buffered at all -- see
/// `volatile_arrival_while_disconnected_is_dropped_and_key_min_merged`), so this test
/// no longer doubles as a waiter-path regression the way an earlier version of it did.
#[tokio::test]
async fn handler_less_volatile_entry_survives_pre_send_skip_on_a_live_session() {
    let address = "127.0.0.1:5303".parse::<SocketAddr>().unwrap();
    let handle = listener(address, "volatile".to_string());

    // Listener already up -- this send reaches the wire over a live session, never
    // touching the reconnect-waiter arm at all.
    let mut sender = ReliableSender::new();
    sender
        .send_volatile(address, Bytes::from("volatile"), 42)
        .await;

    assert!(handle.await.is_ok());
}

/// reconnect-replay plan §7 (the "reconnect-waiter drain" discard path, `run`'s
/// `'waiter` loop) / audit-3 V-b ("mpsc-limbo items are either waiter-counted or
/// next-session-delivered", blessing waiter-COUNTED here): a volatile arrival while
/// the link is down is dropped immediately -- min-merged into the drop map, NEVER
/// buffered -- so it cannot ride out an outage as part of an unpaced flush burst at
/// the next reconnect. This is the fix for the exact defect the design's §2.2/§12
/// A/B criterion (paced replay, no flush spike) depends on: buffering it instead (as
/// an earlier version of this arm did) would have delivered the whole outage backlog
/// of one-shots as a single unpaced transport flush on reconnect, resurrecting
/// exactly the burst the outbox+replay mechanism exists to eliminate.
#[tokio::test]
async fn volatile_arrival_while_disconnected_is_dropped_and_key_min_merged() {
    let address = "127.0.0.1:5308".parse::<SocketAddr>().unwrap();
    let drop_map: DirtyMap = Arc::new(Mutex::new(HashMap::new()));

    // No listener at all yet -- every send below is queued via the reconnect-waiter
    // loop, never a live session.
    let mut sender = ReliableSender::new().with_drop_map(drop_map.clone());
    sender
        .send_volatile(address, Bytes::from("dropped-1"), 55)
        .await;
    sender
        .send_volatile(address, Bytes::from("dropped-2"), 60)
        .await;
    // Give the waiter arm time to actually drain and drop both arrivals.
    sleep(Duration::from_millis(100)).await;

    assert_eq!(
        drop_map.lock().unwrap().get(&address).copied(),
        Some(55),
        "both volatile arrivals must be dropped and min-merged (55, the smaller key)"
    );

    // Bring a listener up (well within the sender's own capped 2s retry backoff)
    // and confirm nothing arrives -- neither dropped message was ever buffered to
    // resurrect on this later reconnect.
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

/// reconnect-replay plan §14 A1: same regression, exercised against the delayed-path
/// pop skip (`keep_alive_delayed`'s `due` arm, gated on `with_latency`) rather than
/// the immediate path's pre-send skip.
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

/// reconnect-replay plan §2.3/§7: the `pending_replies` requeue tail discards a
/// volatile entry (never requeues it) and reports its key as the min dropped key for
/// this destination -- the exact accounting path the server-floored replay
/// mechanism's `pending_low` depends on.
#[tokio::test]
async fn volatile_entry_dropped_at_session_death_is_reported_via_drop_map() {
    let address = "127.0.0.1:5305".parse::<SocketAddr>().unwrap();
    let drop_map: DirtyMap = Arc::new(Mutex::new(HashMap::new()));

    // A peer that accepts, lets the message actually get WRITTEN (moving it into
    // `pending_replies`), then drops the connection without ever acking.
    let handle = tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let (_writer, mut reader) = Framed::new(socket, LengthDelimitedCodec::new()).split();
        // Read exactly one frame (the volatile send), then drop -- never ack.
        let _ = reader.next().await;
    });

    let mut sender = ReliableSender::new().with_drop_map(drop_map.clone());
    sender
        .send_volatile(address, Bytes::from("will be dropped"), 99)
        .await;
    assert!(handle.await.is_ok());

    // Give `keep_alive_immediate`'s `FailedToReceiveAck` error path a moment to run
    // its session-death tail after the peer vanishes.
    sleep(Duration::from_millis(200)).await;

    let map = drop_map.lock().unwrap();
    assert_eq!(map.get(&address), Some(&99));
}

/// reconnect-replay plan §2.3/§7: a volatile message still sitting UNFLUSHED in the
/// volatile coalescer when the peer disappears mid-flight is also discarded and
/// accounted (mirrors `reconnect_after_mid_coalesce_peer_drop_preserves_bundle_
/// framing`'s durable analog, but for the drop-map path instead of a requeue).
#[tokio::test]
async fn volatile_entry_dropped_mid_coalesce_is_reported_via_drop_map() {
    let address = "127.0.0.1:5306".parse::<SocketAddr>().unwrap();
    let drop_map: DirtyMap = Arc::new(Mutex::new(HashMap::new()));

    let handle = tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let (first, _) = listener.accept().await.unwrap();
        // Long enough for the volatile send to land in the connected coalescer
        // (well within its 2s flush window) before the peer disappears.
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

    let map = drop_map.lock().unwrap();
    assert_eq!(map.get(&address), Some(&13));
}

/// reconnect-replay plan §2.1/§7: the reconnect-event channel fires on
/// re-establishment AFTER a failure, never on the connection's first-ever clean
/// connect.
#[tokio::test]
async fn reconnect_event_fires_only_after_a_failure() {
    let address = "127.0.0.1:5307".parse::<SocketAddr>().unwrap();
    let (tx, mut rx) = mpsc::channel(8);

    // First listener is already up -- the very FIRST connect succeeds cleanly.
    let first_listener = TcpListener::bind(&address).await.unwrap();
    let mut sender = ReliableSender::new().with_reconnect_events(tx);
    let _cancel = sender.send(address, Bytes::from("hello")).await;
    let (first_socket, _) = first_listener.accept().await.unwrap();

    // No event for the first-ever connect.
    let none_yet = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await;
    assert!(
        none_yet.is_err(),
        "the very first clean connect must not fire a reconnect event"
    );

    // Kill the session -- the connection will retry and eventually re-establish.
    drop(first_socket);
    drop(first_listener);
    sleep(Duration::from_millis(100)).await;
    let second_listener = TcpListener::bind(&address).await.unwrap();
    // Keep the sender alive (and retrying) while we wait for the reconnection.
    let _cancel2 = sender.send(address, Bytes::from("again")).await;

    let event = tokio::time::timeout(Duration::from_secs(3), rx.recv()).await;
    assert_eq!(
        event.expect("reconnect event must fire after re-establishing post-failure"),
        Some(address)
    );
    drop(second_listener);
}

/// reconnect-replay plan §7: today's (pre-existing, unchanged) behavior pinned
/// against the reconnect-waiter arm's own class split -- a DURABLE arrival while
/// disconnected is buffered (never dropped) and delivered once the connection
/// re-establishes, exactly as `retry` already covers via the untyped `send` API;
/// this pins it explicitly against the class-aware waiter arm this change
/// introduces, so a future edit that accidentally routes durable arrivals through
/// the volatile (drop) branch fails here first.
#[tokio::test]
async fn durable_arrival_while_disconnected_is_buffered_and_delivered_on_reconnect() {
    let address = "127.0.0.1:5309".parse::<SocketAddr>().unwrap();
    let message = "durable while disconnected";

    // No listener yet -- queued via the reconnect-waiter loop.
    let mut sender = ReliableSender::new();
    let cancel_handler = sender.send(address, Bytes::from(message)).await;

    sleep(Duration::from_millis(50)).await;
    let handle = listener(address, message.to_string());

    assert!(cancel_handler.await.is_ok());
    assert!(handle.await.is_ok());
}

/// reconnect-replay plan §7/§14 A1: interleaved durable and volatile arrivals while
/// disconnected -- only the durable one survives to be buffered and delivered; the
/// volatile one is dropped and its key min-merged into the drop map, never
/// resurrected alongside the durable arrival on the later reconnect.
#[tokio::test]
async fn interleaved_arrivals_while_disconnected_only_durable_survives() {
    let address = "127.0.0.1:5310".parse::<SocketAddr>().unwrap();
    let drop_map: DirtyMap = Arc::new(Mutex::new(HashMap::new()));
    let message = "durable-survivor";

    // No listener yet -- both sends are queued via the reconnect-waiter loop, in
    // this order: volatile, then durable.
    let mut sender = ReliableSender::new().with_drop_map(drop_map.clone());
    sender
        .send_volatile(address, Bytes::from("volatile-casualty"), 77)
        .await;
    let cancel_handler = sender.send(address, Bytes::from(message)).await;

    sleep(Duration::from_millis(50)).await;
    assert_eq!(
        drop_map.lock().unwrap().get(&address).copied(),
        Some(77),
        "the volatile arrival must already be dropped and min-merged before any \
         connection ever succeeds"
    );

    // The durable arrival must still be delivered once a listener comes up -- and
    // it must be the ONLY frame the listener ever sees.
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
        // Confirm the volatile casualty never follows it as a second frame.
        let second = tokio::time::timeout(Duration::from_millis(300), reader.next()).await;
        assert!(
            second.is_err(),
            "the dropped volatile arrival must never resurrect"
        );
    });

    assert!(cancel_handler.await.is_ok());
    assert!(handle.await.is_ok());
}

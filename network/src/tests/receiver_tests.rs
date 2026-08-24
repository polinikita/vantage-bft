// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Sender;
use tokio::time::{sleep, Duration};

#[derive(Clone)]
struct TestHandler {
    deliver: Sender<(Option<u8>, String)>,
}

#[async_trait]
impl MessageHandler for TestHandler {
    async fn dispatch(
        &self,
        writer: &mut Writer,
        authenticated_peer: Option<u8>,
        message: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        let _ = writer.send(Bytes::from("Ack")).await;

        let message = bincode::deserialize(&message).unwrap();

        self.deliver
            .send((authenticated_peer, message))
            .await
            .unwrap();
        Ok(())
    }
}

#[tokio::test]
async fn receive() {
    let address = "127.0.0.1:4000".parse::<SocketAddr>().unwrap();
    let (tx, mut rx) = channel(1);
    Receiver::spawn(address, TestHandler { deliver: tx });
    sleep(Duration::from_millis(50)).await;

    let sent = "Hello, world!";
    let bytes = Bytes::from(bincode::serialize(sent).unwrap());
    let stream = TcpStream::connect(address).await.unwrap();
    let mut transport = Framed::new(stream, frame_codec());
    transport.send(bytes.clone()).await.unwrap();

    let message = rx.recv().await;
    assert!(message.is_some());
    let (authenticated_peer, received) = message.unwrap();
    assert_eq!(authenticated_peer, None);
    assert_eq!(received, sent);
}

#[tokio::test]
async fn authenticated_receiver_passes_the_handshake_identity_to_dispatch() {
    let address = "127.0.0.1:4001".parse::<SocketAddr>().unwrap();
    let seed = [91; 32];
    let listener_auth = Arc::new(ChannelAuth::new(&seed, 1, 2, HashMap::new()));
    let dialer_auth = ChannelAuth::new(&seed, 0, 2, HashMap::from([(address, 1)]));
    let (tx, mut rx) = channel(1);
    Receiver::spawn_full(
        address,
        TestHandler { deliver: tx },
        None,
        false,
        false,
        "test",
        Some(listener_auth),
    );
    sleep(Duration::from_millis(50)).await;

    let sent = "authenticated";
    let bytes = Bytes::from(bincode::serialize(sent).unwrap());
    let mut stream = TcpStream::connect(address).await.unwrap();
    let key = dialer_auth.handshake_dialer(&mut stream, 1).await.unwrap();
    let mut transport = Framed::new(stream, authenticated_frame_codec(key, Role::Dialer));
    transport.send(bytes).await.unwrap();

    let (authenticated_peer, received) = rx.recv().await.unwrap();
    assert_eq!(authenticated_peer, Some(0));
    assert_eq!(received, sent);
}

#[test]
fn connection_metrics_distinguish_sessions_from_peers() {
    let registry = prometheus::Registry::new();
    let (metrics, _reporter) = Metrics::new(&registry);
    let peers = Arc::new(PeerSessions::default());
    let first = "127.0.0.1".parse().unwrap();
    let second = "127.0.0.2".parse().unwrap();

    let first_session = ConnectionMetricGuard::new(
        Some(metrics.clone()),
        "test",
        first,
        peers.clone(),
        Some(1_000),
    );
    let duplicate_session = ConnectionMetricGuard::new(
        Some(metrics.clone()),
        "test",
        first,
        peers.clone(),
        Some(2_000),
    );
    let second_peer =
        ConnectionMetricGuard::new(Some(metrics.clone()), "test", second, peers, Some(3_000));

    let label = &["test"];
    assert_eq!(
        metrics.network_connections.with_label_values(label).get(),
        3
    );
    assert_eq!(
        metrics.network_unique_peers.with_label_values(label).get(),
        2
    );
    assert_eq!(
        metrics
            .network_connections_accepted_total
            .with_label_values(label)
            .get(),
        3
    );
    assert_eq!(
        metrics
            .network_peer_rtt_microseconds_total
            .with_label_values(label)
            .get(),
        4_000
    );
    assert_eq!(
        metrics
            .network_peer_rtt_samples_total
            .with_label_values(label)
            .get(),
        2
    );

    drop(first_session);
    assert_eq!(
        metrics.network_unique_peers.with_label_values(label).get(),
        2
    );
    drop(duplicate_session);
    assert_eq!(
        metrics.network_unique_peers.with_label_values(label).get(),
        1
    );
    drop(second_peer);
    assert_eq!(
        metrics.network_connections.with_label_values(label).get(),
        0
    );
    assert_eq!(
        metrics.network_unique_peers.with_label_values(label).get(),
        0
    );
    assert_eq!(
        metrics
            .network_connections_closed_total
            .with_label_values(label)
            .get(),
        3
    );
}

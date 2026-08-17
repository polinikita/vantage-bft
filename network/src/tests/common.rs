// Copyright(C) Facebook, Inc. and its affiliates.
use crate::channel_auth::{ChannelAuth, Role};
use crate::codec::authenticated_frame_codec;
use bytes::Bytes;
use futures::sink::SinkExt as _;
use futures::stream::StreamExt as _;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub fn listener(address: SocketAddr, expected: String) -> JoinHandle<()> {
    tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let transport = Framed::new(socket, LengthDelimitedCodec::new());
        let (mut writer, mut reader) = transport.split();
        match reader.next().await {
            Some(Ok(received)) => {
                assert_eq!(received, expected);
                writer.send(Bytes::from("Ack")).await.unwrap()
            }
            _ => panic!("Failed to receive network message"),
        }
    })
}

/// Authenticating listener that serves `sessions` connections in turn.
///
/// Every session but the last reads one frame and closes without acknowledging, which
/// makes the sender requeue the payload and transmit it again on a fresh connection under
/// a new session key and counter.
pub fn authenticating_listener(
    address: SocketAddr,
    auth: Arc<ChannelAuth>,
    expected: String,
    sessions: usize,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        for session in 1..=sessions {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (_, key) = auth.handshake_listener(&mut socket).await.unwrap();
            let transport = Framed::new(socket, authenticated_frame_codec(key, Role::Listener));
            let (mut writer, mut reader) = transport.split();
            match reader.next().await {
                Some(Ok(received)) => {
                    assert_eq!(received, expected);
                    if session == sessions {
                        writer.send(Bytes::from("Ack")).await.unwrap();
                    }
                }
                _ => panic!("Failed to receive an authenticated frame"),
            }
        }
    })
}

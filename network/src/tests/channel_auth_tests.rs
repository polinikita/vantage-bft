// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use std::collections::HashMap;
use tokio::net::TcpListener;

const SEED: [u8; 32] = [42; 32];
const COMMITTEE: usize = 4;

/// Committee member `index` reachable at `address`, with a peer map covering `peers`.
fn party(index: u8, peers: &[(SocketAddr, u8)]) -> ChannelAuth {
    ChannelAuth::new(&SEED, index, COMMITTEE, peers.iter().copied().collect())
}

fn address(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

/// Runs both ends of one handshake and returns the session key each side derived.
async fn handshake(
    dialer: &ChannelAuth,
    listener: &ChannelAuth,
    at: SocketAddr,
    claimed: u8,
) -> (io::Result<[u8; 32]>, io::Result<(u8, [u8; 32])>) {
    let bound = TcpListener::bind(at).await.unwrap();
    let accepting = async {
        let (mut socket, _) = bound.accept().await.unwrap();
        listener.handshake_listener(&mut socket).await
    };
    let dialing = async {
        let mut stream = TcpStream::connect(at).await.unwrap();
        dialer.handshake_dialer(&mut stream, claimed).await
    };
    let (accepted, dialed) = tokio::join!(accepting, dialing);
    (dialed, accepted)
}

#[tokio::test]
async fn both_ends_derive_the_same_session_key() {
    let at = address(4300);
    let dialer = party(0, &[(at, 1)]);
    let listener = party(1, &[]);

    let (dialed, accepted) = handshake(&dialer, &listener, at, 1).await;
    let (index, listener_key) = accepted.unwrap();
    assert_eq!(index, 0);
    assert_eq!(dialed.unwrap(), listener_key);
}

/// Fresh salts mean two connections between the same pair never share a session key, so
/// the per-frame counters are safe to restart at zero on every reconnect.
#[tokio::test]
async fn a_second_connection_derives_a_different_session_key() {
    let first = address(4301);
    let second = address(4302);
    let listener = party(1, &[]);

    let (dialed_first, _) = handshake(&party(0, &[(first, 1)]), &listener, first, 1).await;
    let (dialed_second, _) = handshake(&party(0, &[(second, 1)]), &listener, second, 1).await;
    assert_ne!(dialed_first.unwrap(), dialed_second.unwrap());
}

/// The dialer takes the peer's index from its own address map. A committee member that
/// answered on someone else's address authenticates as itself and is refused, rather than
/// receiving traffic meant for the party the dialer intended to reach.
#[tokio::test]
async fn the_dialer_refuses_a_peer_at_the_wrong_address() {
    let at = address(4303);
    let dialer = party(0, &[(at, 1)]);
    let impostor = party(2, &[]);

    let (dialed, _) = handshake(&dialer, &impostor, at, 1).await;
    assert!(dialed.is_err());
}

#[tokio::test]
async fn the_listener_refuses_a_peer_claiming_our_own_index() {
    let at = address(4304);
    // A party that presents the listener's own committee index.
    let dialer = party(1, &[(at, 1)]);
    let listener = party(1, &[]);

    let (_, accepted) = handshake(&dialer, &listener, at, 1).await;
    assert!(accepted.is_err());
}

#[tokio::test]
async fn the_listener_refuses_an_index_outside_the_committee() {
    let at = address(4305);
    let dialer = party(COMMITTEE as u8, &[(at, 1)]);
    let listener = party(1, &[]);

    let (_, accepted) = handshake(&dialer, &listener, at, 1).await;
    assert!(accepted.is_err());
}

/// A peer that speaks plain length-delimited framing sends no hello, so its first frame is
/// read as one and rejected. This is what an authenticated node sees from an
/// unauthenticated one.
#[tokio::test]
async fn the_listener_refuses_a_peer_that_sends_no_hello() {
    let at = address(4306);
    let listener = party(1, &[]);
    let bound = TcpListener::bind(at).await.unwrap();

    let accepting = async {
        let (mut socket, _) = bound.accept().await.unwrap();
        listener.handshake_listener(&mut socket).await
    };
    let sending = async {
        let mut stream = TcpStream::connect(at).await.unwrap();
        // A length-delimited frame, not a hello.
        stream
            .write_all(&[0, 0, 0, 17, b'p', b'l', b'a', b'i', b'n'])
            .await
            .unwrap();
        stream.flush().await.unwrap();
    };
    let (accepted, ()) = tokio::join!(accepting, sending);
    assert!(accepted.is_err());
}

#[test]
fn only_mapped_addresses_are_authenticated() {
    let peer = address(4307);
    let client = address(4308);
    let auth = party(0, &[(peer, 3)]);

    assert_eq!(auth.peer_index(&peer), Some(3));
    assert_eq!(auth.peer_index(&client), None);
    assert_eq!(auth.peer_count(), 1);
}

#[test]
fn peers_that_share_a_seed_share_a_pairwise_key() {
    let map: HashMap<SocketAddr, u8> = HashMap::new();
    let zero = ChannelAuth::new(&SEED, 0, COMMITTEE, map.clone());
    let one = ChannelAuth::new(&SEED, 1, COMMITTEE, map);

    assert_eq!(zero.root(1).unwrap(), one.root(0).unwrap());
    assert_ne!(zero.root(1).unwrap(), zero.root(2).unwrap());
    assert!(zero.root(COMMITTEE as u8).is_err());
}

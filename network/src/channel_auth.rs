//! Pairwise channel authentication for committee connections.
//!
//! The protocol model assumes an authenticated point-to-point channel between every pair
//! of parties: the receiver learns who sent a message, and the adversary can neither forge
//! nor tamper with traffic between correct parties. This module supplies that link with a
//! symmetric tag per wire frame.
//!
//! Authentication is deliberately **non-transferable**. Tags are verified when a frame
//! arrives and then discarded: they are never stored, relayed, or counted as protocol
//! evidence, and no message carries a vector of recipient-specific tags.

use crypto::{channel_root_key, channel_session_key};
use rand::rngs::SmallRng;
use rand::{RngCore as _, SeedableRng as _};
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};

/// Magic and version prefix of the connection-binding hello.
const HELLO_MAGIC: [u8; 4] = *b"VCA1";

/// Per-connection salt length.
const SALT_LEN: usize = 16;

/// Serialized hello: magic, committee index, salt.
const HELLO_LEN: usize = HELLO_MAGIC.len() + 1 + SALT_LEN;

/// Time allowed for the hello exchange before the connection is abandoned.
///
/// Without a bound, a peer that connects and stays silent holds a connection open
/// indefinitely.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// Which end of a connection a party holds.
///
/// The role fixes the salt order in the session key and the direction byte bound into
/// every tag, so a frame cannot be replayed back at the party that sent it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Role {
    /// Opened the connection.
    Dialer,
    /// Accepted the connection.
    Listener,
}

impl Role {
    /// Direction byte this role binds into the tags it sends.
    pub(crate) fn send_direction(self) -> u8 {
        match self {
            Self::Dialer => 0,
            Self::Listener => 1,
        }
    }

    /// Direction byte this role expects on the tags it receives.
    pub(crate) fn recv_direction(self) -> u8 {
        match self {
            Self::Dialer => Self::Listener.send_direction(),
            Self::Listener => Self::Dialer.send_direction(),
        }
    }
}

/// Static pairwise keys plus the peer addresses they cover.
///
/// One value is built per process and shared by every sender and listener. Addresses
/// outside `peers` — clients and same-host links between our own primary and workers —
/// are not authenticated.
pub struct ChannelAuth {
    /// Our own committee index.
    my_index: u8,
    /// Static pairwise keys, indexed by the peer's committee index.
    roots: Vec<[u8; 32]>,
    /// Committee index of each authenticated peer listener address.
    peers: HashMap<SocketAddr, u8>,
}

impl ChannelAuth {
    /// Derives every pairwise key of a committee of `committee_size` parties.
    ///
    /// `seed` stands in for out-of-band key provisioning: a deployment would install
    /// pairwise keys directly rather than expand them from shared material.
    pub fn new(
        seed: &[u8; 32],
        my_index: u8,
        committee_size: usize,
        peers: HashMap<SocketAddr, u8>,
    ) -> Self {
        let roots = (0..committee_size)
            .map(|peer| channel_root_key(seed, my_index, peer as u8))
            .collect();
        Self {
            my_index,
            roots,
            peers,
        }
    }

    /// Committee index of an authenticated peer address, if we have one.
    pub fn peer_index(&self, address: &SocketAddr) -> Option<u8> {
        self.peers.get(address).copied()
    }

    /// Number of authenticated peer addresses. Reported at startup so a misbuilt map is
    /// visible before a run rather than after it.
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Static key shared with one peer.
    fn root(&self, peer: u8) -> io::Result<&[u8; 32]> {
        self.roots.get(peer as usize).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("committee index {peer} is outside the committee"),
            )
        })
    }

    /// Completes the hello exchange as the party that opened the connection.
    ///
    /// `expected` comes from our own address map, never from the peer's claim: a
    /// committee member that intercepted this address would otherwise authenticate under
    /// its own key and receive traffic meant for someone else.
    pub async fn handshake_dialer(
        &self,
        stream: &mut TcpStream,
        expected: u8,
    ) -> io::Result<[u8; 32]> {
        let root = *self.root(expected)?;
        let salt = fresh_salt();
        let peer = timeout(HANDSHAKE_TIMEOUT, self.exchange(stream, &salt)).await??;
        if peer.index != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "peer claims committee index {} but this address belongs to {expected}",
                    peer.index
                ),
            ));
        }
        Ok(channel_session_key(&root, &salt, &peer.salt))
    }

    /// Completes the hello exchange as the party that accepted the connection.
    ///
    /// The claimed index only selects a key. Nothing is bound until the first tag
    /// verifies, which a party that does not hold that key cannot produce.
    pub async fn handshake_listener(&self, stream: &mut TcpStream) -> io::Result<(u8, [u8; 32])> {
        let salt = fresh_salt();
        let peer = timeout(HANDSHAKE_TIMEOUT, self.exchange(stream, &salt)).await??;
        if peer.index == self.my_index {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "peer claims our own committee index".to_string(),
            ));
        }
        let root = self.root(peer.index)?;
        Ok((peer.index, channel_session_key(root, &peer.salt, &salt)))
    }

    /// Writes our hello and reads the peer's.
    ///
    /// Both ends write before reading, so neither waits on the other.
    async fn exchange(&self, stream: &mut TcpStream, salt: &[u8; SALT_LEN]) -> io::Result<Hello> {
        let mut hello = [0u8; HELLO_LEN];
        hello[..HELLO_MAGIC.len()].copy_from_slice(&HELLO_MAGIC);
        hello[HELLO_MAGIC.len()] = self.my_index;
        hello[HELLO_MAGIC.len() + 1..].copy_from_slice(salt);
        stream.write_all(&hello).await?;
        stream.flush().await?;

        let mut received = [0u8; HELLO_LEN];
        stream.read_exact(&mut received).await?;
        Hello::parse(&received)
    }
}

/// A peer's parsed hello.
struct Hello {
    index: u8,
    salt: [u8; SALT_LEN],
}

impl Hello {
    fn parse(bytes: &[u8; HELLO_LEN]) -> io::Result<Self> {
        if bytes[..HELLO_MAGIC.len()] != HELLO_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unrecognized channel-authentication hello",
            ));
        }
        let mut salt = [0u8; SALT_LEN];
        salt.copy_from_slice(&bytes[HELLO_MAGIC.len() + 1..]);
        Ok(Self {
            index: bytes[HELLO_MAGIC.len()],
            salt,
        })
    }
}

/// Draws a salt for one connection.
///
/// The salt needs to be fresh, not secret: it re-keys the session so that the per-frame
/// counters can restart at zero on every reconnect.
fn fresh_salt() -> [u8; SALT_LEN] {
    let mut salt = [0u8; SALT_LEN];
    SmallRng::from_entropy().fill_bytes(&mut salt);
    salt
}

#[cfg(test)]
#[path = "tests/channel_auth_tests.rs"]
mod channel_auth_tests;

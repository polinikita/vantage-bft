// Copyright(C) Facebook, Inc. and its affiliates.
//! Shared length-delimited framing for every peer connection, with optional
//! per-frame channel authentication.

use bytes::{BufMut as _, Bytes, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

use crate::channel_auth::Role;

/// Largest single message the transport accepts.
///
/// `LengthDelimitedCodec` defaults to 8 MiB. An `n = 100` committee exceeds
/// that on the consensus plane, and the sender then fails to encode the frame
/// and drops it: the message is lost with no retransmission, so the protocol
/// stalls rather than degrades. Both ends build their codec here because a
/// receiver with a smaller limit rejects what the sender emits.
pub const MAX_FRAME_LENGTH: usize = 64 * 1024 * 1024;

/// Truncated tag length appended to an authenticated frame.
///
/// Half of Blake3's output. Sixteen bytes is the usual MAC width and halves the wire
/// cost of the tag.
pub const TAG_LEN: usize = 16;

/// Width of the length prefix `LengthDelimitedCodec` writes by default.
pub const LENGTH_PREFIX_LEN: usize = 4;

/// Keys and counters bound to one authenticated connection.
struct Session {
    /// Session key derived from the pairwise key and both hello salts.
    key: [u8; 32],
    /// Which end of the connection we hold.
    role: Role,
    /// Frames sent on this connection so far.
    sent: u64,
    /// Frames accepted on this connection so far.
    received: u64,
}

/// Length-delimited framing, authenticating each frame when a session is present.
///
/// An unauthenticated codec delegates to `LengthDelimitedCodec` unchanged, so runs with
/// authentication disabled take the same path they always did.
pub struct AuthCodec {
    inner: LengthDelimitedCodec,
    session: Option<Session>,
    /// Largest payload this codec will encode, excluding any tag.
    payload_limit: usize,
}

impl AuthCodec {
    /// Framing without authentication.
    fn plain() -> Self {
        Self::with_payload_limit(MAX_FRAME_LENGTH, None)
    }

    /// Framing that tags every outgoing frame and verifies every incoming one.
    fn authenticated(key: [u8; 32], role: Role) -> Self {
        Self::with_payload_limit(
            MAX_FRAME_LENGTH,
            Some(Session {
                key,
                role,
                sent: 0,
                received: 0,
            }),
        )
    }

    /// Framing for a given payload limit. Tests use a small limit to exercise the
    /// boundary without allocating a maximal frame.
    fn with_payload_limit(payload_limit: usize, session: Option<Session>) -> Self {
        // An authenticated frame is its payload plus a tag, so the frame limit sits above
        // the payload limit. Leaving them equal would make the largest legal message
        // undeliverable, which is the silent-drop stall described above.
        let frame_limit = match session {
            Some(_) => payload_limit + TAG_LEN,
            None => payload_limit,
        };
        Self {
            inner: length_codec(frame_limit),
            session,
            payload_limit,
        }
    }

    /// Whether this codec authenticates its frames.
    pub fn is_authenticated(&self) -> bool {
        self.session.is_some()
    }
}

/// Build the length-delimited codec used by senders and receivers.
pub fn frame_codec() -> AuthCodec {
    AuthCodec::plain()
}

/// Build a codec bound to an authenticated connection.
pub fn authenticated_frame_codec(key: [u8; 32], role: Role) -> AuthCodec {
    AuthCodec::authenticated(key, role)
}

fn length_codec(max_frame_length: usize) -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(max_frame_length)
        .new_codec()
}

/// Tag covering the frame's direction, its position in the session, and its payload.
///
/// The payload is streamed rather than concatenated into a contiguous buffer: copying a
/// frame that may reach 64 MiB would cost more than the tag itself.
fn tag(key: &[u8; 32], direction: u8, counter: u64, payload: &[u8]) -> [u8; TAG_LEN] {
    let mut hasher = blake3::Hasher::new_keyed(key);
    hasher.update(&[direction]);
    hasher.update(&counter.to_le_bytes());
    hasher.update(payload);
    let mut out = [0u8; TAG_LEN];
    out.copy_from_slice(&hasher.finalize().as_bytes()[..TAG_LEN]);
    out
}

/// Compares tags without leaking where they first differ.
fn tags_match(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut difference = 0u8;
    for (a, b) in left.iter().zip(right) {
        difference |= a ^ b;
    }
    difference == 0
}

impl Encoder<Bytes> for AuthCodec {
    type Error = io::Error;

    fn encode(&mut self, data: Bytes, dst: &mut BytesMut) -> io::Result<()> {
        let Some(session) = self.session.as_mut() else {
            return self.inner.encode(data, dst);
        };

        // Enforce the payload limit here: the inner codec's own limit was raised to make
        // room for the tag and would otherwise let an oversized payload through.
        if data.len() > self.payload_limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "frame of {} bytes exceeds the {} byte payload limit",
                    data.len(),
                    self.payload_limit
                ),
            ));
        }

        let tag = tag(
            &session.key,
            session.role.send_direction(),
            session.sent,
            &data,
        );
        session.sent += 1;

        let length = data.len() + TAG_LEN;
        dst.reserve(LENGTH_PREFIX_LEN + length);
        dst.put_u32(length as u32);
        dst.put_slice(&data);
        dst.put_slice(&tag);
        Ok(())
    }
}

impl Decoder for AuthCodec {
    type Item = BytesMut;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<BytesMut>> {
        // Framing stays with the inner decoder, which returns the payload and its tag as
        // one frame.
        let Some(mut frame) = self.inner.decode(src)? else {
            return Ok(None);
        };
        let Some(session) = self.session.as_mut() else {
            return Ok(Some(frame));
        };

        if frame.len() < TAG_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "authenticated frame of {} bytes carries no tag",
                    frame.len()
                ),
            ));
        }
        let received = frame.split_off(frame.len() - TAG_LEN);
        let expected = tag(
            &session.key,
            session.role.recv_direction(),
            session.received,
            &frame,
        );
        if !tags_match(&received, &expected) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "channel authentication tag does not verify",
            ));
        }
        session.received += 1;
        Ok(Some(frame))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMIT: usize = 1024;
    const KEY: [u8; 32] = [9; 32];

    fn session(key: [u8; 32], role: Role) -> Option<Session> {
        Some(Session {
            key,
            role,
            sent: 0,
            received: 0,
        })
    }

    /// A connected pair: what the dialer writes, the listener reads.
    fn pair() -> (AuthCodec, AuthCodec) {
        (
            AuthCodec::with_payload_limit(LIMIT, session(KEY, Role::Dialer)),
            AuthCodec::with_payload_limit(LIMIT, session(KEY, Role::Listener)),
        )
    }

    fn encoded(codec: &mut AuthCodec, payload: &'static [u8]) -> BytesMut {
        let mut wire = BytesMut::new();
        codec
            .encode(Bytes::from_static(payload), &mut wire)
            .unwrap();
        wire
    }

    #[test]
    fn plain_framing_is_unchanged() {
        let mut codec = AuthCodec::plain();
        assert!(!codec.is_authenticated());
        let wire = encoded(&mut codec, b"hello");
        assert_eq!(wire.len(), LENGTH_PREFIX_LEN + 5);

        let mut received = wire;
        let mut decoder = AuthCodec::plain();
        let frame = decoder.decode(&mut received).unwrap().unwrap();
        assert_eq!(&frame[..], b"hello");
    }

    #[test]
    fn authenticated_roundtrip_costs_one_tag() {
        let (mut dialer, mut listener) = pair();
        assert!(dialer.is_authenticated());
        let mut wire = encoded(&mut dialer, b"hello");
        assert_eq!(wire.len(), LENGTH_PREFIX_LEN + 5 + TAG_LEN);

        let frame = listener.decode(&mut wire).unwrap().unwrap();
        assert_eq!(&frame[..], b"hello");
        assert!(wire.is_empty());
    }

    #[test]
    fn tampered_payload_is_rejected() {
        let (mut dialer, mut listener) = pair();
        let mut wire = encoded(&mut dialer, b"hello");
        // Flip one payload byte, leaving the length prefix and tag intact.
        wire[LENGTH_PREFIX_LEN] ^= 0x01;
        assert!(listener.decode(&mut wire).is_err());
    }

    #[test]
    fn tampered_tag_is_rejected() {
        let (mut dialer, mut listener) = pair();
        let mut wire = encoded(&mut dialer, b"hello");
        let last = wire.len() - 1;
        wire[last] ^= 0x01;
        assert!(listener.decode(&mut wire).is_err());
    }

    #[test]
    fn a_different_session_key_is_rejected() {
        let (mut dialer, _) = pair();
        let mut wire = encoded(&mut dialer, b"hello");
        let mut stranger = AuthCodec::with_payload_limit(LIMIT, session([8; 32], Role::Listener));
        assert!(stranger.decode(&mut wire).is_err());
    }

    /// A frame reflected at the party that sent it must not verify, or an adversary could
    /// bounce traffic back and have it accepted as the peer's own.
    #[test]
    fn a_reflected_frame_is_rejected() {
        let (mut dialer, _) = pair();
        let mut wire = encoded(&mut dialer, b"hello");
        let mut same_role = AuthCodec::with_payload_limit(LIMIT, session(KEY, Role::Dialer));
        assert!(same_role.decode(&mut wire).is_err());
    }

    #[test]
    fn a_replayed_frame_is_rejected() {
        let (mut dialer, mut listener) = pair();
        let wire = encoded(&mut dialer, b"hello");

        let mut first = wire.clone();
        assert_eq!(&listener.decode(&mut first).unwrap().unwrap()[..], b"hello");

        // The same bytes again: the receive counter has moved on.
        let mut again = wire;
        assert!(listener.decode(&mut again).is_err());
    }

    #[test]
    fn reordered_frames_are_rejected() {
        let (mut dialer, mut listener) = pair();
        let first = encoded(&mut dialer, b"first");
        let second = encoded(&mut dialer, b"second");

        let mut out_of_order = second;
        out_of_order.extend_from_slice(&first);
        assert!(listener.decode(&mut out_of_order).is_err());
    }

    /// The tag must fit above the payload limit, not inside it: a maximal payload has to
    /// survive the round trip or the largest legal message becomes undeliverable.
    #[test]
    fn a_maximal_payload_roundtrips_and_one_more_byte_is_rejected() {
        let mut dialer = AuthCodec::with_payload_limit(LIMIT, session(KEY, Role::Dialer));
        let mut listener = AuthCodec::with_payload_limit(LIMIT, session(KEY, Role::Listener));

        let maximal = Bytes::from(vec![7u8; LIMIT]);
        let mut wire = BytesMut::new();
        dialer.encode(maximal.clone(), &mut wire).unwrap();
        assert_eq!(wire.len(), LENGTH_PREFIX_LEN + LIMIT + TAG_LEN);
        assert_eq!(listener.decode(&mut wire).unwrap().unwrap(), maximal);

        let oversized = Bytes::from(vec![7u8; LIMIT + 1]);
        assert!(dialer.encode(oversized, &mut BytesMut::new()).is_err());
    }

    #[test]
    fn a_frame_shorter_than_its_tag_is_rejected() {
        let mut listener = AuthCodec::with_payload_limit(LIMIT, session(KEY, Role::Listener));
        let mut wire = BytesMut::new();
        wire.put_u32(4);
        wire.put_slice(&[0; 4]);
        assert!(listener.decode(&mut wire).is_err());
    }

    #[test]
    fn a_partial_frame_yields_no_message() {
        let (mut dialer, mut listener) = pair();
        let wire = encoded(&mut dialer, b"hello");
        let mut truncated = wire.clone();
        truncated.truncate(wire.len() - 1);
        assert!(listener.decode(&mut truncated).unwrap().is_none());

        // The remaining byte completes the frame.
        truncated.extend_from_slice(&wire[wire.len() - 1..]);
        assert_eq!(
            &listener.decode(&mut truncated).unwrap().unwrap()[..],
            b"hello"
        );
    }
}

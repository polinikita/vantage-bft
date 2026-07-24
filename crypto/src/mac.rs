// Symmetric pairwise-MAC AUTHENTICATED CHANNELS for inter-validator messages
// (signature-free, quantum-safe): flag-gated message authentication using only a
// shared committee master secret plus BLAKE3 keyed hashing -- no signatures, no PKI,
// no handshake.
//
// The one hard correctness requirement: the MAC binds the message's OWN declared
// sender field (the exact serialized bytes the caller hands to `PairwiseKeys::
// tag_for`/`verify` already include it), so verifying with the key shared between
// `self` and the claimed sender proves the message genuinely came from that party --
// closing wire-level sender impersonation without adding any asymmetric cryptography.
use crate::PublicKey;
use base64::prelude::{Engine as _, BASE64_STANDARD};
use rand::rngs::OsRng;
use rand::RngCore as _;
use serde::{de, ser, Deserialize, Serialize};
use std::collections::HashMap;

#[cfg(test)]
#[path = "tests/mac_tests.rs"]
pub mod mac_tests;

/// Wire size of a MAC tag: the full, untruncated BLAKE3 output (32 bytes) -- not
/// truncated to 16, so `verify` can reuse `blake3::Hash`'s own constant-time
/// `PartialEq<[u8; 32]>` (backed by the `constant_time_eq` crate) directly instead of
/// hand-rolling a constant-time compare over a truncated width.
pub const TAG_LEN: usize = 32;

/// The committee-wide symmetric master secret every pairwise key is derived from --
/// the only new secret this scheme introduces. Distributed identically to every
/// committee member (`node local-benchmark` generates one in-process and shares it
/// across every node it spawns; `fab remote` writes it into every node's distributed
/// `parameters.json`).
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct MacSecret(pub [u8; 32]);

impl MacSecret {
    pub fn generate() -> Self {
        let mut bytes = [0u8; 32];
        OsRng.fill_bytes(&mut bytes);
        Self(bytes)
    }
}

impl std::fmt::Debug for MacSecret {
    /// Never prints the secret bytes themselves -- this is key material.
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MacSecret(..)")
    }
}

impl Serialize for MacSecret {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&BASE64_STANDARD.encode(self.0))
    }
}

impl<'de> Deserialize<'de> for MacSecret {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let bytes = BASE64_STANDARD
            .decode(&s)
            .map_err(|e| de::Error::custom(e.to_string()))?;
        let array: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
            de::Error::custom(format!(
                "mac secret must decode to exactly 32 bytes, got {}",
                bytes.len()
            ))
        })?;
        Ok(Self(array))
    }
}

/// `k_{i,j} = blake3::keyed_hash(secret, sorted(i, j))` -- sorted so the derivation is
/// symmetric in `(i, j)`: both `i` and `j` independently compute the identical key,
/// with no handshake and no need to agree in advance which of them is "first".
fn derive_pairwise_key(secret: &MacSecret, i: &PublicKey, j: &PublicKey) -> [u8; 32] {
    let mut input = [0u8; 64];
    if i.0 <= j.0 {
        input[..32].copy_from_slice(&i.0);
        input[32..].copy_from_slice(&j.0);
    } else {
        input[..32].copy_from_slice(&j.0);
        input[32..].copy_from_slice(&i.0);
    }
    *blake3::keyed_hash(&secret.0, &input).as_bytes()
}

/// Every committee member's pairwise key with this node (`me`), precomputed once at
/// spawn time -- one `derive_pairwise_key` call per committee member, including a
/// degenerate `k_{me,me}` self-entry (see `tag_unverified`'s doc comment for why that
/// entry is required).
pub struct PairwiseKeys {
    me: PublicKey,
    keys: HashMap<PublicKey, [u8; 32]>,
}

impl PairwiseKeys {
    /// `members` must include `me` itself -- the degenerate self-pair key backs both
    /// the worker<->primary intra-authority channel (both ends share the same
    /// authority public key) and `tag_unverified`'s placeholder tag.
    pub fn build(
        secret: &MacSecret,
        me: PublicKey,
        members: impl IntoIterator<Item = PublicKey>,
    ) -> Self {
        let keys = members
            .into_iter()
            .map(|peer| (peer, derive_pairwise_key(secret, &me, &peer)))
            .collect();
        Self { me, keys }
    }

    /// The tag this node must attach when it (`self.me`) sends `payload` to `dest`, so
    /// `dest` can verify it against `k_{self.me, dest}` = `k_{dest, self.me}`. `None`
    /// if `dest` isn't a committee member we hold a key for (never the case for a real
    /// committee peer -- `build` is always seeded from the full committee).
    pub fn tag_for(&self, dest: &PublicKey, payload: &[u8]) -> Option<[u8; TAG_LEN]> {
        self.keys
            .get(dest)
            .map(|key| *blake3::keyed_hash(key, payload).as_bytes())
    }

    /// A framing-only placeholder tag for message variants that carry no sender claim
    /// at all to bind (the pre-existing D4-class gap: e.g. a `Serve`/`ControlServe`
    /// reply, or a worker's relayed `Batch` -- see each call site's own comment).
    /// Computed with the degenerate `k_{self.me, self.me}` key, independent of the
    /// destination -- one hash regardless of how many peers this frame is broadcast
    /// to, since the receiver never verifies it (there is no declared sender to check
    /// it against). It exists purely so the wire format's "every message carries
    /// exactly one trailing tag" framing stays uniform across every variant.
    pub fn tag_unverified(&self, payload: &[u8]) -> [u8; TAG_LEN] {
        let key = self
            .keys
            .get(&self.me)
            .expect("PairwiseKeys::build's `members` must include `me`");
        *blake3::keyed_hash(key, payload).as_bytes()
    }

    /// Verify `tag` was produced by `claimed_sender`'s own copy of `k_{claimed_sender,
    /// self.me}` over `payload`. `false` if `claimed_sender` isn't a committee member
    /// we hold a key for, or the tag doesn't match. The comparison is constant-time
    /// (`blake3::Hash`'s `PartialEq<[u8; 32]>`, backed by the `constant_time_eq`
    /// crate) -- it never leaks timing information about which byte first diverged.
    pub fn verify(&self, claimed_sender: &PublicKey, payload: &[u8], tag: &[u8; TAG_LEN]) -> bool {
        match self.keys.get(claimed_sender) {
            Some(key) => blake3::keyed_hash(key, payload) == *tag,
            None => false,
        }
    }
}

/// Split a wire frame into `(payload, tag)` -- the inverse of appending `tag_for`/
/// `tag_unverified`'s output to a serialized message. `None` if `frame` is too short
/// to even contain a tag (malformed or adversarial input); the caller must drop the
/// frame in that case, never panic.
pub fn split_tag(frame: &[u8]) -> Option<(&[u8], [u8; TAG_LEN])> {
    if frame.len() < TAG_LEN {
        return None;
    }
    let (payload, tag) = frame.split_at(frame.len() - TAG_LEN);
    let mut tag_arr = [0u8; TAG_LEN];
    tag_arr.copy_from_slice(tag);
    Some((payload, tag_arr))
}

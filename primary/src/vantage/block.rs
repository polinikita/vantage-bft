// PHASE3-SPEC.md §1, §3.1, §6.1 -- block/session primitives.
//
// A vantage "data block" is the existing `Header` struct with its vantage-only
// `Option` fields populated (`signature: None`, `sid: Some(_)`) -- see
// `messages::Header::new_vantage`. This module holds what's genuinely new:
// session-id/genesis derivation, a domain-tag hash helper (shared with later phases),
// and `BlockOK`.

use crate::messages::Header;
use crate::primary::Height;
use config::Committee;
use crypto::{Blake3Hasher, Digest, Hash as _, PublicKey};

/// A block reference `(a, k, h)` -- author, height, digest -- as used throughout
/// PHASE3-SPEC.md §2 (N1-N9) to name an exact tuple.
pub type BlockRef = (PublicKey, Height, Digest);

/// Domain-tag helper (§1 row `H("data-block" || enc(b))`, §6.1): blake3 over a fixed
/// domain tag, the session id, and caller-supplied payload bytes. Shared with later
/// phases (manifests etc.). Inside Phase 3 it is used only for the genesis digest --
/// ordinary (non-genesis) blocks are identified by `Header::digest()` (`.id`), which
/// already folds `sid` in per §3.1's `Hash for Header` change, so every real block's
/// wire identity is session-scoped without computing a second, redundant hash over its
/// own encoding.
pub fn domain_hash(tag: &[u8], sid: &Digest, payload: &[u8]) -> Digest {
    let mut hasher = Blake3Hasher::new();
    hasher.update(tag);
    hasher.update(&sid.0);
    hasher.update(payload);
    Digest(hasher.finalize().into())
}

/// `sid = blake3("vantage-sid" || canonical committee encoding)` (§6.1). Deterministic
/// given the committee (bincode over `BTreeMap<PublicKey, Authority>` is
/// order-stable), so every correct party derives the same value from the same
/// committee file without exchanging it.
pub fn session_id(committee: &Committee) -> Digest {
    let mut hasher = Blake3Hasher::new();
    hasher.update(b"vantage-sid");
    let encoded = bincode::serialize(committee).expect("committee always serializes");
    hasher.update(&encoded);
    Digest(hasher.finalize().into())
}

/// Genesis digest (§6.1): height-0 is implicit, no genesis block is ever sent on the
/// wire -- this is the fixed predecessor pointer every author's height-1 block carries
/// in `parent_cert.header_digest`.
pub fn genesis_digest(sid: &Digest) -> Digest {
    domain_hash(b"data-block", sid, b"genesis")
}

/// `BlockOK` (§3.1 last row / §2 N9): deterministic, state-independent well-formedness.
/// Reuses the exact self-consistency check `Header::verify` already performs for
/// Autobahn (`digest() == id`) as the "canonical encoding" requirement -- a header
/// whose declared `id` doesn't match its recomputed digest is malformed the same way a
/// bad signature would be on the Autobahn path. There is no separate raw-byte
/// canonicalization step: by the time a `Header` reaches this check the network
/// dispatcher has already decoded it once, so nothing here re-parses bytes (see
/// PHASE3-NOTES.md for the reasoning).
///
/// Also checks the block's *self-contained* half of chain integrity (predecessor
/// height arithmetic); the cross-block half (does `parent_cert.header_digest` really
/// name the real predecessor) is necessarily stateful and lives in
/// `lanes::BlockCache::verified_prefix_through_genesis`.
pub fn block_ok(
    header: &Header,
    committee: &Committee,
    sid: &Digest,
    max_block_payload: usize,
) -> bool {
    header.digest() == header.id
        && header.sid.as_ref() == Some(sid)
        && header.payload.len() <= max_block_payload
        && header.height >= 1
        && header.parent_cert.height + 1 == header.height
        && header.signature.is_none()
        && header.parent_cert.votes.is_empty()
        && header.consensus_messages.is_empty()
        && header.num_active_instances == 0
        && !header.special
        && committee.stake(&header.author) > 0
        && header
            .payload
            .values()
            .all(|worker_id| committee.worker(&header.author, worker_id).is_ok())
}

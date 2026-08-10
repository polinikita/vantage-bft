use crate::messages::Header;
use crate::primary::Height;
use config::Committee;
use crypto::{Blake3Hasher, Digest, Hash as _, PublicKey};

/// Identifies a block by `(author, height, digest)`.
pub type BlockRef = (PublicKey, Height, Digest);

/// Hashes a domain tag, session identifier, and payload with BLAKE3.
pub fn domain_hash(tag: &[u8], sid: &Digest, payload: &[u8]) -> Digest {
    let mut hasher = Blake3Hasher::new();
    hasher.update(tag);
    hasher.update(&sid.0);
    hasher.update(payload);
    Digest(hasher.finalize().into())
}

/// Derives the same session identifier for every canonical encoding of a committee.
pub fn session_id(committee: &Committee) -> Digest {
    let mut hasher = Blake3Hasher::new();
    hasher.update(b"vantage-sid");
    let encoded = bincode::serialize(committee).expect("committee always serializes");
    hasher.update(&encoded);
    Digest(hasher.finalize().into())
}

/// Returns the implicit predecessor digest for height-one blocks.
pub fn genesis_digest(sid: &Digest) -> Digest {
    domain_hash(b"data-block", sid, b"genesis")
}

/// Checks state-independent block validity.
///
/// The block cache separately verifies that the declared predecessor exists and matches.
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

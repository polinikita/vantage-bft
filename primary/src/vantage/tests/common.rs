// Test fixtures shared by every PHASE3-SPEC.md §7 / PHASE4-SPEC.md §12 test module.
// Reuses the crate's existing 4-authority (n=4, f=1 => f+1=2, 2f+1=3) committee/key
// fixtures.

use crate::common::{committee, keys};
use crate::messages::Header;
use crate::vantage::agb::{self, AgbEngine, Manifest};
use crate::vantage::block::{self, BlockRef};
use crate::vantage::lanes::{AckAvailability, AckThreshold, LaneManager};
use crate::vantage::repair::Repairer;
use config::Committee;
use crypto::{Digest, PublicKey, SecretKey};
use std::collections::BTreeMap;
use store::Store;

pub const MAX_BLOCK_PAYLOAD: usize = 16;
/// A small Δ (PHASE4-SPEC.md §10) so timer-boundary tests use human-scale
/// `Duration`s (θE = 500ms, θR = 600ms) instead of the production 1s default.
pub const TEST_DELTA_MS: u64 = 100;

pub fn test_committee() -> Committee {
    committee()
}

pub fn authors() -> Vec<(PublicKey, SecretKey)> {
    keys()
}

pub fn test_sid() -> Digest {
    block::session_id(&test_committee())
}

pub fn new_agb_engine(name: PublicKey) -> AgbEngine {
    AgbEngine::new(name, test_committee(), test_sid(), TEST_DELTA_MS)
}

pub fn proposer_of(view: crate::primary::View) -> PublicKey {
    agb::proposer(&test_committee(), view)
}

/// Build a well-formed, directly-published, empty-payload chain of height `n` for
/// `author` in `lm`'s own view. No ack quorum needed: `direct_pub` only requires
/// direct + `payload_ok`, and an empty payload is trivially `payload_ok` regardless of
/// author (`LaneManager::payload_present`'s loop has nothing to check).
pub async fn direct_chain(lm: &mut LaneManager, author: PublicKey, n: u64) -> Vec<Header> {
    let sid = lm.sid().clone();
    let mut prev = lm.genesis().clone();
    let mut headers = Vec::new();
    for h in 1..=n {
        let header = Header::new_vantage(author, h, BTreeMap::new(), prev.clone(), sid.clone());
        lm.process_publish(author, header.clone()).await;
        prev = header.id.clone();
        headers.push(header);
    }
    headers
}

/// A header carrying one distinct payload entry (worker 0), so its digest differs from
/// an otherwise-identical empty-payload header at the same coordinate -- used to build
/// sibling forks.
pub fn tagged_header(
    author: PublicKey,
    height: crate::primary::Height,
    prev: Digest,
    sid: Digest,
    tag: u8,
) -> Header {
    let mut payload = BTreeMap::new();
    let mut bytes = [0u8; 32];
    bytes[0] = tag;
    payload.insert(Digest(bytes), 0u32);
    Header::new_vantage(author, height, payload, prev, sid)
}

pub fn block_ref(header: &Header) -> BlockRef {
    (header.author, header.height, header.id.clone())
}

pub fn mark_ack_available(lm: &mut LaneManager, reference: BlockRef, threshold: AckThreshold) {
    lm.process_ack_availability(AckAvailability {
        reference,
        threshold,
    });
}

pub fn mark_validity_available(lm: &mut LaneManager, reference: BlockRef) {
    mark_ack_available(lm, reference, AckThreshold::Validity);
}

pub fn mark_quorum_available(lm: &mut LaneManager, reference: BlockRef) {
    mark_ack_available(lm, reference, AckThreshold::Quorum);
}

/// Sort a manifest by author -- `Formed_v`'s "strictly increasing author order".
pub fn sorted_manifest(mut m: Manifest) -> Manifest {
    m.sort_by_key(|r| r.0);
    m
}

/// A fresh on-disk store at `path` (removed first, so re-running a test starts clean).
pub fn fresh_store(path: &str) -> Store {
    let _ = std::fs::remove_dir_all(path);
    Store::new(path).unwrap()
}

/// A `LaneManager` plus a second handle onto the *same* store, so a test can write
/// payload-presence markers behind its back (simulating a worker's `OthersBatch`
/// report) the same way `payload_receiver::PayloadReceiver` would in production.
pub fn new_lane_manager(name: PublicKey, path: &str) -> (LaneManager, Store) {
    let store = fresh_store(path);
    (
        LaneManager::new(name, test_committee(), MAX_BLOCK_PAYLOAD, store.clone()),
        store,
    )
}

pub fn new_repairer(name: PublicKey, lm: &LaneManager) -> Repairer {
    Repairer::new(
        name,
        test_committee(),
        lm.sid().clone(),
        lm.genesis().clone(),
        MAX_BLOCK_PAYLOAD,
        lm.blocks_handle(),
    )
}

/// Writes the payload-presence marker `payload_receiver::PayloadReceiver` would write
/// on receiving `OthersBatch(digest, worker_id)` -- the same key shape
/// `LaneManager::payload_present` (D1) probes.
pub async fn mark_payload_present(
    store: &mut Store,
    digest: &crypto::Digest,
    worker_id: config::WorkerId,
) {
    let key = [digest.as_ref(), &worker_id.to_le_bytes()].concat();
    store.write(key, Vec::new()).await;
}

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

pub fn new_agb_engine_with_committee(name: PublicKey, committee: Committee) -> AgbEngine {
    let sid = block::session_id(&committee);
    AgbEngine::new(name, committee, sid, TEST_DELTA_MS)
}

pub fn proposer_of(view: crate::primary::View) -> PublicKey {
    agb::proposer(&test_committee(), view)
}

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

pub fn sorted_manifest(mut m: Manifest) -> Manifest {
    m.sort_by_key(|r| r.0);
    m
}

pub fn fresh_store(path: &str) -> Store {
    let _ = std::fs::remove_dir_all(path);
    Store::new(path).unwrap()
}

pub fn new_lane_manager(name: PublicKey, path: &str) -> (LaneManager, Store) {
    let store = fresh_store(path);
    (
        LaneManager::new(name, test_committee(), MAX_BLOCK_PAYLOAD, store.clone()),
        store,
    )
}

pub fn new_lane_manager_with_committee(
    name: PublicKey,
    path: &str,
    committee: Committee,
) -> (LaneManager, Store) {
    let store = fresh_store(path);
    (
        LaneManager::new(name, committee, MAX_BLOCK_PAYLOAD, store.clone()),
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

pub fn new_repairer_with_committee(
    name: PublicKey,
    lm: &LaneManager,
    committee: Committee,
) -> Repairer {
    Repairer::new(
        name,
        committee,
        lm.sid().clone(),
        lm.genesis().clone(),
        MAX_BLOCK_PAYLOAD,
        lm.blocks_handle(),
    )
}

pub async fn mark_payload_present(
    store: &mut Store,
    digest: &crypto::Digest,
    worker_id: config::WorkerId,
) {
    let key = [digest.as_ref(), &worker_id.to_le_bytes()].concat();
    store.write(key, Vec::new()).await;
}

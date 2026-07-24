// PHASE3-SPEC.md §7 "Chain integrity (N1/N2/N9)".
use super::common::*;
use crate::messages::Header;
use crate::vantage::Effect;
use crypto::Digest;
use std::collections::BTreeMap;

fn is_cached(effects: &[Effect]) -> bool {
    effects.iter().any(|e| matches!(e, Effect::BlockCached(_)))
}

fn is_acked(effects: &[Effect]) -> bool {
    effects.iter().any(|e| matches!(e, Effect::BroadcastAck(_)))
}

/// N1/N2: a well-formed, directly-published height-1 block (empty payload, so D1's
/// payload gate is trivially satisfied) is accepted and immediately becomes DirectPub.
#[tokio::test]
async fn accepts_valid_direct_publish() {
    let (author, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(author, ".db_test_vantage_accept_direct");
    let (header, effects) = lm.publish_own(BTreeMap::new()).await;
    let r = (author, header.height, header.id.clone());
    assert!(lm.direct_pub(&r));
    assert!(is_acked(&effects));
}

/// N9: a block claiming the wrong session id is rejected before storing or counting.
#[tokio::test]
async fn rejects_wrong_sid() {
    let (author, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(author, ".db_test_vantage_wrong_sid");
    let genesis = lm.genesis().clone();
    let wrong_sid = Digest::default();
    let header = Header::new_vantage(author, 1, BTreeMap::new(), genesis, wrong_sid);
    let effects = lm.process_publish(author, header.clone()).await;
    assert!(effects.is_empty());
    assert!(!lm.holds_prefix(&(author, 1, header.id)));
}

/// N9: a non-canonical header (declared `id` doesn't match its recomputed digest) is
/// malformed and rejected -- no state change.
#[tokio::test]
async fn rejects_non_canonical() {
    let (author, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(author, ".db_test_vantage_noncanonical");
    let genesis = lm.genesis().clone();
    let sid = lm.sid().clone();
    let mut header = Header::new_vantage(author, 1, BTreeMap::new(), genesis, sid);
    header.id = Digest::default(); // tamper with the declared id
    let effects = lm.process_publish(author, header.clone()).await;
    assert!(effects.is_empty());
    assert!(!lm.holds_prefix(&(author, 1, header.id)));
}

/// N9/§3.1: a block whose payload exceeds the digest-count cap is rejected outright.
#[tokio::test]
async fn rejects_oversized_payload() {
    let (author, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(author, ".db_test_vantage_oversized");
    let genesis = lm.genesis().clone();
    let sid = lm.sid().clone();
    let mut payload = BTreeMap::new();
    for i in 0..(MAX_BLOCK_PAYLOAD as u64 + 1) {
        let mut bytes = [0u8; 32];
        bytes[0..8].copy_from_slice(&i.to_le_bytes());
        payload.insert(Digest(bytes), 0);
    }
    let header = Header::new_vantage(author, 1, payload, genesis, sid);
    let effects = lm.process_publish(author, header.clone()).await;
    assert!(effects.is_empty());
    assert!(!lm.holds_prefix(&(author, 1, header.id)));
}

/// N1/N2: wrong predecessor -- a height-1 block not pointing at the session genesis is
/// structurally well-formed (`BlockOK` holds) so it *is* cached, but its chain never
/// reaches genesis, so it never becomes `DirectPub`.
#[tokio::test]
async fn rejects_wrong_predecessor() {
    let (author, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(author, ".db_test_vantage_wrong_pred");
    let sid = lm.sid().clone();
    let bogus_prev = Digest([7u8; 32]);
    let header = Header::new_vantage(author, 1, BTreeMap::new(), bogus_prev, sid);
    let r = (author, 1, header.id.clone());
    let effects = lm.process_publish(author, header.clone()).await;
    assert!(is_cached(&effects));
    assert!(!lm.direct_pub(&r));
    assert!(!lm.holds_prefix(&r));
}

/// N1/N2: non-consecutive height (`parent_cert.height` not `height - 1`) fails
/// `BlockOK` on the block alone -- rejected outright, never even cached.
#[tokio::test]
async fn rejects_non_consecutive_height() {
    let (author, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(author, ".db_test_vantage_nonconsecutive");
    let genesis = lm.genesis().clone();
    let sid = lm.sid().clone();
    let mut header = Header::new_vantage(author, 2, BTreeMap::new(), genesis, sid);
    header.parent_cert.height = 5; // `Header::digest()` doesn't fold `parent_cert.height`,
                                   // so `id` is still consistent -- only BlockOK's
                                   // explicit arithmetic check should catch this.
    let effects = lm.process_publish(author, header.clone()).await;
    assert!(effects.is_empty());
    assert!(!lm.holds_prefix(&(author, 2, header.id)));
}

/// N1/N2 "consecutive heights": a block whose predecessor pointer names a *real,
/// already-cached* block at a non-consecutive height (a phantom gap, skipping several
/// heights) must not verify -- each block's own `BlockOK` only checks internal
/// self-consistency (`parent_cert.height + 1 == height`), never the *actual* height of
/// whatever the pointer resolves to, so the chain walk itself must enforce this.
#[tokio::test]
async fn rejects_non_consecutive_real_predecessor() {
    let (author, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(author, ".db_test_vantage_gap_predecessor");
    let genesis = lm.genesis().clone();
    let sid = lm.sid().clone();

    // A real, validly-chained height-1 block.
    let h1 = Header::new_vantage(author, 1, BTreeMap::new(), genesis, sid.clone());
    lm.process_publish(author, h1.clone()).await;

    // A block claiming height 10 whose predecessor pointer names h1 (real height 1) --
    // internally self-consistent (`parent_cert.height` = 9), but the actual referenced
    // block is nowhere near height 9.
    let gap = Header::new_vantage(author, 10, BTreeMap::new(), h1.id.clone(), sid);
    let r = (author, 10, gap.id.clone());
    let effects = lm.process_publish(author, gap.clone()).await;
    assert!(is_cached(&effects)); // structurally well-formed on its own, so still cached
    assert!(!lm.direct_pub(&r));
    assert!(!lm.holds_prefix(&r));
}

/// P1-1: cross-author graft -- Byzantine author A publishes a block whose predecessor
/// pointer names author B's genuine, cached, direct+payload_ok, chain-valid block at
/// the arithmetically-consecutive height. `BlockOK` alone cannot catch this (it only
/// checks A's own height arithmetic, never the author of whatever the pointer resolves
/// to) -- §1's "one author index" clause must be enforced by the walk itself. Uses a
/// height boundary (5 -> B's real height 4) deep enough that the walk actually reaches
/// and inspects B's block, rather than exhausting its height budget first (a height-1
/// graft would already fail via "ran out of height before reaching genesis" without
/// ever checking authorship, which wouldn't regression-test the author check itself).
#[tokio::test]
async fn rejects_cross_author_graft() {
    let (author_b, _) = authors()[0];
    let (author_a, _) = authors()[1];
    let (mut lm, _store) = new_lane_manager(author_b, ".db_test_vantage_cross_author_graft");

    // B honestly builds a real, direct+payload_ok, chain-valid lane to height 4.
    let mut b_tip = None;
    for _ in 0..4 {
        let (header, _) = lm.publish_own(BTreeMap::new()).await;
        b_tip = Some(header);
    }
    let b4 = b_tip.unwrap();
    assert!(lm.direct_pub(&(author_b, 4, b4.id.clone())));

    // A grafts a height-5 block onto B's height-4 block instead of A's own chain.
    let sid = lm.sid().clone();
    let graft = Header::new_vantage(author_a, 5, BTreeMap::new(), b4.id.clone(), sid);
    let r = (author_a, 5, graft.id.clone());
    let effects = lm.process_publish(author_a, graft.clone()).await;

    // Structurally well-formed on its own (BlockOK's arithmetic check can't see this),
    // so it's still cached...
    assert!(effects.iter().any(|e| matches!(e, Effect::BlockCached(_))));
    // ...but must never become DirectPub / get acked (a): the graft must not let A
    // forge a first-hand availability statement by riding on B's real chain.
    assert!(!lm.direct_pub(&r));
    assert!(!effects.iter().any(|e| matches!(e, Effect::BroadcastAck(_))));
    // ...and must never be considered "held" either (b): `holds_prefix` must reject it
    // for the same reason, not just fail to ack it.
    assert!(!lm.holds_prefix(&r));

    // B's own chain must be entirely unaffected by A's attempted graft.
    assert!(lm.direct_pub(&(author_b, 4, b4.id)));
}

/// N1: author != sender -- a relayed publish establishes provenance for nobody (cached,
/// but not DirectPub); the identical bytes later arriving authentically from the real
/// author upgrade the same cache entry (N2 "a later publish may upgrade").
#[tokio::test]
async fn relay_then_authentic_upgrades() {
    let (author, _) = authors()[0];
    let (relayer, _) = authors()[1];
    let (mut lm, _store) = new_lane_manager(author, ".db_test_vantage_relay_upgrade");
    let genesis = lm.genesis().clone();
    let sid = lm.sid().clone();
    let header = Header::new_vantage(author, 1, BTreeMap::new(), genesis, sid);
    let r = (author, 1, header.id.clone());

    let effects = lm.process_publish(relayer, header.clone()).await;
    assert!(is_cached(&effects));
    assert!(!lm.direct_pub(&r));
    assert!(!is_acked(&effects));

    let effects = lm.process_publish(author, header.clone()).await;
    assert!(lm.direct_pub(&r));
    assert!(is_acked(&effects));
}

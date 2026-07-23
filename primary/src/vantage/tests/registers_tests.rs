// PHASE3-SPEC.md §7 "Registers (N5)".
use super::common::*;
use crate::messages::Header;
use crate::vantage::lanes::LaneManager;
use crypto::{Digest, PublicKey};
use std::collections::BTreeMap;
use store::Store;

/// Publishes a block with a single, distinct (pre-marked-present) payload entry so
/// distinct calls produce distinct digests (forks) while still exercising D1's payload
/// gate honestly (watcher != author throughout this file).
async fn publish_block(
    lm: &mut LaneManager,
    store: &mut Store,
    author: PublicKey,
    height: u64,
    prev: Digest,
    sid: Digest,
    marker: u8,
) -> Header {
    let digest = Digest([marker; 32]);
    mark_payload_present(store, &digest, 0).await;
    let mut payload = BTreeMap::new();
    payload.insert(digest, 0u32);
    let header = Header::new_vantage(author, height, payload, prev, sid);
    lm.process_publish(author, header.clone()).await;
    header
}

fn quorum_ack(lm: &mut LaneManager, senders: &[PublicKey], r: (PublicKey, u64, Digest)) {
    for s in senders {
        lm.process_ack(*s, r.clone());
    }
}

/// N5: newest = greatest height, ties at the same height broken by the
/// lexicographically smaller digest.
#[tokio::test]
async fn newest_tiebreak_by_smallest_digest() {
    let all = authors();
    let (watcher, _) = all[0];
    let (author, _) = all[1];
    let senders: Vec<PublicKey> = vec![all[0].0, all[2].0, all[3].0]; // 2f+1 = 3

    let (mut lm, mut store) = new_lane_manager(watcher, ".db_test_vantage_registers_tiebreak");
    let genesis = lm.genesis().clone();
    let sid = lm.sid().clone();

    let h1 = publish_block(&mut lm, &mut store, author, 1, genesis, sid.clone(), 1).await;
    let a2 = publish_block(&mut lm, &mut store, author, 2, h1.id.clone(), sid.clone(), 2).await;
    let b2 = publish_block(&mut lm, &mut store, author, 2, h1.id.clone(), sid.clone(), 3).await;
    assert_ne!(a2.id, b2.id);

    let ra = (author, 2, a2.id.clone());
    let rb = (author, 2, b2.id.clone());
    quorum_ack(&mut lm, &senders, ra.clone());
    quorum_ack(&mut lm, &senders, rb.clone());

    let expected = if a2.id < b2.id { ra } else { rb };
    assert_eq!(lm.c_candidate(&author), Some(expected));
}

/// N5: fork rule -- two branches acked by disjoint senders; C pins one branch, and a
/// taller directly-published tip on the *other* branch is never picked as T. Also
/// covers "strict-containment (T=C height) excluded".
#[tokio::test]
async fn fork_pins_one_branch_for_t() {
    let all = authors();
    let (watcher, _) = all[0];
    let (author, _) = all[1];
    let senders: Vec<PublicKey> = vec![all[0].0, all[2].0, all[3].0];

    let (mut lm, mut store) = new_lane_manager(watcher, ".db_test_vantage_registers_fork");
    let genesis = lm.genesis().clone();
    let sid = lm.sid().clone();

    let h1 = publish_block(&mut lm, &mut store, author, 1, genesis, sid.clone(), 1).await;
    let a2 = publish_block(&mut lm, &mut store, author, 2, h1.id.clone(), sid.clone(), 2).await;
    let b2 = publish_block(&mut lm, &mut store, author, 2, h1.id.clone(), sid.clone(), 3).await;

    // Only branch A reaches quorum -- C is unambiguous.
    let ra2 = (author, 2, a2.id.clone());
    quorum_ack(&mut lm, &senders, ra2.clone());
    assert_eq!(lm.c_candidate(&author), Some(ra2.clone()));

    // Before any strictly-taller tip exists, T is None (nothing satisfies "strictly
    // contains C", and C itself is excluded by the strict height requirement).
    assert_eq!(lm.t_candidate(&author), None);

    // Branch B grows taller than C but never through it -- must never become T.
    let b3 = publish_block(&mut lm, &mut store, author, 3, b2.id.clone(), sid.clone(), 4).await;
    assert_ne!(lm.t_candidate(&author), Some((author, 3, b3.id.clone())));
    assert_eq!(lm.t_candidate(&author), None);

    // Branch A grows through C -- this (and only this) becomes T.
    let a3 = publish_block(&mut lm, &mut store, author, 3, a2.id.clone(), sid.clone(), 5).await;
    assert_eq!(lm.t_candidate(&author), Some((author, 3, a3.id.clone())));
}

/// N5: an author with no C entry is anchored at genesis -- any directly published tip
/// qualifies for T.
#[tokio::test]
async fn no_c_entry_any_direct_tip_qualifies() {
    let all = authors();
    let (watcher, _) = all[0];
    let (author, _) = all[2];

    let (mut lm, mut store) = new_lane_manager(watcher, ".db_test_vantage_registers_no_c");
    let genesis = lm.genesis().clone();
    let sid = lm.sid().clone();

    assert_eq!(lm.c_candidate(&author), None);
    let h1 = publish_block(&mut lm, &mut store, author, 1, genesis, sid, 1).await;
    assert_eq!(lm.t_candidate(&author), Some((author, 1, h1.id)));
}

/// P1-1(c): a cross-author graft must never win T-candidate selection, even though it
/// is taller than the author's own genuine tip -- `direct_pub` already rejects it
/// (chain_tests::rejects_cross_author_graft), so it must never even enter the
/// candidate pool `recompute_registers` scans.
#[tokio::test]
async fn cross_author_graft_never_selected_as_t_candidate() {
    let all = authors();
    let (watcher, _) = all[0];
    let (author_b, _) = all[1];
    let (author_a, _) = all[2];

    let (mut lm, mut store) = new_lane_manager(watcher, ".db_test_vantage_registers_graft");
    let genesis = lm.genesis().clone();
    let sid = lm.sid().clone();

    // B honestly builds a real, direct+payload_ok chain to height 4.
    let mut b_prev = genesis.clone();
    let mut b4 = None;
    for h in 1..=4u64 {
        let block = publish_block(&mut lm, &mut store, author_b, h, b_prev.clone(), sid.clone(), 10 + h as u8).await;
        b_prev = block.id.clone();
        b4 = Some(block);
    }
    let b4 = b4.unwrap();

    // A's own, genuine height-1 tip -- the only *legitimate* T candidate it has.
    let a1 = publish_block(&mut lm, &mut store, author_a, 1, genesis, sid.clone(), 20).await;
    assert_eq!(lm.t_candidate(&author_a), Some((author_a, 1, a1.id.clone())));

    // A grafts a taller, height-5 block onto B's real height-4 block instead of onto
    // A's own chain. Without the author check this would win T-candidate selection
    // outright (taller wins "newest"); with it, it must never even be considered.
    let graft = Header::new_vantage(author_a, 5, BTreeMap::new(), b4.id.clone(), sid);
    lm.process_publish(author_a, graft.clone()).await;

    assert_ne!(lm.t_candidate(&author_a), Some((author_a, 5, graft.id)));
    assert_eq!(lm.t_candidate(&author_a), Some((author_a, 1, a1.id)));
    // B's own registers are untouched by A's attempted graft.
    assert!(lm.direct_pub(&(author_b, 4, b4.id)));
}

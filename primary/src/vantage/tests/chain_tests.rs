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

#[tokio::test]
async fn accepts_valid_direct_publish() {
    let (author, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(author, ".db_test_vantage_accept_direct");
    let (header, effects) = lm.publish_own(BTreeMap::new()).await;
    let r = (author, header.height, header.id.clone());
    assert!(lm.direct_pub(&r));
    assert!(is_acked(&effects));
}

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

#[tokio::test]
async fn rejects_non_canonical() {
    let (author, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(author, ".db_test_vantage_noncanonical");
    let genesis = lm.genesis().clone();
    let sid = lm.sid().clone();
    let mut header = Header::new_vantage(author, 1, BTreeMap::new(), genesis, sid);
    header.id = Digest::default(); // Corrupt the declared ID.
    let effects = lm.process_publish(author, header.clone()).await;
    assert!(effects.is_empty());
    assert!(!lm.holds_prefix(&(author, 1, header.id)));
}

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

#[tokio::test]
async fn pending_direct_payload_blocker_sleeps_until_payload_ready() {
    let (self_name, _) = authors()[3];
    let (author, _) = authors()[0];
    let (mut lm, mut store) = new_lane_manager(self_name, ".db_test_vantage_pending_blocker");
    let sid = lm.sid().clone();
    let genesis = lm.genesis().clone();

    let payload_digest = Digest([91u8; 32]);
    let mut payload = BTreeMap::new();
    payload.insert(payload_digest.clone(), 0);
    let h1 = Header::new_vantage(author, 1, payload, genesis, sid.clone());
    let h1_effects = lm.process_publish(author, h1.clone()).await;
    assert!(!is_acked(&h1_effects));
    let after_h1 = {
        let blocks = lm.blocks_handle();
        let blocks = blocks.lock();
        (blocks.walk_steps(), blocks.walk_failures())
    };

    let h2 = Header::new_vantage(author, 2, BTreeMap::new(), h1.id.clone(), sid);
    let h2_effects = lm.process_publish(author, h2.clone()).await;
    assert!(
        !is_acked(&h2_effects),
        "h2 is direct and payload-ready, but h1's payload still blocks its direct prefix"
    );
    let after_block = {
        let blocks = lm.blocks_handle();
        let blocks = blocks.lock();
        (blocks.walk_steps(), blocks.walk_failures())
    };
    assert_eq!(
        after_block, after_h1,
        "a direct child of a known payload-missing block should sleep without one walk"
    );

    let relay_effects = lm.process_publish(self_name, h2.clone()).await;
    assert!(!is_acked(&relay_effects));
    let after_relay = {
        let blocks = lm.blocks_handle();
        let blocks = blocks.lock();
        (blocks.walk_steps(), blocks.walk_failures())
    };
    assert_eq!(
        after_relay, after_block,
        "a candidate already blocked on a specific payload hole must sleep, not re-walk"
    );

    let h3 = Header::new_vantage(author, 3, BTreeMap::new(), h2.id.clone(), lm.sid().clone());
    let h3_effects = lm.process_publish(author, h3.clone()).await;
    assert!(!is_acked(&h3_effects));
    let after_inherited = {
        let blocks = lm.blocks_handle();
        let blocks = blocks.lock();
        (blocks.walk_steps(), blocks.walk_failures())
    };
    assert_eq!(
        after_inherited, after_relay,
        "a new descendant of a blocked ref should inherit the blocker without one walk"
    );

    let h3_ref = block_ref(&h3);
    for _ in 0..10 {
        assert!(!lm.author_ok(&h3_ref));
    }
    let after_author_checks = {
        let blocks = lm.blocks_handle();
        let blocks = blocks.lock();
        (blocks.walk_steps(), blocks.walk_failures())
    };
    assert_eq!(
        after_author_checks, after_inherited,
        "a known direct-prefix blocker must bypass repeated author checks"
    );

    mark_payload_present(&mut store, &payload_digest, 0).await;
    let wake_effects = lm.set_payload_ready(&h1.id);
    let ack_count = wake_effects
        .iter()
        .filter(|e| matches!(e, Effect::BroadcastAck(_)))
        .count();
    assert_eq!(
        ack_count, 3,
        "payload readiness should ack the blocker first, then wake and ack descendants"
    );
    assert!(lm.direct_pub(&block_ref(&h3)));
    assert!(lm.direct_pub(&block_ref(&h2)));
}

#[tokio::test]
async fn rejects_non_consecutive_height() {
    let (author, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(author, ".db_test_vantage_nonconsecutive");
    let genesis = lm.genesis().clone();
    let sid = lm.sid().clone();
    let mut header = Header::new_vantage(author, 2, BTreeMap::new(), genesis, sid);
    header.parent_cert.height = 5; // The digest excludes `parent_cert.height`.
    let effects = lm.process_publish(author, header.clone()).await;
    assert!(effects.is_empty());
    assert!(!lm.holds_prefix(&(author, 2, header.id)));
}

#[tokio::test]
async fn rejects_non_consecutive_real_predecessor() {
    let (author, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(author, ".db_test_vantage_gap_predecessor");
    let genesis = lm.genesis().clone();
    let sid = lm.sid().clone();

    let h1 = Header::new_vantage(author, 1, BTreeMap::new(), genesis, sid.clone());
    lm.process_publish(author, h1.clone()).await;

    let gap = Header::new_vantage(author, 10, BTreeMap::new(), h1.id.clone(), sid);
    let r = (author, 10, gap.id.clone());
    let effects = lm.process_publish(author, gap.clone()).await;
    assert!(is_cached(&effects)); // The header is valid without its missing predecessor.
    assert!(!lm.direct_pub(&r));
    assert!(!lm.holds_prefix(&r));
}

#[tokio::test]
async fn rejects_cross_author_graft() {
    let (author_b, _) = authors()[0];
    let (author_a, _) = authors()[1];
    let (mut lm, _store) = new_lane_manager(author_b, ".db_test_vantage_cross_author_graft");

    let mut b_tip = None;
    for _ in 0..4 {
        let (header, _) = lm.publish_own(BTreeMap::new()).await;
        b_tip = Some(header);
    }
    let b4 = b_tip.unwrap();
    assert!(lm.direct_pub(&(author_b, 4, b4.id.clone())));

    let sid = lm.sid().clone();
    let graft = Header::new_vantage(author_a, 5, BTreeMap::new(), b4.id.clone(), sid);
    let r = (author_a, 5, graft.id.clone());
    let effects = lm.process_publish(author_a, graft.clone()).await;

    assert!(effects.iter().any(|e| matches!(e, Effect::BlockCached(_))));
    assert!(!lm.direct_pub(&r));
    assert!(!effects.iter().any(|e| matches!(e, Effect::BroadcastAck(_))));
    assert!(!lm.holds_prefix(&r));

    assert!(lm.direct_pub(&(author_b, 4, b4.id)));
}

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

#[tokio::test]
async fn restart_continues_the_lane_instead_of_forking_it() {
    let (author, _) = authors()[0];
    let (mut lm, store) = new_lane_manager(author, ".db_test_vantage_restart_frontier");

    let mut payload = BTreeMap::new();
    payload.insert(Digest::default(), 0);
    let (first, _) = lm.publish_own(payload).await;
    assert_eq!(first.height, 1);

    let mut restarted = crate::vantage::lanes::LaneManager::new(
        author,
        test_committee(),
        MAX_BLOCK_PAYLOAD,
        store.clone(),
    );
    restarted.restore_own_frontier().await;
    assert_eq!(
        restarted.own_tip_height(),
        1,
        "restart must adopt the persisted lane frontier"
    );

    let (second, _) = restarted.publish_own(BTreeMap::new()).await;
    assert_eq!(second.height, 2, "restart must not reuse a spent height");
    assert_eq!(
        second.parent_cert.header_digest, first.id,
        "the post-restart block must chain onto the pre-restart tip, not genesis"
    );
}

#[tokio::test]
async fn restart_can_still_prove_its_own_lane() {
    let (author, _) = authors()[0];
    let (mut lm, store) = new_lane_manager(author, ".db_test_vantage_restart_anchor");
    let (first, _) = lm.publish_own(BTreeMap::new()).await;

    let mut restarted = crate::vantage::lanes::LaneManager::new(
        author,
        test_committee(),
        MAX_BLOCK_PAYLOAD,
        store.clone(),
    );
    restarted.restore_own_frontier().await;

    let tip = (author, first.height, first.id.clone());
    assert!(
        restarted.direct_pub(&tip),
        "the restored frontier header must be seeded as a verified anchor"
    );
    assert!(restarted.holds_prefix(&tip));

    let (second, effects) = restarted.publish_own(BTreeMap::new()).await;
    let r = (author, second.height, second.id.clone());
    assert!(
        restarted.direct_pub(&r),
        "a post-restart block must verify through the seeded anchor"
    );
    assert!(
        is_acked(&effects),
        "the post-restart publish must re-arm the ack pipeline"
    );

    let anchor = restarted
        .take_seeded_anchor()
        .expect("the restore must stage the anchor for re-broadcast");
    assert_eq!(anchor.id, first.id);
    assert!(
        restarted.take_seeded_anchor().is_none(),
        "the anchor is taken once"
    );
}

#[tokio::test]
async fn checkpoint_reconciliation_replaces_an_uncommitted_local_fork() {
    let (author, _) = authors()[0];
    let (mut source, _source_store) = new_lane_manager(author, ".db_test_vantage_reconcile_source");
    let (committed_one, _) = source.publish_own(BTreeMap::new()).await;
    let (committed_two, _) = source.publish_own(BTreeMap::new()).await;

    let (mut recovering, recovering_store) =
        new_lane_manager(author, ".db_test_vantage_reconcile_local");
    let mut conflicting_payload = BTreeMap::new();
    conflicting_payload.insert(Digest([0xA5; 32]), 0);
    let (fork_one, _) = recovering.publish_own(conflicting_payload).await;
    let (fork_two, _) = recovering.publish_own(BTreeMap::new()).await;
    let (fork_three, _) = recovering.publish_own(BTreeMap::new()).await;
    assert_ne!(fork_one.id, committed_one.id);
    assert_ne!(fork_two.id, committed_two.id);
    assert_eq!(fork_three.height, 3);

    assert!(recovering.recover_own_frontier(committed_two.clone()).await);
    assert_eq!(recovering.own_tip_height(), 2);
    assert_eq!(recovering.own_direct_frontier(&author), 2);

    let (next, effects) = recovering.publish_own(BTreeMap::new()).await;
    assert_eq!(next.height, 3);
    assert_eq!(next.parent_cert.header_digest, committed_two.id);
    let next_ref = (author, 3, next.id.clone());
    assert!(
        recovering.holds_prefix(&next_ref),
        "the recovered chain is valid"
    );
    let blocks = recovering.blocks_handle();
    {
        let cache = blocks.lock();
        let next_entry = cache.get(&next.id).expect("new block is cached");
        let anchor_entry = cache
            .get(&committed_two.id)
            .expect("recovered anchor is cached");
        assert!(
            next_entry.direct && next_entry.payload_ok && next_entry.direct_prefix_verified,
            "new block must pass the seeded direct-prefix gate"
        );
        assert!(anchor_entry.direct_prefix_verified);
    }
    assert!(
        recovering.direct_pub(&next_ref),
        "the recovered anchor enables direct publication"
    );
    assert!(is_acked(&effects));

    let mut restarted = crate::vantage::lanes::LaneManager::new(
        author,
        test_committee(),
        MAX_BLOCK_PAYLOAD,
        recovering_store,
    );
    restarted.restore_own_frontier().await;
    assert_eq!(restarted.own_tip_height(), 3);
}

#[tokio::test]
async fn failing_walk_reports_the_missing_parent() {
    let (author, _) = authors()[0];
    let (mut sender_lm, _s1) = new_lane_manager(author, ".db_test_vantage_missing_parent_src");
    let (first, _) = sender_lm.publish_own(BTreeMap::new()).await;
    let (second, _) = sender_lm.publish_own(BTreeMap::new()).await;

    let (receiver, _) = authors()[1];
    let (mut peer, _s2) = new_lane_manager(receiver, ".db_test_vantage_missing_parent_dst");
    peer.process_publish(author, second.clone()).await;

    let r = (author, second.height, second.id.clone());
    assert!(!peer.direct_pub(&r), "the prefix has a hole");
    let reported = peer.take_missing_parents(8);
    assert!(
        reported.contains(&(author, first.height, first.id.clone())),
        "the walk must report the hole it failed on, got {reported:?}"
    );
    assert!(
        peer.take_missing_parents(8).is_empty(),
        "draining empties the set until a walk re-reports"
    );
}

#[tokio::test]
async fn restart_ignores_a_frontier_from_another_session() {
    let (author, _) = authors()[0];
    let (mut lm, store) = new_lane_manager(author, ".db_test_vantage_restart_other_sid");
    let (first, _) = lm.publish_own(BTreeMap::new()).await;
    assert_eq!(first.height, 1);

    let mut next_committee = test_committee();
    let evicted = *next_committee
        .authorities
        .keys()
        .last()
        .expect("test committee is non-empty");
    next_committee.authorities.remove(&evicted);
    assert_ne!(
        crate::vantage::block::session_id(&next_committee),
        test_sid(),
        "test fixture requires the two committees to differ in sid"
    );

    let mut other = crate::vantage::lanes::LaneManager::new(
        author,
        next_committee,
        MAX_BLOCK_PAYLOAD,
        store.clone(),
    );
    other.restore_own_frontier().await;
    assert_eq!(
        other.own_tip_height(),
        0,
        "a frontier from another session must not be adopted"
    );
}

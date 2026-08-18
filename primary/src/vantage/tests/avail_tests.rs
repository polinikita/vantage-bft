use super::common::*;
use crate::messages::Header;
use crate::vantage::lanes::{AckAggregator, AckAvailability, AckThreshold, AvailEntry};
use crypto::Digest;
use std::collections::BTreeMap;

#[tokio::test]
async fn take_avail_flush_tracks_own_direct_pub_watermark() {
    let (author, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(author, ".db_test_vantage_avail_own_watermark");
    assert!(lm.take_avail_flush().is_none(), "nothing dirty yet");

    let headers = direct_chain(&mut lm, author, 3).await;
    let flush = lm
        .take_avail_flush()
        .expect("advancing DirectPub must dirty the watermark");
    assert_eq!(
        flush,
        vec![AvailEntry {
            author,
            height: 3,
            head: headers[2].id.clone(),
        }]
    );

    assert!(lm.take_avail_flush().is_none());
}

#[tokio::test]
async fn resolve_watermark_credits_full_resolvable_prefix_then_is_monotone() {
    let (watcher, _) = authors()[0];
    let (author, _) = authors()[1];
    let (sender, _) = authors()[2];
    let (mut lm, _store) = new_lane_manager(watcher, ".db_test_vantage_avail_resolve");
    let headers = direct_chain(&mut lm, author, 3).await;

    let entry = AvailEntry {
        author,
        height: 3,
        head: headers[2].id.clone(),
    };
    let refs = lm.resolve_watermark(sender, std::slice::from_ref(&entry));
    let expected = vec![
        (author, 1u64, headers[0].id.clone()),
        (author, 2u64, headers[1].id.clone()),
        (author, 3u64, headers[2].id.clone()),
    ];
    assert_eq!(refs, expected);

    let refs_again = lm.resolve_watermark(sender, &[entry]);
    assert!(
        refs_again.is_empty(),
        "monotone floor: an identical resend must yield zero new credits"
    );
}

#[tokio::test]
async fn resolve_watermark_binds_to_the_named_fork_only() {
    let (watcher, _) = authors()[0];
    let (author, _) = authors()[1];
    let (sender, _) = authors()[2];
    let (mut lm, _store) = new_lane_manager(watcher, ".db_test_vantage_avail_fork");
    let sid = lm.sid().clone();
    let genesis = lm.genesis().clone();

    let a1 = tagged_header(author, 1, genesis.clone(), sid.clone(), 1);
    let a2 = tagged_header(author, 2, a1.id.clone(), sid.clone(), 1);
    let b1 = tagged_header(author, 1, genesis, sid.clone(), 2);
    let b2 = tagged_header(author, 2, b1.id.clone(), sid, 2);
    for h in [&a1, &a2, &b1, &b2] {
        lm.process_publish(author, h.clone()).await;
    }

    let entry_a = AvailEntry {
        author,
        height: 2,
        head: a2.id.clone(),
    };
    let refs_a = lm.resolve_watermark(sender, &[entry_a]);
    assert_eq!(
        refs_a,
        vec![(author, 1, a1.id.clone()), (author, 2, a2.id.clone())]
    );
    assert!(!refs_a.iter().any(|r| r.2 == b1.id || r.2 == b2.id));

    let mut aggregator = AckAggregator::new(test_committee());
    for r in refs_a {
        aggregator.record_ack(sender, r);
    }
    let (third, _) = authors()[3];
    let result = aggregator.record_ack(third, (author, 1, b1.id.clone()));
    assert!(
        result.availability.is_none(),
        "branch B's own ref must still be at exactly one sender after this call -- \
         crediting branch A must never have silently counted `sender` toward branch B too"
    );
}

#[tokio::test]
async fn resolve_watermark_credits_head_alone_when_unresolvable_then_backfills_on_retry() {
    let (watcher, _) = authors()[0];
    let (author, _) = authors()[1];
    let (sender, _) = authors()[2];
    let (mut lm, _store) = new_lane_manager(watcher, ".db_test_vantage_avail_behind_head");
    let sid = lm.sid().clone();
    let genesis = lm.genesis().clone();

    let h1 = Header::new_vantage(author, 1, BTreeMap::new(), genesis, sid.clone());
    let h2 = Header::new_vantage(author, 2, BTreeMap::new(), h1.id.clone(), sid.clone());
    let h3 = Header::new_vantage(author, 3, BTreeMap::new(), h2.id.clone(), sid);

    let entry = AvailEntry {
        author,
        height: 3,
        head: h3.id.clone(),
    };
    let refs = lm.resolve_watermark(sender, &[entry]);
    assert_eq!(
        refs,
        vec![(author, 3, h3.id.clone())],
        "head alone, credited by attestation, since the segment below it is unresolvable"
    );

    lm.process_publish(author, h1.clone()).await;
    lm.process_publish(author, h2.clone()).await;
    lm.process_publish(author, h3.clone()).await;

    let backfilled = lm.retry_pending_avail(&h1.id);
    assert_eq!(
        backfilled,
        vec![
            (sender, (author, 1, h1.id.clone())),
            (sender, (author, 2, h2.id.clone())),
            (sender, (author, 3, h3.id.clone())),
        ]
    );
    assert!(
        lm.retry_pending_avail(&h1.id).is_empty(),
        "a second retry for the same author must yield nothing new"
    );
}

#[tokio::test]
async fn resolve_watermark_credits_reach_the_same_availability_marks_as_direct_acks() {
    let all = authors();
    let (watcher, _) = all[0];
    let (author, _) = all[1];
    let (mut lm, _store) =
        new_lane_manager(watcher, ".db_test_vantage_avail_threshold_equivalence");
    let headers = direct_chain(&mut lm, author, 1).await;
    let r = (author, 1u64, headers[0].id.clone());
    let quorum = test_committee().quorum_threshold();
    let validity = test_committee().validity_threshold();

    let mut aggregator = AckAggregator::new(test_committee());
    for sender in [all[0].0, all[2].0] {
        let entry = AvailEntry {
            author,
            height: 1,
            head: headers[0].id.clone(),
        };
        for r in lm.resolve_watermark(sender, &[entry]) {
            let result = aggregator.record_ack(sender, r);
            if let Some(availability) = result.availability {
                lm.process_ack_availability(availability);
            }
        }
    }
    assert!(
        lm.is_q_available(&r, validity),
        "two distinct senders cross f+1"
    );
    assert!(!lm.is_q_available(&r, quorum), "not yet 2f+1");

    let entry = AvailEntry {
        author,
        height: 1,
        head: headers[0].id.clone(),
    };
    for r in lm.resolve_watermark(all[3].0, &[entry]) {
        let result = aggregator.record_ack(all[3].0, r);
        if let Some(availability) = result.availability {
            lm.process_ack_availability(availability);
        }
    }
    assert!(
        lm.is_q_available(&r, quorum),
        "the third distinct sender's watermark credit must cross 2f+1, exactly as a \
         third direct ack would"
    );
}

#[tokio::test]
async fn height_zero_entry_credits_nothing_and_stashes_nothing() {
    let (watcher, _) = authors()[0];
    let (author, _) = authors()[1];
    let (sender, _) = authors()[2];
    let (mut lm, _store) = new_lane_manager(watcher, ".db_test_vantage_avail_height_zero");
    let sid = lm.sid().clone();
    let genesis = lm.genesis().clone();

    let h1 = Header::new_vantage(author, 1, BTreeMap::new(), genesis, sid);
    let entry = AvailEntry {
        author,
        height: 0,
        head: h1.id.clone(),
    };

    assert!(
        lm.resolve_watermark(sender, &[entry]).is_empty(),
        "the genesis floor already covers height zero, so there is nothing to credit"
    );
    assert!(
        lm.pending_avail_keys_for_test().is_empty(),
        "a covered entry must not be stashed for retry"
    );
}

#[tokio::test]
async fn pending_avail_index_mirrors_the_map_through_stash_and_resolve() {
    let (watcher, _) = authors()[0];
    let (author, _) = authors()[1];
    let (sender_a, _) = authors()[2];
    let (sender_b, _) = authors()[3];
    let (mut lm, _store) = new_lane_manager(watcher, ".db_test_avail_index_mirror");
    let sid = lm.sid().clone();
    let genesis = lm.genesis().clone();

    let h1 = Header::new_vantage(author, 1, BTreeMap::new(), genesis, sid.clone());
    let h2 = Header::new_vantage(author, 2, BTreeMap::new(), h1.id.clone(), sid);
    let entry = AvailEntry {
        author,
        height: 2,
        head: h2.id.clone(),
    };

    let check = |lm: &crate::vantage::lanes::LaneManager, note: &str| {
        assert_eq!(
            lm.pending_avail_index_for_test(),
            lm.pending_avail_keys_for_test(),
            "index drifted from pending_avail {note}"
        );
    };

    check(&lm, "before anything");
    lm.resolve_watermark(sender_a, std::slice::from_ref(&entry));
    check(&lm, "after the first stash");
    lm.resolve_watermark(sender_b, std::slice::from_ref(&entry));
    check(&lm, "after the second stash");
    assert_eq!(lm.pending_avail_keys_for_test().len(), 2);

    lm.process_publish(author, h1.clone()).await;
    lm.process_publish(author, h2.clone()).await;
    let backfilled = lm.retry_pending_avail(&h1.id);
    assert!(
        !backfilled.is_empty(),
        "the retry should have resolved something"
    );
    check(&lm, "after the resolving retry");
    assert!(
        lm.pending_avail_keys_for_test().is_empty(),
        "both stashed entries should have resolved and been removed"
    );
}

#[tokio::test]
async fn a_watermark_does_not_recredit_a_ref_already_at_quorum() {
    let all = authors();
    let (watcher, _) = all[0];
    let (author, _) = all[1];
    let (mut lm, _store) = new_lane_manager(watcher, ".db_test_avail_skip_quorum");
    let chain = direct_chain(&mut lm, author, 2).await;
    let head = chain.last().unwrap();
    let r = (author, 2u64, head.id.clone());

    let entry = AvailEntry {
        author,
        height: 2,
        head: head.id.clone(),
    };
    let first = lm.resolve_watermark(all[2].0, std::slice::from_ref(&entry));
    assert!(
        first.iter().any(|x| x.1 == 2),
        "a ref below quorum must still be credited"
    );

    lm.process_ack_availability(AckAvailability {
        reference: r.clone(),
        threshold: AckThreshold::Quorum,
    });

    let later = lm.resolve_watermark(all[3].0, &[entry]);
    assert!(
        !later.iter().any(|x| x == &r),
        "a ref already at quorum was re-credited: {later:?}"
    );
}

#[tokio::test]
async fn at_quorum_memo_is_bounded_per_author() {
    use crate::vantage::avail::{AvailResolver, AT_QUORUM_HEIGHTS};
    let committee = test_committee();
    let sid = crate::vantage::block::session_id(&committee);
    let genesis = crate::vantage::block::genesis_digest(&sid);
    let (author, _) = authors()[1];
    let mut r = AvailResolver::new(
        committee,
        sid,
        genesis,
        MAX_BLOCK_PAYLOAD,
        std::sync::Arc::new(parking_lot::Mutex::new(
            crate::vantage::lanes::BlockCache::new(),
        )),
    );

    for h in 1..=(AT_QUORUM_HEIGHTS as u64 * 3) {
        r.note_threshold(
            &(author, h, Digest([(h % 251) as u8; 32])),
            crate::vantage::lanes::AckThreshold::Quorum,
        );
    }
    let held = r.at_quorum_len_for_test(&author);
    assert!(
        held <= AT_QUORUM_HEIGHTS,
        "at_quorum grew to {held}, cap is {AT_QUORUM_HEIGHTS}"
    );
    assert!(
        r.is_at_quorum_for_test(&(
            author,
            AT_QUORUM_HEIGHTS as u64 * 3,
            Digest([((AT_QUORUM_HEIGHTS as u64 * 3) % 251) as u8; 32])
        )),
        "pruning dropped the newest entry instead of the oldest"
    );
}

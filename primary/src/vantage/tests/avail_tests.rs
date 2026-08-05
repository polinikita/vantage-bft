// Optional, flag-gated ack-watermark front-end (`Parameters::ack_watermarks`): a
// periodic per-lane availability broadcast that replaces per-block acks --
// `LaneManager::{take_avail_flush, resolve_watermark, retry_pending_avail}`.
use super::common::*;
use crate::messages::Header;
use crate::vantage::lanes::{AckAggregator, AvailEntry};
use std::collections::BTreeMap;

/// Own DIRECT-PREFIX watermark: advances incrementally as blocks are confirmed
/// DirectPub, `take_avail_flush` is full-vector-when-dirty, and a second call with
/// nothing new returns `None` (idempotent).
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

    // Idempotent: nothing new since the last flush.
    assert!(lm.take_avail_flush().is_none());
}

/// Watermark from peer `sender` credits exactly the resolvable refs (heights 1..=3);
/// re-sending the identical vector afterwards yields zero new credits (monotone
/// floor).
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

/// Fork binding: two sibling branches at the same heights; a watermark naming branch
/// A's head credits ONLY branch-A refs -- `collect_verified_suffix`'s own
/// parent-pointer walk never crosses branches -- and branch B's counts in the shared
/// `AckAggregator` are left completely untouched (zero, exactly as before).
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
    // If crediting branch A had ALSO (wrongly) silently counted `sender` toward
    // branch B's refs, then a single additional, genuinely distinct sender would
    // already cross f+1 (2 senders) here. It must not: this call is the first-ever
    // recording for branch B's own ref, so it stays below f+1.
    let (third, _) = authors()[3];
    let result = aggregator.record_ack(third, (author, 1, b1.id.clone()));
    assert!(
        result.availability.is_none(),
        "branch B's own ref must still be at exactly one sender after this call -- \
         crediting branch A must never have silently counted `sender` toward branch B too"
    );
}

/// Behind-the-head: the watermark's head is not yet in the local cache -> the head
/// ref alone is credited (attested, per the same trust model a direct ack already
/// has), the floor is NOT advanced, and the entry is stashed pending. Once the
/// missing blocks arrive and the retry hook fires, the intermediate refs are
/// credited exactly once.
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

    // The missing blocks arrive (e.g. via publish/repair) ...
    lm.process_publish(author, h1.clone()).await;
    lm.process_publish(author, h2.clone()).await;
    lm.process_publish(author, h3.clone()).await;

    // ... and the retry hook (a newly-cached block for this author) backfills the
    // intermediate refs, exactly once.
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

/// Threshold equivalence: a ref reaching 2f+1 via watermark credits triggers the same
/// `AckAvailability` marks (`is_q_available`) as it would via direct acks. n=4, f=1 =>
/// f+1=2, 2f+1=3.
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

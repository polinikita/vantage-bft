// PHASE3-SPEC.md §7 "ACK discipline (N3/N4)".
use super::common::*;
use crate::messages::Header;
use crate::vantage::lanes::{AckAggregator, AckThreshold};
use crate::vantage::Effect;
use crypto::Digest;
use std::collections::BTreeMap;

fn ack_count(effects: &[Effect]) -> usize {
    effects
        .iter()
        .filter(|e| matches!(e, Effect::BroadcastAck(_)))
        .count()
}

/// N3: ack fires exactly once per tuple, even if the same (already-acked) direct
/// publication is reprocessed.
#[tokio::test]
async fn acks_exactly_once_per_tuple() {
    let (author, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(author, ".db_test_vantage_ack_once");
    let (header, effects) = lm.publish_own(BTreeMap::new()).await;
    assert_eq!(ack_count(&effects), 1);

    // Reprocessing the identical direct publish must not re-ack.
    let effects = lm.process_publish(author, header).await;
    assert_eq!(ack_count(&effects), 0);
}

/// N3: a prefix obtained only through repair (never a direct publish) is held but
/// never acked.
#[tokio::test]
async fn repaired_only_prefix_never_acked() {
    let (watcher, _) = authors()[0];
    let (other_author, _) = authors()[1];
    let (mut lm, _store) = new_lane_manager(watcher, ".db_test_vantage_no_ack_repair");
    let mut repairer = new_repairer(watcher, &lm);

    let genesis = lm.genesis().clone();
    let sid = lm.sid().clone();
    let header = Header::new_vantage(other_author, 1, BTreeMap::new(), genesis, sid);
    let r = (other_author, 1, header.id.clone());

    // Arrives only via `serve` (after being requested, P1-2), never via `publish`.
    repairer.authorize(r.clone());
    let effects = repairer.on_serve(header.clone());
    assert!(!effects.iter().any(|e| matches!(e, Effect::BroadcastAck(_))));
    assert!(!lm.direct_pub(&r));
    // It IS cached/held (holds_prefix looks at the shared cache regardless of
    // provenance) -- just never acked.
    assert!(lm.holds_prefix(&r));
}

/// N4: the same ack, received twice from the same sender, counts once.
#[tokio::test]
async fn per_sender_ack_dedup() {
    let (watcher, _) = authors()[0];
    let (author, _) = authors()[1];
    let (sender, _) = authors()[2];
    let r = (author, 1u64, Digest([9u8; 32]));
    let mut aggregator = AckAggregator::new(test_committee());

    assert!(aggregator
        .record_ack(sender, r.clone())
        .availability
        .is_none());
    assert!(aggregator
        .record_ack(sender, r.clone())
        .availability
        .is_none());
    let availability = aggregator
        .record_ack(watcher, r)
        .availability
        .expect("second distinct sender crosses f+1");
    assert_eq!(availability.threshold, AckThreshold::Validity);
}

/// N4: q-available at the exact f+1 / 2f+1 stake boundaries (n=4, f=1 => f+1=2, 2f+1=3).
#[tokio::test]
async fn q_available_exact_boundaries() {
    let (watcher, _) = authors()[0];
    let all = authors();
    let (author, _) = all[1];
    let (mut lm, _store) = new_lane_manager(watcher, ".db_test_vantage_q_available");
    let r = (author, 1u64, Digest([9u8; 32]));
    let mut aggregator = AckAggregator::new(test_committee());

    let validity = 2; // f+1
    let quorum = 3; // 2f+1

    assert!(aggregator
        .record_ack(all[0].0, r.clone())
        .availability
        .is_none());
    assert!(!lm.is_q_available(&r, validity));

    let availability = aggregator
        .record_ack(all[2].0, r.clone())
        .availability
        .expect("second distinct ACK crosses f+1");
    lm.process_ack_availability(availability);
    assert!(lm.is_q_available(&r, validity));
    assert!(!lm.is_q_available(&r, quorum));

    let availability = aggregator
        .record_ack(all[3].0, r.clone())
        .availability
        .expect("third distinct ACK crosses 2f+1");
    lm.process_ack_availability(availability);
    assert!(lm.is_q_available(&r, quorum));
}

/// ACK discipline / D1: withholding one worker batch withholds the ack until it
/// arrives, even though the block itself was directly published in full.
#[tokio::test]
async fn ack_withheld_until_payload_arrives() {
    let (watcher, _) = authors()[0];
    let (author, _) = authors()[1];
    let (mut lm, mut store) = new_lane_manager(watcher, ".db_test_vantage_ack_payload_gate");

    let genesis = lm.genesis().clone();
    let sid = lm.sid().clone();
    let batch_digest = Digest([3u8; 32]);
    let mut payload = BTreeMap::new();
    payload.insert(batch_digest.clone(), 0u32);
    let header = Header::new_vantage(author, 1, payload, genesis, sid);
    let r = (author, 1, header.id.clone());

    let effects = lm.process_publish(author, header.clone()).await;
    assert!(!effects.iter().any(|e| matches!(e, Effect::BroadcastAck(_))));
    assert!(!lm.direct_pub(&r));
    assert!(effects
        .iter()
        .any(|e| matches!(e, Effect::SyncBatches(a, _, _) if *a == author)));

    mark_payload_present(&mut store, &batch_digest, 0u32).await;
    let effects = lm.set_payload_ready(&header.id);
    assert!(effects.iter().any(|e| matches!(e, Effect::BroadcastAck(_))));
    assert!(lm.direct_pub(&r));
}

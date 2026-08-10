use super::common::*;
use crate::messages::Header;
use crate::vantage::repair::Repairer;
use crate::vantage::Effect;
use std::collections::BTreeMap;

#[tokio::test]
async fn acked_prefix_served_to_later_requester() {
    let all = authors();
    let (author, _) = all[0];
    let (requester, _) = all[1];
    let (mut lm, _store) = new_lane_manager(author, ".db_test_vantage_retention_acked");
    let (header, _effects) = lm.publish_own(BTreeMap::new()).await;
    assert!(lm.direct_pub(&(author, header.height, header.id.clone())));

    let mut repairer = Repairer::new(
        author,
        test_committee(),
        lm.sid().clone(),
        lm.genesis().clone(),
        MAX_BLOCK_PAYLOAD,
        lm.blocks_handle(),
    );
    let effects = repairer.on_request(requester, header.id.clone());
    assert_eq!(
        effects
            .iter()
            .filter(|e| matches!(e, Effect::ServeTo(p, b) if *p == requester && b.id == header.id))
            .count(),
        1
    );
}

#[tokio::test]
async fn late_request_still_served_after_retention() {
    let all = authors();
    let (watcher, _) = all[0];
    let (author, _) = all[1];
    let (requester, _) = all[2];

    let (lm, _store) = new_lane_manager(watcher, ".db_test_vantage_retention_late");
    let sid = lm.sid().clone();
    let genesis = lm.genesis().clone();
    let mut repairer = Repairer::new(
        watcher,
        test_committee(),
        sid.clone(),
        genesis.clone(),
        MAX_BLOCK_PAYLOAD,
        lm.blocks_handle(),
    );

    let block = Header::new_vantage(author, 1, BTreeMap::new(), genesis, sid);
    let h = block.id.clone();
    repairer.authorize((author, 1, h.clone()));
    repairer.on_serve(block.clone());

    let effects = repairer.on_request(requester, h.clone());
    assert_eq!(
        effects
            .iter()
            .filter(|e| matches!(e, Effect::ServeTo(p, b) if *p == requester && b.id == h))
            .count(),
        1
    );
}

#[tokio::test]
async fn no_discard_on_local_events() {
    let all = authors();
    let (author, _) = all[0];
    let (mut lm, _store) = new_lane_manager(author, ".db_test_vantage_retention_no_discard");
    let (header, _) = lm.publish_own(BTreeMap::new()).await;
    let r = (author, header.height, header.id.clone());
    assert!(lm.holds_prefix(&r));

    let (other, _) = all[1];
    mark_validity_available(&mut lm, (other, 99, crypto::Digest([1u8; 32])));
    let (_second_header, _) = lm.publish_own(BTreeMap::new()).await;

    assert!(lm.holds_prefix(&r));
    assert!(lm.direct_pub(&r));
}

// PHASE3-SPEC.md §6.4 -- vantage metrics counters actually observe, not just compile.
use super::common::*;
use crate::vantage::repair::Repairer;
use crypto::Digest;
use metrics::Metrics;
use prometheus::Registry;
use std::collections::BTreeMap;

#[tokio::test]
async fn lane_manager_counters_observe() {
    let (author, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(author, ".db_test_vantage_metrics_lanes");
    let registry = Registry::new();
    let (metrics, _reporter) = Metrics::new(&registry);
    lm = lm.with_metrics(metrics.clone());

    let (_header, _effects) = lm.publish_own(BTreeMap::new()).await;

    assert_eq!(metrics.vantage_blocks_published.get(), 1);
    assert_eq!(metrics.vantage_acks_sent.get(), 1);
    assert!(metrics.vantage_retained_bytes.get() > 0);

    // A relayed (non-self-authored) publish increments `blocks_received`, and an
    // externally-counted ack increments `acks_received`.
    let (other, _) = authors()[1];
    let genesis = lm.genesis().clone();
    let sid = lm.sid().clone();
    let header = crate::messages::Header::new_vantage(other, 1, BTreeMap::new(), genesis, sid);
    lm.process_publish(other, header.clone()).await;
    assert_eq!(metrics.vantage_blocks_received.get(), 1);

    lm.process_ack(authors()[2].0, (other, 1, header.id));
    assert_eq!(metrics.vantage_acks_received.get(), 1);
}

#[tokio::test]
async fn repairer_counters_observe() {
    let all = authors();
    let (watcher, _) = all[0];
    let (author, _) = all[1];
    let (requester, _) = all[2];

    let (lm, _store) = new_lane_manager(watcher, ".db_test_vantage_metrics_repair");
    let sid = lm.sid().clone();
    let genesis = lm.genesis().clone();
    let registry = Registry::new();
    let (metrics, _reporter) = Metrics::new(&registry);

    let mut repairer =
        Repairer::new(watcher, test_committee(), sid.clone(), genesis.clone(), MAX_BLOCK_PAYLOAD, lm.blocks_handle())
            .with_metrics(metrics.clone());

    let block = crate::messages::Header::new_vantage(author, 1, BTreeMap::new(), genesis, sid);
    let h = block.id.clone();

    repairer.authorize((author, 1, Digest([1u8; 32]))); // unrelated digest -> pure request fan-out
    assert!(metrics.vantage_repairs_requested.get() > 0);

    repairer.on_request(requester, h.clone());
    repairer.authorize((author, 1, h));
    repairer.on_serve(block);
    assert_eq!(metrics.vantage_repairs_served.get(), 1);
}

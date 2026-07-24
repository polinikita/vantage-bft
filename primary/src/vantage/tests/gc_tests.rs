use super::common::*;
use crate::vantage::control::ControlLog;
use crate::vantage::Frontier;
use crypto::Digest;
use std::time::Instant;

#[tokio::test]
async fn agb_gc_makes_old_view_messages_noops() {
    let all = authors();
    let (name, _) = all[0];
    let (sender, _) = all[1];
    let (mut lm, _store) = new_lane_manager(name, ".db_test_vantage_gc_agb");
    let mut rep = new_repairer(name, &lm);
    let mut agb = new_agb_engine(name);

    agb.enter(1, Instant::now(), &mut lm, &mut rep);
    assert!(agb.has_any_state(1));

    agb.gc_below(2);

    assert!(agb.is_sealed(1));
    assert!(agb.echo_sent(1));
    assert!(agb.ready_sent(1));
    assert!(agb.on_echo_skip(1, sender).is_empty());
    assert!(agb.on_ready_timer(1).is_empty());
}

#[test]
fn frontier_gc_drops_old_view_state() {
    let (name, _) = authors()[0];
    let mut frontier = Frontier::new(name, test_committee());

    assert_eq!(frontier.enter(1), vec![1]);
    assert!(frontier.is_active(1));

    frontier.gc_below(2);

    assert!(!frontier.is_active(1));
    assert_eq!(frontier.a_i(), 1);
    assert!(frontier.enter(1).is_empty());
    assert!(frontier.record_fixed(1, true).is_empty());
    assert_eq!(frontier.record_fixed(2, true), vec![2]);
}

#[test]
fn control_gc_drops_old_view_census_and_resolves_old_anchors() {
    let all = authors();
    let (name, _) = all[0];
    let (sender, _) = all[1];
    let mut control = ControlLog::new(name, test_committee(), test_sid(), TEST_DELTA_MS);
    let digest = Digest([7; 32]);

    control.on_comp_report(1, digest.clone(), sender);
    assert_eq!(control.report_count_for(1, &digest), 1);

    control.gc_below(2);

    assert_eq!(control.report_count_for(1, &digest), 0);
    assert!(control.is_anchor_resolved(1));
    assert!(control.on_comp_report(1, digest, sender).is_empty());
}

use super::common::*;
use crate::vantage::Frontier;

fn new_frontier() -> Frontier {
    Frontier::new(authors()[3].0, test_committee())
}

#[tokio::test]
async fn frontier_trigger_boundary_a_i_v_minus_2_means_no_propose() {
    let proposer2 = crate::vantage::agb::proposer(&test_committee(), 2);
    let proposer1 = crate::vantage::agb::proposer(&test_committee(), 1);
    assert_ne!(
        proposer1, proposer2,
        "test fixture requires distinct round-robin proposers"
    );

    let mut frontier = Frontier::new(proposer2, test_committee());
    let (lm, _store) = new_lane_manager(proposer2, ".db_test_frontier_boundary");
    assert!(frontier.try_propose(&lm, None).is_none());

    frontier.record_fixed(1, true);
    assert!(frontier.try_propose(&lm, None).is_some());
}

#[tokio::test]
async fn r1_proposes_exactly_once_for_its_own_turn() {
    let proposer1 = crate::vantage::agb::proposer(&test_committee(), 1);
    let mut frontier = Frontier::new(proposer1, test_committee());
    let (lm, _store) = new_lane_manager(proposer1, ".db_test_frontier_propose_once");
    let first = frontier.try_propose(&lm, None);
    assert!(
        first.is_some(),
        "a_i=0 >= view1-1=0 must allow proposer(1) to propose"
    );
    assert_eq!(first.unwrap().view, 1);
    let second = frontier.try_propose(&lm, None);
    assert!(second.is_none());
}

#[tokio::test]
async fn non_proposer_never_triggers_r1() {
    let proposer1 = crate::vantage::agb::proposer(&test_committee(), 1);
    let (other, _) = authors()
        .into_iter()
        .find(|(pk, _)| *pk != proposer1)
        .unwrap();
    let mut frontier = Frontier::new(other, test_committee());
    let (lm, _store) = new_lane_manager(other, ".db_test_frontier_not_proposer");
    assert!(frontier.try_propose(&lm, None).is_none());
}

#[tokio::test]
async fn construction_determinism_from_register_state() {
    let proposer1 = crate::vantage::agb::proposer(&test_committee(), 1);
    let (author_c, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(proposer1, ".db_test_frontier_determinism");
    direct_chain(&mut lm, author_c, 2).await;

    let mut f1 = Frontier::new(proposer1, test_committee());
    let mut f2 = Frontier::new(proposer1, test_committee());
    let p1 = f1.try_propose(&lm, None).unwrap();
    let p2 = f2.try_propose(&lm, None).unwrap();
    assert_eq!(p1.c, p2.c);
    assert_eq!(p1.t, p2.t);
}

#[test]
fn buffered_proposal_activates_when_contiguous_prefix_arrives() {
    let mut frontier = new_frontier();
    let activated = frontier.record_fixed(2, true);
    assert!(activated.is_empty());
    assert_eq!(frontier.a_i(), 0);
    assert!(!frontier.is_active(2));

    let activated = frontier.record_fixed(1, true);
    assert_eq!(activated, vec![1, 2]);
    assert_eq!(frontier.a_i(), 2);
    assert!(frontier.is_active(1) && frontier.is_active(2));
}

#[test]
fn malformed_fixed_proposal_never_advances_frontier() {
    let mut frontier = new_frontier();
    let activated = frontier.record_fixed(1, false); // Reject view 1.
    assert!(activated.is_empty());
    assert_eq!(frontier.a_i(), 0);
    let activated = frontier.record_fixed(2, true);
    assert!(activated.is_empty());
    assert_eq!(frontier.a_i(), 0);
}

#[test]
fn enter_also_activates_independent_of_frontier_advance() {
    let mut frontier = new_frontier();
    assert!(!frontier.is_active(1));
    let activated = frontier.enter(1);
    assert_eq!(activated, vec![1]);
    assert!(frontier.is_active(1));
    assert_eq!(
        frontier.a_i(),
        0,
        "entering v=1 floors a_i to v-1=0, a no-op here"
    );
    assert_eq!(frontier.enter(1), Vec::<crate::primary::View>::new());
}

#[test]
fn enter_floors_a_i_to_v_minus_1() {
    let mut frontier = new_frontier();
    let activated = frontier.enter(5);
    assert_eq!(activated, vec![5]);
    assert_eq!(frontier.a_i(), 4, "entering v=5 must floor a_i to v-1=4");
    assert!(frontier.is_active(5));
}

#[test]
fn enter_floor_never_lowers_a_i() {
    let mut frontier = new_frontier();
    frontier.record_fixed(1, true);
    frontier.record_fixed(2, true);
    assert_eq!(frontier.a_i(), 2);
    frontier.enter(2);
    assert_eq!(frontier.a_i(), 2);
}

#[test]
fn enter_floor_re_runs_contiguous_advance_from_new_floor() {
    let mut frontier = new_frontier();
    let activated = frontier.record_fixed(6, true);
    assert!(activated.is_empty());
    assert_eq!(frontier.a_i(), 0);

    let activated = frontier.enter(6);
    assert_eq!(activated, vec![6]);
    assert_eq!(frontier.a_i(), 6);
}

#[tokio::test]
async fn enter_floor_enables_r1_without_having_seen_v_minus_1() {
    let view = 6;
    let this_proposer = crate::vantage::agb::proposer(&test_committee(), view);
    assert_ne!(
        this_proposer,
        crate::vantage::agb::proposer(&test_committee(), 1)
    );
    let mut frontier = Frontier::new(this_proposer, test_committee());
    let (lm, _store) = new_lane_manager(this_proposer, ".db_test_frontier_floor_enables_r1");

    assert!(
        frontier.try_propose(&lm, None).is_none(),
        "a_i=0 must not yet allow proposing view 6"
    );
    frontier.enter(view);
    assert_eq!(frontier.a_i(), view - 1);
    let proposal = frontier.try_propose(&lm, None);
    assert_eq!(proposal.map(|p| p.view), Some(view));
}

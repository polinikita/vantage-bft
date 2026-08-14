use super::common::*;
use crate::messages::Header;
use crate::vantage::Frontier;
use std::collections::BTreeMap;

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

#[tokio::test]
async fn completed_open_tip_reprobes_at_exponential_own_turns_and_clears() {
    let proposer1 = crate::vantage::agb::proposer(&test_committee(), 1);
    let (author, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(proposer1, ".db_test_frontier_mixed_tip_quarantine");
    let mut frontier = Frontier::new(proposer1, test_committee());

    let first_chain = direct_chain(&mut lm, author, 1).await;
    let first_ref = block_ref(&first_chain[0]);
    let first = frontier.propose_view(1, &lm, None).unwrap();
    assert_eq!(first.t, vec![first_ref.clone()]);

    let second_header = Header::new_vantage(
        author,
        2,
        BTreeMap::new(),
        first_ref.2.clone(),
        lm.sid().clone(),
    );
    lm.process_publish(author, second_header.clone()).await;
    let second_ref = block_ref(&second_header);
    let second = frontier.propose_view(5, &lm, None).unwrap();
    assert_eq!(
        second.t,
        vec![second_ref.clone()],
        "ordinary inclusion must not suppress a fault-free author's next tip"
    );

    frontier.quarantine_tips(&second.t, &lm);
    assert_eq!(
        frontier.quarantine_for_test(&author),
        Some((second_ref.clone(), 0, 3)),
        "a new witness is due on the next local proposer opportunity"
    );
    let third_header = Header::new_vantage(
        author,
        3,
        BTreeMap::new(),
        second_ref.2.clone(),
        lm.sid().clone(),
    );
    lm.process_publish(author, third_header.clone()).await;
    let third_ref = block_ref(&third_header);
    let third = frontier.propose_view(9, &lm, None).unwrap();
    assert_eq!(
        third.t,
        vec![third_ref.clone()],
        "the first due turn re-probes the newest direct tip, not the stored witness"
    );
    assert_eq!(
        frontier.quarantine_for_test(&author),
        Some((second_ref.clone(), 1, 4))
    );

    let fourth_header = Header::new_vantage(
        author,
        4,
        BTreeMap::new(),
        third_ref.2.clone(),
        lm.sid().clone(),
    );
    lm.process_publish(author, fourth_header.clone()).await;
    let fourth_ref = block_ref(&fourth_header);
    let fourth = frontier.propose_view(13, &lm, None).unwrap();
    assert_eq!(fourth.t, vec![fourth_ref.clone()]);
    assert_eq!(
        frontier.quarantine_for_test(&author),
        Some((second_ref.clone(), 2, 6)),
        "after probe gaps 1 then 2 local opportunities"
    );

    let fifth_header = Header::new_vantage(
        author,
        5,
        BTreeMap::new(),
        fourth_ref.2.clone(),
        lm.sid().clone(),
    );
    lm.process_publish(author, fifth_header.clone()).await;
    let fifth_ref = block_ref(&fifth_header);
    let fifth = frontier.propose_view(17, &lm, None).unwrap();
    assert!(
        fifth.t.iter().all(|r| r.0 != author),
        "the intervening own turn suppresses ordinary inclusion"
    );

    mark_quorum_available(&mut lm, second_ref.clone());
    let sixth = frontier.propose_view(21, &lm, None).unwrap();
    assert!(sixth.c.contains(&second_ref));
    assert!(
        sixth.t.contains(&fifth_ref),
        "a quorum-available quarantine witness must reopen the author's tip slot"
    );
    assert!(frontier.quarantine_for_test(&author).is_none());
}

#[tokio::test]
async fn reprobe_selects_one_eligible_author_and_skips_an_ineligible_earlier_one() {
    let proposer1 = crate::vantage::agb::proposer(&test_committee(), 1);
    let all = authors();
    let ineligible = all[0].0;
    let eligible_a = all[1].0;
    let eligible_b = all[2].0;
    let (mut lm, _store) = new_lane_manager(proposer1, ".db_test_frontier_probe_fair_selection");
    let mut frontier = Frontier::new(proposer1, test_committee());

    let a_ref = block_ref(&direct_chain(&mut lm, eligible_a, 1).await[0]);
    let b_ref = block_ref(&direct_chain(&mut lm, eligible_b, 1).await[0]);
    frontier.propose_view(1, &lm, None).unwrap();
    let fabricated = (ineligible, 1, crypto::Digest([250; 32]));
    frontier.quarantine_tips(&vec![fabricated.clone(), a_ref.clone(), b_ref.clone()], &lm);

    let first_probe = frontier.propose_view(5, &lm, None).unwrap();
    assert_eq!(first_probe.t.len(), 1, "at most one re-probe per proposal");
    assert!(first_probe.t[0] == a_ref || first_probe.t[0] == b_ref);
    assert_eq!(
        frontier.quarantine_for_test(&ineligible),
        Some((fabricated, 0, 2)),
        "an ineligible earlier author neither blocks another nor advances"
    );

    let second_probe = frontier.propose_view(9, &lm, None).unwrap();
    assert_eq!(second_probe.t.len(), 1);
    assert_ne!(
        first_probe.t[0].0, second_probe.t[0].0,
        "earliest-due selection must give the other eligible author its turn"
    );
}

#[tokio::test]
async fn reprobe_preserves_canonical_order_with_ordinary_tips() {
    let committee = test_committee();
    let proposer1 = crate::vantage::agb::proposer(&committee, 1);
    let authors: Vec<_> = committee.authorities.keys().copied().collect();
    let quarantined = authors[0];
    let (mut lm, _store) = new_lane_manager(proposer1, ".db_test_frontier_probe_canonical_order");
    let mut frontier = Frontier::new(proposer1, committee);

    for author in &authors {
        direct_chain(&mut lm, *author, 1).await;
    }
    let initial = frontier.propose_view(1, &lm, None).unwrap();
    let witness = initial
        .t
        .iter()
        .find(|r| r.0 == quarantined)
        .expect("the first author has a tip")
        .clone();
    frontier.quarantine_tips(&vec![witness], &lm);

    let proposal = frontier.propose_view(5, &lm, None).unwrap();
    assert_eq!(proposal.t.len(), authors.len());
    assert!(
        proposal.t.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "a probe inserted after ordinary selection must still satisfy Formed order"
    );
}

#[tokio::test]
async fn reprobe_confirms_a_stable_quorum_prefix_before_chasing_the_newest_tip() {
    let committee = test_committee();
    let proposer1 = crate::vantage::agb::proposer(&committee, 1);
    let author = *committee.authorities.keys().next().unwrap();
    let (mut lm, _store) = new_lane_manager(proposer1, ".db_test_frontier_probe_confirmation");
    let mut frontier = Frontier::new(proposer1, committee);
    let chain = direct_chain(&mut lm, author, 3).await;
    let stable = block_ref(&chain[0]);
    let newest = block_ref(&chain[2]);

    let initial = frontier.propose_view(1, &lm, None).unwrap();
    assert!(initial.t.contains(&newest));
    frontier.quarantine_tips(&initial.t, &lm);
    lm.process_claim_availability(crate::vantage::lanes::AckAvailability {
        reference: stable.clone(),
        threshold: crate::vantage::lanes::AckThreshold::Quorum,
    });
    lm.process_claim_availability(crate::vantage::lanes::AckAvailability {
        reference: block_ref(&chain[1]),
        threshold: crate::vantage::lanes::AckThreshold::Quorum,
    });

    let probe = frontier.propose_view(5, &lm, None).unwrap();
    assert_eq!(
        probe.t,
        vec![stable],
        "the oldest unconfirmed quorum prefix must stay fixed despite fresher candidates"
    );
    assert!(
        frontier.quarantine_for_test(&author).is_some(),
        "a digest-free claim quorum must not clear quarantine before exact confirmation"
    );
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

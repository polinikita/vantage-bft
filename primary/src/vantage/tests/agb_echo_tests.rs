use super::common::*;
use crate::vantage::agb::{self, formed, Echo, EchoOut, TimerKind, ViewProposal};
use crate::vantage::Effect;
use crypto::Digest;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

fn echo_effect(effects: &[Effect]) -> Option<&Echo> {
    effects.iter().find_map(|e| match e {
        Effect::BroadcastEcho(EchoOut::Single(echo)) => Some(echo),
        _ => None,
    })
}

fn skip_effect(effects: &[Effect]) -> bool {
    effects
        .iter()
        .any(|e| matches!(e, Effect::BroadcastEchoSkip(_)))
}

fn fixed_effect(effects: &[Effect]) -> Option<bool> {
    effects.iter().find_map(|e| match e {
        Effect::Fixed(_, well_formed) => Some(*well_formed),
        _ => None,
    })
}

#[test]
fn formed_rejects_unsorted_or_duplicate_author() {
    let committee = test_committee();
    let (a0, _) = authors()[0];
    let (a1, _) = authors()[1];
    let (small, big) = if a0 < a1 { (a0, a1) } else { (a1, a0) };
    let unsorted = vec![(big, 1, Digest([1u8; 32])), (small, 2, Digest([2u8; 32]))];
    assert!(!formed(&committee, 100, &unsorted, &Vec::new(), &None));
    let duplicate_author = vec![(a0, 1, Digest([1u8; 32])), (a0, 2, Digest([2u8; 32]))];
    assert!(!formed(
        &committee,
        100,
        &duplicate_author,
        &Vec::new(),
        &None
    ));
}

#[test]
fn formed_rejects_duplicate_hash_across_c_and_t() {
    let committee = test_committee();
    let (a0, _) = authors()[0];
    let (a1, _) = authors()[1];
    let shared = Digest([9u8; 32]);
    let c = vec![(a0, 1, shared.clone())];
    let t = vec![(a1, 1, shared)];
    assert!(!formed(&committee, 100, &c, &t, &None));
}

#[test]
fn formed_accepts_well_formed_disjoint_manifests() {
    let committee = test_committee();
    let (a0, _) = authors()[0];
    let (a1, _) = authors()[1];
    let c = vec![(a0, 1, Digest([1u8; 32]))];
    let t = vec![(a1, 1, Digest([2u8; 32]))];
    assert!(formed(&committee, 100, &c, &t, &None));
}

#[test]
fn proposer_round_robins_over_committee_in_sorted_order() {
    let committee = test_committee();
    let names: Vec<_> = committee.authorities.keys().cloned().collect();
    for v in 1..=8u64 {
        assert_eq!(
            agb::proposer(&committee, v),
            names[((v - 1) % names.len() as u64) as usize]
        );
    }
    assert_eq!(
        agb::proposer(&committee, 1),
        agb::proposer(&committee, 1 + names.len() as u64)
    );
}

async fn positive_scenario(
    path: &str,
) -> (
    crate::vantage::LaneManager,
    crate::vantage::BlockRef,
    crate::vantage::BlockRef,
    crate::vantage::BlockRef,
) {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (author_t_only, _) = authors()[1];
    let (mut lm, _store) = new_lane_manager(self_name, path);
    let chain_c = direct_chain(&mut lm, author_c, 3).await;
    let chain_only = direct_chain(&mut lm, author_t_only, 1).await;
    let c_ref = block_ref(&chain_c[1]); // Height 2.
    let t_ref_same = block_ref(&chain_c[2]); // Height 3; extends `c_ref`.
    let t_ref_only = block_ref(&chain_only[0]); // Height 1; author absent from C.
    (lm, c_ref, t_ref_same, t_ref_only)
}

fn proposal_for(
    view: u64,
    c: Vec<crate::vantage::BlockRef>,
    t: Vec<crate::vantage::BlockRef>,
) -> ViewProposal {
    ViewProposal {
        view,
        c: sorted_manifest(c),
        t: sorted_manifest(t),
        m: None,
    }
}

#[tokio::test]
async fn positive_gate_fires_on_exact_predicate_satisfaction() {
    let (mut lm, c_ref, t_ref_same, t_ref_only) =
        positive_scenario(".db_test_agb_positive_gate_1").await;
    let mut rep = new_repairer(authors()[3].0, &lm);
    let mut agb = new_agb_engine(authors()[3].0);
    let now = Instant::now();

    agb.enter(1, now, &mut lm, &mut rep);
    let view = 1;
    let sender = proposer_of(view);
    let proposal = proposal_for(view, vec![c_ref], vec![t_ref_same, t_ref_only]);
    let effects = agb.on_propose(sender, proposal.clone(), now, &mut lm, &mut rep);

    assert_eq!(fixed_effect(&effects), Some(true));
    let echo = echo_effect(&effects).expect("positive gate must fire an echo");
    assert_eq!(echo.grade, 1);
    assert_eq!(echo.proposal.view, view);
}

#[tokio::test]
async fn core_entry_not_author_ok_blocks_gate() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_agb_core_not_ok");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);

    let bogus_c = (author_c, 1, Digest([42u8; 32]));
    let view = 1;
    let now = Instant::now();
    agb.enter(view, now, &mut lm, &mut rep);
    let sender = proposer_of(view);
    let proposal = proposal_for(view, vec![bogus_c], Vec::new());
    let effects = agb.on_propose(sender, proposal, now, &mut lm, &mut rep);

    assert_eq!(fixed_effect(&effects), Some(true)); // Well-formed but does not pass the gate.
    assert!(echo_effect(&effects).is_none());
}

#[tokio::test]
async fn tip_acked_but_not_held_blocks_gate() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_agb_acked_not_held");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);

    let c_ref = (author_c, 1, Digest([11u8; 32]));
    let t_ref = (author_c, 2, Digest([12u8; 32]));
    mark_validity_available(&mut lm, c_ref.clone());
    mark_validity_available(&mut lm, t_ref.clone());
    assert!(lm.author_ok(&c_ref));
    assert!(lm.author_ok(&t_ref));
    assert!(!lm.holds_prefix(&t_ref.clone()));

    let view = 1;
    let now = Instant::now();
    agb.enter(view, now, &mut lm, &mut rep);
    let sender = proposer_of(view);
    let proposal = proposal_for(view, vec![c_ref], vec![t_ref]);
    let effects = agb.on_propose(sender, proposal, now, &mut lm, &mut rep);
    assert!(echo_effect(&effects).is_none());
}

#[tokio::test]
async fn author_ok_uses_availability_before_direct_walk() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_agb_author_ok_fast_avail");

    let mut payload = BTreeMap::new();
    payload.insert(Digest([55u8; 32]), 0);
    let header = crate::messages::Header::new_vantage(
        author_c,
        1,
        payload,
        lm.genesis().clone(),
        lm.sid().clone(),
    );
    lm.process_publish(author_c, header.clone()).await;
    let r = block_ref(&header);
    mark_validity_available(&mut lm, r.clone());

    let before = {
        let blocks = lm.blocks_handle();
        let blocks = blocks.lock();
        (blocks.walk_steps(), blocks.walk_failures())
    };
    assert!(lm.author_ok(&r));
    let after = {
        let blocks = lm.blocks_handle();
        let blocks = blocks.lock();
        (blocks.walk_steps(), blocks.walk_failures())
    };
    assert_eq!(
        after, before,
        "f+1 availability already satisfies author_ok; direct-prefix walks are wasted"
    );
}

#[tokio::test]
async fn tip_not_strictly_containing_core_blocks_gate() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_agb_fork_tip");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);

    let sid = lm.sid().clone();
    let genesis = lm.genesis().clone();
    let h1 = crate::messages::Header::new_vantage(
        author_c,
        1,
        std::collections::BTreeMap::new(),
        genesis,
        sid.clone(),
    );
    lm.process_publish(author_c, h1.clone()).await;
    let h2 = crate::messages::Header::new_vantage(
        author_c,
        2,
        std::collections::BTreeMap::new(),
        h1.id.clone(),
        sid.clone(),
    );
    lm.process_publish(author_c, h2.clone()).await;
    let h2_fork = tagged_header(author_c, 2, h1.id.clone(), sid.clone(), 7);
    lm.process_publish(author_c, h2_fork.clone()).await;
    let h3_fork = crate::messages::Header::new_vantage(
        author_c,
        3,
        std::collections::BTreeMap::new(),
        h2_fork.id.clone(),
        sid,
    );
    lm.process_publish(author_c, h3_fork.clone()).await;

    let c_ref = block_ref(&h2);
    let t_ref = block_ref(&h3_fork); // Held at height 3 but not descended from C.
    assert!(lm.author_ok(&c_ref));
    assert!(lm.holds_prefix(&t_ref.clone()));

    let view = 1;
    let now = Instant::now();
    agb.enter(view, now, &mut lm, &mut rep);
    let sender = proposer_of(view);
    let proposal = proposal_for(view, vec![c_ref], vec![t_ref]);
    let effects = agb.on_propose(sender, proposal, now, &mut lm, &mut rep);
    assert!(echo_effect(&effects).is_none());
}

#[tokio::test]
async fn equal_height_tip_excluded() {
    let (mut lm, c_ref, _t_ref_same, _t_only) =
        positive_scenario(".db_test_agb_positive_gate_2").await;
    let mut rep = new_repairer(authors()[3].0, &lm);
    let mut agb = new_agb_engine(authors()[3].0);

    let t_ref_equal_height = c_ref.clone();
    let view = 1;
    let now = Instant::now();
    agb.enter(view, now, &mut lm, &mut rep);
    let sender = proposer_of(view);
    let proposal = proposal_for(view, vec![c_ref], vec![t_ref_equal_height]);
    let effects = agb.on_propose(sender, proposal, now, &mut lm, &mut rep);
    assert!(echo_effect(&effects).is_none());
}

#[tokio::test]
async fn malformed_proposal_sticky_reject_later_versions_ignored() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (author_c2, _) = authors()[1];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_agb_malformed_sticky");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);

    let view = 1;
    let sender = proposer_of(view);
    let malformed = ViewProposal {
        view,
        c: vec![
            (author_c, 1, Digest([1u8; 32])),
            (author_c, 2, Digest([2u8; 32])),
        ],
        t: Vec::new(),
        m: None,
    };
    let effects = agb.on_propose(sender, malformed, Instant::now(), &mut lm, &mut rep);
    assert_eq!(fixed_effect(&effects), Some(false));

    let well_formed = proposal_for(view, vec![(author_c2, 1, Digest([3u8; 32]))], Vec::new());
    let effects2 = agb.on_propose(sender, well_formed, Instant::now(), &mut lm, &mut rep);
    assert!(effects2.is_empty());
}

#[tokio::test]
async fn echo_stage_one_shot_after_positive_gate() {
    let (mut lm, c_ref, t_ref_same, t_ref_only) =
        positive_scenario(".db_test_agb_positive_gate_3").await;
    let mut rep = new_repairer(authors()[3].0, &lm);
    let mut agb = new_agb_engine(authors()[3].0);
    let view = 1;
    let now = Instant::now();
    agb.enter(view, now, &mut lm, &mut rep);
    let sender = proposer_of(view);
    let proposal = proposal_for(view, vec![c_ref], vec![t_ref_same, t_ref_only]);
    let effects = agb.on_propose(sender, proposal, now, &mut lm, &mut rep);
    assert!(echo_effect(&effects).is_some());

    let more = agb.recheck_all(&mut lm, &mut rep);
    assert!(!more
        .iter()
        .any(|e| matches!(e, Effect::BroadcastEcho(_) | Effect::BroadcastEchoSkip(_))));
}

#[tokio::test]
async fn grade0_fallback_fires_at_t1_when_core_ok_but_gate_never_holds() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_agb_grade0_fallback");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);

    let chain = direct_chain(&mut lm, author_c, 1).await;
    let c_ref = block_ref(&chain[0]);

    let boot = Instant::now();
    agb.enter(1, boot, &mut lm, &mut rep);
    let sender = proposer_of(1);
    let bogus_t = (author_c, 99, Digest([77u8; 32]));
    let proposal = proposal_for(1, vec![c_ref], vec![bogus_t]);
    let effects = agb.on_propose(sender, proposal, boot, &mut lm, &mut rep);
    assert!(echo_effect(&effects).is_none()); // The invalid tip blocks the gate.

    let fallback_deadline = boot + Duration::from_millis(TEST_DELTA_MS);
    let _ = fallback_deadline;
    let effects = agb.on_echo_fallback_timer(1, &mut lm, &mut rep);
    let echo = echo_effect(&effects).expect("fallback must send a grade-0 echo (CoreOK holds)");
    assert_eq!(echo.grade, 0);
}

#[tokio::test]
async fn echo_skip_at_t1_when_core_not_ok() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_agb_skip_fallback");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);

    let boot = Instant::now();
    agb.enter(1, boot, &mut lm, &mut rep);
    let sender = proposer_of(1);
    let bogus_c = (author_c, 1, Digest([88u8; 32])); // Unavailable, so `author_ok` is false.
    let proposal = proposal_for(1, vec![bogus_c], Vec::new());
    agb.on_propose(sender, proposal, boot, &mut lm, &mut rep);

    let effects = agb.on_echo_fallback_timer(1, &mut lm, &mut rep);
    assert!(skip_effect(&effects));
    assert!(echo_effect(&effects).is_none());
}

#[tokio::test]
async fn echo_skip_at_absolute_deadline_with_no_fixed_proposal() {
    let (self_name, _) = authors()[3];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_agb_absolute_skip");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);

    agb.enter(1, Instant::now(), &mut lm, &mut rep);
    let effects = agb.on_echo_absolute_timer(1, &mut rep);
    assert!(skip_effect(&effects));
}

#[tokio::test]
async fn proposal_delivered_after_theta_echo_is_ignored() {
    let (mut lm, c_ref, t_ref_same, t_ref_only) =
        positive_scenario(".db_test_agb_positive_gate_4").await;
    let mut rep = new_repairer(authors()[3].0, &lm);
    let mut agb = new_agb_engine(authors()[3].0);

    let boot = Instant::now();
    agb.enter(1, boot, &mut lm, &mut rep);
    let late = boot + agb.theta_echo() + Duration::from_millis(1);
    let sender = proposer_of(1);
    let proposal = proposal_for(1, vec![c_ref], vec![t_ref_same, t_ref_only]);
    let effects = agb.on_propose(sender, proposal, late, &mut lm, &mut rep);
    assert!(effects.is_empty());
}

#[tokio::test]
async fn positive_gate_fires_when_final_enabling_event_is_an_ack() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_agb_wiring_ack_enables_gate");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);

    let c_ref = (author_c, 1, Digest([13u8; 32]));
    let now = Instant::now();
    agb.enter(1, now, &mut lm, &mut rep);
    let sender = proposer_of(1);
    let proposal = proposal_for(1, vec![c_ref.clone()], Vec::new());
    let effects = agb.on_propose(sender, proposal, now, &mut lm, &mut rep);
    assert_eq!(fixed_effect(&effects), Some(true));
    assert!(
        echo_effect(&effects).is_none(),
        "gate must not fire before the entry is author_ok"
    );

    mark_validity_available(&mut lm, c_ref.clone());
    assert!(
        lm.author_ok(&c_ref),
        "test setup: ack stake must actually cross the threshold"
    );
    let effects = agb.recheck_all(&mut lm, &mut rep);
    let echo =
        echo_effect(&effects).expect("the gate must fire once the ack pushes author_ok true");
    assert_eq!(echo.grade, 1);
}

#[tokio::test]
async fn positive_gate_fires_when_final_enabling_event_is_a_payload_ready() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0]; // A remote author requires local payload data.
    let (mut lm, mut store) =
        new_lane_manager(self_name, ".db_test_agb_wiring_payload_enables_gate");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);

    let sid = lm.sid().clone();
    let genesis = lm.genesis().clone();
    let batch_digest = Digest([21u8; 32]);
    let mut payload = std::collections::BTreeMap::new();
    payload.insert(batch_digest.clone(), 0u32);
    let header = crate::messages::Header::new_vantage(author_c, 1, payload, genesis, sid);
    let c_ref = block_ref(&header);

    let now = Instant::now();
    agb.enter(1, now, &mut lm, &mut rep);
    lm.process_publish(author_c, header.clone()).await;
    assert!(!lm.author_ok(&c_ref));

    let sender = proposer_of(1);
    let proposal = proposal_for(1, vec![c_ref.clone()], Vec::new());
    let effects = agb.on_propose(sender, proposal, now, &mut lm, &mut rep);
    assert_eq!(fixed_effect(&effects), Some(true));
    assert!(
        echo_effect(&effects).is_none(),
        "gate must not fire before the payload arrives"
    );

    mark_payload_present(&mut store, &batch_digest, 0u32).await;
    let effects = lm.set_payload_ready(&header.id);
    assert!(
        effects.iter().any(|e| matches!(e, Effect::BroadcastAck(_))),
        "test setup: payload arrival must confirm DirectPub"
    );
    let effects = agb.recheck_all(&mut lm, &mut rep);
    let echo = echo_effect(&effects)
        .expect("the gate must fire once the payload arrival makes the entry author_ok");
    assert_eq!(echo.grade, 1);
}

#[tokio::test]
async fn echo_with_out_of_range_grade_is_dropped_not_counted() {
    let (name, _) = authors()[3];
    let (lm, _store) = new_lane_manager(name, ".db_test_agb_echo_bad_grade");
    let mut rep = new_repairer(name, &lm);
    let mut agb = new_agb_engine(name);
    let (a0, _) = authors()[0];
    let proposal = proposal_for(1, vec![(a0, 1, Digest([5u8; 32]))], Vec::new());
    let make_echo = |grade: u8, sender: crypto::PublicKey| Echo {
        proposal: proposal.clone(),
        grade,
        sender,
        wish: 0,
        origin: None,
        avail: None,
    };
    let (bad_sender, _) = authors()[1];

    let effects = agb.on_echo(make_echo(2, bad_sender), &mut rep);
    assert!(
        effects.is_empty(),
        "a malformed grade must produce no effects"
    );

    let effects = agb.on_echo(make_echo(1, bad_sender), &mut rep);
    assert!(
        effects
            .iter()
            .all(|e| !matches!(e, Effect::BroadcastReady(_))),
        "quorum not yet reached (1 of 3)"
    );

    let (sender2, _) = authors()[2];
    let effects = agb.on_echo(make_echo(1, sender2), &mut rep);
    assert!(
        effects
            .iter()
            .all(|e| !matches!(e, Effect::BroadcastReady(_))),
        "quorum not yet reached (2 of 3)"
    );

    let (sender3, _) = authors()[3];
    let effects = agb.on_echo(make_echo(1, sender3), &mut rep);
    assert!(
        effects.iter().any(|e| matches!(e, Effect::BroadcastReady(_))),
        "the sender whose malformed echo was dropped must still be able to send a legal one that counts toward quorum"
    );
}

#[tokio::test]
async fn w5_entry_arms_echo_and_ready_absolute_timers() {
    let (self_name, _) = authors()[3];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_w5_entry_arms_absolute");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);
    let now = Instant::now();
    let (theta_echo, theta_ready) = (agb.theta_echo(), agb.theta_ready());

    let effects = agb.enter(1, now, &mut lm, &mut rep);
    assert!(effects.iter().any(
        |e| matches!(e, Effect::ArmTimer(1, TimerKind::EchoAbsolute, d) if *d == now + theta_echo)
    ));
    assert!(effects.iter().any(
        |e| matches!(e, Effect::ArmTimer(1, TimerKind::ReadyAbsolute, d) if *d == now + theta_ready)
    ));
}

#[tokio::test]
async fn w5_entry_never_re_enters() {
    let (self_name, _) = authors()[3];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_w5_no_reenter");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);
    let now = Instant::now();

    let first = agb.enter(1, now, &mut lm, &mut rep);
    assert!(!first.is_empty());
    let second = agb.enter(1, now + Duration::from_millis(5), &mut lm, &mut rep);
    assert!(
        second.is_empty(),
        "a view already entered must never re-enter"
    );
}

#[tokio::test]
async fn entry_arms_echo_fallback_for_a_previously_fixed_pending_proposal() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_w5b_carry_over");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);

    let propose_instant = Instant::now();
    let sender = proposer_of(1);
    let bogus_t = (author_c, 99, Digest([77u8; 32]));
    let proposal = proposal_for(1, Vec::new(), vec![bogus_t]);
    let effects = agb.on_propose(sender, proposal, propose_instant, &mut lm, &mut rep);
    assert_eq!(fixed_effect(&effects), Some(true));
    assert!(
        !effects
            .iter()
            .any(|e| matches!(e, Effect::ArmTimer(_, TimerKind::EchoFallback, _))),
        "on_propose must not arm EchoFallback before entry -- entry_instant is still None"
    );
    assert!(
        echo_effect(&effects).is_none(),
        "positive gate must stay blocked (bogus T)"
    );

    let entry_instant = propose_instant + Duration::from_millis(10);
    let effects = agb.enter(1, entry_instant, &mut lm, &mut rep);
    let expected_deadline = std::cmp::min(
        std::cmp::max(entry_instant, propose_instant) + Duration::from_millis(TEST_DELTA_MS),
        entry_instant + agb.theta_echo(),
    );
    let arm = effects.iter().find_map(|e| match e {
        Effect::ArmTimer(1, TimerKind::EchoFallback, d) => Some(*d),
        _ => None,
    });
    assert_eq!(
        arm,
        Some(expected_deadline),
        "enter() must arm EchoFallback for the already-fixed pending proposal"
    );

    let effects = agb.on_echo_fallback_timer(1, &mut lm, &mut rep);
    let echo = echo_effect(&effects).expect("the carried-over fallback must actually fire");
    assert_eq!(echo.grade, 0);
}

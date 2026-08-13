use super::common::*;
use crate::vantage::agb::{Echo, Ready, ReadyGrade, ReadyOut, ViewProposal};
use crate::vantage::Effect;
use crypto::Digest;

fn sample_proposal(view: u64) -> ViewProposal {
    let (a0, _) = authors()[0];
    ViewProposal {
        view,
        c: vec![(a0, 1, Digest([1u8; 32]))],
        t: Vec::new(),
        m: None,
    }
}

fn ready_effect(effects: &[Effect]) -> Option<&Ready> {
    effects.iter().find_map(|e| match e {
        Effect::BroadcastReady(ReadyOut::Single(r)) => Some(r),
        _ => None,
    })
}

fn echo(proposal: ViewProposal, grade: u8, sender: crypto::PublicKey) -> Echo {
    Echo {
        proposal,
        grade,
        sender,
        wish: 0,
        origin: None,
        avail: None,
    }
}

fn dummy_repairer(name: crypto::PublicKey, path: &str) -> crate::vantage::Repairer {
    let (lm, _store) = new_lane_manager(name, path);
    new_repairer(name, &lm)
}

#[tokio::test]
async fn ready_fires_at_quorum_with_grade_one() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_ready_quorum_grade1");
    let proposal = sample_proposal(1);

    for (i, (sender, _)) in authors().into_iter().enumerate().take(2) {
        let effects = agb.on_echo(echo(proposal.clone(), 1, sender), &mut rep);
        assert!(
            ready_effect(&effects).is_none(),
            "quorum not yet reached at echo {}",
            i
        );
    }
    let (third, _) = authors()[2];
    let effects = agb.on_echo(echo(proposal.clone(), 1, third), &mut rep);
    let ready = ready_effect(&effects).expect("Q=2f+1 grade-1 echoes must trigger a ready");
    assert_eq!(ready.grade, ReadyGrade::One);
}

#[tokio::test]
async fn ready_grade_zero_when_quorum_all_grade0() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_ready_quorum_grade0");
    let proposal = sample_proposal(1);
    let mut last = Vec::new();
    for (sender, _) in authors().into_iter().take(3) {
        last = agb.on_echo(echo(proposal.clone(), 0, sender), &mut rep);
    }
    assert_eq!(ready_effect(&last).unwrap().grade, ReadyGrade::Zero);
}

#[tokio::test]
async fn ready_grade_mix_fires_at_the_first_split_quorum() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_ready_grade_mix");
    let proposal = sample_proposal(1);
    let all = authors();
    agb.on_echo(echo(proposal.clone(), 1, all[0].0), &mut rep);
    agb.on_echo(echo(proposal.clone(), 0, all[1].0), &mut rep);
    let effects = agb.on_echo(echo(proposal.clone(), 1, all[2].0), &mut rep);
    assert_eq!(ready_effect(&effects).unwrap().grade, ReadyGrade::Mix);
    assert!(agb.ready_mix_open_for_test(1));

    let effects = agb.on_echo(echo(proposal, 0, all[3].0), &mut rep);
    assert!(ready_effect(&effects).is_none());
    assert!(
        !agb.ready_mix_open_for_test(1),
        "an all-party residual split is final and cannot refine later"
    );
}

#[tokio::test]
async fn initial_ready_mix_quarantines_its_tips_before_broadcast() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_ready_mix_tip_quarantine");
    let mut proposal = sample_proposal(1);
    let tip = (authors()[1].0, 2, Digest([2u8; 32]));
    proposal.t.push(tip.clone());
    let all = authors();
    agb.on_echo(echo(proposal.clone(), 1, all[0].0), &mut rep);
    agb.on_echo(echo(proposal.clone(), 0, all[1].0), &mut rep);
    let effects = agb.on_echo(echo(proposal, 1, all[2].0), &mut rep);

    let quarantine_index = effects
        .iter()
        .position(
            |effect| matches!(effect, Effect::QuarantineTips(tips) if tips == &vec![tip.clone()]),
        )
        .expect("an initial READY-mix must quarantine its non-core tips");
    let ready_index = effects
        .iter()
        .position(|effect| matches!(effect, Effect::BroadcastReady(_)))
        .expect("the initial READY-mix must still broadcast immediately");
    assert!(quarantine_index < ready_index);
}

#[tokio::test]
async fn provisional_mix_refines_when_grade_zero_later_reaches_quorum() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_ready_homogeneous_after_split");
    let proposal = sample_proposal(1);
    let all = authors();
    agb.on_echo(echo(proposal.clone(), 1, all[0].0), &mut rep);
    agb.on_echo(echo(proposal.clone(), 0, all[1].0), &mut rep);
    let effects = agb.on_echo(echo(proposal.clone(), 0, all[2].0), &mut rep);
    assert_eq!(ready_effect(&effects).unwrap().grade, ReadyGrade::Mix);
    assert!(agb.ready_mix_open_for_test(1));

    let effects = agb.on_echo(echo(proposal, 0, all[3].0), &mut rep);
    assert_eq!(ready_effect(&effects).unwrap().grade, ReadyGrade::Zero);
    assert!(!agb.ready_mix_open_for_test(1));
}

#[tokio::test]
async fn provisional_mix_refines_when_grade_one_later_reaches_quorum() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_ready_grade_one_after_split");
    let proposal = sample_proposal(1);
    let all = authors();
    agb.on_echo(echo(proposal.clone(), 0, all[0].0), &mut rep);
    agb.on_echo(echo(proposal.clone(), 1, all[1].0), &mut rep);
    let effects = agb.on_echo(echo(proposal.clone(), 1, all[2].0), &mut rep);
    assert_eq!(ready_effect(&effects).unwrap().grade, ReadyGrade::Mix);
    assert!(agb.ready_mix_open_for_test(1));

    let effects = agb.on_echo(echo(proposal, 1, all[3].0), &mut rep);
    assert_eq!(ready_effect(&effects).unwrap().grade, ReadyGrade::One);
    assert!(!agb.ready_mix_open_for_test(1));
}

#[tokio::test]
async fn n10_three_direct_holders_refine_mix_to_core_grade() {
    let (committee, keys) = config::Committee::local_benchmark(10, 1, 9720);
    let name = keys[9].name;
    let mut agb = new_agb_engine_with_committee(name, committee.clone());
    let (lm, _store) = new_lane_manager_with_committee(
        name,
        ".db_test_ready_n10_three_direct_holders",
        committee.clone(),
    );
    let mut rep = new_repairer_with_committee(name, &lm, committee);
    let proposal = ViewProposal {
        view: 1,
        c: vec![(keys[0].name, 1, Digest([9u8; 32]))],
        t: Vec::new(),
        m: None,
    };

    let mut effects = Vec::new();
    for key in keys.iter().take(3) {
        effects = agb.on_echo(echo(proposal.clone(), 1, key.name), &mut rep);
    }
    assert!(ready_effect(&effects).is_none());
    for key in keys.iter().skip(3).take(4) {
        effects = agb.on_echo(echo(proposal.clone(), 0, key.name), &mut rep);
    }
    assert_eq!(ready_effect(&effects).unwrap().grade, ReadyGrade::Mix);

    for key in keys.iter().skip(7) {
        effects = agb.on_echo(echo(proposal.clone(), 0, key.name), &mut rep);
    }
    assert_eq!(ready_effect(&effects).unwrap().grade, ReadyGrade::Zero);
    assert!(agb.ready_finalized(1));
}

#[tokio::test]
async fn ready_deadline_finalizes_a_residual_mix_without_a_second_broadcast() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_ready_deadline_mix");
    let proposal = sample_proposal(1);
    let all = authors();
    agb.on_echo(echo(proposal.clone(), 1, all[0].0), &mut rep);
    agb.on_echo(echo(proposal.clone(), 0, all[1].0), &mut rep);
    let effects = agb.on_echo(echo(proposal, 1, all[2].0), &mut rep);
    assert_eq!(ready_effect(&effects).unwrap().grade, ReadyGrade::Mix);
    assert!(agb.ready_mix_open_for_test(1));

    let effects = agb.on_ready_timer(1, &mut rep);
    assert!(ready_effect(&effects).is_none());
    assert!(!effects
        .iter()
        .any(|effect| matches!(effect, Effect::BroadcastNoReady(_))));
    assert!(!agb.ready_mix_open_for_test(1));
}

#[tokio::test]
async fn q_boundary_exact() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_ready_q_boundary");
    let proposal = sample_proposal(1);
    let all = authors();
    let e1 = agb.on_echo(echo(proposal.clone(), 1, all[0].0), &mut rep);
    assert!(ready_effect(&e1).is_none());
    let e2 = agb.on_echo(echo(proposal.clone(), 1, all[1].0), &mut rep);
    assert!(
        ready_effect(&e2).is_none(),
        "2 < Q=3 must not trigger ready"
    );
    let e3 = agb.on_echo(echo(proposal, 1, all[2].0), &mut rep);
    assert!(ready_effect(&e3).is_some(), "3 == Q must trigger ready");
}

#[tokio::test]
async fn ready_without_own_echo_or_fixed_proposal() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_ready_no_own_echo");
    let proposal = sample_proposal(1);
    let mut last = Vec::new();
    for (sender, _) in authors().into_iter().filter(|(pk, _)| *pk != name).take(3) {
        last = agb.on_echo(echo(proposal.clone(), 1, sender), &mut rep);
    }
    assert!(ready_effect(&last).is_some());
}

#[tokio::test]
async fn homogeneous_ready_is_one_shot_per_view() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_ready_one_shot");
    let proposal = sample_proposal(1);
    let all = authors();
    for (sender, _) in all.iter().take(3) {
        agb.on_echo(echo(proposal.clone(), 1, *sender), &mut rep);
    }
    let effects = agb.on_echo(echo(proposal, 0, all[3].0), &mut rep);
    assert!(ready_effect(&effects).is_none());
}

#[tokio::test]
async fn noready_fires_when_ready_pending_at_theta_r_and_never_after() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_ready_noready_then_never");
    let effects = agb.on_ready_timer(1, &mut rep);
    assert!(effects
        .iter()
        .any(|e| matches!(e, Effect::BroadcastNoReady(1))));

    let proposal = sample_proposal(1);
    let mut any_ready = false;
    for (sender, _) in authors() {
        let effects = agb.on_echo(echo(proposal.clone(), 1, sender), &mut rep);
        any_ready |= ready_effect(&effects).is_some();
    }
    assert!(!any_ready);
}

#[tokio::test]
async fn noready_is_noop_once_ready_already_sent() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_ready_noready_noop");
    let proposal = sample_proposal(1);
    for (sender, _) in authors().into_iter().take(3) {
        agb.on_echo(echo(proposal.clone(), 1, sender), &mut rep);
    }
    let effects = agb.on_ready_timer(1, &mut rep);
    assert!(effects.is_empty());
}

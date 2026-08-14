use super::common::*;
use crate::vantage::agb::{Outcome, Ready, ReadyGrade, ViewProposal};
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

fn ready(proposal: ViewProposal, grade: ReadyGrade, sender: crypto::PublicKey) -> Ready {
    Ready {
        proposal,
        grade,
        sender,
        wish: 0,
    }
}

fn dummy_repairer(name: crypto::PublicKey, path: &str) -> crate::vantage::Repairer {
    let (lm, _store) = new_lane_manager(name, path);
    new_repairer(name, &lm)
}

fn sealed_effects(effects: &[Effect]) -> Vec<Outcome> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Sealed(_, o) => Some(o.clone()),
            _ => None,
        })
        .collect()
}

fn completed_effects(effects: &[Effect]) -> usize {
    effects
        .iter()
        .filter(|e| matches!(e, Effect::Completed(_, _, _)))
        .count()
}

#[tokio::test]
async fn completion_fires_once_on_mixed_grade_quorum() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_completion_mixed_grade");
    let mut proposal = sample_proposal(1);
    let tip = (authors()[1].0, 2, Digest([2u8; 32]));
    proposal.t.push(tip.clone());
    let all = authors();

    let e1 = agb.on_ready(ready(proposal.clone(), ReadyGrade::One, all[0].0), &mut rep);
    assert_eq!(completed_effects(&e1), 0);
    let e2 = agb.on_ready(ready(proposal.clone(), ReadyGrade::One, all[1].0), &mut rep);
    assert_eq!(completed_effects(&e2), 0);
    let e3 = agb.on_ready(
        ready(proposal.clone(), ReadyGrade::Zero, all[2].0),
        &mut rep,
    );
    assert_eq!(completed_effects(&e3), 1);
    assert!(agb.completed_for_test(1).is_some());
    assert!(agb.directed_for_test(1).is_none());
    assert!(sealed_effects(&e3).is_empty());
    assert!(rep.requested_count() > 0);
    let quarantine_index = e3
        .iter()
        .position(
            |effect| matches!(effect, Effect::QuarantineTips(tips) if tips == &vec![tip.clone()]),
        )
        .expect("completed-open transition must quarantine its non-quorum tips");
    let completed_index = e3
        .iter()
        .position(|effect| matches!(effect, Effect::Completed(..)))
        .expect("the transition completes");
    assert!(quarantine_index < completed_index);

    let e4 = agb.on_ready(ready(proposal, ReadyGrade::Zero, all[3].0), &mut rep);
    assert_eq!(completed_effects(&e4), 0);
}

#[tokio::test]
async fn direct_seal_full_on_homogeneous_grade1_quorum() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_completion_direct_full");
    let proposal = sample_proposal(1);
    let all = authors();
    let mut last = Vec::new();
    for (sender, _) in all.iter().take(3) {
        last = agb.on_ready(ready(proposal.clone(), ReadyGrade::One, *sender), &mut rep);
    }
    let sealed = sealed_effects(&last);
    assert_eq!(sealed.len(), 1);
    assert!(matches!(&sealed[0], Outcome::Full(c, t) if *c == proposal.c && *t == proposal.t));
    assert!(last
        .iter()
        .all(|effect| !matches!(effect, Effect::QuarantineTips(_))));
}

#[tokio::test]
async fn direct_seal_core_on_homogeneous_grade0_quorum() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_completion_direct_core");
    let proposal = sample_proposal(1);
    let all = authors();
    let mut last = Vec::new();
    for (sender, _) in all.iter().take(3) {
        last = agb.on_ready(ready(proposal.clone(), ReadyGrade::Zero, *sender), &mut rep);
    }
    let sealed = sealed_effects(&last);
    assert_eq!(sealed.len(), 1);
    assert!(matches!(&sealed[0], Outcome::Core(c) if *c == proposal.c));
}

#[tokio::test]
async fn late_homogeneous_quorum_after_completion_still_seals() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_completion_late_homogeneous");
    let proposal = sample_proposal(1);
    let all = authors();

    agb.on_ready(ready(proposal.clone(), ReadyGrade::One, all[0].0), &mut rep);
    agb.on_ready(ready(proposal.clone(), ReadyGrade::One, all[1].0), &mut rep);
    let e3 = agb.on_ready(
        ready(proposal.clone(), ReadyGrade::Zero, all[2].0),
        &mut rep,
    );
    assert_eq!(completed_effects(&e3), 1);
    assert!(agb.directed_for_test(1).is_none());

    let e4 = agb.on_ready(ready(proposal.clone(), ReadyGrade::One, all[3].0), &mut rep);
    let sealed = sealed_effects(&e4);
    assert_eq!(sealed.len(), 1);
    assert!(matches!(&sealed[0], Outcome::Full(c, t) if *c == proposal.c && *t == proposal.t));
}

#[tokio::test]
async fn same_sender_mix_refinements_seal_without_double_counting_completion() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_completion_mix_refinement");
    let proposal = sample_proposal(1);
    let all = authors();

    let mut completion_count = 0;
    for (sender, _) in all.iter().take(3) {
        let effects = agb.on_ready(ready(proposal.clone(), ReadyGrade::Mix, *sender), &mut rep);
        completion_count += completed_effects(&effects);
    }
    assert_eq!(completion_count, 1);
    assert!(agb.directed_for_test(1).is_none());
    assert_eq!(agb.ready_stage_total(1), 3);

    let mut sealed = Vec::new();
    for (sender, _) in all.iter().take(3) {
        let effects = agb.on_ready(ready(proposal.clone(), ReadyGrade::Zero, *sender), &mut rep);
        assert_eq!(completed_effects(&effects), 0);
        sealed.extend(sealed_effects(&effects));
    }
    assert_eq!(agb.ready_stage_total(1), 3);
    assert_eq!(sealed.len(), 1);
    assert!(matches!(&sealed[0], Outcome::Core(c) if *c == proposal.c));
}

#[tokio::test]
async fn homogeneous_ready_counted_first_ignores_later_mix() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_completion_homogeneous_before_mix");
    let proposal = sample_proposal(1);
    let all = authors();

    agb.on_ready(ready(proposal.clone(), ReadyGrade::One, all[0].0), &mut rep);
    let ignored = agb.on_ready(ready(proposal.clone(), ReadyGrade::Mix, all[0].0), &mut rep);
    assert!(ignored.is_empty());
    assert_eq!(agb.ready_stage_total(1), 1);
    assert_eq!(
        agb.ready_stage_non_grade1_count(1),
        0,
        "an uncounted late MIX must not become historical resolver evidence"
    );

    agb.on_ready(ready(proposal.clone(), ReadyGrade::One, all[1].0), &mut rep);
    let effects = agb.on_ready(ready(proposal.clone(), ReadyGrade::One, all[2].0), &mut rep);
    assert!(matches!(
        sealed_effects(&effects).as_slice(),
        [Outcome::Full(c, t)] if *c == proposal.c && *t == proposal.t
    ));
}

#[tokio::test]
async fn same_sender_cannot_switch_grade_after_mix_refinement() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_completion_single_mix_refinement");
    let proposal = sample_proposal(1);
    let all = authors();

    agb.on_ready(ready(proposal.clone(), ReadyGrade::Mix, all[0].0), &mut rep);
    agb.on_ready(
        ready(proposal.clone(), ReadyGrade::Zero, all[0].0),
        &mut rep,
    );
    let ignored = agb.on_ready(ready(proposal.clone(), ReadyGrade::One, all[0].0), &mut rep);
    assert!(ignored.is_empty());
    assert_eq!(agb.ready_stage_total(1), 1);
    assert_eq!(
        agb.ready_stage_non_grade1_count(1),
        1,
        "a counted MIX remains historical non-grade-1 evidence"
    );

    agb.on_ready(ready(proposal.clone(), ReadyGrade::One, all[1].0), &mut rep);
    let effects = agb.on_ready(ready(proposal, ReadyGrade::One, all[2].0), &mut rep);
    assert_eq!(completed_effects(&effects), 1);
    assert!(
        sealed_effects(&effects).is_empty(),
        "the rejected second refinement must not create a grade-1 quorum"
    );
}

#[tokio::test]
async fn arbiter_first_submission_wins_later_compatible_submission_ignored() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_completion_arbiter");
    let proposal = sample_proposal(1);
    let all = authors();

    let mut last = Vec::new();
    for (sender, _) in all.iter().take(3) {
        last = agb.on_ready(ready(proposal.clone(), ReadyGrade::One, *sender), &mut rep);
    }
    assert_eq!(sealed_effects(&last).len(), 1);

    let e4 = agb.on_ready(ready(proposal, ReadyGrade::One, all[3].0), &mut rep);
    assert!(sealed_effects(&e4).is_empty());
}

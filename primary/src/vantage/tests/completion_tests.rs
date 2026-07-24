// PHASE4-SPEC.md §12 "R4" -- completion, direct-seal, and the try-seal arbiter (§7),
// driven directly against `AgbEngine::on_ready`.

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

/// Constructing a `LaneManager` opens a real (on-disk) `Store`, which spawns a tokio
/// task internally -- so every caller needs a live runtime (`#[tokio::test]`), and
/// every call site needs its own path (parallel test threads open RocksDB
/// concurrently; a shared path deadlocks on RocksDB's file lock).
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
    let proposal = sample_proposal(1);
    let all = authors();

    let e1 = agb.on_ready(ready(proposal.clone(), ReadyGrade::One, all[0].0), &mut rep);
    assert_eq!(completed_effects(&e1), 0);
    let e2 = agb.on_ready(ready(proposal.clone(), ReadyGrade::One, all[1].0), &mut rep);
    assert_eq!(completed_effects(&e2), 0);
    // Third (mixed grade, e.g. Zero) reaches Q=3 total, but neither grade alone does --
    // completion (any grade) fires; direct does not.
    let e3 = agb.on_ready(
        ready(proposal.clone(), ReadyGrade::Zero, all[2].0),
        &mut rep,
    );
    assert_eq!(completed_effects(&e3), 1);
    assert!(agb.completed_for_test(1).is_some());
    assert!(agb.directed_for_test(1).is_none());
    assert!(sealed_effects(&e3).is_empty());
    // repair authorized C's lane prefix -- since it was never actually cached, this
    // manifests as at least one outstanding `request`.
    assert!(rep.requested_count() > 0);

    // Completion is idempotent -- a further ready naming the same B never re-fires it.
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

    // 2 grade-1 + 1 grade-0 = mixed-grade completion, no direct result yet.
    agb.on_ready(ready(proposal.clone(), ReadyGrade::One, all[0].0), &mut rep);
    agb.on_ready(ready(proposal.clone(), ReadyGrade::One, all[1].0), &mut rep);
    let e3 = agb.on_ready(
        ready(proposal.clone(), ReadyGrade::Zero, all[2].0),
        &mut rep,
    );
    assert_eq!(completed_effects(&e3), 1);
    assert!(agb.directed_for_test(1).is_none());

    // A late 4th ready, grade-1, pushes the grade-1 tally from 2 to 3 (Q) -- a late
    // homogeneous quorum still produces the direct result, even after completion.
    let e4 = agb.on_ready(ready(proposal.clone(), ReadyGrade::One, all[3].0), &mut rep);
    let sealed = sealed_effects(&e4);
    assert_eq!(sealed.len(), 1);
    assert!(matches!(&sealed[0], Outcome::Full(c, t) if *c == proposal.c && *t == proposal.t));
}

#[tokio::test]
async fn arbiter_first_submission_wins_later_compatible_submission_ignored() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_completion_arbiter");
    let proposal = sample_proposal(1);
    let all = authors();

    // Homogeneous grade-1 quorum -> arbiter's first (and only) real submission: Full.
    let mut last = Vec::new();
    for (sender, _) in all.iter().take(3) {
        last = agb.on_ready(ready(proposal.clone(), ReadyGrade::One, *sender), &mut rep);
    }
    assert_eq!(sealed_effects(&last).len(), 1);

    // A 4th ready (also grade-1, same B) recomputes the same tally -- `directed` is
    // already `Some`, so `recheck_completion_and_direct` never calls `try_seal` again;
    // no second `Sealed` effect is ever produced for this view.
    let e4 = agb.on_ready(ready(proposal, ReadyGrade::One, all[3].0), &mut rep);
    assert!(sealed_effects(&e4).is_empty());
}

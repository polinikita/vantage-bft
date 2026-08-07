// PHASE4-SPEC.md §12 "R3" -- the ready stage (§6), driven directly against
// `AgbEngine::on_echo`/`on_echo_skip`/`on_ready_timer`.

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

/// PHASE7: this file only ever drives the `Single` path (never
/// `on_propose_batch`/`on_ready_batch`) -- see `agb_echo_tests.rs::echo_effect`'s
/// identical doc comment.
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

/// Constructing a `LaneManager` opens a real (on-disk) `Store`, which spawns a tokio
/// task internally -- so every caller needs a live runtime (`#[tokio::test]`), and
/// every call site needs its own path (parallel test threads open RocksDB
/// concurrently; a shared path deadlocks on RocksDB's file lock).
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
async fn ready_grade_mix_when_split() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_ready_grade_mix");
    let proposal = sample_proposal(1);
    let all = authors();
    agb.on_echo(echo(proposal.clone(), 1, all[0].0), &mut rep);
    agb.on_echo(echo(proposal.clone(), 0, all[1].0), &mut rep);
    let effects = agb.on_echo(echo(proposal.clone(), 1, all[2].0), &mut rep);
    // 2 grade-1 + 1 grade-0 = quorum (3), neither grade alone reaches quorum -> Mix.
    assert_eq!(ready_effect(&effects).unwrap().grade, ReadyGrade::Mix);
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
    // "No entry, fixed-proposal, or own-echo guard -- a party can go ready purely on
    // others' echoes." This engine never had `enter`/`on_propose` called at all.
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
async fn ready_one_shot_per_view() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_ready_one_shot");
    let proposal = sample_proposal(1);
    let all = authors();
    for (sender, _) in all.iter().take(3) {
        agb.on_echo(echo(proposal.clone(), 1, *sender), &mut rep);
    }
    // A 4th (grade-0) echo must not produce a second ready-stage statement.
    let effects = agb.on_echo(echo(proposal, 0, all[3].0), &mut rep);
    assert!(ready_effect(&effects).is_none());
}

#[tokio::test]
async fn noready_fires_when_ready_pending_at_theta_r_and_never_after() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let effects = agb.on_ready_timer(1);
    assert!(effects
        .iter()
        .any(|e| matches!(e, Effect::BroadcastNoReady(1))));

    let mut rep = dummy_repairer(name, ".db_test_ready_noready_then_never");
    // Once already "ready" (one-shot marker consumed by the no-ready above), further
    // echoes must not retroactively produce a real ready.
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
    let effects = agb.on_ready_timer(1);
    assert!(effects.is_empty());
}

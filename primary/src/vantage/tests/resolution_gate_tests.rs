// PHASE6-SPEC.md §2/§3 -- `MetaOK`, the fast-seal lock rule as consulted by `MetaOK`,
// the origin bit (`Ann`), `ReadyOK`, and D6-5's noready census, driven directly against
// `AgbEngine` (no network/node wiring, per the established test-style note). n=4, f=1
// (f+1=2, 2f+1=3), equal stake -- `test_committee()`.

use super::common::*;
use crate::vantage::agb::{
    Echo, EchoOut, Ready, ReadyGrade, ReadyOut, ResolutionEntry, ViewProposal,
};
use crate::vantage::Effect;

/// PHASE7: this file only ever drives the `Single` path (never
/// `on_propose_batch`/`on_echo_batch`) -- see `agb_echo_tests.rs::echo_effect`'s
/// identical doc comment.
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

fn ready_effect(effects: &[Effect]) -> Option<&Ready> {
    effects.iter().find_map(|e| match e {
        Effect::BroadcastReady(ReadyOut::Single(r)) => Some(r),
        _ => None,
    })
}

/// Drives view `u`'s own positive gate (our own grade-1 echo) using a real, directly
/// published+acked one-height chain for `author_c` as `C = [c_ref]`, `T = []`. Returns
/// `(c_ref, proposal_u)` -- `proposal_u` is exactly the payload our own lock/echo/ready
/// end up recorded against.
async fn drive_own_positive_echo(
    agb: &mut crate::vantage::AgbEngine,
    lm: &mut crate::vantage::LaneManager,
    rep: &mut crate::vantage::Repairer,
    u: crate::primary::View,
    author_c: crypto::PublicKey,
    now: std::time::Instant,
) -> (crate::vantage::BlockRef, ViewProposal) {
    let chain = direct_chain(lm, author_c, 1).await;
    let c_ref = block_ref(&chain[0]);
    agb.enter(u, now, lm, rep);
    let sender = proposer_of(u);
    let proposal = ViewProposal {
        view: u,
        c: vec![c_ref.clone()],
        t: Vec::new(),
        m: None,
    };
    let effects = agb.on_propose(sender, proposal.clone(), now, lm, rep);
    assert!(
        echo_effect(&effects).is_some(),
        "own positive gate must fire for the setup view"
    );
    (c_ref, proposal)
}

/// A carrying proposal for view `w` with resolution entry `m`, over an independent
/// one-height chain for `author_w` (so its own CoreOK/TipOK never depend on `u`'s data).
async fn carrying_proposal(
    lm: &mut crate::vantage::LaneManager,
    author_w: crypto::PublicKey,
    w: crate::primary::View,
    m: Option<ResolutionEntry>,
) -> ViewProposal {
    let chain = direct_chain(lm, author_w, 1).await;
    let c_w = block_ref(&chain[0]);
    ViewProposal {
        view: w,
        c: vec![c_w],
        t: Vec::new(),
        m,
    }
}

#[tokio::test]
async fn meta_ok_full_blocks_until_own_target_ready_emitted_then_unblocks() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (author_w, _) = authors()[1];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_metaok_full_pending");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);
    let now = std::time::Instant::now();

    // u=1: fire our own positive echo, but do NOT yet complete its ready stage.
    let (c_ref, _proposal_u) =
        drive_own_positive_echo(&mut agb, &mut lm, &mut rep, 1, author_c, now).await;

    // w=4 (u=1 <= w-3=1, boundary-exact): carrying proposal targets Full(1, [c_ref], []).
    agb.enter(4, now, &mut lm, &mut rep);
    let m = Some(ResolutionEntry::Full(1, vec![c_ref.clone()], Vec::new()));
    let proposal_w = carrying_proposal(&mut lm, author_w, 4, m).await;
    let sender_w = proposer_of(4);
    let effects = agb.on_propose(sender_w, proposal_w, now, &mut lm, &mut rep);
    assert!(
        echo_effect(&effects).is_none(),
        "MetaOK must block: own R_i(1) not yet emitted"
    );
    assert!(!skip_effect(&effects), "on_propose's positive-gate path simply doesn't fire when blocked -- no echo-skip either (that only comes from the fallback/absolute timers)");
}

#[tokio::test]
async fn meta_ok_full_passes_once_own_ready_is_grade_one_same_payload() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (author_w, _) = authors()[1];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_metaok_full_pass");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);
    let now = std::time::Instant::now();

    let (c_ref, proposal_u) =
        drive_own_positive_echo(&mut agb, &mut lm, &mut rep, 1, author_c, now).await;
    // Complete u=1's ready stage at homogeneous grade-1 (2 more matching echoes).
    let others: Vec<_> = authors()
        .into_iter()
        .filter(|(pk, _)| *pk != self_name)
        .take(2)
        .collect();
    for (sender, _) in &others {
        agb.on_echo(
            Echo {
                proposal: proposal_u.clone(),
                grade: 1,
                sender: *sender,
                wish: 0,
                origin: None,
            },
            &mut rep,
        );
    }

    agb.enter(4, now, &mut lm, &mut rep);
    let m = Some(ResolutionEntry::Full(1, vec![c_ref], Vec::new()));
    let proposal_w = carrying_proposal(&mut lm, author_w, 4, m).await;
    let sender_w = proposer_of(4);
    let effects = agb.on_propose(sender_w, proposal_w, now, &mut lm, &mut rep);
    let echo = echo_effect(&effects)
        .expect("MetaOK must now pass: own R_i(1) is grade-1 for exactly the entry's payload");
    assert_eq!(echo.grade, 1);
}

/// Once view `u` is pruned, MetaOK must DECLINE a carrier targeting it rather than wave it
/// through. The pruned branch used to `return true`, and `compute_origin` used to stamp
/// `origin = Some(1)` alongside -- a first-hand attestation peers count toward ReadyOK's
/// `origin_ones >= f+1`. Since `formed` puts no lower bound on the target view, a Byzantine
/// proposer of a live carrier could name any pruned `u` with a fabricated payload and
/// harvest those endorsements from every party past its own GC floor.
///
/// This is the same setup as `meta_ok_full_passes_once_own_ready_is_grade_one_same_payload`
/// -- where the entry is genuinely justified and DOES echo -- with only a `gc_below` added,
/// so it isolates pruning as the cause.
#[tokio::test]
async fn meta_ok_rejects_carrier_targeting_a_pruned_view() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (author_w, _) = authors()[1];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_metaok_pruned_target");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);
    let now = std::time::Instant::now();

    let (c_ref, proposal_u) =
        drive_own_positive_echo(&mut agb, &mut lm, &mut rep, 1, author_c, now).await;
    let others: Vec<_> = authors()
        .into_iter()
        .filter(|(pk, _)| *pk != self_name)
        .take(2)
        .collect();
    for (sender, _) in &others {
        agb.on_echo(
            Echo {
                proposal: proposal_u.clone(),
                grade: 1,
                sender: *sender,
                wish: 0,
                origin: None,
            },
            &mut rep,
        );
    }

    // The only difference from the passing test: u=1's evidence is now gone.
    agb.gc_below(2);

    agb.enter(4, now, &mut lm, &mut rep);
    let m = Some(ResolutionEntry::Full(1, vec![c_ref], Vec::new()));
    let proposal_w = carrying_proposal(&mut lm, author_w, 4, m).await;
    let sender_w = proposer_of(4);
    let effects = agb.on_propose(sender_w, proposal_w, now, &mut lm, &mut rep);

    assert!(
        echo_effect(&effects).is_none(),
        "MetaOK must decline a target whose evidence we pruned -- endorsing it would \
         assert a first-hand echo we can no longer substantiate"
    );
}

#[tokio::test]
async fn meta_ok_full_rejects_when_own_ready_is_grade_zero() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (author_w, _) = authors()[1];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_metaok_full_reject_grade0");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);
    let now = std::time::Instant::now();

    let chain = direct_chain(&mut lm, author_c, 1).await;
    let c_ref = block_ref(&chain[0]);
    agb.enter(1, now, &mut lm, &mut rep);
    let proposal_u = ViewProposal {
        view: 1,
        c: vec![c_ref.clone()],
        t: Vec::new(),
        m: None,
    };
    // Every party's counted echo (including our own, via `on_echo` directly rather
    // than the engine's own gate) is grade-0 -- homogeneous ReadyGrade::Zero, so own
    // R_i(1) ends up recorded as grade-0 regardless of who contributed.
    for (s, _) in authors() {
        agb.on_echo(
            Echo {
                proposal: proposal_u.clone(),
                grade: 0,
                sender: s,
                wish: 0,
                origin: None,
            },
            &mut rep,
        );
    }

    agb.enter(4, now, &mut lm, &mut rep);
    let m = Some(ResolutionEntry::Full(1, vec![c_ref], Vec::new()));
    let proposal_w = carrying_proposal(&mut lm, author_w, 4, m).await;
    let sender_w = proposer_of(4);
    let effects = agb.on_propose(sender_w, proposal_w, now, &mut lm, &mut rep);
    assert!(
        echo_effect(&effects).is_none(),
        "own R_i(1) is exactly grade-0 -- Full entry's rule excludes this outright"
    );
}

#[tokio::test]
async fn meta_ok_full_rejects_when_own_ready_names_different_payload() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (other_c, _) = authors()[2];
    let (author_w, _) = authors()[1];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_metaok_full_reject_diff_payload");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);
    let now = std::time::Instant::now();

    let (_c_ref, proposal_u) =
        drive_own_positive_echo(&mut agb, &mut lm, &mut rep, 1, author_c, now).await;
    let others: Vec<_> = authors()
        .into_iter()
        .filter(|(pk, _)| *pk != self_name)
        .take(2)
        .collect();
    for (sender, _) in &others {
        agb.on_echo(
            Echo {
                proposal: proposal_u.clone(),
                grade: 1,
                sender: *sender,
                wish: 0,
                origin: None,
            },
            &mut rep,
        );
    }
    // own R_i(1) is grade-1 for proposal_u's payload -- but the entry names a DIFFERENT
    // (unrelated) payload.
    let different_ref = (other_c, 1, crypto::Digest([77u8; 32]));

    agb.enter(4, now, &mut lm, &mut rep);
    let m = Some(ResolutionEntry::Full(1, vec![different_ref], Vec::new()));
    let proposal_w = carrying_proposal(&mut lm, author_w, 4, m).await;
    let sender_w = proposer_of(4);
    let effects = agb.on_propose(sender_w, proposal_w, now, &mut lm, &mut rep);
    assert!(
        echo_effect(&effects).is_none(),
        "own R_i(1) names a different payload than the entry"
    );
}

#[tokio::test]
async fn meta_ok_core_passes_with_grade_zero_and_rejects_grade_one() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (author_w, _) = authors()[1];

    // Grade-0 (Core-compatible) case.
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_metaok_core_pass");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);
    let now = std::time::Instant::now();
    let chain = direct_chain(&mut lm, author_c, 1).await;
    let c_ref = block_ref(&chain[0]);
    agb.enter(1, now, &mut lm, &mut rep);
    let proposal_u = ViewProposal {
        view: 1,
        c: vec![c_ref.clone()],
        t: Vec::new(),
        m: None,
    };
    // Homogeneous grade-0 quorum via `on_echo` (including a statement claiming to be
    // from `self_name`) -- `recheck_ready` always records ITS OWN ready determination
    // under `self.name` once quorum is reached, regardless of who contributed to the
    // tally, so own R_i(1) ends up grade-0.
    for (s, _) in authors() {
        agb.on_echo(
            Echo {
                proposal: proposal_u.clone(),
                grade: 0,
                sender: s,
                wish: 0,
                origin: None,
            },
            &mut rep,
        );
    }
    assert!(agb.ready_stage_total(1) > 0);

    agb.enter(4, now, &mut lm, &mut rep);
    let m = Some(ResolutionEntry::Core(1, vec![c_ref.clone()], Vec::new()));
    let proposal_w = carrying_proposal(&mut lm, author_w, 4, m).await;
    let sender_w = proposer_of(4);
    let effects = agb.on_propose(sender_w, proposal_w, now, &mut lm, &mut rep);
    let echo = echo_effect(&effects)
        .expect("MetaOK Core must pass: own R_i(1) is grade-0 for exactly the entry's payload");
    assert_eq!(echo.grade, 1);

    // Grade-1 (Core-incompatible) case, independent setup.
    let (mut lm2, _store2) = new_lane_manager(self_name, ".db_test_metaok_core_reject");
    let mut rep2 = new_repairer(self_name, &lm2);
    let mut agb2 = new_agb_engine(self_name);
    let (c_ref2, proposal_u2) =
        drive_own_positive_echo(&mut agb2, &mut lm2, &mut rep2, 1, author_c, now).await;
    let others: Vec<_> = authors()
        .into_iter()
        .filter(|(pk, _)| *pk != self_name)
        .take(2)
        .collect();
    for (s, _) in &others {
        agb2.on_echo(
            Echo {
                proposal: proposal_u2.clone(),
                grade: 1,
                sender: *s,
                wish: 0,
                origin: None,
            },
            &mut rep2,
        );
    }
    agb2.enter(4, now, &mut lm2, &mut rep2);
    let m2 = Some(ResolutionEntry::Core(1, vec![c_ref2], Vec::new()));
    let proposal_w2 = carrying_proposal(&mut lm2, author_w, 4, m2).await;
    let sender_w2 = proposer_of(4);
    let effects2 = agb2.on_propose(sender_w2, proposal_w2, now, &mut lm2, &mut rep2);
    assert!(
        echo_effect(&effects2).is_none(),
        "own R_i(1) is grade-1 -- Core entry's rule excludes exactly this"
    );
}

#[tokio::test]
async fn meta_ok_skip_requires_own_noready_for_target() {
    let (self_name, _) = authors()[3];
    let (author_w, _) = authors()[1];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_metaok_skip");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);
    let now = std::time::Instant::now();

    agb.enter(1, now, &mut lm, &mut rep);
    // Before our own no-ready fires: MetaOK must block (own R_i(1) not yet emitted).
    agb.enter(4, now, &mut lm, &mut rep);
    let m = Some(ResolutionEntry::Skip(1));
    let proposal_w = carrying_proposal(&mut lm, author_w, 4, m.clone()).await;
    let sender_w = proposer_of(4);
    let effects = agb.on_propose(sender_w, proposal_w.clone(), now, &mut lm, &mut rep);
    assert!(echo_effect(&effects).is_none());

    // Own E_i(1) must ALSO be emitted (bullet 1 applies to Skip too) -- no proposal
    // for u=1 ever fixed, so the absolute echo deadline's echo-skip is the path.
    agb.on_echo_absolute_timer(1, &mut rep);
    // Fire our own no-ready for u=1 (R3 absolute deadline), then re-check via
    // `recheck_all` (mirrors the production/harness trigger after any own-response
    // change).
    agb.on_ready_timer(1);
    let effects = agb.recheck_all(&mut lm, &mut rep);
    let echo = effects
        .iter()
        .find_map(|e| match e {
            Effect::BroadcastEcho(EchoOut::Single(e)) if e.proposal.view == 4 => Some(e),
            _ => None,
        })
        .expect("MetaOK Skip must now pass: own R_i(1) is a noready");
    assert_eq!(echo.grade, 1);
}

#[tokio::test]
async fn meta_ok_lock_rule_blocks_non_matching_entry_while_lock_active() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (author_w, _) = authors()[1];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_metaok_lock_rule");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);
    let now = std::time::Instant::now();

    let (c_ref, proposal_u) =
        drive_own_positive_echo(&mut agb, &mut lm, &mut rep, 1, author_c, now).await;
    // Complete to grade-1 ready (lock stays active: 0 nonmatching the whole time).
    let others: Vec<_> = authors()
        .into_iter()
        .filter(|(pk, _)| *pk != self_name)
        .take(2)
        .collect();
    for (s, _) in &others {
        agb.on_echo(
            Echo {
                proposal: proposal_u.clone(),
                grade: 1,
                sender: *s,
                wish: 0,
                origin: None,
            },
            &mut rep,
        );
    }
    assert_eq!(agb.lock_active_for_test(1), Some(true));

    // A Core(1, [c_ref], []) entry -- NOT the exact matching Full -- must be rejected
    // by the lock rule alone, even though the outcome-specific Core check (own R_i(1)
    // not grade-1) would otherwise fail anyway (grade-1 excludes Core too) -- to
    // isolate the LOCK rule specifically, use a Full entry naming a DIFFERENT payload
    // than the lock's own (C,T): still fails, but for the lock-rule reason.
    agb.enter(4, now, &mut lm, &mut rep);
    let (other_c, _) = authors()[2];
    let different_ref = (other_c, 1, crypto::Digest([55u8; 32]));
    let m = Some(ResolutionEntry::Full(1, vec![different_ref], Vec::new()));
    let proposal_w = carrying_proposal(&mut lm, author_w, 4, m).await;
    let sender_w = proposer_of(4);
    let effects = agb.on_propose(sender_w, proposal_w, now, &mut lm, &mut rep);
    assert!(
        echo_effect(&effects).is_none(),
        "an active lock only lets the EXACT matching Full entry through"
    );

    // The exact matching Full entry, by contrast, passes the lock rule (and the rest
    // of MetaOK, since own R_i(1) is grade-1 for exactly this payload).
    agb.enter(5, now, &mut lm, &mut rep);
    let m2 = Some(ResolutionEntry::Full(1, vec![c_ref], Vec::new()));
    let proposal_w2 = carrying_proposal(&mut lm, author_w, 5, m2).await;
    let sender_w2 = proposer_of(5);
    let effects2 = agb.on_propose(sender_w2, proposal_w2, now, &mut lm, &mut rep);
    assert!(
        echo_effect(&effects2).is_some(),
        "the exact matching Full entry passes the active lock rule"
    );
}

#[tokio::test]
async fn origin_bit_one_iff_own_echo_matches_full_payload_grade1() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (author_w, _) = authors()[1];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_origin_bit");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);
    let now = std::time::Instant::now();

    let (c_ref, proposal_u) =
        drive_own_positive_echo(&mut agb, &mut lm, &mut rep, 1, author_c, now).await;
    let others: Vec<_> = authors()
        .into_iter()
        .filter(|(pk, _)| *pk != self_name)
        .take(2)
        .collect();
    for (s, _) in &others {
        agb.on_echo(
            Echo {
                proposal: proposal_u.clone(),
                grade: 1,
                sender: *s,
                wish: 0,
                origin: None,
            },
            &mut rep,
        );
    }

    agb.enter(4, now, &mut lm, &mut rep);
    let m = Some(ResolutionEntry::Full(1, vec![c_ref], Vec::new()));
    let proposal_w = carrying_proposal(&mut lm, author_w, 4, m).await;
    let sender_w = proposer_of(4);
    let effects = agb.on_propose(sender_w, proposal_w, now, &mut lm, &mut rep);
    let echo = echo_effect(&effects).expect("MetaOK must pass here");
    assert_eq!(
        echo.origin,
        Some(1),
        "own E_i(1) is a grade-1 echo for exactly the entry's payload"
    );
}

#[tokio::test]
async fn ready_ok_blocks_ready_until_f_plus_1_origin_one_echoes_then_fires() {
    // Build a 4-node in-proc scenario (via the raw engine, one instance) where the
    // CARRYING view w's own echo quorum reaches 2f+1 (party count under equal stake)
    // but fewer than f+1=2 of them carry origin=1 -- ReadyOK must block; once a second
    // origin=1 echo arrives, ReadyOK passes and Ready fires.
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (author_w, _) = authors()[1];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_ready_ok");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);
    let now = std::time::Instant::now();

    // u=1: drive to a real grade-1 ready so at least ONE origin=1 echo (our own, on w)
    // is achievable.
    let (c_ref, proposal_u) =
        drive_own_positive_echo(&mut agb, &mut lm, &mut rep, 1, author_c, now).await;
    let others: Vec<_> = authors()
        .into_iter()
        .filter(|(pk, _)| *pk != self_name)
        .take(2)
        .collect();
    for (s, _) in &others {
        agb.on_echo(
            Echo {
                proposal: proposal_u.clone(),
                grade: 1,
                sender: *s,
                wish: 0,
                origin: None,
            },
            &mut rep,
        );
    }

    // w=4: our own echo (origin=1, since own R_i... wait Ann reads own E_i(u), already
    // satisfied). Entering + proposing fires our own echo with origin=1 (previous
    // test already confirms this).
    agb.enter(4, now, &mut lm, &mut rep);
    let m = Some(ResolutionEntry::Full(1, vec![c_ref], Vec::new()));
    let proposal_w = carrying_proposal(&mut lm, author_w, 4, m).await;
    let sender_w = proposer_of(4);
    let effects = agb.on_propose(sender_w, proposal_w.clone(), now, &mut lm, &mut rep);
    let our_echo = echo_effect(&effects).expect("own echo for w fires").clone();
    assert_eq!(our_echo.origin, Some(1));

    // Feed one more grade-1 echo for w from a party WITHOUT origin=1 (never resolved
    // u=1 itself -- origin computed at THEIR emission time from THEIR OWN E_i(1),
    // which they never emitted, so `compute_origin` on their side would be `Some(0)`;
    // simulate that received bit directly).
    let (p1, _) = others[0];
    let e1 = agb.on_echo(
        Echo {
            proposal: proposal_w.clone(),
            grade: 1,
            sender: p1,
            wish: 0,
            origin: Some(0),
        },
        &mut rep,
    );
    assert!(ready_effect(&e1).is_none(), "2 counted echoes (self origin=1, p1 origin=0) -- quorum not yet reached (3 needed) and origin_ones=1 < f+1=2 anyway");

    // A third echo, ALSO origin=0, reaches the 2f+1=3 total-echo quorum but ReadyOK's
    // origin-ones count is still just 1 (< f+1=2) -- ready must NOT fire.
    let (p2, _) = others[1];
    let e2 = agb.on_echo(
        Echo {
            proposal: proposal_w.clone(),
            grade: 1,
            sender: p2,
            wish: 0,
            origin: Some(0),
        },
        &mut rep,
    );
    assert!(
        ready_effect(&e2).is_none(),
        "quorum reached but ReadyOK's origin=1 count (1) is below f+1=2 -- must not go ready"
    );

    // Now replace the scenario with a 4th, origin=1 statement is impossible (each
    // sender counts once) -- instead verify the positive direction with a fresh
    // engine where the SECOND external echo carries origin=1.
    let (mut lm2, _store2) = new_lane_manager(self_name, ".db_test_ready_ok_pass");
    let mut rep2 = new_repairer(self_name, &lm2);
    let mut agb2 = new_agb_engine(self_name);
    let (c_ref2, proposal_u2) =
        drive_own_positive_echo(&mut agb2, &mut lm2, &mut rep2, 1, author_c, now).await;
    for (s, _) in &others {
        agb2.on_echo(
            Echo {
                proposal: proposal_u2.clone(),
                grade: 1,
                sender: *s,
                wish: 0,
                origin: None,
            },
            &mut rep2,
        );
    }
    agb2.enter(4, now, &mut lm2, &mut rep2);
    let m2 = Some(ResolutionEntry::Full(1, vec![c_ref2], Vec::new()));
    let proposal_w2 = carrying_proposal(&mut lm2, author_w, 4, m2).await;
    let sender_w2 = proposer_of(4);
    let effects2 = agb2.on_propose(sender_w2, proposal_w2.clone(), now, &mut lm2, &mut rep2);
    let our_echo2 = echo_effect(&effects2)
        .expect("own echo for w2 fires")
        .clone();
    assert_eq!(our_echo2.origin, Some(1));
    let (p1b, _) = others[0];
    agb2.on_echo(
        Echo {
            proposal: proposal_w2.clone(),
            grade: 1,
            sender: p1b,
            wish: 0,
            origin: Some(1),
        },
        &mut rep2,
    );
    let (p2b, _) = others[1];
    let e3 = agb2.on_echo(
        Echo {
            proposal: proposal_w2.clone(),
            grade: 1,
            sender: p2b,
            wish: 0,
            origin: Some(0),
        },
        &mut rep2,
    );
    let ready = ready_effect(&e3)
        .expect("quorum=3 reached AND origin_ones=2 >= f+1=2 -- ReadyOK now passes");
    assert_eq!(ready.grade, ReadyGrade::One);
}

#[tokio::test]
async fn d6_5_noready_counted_in_ready_stage_census_by_sender() {
    let (self_name, _) = authors()[3];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_d6_5_noready_census");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);

    agb.enter(1, std::time::Instant::now(), &mut lm, &mut rep);
    assert_eq!(agb.noready_count(1), 0);
    assert_eq!(agb.ready_stage_total(1), 0);

    let others: Vec<_> = authors()
        .into_iter()
        .filter(|(pk, _)| *pk != self_name)
        .collect();
    agb.on_noready(1, others[0].0);
    assert_eq!(agb.noready_count(1), 1);
    assert_eq!(agb.ready_stage_total(1), 1);

    // Dedup: a second noready from the SAME sender never re-counts.
    agb.on_noready(1, others[0].0);
    assert_eq!(agb.noready_count(1), 1);

    agb.on_noready(1, others[1].0);
    assert_eq!(agb.noready_count(1), 2);

    // Our own no-ready (R3 absolute deadline) also lands in the SAME census.
    agb.on_ready_timer(1);
    assert_eq!(agb.noready_count(1), 3);
    assert_eq!(agb.ready_stage_total(1), 3);
    assert!(
        agb.ready_stage_non_grade1_count(1) >= 3,
        "noready counts as non-grade-1 too"
    );
}

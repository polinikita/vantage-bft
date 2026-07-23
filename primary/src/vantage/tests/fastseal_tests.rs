// PHASE4-SPEC.md §12 "Fast seal" (§8): the optimistic lock and the all-n matching-echo
// fast seal, driven directly against `AgbEngine`.

use super::common::*;
use crate::vantage::agb::{Echo, Outcome, ViewProposal};
use crate::vantage::Effect;

fn sealed_full(effects: &[Effect]) -> Option<(crate::vantage::Manifest, crate::vantage::Manifest)> {
    effects.iter().find_map(|e| match e {
        Effect::Sealed(_, Outcome::Full(c, t)) => Some((c.clone(), t.clone())),
        _ => None,
    })
}

/// Sets up party `self_name`'s `LaneManager` with a real directly-published chain for
/// `author_c`, its own positive gate firing for view 1 (so a fast-seal lock is
/// recorded), and returns the engine/repairer plus the fixed proposal.
async fn fired_scenario(path: &str) -> (crate::vantage::AgbEngine, crate::vantage::Repairer, ViewProposal) {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(self_name, path);
    let chain = direct_chain(&mut lm, author_c, 1).await;
    let c_ref = block_ref(&chain[0]);
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);
    let now = std::time::Instant::now();
    agb.enter(1, now, &mut lm, &mut rep);
    let sender = proposer_of(1);
    let proposal = crate::vantage::agb::ViewProposal { view: 1, c: vec![c_ref], t: Vec::new(), m: None };
    let effects = agb.on_propose(sender, proposal.clone(), now, &mut lm, &mut rep);
    assert!(effects.iter().any(|e| matches!(e, Effect::BroadcastEcho(_))), "our own positive gate must fire first");
    (agb, rep, proposal)
}

#[tokio::test]
async fn lock_is_recorded_before_our_matching_echo_is_sent() {
    let (agb, _rep, _proposal) = fired_scenario(".db_test_fastseal_lock_recorded").await;
    assert_eq!(agb.lock_active_for_test(1), Some(true));
}

#[tokio::test]
async fn fastseal_fires_on_all_n_matching_echoes() {
    let (mut agb, mut rep, proposal) = fired_scenario(".db_test_fastseal_all_n").await;
    let all = authors();
    // We (authors()[3]) already counted our own matching grade-1 echo when the gate
    // fired. Feed the remaining n-1 = 3 parties' matching echoes.
    let mut last = Vec::new();
    for (sender, _) in all.into_iter().filter(|(pk, _)| *pk != authors()[3].0) {
        last = agb.on_echo(
            Echo {
                proposal: proposal.clone(),
                grade: 1,
                sender,
                wish: 0,
            origin: None,
            },
            &mut rep,
        );
    }
    let (c, t) = sealed_full(&last).expect("fastseal must fire once all n parties match");
    assert_eq!(c, proposal.c);
    assert_eq!(t, proposal.t);
    // Fastseal alone never fires completion or a direct result.
    assert!(agb.completed_for_test(1).is_none());
    assert!(agb.directed_for_test(1).is_none());
}

#[tokio::test]
async fn lock_deactivates_at_f_plus_1_nonmatching_and_never_reactivates() {
    let (mut agb, mut rep, proposal) = fired_scenario(".db_test_fastseal_deactivate").await;
    let all = authors();
    let others: Vec<_> = all.into_iter().filter(|(pk, _)| *pk != authors()[3].0).collect();

    // f+1 = 2 non-matching echo-stage statements (echo-skip counts as non-matching).
    agb.on_echo_skip(1, others[0].0);
    agb.on_echo_skip(1, others[1].0);
    assert_eq!(agb.lock_active_for_test(1), Some(false));

    // Even if the remaining parties now send *matching* echoes, the lock stays dead --
    // fastseal must never fire.
    let effects = agb.on_echo(
        Echo {
            proposal: proposal.clone(),
            grade: 1,
            sender: others[2].0,
            wish: 0,
        origin: None,
        },
        &mut rep,
    );
    assert!(sealed_full(&effects).is_none());
    assert_eq!(agb.lock_active_for_test(1), Some(false));
}

#[tokio::test]
async fn fastseal_produces_no_completion_or_direct_side_effects() {
    let (mut agb, mut rep, proposal) = fired_scenario(".db_test_fastseal_no_side_effects").await;
    let all = authors();
    for (sender, _) in all.into_iter().filter(|(pk, _)| *pk != authors()[3].0) {
        agb.on_echo(
            Echo {
                proposal: proposal.clone(),
                grade: 1,
                sender,
                wish: 0,
            origin: None,
            },
            &mut rep,
        );
    }
    assert!(agb.sealed_for_test(1).is_some());
    assert!(agb.completed_for_test(1).is_none());
    assert!(agb.directed_for_test(1).is_none());
}

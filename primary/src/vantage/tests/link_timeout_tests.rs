//! Early refusal of proposers and resolver primaries whose transport link is down.
use super::common::*;
use crate::vantage::agb::{ResolutionEntry, ViewProposal};
use crate::vantage::direct_resolution::{
    DirectResolutionEffect, DirectResolutionTimerKind, DirectResolver,
};
use crate::vantage::Effect;
use std::time::Instant;

#[tokio::test]
async fn a_down_proposer_view_without_a_proposal_is_refused_once() {
    let (self_name, _) = authors()[3];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_link_down_refusal");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);
    let now = Instant::now();

    agb.enter(1, now, &mut lm, &mut rep);
    assert_eq!(agb.views_awaiting_proposal_from(proposer_of(1)), vec![1]);
    assert!(agb.views_awaiting_proposal_from(proposer_of(2)).is_empty());

    let effects = agb.refuse_unproposed_view(1, "link_down", &mut rep);
    assert!(effects
        .iter()
        .any(|e| matches!(e, Effect::BroadcastEchoSkip(1))));
    assert!(effects
        .iter()
        .any(|e| matches!(e, Effect::BroadcastNoReady(1))));
    assert!(agb.views_awaiting_proposal_from(proposer_of(1)).is_empty());

    // The absolute timers and a repeated link signal find nothing left to refuse.
    assert!(agb.refuse_unproposed_view(1, "link_down", &mut rep).is_empty());
    assert!(agb.on_echo_absolute_timer(1, &mut rep).is_empty());
}

#[tokio::test]
async fn a_view_with_a_fixed_proposal_ignores_the_link_signal() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_link_down_fixed");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);
    let now = Instant::now();

    let chain_c = direct_chain(&mut lm, author_c, 1).await;
    let c_ref = block_ref(&chain_c[0]);
    agb.enter(1, now, &mut lm, &mut rep);
    let proposal = ViewProposal {
        view: 1,
        c: vec![c_ref],
        t: Vec::new(),
        m: None,
    };
    agb.on_propose(proposer_of(1), proposal, now, &mut lm, &mut rep);

    assert!(agb.views_awaiting_proposal_from(proposer_of(1)).is_empty());
    let effects = agb.refuse_unproposed_view(1, "link_down", &mut rep);
    assert!(
        !effects.iter().any(|e| matches!(
            e,
            Effect::BroadcastEchoSkip(_) | Effect::BroadcastNoReady(_)
        )),
        "a received proposal is judged on its own; the link signal must not refuse it"
    );
}

#[test]
fn a_down_resolver_primary_is_rotated_by_the_link_timer() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let target = 8;
    let probe = DirectResolver::new(names[0], test_committee(), test_sid(), TEST_DELTA_MS);
    let silent_primary = probe.resolution_leader(target, 1);
    let observer = names
        .iter()
        .copied()
        .find(|name| *name != silent_primary)
        .unwrap();

    let mut resolver = DirectResolver::new(observer, test_committee(), test_sid(), TEST_DELTA_MS);
    resolver.update_candidates(target, [ResolutionEntry::Skip(target)]);
    for sender in &names {
        resolver.on_wish(crate::vantage::direct_resolution::DirectResolutionWish {
            target,
            view: 1,
            sender: *sender,
        });
    }
    assert_eq!(resolver.current_view(target), 1);
    assert_eq!(
        resolver.views_awaiting_proposal_from(silent_primary),
        vec![(target, 1)]
    );
    assert!(resolver
        .views_awaiting_proposal_from(probe.resolution_leader(target, 2))
        .is_empty());

    let effects = resolver.on_timer(target, 1, DirectResolutionTimerKind::PrimaryDown);
    assert!(effects.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::BroadcastWish(wish) if wish.view == 2
    )));
}

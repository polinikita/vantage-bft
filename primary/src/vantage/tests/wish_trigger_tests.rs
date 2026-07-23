// PHASE5-SPEC.md §12 "W3"/"W4" -- the two-response wish trigger (`AgbEngine`'s
// `two_response_wish_target` hook, exercised end-to-end through its five
// response-emission call sites) and the piggyback-outside-identity convention.

use super::common::*;
use super::harness::{drain_local, Node};
use crate::vantage::agb::{Echo, ReadyGrade, Ready, ViewProposal};
use crate::vantage::node::Inbound;
use crate::vantage::Effect;
use crypto::Digest;
use std::collections::VecDeque;
use std::time::Instant;

fn sample_proposal(view: u64) -> ViewProposal {
    let (a0, _) = authors()[0];
    ViewProposal {
        view,
        c: vec![(a0, 1, Digest([1u8; 32]))],
        t: Vec::new(),
        m: None,
    }
}

fn raise_wish_target(effects: &[Effect]) -> Option<crate::primary::View> {
    effects.iter().find_map(|e| match e {
        Effect::RaiseWish(v) => Some(*v),
        _ => None,
    })
}

// --- W3: both trigger-arithmetic cases -------------------------------------------------

#[tokio::test]
async fn w3_echo_stage_completes_pair_raises_wish_to_u_plus_2() {
    let (self_name, _) = authors()[3];
    let (lm, _store) = new_lane_manager(self_name, ".db_test_w3_echo_pair");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);

    // A real 2f+1=3-sender quorum of counted proposal echoes for view 1 (any grade,
    // any sender -- R3's tally doesn't care who) fires our own ready-stage response
    // for view 1, with no proposal/lane-content machinery needed at all.
    let proposal1 = sample_proposal(1);
    let senders: Vec<_> = authors().into_iter().take(3).collect();
    let mut last = Vec::new();
    for (pk, _) in &senders {
        last = agb.on_echo(Echo { proposal: proposal1.clone(), grade: 1, sender: *pk, wish: 0, origin: None }, &mut rep);
    }
    assert!(last.iter().any(|e| matches!(e, Effect::BroadcastReady(_))), "test setup: view 1's ready-stage response must be sent");

    // Now the echo-stage response for u=2 is about to be emitted (the absolute
    // deadline unconditionally emits an echo-skip -- no proposal/entry needed either)
    // with the ready-stage response for u-1=1 already sent -- W3 must raise the wish
    // to u+2=4, *before* the response effect.
    let effects = agb.on_echo_absolute_timer(2, &mut rep);
    assert!(effects.iter().any(|e| matches!(e, Effect::BroadcastEchoSkip(2))));
    assert_eq!(raise_wish_target(&effects), Some(4));
    let raise_pos = effects.iter().position(|e| matches!(e, Effect::RaiseWish(_))).unwrap();
    let resp_pos = effects.iter().position(|e| matches!(e, Effect::BroadcastEchoSkip(_))).unwrap();
    assert!(raise_pos < resp_pos, "the raise must precede the response effect it piggybacks on");
}

#[tokio::test]
async fn w3_echo_stage_without_the_pairing_ready_never_raises() {
    let (self_name, _) = authors()[3];
    let (lm, _store) = new_lane_manager(self_name, ".db_test_w3_echo_no_pair");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);

    // Ready-stage response for view 1 was never sent -- the echo-stage response for
    // u=2 must not raise anything.
    let effects = agb.on_echo_absolute_timer(2, &mut rep);
    assert!(raise_wish_target(&effects).is_none());
}

#[tokio::test]
async fn w3_ready_stage_completes_pair_raises_wish_to_u_plus_3() {
    let (self_name, _) = authors()[3];
    let (lm, _store) = new_lane_manager(self_name, ".db_test_w3_ready_pair");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);

    // Echo-stage response for view 2 already sent (an unconditional echo-skip at the
    // absolute deadline, no pairing needed for *this* one).
    let effects = agb.on_echo_absolute_timer(2, &mut rep);
    assert!(effects.iter().any(|e| matches!(e, Effect::BroadcastEchoSkip(2))));

    // The ready-stage response for u=1 is about to be emitted (its own absolute
    // deadline) with the echo-stage response for u+1=2 already sent -- W3 must raise
    // the wish to u+3=4.
    let effects = agb.on_ready_timer(1);
    assert!(effects.iter().any(|e| matches!(e, Effect::BroadcastNoReady(1))));
    assert_eq!(raise_wish_target(&effects), Some(4));
    let raise_pos = effects.iter().position(|e| matches!(e, Effect::RaiseWish(_))).unwrap();
    let resp_pos = effects.iter().position(|e| matches!(e, Effect::BroadcastNoReady(_))).unwrap();
    assert!(raise_pos < resp_pos);
}

#[tokio::test]
async fn w3_ready_stage_without_the_pairing_echo_never_raises() {
    let (self_name, _) = authors()[3];
    let mut agb = new_agb_engine(self_name);
    let effects = agb.on_ready_timer(1); // echo-stage response for view 2 never sent
    assert!(raise_wish_target(&effects).is_none());
}

// --- W4: piggyback outside identity -----------------------------------------------------

#[tokio::test]
async fn w4_duplicate_response_counted_once_but_wish_absorbed_both_times() {
    // Drives `harness::Node::dispatch` directly (mirrors `VantageCore::dispatch_inbound`
    // exactly): two `Echo`s from the same sender, same proposal/grade, differing only
    // in the piggybacked wish. The underlying statement counts once (confirmed below
    // by needing exactly one *more* distinct sender to reach R3's quorum of 3, not
    // two), but the wish component of *both* deliveries must still be absorbed.
    let mut node = Node::new(authors()[3].0, ".db_test_w4_duplicate_response", 6);
    let now = Instant::now();
    let proposal = sample_proposal(1);
    let (sender, _) = authors()[0];

    node.dispatch(Inbound::Echo(Echo { proposal: proposal.clone(), grade: 1, sender, wish: 3, origin: None }), now).await;
    assert_eq!(node.pacemaker.omega_of_for_test(sender), 3);

    node.dispatch(Inbound::Echo(Echo { proposal: proposal.clone(), grade: 1, sender, wish: 7, origin: None }), now).await;
    assert_eq!(node.pacemaker.omega_of_for_test(sender), 7, "the duplicate's wish must still be absorbed");

    // Confirm the statement really was counted only once: feeding this same `sender`
    // twice must not itself be enough progress toward the quorum -- two genuinely
    // distinct senders are still required afterward.
    let (s2, _) = authors()[1];
    let effects = node.dispatch(Inbound::Echo(Echo { proposal: proposal.clone(), grade: 1, sender: s2, wish: 0, origin: None }), now).await;
    assert!(!effects.iter().any(|e| matches!(e, Effect::BroadcastReady(_))), "quorum not yet reached (2 distinct senders)");
    let (s3, _) = authors()[2];
    let effects = node.dispatch(Inbound::Echo(Echo { proposal, grade: 1, sender: s3, wish: 0, origin: None }), now).await;
    assert!(
        effects.iter().any(|e| matches!(e, Effect::BroadcastReady(_))),
        "the 3rd genuinely distinct sender must complete the quorum -- the duplicate never counted a 2nd time"
    );
}

#[tokio::test]
async fn w4_piggybacked_wish_alone_drives_entry_with_no_standalone_wish_messages() {
    let mut nodes = vec![Node::new(authors()[3].0, ".db_test_w4_piggyback_drives_entry", 6)];
    let now = Instant::now();
    let mut outbox: VecDeque<(usize, Inbound)> = VecDeque::new();
    let others: Vec<_> = authors().into_iter().filter(|(pk, _)| *pk != nodes[0].name).collect();

    // 3 distinct senders' `EchoSkip(view=1, wish=5)` -- pure piggyback, never an
    // `Inbound::Wish` datagram.
    for (pk, _) in others.iter().take(3) {
        let effects = nodes[0].dispatch(Inbound::EchoSkip(1, *pk, 5), now).await;
        drain_local(&mut nodes, 0, effects, now, &mut outbox);
    }

    assert_eq!(nodes[0].pacemaker.entry_target(), 5);
    assert!(nodes[0].frontier.is_active(5), "entry to view 5 must have been recorded");
    assert_eq!(nodes[0].frontier.a_i(), 4, "W5(c)'s floor must have run too");
}

#[tokio::test]
async fn w4_piggyback_rides_on_all_four_response_types() {
    // Every one of `Echo`/`EchoSkip`/`Ready`/`NoReady` must carry an absorbable wish --
    // exercised directly against `Node::dispatch` (mirrors `VantageCore` exactly).
    let mut node = Node::new(authors()[3].0, ".db_test_w4_all_four_response_types", 6);
    let now = Instant::now();
    let (s1, _) = authors()[0];
    let (s2, _) = authors()[1];
    let (s3, _) = authors()[2];
    let (s4, _) = authors()[1]; // reused sender is fine -- each check is independent

    node.dispatch(Inbound::Echo(Echo { proposal: sample_proposal(1), grade: 1, sender: s1, wish: 9, origin: None }), now).await;
    assert_eq!(node.pacemaker.omega_of_for_test(s1), 9);

    node.dispatch(Inbound::EchoSkip(1, s2, 10), now).await;
    assert_eq!(node.pacemaker.omega_of_for_test(s2), 10);

    node.dispatch(
        Inbound::Ready(Ready {
            proposal: sample_proposal(1),
            grade: ReadyGrade::One,
            sender: s3,
            wish: 11,
        }),
        now,
    )
    .await;
    assert_eq!(node.pacemaker.omega_of_for_test(s3), 11);

    node.dispatch(Inbound::NoReady(1, s4, 12), now).await;
    assert_eq!(node.pacemaker.omega_of_for_test(s4), 12);
}

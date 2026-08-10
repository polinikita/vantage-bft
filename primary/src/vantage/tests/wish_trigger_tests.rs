use super::common::*;
use super::harness::{drain_local, Node};
use crate::vantage::agb::{Echo, EchoOut, Ready, ReadyGrade, ReadyOut, ViewProposal};
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

#[tokio::test]
async fn w3_echo_stage_completes_pair_raises_wish_to_u_plus_2() {
    let (self_name, _) = authors()[3];
    let (lm, _store) = new_lane_manager(self_name, ".db_test_w3_echo_pair");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);

    let proposal1 = sample_proposal(1);
    let senders: Vec<_> = authors().into_iter().take(3).collect();
    let mut last = Vec::new();
    for (pk, _) in &senders {
        last = agb.on_echo(
            Echo {
                proposal: proposal1.clone(),
                grade: 1,
                sender: *pk,
                wish: 0,
                origin: None,
                avail: None,
            },
            &mut rep,
        );
    }
    assert!(
        last.iter().any(|e| matches!(e, Effect::BroadcastReady(_))),
        "test setup: view 1's ready-stage response must be sent"
    );

    let effects = agb.on_echo_absolute_timer(2, &mut rep);
    assert!(effects
        .iter()
        .any(|e| matches!(e, Effect::BroadcastEchoSkip(2))));
    assert_eq!(raise_wish_target(&effects), Some(4));
    let raise_pos = effects
        .iter()
        .position(|e| matches!(e, Effect::RaiseWish(_)))
        .unwrap();
    let resp_pos = effects
        .iter()
        .position(|e| matches!(e, Effect::BroadcastEchoSkip(_)))
        .unwrap();
    assert!(
        raise_pos < resp_pos,
        "the raise must precede the response effect it piggybacks on"
    );
}

#[tokio::test]
async fn w3_echo_stage_without_the_pairing_ready_never_raises() {
    let (self_name, _) = authors()[3];
    let (lm, _store) = new_lane_manager(self_name, ".db_test_w3_echo_no_pair");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);

    let effects = agb.on_echo_absolute_timer(2, &mut rep);
    assert!(raise_wish_target(&effects).is_none());
}

#[tokio::test]
async fn w3_ready_stage_completes_pair_raises_wish_to_u_plus_3() {
    let (self_name, _) = authors()[3];
    let (lm, _store) = new_lane_manager(self_name, ".db_test_w3_ready_pair");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);

    let effects = agb.on_echo_absolute_timer(2, &mut rep);
    assert!(effects
        .iter()
        .any(|e| matches!(e, Effect::BroadcastEchoSkip(2))));

    let effects = agb.on_ready_timer(1);
    assert!(effects
        .iter()
        .any(|e| matches!(e, Effect::BroadcastNoReady(1))));
    assert_eq!(raise_wish_target(&effects), Some(4));
    let raise_pos = effects
        .iter()
        .position(|e| matches!(e, Effect::RaiseWish(_)))
        .unwrap();
    let resp_pos = effects
        .iter()
        .position(|e| matches!(e, Effect::BroadcastNoReady(_)))
        .unwrap();
    assert!(raise_pos < resp_pos);
}

#[tokio::test]
async fn w3_ready_stage_without_the_pairing_echo_never_raises() {
    let (self_name, _) = authors()[3];
    let mut agb = new_agb_engine(self_name);
    let effects = agb.on_ready_timer(1);
    assert!(raise_wish_target(&effects).is_none());
}

#[tokio::test]
async fn w4_duplicate_response_counted_once_but_wish_absorbed_both_times() {
    let mut node = Node::new(authors()[3].0, ".db_test_w4_duplicate_response", 6);
    let now = Instant::now();
    let proposal = sample_proposal(1);
    let (sender, _) = authors()[0];

    node.dispatch(
        Inbound::Echo(EchoOut::Single(Echo {
            proposal: proposal.clone(),
            grade: 1,
            sender,
            wish: 3,
            origin: None,
            avail: None,
        })),
        now,
    )
    .await;
    assert_eq!(node.pacemaker.omega_of(sender), 3);

    node.dispatch(
        Inbound::Echo(EchoOut::Single(Echo {
            proposal: proposal.clone(),
            grade: 1,
            sender,
            wish: 7,
            origin: None,
            avail: None,
        })),
        now,
    )
    .await;
    assert_eq!(
        node.pacemaker.omega_of(sender),
        7,
        "the duplicate's wish must still be absorbed"
    );

    let (s2, _) = authors()[1];
    let effects = node
        .dispatch(
            Inbound::Echo(EchoOut::Single(Echo {
                proposal: proposal.clone(),
                grade: 1,
                sender: s2,
                wish: 0,
                origin: None,
                avail: None,
            })),
            now,
        )
        .await;
    assert!(
        !effects
            .iter()
            .any(|e| matches!(e, Effect::BroadcastReady(_))),
        "quorum not yet reached (2 distinct senders)"
    );
    let (s3, _) = authors()[2];
    let effects = node
        .dispatch(
            Inbound::Echo(EchoOut::Single(Echo {
                proposal,
                grade: 1,
                sender: s3,
                wish: 0,
                origin: None,
                avail: None,
            })),
            now,
        )
        .await;
    assert!(
        effects.iter().any(|e| matches!(e, Effect::BroadcastReady(_))),
        "the 3rd genuinely distinct sender must complete the quorum -- the duplicate never counted a 2nd time"
    );
}

#[tokio::test]
async fn w4_piggybacked_wish_alone_drives_entry_with_no_standalone_wish_messages() {
    let mut nodes = vec![Node::new(
        authors()[3].0,
        ".db_test_w4_piggyback_drives_entry",
        6,
    )];
    let now = Instant::now();
    let mut outbox: VecDeque<(usize, Inbound)> = VecDeque::new();
    let others: Vec<_> = authors()
        .into_iter()
        .filter(|(pk, _)| *pk != nodes[0].name)
        .collect();

    for (pk, _) in others.iter().take(3) {
        let effects = nodes[0].dispatch(Inbound::EchoSkip(1, *pk, 5), now).await;
        drain_local(&mut nodes, 0, effects, now, &mut outbox);
    }

    assert_eq!(nodes[0].pacemaker.entry_target(), 5);
    assert!(
        nodes[0].frontier.is_active(5),
        "entry to view 5 must have been recorded"
    );
    assert_eq!(
        nodes[0].frontier.a_i(),
        4,
        "W5(c)'s floor must have run too"
    );
}

#[tokio::test]
async fn w4_piggyback_rides_on_all_four_response_types() {
    let mut node = Node::new(authors()[3].0, ".db_test_w4_all_four_response_types", 6);
    let now = Instant::now();
    let (s1, _) = authors()[0];
    let (s2, _) = authors()[1];
    let (s3, _) = authors()[2];
    let (s4, _) = authors()[1];

    node.dispatch(
        Inbound::Echo(EchoOut::Single(Echo {
            proposal: sample_proposal(1),
            grade: 1,
            sender: s1,
            wish: 9,
            origin: None,
            avail: None,
        })),
        now,
    )
    .await;
    assert_eq!(node.pacemaker.omega_of(s1), 9);

    node.dispatch(Inbound::EchoSkip(1, s2, 10), now).await;
    assert_eq!(node.pacemaker.omega_of(s2), 10);

    node.dispatch(
        Inbound::Ready(ReadyOut::Single(Ready {
            proposal: sample_proposal(1),
            grade: ReadyGrade::One,
            sender: s3,
            wish: 11,
        })),
        now,
    )
    .await;
    assert_eq!(node.pacemaker.omega_of(s3), 11);

    node.dispatch(Inbound::NoReady(1, s4, 12), now).await;
    assert_eq!(node.pacemaker.omega_of(s4), 12);
}

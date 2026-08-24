use super::common::*;
use crate::primary::View;
use crate::vantage::agb::{BatchViewProposal, ProposalOut, ResolutionEntry, ViewProposal};
use crate::vantage::resolution_chain::{
    AnchorRef, ResolutionBlock, ResolutionChain, ResolutionDone, ResolutionPhase, ResolutionProof,
    ResolutionProposal, ResolutionStatement, ResolutionSuggest, ResolutionWish, ResolutionWitness,
};
use crate::vantage::Effect;
use config::Committee;
use crypto::{Digest, PublicKey};
use std::collections::VecDeque;

fn drain_resolution(
    chains: &mut [ResolutionChain],
    names: &[PublicKey],
    initial: Vec<(usize, Effect)>,
) -> Vec<Vec<View>> {
    let mut queue: VecDeque<(usize, Effect)> = initial.into();
    let mut applied = vec![Vec::new(); chains.len()];
    while let Some((origin, effect)) = queue.pop_front() {
        let mut enqueue = |node: usize, effects: Vec<Effect>| {
            queue.extend(effects.into_iter().map(|effect| (node, effect)));
        };
        match effect {
            Effect::BroadcastResolutionWitness(message) => {
                for (node, chain) in chains.iter_mut().enumerate() {
                    if node != origin {
                        enqueue(node, chain.on_resolution_witness(message.clone()));
                    }
                }
            }
            Effect::BroadcastResolutionWish(message) => {
                for (node, chain) in chains.iter_mut().enumerate() {
                    if node != origin {
                        enqueue(node, chain.on_resolution_wish(message.clone()));
                    }
                }
            }
            Effect::ResolutionSuggestTo(peer, message) => {
                let node = names.iter().position(|name| *name == peer).unwrap();
                enqueue(node, chains[node].on_resolution_suggest(message));
            }
            Effect::BroadcastResolutionProof(message) => {
                for (node, chain) in chains.iter_mut().enumerate() {
                    if node != origin {
                        enqueue(node, chain.on_resolution_proof(message.clone()));
                    }
                }
            }
            Effect::BroadcastResolutionProposal(message) => {
                for (node, chain) in chains.iter_mut().enumerate() {
                    if node != origin {
                        enqueue(node, chain.on_resolution_proposal(message.clone()));
                    }
                }
            }
            Effect::BroadcastResolutionStatement(message) => {
                for (node, chain) in chains.iter_mut().enumerate() {
                    if node != origin {
                        enqueue(node, chain.on_resolution_statement(message.clone()));
                    }
                }
            }
            Effect::BroadcastResolutionDone(message) => {
                for (node, chain) in chains.iter_mut().enumerate() {
                    if node != origin {
                        enqueue(node, chain.on_resolution_done(message.clone()));
                    }
                }
            }
            Effect::ResolutionDoneTo(peer, message) => {
                let node = names.iter().position(|name| *name == peer).unwrap();
                enqueue(node, chains[node].on_resolution_done(message));
            }
            Effect::ResolutionCarrierFetchTo(peer, view, digest) => {
                let node = names.iter().position(|name| *name == peer).unwrap();
                enqueue(
                    node,
                    chains[node].on_carrier_fetch(names[origin], view, digest),
                );
            }
            Effect::ResolutionCarrierServeTo(peer, view, proposal) => {
                let node = names.iter().position(|name| *name == peer).unwrap();
                enqueue(node, chains[node].on_carrier_serve(view, proposal));
            }
            Effect::ResolutionBlockFetchTo(peer, height, digest) => {
                let node = names.iter().position(|name| *name == peer).unwrap();
                enqueue(
                    node,
                    chains[node].on_resolution_block_fetch(names[origin], height, digest),
                );
            }
            Effect::ResolutionBlockServeTo(peer, block) => {
                let node = names.iter().position(|name| *name == peer).unwrap();
                enqueue(node, chains[node].on_resolution_block_serve(block));
            }
            Effect::BroadcastResolutionDecisionRequest(height, requester) => {
                for (node, chain) in chains.iter().enumerate() {
                    if node != origin {
                        enqueue(node, chain.on_decision_request(height, requester));
                    }
                }
            }
            Effect::ApplyAnchor(view, _, _) => applied[origin].push(view),
            Effect::ArmResolutionTimer(..) => {}
            other => panic!("unexpected resolution effect: {other:?}"),
        }
    }
    applied
}

fn chains(committee: &Committee, names: &[PublicKey], sid: &Digest) -> Vec<ResolutionChain> {
    names
        .iter()
        .map(|name| ResolutionChain::new(*name, committee.clone(), sid.clone(), TEST_DELTA_MS))
        .collect()
}

fn resolution_carrier(view: View, target: View) -> ProposalOut {
    ProposalOut::Single(ViewProposal {
        view,
        c: Vec::new(),
        t: Vec::new(),
        m: Some(ResolutionEntry::Skip(target)),
    })
}

fn make_anchor_eligible(
    chain: &mut ResolutionChain,
    names: &[PublicKey],
    view: View,
    target: View,
) -> AnchorRef {
    let carrier = resolution_carrier(view, target);
    let digest = carrier.digest(&test_sid());
    chain.on_completion_reportable(view, carrier);
    for sender in names {
        chain.on_resolution_witness(ResolutionWitness {
            carrier_view: view,
            carrier_digest: digest.clone(),
            sender: *sender,
        });
    }
    assert!(chain.is_eligible_for_test(view, &digest));
    AnchorRef { view, digest }
}

fn enter_resolver_view(chain: &mut ResolutionChain, names: &[PublicKey], view: u64) -> Vec<Effect> {
    let mut effects = Vec::new();
    let height = chain.decided_height() + 1;
    let parent = chain.head().clone();
    for sender in names {
        effects.extend(chain.on_resolution_wish(ResolutionWish {
            height,
            parent: parent.clone(),
            view,
            sender: *sender,
        }));
    }
    assert_eq!(chain.current_resolver_view(), view);
    effects
}

fn drive_to_lock(
    chain: &mut ResolutionChain,
    names: &[PublicKey],
    block: &ResolutionBlock,
    view: u64,
) -> Digest {
    let value = block.digest(&test_sid());
    let height = chain.decided_height() + 1;
    let parent = chain.head().clone();
    let proposal_effects = chain.on_resolution_proposal(ResolutionProposal {
        height,
        parent: parent.clone(),
        view,
        key_view: 0,
        value: value.clone(),
        block: block.clone(),
        sender: chain.resolution_leader(height, view),
    });
    assert!(proposal_effects.iter().any(|effect| matches!(
        effect,
        Effect::BroadcastResolutionStatement(statement)
            if statement.phase == ResolutionPhase::Echo && statement.value == value
    )));

    for phase in [
        ResolutionPhase::Echo,
        ResolutionPhase::Key1,
        ResolutionPhase::Key2,
        ResolutionPhase::Key3,
    ] {
        for sender in names {
            chain.on_resolution_statement(ResolutionStatement {
                height,
                parent: parent.clone(),
                view,
                value: value.clone(),
                phase,
                sender: *sender,
            });
        }
    }
    value
}

#[test]
fn genesis_starts_no_empty_resolution_height() {
    let committee = test_committee();
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let mut chain = ResolutionChain::new(names[0], committee, test_sid(), TEST_DELTA_MS);

    assert!(chain.genesis().is_empty());
    assert_eq!(chain.decided_height(), 0);
    assert_eq!(chain.current_resolver_view(), 0);
}

#[test]
fn resolver_wishes_are_bound_to_the_current_parent() {
    let committee = test_committee();
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let mut chain = ResolutionChain::new(names[0], committee, test_sid(), TEST_DELTA_MS);
    let parent = chain.head().clone();
    let wrong_parent = Digest([99; 32]);

    for sender in names.iter().skip(1) {
        assert!(chain
            .on_resolution_wish(ResolutionWish {
                height: 1,
                parent: wrong_parent.clone(),
                view: 1,
                sender: *sender,
            })
            .is_empty());
    }
    assert!(chain.active_coordinate_for_test().is_none());

    for sender in names.iter().skip(1) {
        chain.on_resolution_wish(ResolutionWish {
            height: 1,
            parent: parent.clone(),
            view: 1,
            sender: *sender,
        });
    }
    assert_eq!(chain.active_coordinate_for_test(), Some((1, parent, 1)));
}

#[test]
fn late_phase_statements_cannot_reopen_an_older_resolver_view() {
    let committee = test_committee();
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let mut chain = ResolutionChain::new(names[3], committee, test_sid(), TEST_DELTA_MS);
    let parent = chain.head().clone();

    for sender in names.iter().take(3) {
        chain.on_resolution_wish(ResolutionWish {
            height: 1,
            parent: parent.clone(),
            view: 2,
            sender: *sender,
        });
    }
    assert_eq!(chain.current_resolver_view(), 2);

    for sender in names.iter().take(3) {
        let effects = chain.on_resolution_statement(ResolutionStatement {
            height: 1,
            parent: parent.clone(),
            view: 1,
            value: Digest([0x55; 32]),
            phase: ResolutionPhase::Echo,
            sender: *sender,
        });
        assert!(effects.is_empty(), "stale phase traffic must be ignored");
    }
    assert_eq!(chain.current_resolver_view(), 2);
}

#[test]
fn body_fetch_retries_each_new_matching_witness_author() {
    let committee = test_committee();
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let mut chain = ResolutionChain::new(names[3], committee, test_sid(), TEST_DELTA_MS);
    let digest = Digest([0xA5; 32]);

    let first = chain.on_resolution_witness(ResolutionWitness {
        carrier_view: 9,
        carrier_digest: digest.clone(),
        sender: names[0],
    });
    assert!(first.iter().any(|effect| matches!(
        effect,
        Effect::ResolutionCarrierFetchTo(peer, 9, d)
            if *peer == names[0] && d == &digest
    )));

    let second = chain.on_resolution_witness(ResolutionWitness {
        carrier_view: 9,
        carrier_digest: digest.clone(),
        sender: names[1],
    });
    assert!(second.iter().any(|effect| matches!(
        effect,
        Effect::ResolutionCarrierFetchTo(peer, 9, d)
            if *peer == names[1] && d == &digest
    )));
}

#[test]
fn one_witness_author_cannot_equivocate_into_two_counts() {
    let committee = test_committee();
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let mut chain = ResolutionChain::new(names[3], committee, test_sid(), TEST_DELTA_MS);
    let first = resolution_carrier(9, 1);
    let second = resolution_carrier(9, 2);
    let first_digest = first.digest(&test_sid());
    let second_digest = second.digest(&test_sid());
    chain.on_completion_reportable(9, first);
    chain.on_completion_reportable(9, second);

    chain.on_resolution_witness(ResolutionWitness {
        carrier_view: 9,
        carrier_digest: first_digest,
        sender: names[0],
    });
    chain.on_resolution_witness(ResolutionWitness {
        carrier_view: 9,
        carrier_digest: second_digest.clone(),
        sender: names[0],
    });
    chain.on_resolution_witness(ResolutionWitness {
        carrier_view: 9,
        carrier_digest: second_digest.clone(),
        sender: names[1],
    });

    assert!(!chain.is_eligible_for_test(9, &second_digest));
}

#[test]
fn a_carrier_body_is_served_once_per_requester() {
    let committee = test_committee();
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let mut chain = ResolutionChain::new(names[3], committee, test_sid(), TEST_DELTA_MS);
    let carrier = resolution_carrier(9, 1);
    let digest = carrier.digest(&test_sid());
    chain.on_completion_reportable(9, carrier);

    let first = chain.on_carrier_fetch(names[0], 9, digest.clone());
    assert!(first.iter().any(|effect| matches!(
        effect,
        Effect::ResolutionCarrierServeTo(peer, 9, _) if *peer == names[0]
    )));
    assert!(chain
        .on_carrier_fetch(names[0], 9, digest.clone())
        .is_empty());
    assert!(chain
        .on_carrier_fetch(names[1], 9, digest)
        .iter()
        .any(|effect| matches!(
            effect,
            Effect::ResolutionCarrierServeTo(peer, 9, _) if *peer == names[1]
        )));
}

#[test]
fn f_byzantine_witnesses_cannot_activate_a_resolution_height() {
    let committee = test_committee();
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let mut chain = ResolutionChain::new(names[3], committee, test_sid(), TEST_DELTA_MS);
    let digest = Digest([0xE1; 32]);

    for sender in names.iter().take(1) {
        chain.on_resolution_witness(ResolutionWitness {
            carrier_view: 9,
            carrier_digest: digest.clone(),
            sender: *sender,
        });
    }

    assert_eq!(chain.active_coordinate_for_test(), None);
    assert!(!chain.is_eligible_for_test(9, &digest));
}

#[test]
fn a_precreated_height_still_raises_wish_when_an_anchor_becomes_eligible() {
    let committee = test_committee();
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let mut chain = ResolutionChain::new(names[3], committee.clone(), test_sid(), TEST_DELTA_MS);
    let parent = chain.head().clone();
    chain.on_resolution_wish(crate::vantage::ResolutionWish {
        height: 1,
        parent,
        view: 1,
        sender: names[0],
    });

    let carrier = ProposalOut::Single(ViewProposal {
        view: 9,
        c: Vec::new(),
        t: Vec::new(),
        m: Some(ResolutionEntry::Skip(1)),
    });
    let digest = carrier.digest(&test_sid());
    chain.on_completion_reportable(9, carrier);
    chain.on_resolution_witness(ResolutionWitness {
        carrier_view: 9,
        carrier_digest: digest.clone(),
        sender: names[0],
    });
    let effects = chain.on_resolution_witness(ResolutionWitness {
        carrier_view: 9,
        carrier_digest: digest,
        sender: names[1],
    });

    assert!(effects
        .iter()
        .any(|effect| matches!(effect, Effect::BroadcastResolutionWish(_))));
}

#[test]
fn a_proposal_arriving_before_wish_is_buffered_until_entry() {
    let committee = test_committee();
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let sid = test_sid();
    let mut chain = ResolutionChain::new(names[3], committee, sid.clone(), TEST_DELTA_MS);
    let carrier = ProposalOut::Single(ViewProposal {
        view: 9,
        c: Vec::new(),
        t: Vec::new(),
        m: Some(ResolutionEntry::Skip(1)),
    });
    let carrier_digest = carrier.digest(&sid);
    let leader = chain.resolution_leader(1, 1);
    let block = ResolutionBlock {
        height: 1,
        parent: chain.head().clone(),
        anchors: vec![AnchorRef {
            view: 9,
            digest: carrier_digest.clone(),
        }],
    };
    let proposal = ResolutionProposal {
        height: 1,
        parent: chain.head().clone(),
        view: 1,
        key_view: 0,
        value: block.digest(&sid),
        block,
        sender: leader,
    };

    assert!(chain.on_resolution_proposal(proposal).is_empty());
    assert_eq!(
        chain.active_coordinate_for_test(),
        Some((1, chain.head().clone(), 0))
    );

    chain.on_completion_reportable(9, carrier);
    for sender in names.iter().take(2) {
        chain.on_resolution_witness(ResolutionWitness {
            carrier_view: 9,
            carrier_digest: carrier_digest.clone(),
            sender: *sender,
        });
    }
    let mut effects = Vec::new();
    for sender in names.iter().take(2) {
        effects.extend(chain.on_resolution_wish(ResolutionWish {
            height: 1,
            parent: chain.head().clone(),
            view: 1,
            sender: *sender,
        }));
    }

    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::BroadcastResolutionStatement(statement)
            if statement.phase == ResolutionPhase::Echo && statement.view == 1
    )));
}

#[test]
fn resolver_timer_advances_to_the_next_view() {
    let committee = test_committee();
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let mut chain = ResolutionChain::new(names[3], committee, test_sid(), TEST_DELTA_MS);
    make_anchor_eligible(&mut chain, &names, 9, 1);
    enter_resolver_view(&mut chain, &names, 1);

    let timer = chain.on_resolution_timer(1, 1);
    assert!(timer.iter().any(|effect| matches!(
        effect,
        Effect::BroadcastResolutionWish(wish) if wish.view == 2
    )));
    let entered = enter_resolver_view(&mut chain, &names, 2);
    assert!(entered
        .iter()
        .any(|effect| matches!(effect, Effect::ArmResolutionTimer(1, 2, _))));
    assert!(chain.on_resolution_timer(1, 1).is_empty());
}

#[test]
fn locked_key3_is_carried_by_next_view_suggestions() {
    let committee = test_committee();
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let sid = test_sid();
    let probe = ResolutionChain::new(names[0], committee.clone(), sid.clone(), TEST_DELTA_MS);
    let leader = probe.resolution_leader(1, 2);
    let mut chain = ResolutionChain::new(leader, committee, sid, TEST_DELTA_MS);
    let anchor = make_anchor_eligible(&mut chain, &names, 9, 1);
    let block = ResolutionBlock {
        height: 1,
        parent: chain.head().clone(),
        anchors: vec![anchor],
    };
    enter_resolver_view(&mut chain, &names, 1);
    let value = drive_to_lock(&mut chain, &names, &block, 1);
    chain.on_resolution_timer(1, 1);
    enter_resolver_view(&mut chain, &names, 2);

    let mut effects = Vec::new();
    for sender in names.iter().filter(|sender| **sender != leader).take(2) {
        effects.extend(chain.on_resolution_suggest(ResolutionSuggest {
            height: 1,
            parent: chain.head().clone(),
            view: 2,
            sender: *sender,
            key3_view: 1,
            key3_value: value.clone(),
            key2_view: 1,
            key2_value: value.clone(),
            prev_key2: 0,
            block: Some(block.clone()),
        }));
    }

    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::BroadcastResolutionProposal(proposal)
            if proposal.view == 2 && proposal.key_view == 1 && proposal.value == value
    )));
}

#[test]
fn a_byzantine_primary_cannot_replace_a_closed_lock() {
    let committee = test_committee();
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let leader = ResolutionChain::new(names[0], committee.clone(), test_sid(), TEST_DELTA_MS)
        .resolution_leader(1, 2);
    let follower = names.iter().copied().find(|name| *name != leader).unwrap();
    let mut chain = ResolutionChain::new(follower, committee, test_sid(), TEST_DELTA_MS);
    let locked_anchor = make_anchor_eligible(&mut chain, &names, 9, 1);
    let conflicting_anchor = make_anchor_eligible(&mut chain, &names, 10, 2);
    let locked = ResolutionBlock {
        height: 1,
        parent: chain.head().clone(),
        anchors: vec![locked_anchor],
    };
    let conflicting = ResolutionBlock {
        height: 1,
        parent: chain.head().clone(),
        anchors: vec![conflicting_anchor],
    };
    enter_resolver_view(&mut chain, &names, 1);
    drive_to_lock(&mut chain, &names, &locked, 1);
    chain.on_resolution_timer(1, 1);
    enter_resolver_view(&mut chain, &names, 2);

    let effects = chain.on_resolution_proposal(ResolutionProposal {
        height: 1,
        parent: chain.head().clone(),
        view: 2,
        key_view: 0,
        value: conflicting.digest(&test_sid()),
        block: conflicting,
        sender: leader,
    });
    assert!(!effects.iter().any(|effect| matches!(
        effect,
        Effect::BroadcastResolutionStatement(statement)
            if statement.phase == ResolutionPhase::Echo
    )));
}

#[test]
fn first_key_proofs_open_an_older_conflicting_lock() {
    let committee = test_committee();
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let leader = ResolutionChain::new(names[0], committee.clone(), test_sid(), TEST_DELTA_MS)
        .resolution_leader(1, 3);
    let follower = names.iter().copied().find(|name| *name != leader).unwrap();
    let mut chain = ResolutionChain::new(follower, committee, test_sid(), TEST_DELTA_MS);
    let locked_anchor = make_anchor_eligible(&mut chain, &names, 9, 1);
    let conflicting_anchor = make_anchor_eligible(&mut chain, &names, 10, 2);
    let locked = ResolutionBlock {
        height: 1,
        parent: chain.head().clone(),
        anchors: vec![locked_anchor],
    };
    let conflicting = ResolutionBlock {
        height: 1,
        parent: chain.head().clone(),
        anchors: vec![conflicting_anchor],
    };
    enter_resolver_view(&mut chain, &names, 1);
    drive_to_lock(&mut chain, &names, &locked, 1);
    enter_resolver_view(&mut chain, &names, 3);

    let value = conflicting.digest(&test_sid());
    assert!(chain
        .on_resolution_proposal(ResolutionProposal {
            height: 1,
            parent: chain.head().clone(),
            view: 3,
            key_view: 2,
            value: value.clone(),
            block: conflicting,
            sender: leader,
        })
        .is_empty());

    let mut effects = Vec::new();
    for sender in names.iter().filter(|sender| **sender != follower).take(2) {
        effects.extend(chain.on_resolution_proof(ResolutionProof {
            height: 1,
            parent: chain.head().clone(),
            view: 3,
            sender: *sender,
            key1_view: 2,
            key1_value: value.clone(),
            prev_key1: 0,
        }));
    }
    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::BroadcastResolutionStatement(statement)
            if statement.phase == ResolutionPhase::Echo && statement.value == value
    )));
}

#[test]
fn a_noneligible_anchor_never_receives_a_resolver_echo() {
    let committee = test_committee();
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let mut chain = ResolutionChain::new(names[3], committee, test_sid(), TEST_DELTA_MS);
    enter_resolver_view(&mut chain, &names, 1);
    let block = ResolutionBlock {
        height: 1,
        parent: chain.head().clone(),
        anchors: vec![AnchorRef {
            view: 99,
            digest: Digest([0xCC; 32]),
        }],
    };
    let effects = chain.on_resolution_proposal(ResolutionProposal {
        height: 1,
        parent: chain.head().clone(),
        view: 1,
        key_view: 0,
        value: block.digest(&test_sid()),
        block,
        sender: chain.resolution_leader(1, 1),
    });
    assert!(!effects.iter().any(|effect| matches!(
        effect,
        Effect::BroadcastResolutionStatement(statement)
            if statement.phase == ResolutionPhase::Echo
    )));
}

#[test]
fn future_height_repair_requires_f_plus_one_hints_and_is_deduplicated() {
    let committee = test_committee();
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let mut chain = ResolutionChain::new(names[3], committee, test_sid(), TEST_DELTA_MS);
    let parent = Digest([0xDD; 32]);

    assert!(chain
        .on_resolution_wish(ResolutionWish {
            height: 3,
            parent: parent.clone(),
            view: 1,
            sender: names[0],
        })
        .is_empty());
    let threshold = chain.on_resolution_wish(ResolutionWish {
        height: 3,
        parent: parent.clone(),
        view: 1,
        sender: names[1],
    });
    assert!(threshold.iter().any(|effect| matches!(
        effect,
        Effect::BroadcastResolutionDecisionRequest(1, requester)
            if *requester == names[3]
    )));
    assert!(chain
        .on_resolution_wish(ResolutionWish {
            height: 4,
            parent,
            view: 2,
            sender: names[2],
        })
        .is_empty());

    let anchor = make_anchor_eligible(&mut chain, &names, 9, 1);
    let block = ResolutionBlock {
        height: 1,
        parent: chain.head().clone(),
        anchors: vec![anchor],
    };
    let value = block.digest(&test_sid());
    let mut decided = Vec::new();
    for sender in names.iter().take(3) {
        decided.extend(chain.on_resolution_done(ResolutionDone {
            height: 1,
            parent: chain.head().clone(),
            value: value.clone(),
            block: block.clone(),
            sender: *sender,
        }));
    }
    assert!(decided.iter().any(|effect| matches!(
        effect,
        Effect::BroadcastResolutionDecisionRequest(2, requester)
            if *requester == names[3]
    )));
}

#[test]
fn data_gc_does_not_invalidate_an_eligible_resolution_anchor() {
    let committee = test_committee();
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let mut chain = ResolutionChain::new(names[3], committee, test_sid(), TEST_DELTA_MS);
    let carrier = ProposalOut::Single(ViewProposal {
        view: 9,
        c: Vec::new(),
        t: Vec::new(),
        m: Some(ResolutionEntry::Skip(1)),
    });
    let digest = carrier.digest(&test_sid());

    chain.on_completion_reportable(9, carrier);
    for sender in names.iter().take(2) {
        chain.on_resolution_witness(ResolutionWitness {
            carrier_view: 9,
            carrier_digest: digest.clone(),
            sender: *sender,
        });
    }
    assert!(chain.is_eligible_for_test(9, &digest));

    chain.advance_resolved_target_floor(20);

    assert!(chain.is_anchor_resolved(1));
    assert!(chain.is_eligible_for_test(9, &digest));
    assert!(chain.held_carrier_for_test(9, &digest));
}

#[test]
fn stable_witnesses_decide_one_batched_anchor_and_ignore_a_later_target_duplicate() {
    let (committee, keys) = Committee::local_benchmark(7, 1, 18_900);
    let names: Vec<_> = keys.iter().map(|key| key.name).collect();
    let sid = crate::vantage::block::session_id(&committee);
    let mut chains = chains(&committee, &names, &sid);

    let carrier = ProposalOut::Batch(BatchViewProposal {
        view: 10,
        c: Vec::new(),
        t: Vec::new(),
        m: vec![ResolutionEntry::Skip(1), ResolutionEntry::Skip(2)],
    });
    assert!(carrier.formed(&committee));
    let carrier_digest = carrier.digest(&sid);
    let mut initial = Vec::new();
    for (node, chain) in chains.iter_mut().enumerate().take(3) {
        initial.extend(
            chain
                .on_completion_reportable(10, carrier.clone())
                .into_iter()
                .map(|effect| (node, effect)),
        );
    }
    let applied = drain_resolution(&mut chains, &names, initial);

    for (node, chain) in chains.iter().enumerate() {
        assert_eq!(chain.decided_height(), 1, "node {node} did not decide");
        assert!(chain.held_carrier_for_test(10, &carrier_digest));
        assert_eq!(chain.decided_block_for_test(1).unwrap().anchors.len(), 1);
        assert!(chain.is_anchor_resolved(1));
        assert!(chain.is_anchor_resolved(2));
        assert_eq!(applied[node], vec![1, 2]);
    }
    let decided_parent = chains[0].decided_block_for_test(1).unwrap().parent.clone();
    let catchup = chains[0].on_resolution_wish(crate::vantage::ResolutionWish {
        height: 1,
        parent: decided_parent,
        view: 1,
        sender: names[6],
    });
    assert!(catchup.iter().any(|effect| matches!(
        effect,
        Effect::ResolutionDoneTo(peer, done) if *peer == names[6] && done.height == 1
    )));

    let duplicate = ProposalOut::Single(ViewProposal {
        view: 20,
        c: Vec::new(),
        t: Vec::new(),
        m: Some(ResolutionEntry::Skip(1)),
    });
    let mut next = Vec::new();
    for (node, chain) in chains.iter_mut().enumerate().take(3) {
        next.extend(
            chain
                .on_completion_reportable(20, duplicate.clone())
                .into_iter()
                .map(|effect| (node, effect)),
        );
    }
    let duplicate_applies = drain_resolution(&mut chains, &names, next);

    for (node, chain) in chains.iter().enumerate() {
        assert_eq!(
            chain.decided_height(),
            2,
            "node {node} did not drain height 2"
        );
        assert!(duplicate_applies[node].is_empty());
    }
}

use super::common::*;
use crate::primary::View;
use crate::vantage::agb::{BatchViewProposal, ProposalOut, ResolutionEntry, ViewProposal};
use crate::vantage::resolution_chain::{
    AnchorRef, ResolutionBlock, ResolutionChain, ResolutionPhase, ResolutionProposal,
    ResolutionStatement, ResolutionWish, ResolutionWitness,
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

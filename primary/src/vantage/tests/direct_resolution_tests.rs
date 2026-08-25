use super::common::*;
use crate::primary::View;
use crate::vantage::{
    DirectResolutionEffect, DirectResolutionPhase, DirectResolutionProposal,
    DirectResolutionStatement, DirectResolutionSuggest, DirectResolutionTimerKind,
    DirectResolutionValueFetch, DirectResolutionValueServe, DirectResolutionVote,
    DirectResolutionWish, DirectResolutionWitness, DirectResolver, ResolutionEntry,
};
use crypto::{Digest, PublicKey};
use std::collections::VecDeque;

fn resolvers(names: &[PublicKey]) -> Vec<DirectResolver> {
    let committee = test_committee();
    names
        .iter()
        .map(|name| DirectResolver::new(*name, committee.clone(), test_sid(), TEST_DELTA_MS))
        .collect()
}

fn drain(
    resolvers: &mut [DirectResolver],
    names: &[PublicKey],
    initial: Vec<(usize, DirectResolutionEffect)>,
) -> Vec<Vec<ResolutionEntry>> {
    let mut queue: VecDeque<_> = initial.into();
    let mut decisions = vec![Vec::new(); resolvers.len()];
    while let Some((origin, effect)) = queue.pop_front() {
        let mut enqueue = |node: usize, effects: Vec<DirectResolutionEffect>| {
            queue.extend(effects.into_iter().map(|effect| (node, effect)));
        };
        match effect {
            DirectResolutionEffect::BroadcastWish(message) => {
                for (node, resolver) in resolvers.iter_mut().enumerate() {
                    if node != origin {
                        enqueue(node, resolver.on_wish(message.clone()));
                    }
                }
            }
            DirectResolutionEffect::WishTo(peer, message) => {
                let node = names.iter().position(|name| *name == peer).unwrap();
                enqueue(node, resolvers[node].on_wish(message));
            }
            DirectResolutionEffect::SuggestTo(peer, message) => {
                let node = names.iter().position(|name| *name == peer).unwrap();
                enqueue(node, resolvers[node].on_suggest(message));
            }
            DirectResolutionEffect::BroadcastProof(message) => {
                for (node, resolver) in resolvers.iter_mut().enumerate() {
                    if node != origin {
                        enqueue(node, resolver.on_proof(message.clone()));
                    }
                }
            }
            DirectResolutionEffect::ProofTo(peer, message) => {
                let node = names.iter().position(|name| *name == peer).unwrap();
                enqueue(node, resolvers[node].on_proof(message));
            }
            DirectResolutionEffect::BroadcastProposal(message) => {
                for (node, resolver) in resolvers.iter_mut().enumerate() {
                    if node != origin {
                        enqueue(node, resolver.on_proposal(message.clone()));
                    }
                }
            }
            DirectResolutionEffect::ProposalTo(peer, message) => {
                let node = names.iter().position(|name| *name == peer).unwrap();
                enqueue(node, resolvers[node].on_proposal(message));
            }
            DirectResolutionEffect::BroadcastStatement(message) => {
                for (node, resolver) in resolvers.iter_mut().enumerate() {
                    if node != origin {
                        enqueue(node, resolver.on_statement(message.clone()));
                    }
                }
            }
            DirectResolutionEffect::StatementTo(peer, message) => {
                let node = names.iter().position(|name| *name == peer).unwrap();
                enqueue(node, resolvers[node].on_statement(message));
            }
            DirectResolutionEffect::BroadcastWitness(message) => {
                for (node, resolver) in resolvers.iter_mut().enumerate() {
                    if node != origin {
                        enqueue(node, resolver.on_witness(message.clone()));
                    }
                }
            }
            DirectResolutionEffect::WitnessTo(peer, message) => {
                let node = names.iter().position(|name| *name == peer).unwrap();
                enqueue(node, resolvers[node].on_witness(message));
            }
            DirectResolutionEffect::BroadcastDone(message) => {
                for (node, resolver) in resolvers.iter_mut().enumerate() {
                    if node != origin {
                        enqueue(node, resolver.on_done(message.clone()));
                    }
                }
            }
            DirectResolutionEffect::DoneTo(peer, message) => {
                let node = names.iter().position(|name| *name == peer).unwrap();
                enqueue(node, resolvers[node].on_done(message));
            }
            DirectResolutionEffect::ValueFetchTo(peer, message) => {
                let node = names.iter().position(|name| *name == peer).unwrap();
                enqueue(node, resolvers[node].on_value_fetch(message));
            }
            DirectResolutionEffect::ValueServeTo(peer, message) => {
                let node = names.iter().position(|name| *name == peer).unwrap();
                enqueue(node, resolvers[node].on_value_serve(message));
            }
            DirectResolutionEffect::ValidateVote {
                target,
                view,
                value,
                entry,
                fresh,
            } => {
                let origin_bit = match (&entry, fresh) {
                    (ResolutionEntry::Full(..) | ResolutionEntry::Core(..), true) => Some(1),
                    _ => None,
                };
                enqueue(
                    origin,
                    resolvers[origin].on_vote(
                        target,
                        view,
                        value,
                        DirectResolutionVote::Accept { origin: origin_bit },
                    ),
                );
            }
            DirectResolutionEffect::Decide(entry) => decisions[origin].push(entry),
            DirectResolutionEffect::ArmTimer(..) => {}
        }
    }
    decisions
}

#[test]
fn all_correct_skip_resolves_without_later_agb_metadata() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let mut nodes = resolvers(&names);
    let target = 7;
    let entry = ResolutionEntry::Skip(target);
    let initial = nodes
        .iter_mut()
        .enumerate()
        .flat_map(|(node, resolver)| {
            resolver
                .update_candidates(target, [entry.clone()])
                .into_iter()
                .map(move |effect| (node, effect))
        })
        .collect();

    let decisions = drain(&mut nodes, &names, initial);
    assert!(decisions.iter().all(|node| node == &vec![entry.clone()]));
    assert!(nodes.iter().all(|resolver| resolver.is_decided(target)));

    let reply = nodes[0].on_wish(DirectResolutionWish {
        target,
        view: 2,
        sender: names[1],
    });
    assert!(reply.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::DoneTo(peer, done)
            if *peer == names[1] && done.entry == entry
    )));
    assert!(nodes[0]
        .on_wish(DirectResolutionWish {
            target,
            view: 3,
            sender: names[1],
        })
        .is_empty());
}

#[test]
fn independent_targets_drain_concurrently() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let mut nodes = resolvers(&names);
    let targets: Vec<View> = (10..42).collect();
    let mut initial = Vec::new();
    for (node, resolver) in nodes.iter_mut().enumerate() {
        for target in &targets {
            initial.extend(
                resolver
                    .update_candidates(*target, [ResolutionEntry::Skip(*target)])
                    .into_iter()
                    .map(|effect| (node, effect)),
            );
        }
    }

    let decisions = drain(&mut nodes, &names, initial);
    for (node, resolver) in nodes.iter().enumerate() {
        assert_eq!(decisions[node].len(), targets.len());
        assert!(targets.iter().all(|target| resolver.is_decided(*target)));
    }
}

fn enter_view(
    resolver: &mut DirectResolver,
    names: &[PublicKey],
    target: View,
    view: u64,
) -> Vec<DirectResolutionEffect> {
    let mut effects = Vec::new();
    for sender in names {
        effects.extend(resolver.on_wish(DirectResolutionWish {
            target,
            view,
            sender: *sender,
        }));
    }
    assert_eq!(resolver.current_view(target), view);
    effects
}

#[test]
fn resolver_starts_after_the_target_proposer_and_cycles_the_committee() {
    let committee = test_committee();
    let names: Vec<_> = committee.authorities.keys().copied().collect();
    let resolver = DirectResolver::new(names[0], committee, test_sid(), TEST_DELTA_MS);
    let target = 11;

    assert_ne!(
        resolver.resolution_leader(target, 1),
        crate::leader::one_based_authority(&test_committee(), target)
    );
    let leaders: std::collections::BTreeSet<_> = (1..=names.len() as u64)
        .map(|view| resolver.resolution_leader(target, view))
        .collect();
    assert_eq!(leaders.len(), names.len());
}

#[test]
fn repeated_primary_turns_cycle_candidates_independently_of_view_modulus() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let target = 11;
    let probe = DirectResolver::new(names[0], test_committee(), test_sid(), TEST_DELTA_MS);
    let leader = probe.resolution_leader(target, 1);
    assert_eq!(leader, probe.resolution_leader(target, 5));

    let full = ResolutionEntry::Full(target, Vec::new(), Vec::new());
    let core = ResolutionEntry::Core(target, Vec::new(), Vec::new());
    let skip = ResolutionEntry::Skip(target);
    let mut resolver = DirectResolver::new(leader, test_committee(), test_sid(), TEST_DELTA_MS);
    resolver.update_candidates(target, [skip.clone(), core.clone(), full.clone()]);

    let mut proposed = Vec::new();
    for view in [1, 5, 9] {
        enter_view(&mut resolver, &names, target, view);
        for sender in names
            .iter()
            .copied()
            .filter(|sender| *sender != leader)
            .take(2)
        {
            proposed.extend(
                resolver
                    .on_suggest(DirectResolutionSuggest {
                        target,
                        view,
                        sender,
                        key3_view: 0,
                        key3_value: Digest::default(),
                        key2_view: 0,
                        key2_value: Digest::default(),
                        prev_key2: 0,
                        entry: None,
                    })
                    .into_iter()
                    .filter_map(|effect| match effect {
                        DirectResolutionEffect::BroadcastProposal(proposal) => Some(proposal.entry),
                        _ => None,
                    }),
            );
        }
    }

    assert_eq!(proposed, vec![full, core, skip]);
}

#[test]
fn a_candidate_arriving_after_zero_key_suggestions_is_proposed_immediately() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let target = 12;
    let probe = DirectResolver::new(names[0], test_committee(), test_sid(), TEST_DELTA_MS);
    let leader = probe.resolution_leader(target, 1);
    let mut resolver = DirectResolver::new(leader, test_committee(), test_sid(), TEST_DELTA_MS);
    resolver.admit_targets_through(target);
    enter_view(&mut resolver, &names, target, 1);

    for sender in names
        .iter()
        .copied()
        .filter(|sender| *sender != leader)
        .take(2)
    {
        assert!(!resolver
            .on_suggest(DirectResolutionSuggest {
                target,
                view: 1,
                sender,
                key3_view: 0,
                key3_value: Digest::default(),
                key2_view: 0,
                key2_value: Digest::default(),
                prev_key2: 0,
                entry: None,
            })
            .iter()
            .any(|effect| matches!(effect, DirectResolutionEffect::BroadcastProposal(_))));
    }

    let effects = resolver.update_candidates(target, [ResolutionEntry::Skip(target)]);
    assert!(effects.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::BroadcastProposal(proposal)
            if proposal.entry == ResolutionEntry::Skip(target)
    )));
}

#[test]
fn proposal_deadline_rotates_only_if_no_proposal_arrived() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let target = 8;
    let leader = DirectResolver::new(names[0], test_committee(), test_sid(), TEST_DELTA_MS)
        .resolution_leader(target, 1);
    let follower = names.iter().copied().find(|name| *name != leader).unwrap();

    let mut silent = DirectResolver::new(follower, test_committee(), test_sid(), TEST_DELTA_MS);
    silent.update_candidates(target, [ResolutionEntry::Skip(target)]);
    enter_view(&mut silent, &names, target, 1);
    let effects = silent.on_timer(target, 1, DirectResolutionTimerKind::Proposal);
    assert!(effects.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::BroadcastWish(wish) if wish.view == 2
    )));

    let entry = ResolutionEntry::Skip(target);
    let mut proposed = DirectResolver::new(follower, test_committee(), test_sid(), TEST_DELTA_MS);
    proposed.update_candidates(target, [entry.clone()]);
    enter_view(&mut proposed, &names, target, 1);
    let value = proposed.value_digest(&entry);
    proposed.on_proposal(DirectResolutionProposal {
        target,
        view: 1,
        key_view: 0,
        value,
        entry,
        sender: leader,
    });
    assert!(proposed
        .on_timer(target, 1, DirectResolutionTimerKind::Proposal)
        .is_empty());
    assert!(proposed
        .on_timer(target, 1, DirectResolutionTimerKind::View)
        .iter()
        .any(|effect| matches!(
            effect,
            DirectResolutionEffect::BroadcastWish(wish) if wish.view == 2
        )));
}

#[test]
fn a_silent_primary_rotates_into_the_next_primary_view() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let target = 8;
    let probe = DirectResolver::new(names[0], test_committee(), test_sid(), TEST_DELTA_MS);
    let silent_primary = probe.resolution_leader(target, 1);
    let next_primary = probe.resolution_leader(target, 2);
    assert_ne!(silent_primary, next_primary);

    let mut resolver =
        DirectResolver::new(next_primary, test_committee(), test_sid(), TEST_DELTA_MS);
    let entry = ResolutionEntry::Skip(target);
    resolver.update_candidates(target, [entry.clone()]);
    enter_view(&mut resolver, &names, target, 1);

    let timeout = resolver.on_timer(target, 1, DirectResolutionTimerKind::Proposal);
    assert!(timeout.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::BroadcastWish(wish) if wish.view == 2
    )));

    let other_correct: Vec<_> = names
        .iter()
        .copied()
        .filter(|sender| *sender != silent_primary && *sender != next_primary)
        .collect();
    for sender in &other_correct {
        resolver.on_wish(DirectResolutionWish {
            target,
            view: 2,
            sender: *sender,
        });
    }
    assert_eq!(resolver.current_view(target), 2);

    let mut effects = Vec::new();
    for sender in other_correct {
        effects.extend(resolver.on_suggest(DirectResolutionSuggest {
            target,
            view: 2,
            sender,
            key3_view: 0,
            key3_value: Digest::default(),
            key2_view: 0,
            key2_value: Digest::default(),
            prev_key2: 0,
            entry: None,
        }));
    }
    assert!(effects.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::BroadcastProposal(proposal)
            if proposal.view == 2 && proposal.entry == entry
    )));
}

#[test]
fn resolver_timer_budgets_keep_the_full_view_bound() {
    let resolver = DirectResolver::new(authors()[0].0, test_committee(), test_sid(), TEST_DELTA_MS);
    assert_eq!(
        resolver.proposal_timeout().as_millis(),
        5 * TEST_DELTA_MS as u128
    );
    assert_eq!(
        resolver.resolver_timeout().as_millis(),
        11 * TEST_DELTA_MS as u128
    );
}

#[test]
fn initial_wish_retries_until_quorum_entry() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let target = 8;
    let mut resolver = DirectResolver::new(names[0], test_committee(), test_sid(), TEST_DELTA_MS);
    let initial = resolver.update_candidates(target, [ResolutionEntry::Skip(target)]);
    assert!(initial.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::ArmTimer(
            t,
            1,
            DirectResolutionTimerKind::Entry,
            _,
        ) if *t == target
    )));

    let retry = resolver.on_timer(target, 1, DirectResolutionTimerKind::Entry);
    assert!(retry.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::BroadcastWish(wish)
            if wish.target == target && wish.view == 1
    )));
    assert!(retry.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::ArmTimer(
            t,
            1,
            DirectResolutionTimerKind::Entry,
            _,
        ) if *t == target
    )));

    enter_view(&mut resolver, &names, target, 1);
    assert!(resolver
        .on_timer(target, 1, DirectResolutionTimerKind::Entry)
        .is_empty());
}

#[test]
fn fresh_non_skip_is_backed_only_after_origins_and_a_witness_quorum() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let target = 9;
    let probe = DirectResolver::new(names[0], test_committee(), test_sid(), TEST_DELTA_MS);
    let leader = probe.resolution_leader(target, 1);
    let follower = names.iter().copied().find(|name| *name != leader).unwrap();
    let mut resolver = DirectResolver::new(follower, test_committee(), test_sid(), TEST_DELTA_MS);
    let entry = ResolutionEntry::Core(target, Vec::new(), Vec::new());
    resolver.update_candidates(target, [entry.clone()]);
    enter_view(&mut resolver, &names, target, 1);
    let value = resolver.value_digest(&entry);
    let effects = resolver.on_proposal(DirectResolutionProposal {
        target,
        view: 1,
        key_view: 0,
        value: value.clone(),
        entry: entry.clone(),
        sender: leader,
    });
    assert!(effects
        .iter()
        .any(|effect| matches!(effect, DirectResolutionEffect::ValidateVote { .. })));
    let own = resolver.on_vote(
        target,
        1,
        value.clone(),
        DirectResolutionVote::Accept { origin: Some(0) },
    );
    assert!(own.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::BroadcastStatement(statement)
            if statement.phase == DirectResolutionPhase::Echo
    )));

    let other_senders: Vec<_> = names
        .iter()
        .copied()
        .filter(|sender| *sender != follower)
        .collect();
    let mut backing = Vec::new();
    for (index, sender) in other_senders.iter().copied().take(2).enumerate() {
        backing.extend(resolver.on_statement(DirectResolutionStatement {
            target,
            view: 1,
            value: value.clone(),
            phase: DirectResolutionPhase::Echo,
            origin: (index == 0).then_some(1),
            sender,
        }));
    }
    assert!(!backing
        .iter()
        .any(|effect| matches!(effect, DirectResolutionEffect::BroadcastWitness(_))));

    let last = other_senders[2];
    let effects = resolver.on_statement(DirectResolutionStatement {
        target,
        view: 1,
        value,
        phase: DirectResolutionPhase::Echo,
        origin: Some(1),
        sender: last,
    });
    assert!(effects
        .iter()
        .any(|effect| matches!(effect, DirectResolutionEffect::BroadcastWitness(_))));
    assert!(!resolver.backed_for_test(target, &resolver.value_digest(&entry)));

    let value = resolver.value_digest(&entry);
    let mut key1 = Vec::new();
    for sender in other_senders.iter().copied().take(2) {
        key1.extend(resolver.on_witness(DirectResolutionWitness {
            target,
            view: 1,
            value: value.clone(),
            entry: entry.clone(),
            sender,
        }));
    }
    assert!(resolver.backed_for_test(target, &value));
    assert!(key1.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::BroadcastStatement(statement)
            if statement.phase == DirectResolutionPhase::Key1
    )));
}

#[test]
fn accepting_a_late_proposal_rechecks_an_already_received_echo_quorum() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let target = 10;
    let probe = DirectResolver::new(names[0], test_committee(), test_sid(), TEST_DELTA_MS);
    let leader = probe.resolution_leader(target, 1);
    let follower = names.iter().copied().find(|name| *name != leader).unwrap();
    let mut resolver = DirectResolver::new(follower, test_committee(), test_sid(), TEST_DELTA_MS);
    let entry = ResolutionEntry::Skip(target);
    resolver.update_candidates(target, [entry.clone()]);
    enter_view(&mut resolver, &names, target, 1);
    let value = resolver.value_digest(&entry);

    for sender in names
        .iter()
        .copied()
        .filter(|sender| *sender != follower)
        .take(3)
    {
        resolver.on_statement(DirectResolutionStatement {
            target,
            view: 1,
            value: value.clone(),
            phase: DirectResolutionPhase::Echo,
            origin: None,
            sender,
        });
    }

    let effects = resolver.on_proposal(DirectResolutionProposal {
        target,
        view: 1,
        key_view: 0,
        value: value.clone(),
        entry,
        sender: leader,
    });
    assert!(effects
        .iter()
        .any(|effect| matches!(effect, DirectResolutionEffect::ValidateVote { .. })));

    let effects = resolver.on_vote(
        target,
        1,
        value,
        DirectResolutionVote::Accept { origin: None },
    );
    assert!(effects.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::BroadcastWitness(witness)
            if witness.target == target && witness.view == 1
    )));
}

#[test]
fn late_witness_quorum_makes_a_positive_value_backed_in_a_later_view() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let target = 12;
    let probe = DirectResolver::new(names[0], test_committee(), test_sid(), TEST_DELTA_MS);
    let leader = probe.resolution_leader(target, 2);
    let follower = names.iter().copied().find(|name| *name != leader).unwrap();
    let mut resolver = DirectResolver::new(follower, test_committee(), test_sid(), TEST_DELTA_MS);
    let entry = ResolutionEntry::Skip(target);
    resolver.update_candidates(target, [entry.clone()]);
    enter_view(&mut resolver, &names, target, 2);
    let value = resolver.value_digest(&entry);
    let effects = resolver.on_proposal(DirectResolutionProposal {
        target,
        view: 2,
        key_view: 1,
        value: value.clone(),
        entry: entry.clone(),
        sender: leader,
    });
    assert!(!effects
        .iter()
        .any(|effect| matches!(effect, DirectResolutionEffect::ValidateVote { .. })));

    let mut effects = Vec::new();
    for sender in names.iter().filter(|sender| **sender != follower).take(2) {
        effects.extend(resolver.on_witness(DirectResolutionWitness {
            target,
            view: 1,
            value: value.clone(),
            entry: entry.clone(),
            sender: *sender,
        }));
    }
    assert!(resolver.backed_for_test(target, &value));
    assert!(effects.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::ValidateVote { fresh: false, .. }
    )));
}

#[test]
fn one_sender_cannot_equivocate_within_a_backing_view() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let target = 15;
    let mut resolver = DirectResolver::new(names[0], test_committee(), test_sid(), TEST_DELTA_MS);
    resolver.update_candidates(target, [ResolutionEntry::Skip(target)]);
    enter_view(&mut resolver, &names, target, 1);
    let first_entry = ResolutionEntry::Skip(target);
    let second_entry = ResolutionEntry::Core(target, Vec::new(), Vec::new());
    let first = resolver.value_digest(&first_entry);
    let second = resolver.value_digest(&second_entry);
    resolver.on_witness(DirectResolutionWitness {
        target,
        view: 1,
        value: first,
        entry: first_entry,
        sender: names[1],
    });
    resolver.on_witness(DirectResolutionWitness {
        target,
        view: 1,
        value: second.clone(),
        entry: second_entry.clone(),
        sender: names[1],
    });
    resolver.on_witness(DirectResolutionWitness {
        target,
        view: 1,
        value: second.clone(),
        entry: second_entry,
        sender: names[2],
    });
    assert!(!resolver.backed_for_test(target, &second));
}

#[test]
fn one_member_cannot_allocate_arbitrary_future_resolver_views() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let target = 16;
    let mut resolver = DirectResolver::new(names[0], test_committee(), test_sid(), TEST_DELTA_MS);
    resolver.update_candidates(target, [ResolutionEntry::Skip(target)]);

    let effects = resolver.on_statement(DirectResolutionStatement {
        target,
        view: 1_000_000,
        value: Digest([9; 32]),
        phase: DirectResolutionPhase::Key1,
        origin: None,
        sender: names[1],
    });

    assert!(effects.is_empty());
    assert_eq!(resolver.buffered_views_for_test(target), 0);
}

#[test]
fn unsolicited_resolver_traffic_cannot_allocate_targets() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let target = 1_000_000;
    let mut resolver = DirectResolver::new(names[0], test_committee(), test_sid(), TEST_DELTA_MS);
    let entry = ResolutionEntry::Skip(target);
    let value = resolver.value_digest(&entry);

    assert!(resolver
        .on_wish(DirectResolutionWish {
            target,
            view: 1,
            sender: names[1],
        })
        .is_empty());
    assert!(resolver
        .on_statement(DirectResolutionStatement {
            target,
            view: 1,
            value: value.clone(),
            phase: DirectResolutionPhase::Echo,
            origin: None,
            sender: names[1],
        })
        .is_empty());
    assert!(resolver
        .on_done(crate::vantage::DirectResolutionDone {
            target,
            value,
            entry,
            sender: names[1],
        })
        .is_empty());
    assert_eq!(resolver.active_len(), 0);
    assert_eq!(resolver.passive_target_len_for_test(), 0);
}

#[test]
fn a_later_local_candidate_starts_wish_after_pre_horizon_wishes_were_dropped() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let target = 18;
    let mut resolver = DirectResolver::new(names[0], test_committee(), test_sid(), TEST_DELTA_MS);

    for sender in names.iter().copied().skip(1).take(2) {
        assert!(resolver
            .on_wish(DirectResolutionWish {
                target,
                view: 1,
                sender,
            })
            .is_empty());
    }
    assert_eq!(resolver.passive_target_len_for_test(), 0);

    let effects = resolver.update_candidates(target, [ResolutionEntry::Skip(target)]);
    assert!(effects.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::BroadcastWish(wish)
            if wish.target == target && wish.view == 1 && wish.sender == names[0]
    )));
    assert_eq!(resolver.active_len(), 1);
}

#[test]
fn value_serve_requires_a_pending_digest_and_answers_each_requester_once() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let target = 16;
    let owner = names[0];
    let mut resolver = DirectResolver::new(owner, test_committee(), test_sid(), TEST_DELTA_MS);
    resolver.update_candidates(target, [ResolutionEntry::Skip(target)]);
    enter_view(&mut resolver, &names, target, 1);

    let expected = ResolutionEntry::Core(target, Vec::new(), Vec::new());
    let value = resolver.value_digest(&expected);
    assert!(resolver
        .on_value_serve(DirectResolutionValueServe {
            target,
            value: value.clone(),
            entry: expected.clone(),
        })
        .is_empty());
    assert!(resolver
        .on_value_fetch(DirectResolutionValueFetch {
            target,
            value: value.clone(),
            requester: names[1],
        })
        .is_empty());

    let mut fetches = Vec::new();
    for sender in names.iter().copied().skip(1).take(3) {
        fetches.extend(resolver.on_statement(DirectResolutionStatement {
            target,
            view: 1,
            value: value.clone(),
            phase: DirectResolutionPhase::Echo,
            origin: None,
            sender,
        }));
    }
    assert!(fetches
        .iter()
        .any(|effect| matches!(effect, DirectResolutionEffect::ValueFetchTo(..))));

    assert!(resolver
        .on_value_serve(DirectResolutionValueServe {
            target,
            value: value.clone(),
            entry: ResolutionEntry::Skip(target),
        })
        .is_empty());
    resolver.on_value_serve(DirectResolutionValueServe {
        target,
        value: value.clone(),
        entry: expected,
    });

    let request = |requester| DirectResolutionValueFetch {
        target,
        value: value.clone(),
        requester,
    };
    assert!(resolver
        .on_value_fetch(request(names[1]))
        .iter()
        .any(|effect| matches!(effect, DirectResolutionEffect::ValueServeTo(peer, _) if *peer == names[1])));
    assert!(resolver.on_value_fetch(request(names[1])).is_empty());
    assert!(resolver
        .on_value_fetch(request(names[2]))
        .iter()
        .any(|effect| matches!(effect, DirectResolutionEffect::ValueServeTo(peer, _) if *peer == names[2])));
}

#[test]
fn f_plus_one_wishes_activate_and_enter_with_the_own_relay() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let target = 17;
    let mut resolver = DirectResolver::new(names[0], test_committee(), test_sid(), TEST_DELTA_MS);
    resolver.admit_targets_through(target);
    resolver.on_wish(DirectResolutionWish {
        target,
        view: 1,
        sender: names[1],
    });
    assert_eq!(resolver.active_len(), 0);

    resolver.on_wish(DirectResolutionWish {
        target,
        view: 1,
        sender: names[2],
    });
    assert_eq!(resolver.active_len(), 1);
    assert_eq!(resolver.current_view(target), 1);

    resolver.update_candidates(target, [ResolutionEntry::Skip(target)]);
    assert_eq!(resolver.current_view(target), 1);
}

#[test]
fn a_local_terminal_activates_and_relays_after_one_peer_wish() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let target = 19;
    let mut resolver = DirectResolver::new(names[0], test_committee(), test_sid(), TEST_DELTA_MS);

    let effects = resolver.activate_with_local_terminal(target);
    assert!(effects.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::BroadcastWish(wish)
            if wish.target == target && wish.view == 1 && wish.sender == names[0]
    )));
    assert_eq!(resolver.current_view(target), 0);

    resolver.on_wish(DirectResolutionWish {
        target,
        view: 1,
        sender: names[1],
    });
    assert_eq!(resolver.current_view(target), 0);
    resolver.on_wish(DirectResolutionWish {
        target,
        view: 1,
        sender: names[2],
    });
    assert_eq!(resolver.current_view(target), 1);
}

#[test]
fn a_late_wish_gets_each_own_witness_and_done_once() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let target = 20;
    let follower = names[0];
    let (mut resolver, value) = resolver_with_done(&names, follower, target);

    let requester = names[1];
    let replay = resolver.on_wish(DirectResolutionWish {
        target,
        view: 2,
        sender: requester,
    });
    assert!(replay.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::ProofTo(peer, proof)
            if *peer == requester && proof.sender == follower
    )));
    assert!(replay.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::StatementTo(peer, statement)
            if *peer == requester && statement.sender == follower
    )));
    assert!(replay.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::WitnessTo(peer, witness)
            if *peer == requester && witness.view == 1 && witness.value == value
    )));
    assert!(replay.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::DoneTo(peer, done)
            if *peer == requester && done.value == value
    )));

    let duplicate = resolver.on_wish(DirectResolutionWish {
        target,
        view: 3,
        sender: requester,
    });
    assert!(!duplicate.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::WishTo(..)
            | DirectResolutionEffect::SuggestTo(..)
            | DirectResolutionEffect::ProofTo(..)
            | DirectResolutionEffect::ProposalTo(..)
            | DirectResolutionEffect::StatementTo(..)
            | DirectResolutionEffect::WitnessTo(..)
            | DirectResolutionEffect::DoneTo(..)
    )));
}

#[test]
fn a_late_candidate_recovers_the_wish_quorum_from_join_replies() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let target = 24;
    let mut late = DirectResolver::new(names[3], test_committee(), test_sid(), TEST_DELTA_MS);
    late.update_candidates(target, [ResolutionEntry::Skip(target)]);

    let mut responders = resolvers(&names[..2]);
    for responder in &mut responders {
        responder.update_candidates(target, [ResolutionEntry::Skip(target)]);
        let replies = responder.on_wish(DirectResolutionWish {
            target,
            view: 1,
            sender: names[3],
        });
        let wish = replies.into_iter().find_map(|effect| match effect {
            DirectResolutionEffect::WishTo(peer, wish) if peer == names[3] => Some(wish),
            _ => None,
        });
        late.on_wish(wish.expect("an active peer replays its own WISH watermark"));
    }

    assert_eq!(late.current_view(target), 1);
}

#[test]
fn a_primary_wish_replays_a_suggestion_sent_before_primary_activation() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let target = 25;
    let probe = DirectResolver::new(names[0], test_committee(), test_sid(), TEST_DELTA_MS);
    let primary = probe.resolution_leader(target, 1);
    let sender = names.iter().copied().find(|name| *name != primary).unwrap();
    let mut resolver = DirectResolver::new(sender, test_committee(), test_sid(), TEST_DELTA_MS);
    resolver.update_candidates(target, [ResolutionEntry::Skip(target)]);

    let other_non_primary: Vec<_> = names
        .iter()
        .copied()
        .filter(|name| *name != sender && *name != primary)
        .collect();
    let mut entry_effects = Vec::new();
    for peer in other_non_primary {
        entry_effects.extend(resolver.on_wish(DirectResolutionWish {
            target,
            view: 1,
            sender: peer,
        }));
    }
    assert!(entry_effects.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::SuggestTo(peer, suggest)
            if *peer == primary && suggest.sender == sender
    )));

    let replay = resolver.on_wish(DirectResolutionWish {
        target,
        view: 1,
        sender: primary,
    });
    assert!(replay.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::SuggestTo(peer, suggest)
            if *peer == primary && suggest.sender == sender
    )));
}

#[test]
fn a_primary_replays_its_proposal_to_a_late_wish_sender() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let target = 26;
    let probe = DirectResolver::new(names[0], test_committee(), test_sid(), TEST_DELTA_MS);
    let primary = probe.resolution_leader(target, 1);
    let requester = names.iter().copied().find(|name| *name != primary).unwrap();
    let mut resolver = DirectResolver::new(primary, test_committee(), test_sid(), TEST_DELTA_MS);
    let entry = ResolutionEntry::Skip(target);
    resolver.update_candidates(target, [entry.clone()]);
    enter_view(&mut resolver, &names, target, 1);

    let mut proposal_effects = Vec::new();
    for sender in names
        .iter()
        .copied()
        .filter(|sender| *sender != primary)
        .take(2)
    {
        proposal_effects.extend(resolver.on_suggest(DirectResolutionSuggest {
            target,
            view: 1,
            sender,
            key3_view: 0,
            key3_value: Digest::default(),
            key2_view: 0,
            key2_value: Digest::default(),
            prev_key2: 0,
            entry: None,
        }));
    }
    assert!(proposal_effects.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::BroadcastProposal(proposal) if proposal.entry == entry
    )));

    let replay = resolver.on_wish(DirectResolutionWish {
        target,
        view: 2,
        sender: requester,
    });
    assert!(replay.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::ProposalTo(peer, proposal)
            if *peer == requester && proposal.entry == entry
    )));
}

#[test]
fn a_done_replay_stays_deduplicated_after_the_responder_decides() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let target = 23;
    let requester = names[3];
    let entry = ResolutionEntry::Skip(target);
    let (mut resolver, value) = resolver_with_done(&names, names[0], target);

    let first = resolver.on_wish(DirectResolutionWish {
        target,
        view: 2,
        sender: requester,
    });
    assert!(first.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::DoneTo(peer, done)
            if *peer == requester && done.value == value
    )));

    for sender in [names[1], names[2]] {
        resolver.on_done(crate::vantage::DirectResolutionDone {
            target,
            value: value.clone(),
            entry: entry.clone(),
            sender,
        });
    }
    assert!(resolver.is_decided(target));

    let after_decision = resolver.on_wish(DirectResolutionWish {
        target,
        view: 3,
        sender: requester,
    });
    assert!(!after_decision
        .iter()
        .any(|effect| matches!(effect, DirectResolutionEffect::DoneTo(..))));
}

#[test]
fn gc_drops_only_target_state_below_the_retained_floor() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let mut resolver = DirectResolver::new(names[0], test_committee(), test_sid(), TEST_DELTA_MS);
    for target in 20..23 {
        resolver.update_candidates(target, [ResolutionEntry::Skip(target)]);
    }

    resolver.gc_below(22);

    assert_eq!(resolver.active_len(), 1);
    assert_eq!(resolver.current_view(20), 0);
    assert_eq!(resolver.current_view(21), 0);
    assert_eq!(resolver.current_view(22), 0);

    assert!(resolver
        .update_candidates(21, [ResolutionEntry::Skip(21)])
        .is_empty());
    assert_eq!(resolver.active_len(), 1);
}

fn lock_value_in_view_one(
    names: &[PublicKey],
    follower: PublicKey,
    target: View,
    entry: ResolutionEntry,
) -> (DirectResolver, Digest) {
    let mut resolver = DirectResolver::new(follower, test_committee(), test_sid(), TEST_DELTA_MS);
    resolver.update_candidates(target, [entry.clone()]);
    enter_view(&mut resolver, names, target, 1);
    let leader = resolver.resolution_leader(target, 1);
    let value = resolver.value_digest(&entry);
    let proposal_effects = resolver.on_proposal(DirectResolutionProposal {
        target,
        view: 1,
        key_view: 0,
        value: value.clone(),
        entry: entry.clone(),
        sender: leader,
    });
    assert!(proposal_effects
        .iter()
        .any(|effect| matches!(effect, DirectResolutionEffect::ValidateVote { .. })));
    resolver.on_vote(
        target,
        1,
        value.clone(),
        DirectResolutionVote::Accept { origin: None },
    );

    let mut emitted = Vec::new();
    for sender in names
        .iter()
        .copied()
        .filter(|sender| *sender != follower)
        .take(2)
    {
        emitted.extend(resolver.on_statement(DirectResolutionStatement {
            target,
            view: 1,
            value: value.clone(),
            phase: DirectResolutionPhase::Echo,
            origin: None,
            sender,
        }));
    }
    assert!(emitted
        .iter()
        .any(|effect| matches!(effect, DirectResolutionEffect::BroadcastWitness(_))));

    let mut emitted = Vec::new();
    for sender in names
        .iter()
        .copied()
        .filter(|sender| *sender != follower)
        .take(2)
    {
        emitted.extend(resolver.on_witness(DirectResolutionWitness {
            target,
            view: 1,
            value: value.clone(),
            entry: entry.clone(),
            sender,
        }));
    }
    assert!(emitted.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::BroadcastStatement(statement)
            if statement.phase == DirectResolutionPhase::Key1
                && statement.value == value
    )));

    for phase in [
        DirectResolutionPhase::Key1,
        DirectResolutionPhase::Key2,
        DirectResolutionPhase::Key3,
    ] {
        let mut emitted = Vec::new();
        for sender in names
            .iter()
            .copied()
            .filter(|sender| *sender != follower)
            .take(2)
        {
            emitted.extend(resolver.on_statement(DirectResolutionStatement {
                target,
                view: 1,
                value: value.clone(),
                phase,
                origin: None,
                sender,
            }));
        }
        let next = match phase {
            DirectResolutionPhase::Echo => DirectResolutionPhase::Key1,
            DirectResolutionPhase::Key1 => DirectResolutionPhase::Key2,
            DirectResolutionPhase::Key2 => DirectResolutionPhase::Key3,
            DirectResolutionPhase::Key3 => DirectResolutionPhase::Lock,
            DirectResolutionPhase::Lock => unreachable!(),
        };
        assert!(emitted.iter().any(|effect| matches!(
            effect,
            DirectResolutionEffect::BroadcastStatement(statement)
                if statement.phase == next && statement.value == value
        )));
    }
    (resolver, value)
}

fn resolver_with_done(
    names: &[PublicKey],
    follower: PublicKey,
    target: View,
) -> (DirectResolver, Digest) {
    let (mut resolver, value) =
        lock_value_in_view_one(names, follower, target, ResolutionEntry::Skip(target));
    let mut effects = Vec::new();
    for sender in names
        .iter()
        .copied()
        .filter(|sender| *sender != follower)
        .take(2)
    {
        effects.extend(resolver.on_statement(DirectResolutionStatement {
            target,
            view: 1,
            value: value.clone(),
            phase: DirectResolutionPhase::Lock,
            origin: None,
            sender,
        }));
    }
    assert!(effects
        .iter()
        .any(|effect| matches!(effect, DirectResolutionEffect::BroadcastDone(_))));
    (resolver, value)
}

#[test]
fn wish_replay_recovers_a_party_that_missed_backing_and_done_before_activation() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let target = 22;
    let mut responders = [
        resolver_with_done(&names, names[0], target).0,
        resolver_with_done(&names, names[1], target).0,
    ];
    let mut late = DirectResolver::new(names[3], test_committee(), test_sid(), TEST_DELTA_MS);
    late.admit_targets_through(target);

    late.on_wish(DirectResolutionWish {
        target,
        view: 1,
        sender: names[0],
    });
    late.on_wish(DirectResolutionWish {
        target,
        view: 1,
        sender: names[1],
    });
    assert_eq!(late.current_view(target), 1);
    late.on_timer(target, 1, DirectResolutionTimerKind::View);

    let mut replies = Vec::new();
    for responder in &mut responders {
        replies.extend(responder.on_wish(DirectResolutionWish {
            target,
            view: 2,
            sender: names[3],
        }));
    }
    for effect in &replies {
        if let DirectResolutionEffect::WitnessTo(peer, witness) = effect {
            assert_eq!(*peer, names[3]);
            late.on_witness(witness.clone());
        }
    }
    let mut terminal_effects = Vec::new();
    for effect in replies {
        if let DirectResolutionEffect::DoneTo(peer, done) = effect {
            assert_eq!(peer, names[3]);
            terminal_effects.extend(late.on_done(done));
        }
    }
    assert!(terminal_effects
        .iter()
        .any(|effect| matches!(effect, DirectResolutionEffect::Decide(_))));
    assert!(late.is_decided(target));
}

#[test]
fn view_change_carries_the_locked_value_and_rejects_a_fresh_conflict() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let target = 21;
    let probe = DirectResolver::new(names[0], test_committee(), test_sid(), TEST_DELTA_MS);
    let leaders = [
        probe.resolution_leader(target, 1),
        probe.resolution_leader(target, 2),
    ];
    let follower = names
        .iter()
        .copied()
        .find(|name| !leaders.contains(name))
        .unwrap();
    let locked_entry = ResolutionEntry::Skip(target);

    let (mut carries, locked_value) =
        lock_value_in_view_one(&names, follower, target, locked_entry.clone());
    enter_view(&mut carries, &names, target, 2);
    let carry_effects = carries.on_proposal(DirectResolutionProposal {
        target,
        view: 2,
        key_view: 1,
        value: locked_value,
        entry: locked_entry,
        sender: leaders[1],
    });
    assert!(carry_effects.iter().any(|effect| matches!(
        effect,
        DirectResolutionEffect::ValidateVote { fresh: false, .. }
    )));

    let (mut rejects, _) =
        lock_value_in_view_one(&names, follower, target, ResolutionEntry::Skip(target));
    enter_view(&mut rejects, &names, target, 2);
    let conflict = ResolutionEntry::Core(target, Vec::new(), Vec::new());
    let conflict_value = rejects.value_digest(&conflict);
    let conflict_effects = rejects.on_proposal(DirectResolutionProposal {
        target,
        view: 2,
        key_view: 0,
        value: conflict_value,
        entry: conflict,
        sender: leaders[1],
    });
    assert!(!conflict_effects
        .iter()
        .any(|effect| matches!(effect, DirectResolutionEffect::ValidateVote { .. })));
}

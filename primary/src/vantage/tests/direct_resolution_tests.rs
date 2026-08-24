use super::common::*;
use crate::primary::View;
use crate::vantage::{
    DirectResolutionEffect, DirectResolutionPhase, DirectResolutionProposal,
    DirectResolutionStatement, DirectResolutionTimerKind, DirectResolutionVote,
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
            DirectResolutionEffect::BroadcastProposal(message) => {
                for (node, resolver) in resolvers.iter_mut().enumerate() {
                    if node != origin {
                        enqueue(node, resolver.on_proposal(message.clone()));
                    }
                }
            }
            DirectResolutionEffect::BroadcastStatement(message) => {
                for (node, resolver) in resolvers.iter_mut().enumerate() {
                    if node != origin {
                        enqueue(node, resolver.on_statement(message.clone()));
                    }
                }
            }
            DirectResolutionEffect::BroadcastWitness(message) => {
                for (node, resolver) in resolvers.iter_mut().enumerate() {
                    if node != origin {
                        enqueue(node, resolver.on_witness(message.clone()));
                    }
                }
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
}

#[test]
fn an_external_terminal_seal_cancels_only_that_target_instance() {
    let names: Vec<_> = authors().into_iter().map(|(name, _)| name).collect();
    let mut resolver = DirectResolver::new(names[0], test_committee(), test_sid(), TEST_DELTA_MS);
    resolver.update_candidates(17, [ResolutionEntry::Skip(17)]);
    resolver.update_candidates(18, [ResolutionEntry::Skip(18)]);
    assert_eq!(resolver.active_len(), 2);

    resolver.note_terminal(17);

    assert_eq!(resolver.active_len(), 1);
    assert_eq!(resolver.current_view(17), 0);
    assert!(!resolver.is_decided(17));

    let stale = resolver.on_wish(DirectResolutionWish {
        target: 17,
        view: 2,
        sender: names[1],
    });
    assert!(stale.is_empty());
    assert_eq!(resolver.current_view(17), 0);
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

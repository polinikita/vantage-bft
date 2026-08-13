#![allow(clippy::needless_range_loop)]

use super::common::*;
use super::harness::{
    advance_time, boot, boot_without_control, deliver_only_to, drain_local, run_to_quiescence,
    start_control, Node,
};
use crate::messages::Header;
use crate::primary::View;
use crate::vantage::agb::{
    self, Echo, EchoOut, Outcome, ProposalOut, ReadyGrade, ReadyOut, ResolutionEntry, ViewProposal,
};
use crate::vantage::control::{ControlLog, ControlProposal};
use crate::vantage::node::Inbound;
use crate::vantage::Effect;
use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

const MAX_VIEWS: crate::primary::View = 12;
const MAX_VIEWS_NO_ORGANIC: crate::primary::View = 1;

async fn drive_control_rounds(
    nodes: &mut [Node],
    outbox: &mut VecDeque<(usize, Inbound)>,
    now: Instant,
    rounds: usize,
) {
    let control_timeout = nodes
        .iter()
        .find(|n| n.alive)
        .unwrap()
        .control
        .control_round_timeout();
    let mut ct = now;
    for _ in 0..rounds {
        ct += control_timeout + Duration::from_millis(1);
        advance_time(nodes, outbox, ct).await;
        run_to_quiescence(nodes, outbox, ct).await;
    }
}

fn resolve_carrying_entry(
    nodes: &mut [Node],
    carrying_view: View,
    target_view: View,
) -> (usize, ResolutionEntry) {
    let carrier_name = agb::proposer(&test_committee(), carrying_view);
    let carrier_idx = nodes.iter().position(|n| n.name == carrier_name).unwrap();
    let now = Instant::now();
    let first = {
        let node = &mut nodes[carrier_idx];
        let agb = &node.agb;
        let control = &node.control;
        node.resolver.decide(agb, carrying_view, now, |u| {
            agb.is_sealed(u) || control.is_anchor_resolved(u)
        })
    };
    assert_eq!(first, None, "the next-turn bit starts data-only");
    let m = {
        let node = &mut nodes[carrier_idx];
        let agb = &node.agb;
        let control = &node.control;
        node.resolver.decide(agb, carrying_view, now, |u| {
            agb.is_sealed(u) || control.is_anchor_resolved(u)
        })
    };
    let entry = m.expect("the target view must be justified for recovery");
    assert_eq!(entry.target_view(), target_view);
    (carrier_idx, entry)
}

async fn drive_carrying_proposal_to_anchor(
    nodes: &mut [Node],
    outbox: &mut VecDeque<(usize, Inbound)>,
    now: Instant,
    carrying_view: View,
    proposal: ViewProposal,
) {
    let everyone: Vec<usize> = (0..nodes.len()).collect();
    for i in 0..nodes.len() {
        let effects = nodes[i].enter_view_effects(carrying_view, now);
        drain_local(nodes, i, effects, now, outbox);
    }
    run_to_quiescence(nodes, outbox, now).await;
    deliver_only_to(
        nodes,
        outbox,
        &everyone,
        Inbound::Propose(ProposalOut::Single(proposal)),
    );
    run_to_quiescence(nodes, outbox, now).await;
    start_control(nodes, now, outbox).await;
    drive_control_rounds(nodes, outbox, now, 6).await;
}

#[tokio::test]
async fn scenario_1_silent_proposer_sealed_via_grounded_skip_vote() {
    let all = authors();
    let mut nodes: Vec<Node> = all
        .iter()
        .enumerate()
        .map(|(i, (pk, _))| {
            Node::new(
                *pk,
                &format!(".db_test_byz_scenario1_node_{}", i),
                MAX_VIEWS,
            )
        })
        .collect();
    let now = Instant::now();
    let mut outbox: VecDeque<(usize, Inbound)> = VecDeque::new();

    for i in 0..nodes.len() {
        let (_, effects) = nodes[i]
            .lm
            .publish_own(std::collections::BTreeMap::new())
            .await;
        drain_local(&mut nodes, i, effects, now, &mut outbox);
    }
    run_to_quiescence(&mut nodes, &mut outbox, now).await;

    let dead_view: crate::primary::View = 2;
    let dead_name = crate::vantage::agb::proposer(&test_committee(), dead_view);
    let dead_idx = nodes.iter().position(|n| n.name == dead_name).unwrap();
    nodes[dead_idx].alive = false;
    let live: Vec<usize> = (0..nodes.len()).filter(|&i| i != dead_idx).collect();
    assert_eq!(
        live.len(),
        3,
        "n=4, f=1 -- exactly 2f+1=3 correct parties remain"
    );

    boot(&mut nodes, now, &mut outbox).await;

    let theta_echo = nodes[live[0]].agb.theta_echo();
    let theta_ready = nodes[live[0]].agb.theta_ready();
    let entry_instant = now;

    advance_time(
        &mut nodes,
        &mut outbox,
        entry_instant + theta_echo + Duration::from_millis(1),
    )
    .await;
    advance_time(
        &mut nodes,
        &mut outbox,
        entry_instant + theta_ready + Duration::from_millis(1),
    )
    .await;

    for &i in &live {
        assert!(
            nodes[i].agb.noready_count(dead_view) >= 3,
            "D6-5: every live party's first-hand no-ready is counted (2f+1=3)"
        );
    }

    for &i in &live {
        assert_eq!(
            nodes[i].agb.sealed_for_test(dead_view),
            Some(Outcome::Skip),
            "node {} must have sealed gskip for the dead view via the grounded skip-vote quorum",
            i
        );
        assert!(
            !nodes[i].control.is_anchor_resolved(dead_view),
            "node {} must NOT have anchored the dead view -- the vote quorum sealed it directly",
            i
        );
        assert!(
            nodes[i].cursor.next_view() > dead_view,
            "node {} cursor must have advanced past the dead view",
            i
        );
        assert!(
            nodes[i]
                .metrics
                .vantage_seals
                .with_label_values(&["vote_skip"])
                .get()
                >= 1,
            "node {} must show at least one vote_skip route increment",
            i
        );
        assert_eq!(
            nodes[i]
                .metrics
                .vantage_seals
                .with_label_values(&["anchor_skip"])
                .get(),
            0,
            "node {} must show zero anchor_skip route increments -- the anchor never ran",
            i
        );
    }

    let carrying_view: crate::primary::View = 1000;
    let carrier_name = crate::vantage::agb::proposer(&test_committee(), carrying_view);
    let carrier_idx = live
        .iter()
        .find(|&&i| nodes[i].name == carrier_name)
        .copied()
        .expect("a live party must lead the carrying view");
    let now = Instant::now();
    let first = {
        let node = &mut nodes[carrier_idx];
        let agb = &node.agb;
        node.resolver
            .decide(agb, carrying_view, now, |u| agb.is_sealed(u))
    };
    assert_eq!(first, None, "the next-turn bit starts data-only");
    let m = {
        let node = &mut nodes[carrier_idx];
        let agb = &node.agb;
        node.resolver
            .decide(agb, carrying_view, now, |u| agb.is_sealed(u))
    };
    assert_eq!(
        m, None,
        "the dead view is already sealed via the vote quorum -- no carrying proposal is needed"
    );

    let reference = nodes[live[0]].cursor.output_log().to_vec();
    for &i in &live[1..] {
        assert_eq!(
            nodes[i].cursor.output_log(),
            reference.as_slice(),
            "node {} output log must match node {}",
            i,
            live[0]
        );
    }
}

#[tokio::test]
async fn scenario_2_withheld_tip_author_mixed_grades_resolved_via_anchor() {
    let all = authors();
    let mut nodes: Vec<Node> = all
        .iter()
        .enumerate()
        .map(|(i, (pk, _))| {
            Node::new(
                *pk,
                &format!(".db_test_byz_scenario2_node_{}", i),
                MAX_VIEWS_NO_ORGANIC,
            )
        })
        .collect();
    let now = Instant::now();
    let mut outbox: VecDeque<(usize, Inbound)> = VecDeque::new();

    for i in 0..nodes.len() {
        let (_, effects) = nodes[i].lm.publish_own(BTreeMap::new()).await;
        drain_local(&mut nodes, i, effects, now, &mut outbox);
    }
    run_to_quiescence(&mut nodes, &mut outbox, now).await;
    boot_without_control(&mut nodes, now, &mut outbox).await;

    let view: View = 2;
    let (tip_author, _) = all[1];
    let sid = nodes[0].lm.sid().clone();
    let parent1 = nodes[0]
        .lm
        .c_candidate(&tip_author)
        .expect("seeded C candidate")
        .2;
    let tip = Header::new_vantage(tip_author, 2, BTreeMap::new(), parent1, sid);
    let tip_holders = [0usize, 1usize];
    let core_only_holders = [2usize, 3usize];
    deliver_only_to(
        &nodes,
        &mut outbox,
        &tip_holders,
        Inbound::Publish(tip_author, tip.clone()),
    );
    run_to_quiescence(&mut nodes, &mut outbox, now).await;

    let c_ref = nodes[0]
        .lm
        .c_candidate(&tip_author)
        .expect("seeded C candidate");
    let t_ref = (tip_author, tip.height, tip.id.clone());
    let proposal_full = ViewProposal {
        view,
        c: vec![c_ref.clone()],
        t: vec![t_ref.clone()],
        m: None,
    };
    let proposal_core = ViewProposal {
        view,
        c: vec![c_ref],
        t: Vec::new(),
        m: None,
    };
    assert_ne!(
        proposal_full.digest(&nodes[0].lm.sid().clone()),
        proposal_core.digest(&nodes[0].lm.sid().clone()),
        "the two proposals (differing only in whether T is attached) must be genuinely distinct"
    );
    deliver_only_to(
        &nodes,
        &mut outbox,
        &tip_holders,
        Inbound::Propose(ProposalOut::Single(proposal_full)),
    );
    deliver_only_to(
        &nodes,
        &mut outbox,
        &core_only_holders,
        Inbound::Propose(ProposalOut::Single(proposal_core)),
    );
    run_to_quiescence(&mut nodes, &mut outbox, now).await;

    let theta_ready = nodes[0].agb.theta_ready();
    advance_time(
        &mut nodes,
        &mut outbox,
        now + theta_ready + Duration::from_millis(1),
    )
    .await;

    for i in 0..nodes.len() {
        assert!(nodes[i].agb.sealed_for_test(view).is_none(), "node {} must not seal view {} directly -- quorum intersection forbids either digest from reaching quorum alone", i, view);
        assert!(
            nodes[i].agb.completed_for_test(view).is_none(),
            "node {} must not even complete view {} -- neither digest's readies reach quorum",
            i,
            view
        );
        assert!(
            nodes[i].agb.noready_count(view) >= 3,
            "node {} must have the full 2f+1 no-ready census for view {}",
            i,
            view
        );
    }

    let carrying_view: View = 1000;
    let (carrier_idx, entry) = resolve_carrying_entry(&mut nodes, carrying_view, view);
    let entry_carries_tip = match &entry {
        ResolutionEntry::Full(_, _, t) | ResolutionEntry::Core(_, _, t) => {
            t.iter().any(|r| r.2 == tip.id)
        }
        ResolutionEntry::Skip(_) => false,
    };

    let (author0, _) = all[0];
    let carrying_c = nodes[carrier_idx]
        .lm
        .c_candidate(&author0)
        .expect("seeded C candidate");
    let carrying_proposal = ViewProposal {
        view: carrying_view,
        c: vec![carrying_c],
        t: Vec::new(),
        m: Some(entry),
    };
    drive_carrying_proposal_to_anchor(
        &mut nodes,
        &mut outbox,
        now,
        carrying_view,
        carrying_proposal,
    )
    .await;

    let reference_outcome = nodes[0].agb.sealed_for_test(view);
    assert!(
        reference_outcome.is_some(),
        "view {} must be sealed via the anchor",
        view
    );
    for i in 0..nodes.len() {
        assert_eq!(
            nodes[i].agb.sealed_for_test(view),
            reference_outcome,
            "node {} must seal the IDENTICAL outcome for view {}",
            i,
            view
        );
        assert!(
            nodes[i].cursor.next_view() > view,
            "node {} cursor must advance past view {}",
            i,
            view
        );
    }
    let reference_log = nodes[0].cursor.output_log().to_vec();
    for i in 1..nodes.len() {
        assert_eq!(
            nodes[i].cursor.output_log(),
            reference_log.as_slice(),
            "node {} output log must match node 0 (core-prefix property + identical resolution)",
            i
        );
    }
    if entry_carries_tip {
        for i in 0..nodes.len() {
            assert!(
                nodes[i].cursor.output_log().contains(&tip.id),
                "node {} output must contain the tip (repaired via the anchor)",
                i
            );
        }
    }

    for i in 0..nodes.len() {
        let m = &nodes[i].metrics.vantage_seals;
        let anchor_full = m.with_label_values(&["anchor_full"]).get();
        let anchor_core = m.with_label_values(&["anchor_core"]).get();
        assert_eq!(anchor_full + anchor_core, 1, "node {} must show exactly one anchor_full/anchor_core route increment for this mixed-grade view", i);
    }
}

#[tokio::test]
async fn scenario_3_equivocating_leader_disjoint_halves_resolution_settles_it() {
    let all = authors();
    let mut nodes: Vec<Node> = all
        .iter()
        .enumerate()
        .map(|(i, (pk, _))| {
            Node::new(
                *pk,
                &format!(".db_test_byz_scenario3_node_{}", i),
                MAX_VIEWS_NO_ORGANIC,
            )
        })
        .collect();
    let now = Instant::now();
    let mut outbox: VecDeque<(usize, Inbound)> = VecDeque::new();

    for i in 0..nodes.len() {
        let (_, effects) = nodes[i].lm.publish_own(BTreeMap::new()).await;
        drain_local(&mut nodes, i, effects, now, &mut outbox);
    }
    run_to_quiescence(&mut nodes, &mut outbox, now).await;
    boot_without_control(&mut nodes, now, &mut outbox).await;

    let view: View = 2;
    let (author0, _) = all[0];
    let (author1, _) = all[1];
    let x_ref = nodes[0]
        .lm
        .c_candidate(&author0)
        .expect("seeded C candidate");
    let y_ref = nodes[0]
        .lm
        .c_candidate(&author1)
        .expect("seeded C candidate");
    let proposal_x = ViewProposal {
        view,
        c: vec![x_ref],
        t: Vec::new(),
        m: None,
    };
    let proposal_y = ViewProposal {
        view,
        c: vec![y_ref],
        t: Vec::new(),
        m: None,
    };
    assert_ne!(
        proposal_x.digest(&nodes[0].lm.sid().clone()),
        proposal_y.digest(&nodes[0].lm.sid().clone()),
        "the two equivocated proposals must be genuinely distinct"
    );

    let half_a = [0usize, 1usize];
    let half_b = [2usize, 3usize];
    deliver_only_to(
        &nodes,
        &mut outbox,
        &half_a,
        Inbound::Propose(ProposalOut::Single(proposal_x)),
    );
    deliver_only_to(
        &nodes,
        &mut outbox,
        &half_b,
        Inbound::Propose(ProposalOut::Single(proposal_y)),
    );
    run_to_quiescence(&mut nodes, &mut outbox, now).await;

    let theta_ready = nodes[0].agb.theta_ready();
    advance_time(
        &mut nodes,
        &mut outbox,
        now + theta_ready + Duration::from_millis(1),
    )
    .await;

    for i in 0..nodes.len() {
        assert!(nodes[i].agb.sealed_for_test(view).is_none(), "node {} must not seal view {} directly -- quorum intersection forbids either digest from reaching quorum alone", i, view);
        assert!(nodes[i].agb.completed_for_test(view).is_none(), "node {} must not even complete view {} -- neither digest's readies reach quorum, so no ready quorum of ANY digest forms", i, view);
        assert!(
            nodes[i].agb.noready_count(view) >= 3,
            "node {} must have the full 2f+1 no-ready census for view {}",
            i,
            view
        );
    }

    let carrying_view: View = 1000;
    let (carrier_idx, entry) = resolve_carrying_entry(&mut nodes, carrying_view, view);

    let (author2, _) = all[2];
    let carrying_c = nodes[carrier_idx]
        .lm
        .c_candidate(&author2)
        .expect("seeded C candidate");
    let carrying_proposal = ViewProposal {
        view: carrying_view,
        c: vec![carrying_c],
        t: Vec::new(),
        m: Some(entry),
    };
    drive_carrying_proposal_to_anchor(
        &mut nodes,
        &mut outbox,
        now,
        carrying_view,
        carrying_proposal,
    )
    .await;

    let reference_outcome = nodes[0].agb.sealed_for_test(view);
    assert!(
        reference_outcome.is_some(),
        "view {} must be sealed via the anchor",
        view
    );
    for i in 0..nodes.len() {
        assert_eq!(
            nodes[i].agb.sealed_for_test(view),
            reference_outcome,
            "node {} must seal the IDENTICAL outcome for view {} -- no two nodes may diverge",
            i,
            view
        );
        assert!(
            nodes[i].cursor.next_view() > view,
            "node {} cursor must advance past view {}",
            i,
            view
        );
    }
    let reference_log = nodes[0].cursor.output_log().to_vec();
    for i in 1..nodes.len() {
        assert_eq!(
            nodes[i].cursor.output_log(),
            reference_log.as_slice(),
            "node {} output log must match node 0",
            i
        );
    }
}

#[tokio::test]
async fn scenario_4_forked_author_chain_kept_branch_wins_identical_outputs() {
    let all = authors();
    let mut nodes: Vec<Node> = all
        .iter()
        .enumerate()
        .map(|(i, (pk, _))| {
            Node::new(
                *pk,
                &format!(".db_test_byz_scenario4_node_{}", i),
                MAX_VIEWS_NO_ORGANIC,
            )
        })
        .collect();
    let now = Instant::now();
    let mut outbox: VecDeque<(usize, Inbound)> = VecDeque::new();

    for i in 0..nodes.len() {
        let (_, effects) = nodes[i].lm.publish_own(BTreeMap::new()).await;
        drain_local(&mut nodes, i, effects, now, &mut outbox);
    }
    run_to_quiescence(&mut nodes, &mut outbox, now).await;
    boot_without_control(&mut nodes, now, &mut outbox).await;

    let view: View = 2;
    let (fork_author, _) = all[3];
    let sid = nodes[0].lm.sid().clone();
    let parent1 = nodes[0]
        .lm
        .c_candidate(&fork_author)
        .expect("seeded C candidate")
        .2;
    let x2 = tagged_header(fork_author, 2, parent1.clone(), sid.clone(), 0xA0);
    let y2 = tagged_header(fork_author, 2, parent1, sid, 0xB0);
    assert_ne!(
        x2.id, y2.id,
        "the two forked children must be genuinely distinct"
    );

    let x_holders = [0usize, 1usize];
    let y_holders = [2usize, 3usize];
    deliver_only_to(
        &nodes,
        &mut outbox,
        &x_holders,
        Inbound::Publish(fork_author, x2.clone()),
    );
    deliver_only_to(
        &nodes,
        &mut outbox,
        &y_holders,
        Inbound::Publish(fork_author, y2.clone()),
    );
    run_to_quiescence(&mut nodes, &mut outbox, now).await;
    for &i in &x_holders {
        nodes[i].lm.set_payload_ready(&x2.id);
    }
    for &i in &y_holders {
        nodes[i].lm.set_payload_ready(&y2.id);
    }

    let c_ref = nodes[0]
        .lm
        .c_candidate(&fork_author)
        .expect("seeded C candidate");
    let t_x = (fork_author, x2.height, x2.id.clone());
    let t_y = (fork_author, y2.height, y2.id.clone());
    let proposal_x = ViewProposal {
        view,
        c: vec![c_ref.clone()],
        t: vec![t_x],
        m: None,
    };
    let proposal_y = ViewProposal {
        view,
        c: vec![c_ref],
        t: vec![t_y],
        m: None,
    };
    assert_ne!(
        proposal_x.digest(&nodes[0].lm.sid().clone()),
        proposal_y.digest(&nodes[0].lm.sid().clone()),
        "the two per-branch proposals must be genuinely distinct"
    );
    deliver_only_to(
        &nodes,
        &mut outbox,
        &x_holders,
        Inbound::Propose(ProposalOut::Single(proposal_x)),
    );
    deliver_only_to(
        &nodes,
        &mut outbox,
        &y_holders,
        Inbound::Propose(ProposalOut::Single(proposal_y)),
    );
    run_to_quiescence(&mut nodes, &mut outbox, now).await;

    let theta_ready = nodes[0].agb.theta_ready();
    advance_time(
        &mut nodes,
        &mut outbox,
        now + theta_ready + Duration::from_millis(1),
    )
    .await;

    for i in 0..nodes.len() {
        assert!(nodes[i].agb.sealed_for_test(view).is_none(), "node {} must not seal view {} directly -- quorum intersection forbids either branch from reaching quorum alone", i, view);
        assert!(
            nodes[i].agb.completed_for_test(view).is_none(),
            "node {} must not even complete view {} -- neither branch's readies reach quorum",
            i,
            view
        );
        assert!(
            nodes[i].agb.noready_count(view) >= 3,
            "node {} must have the full 2f+1 no-ready census for view {}",
            i,
            view
        );
    }

    let carrying_view: View = 1000;
    let (carrier_idx, entry) = resolve_carrying_entry(&mut nodes, carrying_view, view);
    let winner_is_x = match &entry {
        ResolutionEntry::Full(_, _, t) | ResolutionEntry::Core(_, _, t) => {
            let has_x = t.iter().any(|r| r.2 == x2.id);
            let has_y = t.iter().any(|r| r.2 == y2.id);
            assert!(has_x ^ has_y, "the winning entry must carry EXACTLY ONE branch, never both/neither -- got T = {:?}", t);
            has_x
        }
        ResolutionEntry::Skip(_) => panic!("this view has real, justified Full/Core evidence for both branches -- Skip should never win canonical order here"),
    };
    let (losing_branch_id, kept_branch_id) = if winner_is_x {
        (y2.id.clone(), x2.id.clone())
    } else {
        (x2.id.clone(), y2.id.clone())
    };

    let (author0, _) = all[0];
    let carrying_c = nodes[carrier_idx]
        .lm
        .c_candidate(&author0)
        .expect("seeded C candidate");
    let carrying_proposal = ViewProposal {
        view: carrying_view,
        c: vec![carrying_c],
        t: Vec::new(),
        m: Some(entry),
    };
    drive_carrying_proposal_to_anchor(
        &mut nodes,
        &mut outbox,
        now,
        carrying_view,
        carrying_proposal,
    )
    .await;

    let reference_outcome = nodes[0].agb.sealed_for_test(view);
    assert!(
        reference_outcome.is_some(),
        "view {} must be sealed via the anchor",
        view
    );
    for i in 0..nodes.len() {
        assert_eq!(
            nodes[i].agb.sealed_for_test(view),
            reference_outcome,
            "node {} must seal the IDENTICAL outcome for view {}",
            i,
            view
        );
        assert!(
            nodes[i].cursor.next_view() > view,
            "node {} cursor must advance past view {}",
            i,
            view
        );
    }
    let reference_log = nodes[0].cursor.output_log().to_vec();
    for i in 1..nodes.len() {
        assert_eq!(
            nodes[i].cursor.output_log(),
            reference_log.as_slice(),
            "node {} output log must match node 0 -- including the losing branch's holders, who must have repaired the kept branch via the anchor",
            i
        );
    }
    for i in 0..nodes.len() {
        assert!(
            nodes[i].cursor.output_log().contains(&kept_branch_id),
            "node {} output must contain the kept branch",
            i
        );
        assert!(
            !nodes[i].cursor.output_log().contains(&losing_branch_id),
            "node {} output must NEVER contain the orphaned/losing fork branch",
            i
        );
    }
}

fn drain_control(
    controls: &mut [ControlLog],
    names: &[crypto::PublicKey],
    initial: Vec<(usize, Effect)>,
) {
    let n = controls.len();
    let mut queue: VecDeque<(usize, Effect)> = initial.into();
    while let Some((origin, effect)) = queue.pop_front() {
        match effect {
            Effect::BroadcastControlEcho(p) => {
                for j in 0..n {
                    if j != origin {
                        let out = controls[j].on_control_echo(names[origin], p.clone());
                        queue.extend(out.into_iter().map(|e| (j, e)));
                    }
                }
            }
            Effect::BroadcastControlReady(p) => {
                for j in 0..n {
                    if j != origin {
                        let out = controls[j].on_control_ready(names[origin], p.clone());
                        queue.extend(out.into_iter().map(|e| (j, e)));
                    }
                }
            }
            Effect::ControlFetchTo(peer, w, h) => {
                if let Some(j) = names.iter().position(|nm| *nm == peer) {
                    let out = controls[j].on_control_fetch(names[origin], w, h);
                    queue.extend(out.into_iter().map(|e| (j, e)));
                }
            }
            Effect::ControlServeTo(peer, w, proposal) => {
                if let Some(j) = names.iter().position(|nm| *nm == peer) {
                    let out = controls[j].on_control_serve(w, proposal);
                    queue.extend(out.into_iter().map(|e| (j, e)));
                }
            }
            _ => {} // This scenario keeps `curr_round` at zero.
        }
    }
}

#[tokio::test]
async fn scenario_5_byzantine_control_leader_totality_via_fetch_and_invalid_pair_unreachable() {
    let all = authors();
    let names: Vec<crypto::PublicKey> = all.iter().map(|(pk, _)| *pk).collect();
    let sid = test_sid();

    let mut controls: Vec<ControlLog> = names
        .iter()
        .map(|pk| ControlLog::new(*pk, test_committee(), sid.clone(), TEST_DELTA_MS))
        .collect();
    let b_w = ProposalOut::Single(ViewProposal {
        view: 4,
        c: Vec::new(),
        t: Vec::new(),
        m: Some(ResolutionEntry::Skip(1)),
    });
    let digest = b_w.digest(&sid);
    let leader = controls[0].control_leader(1);

    for i in 0..3 {
        controls[i].on_completion_reportable(4, b_w.clone());
    }
    for i in 0..3 {
        for j in 0..3 {
            controls[i].on_comp_report(4, digest.clone(), names[j]);
        }
    }

    let proposal1 = ControlProposal {
        round: 1,
        parent: 0,
        value: Some((4, digest.clone())),
    };
    let mut initial: Vec<(usize, Effect)> = Vec::new();
    for i in 0..4 {
        let b_w_variant = if i < 3 { Some(b_w.clone()) } else { None }; // Party 3 lacks B_w.
        let effects = controls[i].on_control_init(leader, proposal1.clone(), b_w_variant);
        let echoed = effects
            .iter()
            .any(|e| matches!(e, Effect::BroadcastControlEcho(_)));
        if i < 3 {
            assert!(
                echoed,
                "party {} legitimately holds reports + B_w -- must ECHO immediately",
                i
            );
        } else {
            assert!(
                !echoed,
                "party 3 lacks B_w entirely -- must NOT ECHO (validity gate blocks it)"
            );
        }
        initial.extend(effects.into_iter().map(|e| (i, e)));
    }

    drain_control(&mut controls, &names, initial);

    assert!(
        controls[3].holds_block_for_test(4),
        "party 3 must have obtained B_w via fetch (totality), despite never validating it directly"
    );
    for i in 0..4 {
        assert!(
            controls[i].is_safe_for_test(1),
            "round 1 must be marked safe for every party once delivered (parent=0 is always safe)"
        );
    }

    let mut controls_b: Vec<ControlLog> = names
        .iter()
        .map(|pk| ControlLog::new(*pk, test_committee(), sid.clone(), TEST_DELTA_MS))
        .collect();
    let bogus_digest = crypto::Digest([0xEEu8; 32]);
    let bogus_view: View = 99;
    let leader_b = controls_b[0].control_leader(1);
    let proposal_bogus = ControlProposal {
        round: 1,
        parent: 0,
        value: Some((bogus_view, bogus_digest.clone())),
    };
    let mut any_echo = false;
    for i in 0..4 {
        assert_eq!(
            controls_b[i].report_count_for(bogus_view, &bogus_digest),
            0,
            "no legitimate reports exist anywhere for the fictional pair"
        );
        let effects = controls_b[i].on_control_init(leader_b, proposal_bogus.clone(), None);
        any_echo |= effects
            .iter()
            .any(|e| matches!(e, Effect::BroadcastControlEcho(_)));
    }
    assert!(!any_echo, "an invalid pair with no real backing must never be validated by anyone -- no 2f+1 ECHOs, ever");
    for i in 0..4 {
        assert!(
            !controls_b[i].is_safe_for_test(1),
            "round 1 must never become safe for the invalid pair"
        );
        assert!(
            controls_b[i].delivered_log_for_test().is_empty(),
            "nothing may ever be delivered for the invalid pair"
        );
    }
}

#[tokio::test]
async fn scenario_6_fast_lock_release_unblocks_metaok_no_stale_lock_at_ready_time() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (author_w, _) = authors()[1];
    let other_a = author_c;
    let other_b = author_w;
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_byz_scenario6");
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
    let sender_u = proposer_of(1);
    let effects0 = agb.on_propose(sender_u, proposal_u.clone(), now, &mut lm, &mut rep);
    assert!(
        effects0
            .iter()
            .any(|e| matches!(e, Effect::BroadcastEcho(_))),
        "our own positive gate must fire for view 1"
    );
    assert_eq!(agb.lock_active_for_test(1), Some(true));

    let chain_w = direct_chain(&mut lm, author_w, 1).await;
    let c_w = block_ref(&chain_w[0]);
    agb.enter(4, now, &mut lm, &mut rep);
    let m = Some(ResolutionEntry::Core(1, vec![c_ref], Vec::new()));
    let proposal_w = ViewProposal {
        view: 4,
        c: vec![c_w],
        t: Vec::new(),
        m,
    };
    let sender_w = proposer_of(4);
    let effects_w = agb.on_propose(sender_w, proposal_w.clone(), now, &mut lm, &mut rep);
    assert!(
        !effects_w.iter().any(|e| matches!(e, Effect::BroadcastEcho(_))),
        "the active lock must reject this non-matching Core entry -- MetaOK blocks the carrying view's echo"
    );

    let effects1 = agb.on_echo(
        Echo {
            proposal: proposal_u.clone(),
            grade: 0,
            sender: other_a,
            wish: 0,
            origin: None,
            avail: None,
        },
        &mut rep,
    );
    assert_eq!(
        agb.lock_active_for_test(1),
        Some(true),
        "one nonmatching echo (< f+1=2) must not yet release the lock"
    );
    assert!(
        !effects1
            .iter()
            .any(|e| matches!(e, Effect::BroadcastReady(_))),
        "g1+g0 = 1+1 = 2 is still below the 2f+1=3 ready quorum"
    );

    let effects2 = agb.on_echo(
        Echo {
            proposal: proposal_u.clone(),
            grade: 0,
            sender: other_b,
            wish: 0,
            origin: None,
            avail: None,
        },
        &mut rep,
    );
    assert_eq!(
        agb.lock_active_for_test(1),
        Some(false),
        "the SECOND nonmatching echo must have released the lock"
    );
    let ready = effects2.iter().find_map(|e| match e {
        Effect::BroadcastReady(ReadyOut::Single(r)) if r.proposal.view == 1 => Some(r.grade),
        _ => None,
    });
    assert_eq!(
        ready,
        Some(ReadyGrade::Mix),
        "the split quorum must preserve the immediate READY-mix completion path"
    );

    let effects3 = agb.recheck_all(&mut lm, &mut rep);
    assert!(
        !effects3.iter().any(
            |e| matches!(e, Effect::BroadcastEcho(EchoOut::Single(echo)) if echo.proposal.view == 4)
        ),
        "lock release alone is not an R_i(1); the carrier remains pending until READY is homogeneous"
    );

    let (last_sender, _) = authors()[2];
    let effects4 = agb.on_echo(
        Echo {
            proposal: proposal_u,
            grade: 0,
            sender: last_sender,
            wish: 0,
            origin: None,
            avail: None,
        },
        &mut rep,
    );
    let ready = effects4.iter().find_map(|e| match e {
        Effect::BroadcastReady(ReadyOut::Single(r)) if r.proposal.view == 1 => Some(r.grade),
        _ => None,
    });
    assert_eq!(
        ready,
        Some(ReadyGrade::Zero),
        "the final echo makes grade zero homogeneous, so READY-0 must win over the earlier split"
    );
    let effects5 = agb.recheck_all(&mut lm, &mut rep);
    assert!(
        effects5.iter().any(
            |e| matches!(e, Effect::BroadcastEcho(EchoOut::Single(echo)) if echo.proposal.view == 4)
        ),
        "the new READY-0 must unblock the carrying Core entry on the normal recheck pass"
    );
}

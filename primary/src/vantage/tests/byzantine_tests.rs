#![allow(clippy::needless_range_loop)]

use super::common::*;
use super::harness::{
    advance_time, boot, boot_without_resolution, deliver_only_to, drain_local, run_to_quiescence,
    Node,
};
use crate::messages::Header;
use crate::primary::View;
use crate::vantage::agb::{
    Echo, EchoOut, Outcome, ProposalOut, ReadyGrade, ReadyOut, ResolutionEntry, ViewProposal,
};
use crate::vantage::node::Inbound;
use crate::vantage::Effect;
use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

const MAX_VIEWS: crate::primary::View = 12;
const MAX_VIEWS_NO_ORGANIC: crate::primary::View = 1;

async fn drive_resolution_views(
    nodes: &mut [Node],
    outbox: &mut VecDeque<(usize, Inbound)>,
    now: Instant,
    rounds: usize,
) {
    let resolution_timeout = nodes
        .iter()
        .find(|n| n.alive)
        .unwrap()
        .direct_resolver
        .resolver_timeout();
    let mut ct = now;
    for _ in 0..rounds {
        ct += resolution_timeout + Duration::from_millis(1);
        advance_time(nodes, outbox, ct).await;
        run_to_quiescence(nodes, outbox, ct).await;
    }
}

async fn resolve_target(
    nodes: &mut [Node],
    outbox: &mut VecDeque<(usize, Inbound)>,
    now: Instant,
    target_view: View,
) {
    for i in 0..nodes.len() {
        let effects = nodes[i].refresh_direct_resolution(target_view + 3);
        drain_local(nodes, i, effects, now, outbox);
    }
    run_to_quiescence(nodes, outbox, now).await;
    if nodes
        .iter()
        .filter(|node| node.alive)
        .any(|node| node.agb.sealed_for_test(target_view).is_none())
    {
        drive_resolution_views(nodes, outbox, now, 6).await;
    }
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
                .with_label_values(&["resolver_skip"])
                .get(),
            0,
            "node {} must show zero resolver_skip increments -- the vote path won",
            i
        );
    }

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
async fn scenario_2_withheld_tip_author_mixed_grades_resolved_by_target_agreement() {
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
    boot_without_resolution(&mut nodes, now, &mut outbox).await;

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

    resolve_target(&mut nodes, &mut outbox, now, view).await;

    let reference_outcome = nodes[0].agb.sealed_for_test(view);
    let entry_carries_tip = matches!(
        &reference_outcome,
        Some(Outcome::Full(_, tip_manifest)) if tip_manifest.iter().any(|r| r.2 == tip.id)
    );
    assert!(
        reference_outcome.is_some(),
        "view {} must be sealed by the direct resolver",
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
                "node {} output must contain the tip after resolver-driven repair",
                i
            );
        }
    }

    for i in 0..nodes.len() {
        let m = &nodes[i].metrics.vantage_seals;
        let resolver_full = m.with_label_values(&["resolver_full"]).get();
        let resolver_core = m.with_label_values(&["resolver_core"]).get();
        assert_eq!(resolver_full + resolver_core, 1, "node {} must show exactly one resolver_full/resolver_core increment for this mixed-grade view", i);
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
    boot_without_resolution(&mut nodes, now, &mut outbox).await;

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

    resolve_target(&mut nodes, &mut outbox, now, view).await;

    let reference_outcome = nodes[0].agb.sealed_for_test(view);
    assert!(
        reference_outcome.is_some(),
        "view {} must be sealed by the direct resolver",
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
    boot_without_resolution(&mut nodes, now, &mut outbox).await;

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

    resolve_target(&mut nodes, &mut outbox, now, view).await;
    let reference_outcome = nodes[0].agb.sealed_for_test(view);
    let winner_is_x = match &reference_outcome {
        Some(Outcome::Full(_, t)) => {
            let has_x = t.iter().any(|r| r.2 == x2.id);
            let has_y = t.iter().any(|r| r.2 == y2.id);
            assert!(has_x ^ has_y, "the winning entry must carry EXACTLY ONE branch, never both/neither -- got T = {:?}", t);
            has_x
        }
        other => panic!("justified Full evidence exists for both branches; got {other:?}"),
    };
    let (losing_branch_id, kept_branch_id) = if winner_is_x {
        (y2.id.clone(), x2.id.clone())
    } else {
        (x2.id.clone(), y2.id.clone())
    };

    assert!(
        reference_outcome.is_some(),
        "view {} must be sealed by the direct resolver",
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
            "node {} output log must match node 0 after resolver-driven repair",
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

use super::common::*;
use super::harness::{advance_time, boot, drain_local, run_to_quiescence, Node};
use crate::vantage::agb::Outcome;
use crate::vantage::node::Inbound;
use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

const MAX_VIEWS: crate::primary::View = 8;

#[tokio::test]
async fn crash_fault_dead_proposer_view_seals_via_grounded_skip_vote() {
    let all = authors();
    let mut nodes: Vec<Node> = all
        .iter()
        .enumerate()
        .map(|(i, (pk, _))| Node::new(*pk, &format!(".db_test_crash_fault_node_{}", i), MAX_VIEWS))
        .collect();
    let now = Instant::now();
    let mut outbox: VecDeque<(usize, Inbound)> = VecDeque::new();

    for i in 0..nodes.len() {
        let (_, effects) = nodes[i].lm.publish_own(BTreeMap::new()).await;
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
        "n=4, f=1 -- exactly 2f+1=3 correct parties remain, the tight case"
    );

    boot(&mut nodes, now, &mut outbox).await;

    for &i in &live {
        assert!(
            nodes[i].frontier.is_active(dead_view),
            "node {} must have formally entered the dead view via WISH",
            i
        );
    }

    let theta_echo = nodes[live[0]].agb.theta_echo();
    let theta_ready = nodes[live[0]].agb.theta_ready();
    let entry_instant = now;

    advance_time(
        &mut nodes,
        &mut outbox,
        entry_instant + theta_echo + Duration::from_millis(1),
    )
    .await;
    for &i in &live {
        assert_eq!(
            nodes[i].agb.sealed_for_test(dead_view),
            None,
            "the dead view must never seal before every live party's own no-ready exists"
        );
    }

    advance_time(
        &mut nodes,
        &mut outbox,
        entry_instant + theta_ready + Duration::from_millis(1),
    )
    .await;

    for &i in &live {
        assert!(
            nodes[i].frontier.is_active(dead_view + 1),
            "node {} must have entered past the dead view",
            i
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
    }

    let carrying_view: crate::primary::View = 1000;
    let carrier_name = crate::vantage::agb::proposer(&test_committee(), carrying_view);
    let carrier_idx = live
        .iter()
        .find(|&&i| nodes[i].name == carrier_name)
        .copied()
        .expect("a live party must lead the carrying view");
    let m = {
        let node = &mut nodes[carrier_idx];
        let agb = &node.agb;
        let control = &node.control;
        let resolved = |u: crate::primary::View| agb.is_sealed(u) || control.is_anchor_resolved(u);
        node.resolver
            .decide(agb, carrying_view, entry_instant, resolved);
        node.resolver
            .decide(agb, carrying_view, entry_instant, resolved)
    };
    assert_eq!(
        m, None,
        "the dead view is already sealed via the vote quorum -- no carrying proposal is needed"
    );

    run_to_quiescence(
        &mut nodes,
        &mut outbox,
        entry_instant + theta_ready + Duration::from_millis(1),
    )
    .await;

    for &i in &live {
        assert!(
            nodes[i].cursor.next_view() > dead_view,
            "node {} cursor must have advanced past the dead view via the vote-sealed skip",
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

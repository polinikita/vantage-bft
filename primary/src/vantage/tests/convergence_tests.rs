use super::common::*;
use super::harness::{advance_time, boot, drain_local, run_to_quiescence, Node};
use crate::vantage::node::Inbound;
use std::collections::{BTreeMap, VecDeque};
use std::time::Instant;

const MAX_VIEWS: crate::primary::View = 10;

#[tokio::test]
async fn convergence_partitioned_party_enters_all_missed_views_on_release() {
    let all = authors();
    let mut nodes: Vec<Node> = all
        .iter()
        .enumerate()
        .map(|(i, (pk, _))| Node::new(*pk, &format!(".db_test_convergence_node_{}", i), MAX_VIEWS))
        .collect();
    let now = Instant::now();
    let mut outbox: VecDeque<(usize, Inbound)> = VecDeque::new();

    for i in 0..nodes.len() {
        let (_, effects) = nodes[i].lm.publish_own(BTreeMap::new()).await;
        drain_local(&mut nodes, i, effects, now, &mut outbox);
    }
    run_to_quiescence(&mut nodes, &mut outbox, now).await;

    boot(&mut nodes, now, &mut outbox).await;

    let p = 2; // Partition an arbitrary party.
    let others: Vec<usize> = (0..nodes.len()).filter(|&i| i != p).collect();
    let target_at_partition = nodes[p].pacemaker.entry_target();
    for &i in &others {
        assert_eq!(
            nodes[i].pacemaker.entry_target(),
            target_at_partition,
            "every party starts in lock-step right after genesis"
        );
    }

    nodes[p].wish_partitioned = true;

    let theta_ready = nodes[others[0]].agb.theta_ready();
    for step in 1..=5u32 {
        advance_time(&mut nodes, &mut outbox, now + theta_ready * step).await;
    }

    let max_other_target = others
        .iter()
        .map(|&i| nodes[i].pacemaker.entry_target())
        .max()
        .unwrap();
    assert_eq!(
        nodes[p].pacemaker.entry_target(),
        target_at_partition,
        "no wish may be absorbed while partitioned"
    );
    assert!(
        max_other_target > target_at_partition,
        "test setup: the other parties must have genuinely progressed"
    );
    assert!(
        !nodes[p].held_wishes.is_empty(),
        "wishes must have accumulated, held, during the partition"
    );

    let release_now = now + theta_ready * 5;
    let effects = nodes[p].release_wishes();
    drain_local(&mut nodes, p, effects, release_now, &mut outbox);
    run_to_quiescence(&mut nodes, &mut outbox, release_now).await;

    assert!(
        nodes[p].pacemaker.entry_target() >= max_other_target,
        "the partitioned party must have caught up to (at least) the others' target on release"
    );
    for v in 1..=max_other_target {
        assert!(
            nodes[p].frontier.is_active(v),
            "node {} must have entered every missed view {} after release, in order",
            p,
            v
        );
    }
    assert!(nodes[p].held_wishes.is_empty());
    assert!(!nodes[p].wish_partitioned);
}

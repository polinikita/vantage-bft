// PHASE5-SPEC.md §4 -- convergence integration: delay one party's inbound wishes
// (partition), release -- it enters all missed views in order and rejoins (entries
// converge; the 2delta bound is the lemma's own, asserted here only qualitatively:
// entry happens within the test's release window).

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

    // Seed round, as in the happy-path integration test.
    for i in 0..nodes.len() {
        let (_, effects) = nodes[i].lm.publish_own(BTreeMap::new()).await;
        drain_local(&mut nodes, i, effects, now, &mut outbox);
    }
    run_to_quiescence(&mut nodes, &mut outbox, now).await;

    // No crash in this test -- every party boots normally, so genesis's wish(2)
    // trivially reaches all n=4 (>= 2f+1=3) parties, including the one about to be
    // partitioned.
    boot(&mut nodes, now, &mut outbox).await;

    let p = 2; // the party to be partitioned (arbitrary index)
    let others: Vec<usize> = (0..nodes.len()).filter(|&i| i != p).collect();
    let target_at_partition = nodes[p].pacemaker.entry_target();
    for &i in &others {
        assert_eq!(
            nodes[i].pacemaker.entry_target(),
            target_at_partition,
            "every party starts in lock-step right after genesis"
        );
    }

    // Delay party p's inbound wishes from here on (piggybacked and standalone alike)
    // -- the underlying responses it receives are still processed normally either
    // way (only the wish sub-channel is held back).
    nodes[p].wish_partitioned = true;

    // Let the other 3 parties keep progressing on their own (2f+1=3 of them is
    // already enough to cross every WISH threshold without p's participation at all)
    // -- several theta_R windows' worth of continued echo/ready/no-ready traffic among
    // just the non-partitioned parties.
    let theta_ready = nodes[others[0]].agb.theta_ready();
    for step in 1..=5u32 {
        advance_time(&mut nodes, &mut outbox, now + theta_ready * step).await;
    }

    // Party p must have fallen behind: its own entry target is unchanged (no wish was
    // ever absorbed while partitioned), strictly below at least one live party's, and
    // it must have accumulated held wishes to replay later.
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

    // Release: replay every held wish, in arrival order -- p must enter every missed
    // view, in order, and rejoin (its entry target converges to at least what the
    // other parties had reached by the time of release).
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
    // No wishes are left un-replayed.
    assert!(nodes[p].held_wishes.is_empty());
    assert!(!nodes[p].wish_partitioned);
}

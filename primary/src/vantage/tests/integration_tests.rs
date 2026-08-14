use super::common::*;
use super::harness::{avail_tick, boot, drain_local, run_to_quiescence, Node};
use crate::vantage::node::Inbound;
use std::collections::{BTreeMap, VecDeque};
use std::time::Instant;

const MAX_VIEWS: crate::primary::View = 6;

#[tokio::test]
async fn four_party_happy_path_three_consecutive_views_identical_output() {
    let all = authors();
    let mut nodes: Vec<Node> = all
        .iter()
        .enumerate()
        .map(|(i, (pk, _))| Node::new(*pk, &format!(".db_test_integration_node_{}", i), MAX_VIEWS))
        .collect();
    let now = Instant::now();
    let mut outbox: VecDeque<(usize, Inbound)> = VecDeque::new();

    for i in 0..nodes.len() {
        let (_, effects) = nodes[i].lm.publish_own(BTreeMap::new()).await;
        drain_local(&mut nodes, i, effects, now, &mut outbox);
    }
    run_to_quiescence(&mut nodes, &mut outbox, now).await;

    boot(&mut nodes, now, &mut outbox).await;

    for (i, node) in nodes.iter().enumerate() {
        assert!(
            node.cursor.next_view() >= 4,
            "node {} only reached view {}",
            i,
            node.cursor.next_view()
        );
    }

    let reference = nodes[0].cursor.output_log().to_vec();
    for (i, node) in nodes.iter().enumerate().skip(1) {
        assert_eq!(
            node.cursor.output_log(),
            reference.as_slice(),
            "node {} output log diverged from node 0",
            i
        );
    }

    assert!(!reference.is_empty());

    let fast_full: u64 = nodes[0]
        .metrics
        .vantage_seals
        .with_label_values(&["fast_full"])
        .get();
    let direct_full: u64 = nodes[0]
        .metrics
        .vantage_seals
        .with_label_values(&["direct_full"])
        .get();
    assert!(
        fast_full > 0,
        "the happy path must seal at least one view via fast_full"
    );
    assert!(
        fast_full >= direct_full,
        "the happy path must be dominated by fast_full, not direct_full (fast_full={}, direct_full={})",
        fast_full,
        direct_full
    );
}

#[tokio::test]
async fn four_party_happy_path_identical_output_with_echo_availability_claims() {
    let all = authors();
    let mut nodes: Vec<Node> = all
        .iter()
        .enumerate()
        .map(|(i, (pk, _))| {
            Node::new(
                *pk,
                &format!(".db_test_integration_node_avail_{}", i),
                MAX_VIEWS,
            )
            .with_echo_avail_claims(true)
        })
        .collect();
    let now = Instant::now();
    let mut outbox: VecDeque<(usize, Inbound)> = VecDeque::new();

    for i in 0..nodes.len() {
        let (_, effects) = nodes[i].lm.publish_own(BTreeMap::new()).await;
        drain_local(&mut nodes, i, effects, now, &mut outbox);
    }
    run_to_quiescence(&mut nodes, &mut outbox, now).await;
    boot(&mut nodes, now, &mut outbox).await;

    for (i, node) in nodes.iter().enumerate() {
        assert!(
            node.cursor.next_view() >= 4,
            "node {} only reached view {}",
            i,
            node.cursor.next_view()
        );
    }

    let reference = nodes[0].cursor.output_log().to_vec();
    for (i, node) in nodes.iter().enumerate().skip(1) {
        assert_eq!(
            node.cursor.output_log(),
            reference.as_slice(),
            "node {} output log diverged from node 0",
            i
        );
    }
    assert!(!reference.is_empty());

    let quorum = test_committee().quorum_threshold();
    for (author, _) in &all {
        let refs = nodes[0].lm.blocks_handle().lock().author_refs(author);
        assert_eq!(refs.len(), 1, "exactly one seeded block per author");
        assert!(
            nodes[0].lm.is_q_available(&refs[0], quorum),
            "author {:?}'s seeded block must be quorum-available via proposal ECHO claims",
            author
        );
    }
}

#[tokio::test]
async fn four_party_happy_path_identical_output_with_periodic_availability_watermarks() {
    let all = authors();
    let mut nodes: Vec<Node> = all
        .iter()
        .enumerate()
        .map(|(i, (pk, _))| {
            Node::new(
                *pk,
                &format!(".db_test_integration_node_watermark_{}", i),
                MAX_VIEWS,
            )
            .with_ack_watermarks(true)
        })
        .collect();
    let now = Instant::now();
    let mut outbox: VecDeque<(usize, Inbound)> = VecDeque::new();

    for i in 0..nodes.len() {
        let (_, effects) = nodes[i].lm.publish_own(BTreeMap::new()).await;
        drain_local(&mut nodes, i, effects, now, &mut outbox);
    }
    run_to_quiescence(&mut nodes, &mut outbox, now).await;
    avail_tick(&mut nodes, now, &mut outbox).await;
    boot(&mut nodes, now, &mut outbox).await;

    let reference = nodes[0].cursor.output_log().to_vec();
    assert!(!reference.is_empty());
    for (i, node) in nodes.iter().enumerate() {
        assert!(
            node.cursor.next_view() >= 4,
            "node {} only reached view {}",
            i,
            node.cursor.next_view()
        );
        assert_eq!(node.cursor.output_log(), reference.as_slice());
    }
}

#[tokio::test]
async fn four_party_happy_path_identical_output_with_digest_statements() {
    let all = authors();
    let mut nodes: Vec<Node> = all
        .iter()
        .enumerate()
        .map(|(i, (pk, _))| {
            Node::new(
                *pk,
                &format!(".db_test_integration_node_digest_{}", i),
                MAX_VIEWS,
            )
            .with_digest_statements(true)
        })
        .collect();
    let now = Instant::now();
    let mut outbox: VecDeque<(usize, Inbound)> = VecDeque::new();

    for i in 0..nodes.len() {
        let (_, effects) = nodes[i].lm.publish_own(BTreeMap::new()).await;
        drain_local(&mut nodes, i, effects, now, &mut outbox);
    }
    run_to_quiescence(&mut nodes, &mut outbox, now).await;

    boot(&mut nodes, now, &mut outbox).await;

    for (i, node) in nodes.iter().enumerate() {
        assert!(
            node.cursor.next_view() >= 4,
            "node {} only reached view {}",
            i,
            node.cursor.next_view()
        );
    }

    let reference = nodes[0].cursor.output_log().to_vec();
    for (i, node) in nodes.iter().enumerate().skip(1) {
        assert_eq!(
            node.cursor.output_log(),
            reference.as_slice(),
            "node {} output log diverged from node 0 under digest statements",
            i
        );
    }
    assert!(!reference.is_empty());

    for (i, node) in nodes.iter().enumerate() {
        assert_eq!(
            node.metrics.vantage_body_fetches_sent.get(),
            0,
            "node {} issued a body fetch in the all-correct favorable path",
            i
        );
        assert_eq!(
            node.metrics.vantage_bodies_served.get(),
            0,
            "node {} served a body in the all-correct favorable path",
            i
        );
    }
}

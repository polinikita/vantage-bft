// PHASE4-SPEC.md §12 "Integration" -- 4 in-proc engines wired via the shared
// `harness` module (the same cross-component dispatch `vantage::node::VantageCore`
// performs against a real network/timer runtime -- here driven synchronously, with no
// real sockets or sleeps, so the test is fast and deterministic; the real
// network/timer wiring itself is covered by the local-benchmark gate run,
// PHASE4-NOTES.md/PHASE5-SPEC.md §4).

use super::common::*;
use super::harness::{avail_tick, boot, drain_local, run_to_quiescence, Node};
use crate::vantage::node::Inbound;
use std::collections::{BTreeMap, VecDeque};
use std::time::Instant;

/// See `harness::Node::max_views`'s doc comment.
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

    // Seed round: each party publishes one empty-payload height-1 block of its own
    // lane, broadcast to everyone -- gives every party's N5 registers a real,
    // quorum-acked `c_candidate` for all four authors before any AGB view proposes.
    for i in 0..nodes.len() {
        let (_, effects) = nodes[i].lm.publish_own(BTreeMap::new()).await;
        drain_local(&mut nodes, i, effects, now, &mut outbox);
    }
    run_to_quiescence(&mut nodes, &mut outbox, now).await;

    // Genesis bootstrap (§4/W1): every party enters view 1, the WISH pacemaker sets
    // its own wish to 2, and (if it is proposer(1)) it proposes.
    boot(&mut nodes, now, &mut outbox).await;

    // Every party must have advanced its cursor past at least 3 consecutive views.
    for (i, node) in nodes.iter().enumerate() {
        assert!(
            node.cursor.next_view() >= 4,
            "node {} only reached view {}",
            i,
            node.cursor.next_view()
        );
    }

    // Output logs must be byte-identical across all four parties (deterministic
    // linearization, §9).
    let reference = nodes[0].cursor.output_log().to_vec();
    for (i, node) in nodes.iter().enumerate().skip(1) {
        assert_eq!(
            node.cursor.output_log(),
            reference.as_slice(),
            "node {} output log diverged from node 0",
            i
        );
    }

    // The seeded height-1 blocks (the only real content in this test) must actually
    // have been committed at least once.
    assert!(!reference.is_empty());

    // PHASE6-SPEC.md §9 gate amendment: the happy path (every party correct, every
    // echo matching) should be overwhelmingly sealed via the all-n unanimous fast
    // seal, not merely the (also-correct, but slower) direct grade-1 quorum path.
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

/// Ack-watermarks (optional, flag-gated -- `Parameters::ack_watermarks`) end-to-end:
/// the identical four-party happy path, but with per-block ack broadcasts suppressed
/// and replaced by a periodic watermark flush (`harness::avail_tick`, the test-only
/// substitute for `VantageCore::run`'s production tick). Committed output must still
/// be byte-identical across all four parties -- the watermark front-end and the
/// per-block-ack front-end are indistinguishable below the shared `AckAggregator`.
#[tokio::test]
async fn four_party_happy_path_identical_output_with_ack_watermarks() {
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
            .with_ack_watermarks(true)
        })
        .collect();
    let now = Instant::now();
    let mut outbox: VecDeque<(usize, Inbound)> = VecDeque::new();

    // Seed round, exactly as the plain happy-path test -- but with `ack_watermarks`
    // on, `drain_local`'s `Effect::BroadcastAck` fan-out is suppressed, so every
    // node's own DirectPub-derived acks never reach its peers via a direct `Ack`
    // message. `avail_tick` substitutes the periodic watermark broadcast a real
    // `VantageCore::run` would schedule, letting every node's own N5 registers reach
    // the same quorum-acked state as the plain test.
    for i in 0..nodes.len() {
        let (_, effects) = nodes[i].lm.publish_own(BTreeMap::new()).await;
        drain_local(&mut nodes, i, effects, now, &mut outbox);
    }
    run_to_quiescence(&mut nodes, &mut outbox, now).await;
    avail_tick(&mut nodes, now, &mut outbox).await;

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

    // Availability actually flowed through the watermark front-end, not just
    // silently through some other path (e.g. repair) -- every seeded height-1 ref
    // must have reached 2f+1 (quorum) via `resolve_watermark`'s crediting.
    let quorum = test_committee().quorum_threshold();
    for (author, _) in &all {
        let refs = nodes[0].lm.blocks_handle().lock().author_refs(author);
        assert_eq!(refs.len(), 1, "exactly one seeded block per author");
        assert!(
            nodes[0].lm.is_q_available(&refs[0], quorum),
            "author {:?}'s seeded block must be quorum-available via the watermark front-end",
            author
        );
    }
}

/// Digest-named AGB statements (optional, flag-gated -- `Parameters::
/// digest_statements`, signature-free.tex §8.3): the identical four-party happy path,
/// but with every ECHO/READY travelling digest-named instead of by value. Committed
/// output must still be byte-identical to the by-value run, AND -- the paragraph's own
/// favorable-path claim ("when the proposer is correct, every party already has the
/// body, so this encoding adds no reconstruction delay") -- zero body fetches/serves
/// should ever be needed, since every party directly receives `VantagePropose` before
/// any ECHO/READY for that view can possibly arrive.
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

    // Favorable path: nobody should ever have needed to fetch or serve a body.
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

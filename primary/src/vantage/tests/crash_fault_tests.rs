// PHASE5-SPEC.md §4 -- crash-fault integration (in-proc, 4 engines, injected clocks):
// kill proposer(v); confirm correct parties still formally enter v via wishes,
// echo-skip at theta_E, no-ready at theta_R, and enter v+1 and beyond (lemma (a)'s
// inductive step, observable); later views with live proposers still complete and seal
// normally; the output cursor TRANSIENTLY blocks at the dead view (never advances past
// it on its own, at the AGB layer alone).
//
// PHASE6-SPEC.md §9 gate amendment: updated. The transient block above was Phase 5's
// own documented boundary (entry/wish liveness continues past a dead proposer, output
// liveness did not, yet) -- Phase 6's resolver + control log + anchor adapter close it.
// This test now drives that closure the rest of the way (the identical
// `Resolver::decide` -> carrying-proposal -> control-log-anchor pipeline
// `byzantine_tests.rs`'s scenario 1 exercises, since this crash-fault scenario is
// exactly scenario 1's own setup) and asserts the cursor advances past the dead view --
// i.e. the pre-resolver blocking behavior asserted above is NOT the final state; it is
// superseded once resolution runs. Both checkpoints are kept in one test so the
// "before" and "after" of this phase's fix are both visible in the same place.

use super::common::*;
use super::harness::{advance_time, boot, drain_local, run_to_quiescence, Node};
use crate::vantage::agb::{Outcome, ResolutionEntry, ViewProposal};
use crate::vantage::node::Inbound;
use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

const MAX_VIEWS: crate::primary::View = 8;

#[tokio::test]
async fn crash_fault_dead_proposer_view_blocks_output_but_entry_and_later_views_proceed() {
    let all = authors();
    let mut nodes: Vec<Node> = all
        .iter()
        .enumerate()
        .map(|(i, (pk, _))| Node::new(*pk, &format!(".db_test_crash_fault_node_{}", i), MAX_VIEWS))
        .collect();
    let now = Instant::now();
    let mut outbox: VecDeque<(usize, Inbound)> = VecDeque::new();

    // Seed round (as in the happy-path integration test): every party publishes one
    // empty-payload height-1 block first, so every party's N5 registers have a real,
    // quorum-acked C candidate for all four authors before any AGB view proposes.
    for i in 0..nodes.len() {
        let (_, effects) = nodes[i].lm.publish_own(BTreeMap::new()).await;
        drain_local(&mut nodes, i, effects, now, &mut outbox);
    }
    run_to_quiescence(&mut nodes, &mut outbox, now).await;

    // Kill proposer(2) *before* boot -- it never enters view 1, never sends its own
    // genesis wish(2), and never proposes anything, ever.
    let dead_view: crate::primary::View = 2;
    let dead_name = crate::vantage::agb::proposer(&test_committee(), dead_view);
    let dead_idx = nodes.iter().position(|n| n.name == dead_name).unwrap();
    nodes[dead_idx].alive = false;
    let live: Vec<usize> = (0..nodes.len()).filter(|&i| i != dead_idx).collect();
    assert_eq!(live.len(), 3, "n=4, f=1 -- exactly 2f+1=3 correct parties remain, the tight case");

    boot(&mut nodes, now, &mut outbox).await;

    // 2f+1=3 correct parties booting is exactly enough for the genesis wish(2) to
    // reach quorum -- view 2 must be formally entered by every live party even though
    // its proposer never proposes anything at all (W1/W2, entry via wishes alone).
    for &i in &live {
        assert!(nodes[i].frontier.is_active(dead_view), "node {} must have formally entered the dead view via WISH", i);
    }

    let theta_echo = nodes[live[0]].agb.theta_echo();
    let theta_ready = nodes[live[0]].agb.theta_ready();
    // Entry to view 2 happened synchronously during `boot`'s single quiescence pass,
    // so its timers were armed relative to `now` itself.
    let entry_instant = now;

    // Advance to theta_E(2): no proposal ever arrived (`fixed` stays Unset forever)
    // -- the absolute deadline must fire an echo-skip for every live party.
    advance_time(&mut nodes, &mut outbox, entry_instant + theta_echo + Duration::from_millis(1)).await;
    for &i in &live {
        assert_eq!(nodes[i].agb.sealed_for_test(dead_view), None, "the dead view must never seal");
    }

    // Advance to theta_R(2): no ready quorum ever formed (only echo-skips were ever
    // counted, never a graded proposal echo) -- the absolute deadline fires a
    // no-ready.
    advance_time(&mut nodes, &mut outbox, entry_instant + theta_ready + Duration::from_millis(1)).await;

    // Lemma (a)'s inductive step, observable: entry continued past the dead view --
    // W3's amplification rides out on the very echo-skip/no-ready responses just
    // emitted for view 2, so views beyond it must have been formally entered too
    // (this is also the only way `try_propose` could ever fire again at all: the
    // *true* well-formed contiguous prefix is permanently stuck at view 1, since view
    // 2's proposal never arrives -- only W5(c)'s formal-entry floor can unblock R1 for
    // view 3 and beyond).
    for &i in &live {
        assert!(nodes[i].frontier.is_active(dead_view + 1), "node {} must have entered past the dead view", i);
    }

    // Let the now-unblocked live-proposer views actually run their course.
    run_to_quiescence(&mut nodes, &mut outbox, entry_instant + theta_ready + Duration::from_millis(1)).await;

    // Later views with live proposers must complete and seal normally.
    let mut any_live_view_sealed = false;
    for v in (dead_view + 1)..=(dead_view + 3) {
        if nodes[live[0]].agb.sealed_for_test(v).is_some() {
            any_live_view_sealed = true;
        }
    }
    assert!(any_live_view_sealed, "at least one live-proposer view beyond the dead one must seal at the AGB layer");

    // The pre-resolver checkpoint (Phase 5's own documented boundary, kept here as the
    // "before" half of this test): the output cursor is TRANSIENTLY blocked exactly at
    // the dead view, even though later views sealed at the AGB layer above.
    for &i in &live {
        assert_eq!(nodes[i].cursor.next_view(), dead_view, "node {} cursor must be transiently blocked exactly at the dead view (pre-resolution)", i);
    }

    // PHASE6-SPEC.md §9: drive the resolver -> anchor pipeline the rest of the way
    // (identical to `byzantine_tests.rs`'s scenario 1, since this is the same setup).
    // A later live proposer's recovery turn must carry `Skip(dead_view)` -- the only
    // justified candidate, given the refusal census (echo-skip/no-ready) just
    // established above.
    let carrying_view: crate::primary::View = 1000;
    let carrier_name = crate::vantage::agb::proposer(&test_committee(), carrying_view);
    let carrier_idx = live.iter().find(|&&i| nodes[i].name == carrier_name).copied().expect("a live party must lead the carrying view");
    let m = {
        let node = &mut nodes[carrier_idx];
        let agb = &node.agb;
        let control = &node.control;
        // Consume the (initially data-only) next-turn bit first, exactly like
        // `Node::try_propose_effects` would at this party's own proposer turn.
        node.resolver.decide(agb, carrying_view, |u| agb.is_sealed(u) || control.is_anchor_resolved(u));
        node.resolver.decide(agb, carrying_view, |u| agb.is_sealed(u) || control.is_anchor_resolved(u))
    };
    assert_eq!(m, Some(ResolutionEntry::Skip(dead_view)), "the recovery turn must carry Skip(dead_view) -- it is the only justified candidate");

    let (author0, _) = all[0];
    let c_ref = nodes[carrier_idx].lm.c_candidate(&author0).expect("seeded C candidate");
    let proposal = ViewProposal { view: carrying_view, c: vec![c_ref], t: Vec::new(), m };

    for &i in &live {
        let effects = nodes[i].enter_view_effects(carrying_view, entry_instant);
        drain_local(&mut nodes, i, effects, entry_instant, &mut outbox);
    }
    run_to_quiescence(&mut nodes, &mut outbox, entry_instant).await;

    for &i in &live {
        outbox.push_back((i, Inbound::Propose(proposal.clone())));
    }
    run_to_quiescence(&mut nodes, &mut outbox, entry_instant).await;

    // Drive the control-round timer forward so a fresh round's leader picks up the
    // now-submittable pair (see `byzantine_tests.rs`'s scenario 1 for the full
    // reasoning -- the control round in flight when the report lands may already be
    // stuck on a stale/`⊥` proposal until reliable-notification disables it).
    let control_timeout = nodes[live[0]].control.control_round_timeout();
    let mut ct = entry_instant;
    for _ in 0..6 {
        ct += control_timeout + Duration::from_millis(1);
        advance_time(&mut nodes, &mut outbox, ct).await;
        run_to_quiescence(&mut nodes, &mut outbox, ct).await;
    }

    // The post-resolution checkpoint (this phase's fix, the "after" half): every live
    // node has sealed `gskip` for the dead view via the anchor, and the cursor has
    // ADVANCED PAST it -- the pre-resolver blocking behavior asserted above is gone.
    for &i in &live {
        assert_eq!(nodes[i].agb.sealed_for_test(dead_view), Some(Outcome::Skip), "node {} must have sealed gskip for the dead view via the anchor", i);
        assert!(nodes[i].cursor.next_view() > dead_view, "node {} cursor must have advanced past the dead view -- the pre-resolver block is gone", i);
    }
    let reference = nodes[live[0]].cursor.output_log().to_vec();
    for &i in &live[1..] {
        assert_eq!(nodes[i].cursor.output_log(), reference.as_slice(), "node {} output log must match node {}", i, live[0]);
    }
}

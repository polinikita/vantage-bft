// PHASE5-SPEC.md §4 -- crash-fault integration (in-proc, 4 engines, injected clocks):
// kill proposer(v); confirm correct parties still formally enter v via wishes,
// echo-skip at theta_E, no-ready at theta_R, and enter v+1 and beyond (lemma (a)'s
// inductive step, observable); later views with live proposers still complete and
// seal normally.
//
// PHASE6-SPEC.md §9 gate amendment / signature-free.tex 704fb29 (par:skip-seal,
// cor:crash-skip): this test originally demonstrated a TWO-PHASE story for the dead
// view -- the output cursor TRANSIENTLY blocks at it until Phase 6's resolver/
// control-log/anchor adapter closes the gap via a manually-constructed carrying
// proposal plus control-round advancement. The grounded post-ready skip vote
// (unconditional protocol behavior, no flag) supersedes that second phase for this
// EXACT scenario: n=4/f=1's 3 live parties are exactly Q=2f+1, so every live party's
// own echo-skip quorum and own no-ready are already in place the instant theta_ready
// fires -- the dead view seals gskip via the vote quorum immediately, with zero
// control-log involvement, well before any carrying proposal could even be built.
// Renamed and adapted accordingly; the now-unreachable manual anchor dance is
// replaced by an explicit assertion that the resolver finds nothing left to justify.
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
    assert_eq!(
        live.len(),
        3,
        "n=4, f=1 -- exactly 2f+1=3 correct parties remain, the tight case"
    );

    boot(&mut nodes, now, &mut outbox).await;

    // 2f+1=3 correct parties booting is exactly enough for the genesis wish(2) to
    // reach quorum -- view 2 must be formally entered by every live party even though
    // its proposer never proposes anything at all (W1/W2, entry via wishes alone).
    for &i in &live {
        assert!(
            nodes[i].frontier.is_active(dead_view),
            "node {} must have formally entered the dead view via WISH",
            i
        );
    }

    let theta_echo = nodes[live[0]].agb.theta_echo();
    let theta_ready = nodes[live[0]].agb.theta_ready();
    // Entry to view 2 happened synchronously during `boot`'s single quiescence pass,
    // so its timers were armed relative to `now` itself.
    let entry_instant = now;

    // Advance to theta_E(2): no proposal ever arrived (`fixed` stays Unset forever)
    // -- the absolute deadline must fire an echo-skip for every live party. Own
    // no-ready has not fired yet (theta_R > theta_E), so the vote gate cannot have
    // fired either.
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

    // Advance to theta_R(2): no ready quorum ever formed (only echo-skips were ever
    // counted, never a graded proposal echo) -- the absolute deadline fires a
    // no-ready. Every live party now has both its own durable no-ready AND a
    // first-hand 2f+1 echo-skip quorum (from the theta_E step above), and no other
    // resolution stance/terminal-outcome conjunct is in play -- the grounded skip
    // vote fires within this same advance, and self+peer counting reaches the
    // 2f+1 vote quorum within the same synchronous drain.
    advance_time(
        &mut nodes,
        &mut outbox,
        entry_instant + theta_ready + Duration::from_millis(1),
    )
    .await;

    // Lemma (a)'s inductive step, observable: entry continued past the dead view --
    // W3's amplification rides out on the very echo-skip/no-ready responses just
    // emitted for view 2, so views beyond it must have been formally entered too.
    for &i in &live {
        assert!(
            nodes[i].frontier.is_active(dead_view + 1),
            "node {} must have entered past the dead view",
            i
        );
    }

    // par:skip-seal / cor:crash-skip: every live party has already sealed the dead
    // view gskip via the grounded vote quorum, with NO control-log anchor involved.
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

    // The legacy fallback is now unreachable for this scenario: a later recovery
    // turn's resolver scan finds NOTHING left to justify for the dead view, since it
    // is already sealed -- `Resolver::decide` returns `None` rather than
    // `Some(Skip(dead_view))`. (The first call consumes the per-proposer data-only/
    // recovery alternation bit, exactly as an ordinary proposer turn would; the
    // second is the actual recovery-turn evaluation this assertion is about.)
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

    // Let the now-unblocked live-proposer views actually run their course. The output
    // cursor has ALREADY advanced past the dead view (via the vote-sealed skip) by
    // this point -- no anchor was ever needed.
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

// PHASE6-SPEC.md §8 -- the Byzantine fault-injection suite. Each test names the
// defense it exercises AND the trust boundary it does not.
//
// Scenario 7 (non-defense note, mandatory): declared-sender spoofing -- publish
// provenance, and the `sender` field on every ack/echo/ready/wish/report/control
// message -- is NOT defended anywhere in this codebase until Phase-7's channel
// authentication lands (the standing D4 ruling, carried forward from PHASE3-NOTES.md
// through every phase since). No test in this suite (or anywhere else in the crate)
// forges a sender field to simulate an attack; the one place a "wrong sender" is ever
// constructed (`agb_echo_tests.rs`'s direct-engine unit tests) exercises
// `AgbEngine::on_propose`'s own `sender == proposer(view)` guard as a plain
// correctness check on a value the production wiring never actually verifies (D4,
// PHASE4-SPEC.md §13's standing note) -- it does not model an attack.
//
// Scenario 1 (the marquee test): a silent/withheld proposer never proposes for its
// view -> refusals at deadlines (echo-skip, no-ready) -> a later correct proposer's
// recovery turn carries `Skip(u)` -> the control log anchors it -> every (live) node
// seals `gskip` and the cursor advances past `u`. This is Phase 5's documented,
// asserted-as-correct-at-the-time blocking behavior, now resolved: the crash-fault
// scenario's "cursor permanently blocks at the dead view" boundary from
// PHASE5-NOTES.md/`crash_fault_tests.rs` is exactly what this phase's resolver +
// control log + anchor adapter close.
//
// Scenarios 2-6 (below scenario 1) use the SAME methodology established here: real
// multi-node harness interaction wherever the mechanism under test genuinely spans
// several parties (message-content-level Byzantine behavior only -- never sender
// forgery, per scenario 7's boundary below), and the identical resolver-driven
// carrying-view technique to obtain a genuine recovery `M` without fighting this
// synchronous harness's WISH-cascade timing artifact (documented in scenario 1's own
// note). `harness::deliver_only_to` (added this phase) is the suite's one new
// interception hook: it delivers a constructed message directly to exactly the given
// node indices, bypassing `drain_local`'s broadcast-to-all -- the mechanism used to
// model withheld/forked/equivocated CONTENT (a message every recipient still sees as
// genuinely, honestly sent by whoever the wire format says sent it; only WHO receives
// it, or what distinct copies exist, is manipulated). Scenario 5 (Byzantine control
// leader) is driven directly against several bare `ControlLog` instances instead (no
// AGB/harness needed -- the mechanism under test is entirely within that layer, same
// "test at the layer the mechanism lives in" principle `resolution_gate_tests.rs`/
// `fastseal_tests.rs` already established for Phase 6's engine-level layers). Scenario
// 6 (fast-lock release) is likewise driven directly against a single `AgbEngine`,
// extending (not duplicating) `resolution_gate_tests.rs`'s existing
// `meta_ok_lock_rule_blocks_non_matching_entry_while_lock_active` test, which never
// drives the lock all the way through RELEASE -- this scenario's whole point.
//
// Methodology note: the refusal census (echo-skip/no-ready for the dead view) is
// established exactly as `crash_fault_tests.rs` does, via the real harness timer/WISH
// machinery. The carrying proposal that attaches the recovery entry is then driven
// DETERMINISTICALLY (its `M` value obtained directly from each live node's own
// `Resolver`, matching production's exact call, then dispatched as a real
// `Inbound::Propose` through the unmodified `dispatch`/`drain_local`/`run_to_quiescence`
// pipeline) rather than by waiting for the harness's organic per-view WISH cascade to
// happen to land a fresh, un-proposed view on a live proposer's turn: in this
// synchronous, zero-latency harness (unlike a real network), WISH's formal-entry
// target can race hundreds of views ahead of real time within a single quiescence
// pass -- every one of those views gets ITS one-shot proposer turn (and consumes the
// resolver's next-turn bit, if a target already qualifies at that instant) well before
// later `advance_time` calls even run, which is a harness-timing artifact orthogonal to
// the mechanism under test. Driving the carrying view's `M` directly exercises the
// identical `Resolver::decide` -> `Frontier::try_propose` -> `AgbEngine::on_propose`
// call sequence `Node::try_propose_effects` makes; `resolve_tests.rs` separately covers
// the bit/pointer bookkeeping in isolation.

use super::common::*;
use super::harness::{advance_time, boot, boot_without_control, deliver_only_to, drain_local, run_to_quiescence, start_control, Node};
use crate::messages::Header;
use crate::primary::View;
use crate::vantage::agb::{self, Echo, Outcome, ReadyGrade, ResolutionEntry, ViewProposal};
use crate::vantage::control::{ControlLog, ControlProposal};
use crate::vantage::node::Inbound;
use crate::vantage::Effect;
use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

const MAX_VIEWS: crate::primary::View = 12;
/// Scenarios 2-4 keep ALL FOUR parties alive (unlike scenario 1's dead proposer), so
/// the genuine, real round-robin proposer for view 2 is live and WOULD organically
/// propose (and, since every author's C is already quorum-acked from the seed round,
/// seal) view 2 through the ordinary production pipeline during `boot()`/WISH
/// activation, before any manually-injected equivocated/withheld/forked content ever
/// gets a chance -- `on_propose`'s sticky `Fixed::Unset` guard means whichever proposal
/// arrives FIRST wins, permanently (verified empirically: an earlier draft of these
/// tests asserted the manually-injected content had fixed view 2 and failed, because
/// the organic one already had). `1` lets view 1 organically propose and seal
/// normally (matching every author's uniformly-available, quorum-acked seed content,
/// so `resolved(1)` is trivially true via `agb.is_sealed(1)` -- it needs no resolution
/// machinery of its own) but blocks `try_propose_effects` from EVER organically
/// proposing view 2 onward (`self.frontier.a_i() >= self.max_views` becomes true the
/// instant `a_i` reaches 1) -- formal WISH-driven activation (`Frontier::enter`, itself
/// unconditional on `max_views`) and every manually-injected `Inbound::Propose` (which
/// bypasses `try_propose_effects` entirely, same as scenario 1's carrying-view
/// technique) stay fully functional. (`0`, blocking view 1 too, was tried first and
/// rejected: an un-proposed view 1 becomes ITSELF resolvable via a trivial `Skip`, and
/// the resolver's ascending scan would consume the recovery turn on view 1 before ever
/// reaching view 2.)
const MAX_VIEWS_NO_ORGANIC: crate::primary::View = 1;

/// Shared by scenarios 2-4: drive the control-round timer forward `rounds` times
/// (6Δ each) so the reliable-notification disable path frees up a fresh round whose
/// leader can pick up a just-became-submittable pair -- see scenario 1's own note on
/// why this is needed (a stale/`⊥` round may already be in flight when the pair
/// becomes submittable).
async fn drive_control_rounds(nodes: &mut [Node], outbox: &mut VecDeque<(usize, Inbound)>, now: Instant, rounds: usize) {
    let control_timeout = nodes.iter().find(|n| n.alive).unwrap().control.control_round_timeout();
    let mut ct = now;
    for _ in 0..rounds {
        ct += control_timeout + Duration::from_millis(1);
        advance_time(nodes, outbox, ct).await;
        run_to_quiescence(nodes, outbox, ct).await;
    }
}

/// Shared by scenarios 2-4: consume the resolver's next-turn bit (asserting it starts
/// data-only, per §4 step 2/3) and obtain the recovery entry for `carrying_view`,
/// exactly the same `Resolver::decide` calls `Node::try_propose_effects` would make at
/// `carrying_view`'s own proposer's turn (see scenario 1's own methodology note for why
/// this is driven directly rather than via the organic WISH cascade). Returns (the
/// carrying view's own live proposer's node index, the resolved entry).
fn resolve_carrying_entry(nodes: &mut [Node], carrying_view: View, target_view: View) -> (usize, ResolutionEntry) {
    let carrier_name = agb::proposer(&test_committee(), carrying_view);
    let carrier_idx = nodes.iter().position(|n| n.name == carrier_name).unwrap();
    let now = Instant::now();
    let first = {
        let node = &mut nodes[carrier_idx];
        let agb = &node.agb;
        let control = &node.control;
        node.resolver.decide(agb, carrying_view, now, |u| agb.is_sealed(u) || control.is_anchor_resolved(u))
    };
    assert_eq!(first, None, "the next-turn bit starts data-only");
    let m = {
        let node = &mut nodes[carrier_idx];
        let agb = &node.agb;
        let control = &node.control;
        node.resolver.decide(agb, carrying_view, now, |u| agb.is_sealed(u) || control.is_anchor_resolved(u))
    };
    let entry = m.expect("the target view must be justified for recovery");
    assert_eq!(entry.target_view(), target_view);
    (carrier_idx, entry)
}

/// Shared by scenarios 2-4: formally activate `carrying_view` for every node, dispatch
/// its (directly-constructed) carrying proposal to everyone, then start the
/// control-round clock and drive it forward far enough to reach the anchor (see
/// `boot_without_control`'s doc comment for why the clock starts only now).
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
    deliver_only_to(nodes, outbox, &everyone, Inbound::Propose(proposal));
    run_to_quiescence(nodes, outbox, now).await;
    start_control(nodes, now, outbox).await;
    drive_control_rounds(nodes, outbox, now, 6).await;
}

#[tokio::test]
async fn scenario_1_silent_proposer_sealed_via_skip_anchor_cursor_advances() {
    let all = authors();
    let mut nodes: Vec<Node> = all
        .iter()
        .enumerate()
        .map(|(i, (pk, _))| Node::new(*pk, &format!(".db_test_byz_scenario1_node_{}", i), MAX_VIEWS))
        .collect();
    let now = Instant::now();
    let mut outbox: VecDeque<(usize, Inbound)> = VecDeque::new();

    // Seed round: every party publishes one empty-payload height-1 block, so every
    // party's N5 registers have a real, quorum-acked C candidate for all four authors
    // before any AGB view proposes (same seeding as `crash_fault_tests.rs`).
    for i in 0..nodes.len() {
        let (_, effects) = nodes[i].lm.publish_own(std::collections::BTreeMap::new()).await;
        drain_local(&mut nodes, i, effects, now, &mut outbox);
    }
    run_to_quiescence(&mut nodes, &mut outbox, now).await;

    // The defense: proposer(2) is silent/withheld -- never proposes anything, ever
    // (kill it before boot, same as `crash_fault_tests.rs`'s dead-proposer model).
    let dead_view: crate::primary::View = 2;
    let dead_name = crate::vantage::agb::proposer(&test_committee(), dead_view);
    let dead_idx = nodes.iter().position(|n| n.name == dead_name).unwrap();
    nodes[dead_idx].alive = false;
    let live: Vec<usize> = (0..nodes.len()).filter(|&i| i != dead_idx).collect();
    assert_eq!(live.len(), 3, "n=4, f=1 -- exactly 2f+1=3 correct parties remain");

    boot(&mut nodes, now, &mut outbox).await;

    let theta_echo = nodes[live[0]].agb.theta_echo();
    let theta_ready = nodes[live[0]].agb.theta_ready();
    let entry_instant = now;

    // Refusals at deadlines: echo-skip at theta_E, no-ready at theta_R -- exactly
    // Phase-5's documented behavior (`crash_fault_tests.rs`).
    advance_time(&mut nodes, &mut outbox, entry_instant + theta_echo + Duration::from_millis(1)).await;
    advance_time(&mut nodes, &mut outbox, entry_instant + theta_ready + Duration::from_millis(1)).await;

    for &i in &live {
        assert_eq!(nodes[i].agb.sealed_for_test(dead_view), None, "the dead view never seals directly");
        assert!(nodes[i].agb.noready_count(dead_view) >= 3, "D6-5: every live party's first-hand no-ready is counted (2f+1=3)");
    }

    // A later correct proposer's recovery turn: pick a fresh view (well past
    // `dead_view + 3` and past anything the WISH cascade above could have already
    // used) whose round-robin proposer is one of the live parties, and call the
    // IDENTICAL `Resolver::decide` production uses at that party's own proposer turn
    // -- first consuming the (initially data-only) bit, then obtaining the recovery
    // entry, exactly as `Node::try_propose_effects` would if this view happened to be
    // that party's next unproposed turn.
    let carrying_view: crate::primary::View = 1000;
    let carrier_name = crate::vantage::agb::proposer(&test_committee(), carrying_view);
    let carrier_idx = live.iter().find(|&&i| nodes[i].name == carrier_name).copied().expect("a live party must lead the carrying view");

    let now = Instant::now();
    let first = {
        let node = &mut nodes[carrier_idx];
        let agb = &node.agb;
        node.resolver.decide(agb, carrying_view, now, |u| agb.is_sealed(u))
    };
    assert_eq!(first, None, "the next-turn bit starts data-only");
    let m = {
        let node = &mut nodes[carrier_idx];
        let agb = &node.agb;
        node.resolver.decide(agb, carrying_view, now, |u| agb.is_sealed(u))
    };
    assert_eq!(m, Some(ResolutionEntry::Skip(dead_view)), "the recovery turn must carry Skip(dead_view) -- it is the only justified candidate");

    // Build the carrying proposal over the SAME seeded, already-quorum'd content every
    // other view in this harness uses (author 0's height-1 block).
    let (author0, _) = all[0];
    let c_ref = nodes[carrier_idx].lm.c_candidate(&author0).expect("seeded C candidate");
    let proposal = ViewProposal { view: carrying_view, c: vec![c_ref], t: Vec::new(), m };

    // Every live party must formally activate the carrying view before it can process
    // a direct proposal for it (mirrors `Frontier::enter`'s "also activates" -- WISH
    // itself would do this in production; done directly here since the deliberately
    // small `MAX_VIEWS` cap keeps this test's own WISH cascade well below 1000).
    for &i in &live {
        let effects = nodes[i].enter_view_effects(carrying_view, now);
        drain_local(&mut nodes, i, effects, now, &mut outbox);
    }
    run_to_quiescence(&mut nodes, &mut outbox, now).await;

    // Dispatch the direct proposal to every live party (mirrors `BroadcastPropose`) and
    // let the full pipeline run: R2 echo -> R3 ready -> R4 completion (M != None) ->
    // `CompletionReportable` -> `CompReport` census -> submittable -> the control
    // log's leader turn -> validated Bracha -> commit -> `ApplyAnchor` -> the try-seal
    // arbiter -> the cursor.
    for &i in &live {
        outbox.push_back((i, Inbound::Propose(proposal.clone())));
    }
    run_to_quiescence(&mut nodes, &mut outbox, now).await;

    // The carrying view's own completion/report lands quickly, but whichever control
    // round was already in flight when it did may already be stuck with a sticky,
    // stale (pre-submittable) proposal (possibly `⊥`, possibly led by a since-timed-out
    // leader) -- drive the control-round timer (6Δ) forward a few rounds so the
    // reliable-notification disable path frees up a FRESH round whose leader's own
    // entry-time `Propose` step picks up the now-submittable pair.
    let control_timeout = nodes[live[0]].control.control_round_timeout();
    let mut ct = now;
    for _ in 0..6 {
        ct += control_timeout + Duration::from_millis(1);
        advance_time(&mut nodes, &mut outbox, ct).await;
        run_to_quiescence(&mut nodes, &mut outbox, ct).await;
    }

    // The defense's payoff: every live node has sealed `gskip` for the dead view
    // (reachable at last -- Direct-AGB alone could never produce it, PHASE4-NOTES.md)
    // and the cursor has advanced PAST it -- the exact Phase-5 boundary this phase
    // closes.
    for &i in &live {
        assert_eq!(nodes[i].agb.sealed_for_test(dead_view), Some(Outcome::Skip), "node {} must have sealed gskip for the dead view via the anchor", i);
        assert!(nodes[i].cursor.next_view() > dead_view, "node {} cursor must have advanced past the dead view", i);
        // PHASE6-SPEC.md §9 gate amendment: the dead view can ONLY ever have been
        // sealed via the anchor's Skip route (direct-AGB never produces gskip at all).
        assert_eq!(
            nodes[i].metrics.vantage_seals.with_label_values(&["anchor_skip"]).get(),
            1,
            "node {} must show exactly one anchor_skip route increment for the dead view",
            i
        );
    }

    // Identical outputs across live nodes (the resolver's canonical order + the
    // control log's own totality make this deterministic).
    let reference = nodes[live[0]].cursor.output_log().to_vec();
    for &i in &live[1..] {
        assert_eq!(nodes[i].cursor.output_log(), reference.as_slice(), "node {} output log must match node {}", i, live[0]);
    }
}

/// Scenario 2: a withheld-tip author. **Methodology note (a genuine, load-bearing
/// finding, not a stylistic choice)**: this harness is fully synchronous and
/// zero-latency, and `on_propose`'s own authorize loop (§1's `AuxRefs`/C/T hook) calls
/// `Repairer::authorize` on EVERY C/T entry of whatever proposal a party FIXES,
/// regardless of whether its positive gate holds -- so if all four parties fixed the
/// SAME single proposal naming a tip only two of them initially held directly, repair
/// (N6/N7) would close the gap for the other two WITHIN THE SAME quiescence pass,
/// before any deadline ever fires, converging to an ordinary direct seal with no
/// lasting grade split at all (verified empirically -- an earlier draft of this test
/// asserted exactly the opposite and failed for exactly this reason). A genuinely
/// PERSISTENT split (the spec's own "mixed grades" premise) therefore requires the
/// FIXED content itself to differ across parties -- repair cannot close a gap between
/// two parties who never authorized the same reference in the first place, since each
/// party's sticky `fixed` is set to only the FIRST proposal it ever receives (§5
/// `on_propose`). Modeled here as the withheld-tip author's own proposer sending two
/// proposals that share the SAME core `C` (the common, quorum-acked seeded content --
/// unaffected either way) but differ ONLY in whether a `T` entry is attached: `T =
/// [tip]` to the two parties the author actually published the tip to, `T = []` to the
/// other two (who never even learn the tip reference exists). This is the narrowest
/// possible divergence (a single optional field), distinguishing it from scenario 3's
/// wholesale different-author equivocation. Neither resulting digest can reach the
/// 2f+1=3 echo-quorum alone (quorum intersection, same argument as scenario 3);
/// resolution settles it. Assert: identical outputs everywhere, and (conditionally,
/// depending on which canonically-ordered candidate the resolver actually picks) that
/// IF the winning entry carries the tip, it appears in every node's output including
/// the two parties that never held it directly (repaired via the anchor's `AuxRefs`) --
/// this is the cursor's core-prefix property in action: the common `C` is always
/// emitted regardless of which `T` variant (if any) ultimately wins.
#[tokio::test]
async fn scenario_2_withheld_tip_author_mixed_grades_resolved_via_anchor() {
    let all = authors();
    let mut nodes: Vec<Node> = all
        .iter()
        .enumerate()
        .map(|(i, (pk, _))| Node::new(*pk, &format!(".db_test_byz_scenario2_node_{}", i), MAX_VIEWS_NO_ORGANIC))
        .collect();
    let now = Instant::now();
    let mut outbox: VecDeque<(usize, Inbound)> = VecDeque::new();

    for i in 0..nodes.len() {
        let (_, effects) = nodes[i].lm.publish_own(BTreeMap::new()).await;
        drain_local(&mut nodes, i, effects, now, &mut outbox);
    }
    run_to_quiescence(&mut nodes, &mut outbox, now).await;
    // PHASE6-SPEC.md §8 finding (harness.rs's `boot_without_control` doc comment):
    // defer starting the control-round clock until after this test's own multi-step
    // AGB-level setup (disjoint proposal delivery, timer advances) is done, so the
    // `⊥`-round cascade doesn't burn through the test-only round budget first.
    boot_without_control(&mut nodes, now, &mut outbox).await;

    let view: View = 2;
    let (tip_author, _) = all[1];
    let sid = nodes[0].lm.sid().clone();
    let parent1 = nodes[0].lm.c_candidate(&tip_author).expect("seeded C candidate").2;
    let tip = Header::new_vantage(tip_author, 2, BTreeMap::new(), parent1, sid);
    let tip_holders = [0usize, 1usize];
    let core_only_holders = [2usize, 3usize];
    // The tip is directly published ONLY to the two parties that will ever see a T
    // entry for it -- so their own gate needs no repair round-trip at all.
    deliver_only_to(&nodes, &mut outbox, &tip_holders, Inbound::Publish(tip_author, tip.clone()));
    run_to_quiescence(&mut nodes, &mut outbox, now).await;

    let c_ref = nodes[0].lm.c_candidate(&tip_author).expect("seeded C candidate");
    let t_ref = (tip_author, tip.height, tip.id.clone());
    let proposal_full = ViewProposal { view, c: vec![c_ref.clone()], t: vec![t_ref.clone()], m: None };
    let proposal_core = ViewProposal { view, c: vec![c_ref], t: Vec::new(), m: None };
    assert_ne!(
        proposal_full.digest(&nodes[0].lm.sid().clone()),
        proposal_core.digest(&nodes[0].lm.sid().clone()),
        "the two proposals (differing only in whether T is attached) must be genuinely distinct"
    );
    deliver_only_to(&nodes, &mut outbox, &tip_holders, Inbound::Propose(proposal_full));
    deliver_only_to(&nodes, &mut outbox, &core_only_holders, Inbound::Propose(proposal_core));
    run_to_quiescence(&mut nodes, &mut outbox, now).await;

    // Both halves' own positive gates fire immediately (CoreOK holds for the common,
    // seeded C either way; the tip-holders' TipOK needs no repair, the others' T is
    // vacuously empty) -- 2 grade-1 echoes per digest, each strictly below quorum.
    let theta_ready = nodes[0].agb.theta_ready();
    advance_time(&mut nodes, &mut outbox, now + theta_ready + Duration::from_millis(1)).await;

    for i in 0..nodes.len() {
        assert!(nodes[i].agb.sealed_for_test(view).is_none(), "node {} must not seal view {} directly -- quorum intersection forbids either digest from reaching quorum alone", i, view);
        assert!(nodes[i].agb.completed_for_test(view).is_none(), "node {} must not even complete view {} -- neither digest's readies reach quorum", i, view);
        assert!(nodes[i].agb.noready_count(view) >= 3, "node {} must have the full 2f+1 no-ready census for view {}", i, view);
    }

    let carrying_view: View = 1000;
    let (carrier_idx, entry) = resolve_carrying_entry(&mut nodes, carrying_view, view);
    let entry_carries_tip = match &entry {
        ResolutionEntry::Full(_, _, t) | ResolutionEntry::Core(_, _, t) => t.iter().any(|r| r.2 == tip.id),
        ResolutionEntry::Skip(_) => false,
    };

    let (author0, _) = all[0];
    let carrying_c = nodes[carrier_idx].lm.c_candidate(&author0).expect("seeded C candidate");
    let carrying_proposal = ViewProposal { view: carrying_view, c: vec![carrying_c], t: Vec::new(), m: Some(entry) };
    drive_carrying_proposal_to_anchor(&mut nodes, &mut outbox, now, carrying_view, carrying_proposal).await;

    let reference_outcome = nodes[0].agb.sealed_for_test(view);
    assert!(reference_outcome.is_some(), "view {} must be sealed via the anchor", view);
    for i in 0..nodes.len() {
        assert_eq!(nodes[i].agb.sealed_for_test(view), reference_outcome, "node {} must seal the IDENTICAL outcome for view {}", i, view);
        assert!(nodes[i].cursor.next_view() > view, "node {} cursor must advance past view {}", i, view);
    }
    let reference_log = nodes[0].cursor.output_log().to_vec();
    for i in 1..nodes.len() {
        assert_eq!(nodes[i].cursor.output_log(), reference_log.as_slice(), "node {} output log must match node 0 (core-prefix property + identical resolution)", i);
    }
    // Core-prefix property, made concrete: IFF the anchored entry carries the tip, it
    // must appear in every node's output -- including the two parties that never held
    // it directly and can only have gotten it via repair, driven by the anchor's
    // `AuxRefs` authorization.
    if entry_carries_tip {
        for i in 0..nodes.len() {
            assert!(nodes[i].cursor.output_log().contains(&tip.id), "node {} output must contain the tip (repaired via the anchor)", i);
        }
    }

    // PHASE6-SPEC.md §9 gate amendment: this mixed-grade view can ONLY ever have
    // sealed via the anchor (never direct-AGB -- neither grade alone ever reached
    // quorum, by construction) -- across this whole test, the only view that could
    // ever route through the anchor at all is this one (the carrying view itself
    // completes normally, via fast_full/direct_full), so anchor_full+anchor_core must
    // total exactly 1 (whichever the resolver's canonical order picked).
    for i in 0..nodes.len() {
        let m = &nodes[i].metrics.vantage_seals;
        let anchor_full = m.with_label_values(&["anchor_full"]).get();
        let anchor_core = m.with_label_values(&["anchor_core"]).get();
        assert_eq!(anchor_full + anchor_core, 1, "node {} must show exactly one anchor_full/anchor_core route increment for this mixed-grade view", i);
    }
}

/// Scenario 3: an equivocating leader for view 2 sends two genuinely different
/// proposals (X, over author-0's seeded content; Y, over author-1's) to two disjoint
/// halves of the committee. Quorum intersection (2*(2f+1) > n = 4) means NEITHER
/// digest can ever reach the 2f+1=3 echo-quorum needed for R3's ready trigger on its
/// own -- every party ends up at the ready-stage absolute deadline with a first-hand
/// no-ready. This is "at most one completes" realized as the strongest case (zero
/// completes) -- resolution must settle the whole view. Both `X` and `Y` end up
/// independently justified (2 grade-1 echoes each, exactly f+1), alongside `Skip`,
/// giving the maximal 5-candidate set the spec's own bound allows; the resolver's
/// canonical order deterministically picks one. Assert: no two nodes ever seal a
/// different outcome (every node converges on the identical anchored result).
#[tokio::test]
async fn scenario_3_equivocating_leader_disjoint_halves_resolution_settles_it() {
    let all = authors();
    let mut nodes: Vec<Node> = all
        .iter()
        .enumerate()
        .map(|(i, (pk, _))| Node::new(*pk, &format!(".db_test_byz_scenario3_node_{}", i), MAX_VIEWS_NO_ORGANIC))
        .collect();
    let now = Instant::now();
    let mut outbox: VecDeque<(usize, Inbound)> = VecDeque::new();

    for i in 0..nodes.len() {
        let (_, effects) = nodes[i].lm.publish_own(BTreeMap::new()).await;
        drain_local(&mut nodes, i, effects, now, &mut outbox);
    }
    run_to_quiescence(&mut nodes, &mut outbox, now).await;
    // PHASE6-SPEC.md §8 finding (harness.rs's `boot_without_control` doc comment):
    // defer starting the control-round clock until after this test's own multi-step
    // AGB-level setup (disjoint proposal delivery, timer advances) is done, so the
    // `⊥`-round cascade doesn't burn through the test-only round budget first.
    boot_without_control(&mut nodes, now, &mut outbox).await;

    let view: View = 2;
    let (author0, _) = all[0];
    let (author1, _) = all[1];
    let x_ref = nodes[0].lm.c_candidate(&author0).expect("seeded C candidate");
    let y_ref = nodes[0].lm.c_candidate(&author1).expect("seeded C candidate");
    let proposal_x = ViewProposal { view, c: vec![x_ref], t: Vec::new(), m: None };
    let proposal_y = ViewProposal { view, c: vec![y_ref], t: Vec::new(), m: None };
    assert_ne!(
        proposal_x.digest(&nodes[0].lm.sid().clone()),
        proposal_y.digest(&nodes[0].lm.sid().clone()),
        "the two equivocated proposals must be genuinely distinct"
    );

    let half_a = [0usize, 1usize];
    let half_b = [2usize, 3usize];
    deliver_only_to(&nodes, &mut outbox, &half_a, Inbound::Propose(proposal_x));
    deliver_only_to(&nodes, &mut outbox, &half_b, Inbound::Propose(proposal_y));
    run_to_quiescence(&mut nodes, &mut outbox, now).await;

    // Both halves' own positive gates fire immediately (CoreOK holds for either
    // author's seeded, quorum-acked content) -- 2 grade-1 echoes per digest, each
    // strictly below the 2f+1=3 quorum a single digest needs to trigger R3's ready.
    let theta_ready = nodes[0].agb.theta_ready();
    advance_time(&mut nodes, &mut outbox, now + theta_ready + Duration::from_millis(1)).await;

    for i in 0..nodes.len() {
        assert!(nodes[i].agb.sealed_for_test(view).is_none(), "node {} must not seal view {} directly -- quorum intersection forbids either digest from reaching quorum alone", i, view);
        assert!(nodes[i].agb.completed_for_test(view).is_none(), "node {} must not even complete view {} -- neither digest's readies reach quorum, so no ready quorum of ANY digest forms", i, view);
        assert!(nodes[i].agb.noready_count(view) >= 3, "node {} must have the full 2f+1 no-ready census for view {}", i, view);
    }

    let carrying_view: View = 1000;
    let (carrier_idx, entry) = resolve_carrying_entry(&mut nodes, carrying_view, view);

    let (author2, _) = all[2];
    let carrying_c = nodes[carrier_idx].lm.c_candidate(&author2).expect("seeded C candidate");
    let carrying_proposal = ViewProposal { view: carrying_view, c: vec![carrying_c], t: Vec::new(), m: Some(entry) };
    drive_carrying_proposal_to_anchor(&mut nodes, &mut outbox, now, carrying_view, carrying_proposal).await;

    let reference_outcome = nodes[0].agb.sealed_for_test(view);
    assert!(reference_outcome.is_some(), "view {} must be sealed via the anchor", view);
    for i in 0..nodes.len() {
        assert_eq!(nodes[i].agb.sealed_for_test(view), reference_outcome, "node {} must seal the IDENTICAL outcome for view {} -- no two nodes may diverge", i, view);
        assert!(nodes[i].cursor.next_view() > view, "node {} cursor must advance past view {}", i, view);
    }
    let reference_log = nodes[0].cursor.output_log().to_vec();
    for i in 1..nodes.len() {
        assert_eq!(nodes[i].cursor.output_log(), reference_log.as_slice(), "node {} output log must match node 0", i);
    }
}

/// Scenario 4: a Byzantine author forks its own lane at height 2 -- two genuinely
/// different children (`x2`, `y2`) of the SAME height-1 parent, published to two
/// disjoint halves of the committee. `C` (the height-1 seeded content) is common to
/// everyone and unaffected by the fork -- exactly Phase-3 N5's "C-pinning": the fork
/// height itself never reaches the ack-quorum a `c_candidate` needs (each digest gets
/// acks from only 2 of 4 parties), so `C` never advances past it. **Methodology note**
/// (same finding as scenario 2's own note): naming ONE specific branch (say `x2`) in a
/// SINGLE proposal delivered to everyone would let `on_propose`'s authorize-on-fix hook
/// repair `x2` for the `y2`-holders within the same quiescence pass, closing the split
/// instantly -- no lasting divergence. A genuine, persistent fork therefore needs the
/// PROPOSAL itself to differ (same `C`, but `T = [x2]` vs `T = [y2]`) across the two
/// halves, so each half only ever authorizes/repairs ITS OWN held branch, never the
/// other's -- otherwise repair would silently heal the "fork" before either grade or
/// resolution ever mattered. Neither digest reaches echo-quorum alone (quorum
/// intersection, as in scenario 3); resolution settles it, anchoring whichever branch
/// the canonically-first candidate names. Assert: canonical expansion + first-occurrence
/// dedup make every node's output IDENTICAL -- including the losing branch's holders,
/// who must repair the WINNING branch via the anchor's `AuxRefs` authorization -- and
/// that the LOSING fork branch never appears in anyone's output.
#[tokio::test]
async fn scenario_4_forked_author_chain_kept_branch_wins_identical_outputs() {
    let all = authors();
    let mut nodes: Vec<Node> = all
        .iter()
        .enumerate()
        .map(|(i, (pk, _))| Node::new(*pk, &format!(".db_test_byz_scenario4_node_{}", i), MAX_VIEWS_NO_ORGANIC))
        .collect();
    let now = Instant::now();
    let mut outbox: VecDeque<(usize, Inbound)> = VecDeque::new();

    for i in 0..nodes.len() {
        let (_, effects) = nodes[i].lm.publish_own(BTreeMap::new()).await;
        drain_local(&mut nodes, i, effects, now, &mut outbox);
    }
    run_to_quiescence(&mut nodes, &mut outbox, now).await;
    // PHASE6-SPEC.md §8 finding (harness.rs's `boot_without_control` doc comment):
    // defer starting the control-round clock until after this test's own multi-step
    // AGB-level setup (disjoint proposal delivery, timer advances) is done, so the
    // `⊥`-round cascade doesn't burn through the test-only round budget first.
    boot_without_control(&mut nodes, now, &mut outbox).await;

    let view: View = 2;
    let (fork_author, _) = all[3];
    let sid = nodes[0].lm.sid().clone();
    let parent1 = nodes[0].lm.c_candidate(&fork_author).expect("seeded C candidate").2;
    let x2 = tagged_header(fork_author, 2, parent1.clone(), sid.clone(), 0xA0);
    let y2 = tagged_header(fork_author, 2, parent1, sid, 0xB0);
    assert_ne!(x2.id, y2.id, "the two forked children must be genuinely distinct");

    let x_holders = [0usize, 1usize];
    let y_holders = [2usize, 3usize];
    deliver_only_to(&nodes, &mut outbox, &x_holders, Inbound::Publish(fork_author, x2.clone()));
    deliver_only_to(&nodes, &mut outbox, &y_holders, Inbound::Publish(fork_author, y2.clone()));
    run_to_quiescence(&mut nodes, &mut outbox, now).await;
    // `tagged_header`'s non-empty (tagged) payload needs a payload-presence marker at
    // each RECEIVING holder for `author_ok`/`direct_pub` to hold on the T entry (D1's
    // payload gate -- `positive_gate_holds`'s own separate "every T entry is author_ok"
    // check, independent of `TipOK`'s chain-validity-only `holds_prefix`). A real
    // network would eventually sync these batches; here, directly mark them present
    // (mirrors every other test's `mark_payload_present` + `set_payload_ready` pattern)
    // -- scoped exactly to each branch's own two holders, never the other branch's.
    for &i in &x_holders {
        nodes[i].lm.set_payload_ready(&x2.id);
    }
    for &i in &y_holders {
        nodes[i].lm.set_payload_ready(&y2.id);
    }

    let c_ref = nodes[0].lm.c_candidate(&fork_author).expect("seeded C candidate");
    let t_x = (fork_author, x2.height, x2.id.clone());
    let t_y = (fork_author, y2.height, y2.id.clone());
    let proposal_x = ViewProposal { view, c: vec![c_ref.clone()], t: vec![t_x], m: None };
    let proposal_y = ViewProposal { view, c: vec![c_ref], t: vec![t_y], m: None };
    assert_ne!(
        proposal_x.digest(&nodes[0].lm.sid().clone()),
        proposal_y.digest(&nodes[0].lm.sid().clone()),
        "the two per-branch proposals must be genuinely distinct"
    );
    deliver_only_to(&nodes, &mut outbox, &x_holders, Inbound::Propose(proposal_x));
    deliver_only_to(&nodes, &mut outbox, &y_holders, Inbound::Propose(proposal_y));
    run_to_quiescence(&mut nodes, &mut outbox, now).await;

    let theta_ready = nodes[0].agb.theta_ready();
    advance_time(&mut nodes, &mut outbox, now + theta_ready + Duration::from_millis(1)).await;

    for i in 0..nodes.len() {
        assert!(nodes[i].agb.sealed_for_test(view).is_none(), "node {} must not seal view {} directly -- quorum intersection forbids either branch from reaching quorum alone", i, view);
        assert!(nodes[i].agb.completed_for_test(view).is_none(), "node {} must not even complete view {} -- neither branch's readies reach quorum", i, view);
        assert!(nodes[i].agb.noready_count(view) >= 3, "node {} must have the full 2f+1 no-ready census for view {}", i, view);
    }

    let carrying_view: View = 1000;
    let (carrier_idx, entry) = resolve_carrying_entry(&mut nodes, carrying_view, view);
    // The anchored entry must carry EXACTLY ONE of the two branches (never a mix, and
    // never something else entirely) -- whichever it is, that becomes "the kept
    // branch"; Full/Core over neither x2 nor y2, or over BOTH, would be a genuine
    // protocol bug (Formed_v's own "at most one entry per author" together with the
    // resolver only ever picking from `justified_candidates`' real payloads rules this
    // out, but assert it directly rather than assume it).
    let winner_is_x = match &entry {
        ResolutionEntry::Full(_, _, t) | ResolutionEntry::Core(_, _, t) => {
            let has_x = t.iter().any(|r| r.2 == x2.id);
            let has_y = t.iter().any(|r| r.2 == y2.id);
            assert!(has_x ^ has_y, "the winning entry must carry EXACTLY ONE branch, never both/neither -- got T = {:?}", t);
            has_x
        }
        ResolutionEntry::Skip(_) => panic!("this view has real, justified Full/Core evidence for both branches -- Skip should never win canonical order here"),
    };
    let (losing_branch_id, kept_branch_id) = if winner_is_x { (y2.id.clone(), x2.id.clone()) } else { (x2.id.clone(), y2.id.clone()) };

    let (author0, _) = all[0];
    let carrying_c = nodes[carrier_idx].lm.c_candidate(&author0).expect("seeded C candidate");
    let carrying_proposal = ViewProposal { view: carrying_view, c: vec![carrying_c], t: Vec::new(), m: Some(entry) };
    drive_carrying_proposal_to_anchor(&mut nodes, &mut outbox, now, carrying_view, carrying_proposal).await;

    let reference_outcome = nodes[0].agb.sealed_for_test(view);
    assert!(reference_outcome.is_some(), "view {} must be sealed via the anchor", view);
    for i in 0..nodes.len() {
        assert_eq!(nodes[i].agb.sealed_for_test(view), reference_outcome, "node {} must seal the IDENTICAL outcome for view {}", i, view);
        assert!(nodes[i].cursor.next_view() > view, "node {} cursor must advance past view {}", i, view);
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
        assert!(nodes[i].cursor.output_log().contains(&kept_branch_id), "node {} output must contain the kept branch", i);
        assert!(!nodes[i].cursor.output_log().contains(&losing_branch_id), "node {} output must NEVER contain the orphaned/losing fork branch", i);
    }
}

/// Scenario 5 driver: route validated-Bracha `Effect`s produced by bare `ControlLog`
/// instances (no AGB/harness layer needed -- the mechanism under test is entirely
/// within `ControlLog`, the same "test at the layer the mechanism lives in" principle
/// `resolution_gate_tests.rs`/`fastseal_tests.rs` already established). Each queued
/// item is `(origin index, effect)`; the origin's OWN name is used as the declared
/// sender for the next hop, mirroring `harness::drain_local`'s exact routing rules for
/// the same effect variants (D4: declared-sender trust, never forged here -- every
/// `origin` really is the party that produced this effect).
fn drain_control(controls: &mut [ControlLog], names: &[crypto::PublicKey], initial: Vec<(usize, Effect)>) {
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
            _ => {} // commit/timeout/etc never arise in this contained scenario (curr_round stays 0)
        }
    }
}

/// Scenario 5: a Byzantine control-round leader. Part A: a genuinely submittable pair
/// `(w, h)` -- 3 of 4 parties already legitimately hold reports + the verified `B_w`,
/// the 4th does not (INIT reaches it "without B_w") -- honest parties without a valid
/// `B_w` never ECHO (the validity gate blocks them permanently, no retry can fix a
/// `B_w` that's never supplied directly), yet totality is preserved: once 2f+1=3
/// parties validate and relay through to delivery, the straggler independently reaches
/// its own 2f+1 READY tally (Bracha's relay only needs to SEE the quorum, not have sent
/// a matching ECHO itself) and fetches the missing `B_w` from a matching REPORT/ECHO
/// author. Part B: an INVALID pair (no legitimate reports or `B_w` exist anywhere for
/// it) -- no party can ever validate it, so it never reaches 2f+1 ECHOs, and a delivered
/// anchor for it is therefore impossible (lemma (i)'s mechanism, directly exercised).
#[tokio::test]
async fn scenario_5_byzantine_control_leader_totality_via_fetch_and_invalid_pair_unreachable() {
    let all = authors();
    let names: Vec<crypto::PublicKey> = all.iter().map(|(pk, _)| *pk).collect();
    let sid = test_sid();

    // ---------------- Part A: totality via fetch for a genuinely submittable pair ----------------
    let mut controls: Vec<ControlLog> = names.iter().map(|pk| ControlLog::new(*pk, test_committee(), sid.clone(), TEST_DELTA_MS)).collect();
    let b_w = ViewProposal { view: 4, c: Vec::new(), t: Vec::new(), m: Some(ResolutionEntry::Skip(1)) };
    let digest = b_w.digest(&sid);
    let leader = controls[0].control_leader(1);

    // Parties 0,1,2 legitimately hold reports (>= f+1=2 each) + the verified B_w;
    // party 3 holds neither.
    for i in 0..3 {
        controls[i].on_completion_reportable(4, b_w.clone());
    }
    for i in 0..3 {
        for j in 0..3 {
            controls[i].on_comp_report(4, digest.clone(), names[j]);
        }
    }

    let proposal1 = ControlProposal { round: 1, parent: 0, value: Some((4, digest.clone())) };
    let mut initial: Vec<(usize, Effect)> = Vec::new();
    for i in 0..4 {
        let b_w_variant = if i < 3 { Some(b_w.clone()) } else { None }; // party 3: INIT WITHOUT B_w
        let effects = controls[i].on_control_init(leader, proposal1.clone(), b_w_variant);
        let echoed = effects.iter().any(|e| matches!(e, Effect::BroadcastControlEcho(_)));
        if i < 3 {
            assert!(echoed, "party {} legitimately holds reports + B_w -- must ECHO immediately", i);
        } else {
            assert!(!echoed, "party 3 lacks B_w entirely -- must NOT ECHO (validity gate blocks it)");
        }
        initial.extend(effects.into_iter().map(|e| (i, e)));
    }

    drain_control(&mut controls, &names, initial);

    assert!(controls[3].holds_block_for_test(4), "party 3 must have obtained B_w via fetch (totality), despite never validating it directly");
    for i in 0..4 {
        assert!(controls[i].is_safe_for_test(1), "round 1 must be marked safe for every party once delivered (parent=0 is always safe)");
    }

    // ---------------- Part B: an invalid pair can never be delivered ----------------
    let mut controls_b: Vec<ControlLog> = names.iter().map(|pk| ControlLog::new(*pk, test_committee(), sid.clone(), TEST_DELTA_MS)).collect();
    let bogus_digest = crypto::Digest([0xEEu8; 32]);
    let bogus_view: View = 99;
    let leader_b = controls_b[0].control_leader(1);
    let proposal_bogus = ControlProposal { round: 1, parent: 0, value: Some((bogus_view, bogus_digest.clone())) };
    // No party anywhere has ever reported on view 99 -- this pair is entirely
    // fictional. Attach no B_w either (a Byzantine leader could equally attach a
    // mismatched/corrupt one -- `verify_b_w`'s digest check would reject it the same
    // way `None` does).
    let mut any_echo = false;
    for i in 0..4 {
        assert_eq!(controls_b[i].report_count_for(bogus_view, &bogus_digest), 0, "no legitimate reports exist anywhere for the fictional pair");
        let effects = controls_b[i].on_control_init(leader_b, proposal_bogus.clone(), None);
        any_echo |= effects.iter().any(|e| matches!(e, Effect::BroadcastControlEcho(_)));
    }
    assert!(!any_echo, "an invalid pair with no real backing must never be validated by anyone -- no 2f+1 ECHOs, ever");
    for i in 0..4 {
        assert!(!controls_b[i].is_safe_for_test(1), "round 1 must never become safe for the invalid pair");
        assert!(controls_b[i].delivered_log_for_test().is_empty(), "nothing may ever be delivered for the invalid pair");
    }
}

/// Scenario 6: forced mixed grades + fast-lock interaction, driven directly against a
/// single `AgbEngine` (same layer `resolution_gate_tests.rs`'s
/// `meta_ok_lock_rule_blocks_non_matching_entry_while_lock_active` already covers for
/// the PERMANENTLY-active case -- this scenario extends it through the RELEASE
/// transition that test never exercises). Our own grade-1 echo for view 1 records an
/// active lock; a carrying view's `Core` resolution entry for view 1 (a DIFFERENT
/// OUTCOME KIND than the lock's exact-matching `Full`, though it shares the same C) is
/// rejected by `MetaOK`'s lock rule while the lock stands. Two external
/// grade-0 echoes for the SAME view-1 digest accumulate as "nonmatching" against the
/// lock (D6-4: `recheck_lock_release` runs before R3's ready recheck at every
/// echo-count call site) -- the SECOND one crosses both the f+1=2 release threshold
/// AND (simultaneously, on the very same call) the 2f+1=3 ready-quorum threshold,
/// forcing a genuine same-event race between the two. Assert: by the time that same
/// call's resulting READY (necessarily `Mix`, since neither grade alone reaches
/// quorum) is visible, the lock is ALREADY inactive -- never a stale-active lock
/// coexisting with a contradictory ready -- and the previously-blocked carrying view's
/// echo fires immediately once released (`recheck_all`, the same retry every response
/// dispatch site performs in production).
#[tokio::test]
async fn scenario_6_fast_lock_release_unblocks_metaok_no_stale_lock_at_ready_time() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (author_w, _) = authors()[1];
    // The two external echo senders double as the C-content authors above (any
    // committee member may play both roles -- authorship of a data block and sending
    // an echo statement are unrelated data planes); named separately here only for
    // readability at each call site.
    let other_a = author_c;
    let other_b = author_w;
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_byz_scenario6");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);
    let now = std::time::Instant::now();

    // Our own grade-1 echo for view 1 -- records an active lock (0 nonmatching so far).
    let chain = direct_chain(&mut lm, author_c, 1).await;
    let c_ref = block_ref(&chain[0]);
    agb.enter(1, now, &mut lm, &mut rep);
    let proposal_u = ViewProposal { view: 1, c: vec![c_ref.clone()], t: Vec::new(), m: None };
    let sender_u = proposer_of(1);
    let effects0 = agb.on_propose(sender_u, proposal_u.clone(), now, &mut lm, &mut rep);
    assert!(effects0.iter().any(|e| matches!(e, Effect::BroadcastEcho(_))), "our own positive gate must fire for view 1");
    assert_eq!(agb.lock_active_for_test(1), Some(true));

    // A carrying view (w=4) whose resolution entry targets view 1 with `Core(1,
    // [c_ref], [])` -- NOT the exact matching Full the active lock demands, but (once
    // the lock releases) a payload/grade combination that DOES match our own eventual
    // R_i(1) (which will end up `Mix`-graded below, never `NoReady` -- so `Skip(1)`
    // could never pass MetaOK's outcome-specific check here regardless of the lock;
    // `Core` with the SAME (C, T) as our own view-1 proposal is the entry that is
    // rejected ONLY by the lock rule while active, and passes cleanly once released).
    let chain_w = direct_chain(&mut lm, author_w, 1).await;
    let c_w = block_ref(&chain_w[0]);
    agb.enter(4, now, &mut lm, &mut rep);
    let m = Some(ResolutionEntry::Core(1, vec![c_ref], Vec::new()));
    let proposal_w = ViewProposal { view: 4, c: vec![c_w], t: Vec::new(), m };
    let sender_w = proposer_of(4);
    let effects_w = agb.on_propose(sender_w, proposal_w.clone(), now, &mut lm, &mut rep);
    assert!(
        !effects_w.iter().any(|e| matches!(e, Effect::BroadcastEcho(_))),
        "the active lock must reject this non-matching Core entry -- MetaOK blocks the carrying view's echo"
    );

    // First external, non-matching (grade-0) echo for view 1's own digest: nonmatching
    // count -> 1, still below f+1=2 -- the lock must stay active.
    let effects1 = agb.on_echo(Echo { proposal: proposal_u.clone(), grade: 0, sender: other_a, wish: 0, origin: None }, &mut rep);
    assert_eq!(agb.lock_active_for_test(1), Some(true), "one nonmatching echo (< f+1=2) must not yet release the lock");
    assert!(
        !effects1.iter().any(|e| matches!(e, Effect::BroadcastReady(_))),
        "g1+g0 = 1+1 = 2 is still below the 2f+1=3 ready quorum"
    );

    // Second external, non-matching (grade-0) echo for the SAME digest, from a
    // DIFFERENT sender: this single call crosses BOTH the f+1=2 release threshold
    // (nonmatching now 2) AND the 2f+1=3 ready-quorum threshold (g1+g0 = 1+2 = 3) at
    // once -- exactly the same-event race D6-4's ordering exists to resolve.
    let effects2 = agb.on_echo(Echo { proposal: proposal_u.clone(), grade: 0, sender: other_b, wish: 0, origin: None }, &mut rep);
    assert_eq!(agb.lock_active_for_test(1), Some(false), "the SECOND nonmatching echo must have released the lock");
    let ready = effects2.iter().find_map(|e| match e {
        Effect::BroadcastReady(r) if r.proposal.view == 1 => Some(r.grade),
        _ => None,
    });
    assert_eq!(ready, Some(ReadyGrade::Mix), "neither grade alone reaches quorum (g1=1,g0=2 < 3 each) -- the ready must be Mix");
    // The lock was ALREADY inactive (checked above) by the time this very call
    // produced that Mix ready -- never a stale-active lock coexisting with a
    // contradictory (non-grade-1) ready, on this or any prior call.

    // The previously-blocked carrying view's own gate must now unblock, in exactly the
    // same way `dispatch`'s response arms retry via `recheck_all` in production.
    let effects3 = agb.recheck_all(now, &mut lm, &mut rep);
    assert!(
        effects3.iter().any(|e| matches!(e, Effect::BroadcastEcho(echo) if echo.proposal.view == 4)),
        "once the lock releases, the carrying view's Core(1,...) entry must pass MetaOK and echo"
    );
}

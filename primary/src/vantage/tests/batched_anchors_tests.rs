// PHASE7 (`Parameters::batched_anchors`, signature-free.tex's "Batched resolution
// entries" paragraph) -- items 2-6 of the feature's required test list (item 1, the
// flag-off byte-identity gate, is simply "every pre-existing test in this suite still
// passes", verified by leaving them untouched).
//
// `test_committee()` (n=4, f=1) can never exercise a genuine `k >= 2` batch --
// `agb::batch_cap` floors the vector cap at `f`, which is 1 there. Every test below
// therefore builds its own bigger committee (`Committee::local_benchmark`, n=7, f=2,
// f+1=3, 2f+1=5, n-f=5 -- comfortably enough headroom for a real k=2 batch and its
// surrounding quorums).

use super::common::*;
use super::harness::{advance_time, boot, drain_local, run_to_quiescence, Node};
use crate::vantage::agb::{
    self, AgbEngine, BatchViewProposal, EchoBatch, Outcome, ProposalOut, ResolutionEntry,
};
use crate::vantage::block;
use crate::vantage::control::ControlLog;
use crate::vantage::node::Inbound;
use crate::vantage::resolve::Resolver;
use crate::vantage::Effect;
use config::Committee;
use crypto::PublicKey;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// A 7-party committee (f=2, f+1=3, 2f+1=5, n-f=5) -- `batch_cap` = 2, so a
/// two-target batch is exactly at (not below) the cap, and the surrounding BFT
/// quorums stay small enough for these tests to drive by hand.
fn batch_committee(base_port: u16) -> (Committee, Vec<config::KeyPair>) {
    Committee::local_benchmark(7, 1, base_port)
}

fn setup_engine(
    committee: &Committee,
    name: PublicKey,
    path: &str,
) -> (AgbEngine, crate::vantage::lanes::LaneManager, crate::vantage::repair::Repairer) {
    let agb = new_agb_engine_with_committee(name, committee.clone());
    let (lm, _store) = new_lane_manager_with_committee(name, path, committee.clone());
    let rep = new_repairer_with_committee(name, &lm, committee.clone());
    (agb, lm, rep)
}

/// Directly records, as first-hand statements, that `name` itself echoed Skip(u) and
/// no-readied u -- the minimal state `meta_ok_entry(Skip(u))` needs to pass (bullet 1:
/// both own responses exist; bullet 3: own ready is a no-ready). No `LaneManager`
/// availability is ever consulted for a `Skip` entry, so this alone is enough.
fn make_skip_qualified(agb: &mut AgbEngine, name: PublicKey, u: crate::primary::View) {
    agb.on_echo_skip(u, name);
    agb.on_ready_timer(u);
}

// ============================================================ 2. Echo conjunction

#[tokio::test]
async fn echo_conjunction_one_refusable_coordinate_refuses_the_whole_vector() {
    let (committee, keys) = batch_committee(9300);
    let carrier_sender = agb::proposer(&committee, 5);
    let self_name = keys
        .iter()
        .find(|k| k.name != carrier_sender)
        .expect("a 7-party committee has an observer distinct from the carrier's proposer")
        .name;

    // Case A: u1=1 is echo/ready-qualified for Skip; u2=2 has NO state at all (the
    // refusable coordinate). The carrying view's own C/T are empty, so CoreOK/TipOK
    // are trivially satisfied and the positive gate collapses to exactly `meta_ok`'s
    // conjunction over the two entries.
    {
        let (mut agb, mut lm, mut rep) =
            setup_engine(&committee, self_name, ".db_test_batch_echo_conj_a");
        make_skip_qualified(&mut agb, self_name, 1);
        let proposal = BatchViewProposal {
            view: 5,
            c: Vec::new(),
            t: Vec::new(),
            m: vec![ResolutionEntry::Skip(1), ResolutionEntry::Skip(2)],
        };
        let now = Instant::now();
        agb.enter(5, now, &mut lm, &mut rep);
        let effects = agb.on_propose_batch(carrier_sender, proposal, now, &mut lm, &mut rep);
        assert!(
            !effects.iter().any(|e| matches!(e, Effect::BroadcastEcho(_))),
            "the positive gate must NOT fire while coordinate 2 (u=2) is refusable"
        );
        // The Δ-fallback re-checks the identical conjunction -- it must echo-SKIP the
        // carrying view, never echo it, while even one coordinate still fails MetaOK.
        let effects2 = agb.on_echo_fallback_timer(5, &mut lm, &mut rep);
        assert!(
            effects2
                .iter()
                .any(|e| matches!(e, Effect::BroadcastEchoSkip(v) if *v == 5)),
            "the fallback must echo-skip the whole carrying view"
        );
        assert!(
            !effects2.iter().any(|e| matches!(e, Effect::BroadcastEcho(_))),
            "a refused vector must never partially echo"
        );
    }

    // Case B: the identical vector, but coordinate 2 is no longer refusable (now also
    // echo/ready-qualified) -- the conjunction passes and the carrying view echoes
    // normally, exactly as it would for any ordinary well-formed proposal.
    {
        let (mut agb, mut lm, mut rep) =
            setup_engine(&committee, self_name, ".db_test_batch_echo_conj_b");
        make_skip_qualified(&mut agb, self_name, 1);
        make_skip_qualified(&mut agb, self_name, 2);
        let proposal = BatchViewProposal {
            view: 5,
            c: Vec::new(),
            t: Vec::new(),
            m: vec![ResolutionEntry::Skip(1), ResolutionEntry::Skip(2)],
        };
        let now = Instant::now();
        agb.enter(5, now, &mut lm, &mut rep);
        let effects = agb.on_propose_batch(carrier_sender, proposal, now, &mut lm, &mut rep);
        assert!(
            effects.iter().any(|e| matches!(e, Effect::BroadcastEcho(_))),
            "once every coordinate passes MetaOK, the positive gate must fire"
        );
    }
}

// ============================================================ 3. Ready guard per position

#[tokio::test]
async fn ready_guard_requires_f_plus_1_origin_one_independently_per_position() {
    let (committee, keys) = batch_committee(9310);
    let self_name = keys[0].name;
    let (mut agb, _lm, mut rep) = setup_engine(&committee, self_name, ".db_test_batch_ready_guard");
    let senders: Vec<PublicKey> = keys.iter().map(|k| k.name).collect();

    let proposal = BatchViewProposal {
        view: 10,
        c: Vec::new(),
        t: Vec::new(),
        m: vec![
            ResolutionEntry::Full(1, Vec::new(), Vec::new()),
            ResolutionEntry::Full(2, Vec::new(), Vec::new()),
        ],
    };

    // 5 senders (== the n=7 stake quorum) all grade-1 -- position 0 gets a 1 from
    // every one of them, position 1 gets a 1 from only 2 (< f+1=3). Both positions are
    // non-skip, so `ReadyOK` must withhold the ready while position 1 lags.
    for (i, &sender) in senders.iter().enumerate().take(5) {
        let origin = if i < 2 {
            vec![Some(1), Some(1)]
        } else {
            vec![Some(1), Some(0)]
        };
        let echo = EchoBatch {
            proposal: proposal.clone(),
            grade: 1,
            sender,
            wish: 0,
            origin,
        };
        let effects = agb.on_echo_batch(echo, &mut rep);
        assert!(
            !effects.iter().any(|e| matches!(e, Effect::BroadcastReady(_))),
            "position 1 has fewer than f+1=3 origin-one echoes at sender #{i} -- ready must not fire"
        );
    }

    // A 6th distinct sender also names origin=1 at position 1, bringing IT to f+1=3
    // too -- now both positions clear the guard and the (already quorum-met) ready
    // fires. Monotonicity check folded in: position 0's own count only ever grew.
    let echo6 = EchoBatch {
        proposal: proposal.clone(),
        grade: 1,
        sender: senders[5],
        wish: 0,
        origin: vec![Some(1), Some(1)],
    };
    let effects = agb.on_echo_batch(echo6, &mut rep);
    assert!(
        effects.iter().any(|e| matches!(e, Effect::BroadcastReady(_))),
        "once BOTH positions independently reach f+1 origin-one echoes, the ready must fire"
    );
}

#[tokio::test]
async fn ready_guard_skip_entries_pass_trivially_alongside_a_gated_full_entry() {
    // A companion to the test above: a Skip entry at one position must never itself
    // gate the ready (§4's "skip entries ... pass as today"), even while the OTHER
    // (Full) position is still below its own f+1 threshold.
    let (committee, keys) = batch_committee(9320);
    let self_name = keys[0].name;
    let (mut agb, _lm, mut rep) = setup_engine(&committee, self_name, ".db_test_batch_ready_skip");
    let senders: Vec<PublicKey> = keys.iter().map(|k| k.name).collect();

    let proposal = BatchViewProposal {
        view: 10,
        c: Vec::new(),
        t: Vec::new(),
        m: vec![
            ResolutionEntry::Full(1, Vec::new(), Vec::new()),
            ResolutionEntry::Skip(2),
        ],
    };

    // 5 senders reach quorum, but only 1 of them ever names origin=1 at position 0
    // (well below f+1=3) -- position 1 is Skip, so its own origin bit is always
    // `None`, yet the ready must still be withheld purely because of position 0.
    for (i, &sender) in senders.iter().enumerate().take(5) {
        let origin = if i == 0 {
            vec![Some(1), None]
        } else {
            vec![Some(0), None]
        };
        let echo = EchoBatch {
            proposal: proposal.clone(),
            grade: 1,
            sender,
            wish: 0,
            origin,
        };
        let effects = agb.on_echo_batch(echo, &mut rep);
        assert!(
            !effects.iter().any(|e| matches!(e, Effect::BroadcastReady(_))),
            "position 0 (Full) is still below f+1 -- the Skip at position 1 must not paper over it"
        );
    }
}

// ============================================================ 4. Anchor batch application

#[tokio::test]
async fn anchor_batch_application_resolves_two_targets_in_one_apply_and_ignores_a_later_duplicate() {
    let (committee, keys) = batch_committee(9330);
    let name = keys[3].name;
    let sid = block::session_id(&committee);
    let mut control = ControlLog::new(name, committee.clone(), sid.clone(), TEST_DELTA_MS);
    control.set_max_rounds_for_test(2000);
    let all: Vec<PublicKey> = keys.iter().map(|k| k.name).collect();
    let others: Vec<PublicKey> = all.iter().copied().filter(|pk| *pk != name).collect();

    // ---- Round 1: a batch B_{w=10} resolving {1, 2} -- must apply BOTH in one go ----
    let batch = ProposalOut::Batch(BatchViewProposal {
        view: 10,
        c: Vec::new(),
        t: Vec::new(),
        m: vec![ResolutionEntry::Skip(1), ResolutionEntry::Skip(2)],
    });
    let digest = batch.digest(&sid);

    control.on_completion_reportable(10, batch.clone());
    for &pk in others.iter().take(4) {
        // + our own report (above) = 5 = 2f+1, the submittable threshold.
        control.on_comp_report(10, digest.clone(), pk);
    }

    let leader = control.control_leader(1);
    let mut effects = control.genesis();
    if leader != name {
        let cp = crate::vantage::control::ControlProposal {
            round: 1,
            parent: 0,
            value: Some((10, digest.clone())),
        };
        effects.extend(control.on_control_init(leader, cp, Some(batch.clone())));
    }

    let cp = crate::vantage::control::ControlProposal {
        round: 1,
        parent: 0,
        value: Some((10, digest.clone())),
    };
    let readiers: Vec<PublicKey> = others
        .iter()
        .copied()
        .filter(|pk| *pk != leader)
        .take(4)
        .collect();
    for &pk in &readiers {
        effects.extend(control.on_control_ready(pk, cp.clone()));
    }
    assert!(
        control.is_safe_for_test(1),
        "round 1 must have RB-delivered + marked safe by now"
    );

    let committers: Vec<PublicKey> = others.iter().copied().filter(|pk| *pk != name).take(4).collect();
    for &pk in &committers {
        effects.extend(control.on_control_commit(pk, 1));
    }

    let apply_targets: Vec<crate::primary::View> = effects
        .iter()
        .filter_map(|e| match e {
            Effect::ApplyAnchor(u, _, _) => Some(*u),
            _ => None,
        })
        .collect();
    assert_eq!(
        apply_targets,
        vec![1, 2],
        "one control-log position (round 1's carrier) must apply BOTH targets, in increasing order"
    );
    assert!(control.is_anchor_resolved(1));
    assert!(control.is_anchor_resolved(2));

    // ---- Round 2: a LATER proposal renaming target 1 (a duplicate) plus a genuinely
    // new target 3 -- the duplicate must be ignored (idempotent), the new one applied.
    let dup = ProposalOut::Single(crate::vantage::ViewProposal {
        view: 20,
        c: Vec::new(),
        t: Vec::new(),
        m: Some(ResolutionEntry::Skip(1)),
    });
    let dup_digest = dup.digest(&sid);
    control.on_completion_reportable(20, dup.clone());
    for &pk in others.iter().take(4) {
        control.on_comp_report(20, dup_digest.clone(), pk);
    }

    let round2 = control.curr_round_for_test();
    let leader2 = control.control_leader(round2);
    let cp2 = crate::vantage::control::ControlProposal {
        round: round2,
        parent: round2 - 1,
        value: Some((20, dup_digest.clone())),
    };
    let mut effects2 = Vec::new();
    if leader2 != name {
        effects2.extend(control.on_control_init(leader2, cp2.clone(), Some(dup.clone())));
    }
    let readiers2: Vec<PublicKey> = others
        .iter()
        .copied()
        .filter(|pk| *pk != leader2)
        .take(4)
        .collect();
    for &pk in &readiers2 {
        effects2.extend(control.on_control_ready(pk, cp2.clone()));
    }
    let committers2: Vec<PublicKey> = others.iter().copied().filter(|pk| *pk != name).take(4).collect();
    for &pk in &committers2 {
        effects2.extend(control.on_control_commit(pk, round2));
    }

    let apply_targets2: Vec<crate::primary::View> = effects2
        .iter()
        .filter_map(|e| match e {
            Effect::ApplyAnchor(u, _, _) => Some(*u),
            _ => None,
        })
        .collect();
    assert!(
        apply_targets2.is_empty(),
        "target 1 was already anchored by round 1 -- the duplicate must be ignored, not re-applied: {:?}",
        apply_targets2
    );
}

// ============================================================ 5. Alternation

#[tokio::test]
async fn alternation_fixed_oldest_target_alternates_full_prefix_and_single_entry() {
    let (committee, keys) = batch_committee(9340);
    let self_name = keys[0].name;
    let mut agb = new_agb_engine_with_committee(self_name, committee.clone());
    let mut resolver = Resolver::new(committee.size(), TEST_DELTA_MS);
    let senders: Vec<PublicKey> = keys.iter().map(|k| k.name).take(5).collect();

    // Both u=1 and u=2 reach the >= 2f+1=5 no-ready census that justifies Skip, and
    // stay justified for the whole test (nothing here ever "resolves" them -- the
    // `resolved` closure always answers `false`).
    for &u in &[1u64, 2u64] {
        for &s in &senders {
            agb.on_noready(u, s);
        }
    }

    let w = 10u64;
    let mut now = Instant::now();
    let mut recovery_lengths = Vec::new();
    for _ in 0..8 {
        let entries = resolver.decide_prefix(&agb, w, now, |_| false);
        if !entries.is_empty() {
            recovery_lengths.push(entries.len());
        }
        // D7-1's in-flight marker is 12*delta; advance well past it every call so a
        // chosen target's suppression never masks the NEXT call's own scan.
        now += Duration::from_millis(TEST_DELTA_MS * 13);
    }

    assert_eq!(
        recovery_lengths,
        vec![2, 1, 2, 1],
        "per fixed oldest target (u=1), successive recovery attempts must alternate \
         full-prefix (k=2) and single-entry (k=1)"
    );
    // The internal toggle itself: still pinned to u=1 (never reset, since u=1 was
    // never resolved), and the NEXT attempt would be full-prefix again (mirrors the
    // observed [.., 1] tail above -- one more flip due next).
    assert_eq!(resolver.alternation_state_for_test(), (Some(1), false));
}

// ============================================================ 6. End-to-end

const E2E_MAX_VIEWS: crate::primary::View = 10;

/// Shared setup for the end-to-end comparison below: an n=7 committee with 2 ADJACENT
/// committee-order authorities crashed before boot, so their respective proposer
/// turns land on two CONSECUTIVE views (round-robin over adjacent indices) -- the
/// "burst of >= 2 consecutive views with silent proposers" the spec's test item asks
/// for. Returns the live nodes' outbox-driven cluster already past both dead views'
/// echo/no-ready deadlines (mirrors `crash_fault_tests.rs`'s single-dead-view setup,
/// extended to two).
async fn boot_cluster_with_two_dead_adjacent_proposers(
    committee: &Committee,
    keys: &[config::KeyPair],
    db_prefix: &str,
    batched_anchors: bool,
) -> (Vec<Node>, VecDeque<(usize, Inbound)>, Instant, Vec<usize>, crate::primary::View) {
    let mut nodes: Vec<Node> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| {
            Node::new_with_committee(
                k.name,
                &format!("{}_{}", db_prefix, i),
                E2E_MAX_VIEWS,
                committee.clone(),
            )
            .with_batched_anchors(batched_anchors)
        })
        .collect();
    let now = Instant::now();
    let mut outbox: VecDeque<(usize, Inbound)> = VecDeque::new();

    for i in 0..nodes.len() {
        let (_, effects) = nodes[i].lm.publish_own(std::collections::BTreeMap::new()).await;
        drain_local(&mut nodes, i, effects, now, &mut outbox);
    }
    run_to_quiescence(&mut nodes, &mut outbox, now).await;

    // Committee-order indices 1 and 2 (adjacent) -> views (1+1)=2 and (2+1)=3 are
    // BOTH dead and CONSECUTIVE (view v's proposer is committee-order index (v-1)%n).
    let dead_view: crate::primary::View = 2;
    let dead_names = [
        agb::proposer(committee, dead_view),
        agb::proposer(committee, dead_view + 1),
    ];
    assert_ne!(
        dead_names[0], dead_names[1],
        "two DISTINCT adjacent-index authorities must be dead for a 2-view burst"
    );
    for dead_name in dead_names {
        let idx = nodes.iter().position(|n| n.name == dead_name).unwrap();
        nodes[idx].alive = false;
    }
    let live: Vec<usize> = (0..nodes.len())
        .filter(|&i| nodes[i].alive)
        .collect();
    assert_eq!(live.len(), 5, "n=7, f=2 -- exactly n-f=5 correct parties remain");

    boot(&mut nodes, now, &mut outbox).await;
    for &i in &live {
        assert!(nodes[i].frontier.is_active(dead_view + 1));
    }

    let theta_echo = nodes[live[0]].agb.theta_echo();
    let theta_ready = nodes[live[0]].agb.theta_ready();
    advance_time(&mut nodes, &mut outbox, now + theta_echo + Duration::from_millis(1)).await;
    advance_time(&mut nodes, &mut outbox, now + theta_ready + Duration::from_millis(1)).await;
    run_to_quiescence(&mut nodes, &mut outbox, now + theta_ready + Duration::from_millis(1)).await;

    for &i in &live {
        assert!(
            nodes[i].frontier.is_active(dead_view + 2),
            "entry must have proceeded past the whole 2-view burst"
        );
    }

    (nodes, outbox, now, live, dead_view)
}

#[tokio::test]
async fn e2e_batched_burst_resolved_by_one_carrier_output_matches_flag_off() {
    let (committee, keys) = batch_committee(9350);

    // ---------------- Flag ON: one batched carrier resolves BOTH dead views ----------------
    let (mut nodes_on, mut outbox_on, now_on, live_on, dead_view) =
        boot_cluster_with_two_dead_adjacent_proposers(
            &committee,
            &keys,
            ".db_test_e2e_batch_on",
            true,
        )
        .await;

    let carrying_view: crate::primary::View = 1000;
    let carrier_name = agb::proposer(&committee, carrying_view);
    let carrier_idx = live_on
        .iter()
        .find(|&&i| nodes_on[i].name == carrier_name)
        .copied()
        .expect("a live party must lead the carrying view");
    let entries = {
        let node = &mut nodes_on[carrier_idx];
        let agb = &node.agb;
        let control = &node.control;
        let resolved = |u: crate::primary::View| agb.is_sealed(u) || control.is_anchor_resolved(u);
        node.resolver.decide_prefix(agb, carrying_view, now_on, resolved); // consume the data-only bit
        node.resolver.decide_prefix(agb, carrying_view, now_on, resolved)
    };
    assert_eq!(
        entries,
        vec![
            ResolutionEntry::Skip(dead_view),
            ResolutionEntry::Skip(dead_view + 1)
        ],
        "one recovery turn must carry BOTH dead views -- the burst needs only ONE carrier \
         (fewer anchor applications than the 2 unresolved views), not two"
    );

    let c_ref = nodes_on[carrier_idx]
        .lm
        .c_candidate(&keys[0].name)
        .expect("seeded C candidate");
    let batch_proposal = BatchViewProposal {
        view: carrying_view,
        c: vec![c_ref],
        t: Vec::new(),
        m: entries,
    };

    for &i in &live_on {
        let effects = nodes_on[i].enter_view_effects(carrying_view, now_on);
        drain_local(&mut nodes_on, i, effects, now_on, &mut outbox_on);
    }
    run_to_quiescence(&mut nodes_on, &mut outbox_on, now_on).await;
    for &i in &live_on {
        outbox_on.push_back((
            i,
            Inbound::Propose(ProposalOut::Batch(batch_proposal.clone())),
        ));
    }
    run_to_quiescence(&mut nodes_on, &mut outbox_on, now_on).await;

    let control_timeout = nodes_on[live_on[0]].control.control_round_timeout();
    let mut ct = now_on;
    for _ in 0..6 {
        ct += control_timeout + Duration::from_millis(1);
        advance_time(&mut nodes_on, &mut outbox_on, ct).await;
        run_to_quiescence(&mut nodes_on, &mut outbox_on, ct).await;
    }

    for &i in &live_on {
        assert_eq!(nodes_on[i].agb.sealed_for_test(dead_view), Some(Outcome::Skip));
        assert_eq!(
            nodes_on[i].agb.sealed_for_test(dead_view + 1),
            Some(Outcome::Skip)
        );
        assert!(nodes_on[i].cursor.next_view() > dead_view + 1);
    }
    let output_on = nodes_on[live_on[0]].cursor.output_log().to_vec();
    for &i in &live_on[1..] {
        assert_eq!(nodes_on[i].cursor.output_log(), output_on.as_slice());
    }

    // ---------------- Flag OFF: the SAME burst, resolved via TWO single-entry carriers ----------------
    let (mut nodes_off, mut outbox_off, now_off, live_off, dead_view_off) =
        boot_cluster_with_two_dead_adjacent_proposers(
            &committee,
            &keys,
            ".db_test_e2e_batch_off",
            false,
        )
        .await;
    assert_eq!(dead_view_off, dead_view, "identical crash pattern on both runs");

    for (carrying_view, target) in [(1000u64, dead_view), (1001u64, dead_view + 1)] {
        let carrier_name = agb::proposer(&committee, carrying_view);
        let carrier_idx = live_off
            .iter()
            .find(|&&i| nodes_off[i].name == carrier_name)
            .copied()
            .expect("a live party must lead the carrying view");
        let m = {
            let node = &mut nodes_off[carrier_idx];
            let agb = &node.agb;
            let control = &node.control;
            let resolved = |u: crate::primary::View| agb.is_sealed(u) || control.is_anchor_resolved(u);
            node.resolver.decide(agb, carrying_view, now_off, resolved);
            node.resolver.decide(agb, carrying_view, now_off, resolved)
        };
        assert_eq!(m, Some(ResolutionEntry::Skip(target)));
        let c_ref = nodes_off[carrier_idx]
            .lm
            .c_candidate(&keys[0].name)
            .expect("seeded C candidate");
        let proposal = crate::vantage::ViewProposal {
            view: carrying_view,
            c: vec![c_ref],
            t: Vec::new(),
            m,
        };
        for &i in &live_off {
            let effects = nodes_off[i].enter_view_effects(carrying_view, now_off);
            drain_local(&mut nodes_off, i, effects, now_off, &mut outbox_off);
        }
        run_to_quiescence(&mut nodes_off, &mut outbox_off, now_off).await;
        for &i in &live_off {
            outbox_off.push_back((i, Inbound::Propose(ProposalOut::Single(proposal.clone()))));
        }
        run_to_quiescence(&mut nodes_off, &mut outbox_off, now_off).await;

        let control_timeout = nodes_off[live_off[0]].control.control_round_timeout();
        let mut ct = now_off;
        for _ in 0..6 {
            ct += control_timeout + Duration::from_millis(1);
            advance_time(&mut nodes_off, &mut outbox_off, ct).await;
            run_to_quiescence(&mut nodes_off, &mut outbox_off, ct).await;
        }
    }

    for &i in &live_off {
        assert_eq!(nodes_off[i].agb.sealed_for_test(dead_view), Some(Outcome::Skip));
        assert_eq!(
            nodes_off[i].agb.sealed_for_test(dead_view + 1),
            Some(Outcome::Skip)
        );
        assert!(nodes_off[i].cursor.next_view() > dead_view + 1);
    }
    let output_off = nodes_off[live_off[0]].cursor.output_log().to_vec();
    for &i in &live_off[1..] {
        assert_eq!(nodes_off[i].cursor.output_log(), output_off.as_slice());
    }

    // The headline comparison: identical committed output whether the burst rode in
    // on one batched carrier (flag on) or two single-entry ones (flag off).
    assert_eq!(
        output_on, output_off,
        "committed output must be identical regardless of how many carriers resolved the burst"
    );
}

// signature-free.tex's "Batched resolution entries" paragraph, narrowed by the
// 704fb29 audit (par:batched-anchors) to a skip-only, manifest-free vector -- "a
// vector with a full or core entry is malformed; those outcomes use one general
// entry". Unconditional protocol behavior (no flag): `Resolver::decide_prefix` is the
// only recovery-turn entry point production wiring ever calls.
//
// `test_committee()` (n=4, f=1) can never exercise a genuine `k >= 2` batch --
// `agb::batch_cap` floors the vector cap at `f`, which is 1 there. Every test below
// therefore builds its own bigger committee (`Committee::local_benchmark`, n=7, f=2,
// f+1=3, 2f+1=5, n-f=5 -- comfortably enough headroom for a real k=2 batch and its
// surrounding quorums).
//
// Two tests from this file's PHASE7 original (`ready_guard_requires_f_plus_1_
// origin_one_independently_per_position`, `ready_guard_skip_entries_pass_trivially_
// alongside_a_gated_full_entry`) tested a full/core-carrying batch vector's
// per-position `ReadyOK`/origin-vector machinery -- a shape `formed_batch` now
// rejects outright (`ResolutionEntry::Full`/`Core` can never appear in a `Batch`
// proposal's `m`), and `EchoBatch` no longer even has an origin field to construct
// them with. Removed rather than adapted: there is no longer a well-formed vector to
// build around a full/core coordinate, so the scenario they drove is unreachable.
//
// A third test (`e2e_batched_burst_resolved_by_one_carrier_output_matches_flag_off`,
// plus its dedicated `boot_cluster_with_two_dead_adjacent_proposers` helper) compared
// `Node::with_batched_anchors(true)` against `with_batched_anchors(false)` -- pure
// flag-plumbing coverage that no longer applies now that `batched_anchors` and
// `skip_votes` are both unconditional. It is also not adaptable into an equivalent
// always-on test: with the grounded post-ready skip vote (par:skip-seal) also always
// on, a clean 2-adjacent-crash burst with n-f=5=2f+1 live parties seals BOTH views via
// the vote quorum before either ever reaches an unresolved state a batched CARRIER
// could still attach to (cor:crash-skip) -- the anchor path this test specifically
// exercised is unreachable for this exact scenario. Removed; the vote-based
// equivalent is `skip_vote_tests.rs`'s integration test, and this file's remaining
// `anchor_batch_application_resolves_two_targets_in_one_apply_and_ignores_a_later_
// duplicate` test still directly covers batched anchor APPLICATION (bypassing AGB
// entirely, so it is unaffected by the vote shortcut).

use super::common::*;
use crate::vantage::agb::{self, AgbEngine, BatchViewProposal, ProposalOut, ResolutionEntry};
use crate::vantage::block;
use crate::vantage::control::ControlLog;
use crate::vantage::resolve::Resolver;
use crate::vantage::Effect;
use config::Committee;
use crypto::PublicKey;
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
) -> (
    AgbEngine,
    crate::vantage::lanes::LaneManager,
    crate::vantage::repair::Repairer,
) {
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

// ============================================================ Echo conjunction

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
            !effects
                .iter()
                .any(|e| matches!(e, Effect::BroadcastEcho(_))),
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
            !effects2
                .iter()
                .any(|e| matches!(e, Effect::BroadcastEcho(_))),
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
            effects
                .iter()
                .any(|e| matches!(e, Effect::BroadcastEcho(_))),
            "once every coordinate passes MetaOK, the positive gate must fire"
        );
    }
}

// ============================================================ Anchor batch application

#[tokio::test]
async fn anchor_batch_application_resolves_two_targets_in_one_apply_and_ignores_a_later_duplicate()
{
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

    let committers: Vec<PublicKey> = others
        .iter()
        .copied()
        .filter(|pk| *pk != name)
        .take(4)
        .collect();
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
    let committers2: Vec<PublicKey> = others
        .iter()
        .copied()
        .filter(|pk| *pk != name)
        .take(4)
        .collect();
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

// ============================================================ Alternation

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

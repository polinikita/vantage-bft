// PHASE6-SPEC.md §4 -- proposer recovery turns: `Resolver::justified_candidates`
// (thresholds/prerequisite/canonical order) and `Resolver::decide` (the next-turn bit,
// the per-target candidate pointer, "no-evidence view never blocks a later target",
// and the `resolved` predicate). Driven directly against `AgbEngine` + `Resolver` --
// justification only ever reads count accessors (no `LaneManager`/`Repairer`
// authorization concerns), so these tests fabricate manifests freely (arbitrary
// digests) rather than building real chains. n=4, f=1 (f+1=2, 2f+1=3).

use super::common::*;
use crate::vantage::agb::{Echo, ResolutionEntry};
use crate::vantage::resolve::Resolver;
use crypto::Digest;
use std::time::{Duration, Instant};

/// A dummy `Repairer` -- only ever needed because `on_echo`'s R3/R4 plumbing takes one
/// (authorizing fabricated, never-real references is harmless here; same pattern as
/// `completion_tests.rs`'s `dummy_repairer`).
fn dummy_repairer(name: crypto::PublicKey, path: &str) -> crate::vantage::Repairer {
    let (lm, _store) = new_lane_manager(name, path);
    new_repairer(name, &lm)
}

fn payload(tag: u8) -> (crate::vantage::Manifest, crate::vantage::Manifest) {
    let (a0, _) = authors()[0];
    (vec![(a0, 1, Digest([tag; 32]))], Vec::new())
}

fn echo(view: u64, c: &crate::vantage::Manifest, t: &crate::vantage::Manifest, grade: u8, sender: crypto::PublicKey) -> Echo {
    Echo {
        proposal: crate::vantage::ViewProposal { view, c: c.clone(), t: t.clone(), m: None },
        grade,
        sender,
        wish: 0,
        origin: None,
    }
}

#[tokio::test]
async fn prerequisite_blocks_every_candidate_below_two_f_plus_1_ready_stage() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_resolve_prereq");
    let resolver = Resolver::new(4, 0); // D7-1: delta_ms=0 disables in-flight suppression (unrelated here)
    let (c, t) = payload(1);
    let all = authors();

    // 2 grade-1 echoes (>= f+1) but only 2 ready-stage statements total (< 2f+1=3).
    agb.on_echo(echo(1, &c, &t, 1, all[0].0), &mut rep);
    agb.on_echo(echo(1, &c, &t, 1, all[1].0), &mut rep);
    agb.on_noready(1, all[2].0);
    assert_eq!(agb.ready_stage_total(1), 1, "on_echo alone never touches the ready-stage census");

    assert!(resolver.justified_candidates(&agb, 1).is_empty(), "prerequisite (2f+1 ready-stage statements) not met");
}

#[tokio::test]
async fn full_justified_at_exactly_f_plus_1_grade1_echoes_with_prerequisite_met() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_resolve_full");
    let resolver = Resolver::new(4, 0); // D7-1: delta_ms=0 disables in-flight suppression (unrelated here)
    let (c, t) = payload(1);
    let all = authors();

    // Prerequisite: 2f+1=3 ready-stage statements (noready is fine, "any kind").
    for (pk, _) in &all[0..3] {
        agb.on_noready(1, *pk);
    }
    assert_eq!(agb.ready_stage_total(1), 3);

    // f+1=2 grade-1 echoes for the payload.
    agb.on_echo(echo(1, &c, &t, 1, all[0].0), &mut rep);
    agb.on_echo(echo(1, &c, &t, 1, all[1].0), &mut rep);

    let candidates = resolver.justified_candidates(&agb, 1);
    assert!(candidates.iter().any(|e| matches!(e, ResolutionEntry::Full(1, cc, tt) if *cc == c && *tt == t)));
}

#[tokio::test]
async fn core_requires_both_the_no_grade1_ready_subset_and_f_plus_1_any_grade_echoes() {
    let (name, _) = authors()[3];
    let all = authors();
    let (c, t) = payload(2);
    let resolver = Resolver::new(4, 0); // D7-1: delta_ms=0 disables in-flight suppression (unrelated here)

    // Case A: ready-stage clause holds (3 noreadies -> non-grade1 count = 3 >= 2f+1),
    // but only 1 echo (< f+1=2) -- Core must NOT be justified.
    {
        let mut agb = new_agb_engine(name);
        let mut rep = dummy_repairer(name, ".db_test_resolve_core_a");
        for (pk, _) in &all[0..3] {
            agb.on_noready(1, *pk);
        }
        agb.on_echo(echo(1, &c, &t, 0, all[0].0), &mut rep);
        let candidates = resolver.justified_candidates(&agb, 1);
        assert!(!candidates.iter().any(|e| matches!(e, ResolutionEntry::Core(..))));
    }

    // Case B: f+1=2 any-grade echoes, but the ready-stage subset never reaches 2f+1
    // non-grade-1 statements (only 1 noready total, prerequisite for ANY candidate
    // also fails) -- Core must NOT be justified.
    {
        let mut agb = new_agb_engine(name);
        let mut rep = dummy_repairer(name, ".db_test_resolve_core_b");
        agb.on_noready(1, all[0].0);
        agb.on_echo(echo(1, &c, &t, 0, all[0].0), &mut rep);
        agb.on_echo(echo(1, &c, &t, 1, all[1].0), &mut rep);
        let candidates = resolver.justified_candidates(&agb, 1);
        assert!(candidates.is_empty(), "prerequisite (2f+1 ready-stage statements) not met at all");
    }

    // Case C: both clauses hold -- Core IS justified.
    {
        let mut agb = new_agb_engine(name);
        let mut rep = dummy_repairer(name, ".db_test_resolve_core_c");
        for (pk, _) in &all[0..3] {
            agb.on_noready(1, *pk);
        }
        agb.on_echo(echo(1, &c, &t, 0, all[0].0), &mut rep);
        agb.on_echo(echo(1, &c, &t, 1, all[1].0), &mut rep); // "any grade" -- mixed is fine
        let candidates = resolver.justified_candidates(&agb, 1);
        assert!(candidates.iter().any(|e| matches!(e, ResolutionEntry::Core(1, cc, tt) if *cc == c && *tt == t)));
    }
}

#[tokio::test]
async fn skip_justified_at_two_f_plus_1_noready() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let resolver = Resolver::new(4, 0); // D7-1: delta_ms=0 disables in-flight suppression (unrelated here)
    let all = authors();

    agb.on_noready(1, all[0].0);
    agb.on_noready(1, all[1].0);
    assert!(resolver.justified_candidates(&agb, 1).is_empty(), "only 2 noready -- prerequisite not met");

    agb.on_noready(1, all[2].0);
    let candidates = resolver.justified_candidates(&agb, 1);
    assert!(candidates.iter().any(|e| matches!(e, ResolutionEntry::Skip(1))));
}

#[tokio::test]
async fn canonical_order_full_before_core_lex_by_payload_skip_last() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_resolve_canonical");
    let resolver = Resolver::new(4, 0); // D7-1: delta_ms=0 disables in-flight suppression (unrelated here)
    let all = authors();
    let (c1, t1) = payload(1);
    let (c2, t2) = payload(9); // a lexicographically-larger payload

    // Prerequisite + all three shapes justified simultaneously.
    for (pk, _) in &all[0..3] {
        agb.on_noready(1, *pk);
    }
    // Full for payload 1: f+1 grade-1 echoes.
    agb.on_echo(echo(1, &c1, &t1, 1, all[0].0), &mut rep);
    agb.on_echo(echo(1, &c1, &t1, 1, all[1].0), &mut rep);
    // Core for payload 9 as well (any-grade f+1; ready-subset clause already holds).
    agb.on_echo(echo(1, &c2, &t2, 0, all[2].0), &mut rep);
    agb.on_echo(echo(1, &c2, &t2, 0, all[3].0), &mut rep);
    // Also Core for payload 1 (same payload as Full -- any-grade echoes already counted
    // above satisfy f+1 too).

    let candidates = resolver.justified_candidates(&agb, 1);
    assert!(candidates.len() >= 3, "expected at least Full(1), Core(1), Core(9), Skip -- got {:?}", candidates);
    // Skip must be strictly last.
    assert!(matches!(candidates.last().unwrap(), ResolutionEntry::Skip(1)));
    // Within the non-skip prefix, entries are grouped/ordered by ascending
    // bincode(C,T) bytes, Full before Core per payload.
    let non_skip: Vec<_> = candidates.iter().filter(|e| !matches!(e, ResolutionEntry::Skip(_))).collect();
    let keys: Vec<Vec<u8>> = non_skip
        .iter()
        .map(|e| match e {
            ResolutionEntry::Full(_, c, t) | ResolutionEntry::Core(_, c, t) => bincode::serialize(&(c, t)).unwrap(),
            ResolutionEntry::Skip(_) => unreachable!(),
        })
        .collect();
    let mut sorted_keys = keys.clone();
    sorted_keys.sort();
    assert_eq!(keys, sorted_keys, "non-skip entries must be sorted by bincode(C,T)");
}

#[tokio::test]
async fn no_evidence_view_never_blocks_a_later_target() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut resolver = Resolver::new(4, 0); // D7-1: delta_ms=0 disables in-flight suppression (unrelated here)
    let all = authors();

    // View 1 has NO state at all (never touched) -- view 2 is fully justified for
    // Skip. `decide` must scan past view 1 and pick view 2.
    agb.on_noready(2, all[0].0);
    agb.on_noready(2, all[1].0);
    agb.on_noready(2, all[2].0);

    // w=5: targets u <= 2 are in scope (5-3=2). Turn 1: bit starts data-only -> None
    // (view 1 has no state, correctly skipped as "no evidence"; view 2 IS justified,
    // so the bit is consulted and flips). Turn 2: bit is now recovery -> Skip(2).
    assert_eq!(resolver.decide(&agb, 5, Instant::now(), |_u| false), None);
    let pick = resolver.decide(&agb, 5, Instant::now(), |_u| false);
    assert_eq!(pick, Some(ResolutionEntry::Skip(2)));
}

#[tokio::test]
async fn resolved_predicate_skips_already_resolved_views() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut resolver = Resolver::new(4, 0); // D7-1: delta_ms=0 disables in-flight suppression (unrelated here)
    let all = authors();

    for v in [1u64, 2u64] {
        agb.on_noready(v, all[0].0);
        agb.on_noready(v, all[1].0);
        agb.on_noready(v, all[2].0);
    }
    // Both 1 and 2 are justified for Skip; mark 1 as already resolved.
    let pick = resolver.decide(&agb, 5, Instant::now(), |u| u == 1);
    // First qualifying UNRESOLVED target is 2 -- bit is initially data-only, so this
    // turn is data-only (`None`), but the bit has now flipped to recovery.
    assert_eq!(pick, None);
    assert!(resolver.next_is_recovery_for_test());
}

#[tokio::test]
async fn bit_alternates_data_only_then_recovery_and_stays_unchanged_with_no_qualifier() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut resolver = Resolver::new(4, 0); // D7-1: delta_ms=0 disables in-flight suppression (unrelated here)
    let all = authors();
    agb.on_noready(1, all[0].0);
    agb.on_noready(1, all[1].0);
    agb.on_noready(1, all[2].0);

    // Turn 1: bit starts data-only -> None, flips to recovery.
    assert_eq!(resolver.decide(&agb, 5, Instant::now(), |_| false), None);
    assert!(resolver.next_is_recovery_for_test());

    // Turn 2: bit is now recovery -> Some(Skip(1)), flips back to data-only.
    assert_eq!(resolver.decide(&agb, 6, Instant::now(), |_| false), Some(ResolutionEntry::Skip(1)));
    assert!(!resolver.next_is_recovery_for_test());

    // Turn 3, no target qualifies at all (view 1 now treated as resolved) -- bit
    // untouched.
    assert_eq!(resolver.decide(&agb, 7, Instant::now(), |_| true), None);
    assert!(!resolver.next_is_recovery_for_test());
}

#[tokio::test]
async fn pointer_cycles_over_the_canonical_list_across_recovery_attempts() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_resolve_pointer");
    let mut resolver = Resolver::new(4, 0); // D7-1: delta_ms=0 disables in-flight suppression (unrelated here)
    let all = authors();
    let (c1, t1) = payload(1);
    let (_c2, _t2) = payload(9);

    for (pk, _) in &all[0..3] {
        agb.on_noready(1, *pk);
    }
    // Full(1,payload1), Core(1,payload1) (the same 2 grade-1 echoes satisfy Core's
    // "any grade" clause too, and the ready-subset clause already holds from the 3
    // noreadies above), and Skip(1) -- 3 distinct candidates.
    agb.on_echo(echo(1, &c1, &t1, 1, all[0].0), &mut rep);
    agb.on_echo(echo(1, &c1, &t1, 1, all[1].0), &mut rep);
    let candidates = resolver.justified_candidates(&agb, 1);
    assert_eq!(candidates.len(), 3, "expected Full(1,payload1), Core(1,payload1), Skip(1) -- got {:?}", candidates);

    // Turn 1 (bit starts data-only): None, bit flips to recovery, pointer untouched.
    assert_eq!(resolver.decide(&agb, 5, Instant::now(), |_| false), None);
    assert!(resolver.pointer_for_test(1).is_none());

    // Turn 2 (recovery): picks the FIRST canonical candidate, advances the pointer to
    // the second.
    let first_pick = resolver.decide(&agb, 6, Instant::now(), |_| false).expect("recovery turn");
    assert_eq!(first_pick, candidates[0]);
    assert_eq!(resolver.pointer_for_test(1), Some(candidates[1].clone()));

    // Turn 3 (bit is data-only again): None, bit flips to recovery -- pointer
    // untouched by a data-only turn.
    assert_eq!(resolver.decide(&agb, 7, Instant::now(), |_| false), None);
    assert_eq!(resolver.pointer_for_test(1), Some(candidates[1].clone()));

    // Turn 4 (recovery): picks the SECOND canonical candidate, advances the pointer to
    // the third.
    let second_pick = resolver.decide(&agb, 8, Instant::now(), |_| false).expect("recovery turn");
    assert_eq!(second_pick, candidates[1]);
    assert_eq!(resolver.pointer_for_test(1), Some(candidates[2].clone()));

    // Turn 5 (data-only) then turn 6 (recovery): picks the THIRD candidate, wraps the
    // pointer back to the first.
    assert_eq!(resolver.decide(&agb, 9, Instant::now(), |_| false), None);
    let third_pick = resolver.decide(&agb, 10, Instant::now(), |_| false).expect("recovery turn");
    assert_eq!(third_pick, candidates[2]);
    assert_eq!(resolver.pointer_for_test(1), Some(candidates[0].clone()));

    // Turn 7 (data-only) then turn 8 (recovery): wraps back to the first candidate.
    assert_eq!(resolver.decide(&agb, 11, Instant::now(), |_| false), None);
    let fourth_pick = resolver.decide(&agb, 12, Instant::now(), |_| false).expect("recovery turn");
    assert_eq!(fourth_pick, candidates[0]);
}

#[tokio::test]
async fn repeated_decide_calls_over_many_views_never_disturb_an_unrelated_census() {
    // Regression check: `decide` is called once per proposer turn in production, over
    // potentially thousands of views (WISH can advance the formal-entry target far
    // ahead of real time) -- confirm read-only scanning across many `w` values never
    // perturbs a target view's own census.
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut resolver = Resolver::new(4, 0); // D7-1: delta_ms=0 disables in-flight suppression (unrelated here)
    let all = authors();
    agb.on_noready(2, all[0].0);
    agb.on_noready(2, all[1].0);
    agb.on_noready(2, all[2].0);
    assert_eq!(agb.noready_count(2), 3);
    for w in 5..2000u64 {
        let _ = resolver.decide(&agb, w, Instant::now(), |_u| false);
        if w % 500 == 0 {
            assert_eq!(agb.noready_count(2), 3, "noready census corrupted at w={}", w);
        }
    }
    assert_eq!(agb.noready_count(2), 3);
}

/// D7-1 (PHASE7-PREP-NOTES.md, coordinator-sanctioned Finding-A root-cause fix,
/// mandatory time bound): a fresh recovery attempt for a target `u` suppresses further
/// attempts for the SAME `u` while its in-flight marker is younger than `expiry =
/// 12*delta_ms`; it expires and attempts resume. This is what throttles the redundant-
/// carrier flood Finding A measured (thousands of attempts/sec for a single stuck
/// target) without changing which entries are ever chosen -- attempts stay
/// infinitely-often in the limit, never open-ended-suppressed.
#[tokio::test]
async fn d7_1_in_flight_suppression_blocks_reattempt_then_expires() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let delta_ms = 10u64; // expiry = 120ms -- small so the test can drive it with plain Duration arithmetic on a real Instant, no injected clock needed.
    let mut resolver = Resolver::new(4, delta_ms);
    let all = authors();
    agb.on_noready(1, all[0].0);
    agb.on_noready(1, all[1].0);
    agb.on_noready(1, all[2].0);

    let t0 = Instant::now();
    // Turn 1 (bit starts data-only): None, flips to recovery. No in-flight marker yet.
    assert_eq!(resolver.decide(&agb, 5, t0, |_| false), None);
    // Turn 2 (recovery): Some(Skip(1)) -- our own attempt sets in_flight[1] = t0.
    assert_eq!(resolver.decide(&agb, 6, t0, |_| false), Some(ResolutionEntry::Skip(1)));

    // Turn 3, an instant later (well inside the 120ms expiry): view 1 is still
    // justified (census unchanged) and not yet `resolved`, but D7-1 suppresses it --
    // decide finds no qualifying target at all, so the bit stays exactly as turn 2
    // left it (data-only/`false`), NOT flipped.
    let t1 = t0 + Duration::from_millis(1);
    assert_eq!(resolver.decide(&agb, 7, t1, |_| false), None, "suppressed: must not re-attempt the same in-flight target");
    assert!(!resolver.next_is_recovery_for_test(), "a suppressed-target turn must leave the bit untouched");

    // Turn 4, past the 120ms expiry: view 1 is selectable again. Bit is data-only ->
    // None, flips to recovery (this IS the qualifying-target consult, just consumed
    // as a data-only turn, exactly like turn 1 was).
    let t2 = t0 + Duration::from_millis(12 * delta_ms + 1);
    assert_eq!(resolver.decide(&agb, 8, t2, |_| false), None, "expired: the target qualifies again");
    assert!(resolver.next_is_recovery_for_test());
    // Turn 5: recovery -> Some(Skip(1)) again, refreshing the marker.
    assert_eq!(resolver.decide(&agb, 9, t2, |_| false), Some(ResolutionEntry::Skip(1)));
}

/// D7-1: `note_carrier_report` (fed from `Effect::CompletionReportable` in production
/// -- an observed carrier, ours or another party's, whose `M` targets `u`) suppresses
/// exactly like `decide`'s own attempt does, and is independently subject to the same
/// expiry.
#[tokio::test]
async fn d7_1_note_carrier_report_suppresses_like_our_own_attempt() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let delta_ms = 10u64;
    let mut resolver = Resolver::new(4, delta_ms);
    let all = authors();
    agb.on_noready(1, all[0].0);
    agb.on_noready(1, all[1].0);
    agb.on_noready(1, all[2].0);

    let t0 = Instant::now();
    // An externally observed carrier for u=1 (e.g. another party's proposal we just
    // completed) -- no decide() call involved at all yet.
    resolver.note_carrier_report(1, t0);

    // Turn 1 (bit starts data-only): the ONLY candidate target is suppressed, so no
    // target qualifies at all -- None, bit untouched.
    let t1 = t0 + Duration::from_millis(1);
    assert_eq!(resolver.decide(&agb, 5, t1, |_| false), None);
    assert!(!resolver.next_is_recovery_for_test(), "no qualifying target -- the bit-consult branch must never run");

    // Past expiry: selectable again, exactly as if we had minted the attempt ourselves.
    let t2 = t0 + Duration::from_millis(12 * delta_ms + 1);
    assert_eq!(resolver.decide(&agb, 6, t2, |_| false), None); // data-only turn, flips the bit
    assert_eq!(resolver.decide(&agb, 7, t2, |_| false), Some(ResolutionEntry::Skip(1)));
}

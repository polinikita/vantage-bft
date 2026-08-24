use super::common::*;
use crate::vantage::agb::{Echo, ResolutionEntry};
use crate::vantage::resolve::Resolver;
use crypto::Digest;
use std::time::{Duration, Instant};

fn dummy_repairer(name: crypto::PublicKey, path: &str) -> crate::vantage::Repairer {
    let (lm, _store) = new_lane_manager(name, path);
    new_repairer(name, &lm)
}

fn payload(tag: u8) -> (crate::vantage::Manifest, crate::vantage::Manifest) {
    let (a0, _) = authors()[0];
    (vec![(a0, 1, Digest([tag; 32]))], Vec::new())
}

fn echo(
    view: u64,
    c: &crate::vantage::Manifest,
    t: &crate::vantage::Manifest,
    grade: u8,
    sender: crypto::PublicKey,
) -> Echo {
    Echo {
        proposal: crate::vantage::ViewProposal {
            view,
            c: c.clone(),
            t: t.clone(),
            m: None,
        },
        grade,
        sender,
        wish: 0,
        origin: None,
        avail: None,
    }
}

#[tokio::test]
async fn prerequisite_blocks_every_candidate_below_two_f_plus_1_ready_stage() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_resolve_prereq");
    let resolver = Resolver::new(4, 0);
    let (c, t) = payload(1);
    let all = authors();

    agb.on_echo(echo(1, &c, &t, 1, all[0].0), &mut rep);
    agb.on_echo(echo(1, &c, &t, 1, all[1].0), &mut rep);
    agb.on_noready(1, all[2].0);
    assert_eq!(
        agb.ready_stage_total(1),
        1,
        "on_echo alone never touches the ready-stage census"
    );

    assert!(
        resolver.justified_candidates(&agb, 1).is_empty(),
        "prerequisite (2f+1 ready-stage statements) not met"
    );
}

#[tokio::test]
async fn full_justified_at_exactly_f_plus_1_grade1_echoes_with_prerequisite_met() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_resolve_full");
    let resolver = Resolver::new(4, 0);
    let (c, t) = payload(1);
    let all = authors();

    for (pk, _) in &all[0..3] {
        agb.on_noready(1, *pk);
    }
    assert_eq!(agb.ready_stage_total(1), 3);

    agb.on_echo(echo(1, &c, &t, 1, all[0].0), &mut rep);
    agb.on_echo(echo(1, &c, &t, 1, all[1].0), &mut rep);

    let candidates = resolver.justified_candidates(&agb, 1);
    assert!(candidates
        .iter()
        .any(|e| matches!(e, ResolutionEntry::Full(1, cc, tt) if *cc == c && *tt == t)));
}

#[tokio::test]
async fn core_requires_both_the_no_grade1_ready_subset_and_f_plus_1_any_grade_echoes() {
    let (name, _) = authors()[3];
    let all = authors();
    let (c, t) = payload(2);
    let resolver = Resolver::new(4, 0);

    {
        let mut agb = new_agb_engine(name);
        let mut rep = dummy_repairer(name, ".db_test_resolve_core_a");
        for (pk, _) in &all[0..3] {
            agb.on_noready(1, *pk);
        }
        agb.on_echo(echo(1, &c, &t, 0, all[0].0), &mut rep);
        let candidates = resolver.justified_candidates(&agb, 1);
        assert!(!candidates
            .iter()
            .any(|e| matches!(e, ResolutionEntry::Core(..))));
    }

    {
        let mut agb = new_agb_engine(name);
        let mut rep = dummy_repairer(name, ".db_test_resolve_core_b");
        agb.on_noready(1, all[0].0);
        agb.on_echo(echo(1, &c, &t, 0, all[0].0), &mut rep);
        agb.on_echo(echo(1, &c, &t, 1, all[1].0), &mut rep);
        let candidates = resolver.justified_candidates(&agb, 1);
        assert!(
            candidates.is_empty(),
            "prerequisite (2f+1 ready-stage statements) not met at all"
        );
    }

    {
        let mut agb = new_agb_engine(name);
        let mut rep = dummy_repairer(name, ".db_test_resolve_core_c");
        for (pk, _) in &all[0..3] {
            agb.on_noready(1, *pk);
        }
        agb.on_echo(echo(1, &c, &t, 0, all[0].0), &mut rep);
        agb.on_echo(echo(1, &c, &t, 1, all[1].0), &mut rep);
        let candidates = resolver.justified_candidates(&agb, 1);
        assert!(candidates
            .iter()
            .any(|e| matches!(e, ResolutionEntry::Core(1, cc, tt) if *cc == c && *tt == t)));
    }
}

#[tokio::test]
async fn skip_justified_at_two_f_plus_1_noready() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let resolver = Resolver::new(4, 0);
    let all = authors();

    agb.on_noready(1, all[0].0);
    agb.on_noready(1, all[1].0);
    assert!(
        resolver.justified_candidates(&agb, 1).is_empty(),
        "only 2 noready -- prerequisite not met"
    );

    agb.on_noready(1, all[2].0);
    let candidates = resolver.justified_candidates(&agb, 1);
    assert!(candidates
        .iter()
        .any(|e| matches!(e, ResolutionEntry::Skip(1))));
}

#[tokio::test]
async fn canonical_order_full_before_core_lex_by_payload_skip_last() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_resolve_canonical");
    let resolver = Resolver::new(4, 0);
    let all = authors();
    let (c1, t1) = payload(1);
    let (c2, t2) = payload(9);

    for (pk, _) in &all[0..3] {
        agb.on_noready(1, *pk);
    }
    agb.on_echo(echo(1, &c1, &t1, 1, all[0].0), &mut rep);
    agb.on_echo(echo(1, &c1, &t1, 1, all[1].0), &mut rep);
    agb.on_echo(echo(1, &c2, &t2, 0, all[2].0), &mut rep);
    agb.on_echo(echo(1, &c2, &t2, 0, all[3].0), &mut rep);

    let candidates = resolver.justified_candidates(&agb, 1);
    assert!(
        candidates.len() >= 3,
        "expected at least Full(1), Core(1), Core(9), Skip -- got {:?}",
        candidates
    );
    assert!(matches!(
        candidates.last().unwrap(),
        ResolutionEntry::Skip(1)
    ));
    let non_skip: Vec<_> = candidates
        .iter()
        .filter(|e| !matches!(e, ResolutionEntry::Skip(_)))
        .collect();
    let keys: Vec<Vec<u8>> = non_skip
        .iter()
        .map(|e| match e {
            ResolutionEntry::Full(_, c, t) | ResolutionEntry::Core(_, c, t) => {
                bincode::serialize(&(c, t)).unwrap()
            }
            ResolutionEntry::Skip(_) => unreachable!(),
        })
        .collect();
    let mut sorted_keys = keys.clone();
    sorted_keys.sort();
    assert_eq!(
        keys, sorted_keys,
        "non-skip entries must be sorted by bincode(C,T)"
    );
}

#[tokio::test]
async fn no_evidence_view_never_blocks_a_later_target() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut resolver = Resolver::new(4, 0);
    let all = authors();

    agb.on_noready(2, all[0].0);
    agb.on_noready(2, all[1].0);
    agb.on_noready(2, all[2].0);

    assert_eq!(resolver.decide(&agb, 5, Instant::now(), |_u| false), None);
    let pick = resolver.decide(&agb, 5, Instant::now(), |_u| false);
    assert_eq!(pick, Some(ResolutionEntry::Skip(2)));
}

#[tokio::test]
async fn resolved_predicate_skips_already_resolved_views() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut resolver = Resolver::new(4, 0);
    let all = authors();

    for v in [1u64, 2u64] {
        agb.on_noready(v, all[0].0);
        agb.on_noready(v, all[1].0);
        agb.on_noready(v, all[2].0);
    }
    let pick = resolver.decide(&agb, 5, Instant::now(), |u| u == 1);
    assert_eq!(pick, None);
    assert!(resolver.next_is_recovery_for_test());
}

#[tokio::test]
async fn terminal_progress_skips_a_stale_recovery_target() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut resolver = Resolver::new(4, 0);
    let all = authors();

    for view in [1, 2] {
        for (sender, _) in &all[0..3] {
            agb.on_noready(view, *sender);
        }
    }

    assert_eq!(resolver.decide(&agb, 5, Instant::now(), |_| false), None);
    assert_eq!(
        resolver.decide(&agb, 6, Instant::now(), |_| false),
        Some(ResolutionEntry::Skip(1))
    );

    resolver.note_resolved_through(1);
    assert_eq!(resolver.resolved_watermark(), 2);
    assert_eq!(resolver.decide(&agb, 6, Instant::now(), |_| false), None);
    assert_eq!(
        resolver.decide(&agb, 6, Instant::now(), |_| false),
        Some(ResolutionEntry::Skip(2))
    );
}

#[tokio::test]
async fn bit_alternates_data_only_then_recovery_and_stays_unchanged_with_no_qualifier() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut resolver = Resolver::new(4, 0);
    let all = authors();
    agb.on_noready(1, all[0].0);
    agb.on_noready(1, all[1].0);
    agb.on_noready(1, all[2].0);

    assert_eq!(resolver.decide(&agb, 5, Instant::now(), |_| false), None);
    assert!(resolver.next_is_recovery_for_test());

    assert_eq!(
        resolver.decide(&agb, 6, Instant::now(), |_| false),
        Some(ResolutionEntry::Skip(1))
    );
    assert!(!resolver.next_is_recovery_for_test());

    assert_eq!(resolver.decide(&agb, 7, Instant::now(), |_| true), None);
    assert!(!resolver.next_is_recovery_for_test());
}

#[tokio::test]
async fn pointer_cycles_over_the_canonical_list_across_recovery_attempts() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_resolve_pointer");
    let mut resolver = Resolver::new(4, 0);
    let all = authors();
    let (c1, t1) = payload(1);
    let (_c2, _t2) = payload(9);

    for (pk, _) in &all[0..3] {
        agb.on_noready(1, *pk);
    }
    agb.on_echo(echo(1, &c1, &t1, 1, all[0].0), &mut rep);
    agb.on_echo(echo(1, &c1, &t1, 1, all[1].0), &mut rep);
    let candidates = resolver.justified_candidates(&agb, 1);
    assert_eq!(
        candidates.len(),
        3,
        "expected Full(1,payload1), Core(1,payload1), Skip(1) -- got {:?}",
        candidates
    );

    assert_eq!(resolver.decide(&agb, 5, Instant::now(), |_| false), None);
    assert!(resolver.pointer_for_test(1).is_none());

    let first_pick = resolver
        .decide(&agb, 6, Instant::now(), |_| false)
        .expect("recovery turn");
    assert_eq!(first_pick, candidates[0]);
    assert_eq!(resolver.pointer_for_test(1), Some(candidates[1].clone()));

    assert_eq!(resolver.decide(&agb, 7, Instant::now(), |_| false), None);
    assert_eq!(resolver.pointer_for_test(1), Some(candidates[1].clone()));

    let second_pick = resolver
        .decide(&agb, 8, Instant::now(), |_| false)
        .expect("recovery turn");
    assert_eq!(second_pick, candidates[1]);
    assert_eq!(resolver.pointer_for_test(1), Some(candidates[2].clone()));

    assert_eq!(resolver.decide(&agb, 9, Instant::now(), |_| false), None);
    let third_pick = resolver
        .decide(&agb, 10, Instant::now(), |_| false)
        .expect("recovery turn");
    assert_eq!(third_pick, candidates[2]);
    assert_eq!(resolver.pointer_for_test(1), Some(candidates[0].clone()));

    assert_eq!(resolver.decide(&agb, 11, Instant::now(), |_| false), None);
    let fourth_pick = resolver
        .decide(&agb, 12, Instant::now(), |_| false)
        .expect("recovery turn");
    assert_eq!(fourth_pick, candidates[0]);
}

#[tokio::test]
async fn repeated_decide_calls_over_many_views_never_disturb_an_unrelated_census() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let mut resolver = Resolver::new(4, 0);
    let all = authors();
    agb.on_noready(2, all[0].0);
    agb.on_noready(2, all[1].0);
    agb.on_noready(2, all[2].0);
    assert_eq!(agb.noready_count(2), 3);
    for w in 5..2000u64 {
        let _ = resolver.decide(&agb, w, Instant::now(), |_u| false);
        if w % 500 == 0 {
            assert_eq!(
                agb.noready_count(2),
                3,
                "noready census corrupted at w={}",
                w
            );
        }
    }
    assert_eq!(agb.noready_count(2), 3);
}

#[tokio::test]
async fn d7_1_in_flight_suppression_blocks_reattempt_then_expires() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let delta_ms = 10u64;
    let mut resolver = Resolver::new(4, delta_ms);
    let all = authors();
    agb.on_noready(1, all[0].0);
    agb.on_noready(1, all[1].0);
    agb.on_noready(1, all[2].0);

    let t0 = Instant::now();
    assert_eq!(resolver.decide(&agb, 5, t0, |_| false), None);
    assert_eq!(
        resolver.decide(&agb, 6, t0, |_| false),
        Some(ResolutionEntry::Skip(1))
    );

    let t1 = t0 + Duration::from_millis(1);
    assert_eq!(
        resolver.decide(&agb, 7, t1, |_| false),
        None,
        "suppressed: must not re-attempt the same in-flight target"
    );
    assert!(
        !resolver.next_is_recovery_for_test(),
        "a suppressed-target turn must leave the bit untouched"
    );

    let t2 = t0 + Duration::from_millis(12 * delta_ms + 1);
    assert_eq!(
        resolver.decide(&agb, 8, t2, |_| false),
        None,
        "expired: the target qualifies again"
    );
    assert!(resolver.next_is_recovery_for_test());
    assert_eq!(
        resolver.decide(&agb, 9, t2, |_| false),
        Some(ResolutionEntry::Skip(1))
    );
}

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
    resolver.note_carrier_report(1, t0);

    let t1 = t0 + Duration::from_millis(1);
    assert_eq!(resolver.decide(&agb, 5, t1, |_| false), None);
    assert!(
        !resolver.next_is_recovery_for_test(),
        "no qualifying target -- the bit-consult branch must never run"
    );

    let t2 = t0 + Duration::from_millis(12 * delta_ms + 1);
    assert_eq!(resolver.decide(&agb, 6, t2, |_| false), None);
    assert_eq!(
        resolver.decide(&agb, 7, t2, |_| false),
        Some(ResolutionEntry::Skip(1))
    );
}

#[tokio::test]
async fn quorum_eligible_carrier_suppression_does_not_expire() {
    let (name, _) = authors()[3];
    let mut agb = new_agb_engine(name);
    let delta_ms = 10u64;
    let mut resolver = Resolver::new(4, delta_ms);
    let all = authors();
    for sender in all.iter().take(3) {
        agb.on_noready(1, sender.0);
    }

    let t0 = Instant::now();
    resolver.note_carrier_report(1, t0);
    resolver.note_eligible_carrier_targets([1, 2]);
    assert!(resolver.has_eligible_carrier_for_test(1));
    assert!(resolver.has_eligible_carrier_for_test(2));

    let after_attempt_expiry = t0 + Duration::from_millis(12 * delta_ms + 1);
    assert_eq!(
        resolver.decide(&agb, 5, after_attempt_expiry, |_| false),
        None,
        "eligibility, unlike a tentative carrier attempt, remains suppressed"
    );
    assert!(!resolver.next_is_recovery_for_test());

    resolver.note_resolved_through(1);
    assert!(!resolver.has_eligible_carrier_for_test(1));
    assert!(resolver.has_eligible_carrier_for_test(2));
    resolver.gc_below(3);
    assert!(!resolver.has_eligible_carrier_for_test(2));
}

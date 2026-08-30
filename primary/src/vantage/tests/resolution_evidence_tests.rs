use super::common::*;
use crate::vantage::agb::{Echo, ResolutionEntry};
use crate::vantage::resolution_evidence::ResolutionEvidence;
use crypto::{Digest, PublicKey};

fn repairer(name: PublicKey, path: &str) -> crate::vantage::Repairer {
    let (lm, _store) = new_lane_manager(name, path);
    new_repairer(name, &lm)
}

fn payload(tag: u8) -> (crate::vantage::Manifest, crate::vantage::Manifest) {
    let author = authors()[0].0;
    (vec![(author, 1, Digest([tag; 32]))], Vec::new())
}

fn echo(
    view: u64,
    core: &crate::vantage::Manifest,
    tip: &crate::vantage::Manifest,
    grade: u8,
    sender: PublicKey,
) -> Echo {
    Echo {
        proposal: crate::vantage::ViewProposal {
            view,
            c: core.clone(),
            t: tip.clone(),
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
async fn candidate_extraction_requires_a_ready_stage_quorum() {
    let name = authors()[3].0;
    let mut agb = new_agb_engine(name);
    let mut rep = repairer(name, ".db_test_resolve_prereq");
    let resolver = ResolutionEvidence::new(4, 0);
    let (core, tip) = payload(1);
    let all = authors();

    agb.on_echo(echo(1, &core, &tip, 1, all[0].0), &mut rep);
    agb.on_echo(echo(1, &core, &tip, 1, all[1].0), &mut rep);
    agb.on_noready(1, all[2].0);

    assert!(resolver.justified_candidates(&agb, 1).is_empty());
}

#[tokio::test]
async fn full_candidate_needs_f_plus_one_grade_one_echoes() {
    let name = authors()[3].0;
    let mut agb = new_agb_engine(name);
    let mut rep = repairer(name, ".db_test_resolve_full");
    let resolver = ResolutionEvidence::new(4, 0);
    let (core, tip) = payload(1);
    let all = authors();

    for (sender, _) in &all[0..3] {
        agb.on_noready(1, *sender);
    }
    agb.on_echo(echo(1, &core, &tip, 1, all[0].0), &mut rep);
    assert!(!resolver
        .justified_candidates(&agb, 1)
        .iter()
        .any(|entry| matches!(entry, ResolutionEntry::Full(..))));

    agb.on_echo(echo(1, &core, &tip, 1, all[1].0), &mut rep);
    assert!(resolver
        .justified_candidates(&agb, 1)
        .iter()
        .any(|entry| matches!(entry, ResolutionEntry::Full(1, c, t) if c == &core && t == &tip)));
}

#[tokio::test]
async fn core_candidate_needs_both_ready_and_echo_predicates() {
    let name = authors()[3].0;
    let all = authors();
    let (core, tip) = payload(2);
    let resolver = ResolutionEvidence::new(4, 0);
    let mut agb = new_agb_engine(name);
    let mut rep = repairer(name, ".db_test_resolve_core");

    for (sender, _) in &all[0..3] {
        agb.on_noready(1, *sender);
    }
    agb.on_echo(echo(1, &core, &tip, 0, all[0].0), &mut rep);
    assert!(!resolver
        .justified_candidates(&agb, 1)
        .iter()
        .any(|entry| matches!(entry, ResolutionEntry::Core(..))));

    agb.on_echo(echo(1, &core, &tip, 1, all[1].0), &mut rep);
    assert!(resolver
        .justified_candidates(&agb, 1)
        .iter()
        .any(|entry| matches!(entry, ResolutionEntry::Core(1, c, t) if c == &core && t == &tip)));
}

#[tokio::test]
async fn skip_candidate_needs_a_noready_quorum() {
    let name = authors()[3].0;
    let mut agb = new_agb_engine(name);
    let resolver = ResolutionEvidence::new(4, 0);
    let all = authors();

    agb.on_noready(1, all[0].0);
    agb.on_noready(1, all[1].0);
    assert!(resolver.justified_candidates(&agb, 1).is_empty());
    agb.on_noready(1, all[2].0);
    assert!(resolver
        .justified_candidates(&agb, 1)
        .contains(&ResolutionEntry::Skip(1)));
}

#[tokio::test]
async fn canonical_order_places_non_skip_values_before_skip() {
    let name = authors()[3].0;
    let mut agb = new_agb_engine(name);
    let mut rep = repairer(name, ".db_test_resolve_canonical");
    let resolver = ResolutionEvidence::new(4, 0);
    let all = authors();
    let (core, tip) = payload(1);

    for (sender, _) in &all[0..3] {
        agb.on_noready(1, *sender);
    }
    agb.on_echo(echo(1, &core, &tip, 1, all[0].0), &mut rep);
    agb.on_echo(echo(1, &core, &tip, 1, all[1].0), &mut rep);

    let candidates = resolver.justified_candidates(&agb, 1);
    assert!(matches!(candidates.last(), Some(ResolutionEntry::Skip(1))));
    assert!(matches!(
        candidates.first(),
        Some(ResolutionEntry::Full(..))
    ));
}

#[tokio::test]
async fn full_selection_bar_ignores_refinable_mix_responses() {
    let name = authors()[3].0;
    let mut agb = new_agb_engine(name);
    let mut rep = repairer(name, ".db_test_resolve_full_bar");
    let resolver = ResolutionEvidence::new(4, 0);
    let (core, tip) = payload(3);
    let all = authors();

    let ready = |grade, sender| crate::vantage::Ready {
        proposal: crate::vantage::ViewProposal {
            view: 1,
            c: core.clone(),
            t: tip.clone(),
            m: None,
        },
        grade,
        sender,
        wish: 0,
    };

    agb.on_noready(1, all[0].0);
    agb.on_ready(ready(crate::vantage::ReadyGrade::Zero, all[1].0), &mut rep);
    agb.on_ready(ready(crate::vantage::ReadyGrade::Mix, all[2].0), &mut rep);
    assert_eq!(agb.ready_stage_non_grade1_count(1), 3);
    assert_eq!(
        agb.ready_stage_zero_or_noready_count(1),
        2,
        "a provisional MIX may still refine to grade 1 and must not count"
    );
    assert!(!resolver.full_selection_barred(&agb, 1));

    agb.on_ready(ready(crate::vantage::ReadyGrade::Zero, all[2].0), &mut rep);
    assert_eq!(agb.ready_stage_zero_or_noready_count(1), 3);
    assert!(
        resolver.full_selection_barred(&agb, 1),
        "a refined grade-0 closes the response and joins the permanent-rejecter census"
    );
}

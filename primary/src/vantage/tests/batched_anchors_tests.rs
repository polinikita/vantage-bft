use super::common::*;
use crate::vantage::agb::{self, AgbEngine, BatchViewProposal, ProposalOut, ResolutionEntry};
use crate::vantage::block;
use crate::vantage::control::ControlLog;
use crate::vantage::resolve::Resolver;
use crate::vantage::Effect;
use config::Committee;
use crypto::PublicKey;
use std::time::{Duration, Instant};

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

fn make_skip_qualified(
    agb: &mut AgbEngine,
    rep: &mut crate::vantage::repair::Repairer,
    name: PublicKey,
    u: crate::primary::View,
) {
    agb.on_echo_skip(u, name);
    agb.on_ready_timer(u, rep);
}

#[tokio::test]
async fn echo_conjunction_one_refusable_coordinate_refuses_the_whole_vector() {
    let (committee, keys) = batch_committee(9300);
    let carrier_sender = agb::proposer(&committee, 5);
    let self_name = keys
        .iter()
        .find(|k| k.name != carrier_sender)
        .expect("a 7-party committee has an observer distinct from the carrier's proposer")
        .name;

    {
        let (mut agb, mut lm, mut rep) =
            setup_engine(&committee, self_name, ".db_test_batch_echo_conj_a");
        make_skip_qualified(&mut agb, &mut rep, self_name, 1);
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

    {
        let (mut agb, mut lm, mut rep) =
            setup_engine(&committee, self_name, ".db_test_batch_echo_conj_b");
        make_skip_qualified(&mut agb, &mut rep, self_name, 1);
        make_skip_qualified(&mut agb, &mut rep, self_name, 2);
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

    let batch = ProposalOut::Batch(BatchViewProposal {
        view: 10,
        c: Vec::new(),
        t: Vec::new(),
        m: vec![ResolutionEntry::Skip(1), ResolutionEntry::Skip(2)],
    });
    let digest = batch.digest(&sid);

    control.on_completion_reportable(10, batch.clone());
    for &pk in others.iter().take(4) {
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

#[tokio::test]
async fn alternation_fixed_oldest_target_alternates_full_prefix_and_single_entry() {
    let (committee, keys) = batch_committee(9340);
    let self_name = keys[0].name;
    let mut agb = new_agb_engine_with_committee(self_name, committee.clone());
    let mut resolver = Resolver::new(committee.size(), TEST_DELTA_MS);
    let senders: Vec<PublicKey> = keys.iter().map(|k| k.name).take(5).collect();

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
        now += Duration::from_millis(TEST_DELTA_MS * 13);
    }

    assert_eq!(
        recovery_lengths,
        vec![2, 1, 2, 1],
        "per fixed oldest target (u=1), successive recovery attempts must alternate \
         full-prefix (k=2) and single-entry (k=1)"
    );
    assert_eq!(resolver.alternation_state_for_test(), (Some(1), false));
}

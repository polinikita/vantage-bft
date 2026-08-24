use super::common::*;
use crate::vantage::agb::{
    self, AgbEngine, BatchViewProposal, EchoBatch, ProposalOut, ResolutionEntry,
};
use crate::vantage::claim::{AvailClaim, ClaimRef};
use crate::vantage::Effect;
use config::Committee;
use crypto::PublicKey;
use std::time::Instant;

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
async fn batch_echo_carries_claims_over_c_then_t() {
    let (committee, keys) = batch_committee(9290);
    let name = keys[0].name;
    let sender = keys[1].name;
    let author = keys[2].name;
    let (mut agb, _lm, mut rep) = setup_engine(&committee, name, ".db_test_batch_echo_avail_claim");
    let reference = (author, 1, crypto::Digest([91; 32]));
    let proposal = BatchViewProposal {
        view: 5,
        c: vec![reference.clone()],
        t: Vec::new(),
        m: vec![ResolutionEntry::Skip(1), ResolutionEntry::Skip(2)],
    };
    assert!(ProposalOut::Batch(proposal.clone()).formed(&committee));
    let mut claim = AvailClaim::with_capacity(1);
    claim.set_at_tip(0);

    let effects = agb.on_echo_batch(
        EchoBatch {
            proposal,
            grade: 1,
            sender,
            wish: 0,
            avail: Some(claim),
        },
        &mut rep,
    );
    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::AvailClaimed(s, claims)
            if *s == sender && claims == &vec![ClaimRef::Exact(reference.clone())]
    )));
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

// signature-free.tex's "Grounded post-ready skip" (par:skip-seal) / the persistent
// per-target resolution-stance paragraph / lem:skip-seal -- unconditional protocol
// behavior (no flag): the stance trit, the vote rule, and the vote-quorum seal are
// always active.

use super::common::*;
use super::harness::{advance_time, boot, drain_local, run_to_quiescence, Node};
use crate::vantage::agb::{Echo, Outcome, ResolutionEntry, Stance, ViewProposal};
use crate::vantage::node::Inbound;
use crate::vantage::Effect;
use config::Committee;
use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

// ============================================================ 1. Exclusivity, both orders

/// A vote (z(1): Free -> SkipVoted) then a later non-skip carrier for the same
/// target: the carrier must be refused.
#[tokio::test]
async fn exclusivity_vote_then_non_skip_carrier_is_refused() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (author_w, _) = authors()[1];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_skipvote_excl_vote_then_carrier");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);
    let now = Instant::now();

    // u=1 is silent for this party: own E_i(1) via the absolute echo deadline (no
    // proposal ever fixed for it), then a first-hand 2f+1=3 echo-skip census (our own
    // plus two remote), then own R_i(1) via the absolute ready deadline -- which
    // completes the vote gate and fires the vote.
    agb.enter(1, now, &mut lm, &mut rep);
    agb.on_echo_absolute_timer(1, &mut rep);
    let others: Vec<_> = authors().into_iter().filter(|(pk, _)| *pk != self_name).collect();
    agb.on_echo_skip(1, others[0].0);
    agb.on_echo_skip(1, others[1].0);
    let effects = agb.on_ready_timer(1);
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::BroadcastSkipVote(1))),
        "test setup: own no-ready completing the gate must fire the vote"
    );
    assert_eq!(agb.stance_for_test(1), Stance::SkipVoted);

    // A Full(1, c_ref, []) carrier now arrives, over LOCALLY AVAILABLE content
    // (published directly into this party's own lm) -- absent the stance, this would
    // pass every other MetaOK check (own R_i(1) = NoReady trivially satisfies the
    // Full branch's own-ready check).
    let chain_c = direct_chain(&mut lm, author_c, 1).await;
    let c_ref = block_ref(&chain_c[0]);
    let chain_w = direct_chain(&mut lm, author_w, 1).await;
    let c_w = block_ref(&chain_w[0]);
    agb.enter(4, now, &mut lm, &mut rep);
    let proposal_w = ViewProposal {
        view: 4,
        c: vec![c_w],
        t: Vec::new(),
        m: Some(ResolutionEntry::Full(1, vec![c_ref], Vec::new())),
    };
    let effects_w = agb.on_propose(proposer_of(4), proposal_w, now, &mut lm, &mut rep);
    assert!(
        !effects_w.iter().any(|e| matches!(e, Effect::BroadcastEcho(_))),
        "a skip-voted stance must permanently reject a later non-skip carrier"
    );
}

/// A non-skip carrier endorsement (z(1): Free -> NonSkip) then an otherwise-complete
/// vote gate: the vote must be refused.
#[tokio::test]
async fn exclusivity_endorse_then_vote_gate_refuses() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (author_w, _) = authors()[1];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_skipvote_excl_carrier_then_vote");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);
    let now = Instant::now();

    // u=1 silent for this party: own E_i(1) (echo-skip) then own R_i(1) (no-ready) --
    // but the echo-skip quorum is not yet complete (only our own so far), so no vote
    // fires yet.
    agb.enter(1, now, &mut lm, &mut rep);
    agb.on_echo_absolute_timer(1, &mut rep);
    let effects = agb.on_ready_timer(1);
    assert!(!effects.iter().any(|e| matches!(e, Effect::BroadcastSkipVote(_))));

    // A Full(1, c_ref, []) carrier arrives and is accepted -- own R_i(1) = NoReady
    // trivially satisfies the Full branch's own-ready check, and z(1) is still free.
    let chain_c = direct_chain(&mut lm, author_c, 1).await;
    let c_ref = block_ref(&chain_c[0]);
    let chain_w = direct_chain(&mut lm, author_w, 1).await;
    let c_w = block_ref(&chain_w[0]);
    agb.enter(4, now, &mut lm, &mut rep);
    let proposal_w = ViewProposal {
        view: 4,
        c: vec![c_w],
        t: Vec::new(),
        m: Some(ResolutionEntry::Full(1, vec![c_ref], Vec::new())),
    };
    let effects_w = agb.on_propose(proposer_of(4), proposal_w, now, &mut lm, &mut rep);
    assert!(
        effects_w.iter().any(|e| matches!(e, Effect::BroadcastEcho(_))),
        "test setup: the carrier must be accepted, claiming z(1) = NonSkip"
    );
    assert_eq!(agb.stance_for_test(1), Stance::NonSkip);

    // NOW complete the echo-skip quorum -- the vote gate must refuse, since z(1) is
    // no longer free.
    let others: Vec<_> = authors().into_iter().filter(|(pk, _)| *pk != self_name).collect();
    let e1 = agb.on_echo_skip(1, others[0].0);
    assert!(!e1.iter().any(|e| matches!(e, Effect::BroadcastSkipVote(_))));
    let e2 = agb.on_echo_skip(1, others[1].0);
    assert!(
        !e2.iter().any(|e| matches!(e, Effect::BroadcastSkipVote(_))),
        "z(1) = NonSkip must permanently block the vote gate, even once the echo-skip \
         quorum completes"
    );
    assert_eq!(agb.stance_for_test(1), Stance::NonSkip);
}

// ============================================================ 2. Vote gate conjuncts

/// A complete echo-skip quorum alone, with our own ready-stage response still
/// pending, must never fire the vote.
#[tokio::test]
async fn vote_gate_requires_own_noready_first() {
    let (self_name, _) = authors()[3];
    let mut agb = new_agb_engine(self_name);
    let others: Vec<_> = authors().into_iter().filter(|(pk, _)| *pk != self_name).collect();

    for (sender, _) in &others {
        let effects = agb.on_echo_skip(1, *sender);
        assert!(
            !effects.iter().any(|e| matches!(e, Effect::BroadcastSkipVote(_))),
            "own R_i(1) is still pending -- the vote gate must not fire regardless of \
             the echo-skip census"
        );
    }
    assert_eq!(
        agb.stance_for_test(1),
        Stance::Free,
        "no vote fired -- and hence no stance claim -- without our own no-ready"
    );
}

/// Own no-ready alone, with the echo-skip census below quorum, must never fire the
/// vote; completing the quorum then does.
#[tokio::test]
async fn vote_gate_requires_echo_skip_quorum() {
    let (self_name, _) = authors()[3];
    let mut agb = new_agb_engine(self_name);
    let others: Vec<_> = authors().into_iter().filter(|(pk, _)| *pk != self_name).collect();

    let effects = agb.on_ready_timer(1);
    assert!(!effects.iter().any(|e| matches!(e, Effect::BroadcastSkipVote(_))));

    let e1 = agb.on_echo_skip(1, others[0].0);
    assert!(!e1.iter().any(|e| matches!(e, Effect::BroadcastSkipVote(_))));
    let e2 = agb.on_echo_skip(1, others[1].0);
    assert!(
        !e2.iter().any(|e| matches!(e, Effect::BroadcastSkipVote(_))),
        "2 < 2f+1=3 -- the vote must not fire yet"
    );
    assert_eq!(agb.stance_for_test(1), Stance::Free);

    let e3 = agb.on_echo_skip(1, others[2].0);
    assert!(
        e3.iter().any(|e| matches!(e, Effect::BroadcastSkipVote(1))),
        "completing the 2f+1=3 census (with own no-ready already durable) must fire the vote"
    );
    assert_eq!(agb.stance_for_test(1), Stance::SkipVoted);
}

/// At most one vote per target, ever: a 6th distinct echo-skip arriving after the
/// 2f+1=5 quorum already fired the vote (n=7, f=2) must never fire a second one.
#[tokio::test]
async fn vote_sent_at_most_once_per_target() {
    let (committee, keys) = Committee::local_benchmark(7, 1, 9500);
    let self_name = keys[0].name;
    let mut agb = new_agb_engine_with_committee(self_name, committee);
    let others: Vec<_> = keys.iter().skip(1).map(|k| k.name).collect();
    assert_eq!(others.len(), 6);

    agb.on_ready_timer(1);
    for &sender in &others[..4] {
        let effects = agb.on_echo_skip(1, sender);
        assert!(!effects.iter().any(|e| matches!(e, Effect::BroadcastSkipVote(_))));
    }
    let e5 = agb.on_echo_skip(1, others[4]);
    assert!(
        e5.iter().any(|e| matches!(e, Effect::BroadcastSkipVote(1))),
        "the 5th distinct echo-skip completes 2f+1=5 -- the vote must fire"
    );
    let e6 = agb.on_echo_skip(1, others[5]);
    assert!(
        !e6.iter().any(|e| matches!(e, Effect::BroadcastSkipVote(_))),
        "at most one vote per target, ever"
    );
}

// ============================================================ 3. Quorum seal

/// 2f+1 first-hand `SkipVote` statements (first-per-author; a duplicate from an
/// already-counted author is ignored) submit `gskip` to the try-seal arbiter; a
/// later, compatible anchor submission for the same view is idempotent.
#[tokio::test]
async fn quorum_seal_via_votes_first_per_author_duplicates_ignored_idempotent_with_anchor() {
    let (self_name, _) = authors()[3];
    let mut agb = new_agb_engine(self_name);
    let others: Vec<_> = authors().into_iter().filter(|(pk, _)| *pk != self_name).collect();

    let e0 = agb.on_skip_vote(1, others[0].0);
    assert!(!e0.iter().any(|e| matches!(e, Effect::Sealed(..))));

    let dup = agb.on_skip_vote(1, others[0].0);
    assert!(
        dup.is_empty(),
        "a duplicate SkipVote from an already-counted author changes nothing"
    );

    let e1 = agb.on_skip_vote(1, others[1].0);
    assert!(
        !e1.iter().any(|e| matches!(e, Effect::Sealed(..))),
        "2 distinct authors < 2f+1=3 -- must not seal yet"
    );

    let e2 = agb.on_skip_vote(1, others[2].0);
    assert!(
        e2.iter()
            .any(|e| matches!(e, Effect::Sealed(1, Outcome::Skip))),
        "3 distinct authors == 2f+1 -- must submit skip-seal to the try-seal arbiter"
    );
    assert_eq!(agb.sealed_for_test(1), Some(Outcome::Skip));
    assert_eq!(
        agb.skip_vote_count_for_test(1),
        3,
        "the duplicate from others[0] must never have double-counted"
    );

    // Idempotent with a later, compatible anchor submission for the same view.
    let after = agb.submit_anchor(1, Outcome::Skip);
    assert!(
        after.is_empty(),
        "a later compatible anchor submission for an already-sealed view produces no new effect"
    );
}

// ============================================================ 4. Skip entries bypass the stance

/// A skip entry is accepted regardless of an existing `NonSkip` stance (own-noready
/// holding is all it needs), and does not itself change the stance -- a later
/// non-skip carrier may still reuse it.
#[tokio::test]
async fn skip_entry_bypasses_nonskip_stance_and_does_not_change_it() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (author_w, _) = authors()[1];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_skip_bypasses_stance");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);
    let now = Instant::now();

    // u=1 silent for this party: own E_i(1) (echo-skip), own R_i(1) (no-ready).
    agb.enter(1, now, &mut lm, &mut rep);
    agb.on_echo_absolute_timer(1, &mut rep);
    agb.on_ready_timer(1);

    let chain_c = direct_chain(&mut lm, author_c, 1).await;
    let c_ref = block_ref(&chain_c[0]);
    let chain_w = direct_chain(&mut lm, author_w, 1).await;
    let c_w = block_ref(&chain_w[0]);

    // First: a Full(1, c_ref, []) carrier (view 4) passes -- z(1): Free -> NonSkip.
    agb.enter(4, now, &mut lm, &mut rep);
    let proposal4 = ViewProposal {
        view: 4,
        c: vec![c_w.clone()],
        t: Vec::new(),
        m: Some(ResolutionEntry::Full(1, vec![c_ref.clone()], Vec::new())),
    };
    let effects4 = agb.on_propose(proposer_of(4), proposal4, now, &mut lm, &mut rep);
    assert!(
        effects4.iter().any(|e| matches!(e, Effect::BroadcastEcho(_))),
        "test setup: the Full entry must be accepted"
    );
    assert_eq!(agb.stance_for_test(1), Stance::NonSkip);

    // Second: a Skip(1) carrier (view 5) is ALSO accepted -- skip entries neither
    // require nor change the stance.
    agb.enter(5, now, &mut lm, &mut rep);
    let proposal5 = ViewProposal {
        view: 5,
        c: vec![c_w.clone()],
        t: Vec::new(),
        m: Some(ResolutionEntry::Skip(1)),
    };
    let effects5 = agb.on_propose(proposer_of(5), proposal5, now, &mut lm, &mut rep);
    assert!(
        effects5.iter().any(|e| matches!(e, Effect::BroadcastEcho(_))),
        "a skip entry must be accepted regardless of z(1) = NonSkip"
    );
    assert_eq!(
        agb.stance_for_test(1),
        Stance::NonSkip,
        "a skip entry must not change the stance"
    );

    // Third: another Full(1, c_ref, []) carrier (view 6) still passes -- z(1) is
    // reused ("a later non-skip carrier may reuse that stance"), confirming the skip
    // entry above never disturbed it.
    agb.enter(6, now, &mut lm, &mut rep);
    let proposal6 = ViewProposal {
        view: 6,
        c: vec![c_w],
        t: Vec::new(),
        m: Some(ResolutionEntry::Full(1, vec![c_ref], Vec::new())),
    };
    let effects6 = agb.on_propose(proposer_of(6), proposal6, now, &mut lm, &mut rep);
    assert!(
        effects6.iter().any(|e| matches!(e, Effect::BroadcastEcho(_))),
        "z(1) = NonSkip must be reusable by a later non-skip carrier"
    );
    assert_eq!(agb.stance_for_test(1), Stance::NonSkip);
}

// ============================================================ 5. Structural completion-exclusion

/// A party whose ready-stage response for u is a GRADED proposal-ready (own R_i(u)
/// graded via a genuine completion) can never later have R_i(u) = NoReady, so the
/// vote gate is structurally unsatisfiable for it, regardless of the echo-skip
/// census.
#[tokio::test]
async fn vote_gate_structurally_excludes_a_party_that_went_ready() {
    let (self_name, _) = authors()[3];
    let (author_c, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(self_name, ".db_test_vote_gate_excludes_ready_party");
    let mut rep = new_repairer(self_name, &lm);
    let mut agb = new_agb_engine(self_name);
    let now = Instant::now();

    // Drive u=1 to a genuine completion: our own positive gate fires (grade-1 echo),
    // then 2 more matching echoes complete the 2f+1=3 ready quorum -- own R_i(1)
    // becomes a GRADED proposal-ready, never NoReady.
    let chain = direct_chain(&mut lm, author_c, 1).await;
    let c_ref = block_ref(&chain[0]);
    agb.enter(1, now, &mut lm, &mut rep);
    let proposal = ViewProposal {
        view: 1,
        c: vec![c_ref],
        t: Vec::new(),
        m: None,
    };
    let effects = agb.on_propose(proposer_of(1), proposal.clone(), now, &mut lm, &mut rep);
    assert!(
        effects.iter().any(|e| matches!(e, Effect::BroadcastEcho(_))),
        "test setup: own positive gate must fire"
    );
    let others: Vec<_> = authors()
        .into_iter()
        .filter(|(pk, _)| *pk != self_name)
        .collect();
    let mut last = Vec::new();
    for (sender, _) in others.iter().take(2) {
        last = agb.on_echo(
            Echo {
                proposal: proposal.clone(),
                grade: 1,
                sender: *sender,
                wish: 0,
                origin: None,
            },
            &mut rep,
        );
    }
    assert!(
        last.iter().any(|e| matches!(e, Effect::BroadcastReady(_))),
        "test setup: the 2f+1=3 echo quorum must fire our own (graded) proposal-ready"
    );

    // `on_ready_timer` is now structurally a no-op (`ready_sent` already true) -- own
    // R_i(1) can never become NoReady.
    let effects = agb.on_ready_timer(1);
    assert!(effects.is_empty());

    // Even feeding a full 2f+1 echo-skip census for view 1 (contrived, but isolates
    // the "own no-ready missing" conjunct specifically) must never produce a vote.
    for (sender, _) in others.iter().take(3) {
        let e = agb.on_echo_skip(1, *sender);
        assert!(
            !e.iter().any(|eff| matches!(eff, Effect::BroadcastSkipVote(_))),
            "a party that went ready can never satisfy the vote gate"
        );
    }
    assert_eq!(
        agb.stance_for_test(1),
        Stance::Free,
        "no vote ever fired, so the stance was never claimed"
    );
}

// ============================================================ 6. Integration

/// A silent-proposer view seals `gskip` via the grounded vote quorum with ZERO
/// control-log applications, and every live node converges on the identical
/// committed output.
#[tokio::test]
async fn integration_silent_proposer_view_seals_via_votes_with_zero_control_log_applications() {
    let all = authors();
    let mut nodes: Vec<Node> = all
        .iter()
        .enumerate()
        .map(|(i, (pk, _))| Node::new(*pk, &format!(".db_test_skipvote_integration_{}", i), 8))
        .collect();
    let now = Instant::now();
    let mut outbox: VecDeque<(usize, Inbound)> = VecDeque::new();

    for i in 0..nodes.len() {
        let (_, effects) = nodes[i].lm.publish_own(BTreeMap::new()).await;
        drain_local(&mut nodes, i, effects, now, &mut outbox);
    }
    run_to_quiescence(&mut nodes, &mut outbox, now).await;

    let dead_view: crate::primary::View = 2;
    let dead_name = crate::vantage::agb::proposer(&test_committee(), dead_view);
    let dead_idx = nodes.iter().position(|n| n.name == dead_name).unwrap();
    nodes[dead_idx].alive = false;
    let live: Vec<usize> = (0..nodes.len()).filter(|&i| i != dead_idx).collect();
    assert_eq!(live.len(), 3, "n=4, f=1 -- exactly 2f+1=3 correct parties remain");

    boot(&mut nodes, now, &mut outbox).await;
    let theta_echo = nodes[live[0]].agb.theta_echo();
    let theta_ready = nodes[live[0]].agb.theta_ready();
    advance_time(
        &mut nodes,
        &mut outbox,
        now + theta_echo + Duration::from_millis(1),
    )
    .await;
    advance_time(
        &mut nodes,
        &mut outbox,
        now + theta_ready + Duration::from_millis(1),
    )
    .await;
    run_to_quiescence(
        &mut nodes,
        &mut outbox,
        now + theta_ready + Duration::from_millis(1),
    )
    .await;

    for &i in &live {
        assert_eq!(
            nodes[i].agb.sealed_for_test(dead_view),
            Some(Outcome::Skip),
            "node {} must seal gskip via the grounded vote quorum",
            i
        );
        assert!(
            !nodes[i].control.is_anchor_resolved(dead_view),
            "node {} must show ZERO control-log applications for the dead view",
            i
        );
        assert!(
            nodes[i]
                .metrics
                .vantage_seals
                .with_label_values(&["vote_skip"])
                .get()
                >= 1,
            "node {} must show a vote_skip route increment",
            i
        );
        assert_eq!(
            nodes[i]
                .metrics
                .vantage_seals
                .with_label_values(&["anchor_skip"])
                .get(),
            0,
            "node {} must show zero anchor_skip route increments -- no anchor ever ran",
            i
        );
        assert!(
            nodes[i].cursor.next_view() > dead_view,
            "node {} cursor must have advanced past the dead view",
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

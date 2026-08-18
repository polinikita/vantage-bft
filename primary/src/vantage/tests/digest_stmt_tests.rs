use super::common::*;
use crate::vantage::agb::{
    DigestStatements, Echo, EchoDigest, Outcome, Ready, ReadyDigest, ReadyGrade, ViewProposal,
    FETCH_WIDTH_START,
};
use crate::vantage::{Effect, Repairer};
use crypto::{Digest, PublicKey};
use std::time::{Duration, Instant};

fn sample_proposal(view: u64) -> ViewProposal {
    let (a0, _) = authors()[0];
    ViewProposal {
        view,
        c: vec![(a0, 1, Digest([1u8; 32]))],
        t: Vec::new(),
        m: None,
    }
}

fn dummy_repairer(name: PublicKey, path: &str) -> Repairer {
    let (lm, _store) = new_lane_manager(name, path);
    new_repairer(name, &lm)
}

fn ready_msg(proposal: ViewProposal, grade: ReadyGrade, sender: PublicKey) -> Ready {
    Ready {
        proposal,
        grade,
        sender,
        wish: 0,
    }
}

fn echo_msg(proposal: ViewProposal, grade: u8, sender: PublicKey) -> Echo {
    Echo {
        proposal,
        grade,
        sender,
        wish: 0,
        origin: None,
        avail: None,
    }
}

fn sealed_effects(effects: &[Effect]) -> Vec<Outcome> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Sealed(_, o) => Some(o.clone()),
            _ => None,
        })
        .collect()
}

fn completed_count(effects: &[Effect]) -> usize {
    effects
        .iter()
        .filter(|e| matches!(e, Effect::Completed(..)))
        .count()
}

#[tokio::test]
async fn digest_ready_with_body_held_matches_by_value_transitions() {
    let all = authors();
    let proposal = sample_proposal(1);
    let sid = test_sid();
    let digest = proposal.digest(&sid);

    let (name_a, _) = all[3];
    let mut agb_a = new_agb_engine(name_a);
    let mut rep_a = dummy_repairer(name_a, ".db_test_digest_equiv_a");
    let (mut lm_a, _store_a) = new_lane_manager(name_a, ".db_test_digest_equiv_a_lm");
    let proposer = agb_a.proposer(1);
    agb_a.on_propose(
        proposer,
        proposal.clone(),
        Instant::now(),
        &mut lm_a,
        &mut rep_a,
    );

    let (name_b, _) = all[3];
    let mut agb_b = new_agb_engine(name_b);
    let mut rep_b = dummy_repairer(name_b, ".db_test_digest_equiv_b");
    let (mut lm_b, _store_b) = new_lane_manager(name_b, ".db_test_digest_equiv_b_lm");
    agb_b.on_propose(
        proposer,
        proposal.clone(),
        Instant::now(),
        &mut lm_b,
        &mut rep_b,
    );
    let mut digest_stmts = DigestStatements::new(TEST_DELTA_MS);

    for (sender, _) in all.iter().take(3) {
        let e_a = agb_a.on_ready(
            ready_msg(proposal.clone(), ReadyGrade::One, *sender),
            &mut rep_a,
        );
        let msg = ReadyDigest {
            view: 1,
            digest: digest.clone(),
            grade: ReadyGrade::One,
            sender: *sender,
            wish: 0,
        };
        let e_b = digest_stmts.on_ready_digest(msg, Instant::now(), &mut agb_b, &mut rep_b);
        assert_eq!(
            completed_count(&e_a),
            completed_count(&e_b),
            "completion must fire at the identical statement for both encodings"
        );
        assert_eq!(sealed_effects(&e_a).len(), sealed_effects(&e_b).len());
    }

    assert_eq!(agb_a.completed_for_test(1), agb_b.completed_for_test(1));
    assert_eq!(agb_a.sealed_for_test(1), agb_b.sealed_for_test(1));
    assert!(
        agb_a.sealed_for_test(1).is_some(),
        "3 grade-1 readies must reach the direct-full quorum"
    );

    assert_eq!(digest_stmts.buffered_ready_count_for_test(1), 0);
}

#[tokio::test]
async fn cross_encoding_one_shot_value_then_digest() {
    let all = authors();
    let (sender, _) = all[1];
    let proposal = sample_proposal(1);
    let sid = test_sid();
    let digest = proposal.digest(&sid);

    let (name, _) = all[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_digest_oneshot_fwd");
    let (mut lm, _store) = new_lane_manager(name, ".db_test_digest_oneshot_fwd_lm");
    let proposer = agb.proposer(1);
    agb.on_propose(
        proposer,
        proposal.clone(),
        Instant::now(),
        &mut lm,
        &mut rep,
    );
    let mut digest_stmts = DigestStatements::new(TEST_DELTA_MS);

    agb.on_echo(echo_msg(proposal.clone(), 1, sender), &mut rep);
    assert_eq!(agb.echo_grade1_count_for(1, &proposal.c, &proposal.t), 1);

    let msg = EchoDigest {
        view: 1,
        digest: digest.clone(),
        grade: 1,
        sender,
        wish: 0,
        origin: None,
        avail: None,
    };
    let effects = digest_stmts.on_echo_digest(msg, Instant::now(), &mut agb, &mut rep);
    assert!(
        effects.is_empty(),
        "the same sender's digest-named echo, after its own by-value one, must count nothing new"
    );
    assert_eq!(
        agb.echo_grade1_count_for(1, &proposal.c, &proposal.t),
        1,
        "count must stay at 1, not 2 -- one-shot across encodings"
    );
    assert_eq!(
        digest_stmts.buffered_echo_count_for_test(1),
        0,
        "resolved immediately (body already held) -- never buffered"
    );
}

#[tokio::test]
async fn cross_encoding_one_shot_digest_then_value() {
    let all = authors();
    let (sender, _) = all[1];
    let proposal = sample_proposal(1);
    let sid = test_sid();
    let digest = proposal.digest(&sid);

    let (name, _) = all[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_digest_oneshot_rev");
    let (mut lm, _store) = new_lane_manager(name, ".db_test_digest_oneshot_rev_lm");
    let mut digest_stmts = DigestStatements::new(TEST_DELTA_MS);

    let msg = EchoDigest {
        view: 1,
        digest: digest.clone(),
        grade: 1,
        sender,
        wish: 0,
        origin: None,
        avail: None,
    };
    let effects = digest_stmts.on_echo_digest(msg, Instant::now(), &mut agb, &mut rep);
    assert_eq!(
        agb.echo_grade1_count_for(1, &proposal.c, &proposal.t),
        0,
        "must not count before the body is held"
    );
    assert_eq!(digest_stmts.buffered_echo_count_for_test(1), 1);
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::BodyFetchTo(_, 1, d) if *d == digest)),
        "buffering a first-hand statement for an unheld body must issue a fetch"
    );

    let proposer = agb.proposer(1);
    agb.on_propose(
        proposer,
        proposal.clone(),
        Instant::now(),
        &mut lm,
        &mut rep,
    );
    agb.on_echo(echo_msg(proposal.clone(), 1, sender), &mut rep);
    assert_eq!(
        agb.echo_grade1_count_for(1, &proposal.c, &proposal.t),
        1,
        "the by-value statement counts, exactly once"
    );

    let drained = digest_stmts.on_local_fixed(1, &mut agb, &mut rep);
    assert!(
        drained.is_empty(),
        "draining an already-counted sender must add nothing"
    );
    assert_eq!(
        agb.echo_grade1_count_for(1, &proposal.c, &proposal.t),
        1,
        "still exactly 1 -- the buffered copy must never double-count"
    );
    assert_eq!(
        digest_stmts.buffered_echo_count_for_test(1),
        0,
        "drained either way"
    );
}

#[tokio::test]
async fn buffered_readies_wait_for_body_then_drain_on_serve() {
    let all = authors();
    let proposal = sample_proposal(1);
    let sid = test_sid();
    let digest = proposal.digest(&sid);

    let (name, _) = all[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_digest_buffer_until_body");
    let mut digest_stmts = DigestStatements::new(TEST_DELTA_MS);

    let mut fetch_targets: Vec<PublicKey> = Vec::new();
    for (sender, _) in all.iter().take(3) {
        let msg = ReadyDigest {
            view: 1,
            digest: digest.clone(),
            grade: ReadyGrade::One,
            sender: *sender,
            wish: 0,
        };
        let effects = digest_stmts.on_ready_digest(msg, Instant::now(), &mut agb, &mut rep);
        assert_eq!(
            completed_count(&effects),
            0,
            "buffered-only statements must never complete"
        );
        assert!(
            sealed_effects(&effects).is_empty(),
            "buffered-only statements must never seal"
        );
        for e in &effects {
            if let Effect::BodyFetchTo(peer, v, d) = e {
                assert_eq!(*v, 1);
                assert_eq!(*d, digest);
                fetch_targets.push(*peer);
            }
        }
    }
    assert!(
        !fetch_targets.is_empty(),
        "buffering first-hand statements must issue at least one body fetch"
    );
    assert!(agb.completed_for_test(1).is_none());
    assert!(agb.sealed_for_test(1).is_none());
    assert_eq!(
        digest_stmts.buffered_ready_count_for_test(1),
        3,
        "all 3 must still be buffered, uncounted"
    );
    assert!(
        agb.fixed_proposal(1).is_none(),
        "no direct-receipt state anywhere yet -- on_propose was never called"
    );

    let serve_effects = digest_stmts.on_body_serve(1, proposal.clone(), &mut agb, &mut rep);

    assert_eq!(
        completed_count(&serve_effects),
        1,
        "completion must fire exactly once the body is verified"
    );
    let sealed = sealed_effects(&serve_effects);
    assert_eq!(sealed.len(), 1);
    assert!(matches!(&sealed[0], Outcome::Full(c, t) if *c == proposal.c && *t == proposal.t));
    assert!(agb.completed_for_test(1).is_some());
    assert!(agb.sealed_for_test(1).is_some());
    assert_eq!(
        digest_stmts.buffered_ready_count_for_test(1),
        0,
        "fully drained"
    );

    assert!(
        agb.fixed_proposal(1).is_none(),
        "on_body_serve must never set Fixed::Proposal (no proposal provenance from served bytes)"
    );
}

#[tokio::test]
async fn buffered_ready_mix_is_replaced_by_its_homogeneous_refinement() {
    let all = authors();
    let proposal = sample_proposal(1);
    let digest = proposal.digest(&test_sid());
    let sender = all[0].0;
    let name = all[3].0;
    for (grade, state_path, lane_path) in [
        (
            ReadyGrade::Zero,
            ".db_test_digest_buffer_mix_refinement_zero",
            ".db_test_digest_buffer_mix_refinement_zero_lm",
        ),
        (
            ReadyGrade::One,
            ".db_test_digest_buffer_mix_refinement_one",
            ".db_test_digest_buffer_mix_refinement_one_lm",
        ),
    ] {
        let mut agb = new_agb_engine(name);
        let mut rep = dummy_repairer(name, state_path);
        let (mut lm, _store) = new_lane_manager(name, lane_path);
        let mut digest_stmts = DigestStatements::new(TEST_DELTA_MS);

        for observed_grade in [ReadyGrade::Mix, grade] {
            digest_stmts.on_ready_digest(
                ReadyDigest {
                    view: 1,
                    digest: digest.clone(),
                    grade: observed_grade,
                    sender,
                    wish: 0,
                },
                Instant::now(),
                &mut agb,
                &mut rep,
            );
        }
        assert_eq!(digest_stmts.buffered_ready_count_for_test(1), 1);

        agb.on_propose(
            agb.proposer(1),
            proposal.clone(),
            Instant::now(),
            &mut lm,
            &mut rep,
        );
        digest_stmts.on_local_fixed(1, &mut agb, &mut rep);
        assert_eq!(agb.ready_stage_total(1), 1);

        let mut last = Vec::new();
        for (other, _) in all.iter().skip(1).take(2) {
            last = agb.on_ready(ready_msg(proposal.clone(), grade, *other), &mut rep);
        }
        match grade {
            ReadyGrade::Zero => assert!(matches!(
                sealed_effects(&last).as_slice(),
                [Outcome::Core(c)] if *c == proposal.c
            )),
            ReadyGrade::One => assert!(matches!(
                sealed_effects(&last).as_slice(),
                [Outcome::Full(c, t)] if *c == proposal.c && *t == proposal.t
            )),
            ReadyGrade::Mix => unreachable!(),
        }
    }
}

#[tokio::test]
async fn mismatched_serve_rejected_fetch_retried() {
    let all = authors();
    let true_proposal = sample_proposal(1);
    let sid = test_sid();
    let true_digest = true_proposal.digest(&sid);

    let (name, _) = all[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_digest_mismatch_serve");
    let mut digest_stmts = DigestStatements::new(TEST_DELTA_MS);

    let (sender, _) = all[0];
    let msg = ReadyDigest {
        view: 1,
        digest: true_digest.clone(),
        grade: ReadyGrade::One,
        sender,
        wish: 0,
    };
    digest_stmts.on_ready_digest(msg, Instant::now(), &mut agb, &mut rep);
    assert_eq!(digest_stmts.pending_fetch_count_for_test(), 1);

    let (other_author, _) = all[1];
    let wrong_proposal = ViewProposal {
        view: 1,
        c: vec![(other_author, 1, Digest([9u8; 32]))],
        t: Vec::new(),
        m: None,
    };
    let wrong_digest = wrong_proposal.digest(&sid);
    assert_ne!(wrong_digest, true_digest);

    let rejected = digest_stmts.on_body_serve(1, wrong_proposal, &mut agb, &mut rep);
    assert!(
        rejected.is_empty(),
        "a wrong-digest serve must be rejected outright"
    );
    assert!(
        !digest_stmts.known_body_for_test(1, &wrong_digest),
        "the wrong body must never be memoized"
    );
    assert!(
        !digest_stmts.known_body_for_test(1, &true_digest),
        "the true body still isn't held"
    );
    assert_eq!(
        digest_stmts.buffered_ready_count_for_test(1),
        1,
        "the buffered statement must be untouched"
    );
    assert!(agb.completed_for_test(1).is_none());

    assert_eq!(digest_stmts.pending_fetch_count_for_test(), 1);
    let retry_now =
        Instant::now() + Duration::from_millis(TEST_DELTA_MS) * 8 + Duration::from_millis(1);
    let retried = digest_stmts.retry_fetches(retry_now);
    assert!(
        retried.iter().any(
            |e| matches!(e, Effect::BodyFetchTo(peer, 1, d) if *peer == sender && *d == true_digest)
        ),
        "the retry must re-fan to the still-buffered sender for the still-unheld true digest"
    );

    digest_stmts.on_body_serve(1, true_proposal.clone(), &mut agb, &mut rep);
    assert!(digest_stmts.known_body_for_test(1, &true_digest));
    assert_eq!(digest_stmts.buffered_ready_count_for_test(1), 0);
}

#[tokio::test]
async fn malformed_serve_rejected_even_with_matching_digest() {
    let all = authors();
    let sid = test_sid();
    let (a0, _) = all[0];
    let (a1, _) = all[1];
    let shared = Digest([3u8; 32]);
    let malformed = ViewProposal {
        view: 1,
        c: vec![(a0, 1, shared.clone())],
        t: vec![(a1, 1, shared)],
        m: None,
    };
    let bad_digest = malformed.digest(&sid);

    let (name, _) = all[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_digest_malformed_serve");
    let mut digest_stmts = DigestStatements::new(TEST_DELTA_MS);

    let (sender, _) = all[2];
    let msg = ReadyDigest {
        view: 1,
        digest: bad_digest.clone(),
        grade: ReadyGrade::One,
        sender,
        wish: 0,
    };
    digest_stmts.on_ready_digest(msg, Instant::now(), &mut agb, &mut rep);

    let rejected = digest_stmts.on_body_serve(1, malformed, &mut agb, &mut rep);
    assert!(
        rejected.is_empty(),
        "a malformed body must be rejected even with a matching digest"
    );
    assert!(!digest_stmts.known_body_for_test(1, &bad_digest));
    assert_eq!(digest_stmts.buffered_ready_count_for_test(1), 1);
}

#[tokio::test]
async fn unanswer_fetch_reopens_a_body_serve_that_never_reached_the_transport() {
    let all = authors();
    let (name, _) = all[3];
    let (requester, _) = all[1];
    let (other, _) = all[2];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_digest_unanswer");
    let (mut lm, _store) = new_lane_manager(name, ".db_test_digest_unanswer_lm");
    let mut digest_stmts = DigestStatements::new(TEST_DELTA_MS);

    let proposal = sample_proposal(1);
    let digest = proposal.digest(&test_sid());
    let proposer = agb.proposer(1);
    agb.on_propose(proposer, proposal, Instant::now(), &mut lm, &mut rep);

    assert!(!digest_stmts
        .on_body_fetch(requester, 1, digest.clone(), &agb)
        .is_empty());
    assert!(!digest_stmts
        .on_body_fetch(other, 1, digest.clone(), &agb)
        .is_empty());
    assert!(digest_stmts
        .on_body_fetch(requester, 1, digest.clone(), &agb)
        .is_empty());

    digest_stmts.unanswer_fetch(&requester, 1);
    assert!(
        digest_stmts
            .on_body_fetch(requester, 1, digest.clone(), &agb)
            .iter()
            .any(|e| matches!(e, Effect::BodyServeTo(peer, v, _) if *peer == requester && *v == 1)),
        "a dropped body serve must be servable again after its mark is cleared"
    );
    assert!(
        digest_stmts
            .on_body_fetch(other, 1, digest, &agb)
            .is_empty(),
        "releasing one requester must not reopen another"
    );
}

#[tokio::test]
async fn gc_prunes_buffered_statements_and_fetch_state() {
    let all = authors();
    let proposal = sample_proposal(1);
    let sid = test_sid();
    let digest = proposal.digest(&sid);

    let (name, _) = all[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_digest_gc");
    let (mut lm, _store) = new_lane_manager(name, ".db_test_digest_gc_lm");
    let mut digest_stmts = DigestStatements::new(TEST_DELTA_MS);

    let (sender, _) = all[0];
    let msg = EchoDigest {
        view: 1,
        digest: digest.clone(),
        grade: 1,
        sender,
        wish: 0,
        origin: None,
        avail: None,
    };
    digest_stmts.on_echo_digest(msg, Instant::now(), &mut agb, &mut rep);
    assert_eq!(digest_stmts.buffered_echo_count_for_test(1), 1);
    assert_eq!(digest_stmts.pending_fetch_count_for_test(), 1);

    digest_stmts.on_body_serve(1, proposal.clone(), &mut agb, &mut rep);
    assert!(digest_stmts.known_body_for_test(1, &digest));
    assert_eq!(digest_stmts.pending_fetch_count_for_test(), 0);
    assert_eq!(
        digest_stmts.buffered_echo_count_for_test(1),
        0,
        "drained on accept"
    );

    let proposer = agb.proposer(1);
    agb.on_propose(
        proposer,
        proposal.clone(),
        Instant::now(),
        &mut lm,
        &mut rep,
    );
    let (requester, _) = all[1];
    let served = digest_stmts.on_body_fetch(requester, 1, digest.clone(), &agb);
    assert!(
        !served.is_empty(),
        "must serve its own fixed body on request"
    );
    assert_eq!(digest_stmts.fetch_answered_count_for_test(), 1);

    digest_stmts.gc_below(2);

    assert!(
        !digest_stmts.known_body_for_test(1, &digest),
        "known_bodies must be pruned below the floor"
    );
    assert_eq!(digest_stmts.buffered_echo_count_for_test(1), 0);
    assert_eq!(digest_stmts.pending_fetch_count_for_test(), 0);
    assert_eq!(digest_stmts.fetch_answered_count_for_test(), 0);

    let late = EchoDigest {
        view: 1,
        digest,
        grade: 1,
        sender: all[2].0,
        wish: 0,
        origin: None,
        avail: None,
    };
    let effects = digest_stmts.on_echo_digest(late, Instant::now(), &mut agb, &mut rep);
    assert!(effects.is_empty());
    assert_eq!(digest_stmts.buffered_echo_count_for_test(1), 0);
}

#[tokio::test]
async fn pending_fetch_is_capped_and_evicts_the_highest_views() {
    use crate::vantage::agb::MAX_PENDING_FETCH;
    let all = authors();
    let (name, _) = all[3];
    let (sender, _) = all[0];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_pending_fetch_cap");
    let mut digest_stmts = DigestStatements::new(TEST_DELTA_MS);
    let now = Instant::now();

    let total = MAX_PENDING_FETCH + 50;
    for v in 1..=total {
        let msg = ReadyDigest {
            view: v as u64,
            digest: Digest([(v % 251) as u8; 32]),
            grade: ReadyGrade::One,
            sender,
            wish: 0,
        };
        digest_stmts.on_ready_digest(msg, now, &mut agb, &mut rep);
    }

    let len = digest_stmts.pending_fetch_count_for_test();
    assert!(
        len <= MAX_PENDING_FETCH,
        "pending_fetch grew to {len}, above the {MAX_PENDING_FETCH} ceiling"
    );
    assert!(
        digest_stmts.has_pending_fetch_for_test(1),
        "view 1 was evicted; the lowest pending view must be retained"
    );
    assert!(
        !digest_stmts.has_pending_fetch_for_test(total as u64),
        "the highest view survived; eviction is taking the wrong end"
    );
}

#[tokio::test]
async fn body_fetch_starts_narrow_and_widens_on_retry() {
    let all = authors();
    let sid = test_sid();
    let proposal = ViewProposal {
        view: 1,
        c: vec![(all[0].0, 1, Digest([7u8; 32]))],
        t: Vec::new(),
        m: None,
    };
    let digest = proposal.digest(&sid);

    let (me, _) = all[3];
    let mut agb = new_agb_engine(me);
    let mut rep = dummy_repairer(me, ".db_test_fetch_width");
    let mut ds = DigestStatements::new(TEST_DELTA_MS);

    let mut t0 = Instant::now();
    let count = |effects: &[Effect]| {
        effects
            .iter()
            .filter(|e| matches!(e, Effect::BodyFetchTo(_, 1, d) if *d == digest))
            .count()
    };

    let mut widths = Vec::new();
    for (n, (sender, _)) in all.iter().enumerate() {
        let effects = ds.on_ready_digest(
            ReadyDigest {
                view: 1,
                digest: digest.clone(),
                grade: ReadyGrade::One,
                sender: *sender,
                wish: 0,
            },
            t0,
            &mut agb,
            &mut rep,
        );
        if n == 0 {
            widths.push(count(&effects));
        } else {
            assert_eq!(
                count(&effects),
                0,
                "the per-pair latch must suppress re-asks"
            );
        }
    }
    let full = ds.fetch_targets_len_for_test(1, &digest);
    for _ in 0..2 {
        t0 += Duration::from_millis(TEST_DELTA_MS) * 8 + Duration::from_millis(1);
        widths.push(count(&ds.retry_fetches(t0)));
    }

    assert!(
        widths[0] <= FETCH_WIDTH_START,
        "the first ask must be bounded by the start width: {widths:?}"
    );
    assert!(
        widths[0] < full,
        "the first ask must be narrower than the full buffered set: {widths:?} (full={full})"
    );
    assert!(
        widths[1] > widths[0],
        "an unanswered ask must widen: {widths:?}"
    );
    assert!(
        widths.iter().all(|w| *w <= full),
        "never exceed the buffered set: {widths:?} (full={full})"
    );
}

#[tokio::test]
async fn body_fetch_is_abandoned_after_the_cap_and_recreated_by_a_new_statement() {
    let all = authors();
    let sid = test_sid();
    let proposal = ViewProposal {
        view: 1,
        c: vec![(all[0].0, 1, Digest([5u8; 32]))],
        t: Vec::new(),
        m: None,
    };
    let digest = proposal.digest(&sid);

    let (me, _) = all[3];
    let mut agb = new_agb_engine(me);
    let mut rep = dummy_repairer(me, ".db_test_fetch_abandon");
    let mut ds = DigestStatements::new(TEST_DELTA_MS);

    let (sender, _) = all[0];
    let mut t = Instant::now();
    ds.on_ready_digest(
        ReadyDigest {
            view: 1,
            digest: digest.clone(),
            grade: ReadyGrade::One,
            sender,
            wish: 0,
        },
        t,
        &mut agb,
        &mut rep,
    );
    assert_eq!(ds.pending_fetch_count_for_test(), 1);

    for _ in 0..8 {
        t += Duration::from_millis(TEST_DELTA_MS) * 8 + Duration::from_millis(1);
        ds.retry_fetches(t);
    }
    assert_eq!(
        ds.pending_fetch_count_for_test(),
        0,
        "an unanswerable pair must be abandoned, not retried forever"
    );

    let (sender2, _) = all[1];
    ds.on_ready_digest(
        ReadyDigest {
            view: 1,
            digest: digest.clone(),
            grade: ReadyGrade::One,
            sender: sender2,
            wish: 0,
        },
        t,
        &mut agb,
        &mut rep,
    );
    assert_eq!(
        ds.pending_fetch_count_for_test(),
        1,
        "a fresh statement must revive the fetch"
    );
}

#[tokio::test]
async fn late_body_serve_after_fetch_abandonment_drains_buffered_statements() {
    let all = authors();
    let proposal = sample_proposal(1);
    let sid = test_sid();
    let digest = proposal.digest(&sid);

    let (name, _) = all[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_late_body_after_abandon");
    let mut ds = DigestStatements::new(TEST_DELTA_MS);

    let mut t = Instant::now();
    for (sender, _) in all.iter().take(3) {
        ds.on_ready_digest(
            ReadyDigest {
                view: 1,
                digest: digest.clone(),
                grade: ReadyGrade::One,
                sender: *sender,
                wish: 0,
            },
            t,
            &mut agb,
            &mut rep,
        );
    }
    assert_eq!(ds.buffered_ready_count_for_test(1), 3);
    assert_eq!(ds.pending_fetch_count_for_test(), 1);

    for _ in 0..8 {
        t += Duration::from_millis(TEST_DELTA_MS) * 8 + Duration::from_millis(1);
        ds.retry_fetches(t);
    }
    assert_eq!(
        ds.pending_fetch_count_for_test(),
        0,
        "the active retry slot should be gone before the delayed serve is processed"
    );

    let effects = ds.on_body_serve(1, proposal.clone(), &mut agb, &mut rep);
    assert_eq!(
        completed_count(&effects),
        1,
        "the late exact body must still drain the buffered digest statements"
    );
    let sealed = sealed_effects(&effects);
    assert_eq!(sealed.len(), 1);
    assert!(matches!(&sealed[0], Outcome::Full(c, t) if *c == proposal.c && *t == proposal.t));
    assert_eq!(ds.buffered_ready_count_for_test(1), 0);
    assert!(ds.known_body_for_test(1, &digest));
    assert!(
        agb.fixed_proposal(1).is_none(),
        "served bytes still must not create proposal provenance"
    );
}

#[tokio::test]
async fn duplicate_digest_statement_does_not_create_phantom_fetch() {
    let all = authors();
    let proposal = sample_proposal(1);
    let sid = test_sid();
    let digest = proposal.digest(&sid);
    let wrong_proposal = ViewProposal {
        view: 1,
        c: vec![(all[1].0, 1, Digest([9u8; 32]))],
        t: Vec::new(),
        m: None,
    };
    let wrong_digest = wrong_proposal.digest(&sid);
    assert_ne!(wrong_digest, digest);

    let (name, _) = all[3];
    let mut agb = new_agb_engine(name);
    let mut rep = dummy_repairer(name, ".db_test_duplicate_digest_phantom_fetch");
    let mut ds = DigestStatements::new(TEST_DELTA_MS);
    let (sender, _) = all[0];
    let t = Instant::now();

    ds.on_ready_digest(
        ReadyDigest {
            view: 1,
            digest: digest.clone(),
            grade: ReadyGrade::One,
            sender,
            wish: 0,
        },
        t,
        &mut agb,
        &mut rep,
    );
    assert_eq!(ds.pending_fetch_count_for_test(), 1);
    assert_eq!(ds.fetch_targets_len_for_test(1, &digest), 1);

    ds.on_ready_digest(
        ReadyDigest {
            view: 1,
            digest: wrong_digest.clone(),
            grade: ReadyGrade::One,
            sender,
            wish: 0,
        },
        t + Duration::from_millis(TEST_DELTA_MS) * 8 + Duration::from_millis(1),
        &mut agb,
        &mut rep,
    );
    assert_eq!(
        ds.pending_fetch_count_for_test(),
        1,
        "the ignored duplicate must not create a pending fetch for its ignored digest"
    );
    assert_eq!(ds.fetch_targets_len_for_test(1, &wrong_digest), 0);
}

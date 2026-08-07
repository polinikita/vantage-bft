// signature-free.tex §8.3 "Digest-named AGB statements" (`Parameters::
// digest_statements`) -- unit tests for the translation layer (`vantage::agb::
// DigestStatements`), driven directly against `AgbEngine`/`Repairer`/
// `DigestStatements` (no network, no `Node` harness -- mirrors `completion_tests.rs`'s
// own direct-engine style; `on_ready`/`on_echo` need no `LaneManager` positive-gate
// ceremony, only `on_propose` does, and only for its `lm` parameter's type, never its
// content here). The flag-on end-to-end integration test (identical committed output,
// zero fetches in the favorable path) lives in `integration_tests.rs`, alongside its
// flag-off/ack-watermarks siblings.

use super::common::*;
use crate::vantage::agb::{
    DigestStatements, Echo, EchoDigest, Outcome, Ready, ReadyDigest, ReadyGrade, ViewProposal,
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

/// Mirrors `completion_tests.rs::dummy_repairer` exactly (same doc rationale: opening
/// a `LaneManager`/`Store` needs a live runtime, and every call site needs its own
/// path).
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

// ============================================================ Requirement 2: encoding
// equivalence.

/// A digest-named READY received once its body is already held+verified must produce
/// the IDENTICAL `AgbEngine` state transition, statement by statement, as the by-value
/// READY it encodes -- driven through `DigestStatements::on_ready_digest`, never a
/// parallel counting path.
#[tokio::test]
async fn digest_ready_with_body_held_matches_by_value_transitions() {
    let all = authors();
    let proposal = sample_proposal(1);
    let sid = test_sid();
    let digest = proposal.digest(&sid);

    // Engine A: every statement travels by value (the existing, unchanged path).
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

    // Engine B: identically seeded (the same by-value propose, so the body is
    // already `Fixed` -- held -- by the time any digest statement arrives), but
    // every READY arrives digest-named. `DigestStatements` must resolve+feed it
    // immediately, with no buffering at all.
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

    // The body was already held at every arrival -- nothing was ever buffered.
    assert_eq!(digest_stmts.buffered_ready_count_for_test(1), 0);
}

// ================================================== Requirement 3: cross-encoding
// one-shot.

/// Forward direction: a by-value ECHO followed by a digest-named ECHO from the SAME
/// sender for the SAME view counts once -- the digest arm resolves+feeds straight
/// into the SAME `AgbEngine::count_echo_statement` dedup a second by-value statement
/// would hit, never a parallel counted set.
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

/// Reverse direction: a digest-named ECHO arrives first (buffered -- no body held
/// yet), then the by-value ECHO from the SAME sender arrives directly. Still counted
/// once: the by-value arm's own dedup fires regardless of the buffered digest copy,
/// and draining the now-fixed body finds nothing left to feed for that sender.
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

    // The digest-named echo arrives BEFORE the body is held -- it must buffer, not
    // count, and must issue a fetch.
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

    // The by-value echo from the SAME sender arrives directly, over its own,
    // completely separate path (exactly as if it had come over the wire
    // untranslated) -- first the body itself is fixed (as `on_propose` requires).
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

    // Draining the now-fixed body finds nothing left to feed for this sender --
    // `AgbEngine`'s own dedup already holds its slot, so the drain is a no-op.
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

// ============================================================== Requirement 4:
// buffer-until-body.

/// 2f+1 digest-named READY statements buffered with NO body held anywhere must
/// produce no completion/seal; only once the body arrives (here: via a served
/// response) does draining fire completion -- AND the serve itself must set no
/// direct-receipt state (`AgbEngine::fixed_proposal`, the ONLY provenance marker a
/// real proposal receipt sets, must stay `None`).
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

    // The body arrives via a served response -- never via on_propose.
    let serve_effects = digest_stmts.on_body_serve(1, proposal.clone(), &mut agb, &mut rep);

    // Completion (any grade, >= quorum) and the direct-full outcome (homogeneous
    // grade-1, >= quorum) both fire now that the body is verified and every
    // buffered statement drains through the by-value path.
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

    // CRITICAL: serving must never mark the proposal as directly received from the
    // proposer -- `AgbEngine::fixed_proposal` (the ONLY direct-receipt/rho_i marker
    // this engine keeps) must still be `None`.
    assert!(
        agb.fixed_proposal(1).is_none(),
        "on_body_serve must never set Fixed::Proposal (no proposal provenance from served bytes)"
    );
}

// ========================================================= Requirement 5: mismatched
// serve rejected.

/// A served body naming a DIFFERENT digest than any outstanding fetch is rejected
/// outright -- no state change -- and the original fetch stays outstanding, so the
/// next retry re-asks (possibly a different holder). Mirrors `control_tests.rs`'s
/// `p6_2_wrong_digest_serve_rejected_correct_digest_accepted`/
/// `p6_2_poisoning_attempt_rejected_true_anchor_still_applies`.
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

    // A DIFFERENT well-formed proposal for the same view (a different C entry,
    // hence a different digest) must never be accepted as an answer to our
    // outstanding (view=1, true_digest) fetch.
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

    // The original fetch is still outstanding (untouched by the rejection) -- the
    // next periodic retry re-asks the still-buffered sender.
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

    // The TRUE serve is accepted afterward, and drains cleanly.
    digest_stmts.on_body_serve(1, true_proposal.clone(), &mut agb, &mut rep);
    assert!(digest_stmts.known_body_for_test(1, &true_digest));
    assert_eq!(digest_stmts.buffered_ready_count_for_test(1), 0);
}

/// Malformed half of requirement 5: a served body that hash-matches the outstanding
/// fetch EXACTLY (so the digest-match gate alone would accept it) but fails
/// `Formed_v` is still rejected -- `agb::formed` is an independent, additional gate,
/// not merely a byproduct of the digest check.
#[tokio::test]
async fn malformed_serve_rejected_even_with_matching_digest() {
    let all = authors();
    let sid = test_sid();
    let (a0, _) = all[0];
    let (a1, _) = all[1];
    // Duplicate hash across C and T -- malformed per `formed`'s distinct-hashes rule.
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

    // The EXACT same (malformed) bytes are served back -- the digest matches
    // perfectly, so only the `formed` check can reject this.
    let rejected = digest_stmts.on_body_serve(1, malformed, &mut agb, &mut rep);
    assert!(
        rejected.is_empty(),
        "a malformed body must be rejected even with a matching digest"
    );
    assert!(!digest_stmts.known_body_for_test(1, &bad_digest));
    assert_eq!(digest_stmts.buffered_ready_count_for_test(1), 1);
}

// ======================================================================
// Requirement 6: GC.

/// Buffered statements and fetch bookkeeping are pruned by `gc_below`, mirroring
/// `AgbEngine`/`control::ControlLog`'s own view-window GC (`split_off`, no `retain`).
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

    // buffered_echo + pending_fetch: a first-hand digest echo with no body held yet.
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

    // known_bodies + draining pending_fetch: accept a matching serve.
    digest_stmts.on_body_serve(1, proposal.clone(), &mut agb, &mut rep);
    assert!(digest_stmts.known_body_for_test(1, &digest));
    assert_eq!(digest_stmts.pending_fetch_count_for_test(), 0);
    assert_eq!(
        digest_stmts.buffered_echo_count_for_test(1),
        0,
        "drained on accept"
    );

    // fetch_answered: this party also holds view 1 by value now -- seed it
    // separately so the SERVING side (answering a peer's own fetch) is exercised
    // too, not just the fetching side above.
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

    // A late statement for the now-pruned view is a no-op (mirrors `AgbEngine::
    // gc_below`'s own "old view messages become no-ops" contract).
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

/// n=100 straggler fix (2026-08-08): `pending_fetch` is capped, and eviction takes the
/// HIGHEST views. That direction is load-bearing -- resolution is strictly sequential,
/// so the LOWEST pending view is the one actually blocking progress while far-ahead
/// views are useless until it clears. Evicting the lowest would throw away exactly the
/// fetch the node needs to make progress.
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

    // Fill well past the ceiling, ascending. Each distinct view/digest pair creates one
    // pending fetch (the statement is buffered, so the pair is re-creatable at will).
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
    // The LOW views must have survived -- they are the ones blocking resolution.
    assert!(
        digest_stmts.has_pending_fetch_for_test(1),
        "view 1 was evicted; the lowest pending view must be retained"
    );
    // And the far-ahead ones must be the casualties.
    assert!(
        !digest_stmts.has_pending_fetch_for_test(total as u64),
        "the highest view survived; eviction is taking the wrong end"
    );
}

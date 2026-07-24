// PHASE6-SPEC.md §5/§6 -- unit tests per layer: completion reports, validated Bracha
// (ECHO validity gate, READY quorum/relay, delivery), the Simple-IT round machine
// (safe-parent/mark-safe/vote/timeout/disable/commit/advance), reliable notification
// (vote/accept/cascade/confirm), fetch, and log assembly + anchor derivation. Driven
// directly against `ControlLog` -- n=4, f=1 (f+1=2, 2f+1=3, n-f=3).

use super::common::*;
use crate::vantage::agb::{Outcome, ResolutionEntry};
use crate::vantage::control::{ControlLog, ControlProposal};
use crate::vantage::Effect;
use crate::vantage::ViewProposal;
use crypto::PublicKey;

fn new_control(name: PublicKey) -> ControlLog {
    ControlLog::new(name, test_committee(), test_sid(), TEST_DELTA_MS)
}

/// A `B_w`-shaped proposal for view `w` whose resolution entry targets `u` (Skip, the
/// simplest shape -- no manifest content needed for `Formed_v`/verification).
fn skip_proposal(w: u64, u: u64) -> ViewProposal {
    ViewProposal {
        view: w,
        c: Vec::new(),
        t: Vec::new(),
        m: Some(ResolutionEntry::Skip(u)),
    }
}

fn echo_effect(effects: &[Effect]) -> bool {
    effects
        .iter()
        .any(|e| matches!(e, Effect::BroadcastControlEcho(_)))
}

fn ready_effect(effects: &[Effect]) -> bool {
    effects
        .iter()
        .any(|e| matches!(e, Effect::BroadcastControlReady(_)))
}

// ---------------------------------------------------------------- reports + retention

#[tokio::test]
async fn reports_census_dedup_by_sender_and_matching_count_by_digest() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    let all = authors();
    let d1 = crypto::Digest([1u8; 32]);
    let d2 = crypto::Digest([2u8; 32]);

    control.on_comp_report(4, d1.clone(), all[0].0);
    control.on_comp_report(4, d1.clone(), all[1].0);
    // A different sender reporting a DIFFERENT digest for the same view is tracked
    // separately.
    control.on_comp_report(4, d2.clone(), all[2].0);
    // A duplicate report from an already-counted sender never re-counts.
    control.on_comp_report(4, d2, all[0].0);

    assert_eq!(control.report_count_for(4, &d1), 2);
}

#[tokio::test]
async fn completion_reportable_retains_block_once_and_broadcasts_report_once() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    let proposal = skip_proposal(4, 1);
    let digest = proposal.digest(&test_sid());

    let effects = control.on_completion_reportable(4, proposal.clone());
    assert!(effects
        .iter()
        .any(|e| matches!(e, Effect::BroadcastCompReport(v, d) if *v == 4 && *d == digest)));
    assert!(control.holds_block_for_test(4));
    assert_eq!(
        control.report_count_for(4, &digest),
        1,
        "our own report counts first-hand, immediately"
    );

    // A second completion for the SAME view (shouldn't happen in practice -- `complete`
    // is once-ever at the AGB layer -- but defensively idempotent here too) is a no-op.
    let effects2 = control.on_completion_reportable(4, proposal);
    assert!(effects2.is_empty());
}

// ---------------------------------------------------------------- validated Bracha

#[tokio::test]
async fn bracha_bottom_value_echoes_immediately_with_no_reports_needed() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    let leader = control.control_leader(1);
    let proposal = ControlProposal {
        round: 1,
        parent: 0,
        value: None,
    };
    let effects = control.on_control_init(leader, proposal, None);
    assert!(echo_effect(&effects), "⊥ passes validation immediately");
}

#[tokio::test]
async fn bracha_echo_blocked_below_f_plus_1_reports_then_fires_once_met() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    let all = authors();
    let b_w = skip_proposal(4, 1);
    let digest = b_w.digest(&test_sid());
    let leader = control.control_leader(1);
    let proposal = ControlProposal {
        round: 1,
        parent: 0,
        value: Some((4, digest.clone())),
    };

    let effects = control.on_control_init(leader, proposal, Some(b_w));
    assert!(
        !echo_effect(&effects),
        "0 matching reports -- ReadyOK-style validity gate blocks ECHO"
    );

    control.on_comp_report(4, digest.clone(), all[0].0);
    // Still only 1 report (< f+1=2).
    assert!(control.report_count_for(4, &digest) < 2);

    let effects2 = control.on_comp_report(4, digest.clone(), all[1].0);
    assert!(
        echo_effect(&effects2),
        "f+1=2 matching reports now met -- retry_pending_echoes fires the ECHO"
    );
}

#[tokio::test]
async fn bracha_init_from_non_leader_is_ignored() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    let all = authors();
    let leader = control.control_leader(1);
    let impostor = all.iter().find(|(pk, _)| *pk != leader).unwrap().0;
    let proposal = ControlProposal {
        round: 1,
        parent: 0,
        value: None,
    };
    let effects = control.on_control_init(impostor, proposal, None);
    assert!(
        effects.is_empty(),
        "only the round's declared leader can ever fix a proposal"
    );
}

#[tokio::test]
async fn bracha_ready_on_two_f_plus_1_echoes() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    let all = authors();
    let proposal = ControlProposal {
        round: 1,
        parent: 0,
        value: None,
    };

    control.on_control_echo(all[0].0, proposal.clone());
    let e2 = control.on_control_echo(all[1].0, proposal.clone());
    assert!(!ready_effect(&e2), "only 2 counted echoes (< 2f+1=3)");
    let e3 = control.on_control_echo(all[2].0, proposal.clone());
    assert!(ready_effect(&e3), "2f+1=3 matching echoes -- READY fires");
}

#[tokio::test]
async fn bracha_ready_relay_on_f_plus_1_readies_without_two_f_plus_1_echoes() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    let all = authors();
    let proposal = ControlProposal {
        round: 1,
        parent: 0,
        value: None,
    };

    // Only 1 echo counted (far below 2f+1), but f+1=2 READYs relay the value anyway
    // (Bracha's classic relay rule).
    control.on_control_echo(all[0].0, proposal.clone());
    control.on_control_ready(all[1].0, proposal.clone());
    let effects = control.on_control_ready(all[2].0, proposal.clone());
    assert!(
        ready_effect(&effects),
        "f+1=2 matching READYs relay our own READY even without echo quorum"
    );
}

#[tokio::test]
async fn bracha_delivers_on_two_f_plus_1_readies() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    let all = authors();
    let proposal = ControlProposal {
        round: 1,
        parent: 0,
        value: None,
    };

    let e1 = control.on_control_ready(all[0].0, proposal.clone());
    assert!(
        !ready_effect(&e1),
        "only 1 external READY counted -- below f+1=2, no relay yet"
    );
    // The 2nd external READY reaches the f+1=2 relay threshold, which makes us send
    // our OWN READY -- and since that's the 3rd (2 external + our own), it ALSO
    // immediately reaches the 2f+1=3 delivery threshold in the same call.
    let e2 = control.on_control_ready(all[1].0, proposal);
    assert!(ready_effect(&e2), "f+1=2 relay -- our own READY fires");
    assert!(
        control.is_safe_for_test(1),
        "our own relayed READY completed 2f+1 -- delivered, Mark-safe ran (parent=0 always safe)"
    );
}

// ---------------------------------------------------------------- round machine

#[tokio::test]
async fn round_machine_vote_commit_deliver_advance_on_bottom_round() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    control.genesis(); // enters round 1 (curr_round=1) -- required for Vote's
                       // `r == curr_round` guard to ever pass below.
    let all = authors();
    let proposal = ControlProposal {
        round: 1,
        parent: 0,
        value: None,
    };

    // Deliver round 1 (2f+1 READY, reached via 2 external READYs + our own relay --
    // see `bracha_delivers_on_two_f_plus_1_readies`) -- this also runs Mark-safe and,
    // since we haven't voted/timed-out yet, tries Vote: `safe[1] && !timed_out &&
    // !voted` holds immediately for `curr_round=1`.
    control.on_control_ready(all[0].0, proposal.clone());
    control.on_control_ready(all[1].0, proposal);
    assert!(control.is_safe_for_test(1));

    // n-f=3 matching commits -> committed[1] -- our own vote (from the Vote rule
    // above) plus 2 more.
    control.on_control_commit(all[0].0, 1);
    assert!(
        !control.is_committed_for_test(1),
        "only 2 commits counted so far (self + 1)"
    );
    control.on_control_commit(all[1].0, 1);
    assert!(control.is_committed_for_test(1));

    // Deliver: round 1's (empty, ⊥) chain contributes nothing to the log, but Advance
    // round must have fired (`voted=true` from the Vote rule).
    assert_eq!(
        control.curr_round_for_test(),
        2,
        "advance round: safe[1] && voted -- enter round 2"
    );
    assert!(
        control.delivered_log_for_test().is_empty(),
        "round 1's value was ⊥ -- nothing appended to L"
    );
}

#[tokio::test]
async fn round_machine_disable_via_confirmed_timeout_unblocks_safe_parent_scan() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    control.genesis(); // enters round 1 (curr_round=1) -- required for the round
                       // timer's `r == curr_round` guard to ever pass below.
    let all = authors();

    // Round 1 never delivers (no INIT/ECHO/READY at all) -- disable it via the
    // reliable-notification path instead: our own timeout fires (curr_round=1,
    // never voted), then n-f=3 matching votes -> our own accept, then 2f+1=3
    // matching accepts -> confirm -> disabled[1].
    let effects = control.on_control_round_timer(1);
    assert!(effects
        .iter()
        .any(|e| matches!(e, Effect::BroadcastControlTimeoutVote(1))));

    control.on_control_timeout_vote(all[0].0, 1);
    let e = control.on_control_timeout_vote(all[1].0, 1);
    assert!(
        e.iter()
            .any(|eff| matches!(eff, Effect::BroadcastControlTimeoutAccept(1))),
        "n-f=3 votes (self+2) -- Accept"
    );

    control.on_control_timeout_accept(all[0].0, 1);
    let e2 = control.on_control_timeout_accept(all[1].0, 1);
    // 2f+1=3 accepts (self + 2) -- Confirm -> Disable.
    let _ = e2;
    assert!(control.is_disabled_for_test(1));
    // Disable also tries Advance round: `disabled[curr_round]` holds for curr_round=1.
    assert_eq!(control.curr_round_for_test(), 2);
}

#[tokio::test]
async fn reliable_notification_cascade_fires_accept_at_f_plus_1_without_n_minus_f_votes() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    let all = authors();
    control.on_control_round_timer(1); // our own vote_sent (counts as 1 vote)

    // Only 2 total votes so far (self + 1 -- below n-f=3), but the Cascade rule fires
    // our own accept once f+1=2 accepts are observed from others.
    let e1 = control.on_control_timeout_accept(all[0].0, 1);
    assert!(e1.is_empty(), "only 1 accept observed -- below f+1=2");
    let e2 = control.on_control_timeout_accept(all[1].0, 1);
    assert!(
        e2.iter()
            .any(|eff| matches!(eff, Effect::BroadcastControlTimeoutAccept(1))),
        "f+1=2 accepts observed -- Cascade fires our own accept even without n-f votes"
    );
}

// ---------------------------------------------------------------- fetch + log/anchor

#[tokio::test]
async fn fetch_serves_once_per_requester_and_pair() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    let all = authors();
    let proposal = skip_proposal(4, 1);
    control.on_completion_reportable(4, proposal.clone());
    let digest = proposal.digest(&test_sid());

    let e1 = control.on_control_fetch(all[0].0, 4, digest.clone());
    assert!(e1
        .iter()
        .any(|e| matches!(e, Effect::ControlServeTo(peer, v, _) if *peer == all[0].0 && *v == 4)));
    let e2 = control.on_control_fetch(all[0].0, 4, digest);
    assert!(
        e2.is_empty(),
        "a holder answers at most once per requester-(w,h) pair"
    );
}

#[tokio::test]
async fn end_to_end_completion_report_to_anchor_via_bottom_bracha() {
    // The non-Byzantine baseline this phase's marquee Byzantine test builds on: a
    // genuine M != None completion (fabricated here directly, bypassing the AGB
    // layer's own quorum machinery -- that's `resolution_gate_tests.rs`'s job) is
    // reported, becomes submittable, gets proposed by round 1's control leader,
    // delivered, committed, and its resolution entry emitted as `Effect::ApplyAnchor`.
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    let all = authors();
    let proposal = skip_proposal(4, 1);
    let digest = proposal.digest(&test_sid());

    // Every party (including self) reports BEFORE round 1 is even entered -- 2f+1=3
    // matching reports (the submittable threshold) plus we hold B_w (from our own
    // completion) -- so that whichever path drives round 1's proposal (below), the
    // real (view, digest) pair is already submittable.
    control.on_completion_reportable(4, proposal.clone());
    control.on_comp_report(4, digest.clone(), all[0].0);
    control.on_comp_report(4, digest.clone(), all[1].0);

    let leader = control.control_leader(1);
    let mut effects = control.genesis(); // enters round 1; self-proposes (and
                                         // self-delivers) if `name` is its leader --
                                         // submittable state is already present, so
                                         // the real pair is picked, not ⊥.
    if leader != name {
        // Not our turn to lead -- construct the leader's INIT exactly as it would
        // (the round-machine mechanics above already cover the leader-turn path
        // itself; this isolates the report -> submittable -> anchor pipeline).
        let cp = ControlProposal {
            round: 1,
            parent: 0,
            value: Some((4, digest.clone())),
        };
        effects.extend(control.on_control_init(leader, cp, Some(proposal)));
    }

    // Deliver via 2f+1 matching READYs (2 external + our own relay, as established
    // above) from the two OTHER parties (neither is `leader` nor `name`, so this
    // works regardless of which case above ran).
    let others: Vec<PublicKey> = all
        .iter()
        .map(|(pk, _)| *pk)
        .filter(|pk| *pk != name && *pk != leader)
        .collect();
    let cp = ControlProposal {
        round: 1,
        parent: 0,
        value: Some((4, digest)),
    };
    effects.extend(control.on_control_ready(others[0], cp.clone()));
    effects.extend(control.on_control_ready(others[1], cp.clone()));
    assert!(
        control.is_safe_for_test(1),
        "round 1 must have delivered + marked safe by now"
    );

    // n-f=3 commits (self, from Vote, + 2 more) -> committed -> Deliver -> pump_log.
    control.on_control_commit(others[0], 1);
    let commit_effects = control.on_control_commit(others[1], 1);
    effects.extend(commit_effects);
    let anchor = effects
        .iter()
        .find_map(|e| match e {
            Effect::ApplyAnchor(u, outcome, refs) => Some((*u, outcome.clone(), refs.clone())),
            _ => None,
        })
        .expect("the delivered (view=4,digest) pair's Skip(1) entry must produce ApplyAnchor(1, Skip, [])");
    assert_eq!(anchor.0, 1);
    assert!(matches!(anchor.1, crate::vantage::Outcome::Skip));
    assert!(anchor.2.is_empty());
    assert!(control.is_anchor_resolved(1));
}

// ---------------------------------------------------------------- Fable audit pass 1

/// P6-1 regression: a party whose `blocks[w]` is already populated via `try_echo`'s
/// INIT-attachment path (NOT via its own completion) must STILL broadcast its own
/// `CompReport` once its genuine R4 completion of `w` actually runs -- the once-guard
/// is `reported`, not `blocks.contains_key`.
#[tokio::test]
async fn p6_1_fetch_then_complete_still_reports() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    let all = authors();
    let proposal = skip_proposal(4, 1);
    let digest = proposal.digest(&test_sid());

    // `blocks[4]` becomes held via try_echo's INIT-attachment (>= f+1=2 matching
    // reports, from two OTHER parties -- not `name`), never via `name`'s own
    // completion.
    control.on_comp_report(4, digest.clone(), all[0].0);
    control.on_comp_report(4, digest.clone(), all[1].0);
    let leader = control.control_leader(1);
    let cp = ControlProposal {
        round: 1,
        parent: 0,
        value: Some((4, digest.clone())),
    };
    control.on_control_init(leader, cp, Some(proposal.clone()));
    assert!(control.holds_block_for_test(4), "blocks[4] must already be held via try_echo's INIT-attachment, before any genuine completion");

    // `name`'s OWN genuine R4 completion of view 4 must STILL broadcast a CompReport.
    let effects = control.on_completion_reportable(4, proposal.clone());
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::BroadcastCompReport(v, d) if *v == 4 && *d == digest)),
        "a genuine completion must report even when B_w was already held from elsewhere"
    );
    assert_eq!(
        control.report_count_for(4, &digest),
        3,
        "name's own first-hand report must count too (self + the 2 seeded above)"
    );

    // Idempotent: a second completion call for the same view never re-reports.
    let effects2 = control.on_completion_reportable(4, proposal);
    assert!(
        effects2.is_empty(),
        "a repeated completion for an already-reported view must be a no-op"
    );
}

/// P6-2(i) regression: an unsolicited serve (no matching `pending_fetch` entry at all)
/// must change no state -- same normative class as Phase 3's P1-2.
#[tokio::test]
async fn p6_2_unsolicited_serve_changes_no_state() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    let proposal = skip_proposal(4, 1);

    let effects = control.on_control_serve(4, proposal);
    assert!(effects.is_empty());
    assert!(
        !control.holds_block_for_test(4),
        "an unsolicited serve must never populate blocks[w]"
    );
}

/// P6-2(ii) regression: with a genuine outstanding `pending_fetch` entry for `(w,
/// h_true)`, a served proposal naming a DIFFERENT digest is rejected (no state change);
/// the correct-digest one is accepted.
#[tokio::test]
async fn p6_2_wrong_digest_serve_rejected_correct_digest_accepted() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    let all = authors();
    let true_proposal = skip_proposal(4, 1);
    let h_true = true_proposal.digest(&test_sid());

    // Round 1 delivers (4, h_true) via 2f+1 READY directly -- `name` itself never
    // validates/holds B_w -- `recheck_bracha_deliver`'s own "!blocks.contains_key(w)"
    // branch fires `ensure_fetch`, seeding a genuine `pending_fetch` entry for
    // exactly `(4, h_true)`.
    let others: Vec<PublicKey> = all
        .iter()
        .map(|(pk, _)| *pk)
        .filter(|pk| *pk != name)
        .collect();
    let cp = ControlProposal {
        round: 1,
        parent: 0,
        value: Some((4, h_true.clone())),
    };
    control.on_control_ready(others[0], cp.clone());
    control.on_control_ready(others[1], cp.clone());
    control.on_control_ready(others[2], cp.clone());
    assert!(
        control.is_safe_for_test(1),
        "round 1 must be delivered + marked safe by now"
    );
    assert!(
        !control.holds_block_for_test(4),
        "name itself never validated/held B_w directly"
    );

    // A DIFFERENT well-formed proposal for the same view (a different target u=2,
    // hence a different digest) must be rejected outright.
    let wrong_proposal = skip_proposal(4, 2);
    let wrong_effects = control.on_control_serve(4, wrong_proposal);
    assert!(
        wrong_effects.is_empty(),
        "a serve naming a digest different from any pending fetch must change no state"
    );
    assert!(!control.holds_block_for_test(4));

    // The correct-digest serve is accepted.
    let effects = control.on_control_serve(4, true_proposal);
    assert!(
        control.holds_block_for_test(4),
        "the correct-digest serve, matching the outstanding pending_fetch entry, must be accepted"
    );
    let _ = effects; // pump_log's own effects aren't the point of this test
}

/// P6-2(iii) regression (the RS1/agreement fix, exercised end-to-end): a Byzantine
/// poisoning attempt (serving a DIFFERENT well-formed proposal for the same view,
/// before the true one arrives) must be rejected, and the TRUE anchor must still be
/// the one applied once the round is driven to commit -- proving `pump_log`'s
/// digest-mismatch branch is never reached and the correct anchor is unaffected.
#[tokio::test]
async fn p6_2_poisoning_attempt_rejected_true_anchor_still_applies() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    let all = authors();
    let true_proposal = skip_proposal(4, 1);
    let h_true = true_proposal.digest(&test_sid());
    let others: Vec<PublicKey> = all
        .iter()
        .map(|(pk, _)| *pk)
        .filter(|pk| *pk != name)
        .collect();

    // Enter round 1 BEFORE any report exists (so if `name` happens to be round 1's
    // own leader, it proposes ⊥ -- nothing submittable yet -- and never itself
    // attaches/verifies B_w; `curr_round` is now 1, matching `mark_safe`'s own
    // `try_vote` check below).
    control.genesis();

    // Reports reach the submittable threshold (as if OTHER parties, not `name`,
    // reported this completion) -- `name` itself never holds B_w directly.
    control.on_comp_report(4, h_true.clone(), others[0]);
    control.on_comp_report(4, h_true.clone(), others[1]);
    control.on_comp_report(4, h_true.clone(), others[2]);

    // Round 1 delivers (4, h_true) via 2f+1 READY, driven directly (bypassing
    // `name`'s own echo/propose entirely, same technique as
    // `end_to_end_completion_report_to_anchor_via_bottom_bracha` above) --
    // `ensure_fetch` seeds a genuine `pending_fetch` entry for exactly `(4, h_true)`.
    let cp = ControlProposal {
        round: 1,
        parent: 0,
        value: Some((4, h_true.clone())),
    };
    control.on_control_ready(others[0], cp.clone());
    control.on_control_ready(others[1], cp.clone());
    let mut effects = control.on_control_ready(others[2], cp.clone());
    assert!(
        control.is_safe_for_test(1),
        "round 1 must be delivered + marked safe by now"
    );
    assert!(!control.holds_block_for_test(4));

    // The Byzantine poisoning attempt: served BEFORE the true proposal, must be
    // rejected outright.
    let wrong_proposal = skip_proposal(4, 2);
    let poison_effects = control.on_control_serve(4, wrong_proposal);
    assert!(
        poison_effects.is_empty(),
        "the poisoning attempt must change no state"
    );
    assert!(
        !control.holds_block_for_test(4),
        "the poisoning attempt must never populate blocks[4]"
    );

    // The TRUE serve is accepted afterward.
    effects.extend(control.on_control_serve(4, true_proposal));
    assert!(control.holds_block_for_test(4));

    // Drive round 1 to commit: n-f=3 commit votes (`name`'s own, from Mark-safe's
    // `try_vote` above, + these two) -> committed -> Deliver -> pump_log.
    control.on_control_commit(others[0], 1);
    let commit_effects = control.on_control_commit(others[1], 1);
    effects.extend(commit_effects);

    let anchor = effects.iter().find_map(|e| match e {
        Effect::ApplyAnchor(u, outcome, refs) => Some((*u, outcome.clone(), refs.clone())),
        _ => None,
    });
    assert_eq!(
        anchor,
        Some((1, Outcome::Skip, Vec::new())),
        "the TRUE anchor (target view 1, per true_proposal's own M=Skip(1)) must be applied, unaffected by the poisoning attempt"
    );
    assert!(control.is_anchor_resolved(1));
}

// ---------------------------------------------------------------- Fable audit pass 2:
// P2-1/P2-2/P2-3/P2-4 liveness (round-machine predicates re-evaluated on a subset of
// their enabling transitions -- cross-round message reordering, normal on an async
// network, could wedge the log permanently). Every test below drives `ControlLog`
// directly (bypassing `on_control_init`'s leader check via `on_control_ready`, the same
// technique the round-machine tests above already use) so it can force the exact
// out-of-order arrival the synchronous test harness would otherwise never produce.

/// P2-1 regression: round `r+1` RB-delivers (2f+1 READY) while its parent round `r` is
/// not yet safe. Pre-fix, `mark_safe(r+1)` bails at the `!safe.contains(parent)` guard
/// and is NEVER retried once `r` finally becomes safe -- `safe[r+1]` is wedged false
/// forever. Post-fix, marking `r` safe must cascade to `r+1`. (Note: once `r` becomes
/// safe it also votes/advances immediately -- Advance doesn't wait for `r`'s own commit
/// quorum, only `safe && (voted || timed_out)` -- so `curr_round` races ahead past `r+1`
/// too in this exact drive; the assertion below is deliberately just on `safe[r+1]`,
/// the thing P2-1 is actually about, not on the incidental exact `curr_round` it lands
/// on.)
#[tokio::test]
async fn p2_reorder_deliver_cascades_safe_to_child_round() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    control.genesis(); // curr_round = 1
    let all = authors();
    let others: Vec<PublicKey> = all
        .iter()
        .map(|(pk, _)| *pk)
        .filter(|pk| *pk != name)
        .collect();

    // Round 2 (parent = round 1, still unsafe) RB-delivers FIRST -- 2 external READYs
    // plus our own relay reach 2f+1=3, exactly as `bracha_delivers_on_two_f_plus_1_readies`
    // establishes for round 1 there.
    let round2 = ControlProposal {
        round: 2,
        parent: 1,
        value: None,
    };
    control.on_control_ready(others[0], round2.clone());
    control.on_control_ready(others[1], round2);
    assert!(
        !control.is_safe_for_test(2),
        "round 1 (its parent) isn't safe yet -- correctly deferred, not an error"
    );

    // Round 1 (parent = 0, always safe) RB-delivers next -- this must cascade.
    let round1 = ControlProposal {
        round: 1,
        parent: 0,
        value: None,
    };
    control.on_control_ready(others[0], round1.clone());
    control.on_control_ready(others[1], round1);
    assert!(control.is_safe_for_test(1));
    assert!(
        control.is_safe_for_test(2),
        "P2-1: marking round 1 safe must cascade to the already-RB-delivered child round 2 -- pre-fix this stays false forever"
    );
}

/// P2-2 regression, isolated from P2-1's own cascade so it can't be the thing doing the
/// work: round 2 is fixed with parent = round 0 directly (as a real leader would
/// construct it once it already knows round 1 is disabled -- `safe_parent_for` skips
/// disabled rounds), so round 2 becomes safe the INSTANT it RB-delivers, independent of
/// round 1 entirely, while `curr_round` is still 1. `curr_round` only reaches round 2
/// LATER, via a completely separate transition (round 1 gets DISABLED through the
/// reliable-notification path, whose own `try_advance_round` call fires on
/// `disabled[1]`, unrelated to round 2's safety at all). Pre-fix, `enter_round` never
/// retried Vote, so a round already safe before it's entered sits un-voted until some
/// other, unrelated event happens to retrigger it. Post-fix, `enter_round(2)` must vote
/// immediately.
#[tokio::test]
async fn p2_enter_round_votes_immediately_when_already_safe() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    control.genesis(); // curr_round = 1
    let all = authors();
    let others: Vec<PublicKey> = all
        .iter()
        .map(|(pk, _)| *pk)
        .filter(|pk| *pk != name)
        .collect();

    // Round 2 (parent = 0, unconditionally safe) RB-delivers while curr_round is still
    // 1 -- safe[2] becomes true right away, well before curr_round ever reaches it.
    let round2 = ControlProposal {
        round: 2,
        parent: 0,
        value: None,
    };
    control.on_control_ready(others[0], round2.clone());
    control.on_control_ready(others[1], round2);
    assert!(control.is_safe_for_test(2));
    assert_eq!(
        control.curr_round_for_test(),
        1,
        "safe[2] says nothing about curr_round -- still 1"
    );
    assert!(
        !control.voted(),
        "round 2 isn't curr_round yet -- Vote correctly hasn't fired for it"
    );

    // Disable round 1 via reliable notification (never RB-delivered/voted at all) --
    // n-f=3 votes -> accept, then 2f+1=3 accepts -> confirm -> disabled[1], which tries
    // Advance on `disabled[curr_round]` and enters round 2 -- a transition entirely
    // unrelated to round 2's own safety.
    control.on_control_round_timer(1);
    control.on_control_timeout_vote(others[0], 1);
    control.on_control_timeout_vote(others[1], 1);
    control.on_control_timeout_accept(others[0], 1);
    let effects = control.on_control_timeout_accept(others[1], 1);
    assert!(control.is_disabled_for_test(1));
    // P2-2: round 2 was ALREADY safe by the time `curr_round` reached it -- entering it
    // must vote immediately, which (since Advance doesn't wait for a round's own commit
    // quorum) races `curr_round` one step further still, to round 3. What matters is
    // that round 2's OWN commit vote is among the effects of this very call: pre-fix,
    // `enter_round` never retried Vote, so an already-safe round entered this way would
    // sit un-voted, and `curr_round` would have stopped dead at 2.
    assert!(
        control.curr_round_for_test() >= 2,
        "P2-2: disabled[1] must at least advance into round 2 (curr_round={})",
        control.curr_round_for_test()
    );
    assert!(
        effects.iter().any(|e| matches!(e, Effect::BroadcastControlCommit(2))),
        "P2-2: round 2's commit vote must be among the effects of the very call that advanced into it -- \
         pre-fix, `enter_round` never retried Vote, so an already-safe round entered this way never votes"
    );
}

/// P2-3 regression: `n-f` matching commits for round `r` arrive BEFORE `r` is locally
/// safe. Pre-fix, `committed[r]` is set (commit-counting doesn't check `safe`), but
/// `try_deliver` -- called only from `on_control_commit` -- bails on `!safe[r]` and is
/// NEVER retried once `r` becomes safe: `r`'s log suffix is a permanent gap. Post-fix,
/// `mark_safe` must retry Deliver.
#[tokio::test]
async fn p2_commit_before_safe_still_delivers_once_safe() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    control.genesis(); // curr_round = 1
    let all = authors();
    let others: Vec<PublicKey> = all
        .iter()
        .map(|(pk, _)| *pk)
        .filter(|pk| *pk != name)
        .collect();
    let proposal = skip_proposal(4, 1);
    let digest = proposal.digest(&test_sid());
    control.on_completion_reportable(4, proposal); // seeds `blocks[4]` -- needed for
                                                   // `pump_log` to resolve the entry
                                                   // into `Effect::ApplyAnchor` below.

    // Round 2 (parent = round 1, still unsafe) RB-delivers a REAL (non-bottom) value.
    let round2 = ControlProposal {
        round: 2,
        parent: 1,
        value: Some((4, digest.clone())),
    };
    control.on_control_ready(others[0], round2.clone());
    control.on_control_ready(others[1], round2);
    assert!(!control.is_safe_for_test(2));

    // n-f=3 matching commits for round 2 arrive NOW, while round 2 is still unsafe.
    control.on_control_commit(others[0], 2);
    control.on_control_commit(others[1], 2);
    control.on_control_commit(name, 2);
    assert!(
        control.is_committed_for_test(2),
        "commit-counting doesn't gate on safe -- committed[2] is set"
    );
    assert!(
        control.delivered_log_for_test().is_empty(),
        "not yet safe -- try_deliver correctly bailed, nothing delivered yet"
    );

    // Round 1 (parent = 0) now RB-delivers -- cascades safe to round 2 (P2-1), which
    // must retry Deliver (P2-3) since round 2 was already committed.
    let round1 = ControlProposal {
        round: 1,
        parent: 0,
        value: None,
    };
    control.on_control_ready(others[0], round1.clone());
    let effects = control.on_control_ready(others[1], round1);
    assert!(control.is_safe_for_test(2));
    assert_eq!(
        control.delivered_log_for_test(),
        &[(4, digest)],
        "P2-3: round 2's log suffix must be delivered once it becomes safe, even though its n-f commits arrived earlier"
    );
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::ApplyAnchor(1, ..))),
        "pump_log must have run off the back of the retried Deliver"
    );
}

/// P2-4 regression: the round timer fires (`timed_out = true`) while `curr_round` is
/// still unsafe; `curr_round` only becomes safe AFTERWARD. Pre-fix, Advance's
/// `safe[curr] && (voted || timed_out)` predicate is only re-evaluated from `try_vote`
/// (which itself no-ops once `timed_out`) and from `try_deliver` (which needs
/// `committed` too) -- neither fires here, so the node holds at `curr_round` forever
/// even though its own predicate is now true. Post-fix (`on_control_round_timer` retries
/// Advance, and `mark_safe`'s new `try_deliver` retry lets a round that's both
/// `committed` and freshly `safe` reach Advance through the existing `try_deliver ->
/// try_advance_round` chain), the node must advance.
#[tokio::test]
async fn p2_safe_after_timeout_still_advances() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    control.genesis(); // curr_round = 1
    let all = authors();
    let others: Vec<PublicKey> = all
        .iter()
        .map(|(pk, _)| *pk)
        .filter(|pk| *pk != name)
        .collect();

    // Our own round-1 timer fires first -- `timed_out = true`, `voted` stays false.
    control.on_control_round_timer(1);
    assert_eq!(
        control.curr_round_for_test(),
        1,
        "round 1 isn't safe yet -- Advance correctly doesn't fire yet"
    );

    // n-f=3 matching commits for round 1 reach quorum (from OTHER parties' own votes --
    // this node's local timeout doesn't stop others from voting) BEFORE round 1 is
    // locally safe.
    control.on_control_commit(others[0], 1);
    control.on_control_commit(others[1], 1);
    control.on_control_commit(others[2], 1);
    assert!(control.is_committed_for_test(1));
    assert_eq!(
        control.curr_round_for_test(),
        1,
        "committed but not yet safe -- try_deliver correctly bails, no advance yet"
    );

    // Round 1 (parent = 0) now RB-delivers -- Mark-safe fires; `try_vote` no-ops
    // (`timed_out` already true) but `try_deliver` fires (already committed), whose own
    // `safe[curr] && timed_out` check in Advance now holds.
    let round1 = ControlProposal {
        round: 1,
        parent: 0,
        value: None,
    };
    control.on_control_ready(others[0], round1.clone());
    control.on_control_ready(others[1], round1);
    assert!(control.is_safe_for_test(1));
    assert!(
        !control.voted(),
        "timed_out was already true -- Vote must never fire for this round"
    );
    assert_eq!(
        control.curr_round_for_test(),
        2,
        "P2-4: safe[1] && timed_out now both hold -- Advance must fire even though this node itself never voted"
    );
}

/// P2-4, the OTHER missing site (see `mark_safe`'s doc comment): `timed_out` fires
/// first, and `curr_round` becomes safe LATER with NO commits at all yet counted
/// (`committed[curr]` false). Neither `try_vote` (no-ops once `timed_out`) nor
/// `try_deliver` (no-ops while `!committed`) ever reaches `try_advance_round` in this
/// case -- confirmed empirically to still wedge with only fixes #2/#3 (`enter_round`
/// and the timer path) applied; `mark_safe` itself must also re-check Advance directly.
#[tokio::test]
async fn p2_safe_after_timeout_advances_even_with_no_commits_yet() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    control.genesis(); // curr_round = 1
    let all = authors();
    let others: Vec<PublicKey> = all
        .iter()
        .map(|(pk, _)| *pk)
        .filter(|pk| *pk != name)
        .collect();

    control.on_control_round_timer(1); // timed_out = true, voted stays false
    assert_eq!(control.curr_round_for_test(), 1);

    // Round 1 (parent = 0) RB-delivers -- NO commits counted at all yet.
    let round1 = ControlProposal {
        round: 1,
        parent: 0,
        value: None,
    };
    control.on_control_ready(others[0], round1.clone());
    control.on_control_ready(others[1], round1);
    assert!(control.is_safe_for_test(1));
    assert!(
        !control.is_committed_for_test(1),
        "this scenario has zero commits, unlike the sibling test above"
    );
    assert!(
        !control.voted(),
        "timed_out was already true -- Vote must never fire"
    );
    assert_eq!(
        control.curr_round_for_test(),
        2,
        "P2-4: safe[1] && timed_out hold with no commits at all -- Advance must still fire (its predicate has no committed/voted-by-us term)"
    );
}

/// Fable re-audit pass 1 (P2-1 depth, separate from the recursion-cycle test below):
/// a single `mark_safe` cascade establishing K=5000 CONSECUTIVE already-RB-delivered
/// rounds (deliver rounds K down to 2 first -- each one's parent isn't even
/// RB-delivered yet, so `mark_safe` correctly defers every one -- then deliver round 1
/// last, whose parent, round 0, is unconditionally safe) must walk the entire chain,
/// in one `mark_safe` call, voting and advancing through every round, without
/// overflowing the ambient stack.
#[tokio::test]
async fn p2_deep_cascade_five_thousand_rounds_does_not_overflow_stack() {
    const K: u64 = 5000;
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    control.genesis(); // curr_round = 1
    let all = authors();
    let others: Vec<PublicKey> = all
        .iter()
        .map(|(pk, _)| *pk)
        .filter(|pk| *pk != name)
        .collect();

    // RB-deliver rounds K, K-1, ..., 2 FIRST, out of order -- each round r's parent
    // (r-1) isn't even RB-delivered yet at the moment r itself delivers, so `mark_safe`
    // correctly defers every one of them (no cascade at all yet, just O(1) work each).
    for r in (2..=K).rev() {
        let cp = ControlProposal {
            round: r,
            parent: r - 1,
            value: None,
        };
        control.on_control_ready(others[0], cp.clone());
        control.on_control_ready(others[1], cp);
        assert!(!control.is_safe_for_test(r));
    }

    // Round 1 (parent = 0, unconditionally safe) RB-delivers LAST -- the single
    // `mark_safe` call whose worklist cascade must walk the entire K-round chain,
    // voting and advancing through every one of them.
    let round1 = ControlProposal {
        round: 1,
        parent: 0,
        value: None,
    };
    control.on_control_ready(others[0], round1.clone());
    control.on_control_ready(others[1], round1);

    assert!(
        control.is_safe_for_test(K),
        "the whole K-round chain must become safe in the one cascading mark_safe call"
    );
    assert!(
        control.curr_round_for_test() > K,
        "the node must advance all the way through the K-round chain (curr_round = {})",
        control.curr_round_for_test()
    );
}

/// Fable re-audit pass 1 regression: `enter_round_core` (P2-2's Vote-on-entry retry)
/// and `try_advance_round` (Advance) must not close a mutual-recursion cycle. Before
/// this fix, `enter_round` called `try_vote`, which called `try_advance_round`, which
/// called `enter_round` again.
///
/// IMPORTANT: this needs a DIFFERENT structure from the sibling test above to actually
/// be discriminating. That test's CHAIN structure (`parent = r-1`) drives the K-round
/// cascade entirely through `mark_safe`'s own iterative worklist, which marks ONE round
/// safe at a time and votes it immediately -- so by construction the next round is
/// never already-safe when the (reverted, pre-fix) recursive `try_vote ->
/// try_advance_round -> enter_round` excursion reaches it, and that excursion always
/// bottoms out after a single level REGARDLESS of K. Confirmed empirically: reverting
/// to the pre-fix recursive shape and running the sibling test's exact chain structure
/// at K up to 50,000 with a stack as small as 48 KiB never overflows -- that structure
/// cannot expose this bug at all.
///
/// The genuine trigger needs K rounds ALREADY marked safe, ALL before `curr_round` ever
/// reaches the first of them, via transitions entirely SEPARATE from the one that
/// finally advances into them. This test builds exactly that: every round 2..=K+1 has
/// parent = 0 (unconditionally safe), so each becomes safe via its own independent,
/// trivial `mark_safe` call while `curr_round` is still 1 (none of them voted for yet).
/// Round 1 is then DISABLED via reliable notification (a transition wholly unrelated to
/// any of the K rounds' own safety) advancing `curr_round` into round 2 -- already safe,
/// so voting it recurses (pre-fix) into round 3, ALSO already safe, and so on through
/// all K in one unbroken recursive chain.
///
/// Run inside a thread with an explicitly small, realistic stack (2 MiB -- a typical
/// tokio worker-thread default, not the test harness's own inflated ambient default of
/// several MiB, which would mask the bug regardless of K). Confirmed empirically both
/// ways: the pre-fix recursive shape overflows this exact 2 MiB stack at K=5000; the
/// fixed iterative shape survives it, and (checked separately) survives the same 2 MiB
/// stack at K=500,000 too -- O(1) stack, independent of K.
#[test]
fn p2_deep_recursion_cycle_broken_survives_constrained_stack() {
    const STACK_SIZE: usize = 2 * 1024 * 1024; // 2 MiB, a typical tokio worker-thread default
    const K: u64 = 5000;
    let handle = std::thread::Builder::new()
        .stack_size(STACK_SIZE)
        .spawn(move || {
            let (name, _) = authors()[3];
            let mut control = new_control(name);
            control.genesis(); // curr_round = 1
            let all = authors();
            let others: Vec<PublicKey> = all
                .iter()
                .map(|(pk, _)| *pk)
                .filter(|pk| *pk != name)
                .collect();

            // STAR structure: every round 2..=K+1 has parent = 0 (unconditionally safe),
            // so EACH becomes safe via its OWN separate, trivial `mark_safe` call, ALL
            // while curr_round is still 1 and none of them has been voted for yet.
            for r in 2..=(K + 1) {
                let cp = ControlProposal {
                    round: r,
                    parent: 0,
                    value: None,
                };
                control.on_control_ready(others[0], cp.clone());
                control.on_control_ready(others[1], cp);
                assert!(control.is_safe_for_test(r));
            }
            assert_eq!(control.curr_round_for_test(), 1);

            // Disable round 1 via reliable notification (never RB-delivered/voted) --
            // an entirely separate transition mechanism, unrelated to any of the K
            // rounds' own safety -- advances curr_round into round 2, which is ALREADY
            // safe: voting it recurses (pre-fix) into round 3, ALSO already safe, and so
            // on through all K, in one unbroken chain if the cycle is not actually gone.
            control.on_control_round_timer(1);
            control.on_control_timeout_vote(others[0], 1);
            control.on_control_timeout_vote(others[1], 1);
            control.on_control_timeout_accept(others[0], 1);
            control.on_control_timeout_accept(others[1], 1);
            control.curr_round_for_test()
        })
        .unwrap();
    let curr_round = handle.join().expect("a 2 MiB stack must survive a K=5000 already-safe cascade -- the recursion cycle must be gone");
    assert_eq!(
        curr_round,
        K + 2,
        "must advance all the way through the K already-safe rounds plus the disabled round 1"
    );
}

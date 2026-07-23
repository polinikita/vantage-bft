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
    ViewProposal { view: w, c: Vec::new(), t: Vec::new(), m: Some(ResolutionEntry::Skip(u)) }
}

fn echo_effect(effects: &[Effect]) -> bool {
    effects.iter().any(|e| matches!(e, Effect::BroadcastControlEcho(_)))
}

fn ready_effect(effects: &[Effect]) -> bool {
    effects.iter().any(|e| matches!(e, Effect::BroadcastControlReady(_)))
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
    assert!(effects.iter().any(|e| matches!(e, Effect::BroadcastCompReport(v, d) if *v == 4 && *d == digest)));
    assert!(control.holds_block_for_test(4));
    assert_eq!(control.report_count_for(4, &digest), 1, "our own report counts first-hand, immediately");

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
    let proposal = ControlProposal { round: 1, parent: 0, value: None };
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
    let proposal = ControlProposal { round: 1, parent: 0, value: Some((4, digest.clone())) };

    let effects = control.on_control_init(leader, proposal, Some(b_w));
    assert!(!echo_effect(&effects), "0 matching reports -- ReadyOK-style validity gate blocks ECHO");

    control.on_comp_report(4, digest.clone(), all[0].0);
    // Still only 1 report (< f+1=2).
    assert!(control.report_count_for(4, &digest) < 2);

    let effects2 = control.on_comp_report(4, digest.clone(), all[1].0);
    assert!(echo_effect(&effects2), "f+1=2 matching reports now met -- retry_pending_echoes fires the ECHO");
}

#[tokio::test]
async fn bracha_init_from_non_leader_is_ignored() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    let all = authors();
    let leader = control.control_leader(1);
    let impostor = all.iter().find(|(pk, _)| *pk != leader).unwrap().0;
    let proposal = ControlProposal { round: 1, parent: 0, value: None };
    let effects = control.on_control_init(impostor, proposal, None);
    assert!(effects.is_empty(), "only the round's declared leader can ever fix a proposal");
}

#[tokio::test]
async fn bracha_ready_on_two_f_plus_1_echoes() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    let all = authors();
    let proposal = ControlProposal { round: 1, parent: 0, value: None };

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
    let proposal = ControlProposal { round: 1, parent: 0, value: None };

    // Only 1 echo counted (far below 2f+1), but f+1=2 READYs relay the value anyway
    // (Bracha's classic relay rule).
    control.on_control_echo(all[0].0, proposal.clone());
    control.on_control_ready(all[1].0, proposal.clone());
    let effects = control.on_control_ready(all[2].0, proposal.clone());
    assert!(ready_effect(&effects), "f+1=2 matching READYs relay our own READY even without echo quorum");
}

#[tokio::test]
async fn bracha_delivers_on_two_f_plus_1_readies() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    let all = authors();
    let proposal = ControlProposal { round: 1, parent: 0, value: None };

    let e1 = control.on_control_ready(all[0].0, proposal.clone());
    assert!(!ready_effect(&e1), "only 1 external READY counted -- below f+1=2, no relay yet");
    // The 2nd external READY reaches the f+1=2 relay threshold, which makes us send
    // our OWN READY -- and since that's the 3rd (2 external + our own), it ALSO
    // immediately reaches the 2f+1=3 delivery threshold in the same call.
    let e2 = control.on_control_ready(all[1].0, proposal);
    assert!(ready_effect(&e2), "f+1=2 relay -- our own READY fires");
    assert!(control.is_safe_for_test(1), "our own relayed READY completed 2f+1 -- delivered, Mark-safe ran (parent=0 always safe)");
}

// ---------------------------------------------------------------- round machine

#[tokio::test]
async fn round_machine_vote_commit_deliver_advance_on_bottom_round() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    control.genesis(); // enters round 1 (curr_round=1) -- required for Vote's
                        // `r == curr_round` guard to ever pass below.
    let all = authors();
    let proposal = ControlProposal { round: 1, parent: 0, value: None };

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
    assert!(!control.is_committed_for_test(1), "only 2 commits counted so far (self + 1)");
    control.on_control_commit(all[1].0, 1);
    assert!(control.is_committed_for_test(1));

    // Deliver: round 1's (empty, ⊥) chain contributes nothing to the log, but Advance
    // round must have fired (`voted=true` from the Vote rule).
    assert_eq!(control.curr_round_for_test(), 2, "advance round: safe[1] && voted -- enter round 2");
    assert!(control.delivered_log_for_test().is_empty(), "round 1's value was ⊥ -- nothing appended to L");
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
    assert!(effects.iter().any(|e| matches!(e, Effect::BroadcastControlTimeoutVote(1))));

    control.on_control_timeout_vote(all[0].0, 1);
    let e = control.on_control_timeout_vote(all[1].0, 1);
    assert!(e.iter().any(|eff| matches!(eff, Effect::BroadcastControlTimeoutAccept(1))), "n-f=3 votes (self+2) -- Accept");

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
        e2.iter().any(|eff| matches!(eff, Effect::BroadcastControlTimeoutAccept(1))),
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
    assert!(e1.iter().any(|e| matches!(e, Effect::ControlServeTo(peer, v, _) if *peer == all[0].0 && *v == 4)));
    let e2 = control.on_control_fetch(all[0].0, 4, digest);
    assert!(e2.is_empty(), "a holder answers at most once per requester-(w,h) pair");
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
        let cp = ControlProposal { round: 1, parent: 0, value: Some((4, digest.clone())) };
        effects.extend(control.on_control_init(leader, cp, Some(proposal)));
    }

    // Deliver via 2f+1 matching READYs (2 external + our own relay, as established
    // above) from the two OTHER parties (neither is `leader` nor `name`, so this
    // works regardless of which case above ran).
    let others: Vec<PublicKey> = all.iter().map(|(pk, _)| *pk).filter(|pk| *pk != name && *pk != leader).collect();
    let cp = ControlProposal { round: 1, parent: 0, value: Some((4, digest)) };
    effects.extend(control.on_control_ready(others[0], cp.clone()));
    effects.extend(control.on_control_ready(others[1], cp.clone()));
    assert!(control.is_safe_for_test(1), "round 1 must have delivered + marked safe by now");

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
    let cp = ControlProposal { round: 1, parent: 0, value: Some((4, digest.clone())) };
    control.on_control_init(leader, cp, Some(proposal.clone()));
    assert!(control.holds_block_for_test(4), "blocks[4] must already be held via try_echo's INIT-attachment, before any genuine completion");

    // `name`'s OWN genuine R4 completion of view 4 must STILL broadcast a CompReport.
    let effects = control.on_completion_reportable(4, proposal.clone());
    assert!(
        effects.iter().any(|e| matches!(e, Effect::BroadcastCompReport(v, d) if *v == 4 && *d == digest)),
        "a genuine completion must report even when B_w was already held from elsewhere"
    );
    assert_eq!(control.report_count_for(4, &digest), 3, "name's own first-hand report must count too (self + the 2 seeded above)");

    // Idempotent: a second completion call for the same view never re-reports.
    let effects2 = control.on_completion_reportable(4, proposal);
    assert!(effects2.is_empty(), "a repeated completion for an already-reported view must be a no-op");
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
    assert!(!control.holds_block_for_test(4), "an unsolicited serve must never populate blocks[w]");
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
    let others: Vec<PublicKey> = all.iter().map(|(pk, _)| *pk).filter(|pk| *pk != name).collect();
    let cp = ControlProposal { round: 1, parent: 0, value: Some((4, h_true.clone())) };
    control.on_control_ready(others[0], cp.clone());
    control.on_control_ready(others[1], cp.clone());
    control.on_control_ready(others[2], cp.clone());
    assert!(control.is_safe_for_test(1), "round 1 must be delivered + marked safe by now");
    assert!(!control.holds_block_for_test(4), "name itself never validated/held B_w directly");

    // A DIFFERENT well-formed proposal for the same view (a different target u=2,
    // hence a different digest) must be rejected outright.
    let wrong_proposal = skip_proposal(4, 2);
    let wrong_effects = control.on_control_serve(4, wrong_proposal);
    assert!(wrong_effects.is_empty(), "a serve naming a digest different from any pending fetch must change no state");
    assert!(!control.holds_block_for_test(4));

    // The correct-digest serve is accepted.
    let effects = control.on_control_serve(4, true_proposal);
    assert!(control.holds_block_for_test(4), "the correct-digest serve, matching the outstanding pending_fetch entry, must be accepted");
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
    let others: Vec<PublicKey> = all.iter().map(|(pk, _)| *pk).filter(|pk| *pk != name).collect();

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
    let cp = ControlProposal { round: 1, parent: 0, value: Some((4, h_true.clone())) };
    control.on_control_ready(others[0], cp.clone());
    control.on_control_ready(others[1], cp.clone());
    let mut effects = control.on_control_ready(others[2], cp.clone());
    assert!(control.is_safe_for_test(1), "round 1 must be delivered + marked safe by now");
    assert!(!control.holds_block_for_test(4));

    // The Byzantine poisoning attempt: served BEFORE the true proposal, must be
    // rejected outright.
    let wrong_proposal = skip_proposal(4, 2);
    let poison_effects = control.on_control_serve(4, wrong_proposal);
    assert!(poison_effects.is_empty(), "the poisoning attempt must change no state");
    assert!(!control.holds_block_for_test(4), "the poisoning attempt must never populate blocks[4]");

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

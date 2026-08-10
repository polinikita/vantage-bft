use super::common::*;
use crate::vantage::agb::{Outcome, ProposalOut, ResolutionEntry};
use crate::vantage::control::{ControlLog, ControlProposal};
use crate::vantage::Effect;
use crate::vantage::ViewProposal;
use crypto::PublicKey;

fn new_control(name: PublicKey) -> ControlLog {
    ControlLog::new(name, test_committee(), test_sid(), TEST_DELTA_MS)
}

fn skip_proposal(w: u64, u: u64) -> ProposalOut {
    ProposalOut::Single(ViewProposal {
        view: w,
        c: Vec::new(),
        t: Vec::new(),
        m: Some(ResolutionEntry::Skip(u)),
    })
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

#[tokio::test]
async fn reports_census_dedup_by_sender_and_matching_count_by_digest() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    let all = authors();
    let d1 = crypto::Digest([1u8; 32]);
    let d2 = crypto::Digest([2u8; 32]);

    control.on_comp_report(4, d1.clone(), all[0].0);
    control.on_comp_report(4, d1.clone(), all[1].0);
    control.on_comp_report(4, d2.clone(), all[2].0);
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

    let effects2 = control.on_completion_reportable(4, proposal);
    assert!(effects2.is_empty());
}

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
    let e2 = control.on_control_ready(all[1].0, proposal);
    assert!(ready_effect(&e2), "f+1=2 relay -- our own READY fires");
    assert!(
        control.is_safe_for_test(1),
        "our own relayed READY completed 2f+1 -- delivered, Mark-safe ran (parent=0 always safe)"
    );
}

#[tokio::test]
async fn round_machine_vote_commit_deliver_advance_on_bottom_round() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    control.genesis(); // Enter round 1 before voting.
    let all = authors();
    let proposal = ControlProposal {
        round: 1,
        parent: 0,
        value: None,
    };

    control.on_control_ready(all[0].0, proposal.clone());
    control.on_control_ready(all[1].0, proposal);
    assert!(control.is_safe_for_test(1));

    control.on_control_commit(all[0].0, 1);
    assert!(
        !control.is_committed_for_test(1),
        "only 2 commits counted so far (self + 1)"
    );
    control.on_control_commit(all[1].0, 1);
    assert!(control.is_committed_for_test(1));

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
    control.genesis(); // Enter round 1 before starting its timer.
    let all = authors();

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
    let _ = e2;
    assert!(control.is_disabled_for_test(1));
    assert_eq!(control.curr_round_for_test(), 2);
}

#[tokio::test]
async fn reliable_notification_cascade_fires_accept_at_f_plus_1_without_n_minus_f_votes() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    let all = authors();
    control.on_control_round_timer(1); // Record the local timeout vote.

    let e1 = control.on_control_timeout_accept(all[0].0, 1);
    assert!(e1.is_empty(), "only 1 accept observed -- below f+1=2");
    let e2 = control.on_control_timeout_accept(all[1].0, 1);
    assert!(
        e2.iter()
            .any(|eff| matches!(eff, Effect::BroadcastControlTimeoutAccept(1))),
        "f+1=2 accepts observed -- Cascade fires our own accept even without n-f votes"
    );
}

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
async fn fetch_serves_below_state_floor_down_to_the_serve_floor() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    let requester = authors()[0].0;
    let proposal = skip_proposal(4, 1);
    control.on_completion_reportable(4, proposal.clone());
    let digest = proposal.digest(&test_sid());

    control.gc_below(6, 3);

    assert!(
        control
            .on_control_fetch(requester, 4, digest.clone())
            .iter()
            .any(
                |e| matches!(e, Effect::ControlServeTo(peer, v, _) if *peer == requester && *v == 4)
            ),
        "a body we still hold must be served even though its view is below min_live_view"
    );

    control.gc_below(9, 7);
    assert!(
        control.on_control_fetch(requester, 4, digest).is_empty(),
        "past the serve floor the body is dropped and the fetch goes unanswered"
    );
}

#[tokio::test]
async fn end_to_end_completion_report_to_anchor_via_bottom_bracha() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    let all = authors();
    let proposal = skip_proposal(4, 1);
    let digest = proposal.digest(&test_sid());

    control.on_completion_reportable(4, proposal.clone());
    control.on_comp_report(4, digest.clone(), all[0].0);
    control.on_comp_report(4, digest.clone(), all[1].0);

    let leader = control.control_leader(1);
    let mut effects = control.genesis(); // Enter round 1 and propose if selected.
    if leader != name {
        let cp = ControlProposal {
            round: 1,
            parent: 0,
            value: Some((4, digest.clone())),
        };
        effects.extend(control.on_control_init(leader, cp, Some(proposal)));
    }

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

#[tokio::test]
async fn fetch_then_complete_still_reports() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    let all = authors();
    let proposal = skip_proposal(4, 1);
    let digest = proposal.digest(&test_sid());

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

    let effects2 = control.on_completion_reportable(4, proposal);
    assert!(
        effects2.is_empty(),
        "a repeated completion for an already-reported view must be a no-op"
    );
}

#[tokio::test]
async fn unsolicited_serve_changes_no_state() {
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

#[tokio::test]
async fn wrong_digest_serve_rejected_correct_digest_accepted() {
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

    let wrong_proposal = skip_proposal(4, 2);
    let wrong_effects = control.on_control_serve(4, wrong_proposal);
    assert!(
        wrong_effects.is_empty(),
        "a serve naming a digest different from any pending fetch must change no state"
    );
    assert!(!control.holds_block_for_test(4));

    let effects = control.on_control_serve(4, true_proposal);
    assert!(
        control.holds_block_for_test(4),
        "the correct-digest serve, matching the outstanding pending_fetch entry, must be accepted"
    );
    let _ = effects; // Only the resulting state matters here.
}

#[tokio::test]
async fn poisoning_attempt_rejected_true_anchor_still_applies() {
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

    control.genesis();

    control.on_comp_report(4, h_true.clone(), others[0]);
    control.on_comp_report(4, h_true.clone(), others[1]);
    control.on_comp_report(4, h_true.clone(), others[2]);

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

    effects.extend(control.on_control_serve(4, true_proposal));
    assert!(control.holds_block_for_test(4));

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

#[tokio::test]
async fn reordered_delivery_cascades_safety_to_child_round() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    control.genesis(); // Enter round 1.
    let all = authors();
    let others: Vec<PublicKey> = all
        .iter()
        .map(|(pk, _)| *pk)
        .filter(|pk| *pk != name)
        .collect();

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
        "marking round 1 safe must cascade to the delivered child round"
    );
}

#[tokio::test]
async fn entering_an_already_safe_round_votes_immediately() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    control.genesis(); // Enter round 1.
    let all = authors();
    let others: Vec<PublicKey> = all
        .iter()
        .map(|(pk, _)| *pk)
        .filter(|pk| *pk != name)
        .collect();

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

    control.on_control_round_timer(1);
    control.on_control_timeout_vote(others[0], 1);
    control.on_control_timeout_vote(others[1], 1);
    control.on_control_timeout_accept(others[0], 1);
    let effects = control.on_control_timeout_accept(others[1], 1);
    assert!(control.is_disabled_for_test(1));
    assert!(
        control.curr_round_for_test() >= 2,
        "disabling round 1 must advance into round 2 (curr_round={})",
        control.curr_round_for_test()
    );
    assert!(
        effects.iter().any(|e| matches!(e, Effect::BroadcastControlCommit(2))),
        "the transition into safe round 2 must emit its commit vote"
    );
}

#[tokio::test]
async fn commit_before_safety_delivers_once_safe() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    control.genesis(); // Enter round 1.
    let all = authors();
    let others: Vec<PublicKey> = all
        .iter()
        .map(|(pk, _)| *pk)
        .filter(|pk| *pk != name)
        .collect();
    let proposal = skip_proposal(4, 1);
    let digest = proposal.digest(&test_sid());
    control.on_completion_reportable(4, proposal); // Store the block used by round 2.

    let round2 = ControlProposal {
        round: 2,
        parent: 1,
        value: Some((4, digest.clone())),
    };
    control.on_control_ready(others[0], round2.clone());
    control.on_control_ready(others[1], round2);
    assert!(!control.is_safe_for_test(2));

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
        "round 2 must deliver once safe even if its commits arrived first"
    );
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::ApplyAnchor(1, ..))),
        "pump_log must have run off the back of the retried Deliver"
    );
}

#[tokio::test]
async fn safety_after_timeout_still_advances() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    control.genesis(); // Enter round 1.
    let all = authors();
    let others: Vec<PublicKey> = all
        .iter()
        .map(|(pk, _)| *pk)
        .filter(|pk| *pk != name)
        .collect();

    control.on_control_round_timer(1);
    assert_eq!(
        control.curr_round_for_test(),
        1,
        "round 1 isn't safe yet -- Advance correctly doesn't fire yet"
    );

    control.on_control_commit(others[0], 1);
    control.on_control_commit(others[1], 1);
    control.on_control_commit(others[2], 1);
    assert!(control.is_committed_for_test(1));
    assert_eq!(
        control.curr_round_for_test(),
        1,
        "committed but not yet safe -- try_deliver correctly bails, no advance yet"
    );

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
        "a safe timed-out round must advance without a local vote"
    );
}

#[tokio::test]
async fn safety_after_timeout_advances_without_commits() {
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    control.genesis(); // Enter round 1.
    let all = authors();
    let others: Vec<PublicKey> = all
        .iter()
        .map(|(pk, _)| *pk)
        .filter(|pk| *pk != name)
        .collect();

    control.on_control_round_timer(1); // Mark round 1 timed out without a commit vote.
    assert_eq!(control.curr_round_for_test(), 1);

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
        "a safe timed-out round must advance without commit evidence"
    );
}

#[tokio::test]
async fn deep_cascade_does_not_overflow_stack() {
    const K: u64 = 5000;
    let (name, _) = authors()[3];
    let mut control = new_control(name);
    control.genesis(); // Enter round 1.
    let all = authors();
    let others: Vec<PublicKey> = all
        .iter()
        .map(|(pk, _)| *pk)
        .filter(|pk| *pk != name)
        .collect();

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

#[test]
fn deep_recursion_cycle_survives_constrained_stack() {
    const STACK_SIZE: usize = 2 * 1024 * 1024; // 2 MiB.
    const K: u64 = 5000;
    let handle = std::thread::Builder::new()
        .stack_size(STACK_SIZE)
        .spawn(move || {
            let (name, _) = authors()[3];
            let mut control = new_control(name);
            control.genesis(); // Enter round 1.
            let all = authors();
            let others: Vec<PublicKey> = all
                .iter()
                .map(|(pk, _)| *pk)
                .filter(|pk| *pk != name)
                .collect();

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

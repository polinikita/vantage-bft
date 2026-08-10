// PHASE5-SPEC.md §12/§4 "W1"/"W2" -- the WISH pacemaker's genesis bootstrap and the
// order-statistics amplification/formal-entry-target-advance step, driven directly
// against `Pacemaker` (pure, no wiring -- PHASE5-SPEC.md's suggested implementation
// order).

use super::common::*;
use crate::vantage::pacemaker::Pacemaker;
use crate::vantage::Effect;

fn broadcast_wish(effects: &[Effect]) -> Option<crate::primary::View> {
    effects.iter().find_map(|e| match e {
        Effect::BroadcastWish(v) => Some(*v),
        _ => None,
    })
}

fn entries(effects: &[Effect]) -> Vec<crate::primary::View> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Enter(v) => Some(*v),
            _ => None,
        })
        .collect()
}

// --- W1: genesis + array initialization ----------------------------------------------

#[test]
fn w1_omega_initializes_to_zero_for_every_author() {
    let (name, _) = authors()[0];
    let pm = Pacemaker::new(name, &test_committee());
    for (author, _) in authors() {
        assert_eq!(pm.omega_of(author), 0);
    }
    assert_eq!(pm.own_watermark(), 0);
    assert_eq!(pm.entry_target(), 0);
    assert_eq!(pm.largest_entered_view_for_test(), 0);
}

#[test]
fn w1_genesis_sets_own_wish_to_2_and_broadcasts_with_self_delivery() {
    let (name, _) = authors()[0];
    let mut pm = Pacemaker::new(name, &test_committee());
    let effects = pm.genesis();

    assert_eq!(
        broadcast_wish(&effects),
        Some(2),
        "genesis must broadcast wish(2)"
    );
    // "Self-delivery immediate": the own slot is updated synchronously, right here --
    // no separate self-addressed message round-trip is needed to observe it.
    assert_eq!(pm.own_watermark(), 2);
    assert_eq!(pm.omega_of(name), 2);
    // View 1's entry already happened (the existing boot behavior, outside this
    // struct) -- genesis must record that so it is never re-scheduled.
    assert_eq!(pm.largest_entered_view_for_test(), 1);
    // Only 1 of n=4 slots is raised -- nowhere near 2f+1=3 -- so genesis alone must
    // never itself schedule any further entry.
    assert!(entries(&effects).is_empty());
}

// --- W2: receipt, amplification, entry (strict order + boundaries) --------------------

#[test]
fn w2_omega_plus_boundary_exactly_f_plus_1_senders() {
    // n=4, f=1 => f+1 = 2: a single first-hand wish(5) must not amplify (one short of
    // the boundary); a second, distinct sender's wish(5) must.
    let all = authors();
    let (name, _) = all[3];
    let (p1, _) = all[0];
    let (p2, _) = all[1];
    let mut pm = Pacemaker::new(name, &test_committee());

    let effects = pm.on_wish(p1, 5);
    assert!(
        effects.is_empty(),
        "f+1 boundary not yet met with only 1 of 2 senders"
    );
    assert_eq!(pm.own_watermark(), 0);

    let effects = pm.on_wish(p2, 5);
    assert_eq!(
        broadcast_wish(&effects),
        Some(5),
        "the 2nd (= f+1) distinct sender must trigger amplification"
    );
    assert_eq!(
        pm.own_watermark(),
        5,
        "amplification must update the own slot/watermark"
    );
    assert_eq!(pm.omega_of(name), 5);
}

#[test]
fn w2_omega_q_boundary_exactly_two_f_plus_1_senders_independent_of_amplification() {
    // Isolates the 2f+1 = 3 entry-target boundary from amplification: pre-raise our
    // own slot to the target value directly (W3's `raise_own_wish`, no broadcast), so
    // the two external wishes below can cross 2f+1 without *also* re-triggering
    // amplification (own_watermark is already at the target, so `omega_plus >
    // own_watermark` is false throughout) -- demonstrating "the two updates are
    // independent" from the entry side alone.
    let all = authors();
    let (name, _) = all[3];
    let (p1, _) = all[0];
    let (p2, _) = all[1];
    let mut pm = Pacemaker::new(name, &test_committee());
    assert!(
        pm.raise_own_wish(5).is_empty(),
        "1 of 4 slots at 5 is nowhere near 2f+1=3"
    );

    let effects = pm.on_wish(p1, 5);
    assert!(
        entries(&effects).is_empty(),
        "2f+1 boundary not yet met with only 2 of 3 (self + 1 external)"
    );

    let effects = pm.on_wish(p2, 5);
    assert!(
        broadcast_wish(&effects).is_none(),
        "own watermark already at the target -- no re-amplification"
    );
    assert_eq!(entries(&effects), vec![1, 2, 3, 4, 5], "the 3rd (= 2f+1) party crossing the target must enter every missing view through it, in order");
}

#[test]
fn w2_amplification_precedes_entry_and_entry_is_recorded_in_increasing_order() {
    // After genesis (view 1 already entered), 2 more first-hand wish(5)es from
    // distinct senders amplify our own watermark to 5 (crossing omega_plus's f+1=2
    // boundary) *and* that very amplification is what pushes omega_q to 3-of-4 slots
    // at 5 (self included) -- so entry to every missing view through 5 fires in the
    // same call, immediately after the (order-first) amplification.
    let all = authors();
    let (name, _) = all[3];
    let (p1, _) = all[0];
    let (p2, _) = all[1];
    let mut pm = Pacemaker::new(name, &test_committee());
    let _ = pm.genesis();

    let effects = pm.on_wish(p1, 5);
    assert!(effects.is_empty());

    let effects = pm.on_wish(p2, 5);
    assert_eq!(broadcast_wish(&effects), Some(5));
    assert_eq!(
        entries(&effects),
        vec![2, 3, 4, 5],
        "view 1 already entered by genesis -- must never be re-scheduled"
    );
    // Amplification's `Effect::BroadcastWish` must precede every `Effect::Enter` in
    // the returned order.
    let wish_pos = effects
        .iter()
        .position(|e| matches!(e, Effect::BroadcastWish(_)))
        .unwrap();
    let first_enter_pos = effects
        .iter()
        .position(|e| matches!(e, Effect::Enter(_)))
        .unwrap();
    assert!(wish_pos < first_enter_pos);
}

#[test]
fn w2_a_wish_for_x_supports_every_view_up_to_x() {
    // A quorum of wishes for x=5 must enter every missing view <= 5, not just 5 itself
    // (already exercised by the two tests above via their multi-view `Enter` vectors);
    // this test isolates the "supports every view <= x" framing directly against a
    // pacemaker that starts with a non-trivial `largest_entered_view` (3, simulating
    // prior entries), confirming only the *missing* suffix (4, 5) is scheduled.
    let all = authors();
    let (name, _) = all[3];
    let (p1, _) = all[0];
    let (p2, _) = all[1];
    let mut pm = Pacemaker::new(name, &test_committee());
    assert!(pm.raise_own_wish(3).is_empty());
    assert!(pm.on_wish(p1, 3).is_empty());
    let effects = pm.on_wish(p2, 3);
    assert_eq!(entries(&effects), vec![1, 2, 3]);

    let effects = pm.on_wish(p1, 5);
    assert!(
        effects.is_empty(),
        "only 2 of 3 senders now at >= 5 (self is still at 3)"
    );
    let effects = pm.on_wish(p2, 5);
    assert_eq!(
        entries(&effects),
        vec![4, 5],
        "only the missing suffix through the new target is scheduled"
    );
}

#[test]
fn w2_stale_wish_causes_no_transition() {
    let all = authors();
    let (name, _) = all[3];
    let (p1, _) = all[0];
    let mut pm = Pacemaker::new(name, &test_committee());
    let _ = pm.on_wish(p1, 5);
    assert_eq!(pm.omega_of(p1), 5);

    let effects = pm.on_wish(p1, 3); // stale: 3 <= the sender's already-recorded 5
    assert!(effects.is_empty());
    assert_eq!(
        pm.omega_of(p1),
        5,
        "a stale wish must never lower the sender's slot"
    );

    let effects = pm.on_wish(p1, 5); // stale: equal, not "larger"
    assert!(effects.is_empty());
}

// --- W3 (raise_own_wish's own contract, exercised directly here; the trigger
// arithmetic itself is exercised end-to-end against `AgbEngine` in
// `wish_trigger_tests.rs`) --------------------------------------------------------------

#[test]
fn raise_own_wish_never_broadcasts_and_is_a_no_op_below_current_watermark() {
    let (name, _) = authors()[0];
    let mut pm = Pacemaker::new(name, &test_committee());
    let effects = pm.raise_own_wish(4);
    assert!(
        broadcast_wish(&effects).is_none(),
        "W3's raise must never itself broadcast a standalone VantageWish"
    );
    assert_eq!(pm.own_watermark(), 4);

    let effects = pm.raise_own_wish(2); // not larger than the current watermark
    assert!(effects.is_empty());
    assert_eq!(pm.own_watermark(), 4);
}

#[test]
fn installed_entry_fast_forward_skips_history_but_keeps_future_wish_entry_normal() {
    let all = authors();
    let (name, _) = all[3];
    let (p1, _) = all[0];
    let (p2, _) = all[1];
    let mut pm = Pacemaker::new(name, &test_committee());
    let _ = pm.genesis();

    // A checkpoint install has already made views <= 5 terminal locally. The pacemaker
    // must not later replay Enter(2)..Enter(5), but it should still behave normally for
    // future wish quorums above that floor.
    pm.fast_forward_installed_entry(6);
    assert_eq!(pm.largest_entered_view_for_test(), 6);
    assert_eq!(pm.entry_target(), 6);
    assert_eq!(pm.own_watermark(), 6);
    assert_eq!(pm.omega_of(name), 6);

    assert!(pm.on_wish(p1, 8).is_empty());
    let effects = pm.on_wish(p2, 8);
    assert_eq!(broadcast_wish(&effects), Some(8));
    assert_eq!(
        entries(&effects),
        vec![7, 8],
        "only the post-install suffix should be entered by later WISH quorum"
    );
}

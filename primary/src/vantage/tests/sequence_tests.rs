// SEQUENCE-CHECKPOINT-SYNC-PLAN.md §13 (pure unit tests), PHASE A subset.
//
// Everything here is about ONE property: two correct parties that terminally process the
// same views must derive the identical head, and nothing else must. Phase A cannot be
// promoted to announcement until that holds, because an announced head derived by
// divergent code would let `f+1` honest parties certify a target no correct party can
// serve.

use super::common::{authors, test_sid};
use crate::vantage::agb::Manifest;
use crate::vantage::sequence::*;
use crypto::Digest;

fn digest(byte: u8) -> Digest {
    Digest([byte; 32])
}

fn manifest(byte: u8) -> Manifest {
    let (author, _) = authors()[0];
    vec![(author, 1, digest(byte))]
}

/// A distinct session id, for checking domain separation across sessions.
fn other_sid() -> Digest {
    digest(0xAB)
}

#[test]
fn record_head_is_deterministic_and_session_separated() {
    let sid = test_sid();
    let record = SequenceRecord {
        version: SEQUENCE_VERSION,
        view: 7,
        previous_head: genesis_head(&sid),
        outcome_digest: digest(1),
        delta_len: 2,
        delta_head: digest(2),
    };
    assert_eq!(record.head(&sid), record.head(&sid), "hashing must be pure");
    assert_ne!(
        record.head(&sid),
        record.head(&other_sid()),
        "the same record under a different session must not share a head"
    );
    assert_ne!(
        genesis_head(&sid),
        genesis_head(&other_sid()),
        "genesis heads must be session separated too"
    );
}

/// `Full`, `Core`, and `Skip` must be distinguishable even when they name the SAME
/// manifest. This is why the plan commits the terminal outcome rather than the proposal
/// digest: `Full(c, t)` and `Core(c)` share `c`, so a proposal-keyed encoding would give
/// two different output sequences the same head.
#[test]
fn outcome_variants_have_distinct_digests() {
    let sid = test_sid();
    let c = manifest(1);
    let full = SequenceOutcome::Full {
        c: c.clone(),
        t: manifest(2),
    };
    let core = SequenceOutcome::Core { c };
    let skip = SequenceOutcome::Skip;

    let (a, b, s) = (
        full.digest(&sid, 3),
        core.digest(&sid, 3),
        skip.digest(&sid, 3),
    );
    assert_ne!(a, b);
    assert_ne!(a, s);
    assert_ne!(b, s);
    assert_ne!(
        skip.digest(&sid, 3),
        skip.digest(&sid, 4),
        "the view must be bound into the outcome digest"
    );
}

#[test]
fn every_record_field_is_bound_into_the_head() {
    let sid = test_sid();
    let base = SequenceRecord {
        version: SEQUENCE_VERSION,
        view: 5,
        previous_head: digest(9),
        outcome_digest: digest(1),
        delta_len: 2,
        delta_head: digest(2),
    };
    let head = base.head(&sid);

    let mut view = base.clone();
    view.view = 6;
    let mut previous = base.clone();
    previous.previous_head = digest(10);
    let mut outcome = base.clone();
    outcome.outcome_digest = digest(3);
    let mut len = base.clone();
    len.delta_len = 3;
    let mut delta = base.clone();
    delta.delta_head = digest(4);
    let mut version = base.clone();
    version.version = SEQUENCE_VERSION + 1;

    for (label, changed) in [
        ("view", view),
        ("previous_head", previous),
        ("outcome_digest", outcome),
        ("delta_len", len),
        ("delta_head", delta),
        ("version", version),
    ] {
        assert_ne!(changed.head(&sid), head, "{label} must change the head");
    }
}

/// The delta chain must bind ORDER, not just membership -- the cursor's output is a
/// sequence, and two nodes that emitted the same blocks in different orders have
/// genuinely diverged.
#[test]
fn delta_chain_binds_order_and_position() {
    let sid = test_sid();
    let forward = [digest(1), digest(2), digest(3)];
    let swapped = [digest(1), digest(3), digest(2)];

    let (len_a, head_a) = delta_commitment(&sid, 4, &forward);
    let (len_b, head_b) = delta_commitment(&sid, 4, &swapped);
    assert_eq!(len_a, 3);
    assert_eq!(len_b, 3);
    assert_ne!(head_a, head_b, "reordering must change the delta head");

    assert_ne!(
        delta_commitment(&sid, 4, &forward).1,
        delta_commitment(&sid, 5, &forward).1,
        "the view must be bound into the delta chain"
    );
    assert_ne!(
        delta_commitment(&sid, 4, &forward).1,
        delta_commitment(&other_sid(), 4, &forward).1,
        "the session must be bound into the delta chain"
    );
}

/// A `Skip` emits nothing, so its commitment is the bare seed with length 0. A receiver
/// uses exactly this to reject a `Skip` record that arrives carrying items.
#[test]
fn empty_delta_commits_to_the_seed() {
    let sid = test_sid();
    let (len, head) = delta_commitment(&sid, 11, &[]);
    assert_eq!(len, 0);
    assert_eq!(head, delta_seed(&sid, 11));
}

/// Models the chunked receiver of §7.3: fold the items one at a time, in index order,
/// and arrive at the same head the producer committed to. This is what lets an
/// arbitrarily large delta stream without buffering one oversized frame.
#[test]
fn incremental_verification_reproduces_the_commitment() {
    let sid = test_sid();
    let items: Vec<Digest> = (0..16).map(digest).collect();
    let (len, head) = delta_commitment(&sid, 12, &items);

    let mut running = delta_seed(&sid, 12);
    for (index, item) in items.iter().enumerate() {
        running = delta_step(&sid, 12, index as u64, &running, item);
    }
    assert_eq!(len, items.len() as u64);
    assert_eq!(running, head);

    // A chunk applied at the wrong offset must not validate: the index is bound into
    // every step, so a receiver cannot splice a genuine chunk in at the wrong place.
    let spliced = delta_step(&sid, 12, 0, &delta_seed(&sid, 12), &items[1]);
    let honest = delta_step(&sid, 12, 0, &delta_seed(&sid, 12), &items[0]);
    assert_ne!(spliced, honest);
    assert_ne!(
        delta_step(&sid, 12, 1, &delta_seed(&sid, 12), &items[0]),
        honest,
        "the index must be bound into each step"
    );
}

#[test]
fn store_chains_records_and_tracks_the_head() {
    let sid = test_sid();
    let mut store = SequenceStore::new(sid.clone(), 100);
    assert_eq!(store.head(), &genesis_head(&sid));
    assert_eq!(store.head_view(), 0);
    assert!(store.is_empty());

    let first = store
        .record(1, &SequenceOutcome::Skip, &[])
        .expect("view 1 records")
        .clone();
    assert_eq!(store.head_view(), 1);
    assert_eq!(store.len(), 1);

    let second = store
        .record(2, &SequenceOutcome::Core { c: manifest(1) }, &[digest(7)])
        .expect("view 2 records")
        .clone();
    assert_ne!(first, second);
    assert_eq!(store.head_view(), 2);

    // Each record must name its predecessor, so a chain cannot be re-cut at a different
    // starting point without changing every head above it.
    let record2 = store.record_for(2).expect("record 2 retained");
    assert_eq!(record2.previous_head, first);
    assert_eq!(record2.delta_len, 1);
    assert_eq!(
        store.record_for(1).expect("record 1").previous_head,
        genesis_head(&sid)
    );
}

/// A gap would produce a head no other correct party derives. Refusing loudly is the
/// whole point of Phase A -- silently skipping ahead would hide the divergence this
/// phase exists to detect.
#[test]
fn store_rejects_out_of_order_views() {
    let mut store = SequenceStore::new(test_sid(), 100);
    store.record(1, &SequenceOutcome::Skip, &[]).unwrap();

    assert_eq!(
        store.record(3, &SequenceOutcome::Skip, &[]),
        Err(SequenceError::OutOfOrder {
            expected: 2,
            got: 3
        }),
        "a skipped view must be refused"
    );
    assert_eq!(
        store.record(1, &SequenceOutcome::Skip, &[]),
        Err(SequenceError::OutOfOrder {
            expected: 2,
            got: 1
        }),
        "a replayed view must be refused"
    );
    assert_eq!(store.head_view(), 1, "a refused record must not advance");
    assert_eq!(store.len(), 1, "a refused record must not be retained");
}

#[test]
fn store_records_boundaries_at_the_interval() {
    let mut store = SequenceStore::new(test_sid(), 4);
    for view in 1..=8 {
        store.record(view, &SequenceOutcome::Skip, &[]).unwrap();
    }
    assert!(store.boundary(1).is_none());
    assert!(store.boundary(4).is_some(), "view 4 is a boundary at K=4");
    assert!(store.boundary(8).is_some());
    let (view, head) = store.latest_boundary().expect("a boundary exists");
    assert_eq!(view, 8);
    assert_eq!(
        head,
        store.head(),
        "the latest boundary is the current head"
    );
}

/// A zero interval is a misconfiguration, not a reason to divide by zero.
#[test]
fn zero_interval_makes_every_view_a_boundary() {
    let mut store = SequenceStore::new(test_sid(), 0);
    store.record(1, &SequenceOutcome::Skip, &[]).unwrap();
    assert!(store.boundary(1).is_some());
}

// ------------------------------------------------------------ f+1 checkpoint collector

fn announcement(view: u64, head: Digest, sender: crypto::PublicKey) -> SequenceAnnouncement {
    SequenceAnnouncement {
        version: SEQUENCE_VERSION,
        view,
        head,
        serve_floor: 1,
        sender,
    }
}

fn counted(outcome: AnnouncementOutcome) -> bool {
    matches!(outcome, AnnouncementOutcome::Counted { .. })
}

fn certified(outcome: AnnouncementOutcome) -> bool {
    matches!(
        outcome,
        AnnouncementOutcome::Counted {
            newly_certified: true
        }
    )
}

/// The core of the safety argument: `f+1` distinct first-hand senders certify, `f` do
/// not. For n = 3f+1 any f+1 parties contain a correct one; at f the set can be entirely
/// Byzantine and the head may name output no correct party ever produced.
#[test]
fn exactly_f_plus_one_matching_announcements_certify() {
    let keys = authors();
    let head = digest(1);
    let mut collector = CheckpointCollector::new(3, 16, 1000); // f+1 = 3

    for (index, (sender, _)) in keys.iter().take(2).enumerate() {
        let outcome =
            collector.on_announcement(&announcement(100, head.clone(), *sender), sender, true, 100);
        assert!(counted(outcome), "announcement {index} must count");
        assert!(!certified(outcome), "f announcements must NOT certify");
    }
    assert_eq!(collector.support(100, &head), 2);
    assert_eq!(collector.certified_head(0), None, "f must not certify");

    let (third, _) = keys[2];
    assert!(
        certified(collector.on_announcement(
            &announcement(100, head.clone(), third),
            &third,
            true,
            100
        )),
        "the f+1'th distinct sender must certify"
    );
    assert_eq!(collector.certified_head(0), Some((100, head.clone())));
    assert_eq!(collector.support(100, &head), 3);

    // Certification fires exactly once for a (view, head).
    let (fourth, _) = keys[3];
    let again = collector.on_announcement(&announcement(100, head, fourth), &fourth, true, 100);
    assert!(counted(again));
    assert!(!certified(again), "certification must be reported once");
}

/// The payload's `sender` field is decoration; the connection is authoritative. If a
/// forged `sender` were honoured, one Byzantine peer could mint all f+1 announcements by
/// itself and the whole rule collapses.
#[test]
fn a_forged_sender_field_never_counts() {
    let keys = authors();
    let (real, _) = keys[0];
    let (claimed, _) = keys[1];
    let mut collector = CheckpointCollector::new(1, 16, 1000);

    let forged = announcement(100, digest(1), claimed);
    assert_eq!(
        collector.on_announcement(&forged, &real, true, 100),
        AnnouncementOutcome::Ignored(IgnoreReason::SenderMismatch)
    );
    assert_eq!(collector.certified_head(0), None);

    let outsider = announcement(100, digest(1), real);
    assert_eq!(
        collector.on_announcement(&outsider, &real, false, 100),
        AnnouncementOutcome::Ignored(IgnoreReason::NotAMember),
        "a non-member cannot be one of the f+1"
    );
}

/// One sender must never supply two of the `f+1`. Repeating the SAME claim is harmless
/// and counts once; announcing a DIFFERENT head for the same view discards both, since we
/// cannot tell which (if either) is honest.
#[test]
fn duplicates_count_once_and_equivocation_counts_never() {
    let keys = authors();
    let (a, _) = keys[0];
    let (b, _) = keys[1];
    let mut collector = CheckpointCollector::new(2, 16, 1000);

    assert!(counted(collector.on_announcement(
        &announcement(100, digest(1), a),
        &a,
        true,
        100
    )));
    assert_eq!(
        collector.on_announcement(&announcement(100, digest(1), a), &a, true, 100),
        AnnouncementOutcome::Ignored(IgnoreReason::Duplicate)
    );
    assert_eq!(
        collector.support(100, &digest(1)),
        1,
        "a repeat must not add support"
    );
    assert_eq!(
        collector.certified_head(0),
        None,
        "a duplicate must not certify"
    );

    // Same sender, different head for the same view.
    assert_eq!(
        collector.on_announcement(&announcement(100, digest(2), a), &a, true, 100),
        AnnouncementOutcome::Ignored(IgnoreReason::Equivocation)
    );
    assert_eq!(collector.equivocator_count(), 1);
    assert_eq!(
        collector.support(100, &digest(1)),
        0,
        "an equivocator's earlier vote must be retracted, not left standing"
    );
    // Anything further from that sender is dead, including a later honest-looking claim.
    assert_eq!(
        collector.on_announcement(&announcement(101, digest(1), a), &a, true, 101),
        AnnouncementOutcome::Ignored(IgnoreReason::Equivocation)
    );
    // An honest sender is unaffected.
    assert!(counted(collector.on_announcement(
        &announcement(100, digest(1), b),
        &b,
        true,
        100
    )));
    assert_eq!(collector.support(100, &digest(1)), 1);
}

/// An equivocation discovered AFTER a head reached the threshold must take certification
/// back: the head would otherwise rest on f+1 senders one of which supplied a second,
/// contradictory claim, so the "at least one correct" guarantee no longer holds.
#[test]
fn equivocation_after_certification_retracts_it() {
    let keys = authors();
    let (a, _) = keys[0];
    let (b, _) = keys[1];
    let mut collector = CheckpointCollector::new(2, 16, 1000);

    collector.on_announcement(&announcement(100, digest(1), a), &a, true, 100);
    assert!(certified(collector.on_announcement(
        &announcement(100, digest(1), b),
        &b,
        true,
        100
    )));
    assert_eq!(collector.certified_head(0), Some((100, digest(1))));

    collector.on_announcement(&announcement(100, digest(9), b), &b, true, 100);
    assert_eq!(
        collector.certified_head(0),
        None,
        "certification must not survive its supporter equivocating"
    );
}

/// Competing heads for one view must be counted separately -- f Byzantine parties
/// announcing a fabricated head must never reach the threshold on it while the correct
/// nodes announce another.
#[test]
fn a_minority_head_never_certifies_alongside_the_real_one() {
    // The test committee is n=4, so f=1 and the threshold is f+1=2.
    let keys = authors();
    let mut collector = CheckpointCollector::new(2, 16, 1000);
    let (real, fake) = (digest(1), digest(2));

    // The f = 1 Byzantine party pushes a fabricated head.
    let (liar, _) = keys[0];
    collector.on_announcement(&announcement(100, fake.clone(), liar), &liar, true, 100);
    // The remaining correct parties announce the real one.
    for (sender, _) in keys.iter().skip(1) {
        collector.on_announcement(&announcement(100, real.clone(), *sender), sender, true, 100);
    }
    assert_eq!(collector.support(100, &fake), 1, "the fake stays below f+1");
    assert_eq!(collector.support(100, &real), 3);
    assert_eq!(collector.certified_head(0), Some((100, real)));
}

/// Byzantine peers must not be able to grow memory without bound by announcing arbitrary
/// future boundaries.
#[test]
fn future_views_and_candidate_count_are_bounded() {
    let keys = authors();
    let (a, _) = keys[0];
    let mut collector = CheckpointCollector::new(2, 4, 500);

    assert_eq!(
        collector.on_announcement(&announcement(10_000, digest(1), a), &a, true, 100),
        AnnouncementOutcome::Ignored(IgnoreReason::TooFarAhead)
    );

    for view in 1..=20u64 {
        collector.on_announcement(&announcement(view, digest(view as u8), a), &a, true, 100);
    }
    assert!(
        collector.candidate_view_count() <= 4,
        "retained candidate boundaries must respect the cap, got {}",
        collector.candidate_view_count()
    );
}

/// The requester needs the matching announcer set as its source list, and picks the
/// highest certified target above what it already holds.
#[test]
fn announcers_and_highest_target_above_local() {
    let keys = authors();
    let mut collector = CheckpointCollector::new(2, 16, 1000);
    for view in [100u64, 200] {
        for (sender, _) in keys.iter().take(2) {
            collector.on_announcement(
                &announcement(view, digest(view as u8), *sender),
                sender,
                true,
                250,
            );
        }
    }
    assert_eq!(
        collector.certified_head(0),
        Some((200, digest(200u64 as u8)))
    );
    assert_eq!(
        collector.certified_head(200),
        None,
        "a target must be strictly above the local head"
    );
    let mut sources = collector.announcers(100, &digest(100u64 as u8));
    sources.sort();
    let mut expected: Vec<_> = keys.iter().take(2).map(|(k, _)| *k).collect();
    expected.sort();
    assert_eq!(sources, expected);
}

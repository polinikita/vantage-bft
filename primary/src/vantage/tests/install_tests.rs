// SEQUENCE-CHECKPOINT-SYNC-PLAN.md §10: staging a verified target for installation.
//
// Everything here is about PACING and ORDER. Correctness of the target itself is
// `sequence_tests`' job -- by the time a `SequenceInstall` exists the chain has already
// verified against `f+1` announcements. What remains to get wrong is admitting more work
// than repair can absorb (the failure that turned 60,262 blocks into 612M settle calls),
// and applying views in an order the cursor cannot follow.

use super::common::{authors, tagged_header, test_sid};
use crate::messages::Header;
use crate::primary::{Height, View};
use crate::vantage::agb::Manifest;
use crate::vantage::install::SequenceInstall;
use crate::vantage::lanes::{BlockCache, SharedBlocks};
use crate::vantage::sequence::SequenceOutcome;
use crypto::{Digest, PublicKey};
use parking_lot::Mutex;
use std::sync::Arc;

/// A verified transfer's per-view output, exactly as `VantageCore` copies it out.
type Staged = Vec<(View, SequenceOutcome, Vec<Digest>)>;

fn empty_cache() -> SharedBlocks {
    Arc::new(Mutex::new(BlockCache::new()))
}

fn cache_insert(blocks: &SharedBlocks, headers: &[Header]) {
    let mut cache = blocks.lock();
    for h in headers {
        cache.upsert(h.clone(), false, true, true, true);
    }
}

/// One lane, one new block per view: view `v` is `Core` over author A's block at height
/// `v`, and that block is the view's whole delta. The smallest shape that still exercises
/// "fetch instruction is the manifest, completion test is the delta".
fn linear_target(views: View) -> (Staged, Vec<Header>) {
    let (author, _) = authors()[0];
    let sid = test_sid();
    let mut staged = Vec::new();
    let mut headers = Vec::new();
    let mut prev = Digest::default();
    for view in 1..=views {
        let header = tagged_header(
            author,
            view as Height,
            prev.clone(),
            sid.clone(),
            view as u8,
        );
        prev = header.id.clone();
        let manifest: Manifest = vec![(author, view as Height, header.id.clone())];
        staged.push((
            view,
            SequenceOutcome::Core { c: manifest },
            vec![header.id.clone()],
        ));
        headers.push(header);
    }
    (staged, headers)
}

fn install_of(views: View, window: usize, ceiling: usize) -> (SequenceInstall, Vec<Header>) {
    let (staged, headers) = linear_target(views);
    let install = SequenceInstall::new(0, views, Digest::default(), staged, window, ceiling);
    (install, headers)
}

/// The window is the whole point: a target spanning hundreds of views must not authorize
/// hundreds of views' worth of refs at once, and a view only frees its slot once its
/// blocks are actually in hand.
#[test]
fn the_fetch_window_bounds_views_in_flight() {
    let (mut install, headers) = install_of(10, 3, 4096);
    let blocks = empty_cache();

    let first = install.admit(0);
    assert_eq!(first.len(), 3, "one ref per admitted view, window = 3");
    assert_eq!(install.views_in_flight(), 3);

    assert!(
        install.admit(0).is_empty(),
        "the window is full and nothing has arrived, so nothing more may be admitted"
    );

    // View 1's block arrives; its slot frees and exactly one more view is admitted.
    cache_insert(&blocks, &headers[0..1]);
    install.refresh(&blocks);
    assert_eq!(install.views_complete(), 1);
    assert_eq!(install.views_in_flight(), 2);
    assert_eq!(install.admit(0).len(), 1, "one freed slot, one new view");
    assert_eq!(install.views_in_flight(), 3);
}

/// The gate that matters more than the window. This mechanism runs on nodes that are
/// already behind, which is exactly when repair's backlog is already deep, so admitting
/// work regardless of that backlog would add load precisely where it hurts.
#[test]
fn a_congested_repairer_admits_nothing() {
    let (mut install, _) = install_of(10, 8, 100);

    assert!(
        install.admit(100).is_empty(),
        "at the ceiling, no view is admitted"
    );
    assert!(install.admit(4096).is_empty(), "far above it, still none");
    assert_eq!(install.views_in_flight(), 0);

    // Backlog drains: admission resumes from where it left off, having skipped nothing.
    assert_eq!(install.admit(99).len(), 8);
    assert_eq!(install.views_in_flight(), 8);
}

/// A `Skip` names no manifest and outputs nothing, so it is finished on arrival. Letting
/// one occupy a window slot would stall the fetch on a block that is never coming.
#[test]
fn skip_views_complete_without_a_fetch() {
    let staged = vec![
        (1, SequenceOutcome::Skip, Vec::new()),
        (2, SequenceOutcome::Skip, Vec::new()),
        (3, SequenceOutcome::Skip, Vec::new()),
    ];
    let mut install = SequenceInstall::new(0, 3, Digest::default(), staged, 2, 4096);

    assert!(
        install.admit(0).is_empty(),
        "nothing to fetch for a run of skips"
    );
    assert_eq!(install.views_in_flight(), 0, "no window slot is consumed");
    assert_eq!(install.views_complete(), 3);
    assert_eq!(install.installable(), Some(1));
}

/// The cursor advances one view at a time, so a later view being ready buys nothing while
/// an earlier one is missing. Installing it anyway would leave a hole no correct party's
/// head can ever match.
#[test]
fn views_install_in_order_even_when_a_later_one_is_ready() {
    let (mut install, headers) = install_of(4, 4, 4096);
    let blocks = empty_cache();
    install.admit(0);

    // Views 2..4 arrive; view 1 does not.
    cache_insert(&blocks, &headers[1..]);
    install.refresh(&blocks);
    assert_eq!(install.views_complete(), 3);
    assert_eq!(
        install.installable(),
        None,
        "view 1 still blocks everything"
    );

    cache_insert(&blocks, &headers[0..1]);
    install.refresh(&blocks);
    assert_eq!(install.installable(), Some(1));

    for view in 1..=4 {
        assert_eq!(install.installable(), Some(view));
        assert!(install.view_output(view).is_some());
        install.mark_installed(view);
    }
    assert!(install.is_done());
    assert_eq!(install.installable(), None);
}

/// Repair's holder index keeps a maximum per lane, so seeding it needs one entry per
/// author, not one per manifest entry -- `views * n` updates collapsed to `n`.
#[test]
fn lane_tips_are_the_per_lane_maximum() {
    let keys = authors();
    let (a, _) = keys[0];
    let (b, _) = keys[1];
    let d = |x: u8| Digest([x; 32]);

    let staged = vec![
        (
            1,
            SequenceOutcome::Core {
                c: vec![(a, 5, d(1)), (b, 2, d(2))],
            },
            vec![d(1), d(2)],
        ),
        (
            2,
            SequenceOutcome::Core {
                c: vec![(a, 3, d(3))],
            },
            vec![d(3)],
        ),
        (
            3,
            SequenceOutcome::Core {
                c: vec![(b, 9, d(4))],
            },
            vec![d(4)],
        ),
    ];
    let install = SequenceInstall::new(0, 3, Digest::default(), staged, 8, 4096);

    let tips: Vec<(PublicKey, Height)> = install.lane_tips();
    assert_eq!(tips.len(), 2, "one entry per lane, not per manifest entry");
    let height_of = |k: PublicKey| tips.iter().find(|(p, _)| *p == k).map(|(_, h)| *h);
    assert_eq!(height_of(a), Some(5), "view 2's lower height must not win");
    assert_eq!(height_of(b), Some(9));
}

/// A verified chain cannot have a hole, so a gap means the outcome/delta maps disagree
/// with the chain that verified. Refusing is the only safe answer: skipping the gap
/// installs a sequence no correct party derives.
#[test]
fn a_hole_in_the_verified_output_is_refused() {
    let (mut staged, _) = linear_target(5);
    staged.retain(|(view, _, _)| *view != 3);
    let install = SequenceInstall::new(0, 5, Digest::default(), staged, 8, 4096);

    assert!(!install.is_contiguous());
    assert_eq!(install.views_total(), 4);

    let (whole, _) = linear_target(5);
    assert!(SequenceInstall::new(0, 5, Digest::default(), whole, 8, 4096).is_contiguous());
}

/// `Full` names both the core `c` and the terminal `t`, and the delta is the expansion of
/// both. Authorizing only `c` would leave every block reachable solely through `t`
/// unrequested, and the view would never complete.
#[test]
fn full_outcomes_authorize_both_manifests() {
    let keys = authors();
    let (a, _) = keys[0];
    let (b, _) = keys[1];
    let d = |x: u8| Digest([x; 32]);

    let staged = vec![(
        1,
        SequenceOutcome::Full {
            c: vec![(a, 1, d(1))],
            t: vec![(b, 1, d(2))],
        },
        vec![d(1), d(2)],
    )];
    let mut install = SequenceInstall::new(0, 1, Digest::default(), staged, 8, 4096);

    let refs = install.admit(0);
    assert_eq!(refs.len(), 2, "both manifests are fetch instructions");
    assert!(refs.iter().any(|(p, _, _)| *p == a));
    assert!(refs.iter().any(|(p, _, _)| *p == b));
}

/// Blocks that ordinary dissemination already delivered are not re-fetched: the
/// completion test is cache presence, not "did repair bring it".
#[test]
fn views_already_held_are_never_fetched() {
    let (mut install, headers) = install_of(4, 8, 4096);
    let blocks = empty_cache();
    cache_insert(&blocks, &headers);

    // The first admission pass authorizes them, since nothing has been refreshed yet...
    install.admit(0);
    install.refresh(&blocks);
    assert_eq!(install.views_complete(), 4);
    assert_eq!(install.views_in_flight(), 0);
    assert_eq!(install.blocks_awaited(&blocks), 0);
    assert_eq!(install.installable(), Some(1));
}

use super::common::{authors, tagged_header, test_sid};
use crate::messages::Header;
use crate::primary::{Height, View};
use crate::vantage::agb::Manifest;
use crate::vantage::install::{RebaseOutcome, SequenceInstall};
use crate::vantage::lanes::{BlockCache, SharedBlocks};
use crate::vantage::sequence::SequenceOutcome;
use crypto::{Digest, PublicKey};
use parking_lot::Mutex;
use std::sync::Arc;

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

fn cache_insert_headers_only(blocks: &SharedBlocks, headers: &[Header]) {
    let mut cache = blocks.lock();
    for h in headers {
        cache.upsert(h.clone(), false, true, false, true);
    }
}

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

fn heads_for(views: View) -> Vec<(View, Digest)> {
    (1..=views).map(|v| (v, Digest([v as u8; 32]))).collect()
}

fn install_of(views: View, window: usize, ceiling: usize) -> (SequenceInstall, Vec<Header>) {
    let (staged, headers) = linear_target(views);
    let install = SequenceInstall::new(
        0,
        views,
        Digest::default(),
        staged,
        heads_for(views),
        window,
        ceiling,
    );
    (install, headers)
}

fn head_at(view: View) -> Digest {
    Digest([view as u8; 32])
}

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

    cache_insert(&blocks, &headers[0..1]);
    install.refresh(&blocks);
    assert_eq!(install.views_complete(), 1);
    assert_eq!(install.views_in_flight(), 2);
    assert_eq!(install.admit(0).len(), 1, "one freed slot, one new view");
    assert_eq!(install.views_in_flight(), 3);
}

#[test]
fn repair_backlog_does_not_veto_admission() {
    let (mut install, _) = install_of(10, 8, 100);

    assert_eq!(
        install.admit(4096).len(),
        8,
        "only the install window gates admission"
    );
    assert_eq!(install.views_in_flight(), 8);
}

#[test]
fn skip_views_complete_without_a_fetch() {
    let staged = vec![
        (1, SequenceOutcome::Skip, Vec::new()),
        (2, SequenceOutcome::Skip, Vec::new()),
        (3, SequenceOutcome::Skip, Vec::new()),
    ];
    let mut install = SequenceInstall::new(0, 3, Digest::default(), staged, heads_for(3), 2, 4096);

    assert!(
        install.admit(0).is_empty(),
        "nothing to fetch for a run of skips"
    );
    assert_eq!(install.views_in_flight(), 0, "no window slot is consumed");
    assert_eq!(install.views_complete(), 3);
    assert_eq!(install.installable(), Some(1));
}

#[test]
fn views_install_in_order_even_when_a_later_one_is_ready() {
    let (mut install, headers) = install_of(4, 4, 4096);
    let blocks = empty_cache();
    install.admit(0);

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
    let install = SequenceInstall::new(0, 3, Digest::default(), staged, heads_for(3), 8, 4096);

    let tips: Vec<(PublicKey, Height)> = install.lane_tips();
    assert_eq!(tips.len(), 2, "one entry per lane, not per manifest entry");
    let height_of = |k: PublicKey| tips.iter().find(|(p, _)| *p == k).map(|(_, h)| *h);
    assert_eq!(height_of(a), Some(5), "view 2's lower height must not win");
    assert_eq!(height_of(b), Some(9));
    assert_eq!(install.lane_tip(&a), Some((a, 5, d(1))));
    assert_eq!(install.lane_tip(&b), Some((b, 9, d(4))));
}

#[test]
fn a_hole_in_the_verified_output_is_refused() {
    let (mut staged, _) = linear_target(5);
    staged.retain(|(view, _, _)| *view != 3);
    let install = SequenceInstall::new(0, 5, Digest::default(), staged, heads_for(5), 8, 4096);

    assert!(!install.is_contiguous());
    assert_eq!(install.views_total(), 4);

    let (whole, _) = linear_target(5);
    assert!(
        SequenceInstall::new(0, 5, Digest::default(), whole, heads_for(5), 8, 4096).is_contiguous()
    );
}

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
    let mut install = SequenceInstall::new(0, 1, Digest::default(), staged, heads_for(1), 8, 4096);

    let refs = install.admit(0);
    assert_eq!(refs.len(), 2, "both manifests are fetch instructions");
    assert!(refs.iter().any(|(p, _, _)| *p == a));
    assert!(refs.iter().any(|(p, _, _)| *p == b));
}

#[test]
fn views_already_held_are_never_fetched() {
    let (mut install, headers) = install_of(4, 8, 4096);
    let blocks = empty_cache();
    cache_insert(&blocks, &headers);

    install.admit(0);
    install.refresh(&blocks);
    assert_eq!(install.views_complete(), 4);
    assert_eq!(install.views_in_flight(), 0);
    assert_eq!(install.blocks_awaited(&blocks), 0);
    assert_eq!(install.installable(), Some(1));
}

#[test]
fn missing_header_scan_is_bounded_by_examined_positions() {
    let (mut install, headers) = install_of(5, 8, 4096);
    let blocks = empty_cache();
    install.admit(0);
    cache_insert(&blocks, &[headers[0].clone(), headers[2].clone()]);

    let missing = install.missing_digests(&blocks, 3);
    assert_eq!(missing, vec![headers[1].id.clone()]);
}

#[test]
fn a_cursor_that_advanced_during_the_fetch_rebases_the_target() {
    let (mut install, headers) = install_of(10, 8, 4096);
    let blocks = empty_cache();

    assert_eq!(
        install.rebase(4, &head_at(4)),
        RebaseOutcome::Continue,
        "views 5..10 are still worth installing"
    );
    assert_eq!(install.views_total(), 6);
    assert_eq!(
        install.installable(),
        None,
        "view 5 has no blocks yet, and view 4 is gone rather than stale"
    );

    cache_insert(&blocks, &headers);
    install.admit(0);
    install.refresh(&blocks);
    assert_eq!(
        install.installable(),
        Some(5),
        "installation resumes at the first view the cursor did NOT reach"
    );
}

#[test]
fn a_target_overtaken_outright_is_retired() {
    let (mut install, _) = install_of(6, 8, 4096);

    assert_eq!(
        install.rebase(6, &head_at(6)),
        RebaseOutcome::Overtaken,
        "the cursor reached the target itself"
    );
    assert_eq!(install.views_total(), 0);
    assert!(install.is_done());

    let (mut ahead, _) = install_of(6, 8, 4096);
    assert_eq!(
        ahead.rebase(99, &Digest([0xEE; 32])),
        RebaseOutcome::Overtaken,
        "past the target, where the chain has no head to check against"
    );
}

#[test]
fn rebase_moves_the_admission_point_too() {
    let (mut install, _) = install_of(10, 3, 4096);
    install.admit(0); // Admit views 1 through 3.
    assert_eq!(install.rebase(6, &head_at(6)), RebaseOutcome::Continue);

    let refs = install.admit(0);
    assert_eq!(refs.len(), 3, "admission resumes at view 7, not view 4");
    assert_eq!(install.views_in_flight(), 3);
    assert_eq!(install.views_total(), 4);
}

#[test]
fn headers_without_payloads_are_ready_to_install() {
    let (mut install, headers) = install_of(3, 8, 4096);
    let blocks = empty_cache();
    install.admit(0);

    cache_insert_headers_only(&blocks, &headers);
    install.refresh(&blocks);
    assert_eq!(
        install.views_complete(),
        3,
        "chain-verified headers are sequence-ready even before worker payloads land"
    );
    assert_eq!(install.blocks_awaited(&blocks), 0);
    assert_eq!(install.installable(), Some(1));
}

#[test]
fn rebase_refuses_a_boundary_whose_local_head_disagrees() {
    let (mut install, _) = install_of(10, 8, 4096);

    let outcome = install.rebase(4, &Digest([0xFF; 32]));
    assert_eq!(
        outcome,
        RebaseOutcome::Diverged {
            view: 4,
            expected: head_at(4),
            local: Digest([0xFF; 32]),
        }
    );
    assert_eq!(
        install.views_total(),
        10,
        "nothing is dropped on a refused rebase"
    );
}

#[test]
fn payload_retry_names_stuck_blocks_and_skips_the_ready_prefix() {
    let (mut install, headers) = install_of(2, 8, 4096);
    let blocks = empty_cache();
    install.admit(0);

    cache_insert(&blocks, &headers[0..1]);
    cache_insert_headers_only(&blocks, &headers[1..2]);
    let retry = install.payload_retry_headers(&blocks, 64);
    assert_eq!(retry.len(), 1);
    assert_eq!(retry[0].id, headers[1].id);

    install.refresh(&blocks);
    assert_eq!(install.views_complete(), 2, "both views are sequence-ready");

    assert!(
        install.payload_retry_headers(&blocks, 64).is_empty(),
        "completed views are not rescanned for payload retries"
    );
}

// PHASE4-SPEC.md §12 "Cursor" -- the output cursor (§9), driven directly against
// `Cursor`.

use super::common::*;
use crate::vantage::agb::Outcome;
use crate::vantage::Cursor;

fn new_cursor(lm: &crate::vantage::LaneManager) -> Cursor {
    Cursor::new(
        test_committee(),
        lm.sid().clone(),
        lm.genesis().clone(),
        MAX_BLOCK_PAYLOAD,
        lm.blocks_handle(),
    )
}

#[tokio::test]
async fn expansion_order_and_cross_view_dedup() {
    let (name, _) = authors()[3];
    let (author_a, _) = authors()[0];
    let (author_b, _) = authors()[1];
    let (mut lm, _store) = new_lane_manager(name, ".db_test_cursor_expansion");
    let chain_a = direct_chain(&mut lm, author_a, 2).await;
    let chain_b = direct_chain(&mut lm, author_b, 1).await;
    let mut cursor = new_cursor(&lm);

    // View 1: C = author_a height 1, T = author_a height 2 + author_b height 1 (T's
    // entries traversed in sorted-author order -- whichever of the two authors sorts
    // first, not a fixed literal order).
    let c = vec![block_ref(&chain_a[0])];
    let t = sorted_manifest(vec![block_ref(&chain_a[1]), block_ref(&chain_b[0])]);
    let expected: Vec<_> = std::iter::once(chain_a[0].id.clone())
        .chain(t.iter().map(|r| r.2.clone()))
        .collect();
    cursor.on_completed(1, c.clone(), t.clone());
    let effects = cursor.on_sealed(1, Outcome::Full(c, t));
    assert!(effects
        .iter()
        .any(|e| matches!(e, crate::vantage::Effect::NotifyCommitted(..))));
    assert_eq!(cursor.output_log(), expected.as_slice());
    assert_eq!(cursor.next_view(), 2);

    // View 2 references author_a's height-2 block again (e.g. as its own C) -- already
    // output in view 1, must never be re-output.
    let c2 = vec![block_ref(&chain_a[1])];
    cursor.on_sealed(2, Outcome::Core(c2));
    assert_eq!(cursor.output_log(), expected.as_slice());
    assert_eq!(cursor.next_view(), 3);
}

#[tokio::test]
async fn core_prefix_of_full_property() {
    let (name, _) = authors()[3];
    let (author_a, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(name, ".db_test_cursor_core_prefix");
    let chain_a = direct_chain(&mut lm, author_a, 2).await;
    let mut cursor = new_cursor(&lm);

    let c = vec![block_ref(&chain_a[0])];
    let t = vec![block_ref(&chain_a[1])];

    // Completed-but-open: emits exactly K, does not advance.
    cursor.on_completed(1, c.clone(), t.clone());
    let k_len = cursor.output_log().len();
    assert_eq!(cursor.output_log(), &[chain_a[0].id.clone()]);
    assert_eq!(cursor.next_view(), 1, "tip stays open until sealed");

    // Sealing with gfull appends only T-hat -- K is a literal prefix, never re-emitted.
    cursor.on_sealed(1, Outcome::Full(c, t));
    assert_eq!(&cursor.output_log()[..k_len], &[chain_a[0].id.clone()]);
    assert_eq!(
        cursor.output_log(),
        &[chain_a[0].id.clone(), chain_a[1].id.clone()]
    );
    assert_eq!(cursor.next_view(), 2);
}

#[tokio::test]
async fn open_tip_blocks_later_views_payload() {
    let (name, _) = authors()[3];
    let (author_a, _) = authors()[0];
    let (author_b, _) = authors()[1];
    let (mut lm, _store) = new_lane_manager(name, ".db_test_cursor_open_tip_blocks");
    let chain_a = direct_chain(&mut lm, author_a, 1).await;
    let chain_b = direct_chain(&mut lm, author_b, 1).await;
    let mut cursor = new_cursor(&lm);

    // View 2 seals *before* view 1 does -- no payload from it may cross view 1's still-
    // open cursor position.
    let effects = cursor.on_sealed(2, Outcome::Core(vec![block_ref(&chain_b[0])]));
    assert!(effects.is_empty());
    assert!(cursor.output_log().is_empty());
    assert_eq!(cursor.next_view(), 1);

    // Once view 1 seals, both views' outputs appear, in view order.
    let effects = cursor.on_sealed(1, Outcome::Core(vec![block_ref(&chain_a[0])]));
    assert!(!effects.is_empty());
    assert_eq!(
        cursor.output_log(),
        &[chain_a[0].id.clone(), chain_b[0].id.clone()]
    );
    assert_eq!(cursor.next_view(), 3);
}

#[tokio::test]
async fn gcore_skips_t_hat() {
    let (name, _) = authors()[3];
    let (author_a, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(name, ".db_test_cursor_gcore");
    let chain_a = direct_chain(&mut lm, author_a, 1).await;
    let mut cursor = new_cursor(&lm);
    cursor.on_sealed(1, Outcome::Core(vec![block_ref(&chain_a[0])]));
    assert_eq!(cursor.output_log(), &[chain_a[0].id.clone()]);
    assert_eq!(cursor.next_view(), 2);
}

#[tokio::test]
async fn idempotent_duplicate_seal() {
    let (name, _) = authors()[3];
    let (author_a, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(name, ".db_test_cursor_idempotent");
    let chain_a = direct_chain(&mut lm, author_a, 1).await;
    let mut cursor = new_cursor(&lm);
    let outcome = Outcome::Core(vec![block_ref(&chain_a[0])]);
    cursor.on_sealed(1, outcome.clone());
    let after_first = cursor.output_log().to_vec();
    let next_view_after_first = cursor.next_view();
    // A duplicate/late-compatible seal for the same (now-advanced-past) view must
    // never reopen it or duplicate output.
    let effects = cursor.on_sealed(1, outcome);
    assert!(effects.is_empty());
    assert_eq!(cursor.output_log().to_vec(), after_first);
    assert_eq!(cursor.next_view(), next_view_after_first);
}

#[tokio::test]
async fn missing_prefix_wait_then_emit() {
    let (name, _) = authors()[3];
    let (author_a, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(name, ".db_test_cursor_missing_prefix");
    let sid = lm.sid().clone();
    let genesis = lm.genesis().clone();
    let h1 = crate::messages::Header::new_vantage(
        author_a,
        1,
        std::collections::BTreeMap::new(),
        genesis,
        sid.clone(),
    );
    // h1 is *not* published yet -- h2 (the manifest's actual entry) references it but
    // the prefix isn't obtainable.
    let h2 = crate::messages::Header::new_vantage(
        author_a,
        2,
        std::collections::BTreeMap::new(),
        h1.id.clone(),
        sid,
    );
    let mut cursor = new_cursor(&lm);
    let c = vec![(author_a, 2, h2.id.clone())];
    let effects = cursor.on_sealed(1, Outcome::Core(c));
    assert!(
        effects.is_empty(),
        "must wait for the missing prefix, never emit garbage"
    );
    assert_eq!(cursor.next_view(), 1);

    // The prefix arrives (h1 then h2) -- a `BlockCached` wakeup re-attempts via retry().
    lm.process_publish(author_a, h1.clone()).await;
    lm.process_publish(author_a, h2.clone()).await;
    let effects = cursor.retry();
    assert!(!effects.is_empty());
    assert_eq!(cursor.output_log(), &[h1.id.clone(), h2.id.clone()]);
    assert_eq!(cursor.next_view(), 2);
}

/// SEQUENCE-CHECKPOINT-SYNC-PLAN.md §13: "early core emission plus later full seal
/// produces one final ordered delta".
///
/// The case that makes the per-view delta a `Cursor` FIELD rather than a local. A view
/// that is completed-but-still-open emits its core prefix `K` immediately and then waits
/// for the seal, so the view's output arrives across two separate `pump()` calls, split
/// by an arbitrary amount of wall time. Both halves belong to the same view's delta, in
/// emission order, and exactly one `SequenceFinalized` must be produced -- at the
/// terminal advance, never at the earlier core emission.
///
/// Getting this wrong is invisible in ordinary output (the same blocks are emitted either
/// way) but silently corrupts the sequence head: dropping the early core would commit a
/// delta the fleet does not share, and emitting twice would record view `v` twice and
/// desynchronize every head above it.
#[tokio::test]
async fn early_core_then_terminal_seal_yields_one_ordered_delta() {
    use crate::vantage::sequence::SequenceOutcome;
    use crate::vantage::Effect;

    let (name, _) = authors()[3];
    let (author_a, _) = authors()[0];
    let (author_b, _) = authors()[1];
    let (mut lm, _store) = new_lane_manager(name, ".db_test_cursor_early_core_delta");
    let chain_a = direct_chain(&mut lm, author_a, 1).await;
    let chain_b = direct_chain(&mut lm, author_b, 1).await;
    let mut cursor = new_cursor(&lm);

    let c = vec![block_ref(&chain_a[0])];
    let t = sorted_manifest(vec![block_ref(&chain_a[0]), block_ref(&chain_b[0])]);

    // Step 1: completed but NOT sealed -- the core prefix is emitted while the view is
    // still open, and the view must not finalize.
    let open = cursor.on_completed(1, c.clone(), t.clone());
    assert!(
        open.iter()
            .any(|e| matches!(e, Effect::NotifyCommitted(..))),
        "the core prefix must be emitted while the view is still open"
    );
    assert!(
        !open
            .iter()
            .any(|e| matches!(e, Effect::SequenceFinalized { .. })),
        "an open view must NOT finalize a sequence record"
    );
    assert_eq!(cursor.next_view(), 1, "an open view must not advance");
    let after_core = cursor.output_log().to_vec();
    assert_eq!(after_core, vec![chain_a[0].id.clone()]);

    // Step 2: the terminal seal arrives later and contributes the rest of the view.
    let sealed = cursor.on_sealed(1, Outcome::Full(c.clone(), t.clone()));
    assert_eq!(cursor.next_view(), 2, "the terminal seal must advance");

    let finals: Vec<_> = sealed
        .iter()
        .filter_map(|e| match e {
            Effect::SequenceFinalized {
                view,
                outcome,
                output_delta,
            } => Some((*view, outcome.clone(), output_delta.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        finals.len(),
        1,
        "exactly one record per terminally processed view"
    );
    let (view, outcome, delta) = &finals[0];
    assert_eq!(*view, 1);
    assert_eq!(
        *outcome,
        SequenceOutcome::Full {
            c: c.clone(),
            t: t.clone()
        }
    );

    // The delta is the WHOLE view's output in emission order -- the early core first,
    // then what the seal added -- and matches the cursor's own committed log exactly.
    assert_eq!(
        delta.as_slice(),
        cursor.output_log(),
        "the delta must be the view's full ordered output, early core included"
    );
    assert_eq!(
        delta[0], chain_a[0].id,
        "the early core block leads the delta"
    );
    assert!(
        delta.len() > after_core.len(),
        "the seal must contribute blocks beyond the early core"
    );
    assert_eq!(
        delta.iter().collect::<std::collections::HashSet<_>>().len(),
        delta.len(),
        "a block emitted while the view was open must not repeat after the seal"
    );
}

// -------------------------------------------------- SEQUENCE-CHECKPOINT-SYNC-PLAN.md §10
//
// `Cursor::install`: the one path that turns bytes another party derived into committed
// output. Every test here is about a refusal leaving the cursor EXACTLY as it was, because
// a half-applied view is a hole no later execution can repair.

use crate::vantage::cursor::InstallError;
use crate::vantage::sequence::SequenceOutcome;
use crypto::Digest;

/// The ordinary case: a view this node never executed is applied whole, and the delta it
/// finalizes is the one that was verified.
#[tokio::test]
async fn install_applies_a_view_and_advances() {
    let (name, _) = authors()[3];
    let (author_a, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(name, ".db_test_cursor_install_applies");
    let chain_a = direct_chain(&mut lm, author_a, 3).await;
    let mut cursor = new_cursor(&lm);

    let c = vec![block_ref(&chain_a[2])];
    let delta: Vec<Digest> = chain_a.iter().map(|h| h.id.clone()).collect();
    let (effects, finalized) = cursor
        .install(1, SequenceOutcome::Core { c }, &delta, usize::MAX)
        .expect("a fresh cursor at view 1 installs cleanly");
    assert!(finalized, "the whole delta fits in the budget");

    assert_eq!(cursor.next_view(), 2);
    assert_eq!(cursor.output_log(), delta.as_slice());
    let finalized = effects.iter().find_map(|e| match e {
        crate::vantage::Effect::SequenceFinalized {
            view, output_delta, ..
        } => Some((view, output_delta.clone())),
        _ => None,
    });
    assert_eq!(
        finalized,
        Some((&1, delta)),
        "the finalized delta must be exactly what was verified -- that is what makes the \
         installed head comparable to the certified one"
    );
}

/// A completed-but-open view has already emitted its core prefix `K`. Installing over it
/// must deliver only the remainder, or blocks get output twice.
#[tokio::test]
async fn install_over_an_emitted_core_prefix_emits_only_the_remainder() {
    let (name, _) = authors()[3];
    let (author_a, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(name, ".db_test_cursor_install_prefix");
    let chain_a = direct_chain(&mut lm, author_a, 3).await;
    let mut cursor = new_cursor(&lm);

    // Completed, not sealed: K is emitted and the view stays open.
    let c = vec![block_ref(&chain_a[0])];
    cursor.on_completed(1, c.clone(), Vec::new());
    assert_eq!(cursor.open_delta(), &[chain_a[0].id.clone()]);
    assert_eq!(cursor.next_view(), 1, "still open");

    let t = vec![block_ref(&chain_a[2])];
    let delta: Vec<Digest> = chain_a.iter().map(|h| h.id.clone()).collect();
    cursor
        .install(1, SequenceOutcome::Full { c, t }, &delta, usize::MAX)
        .expect("the emitted prefix matches");

    assert_eq!(cursor.next_view(), 2);
    assert_eq!(
        cursor.output_log(),
        delta.as_slice(),
        "every block output exactly once, in verified order"
    );
}

/// The refusal that can only fire on a real divergence: this node already output blocks
/// for the view in an order the verified delta contradicts.
#[tokio::test]
async fn install_refuses_a_local_partial_that_is_not_a_prefix() {
    let (name, _) = authors()[3];
    let (author_a, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(name, ".db_test_cursor_install_mismatch");
    let chain_a = direct_chain(&mut lm, author_a, 3).await;
    let mut cursor = new_cursor(&lm);

    let c = vec![block_ref(&chain_a[0])];
    cursor.on_completed(1, c.clone(), Vec::new());
    let before = cursor.output_log().to_vec();

    // Verified delta that does NOT start with the locally emitted block.
    let divergent = vec![chain_a[1].id.clone(), chain_a[2].id.clone()];
    let err = cursor
        .install(1, SequenceOutcome::Core { c }, &divergent, usize::MAX)
        .expect_err("a non-prefix local partial must be refused");

    assert!(matches!(err, InstallError::PrefixMismatch { view: 1, .. }));
    assert_eq!(cursor.next_view(), 1, "cursor unchanged");
    assert_eq!(cursor.open_delta(), &[chain_a[0].id.clone()]);
    assert_eq!(cursor.output_log(), before.as_slice());
}

/// `emit` resolves headers by cache lookup and silently omits what it cannot find, so an
/// install over a partial cache would advance the view while dropping output. Caught
/// before anything is touched.
#[tokio::test]
async fn install_refuses_a_delta_whose_blocks_are_not_held() {
    let (name, _) = authors()[3];
    let (author_a, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(name, ".db_test_cursor_install_missing");
    let chain_a = direct_chain(&mut lm, author_a, 2).await;
    let mut cursor = new_cursor(&lm);

    let absent = Digest([0x5a; 32]);
    let delta = vec![chain_a[0].id.clone(), absent.clone()];
    let err = cursor
        .install(
            1,
            SequenceOutcome::Core {
                c: vec![block_ref(&chain_a[1])],
            },
            &delta,
            usize::MAX,
        )
        .expect_err("a delta naming an unheld block must be refused");

    assert_eq!(
        err,
        InstallError::BlocksMissing {
            view: 1,
            digest: absent
        }
    );
    assert_eq!(cursor.next_view(), 1);
    assert!(
        cursor.output_log().is_empty(),
        "the block that WAS held must not have been emitted either -- install is atomic"
    );
}

#[tokio::test]
async fn install_refuses_a_view_the_cursor_is_not_waiting_on() {
    let (name, _) = authors()[3];
    let (author_a, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(name, ".db_test_cursor_install_order");
    let chain_a = direct_chain(&mut lm, author_a, 1).await;
    let mut cursor = new_cursor(&lm);

    let err = cursor
        .install(
            7,
            SequenceOutcome::Core {
                c: vec![block_ref(&chain_a[0])],
            },
            &[chain_a[0].id.clone()],
            usize::MAX,
        )
        .expect_err("view 7 is not view 1");

    assert_eq!(
        err,
        InstallError::OutOfOrder {
            expected: 1,
            got: 7
        }
    );
    assert_eq!(cursor.next_view(), 1);
    assert!(cursor.output_log().is_empty());
}

/// An install that delivered blocks without moving the per-author watermarks would leave
/// the next ordinary seal walking from a stale point across a prefix the node may no
/// longer hold -- the genesis-anew walk the watermark index exists to remove. Forced here
/// by evicting the installed prefix: with the watermark advanced the seal short-circuits,
/// without it the walk cannot complete and the cursor wedges.
#[tokio::test]
async fn install_advances_the_per_author_watermarks() {
    let (name, _) = authors()[3];
    let (author_a, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(name, ".db_test_cursor_install_watermark");
    let chain_a = direct_chain(&mut lm, author_a, 3).await;
    let blocks = lm.blocks_handle();
    let mut cursor = new_cursor(&lm);

    let tip = block_ref(&chain_a[2]);
    let delta: Vec<Digest> = chain_a.iter().map(|h| h.id.clone()).collect();
    cursor
        .install(
            1,
            SequenceOutcome::Core {
                c: vec![tip.clone()],
            },
            &delta,
            usize::MAX,
        )
        .expect("installs");
    assert_eq!(cursor.next_view(), 2);

    // The installed prefix is gone from the cache; only the tip remains.
    blocks.lock().evict_author_below(&author_a, 3);

    // View 2 names the same tip. The watermark makes this a no-op instead of a walk.
    cursor.on_sealed(2, Outcome::Core(vec![tip]));
    assert_eq!(
        cursor.next_view(),
        3,
        "a stale watermark would force a genesis-anew walk across the evicted prefix, \
         return None, and leave the cursor stuck at view 2"
    );
    assert_eq!(
        cursor.output_log(),
        delta.as_slice(),
        "nothing re-output by the second view"
    );
}

/// A view's delta is the whole accumulated lane suffix since the last emitted watermark,
/// so after a multi-second gap at n=100 it is thousands of headers. Emitting that in one
/// core turn is the starvation this mechanism exists to relieve, so the budget leaves the
/// view OPEN and the next call resumes exactly where it stopped.
#[tokio::test]
async fn install_chunks_a_large_delta_and_resumes() {
    let (name, _) = authors()[3];
    let (author_a, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(name, ".db_test_cursor_install_chunked");
    let chain_a = direct_chain(&mut lm, author_a, 6).await;
    let mut cursor = new_cursor(&lm);

    let outcome = SequenceOutcome::Core {
        c: vec![block_ref(&chain_a[5])],
    };
    let delta: Vec<Digest> = chain_a.iter().map(|h| h.id.clone()).collect();

    let (_, finalized) = cursor
        .install(1, outcome.clone(), &delta, 2)
        .expect("installs a chunk");
    assert!(!finalized, "the budget ran out mid-view");
    assert_eq!(cursor.next_view(), 1, "the view stays open");
    assert_eq!(cursor.open_delta(), &delta[..2]);
    assert_eq!(cursor.output_log(), &delta[..2]);

    let (_, finalized) = cursor
        .install(1, outcome.clone(), &delta, 2)
        .expect("resumes from the same point");
    assert!(!finalized);
    assert_eq!(cursor.open_delta(), &delta[..4]);

    let (_, finalized) = cursor
        .install(1, outcome, &delta, 999)
        .expect("finishes the view");
    assert!(finalized);
    assert_eq!(cursor.next_view(), 2);
    assert_eq!(
        cursor.output_log(),
        delta.as_slice(),
        "chunking changes when blocks are emitted, never which or in what order"
    );
}

/// A repaired header whose worker batches have not been synced is still sequence-ready.
/// `emit` resolves its `Header` and `notify_committed` hands the batch digests to the
/// worker synchronizer, which owns payload materialization.
///
/// The undeliverable block is minted here rather than taken from `direct_chain`, because
/// `payload_ok` is monotonic by design -- `upsert` OR-merges it and `set_payload_ok` only
/// ever sets it true -- so a block that was ever published directly cannot be walked back
/// into the repaired-but-unsynced state this checks.
#[tokio::test]
async fn install_accepts_a_block_whose_payload_is_not_materialized() {
    let (name, _) = authors()[3];
    let (author_a, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(name, ".db_test_cursor_install_payload");
    let chain_a = direct_chain(&mut lm, author_a, 1).await;
    let blocks = lm.blocks_handle();
    let mut cursor = new_cursor(&lm);

    // Exactly what `Repairer::on_serve` leaves behind: chain-verified, payload not synced.
    let repaired = tagged_header(author_a, 2, chain_a[0].id.clone(), lm.sid().clone(), 0xC1);
    blocks
        .lock()
        .upsert(repaired.clone(), false, true, false, true);

    let delta = vec![chain_a[0].id.clone(), repaired.id.clone()];
    let (effects, finalized) = cursor
        .install(
            1,
            SequenceOutcome::Core {
                c: vec![block_ref(&repaired)],
            },
            &delta,
            usize::MAX,
        )
        .expect("chain-verified headers are sequence-ready");
    assert!(finalized);
    assert_eq!(cursor.output_log(), delta.as_slice());
    assert!(
        effects.iter().any(|effect| {
            matches!(
                effect,
                crate::vantage::Effect::NotifyCommitted(_, _, headers)
                    if headers.iter().any(|h| h.id == repaired.id)
            )
        }),
        "the worker learns about the repaired header through commit notification"
    );
}

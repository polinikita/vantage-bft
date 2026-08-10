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

    cursor.on_completed(1, c.clone(), t.clone());
    let k_len = cursor.output_log().len();
    assert_eq!(cursor.output_log(), &[chain_a[0].id.clone()]);
    assert_eq!(cursor.next_view(), 1, "tip stays open until sealed");

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

    let effects = cursor.on_sealed(2, Outcome::Core(vec![block_ref(&chain_b[0])]));
    assert!(effects.is_empty());
    assert!(cursor.output_log().is_empty());
    assert_eq!(cursor.next_view(), 1);

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

    lm.process_publish(author_a, h1.clone()).await;
    lm.process_publish(author_a, h2.clone()).await;
    let effects = cursor.retry();
    assert!(!effects.is_empty());
    assert_eq!(cursor.output_log(), &[h1.id.clone(), h2.id.clone()]);
    assert_eq!(cursor.next_view(), 2);
}

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

use crate::vantage::cursor::InstallError;
use crate::vantage::sequence::SequenceOutcome;
use crypto::Digest;

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

#[tokio::test]
async fn install_over_an_emitted_core_prefix_emits_only_the_remainder() {
    let (name, _) = authors()[3];
    let (author_a, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(name, ".db_test_cursor_install_prefix");
    let chain_a = direct_chain(&mut lm, author_a, 3).await;
    let mut cursor = new_cursor(&lm);

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

    let divergent = vec![chain_a[1].id.clone(), chain_a[2].id.clone()];
    let err = cursor
        .install(1, SequenceOutcome::Core { c }, &divergent, usize::MAX)
        .expect_err("a non-prefix local partial must be refused");

    assert!(matches!(err, InstallError::PrefixMismatch { view: 1, .. }));
    assert_eq!(cursor.next_view(), 1, "cursor unchanged");
    assert_eq!(cursor.open_delta(), &[chain_a[0].id.clone()]);
    assert_eq!(cursor.output_log(), before.as_slice());
}

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

    blocks.lock().evict_author_below(&author_a, 3);

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

#[tokio::test]
async fn install_accepts_a_block_whose_payload_is_not_materialized() {
    let (name, _) = authors()[3];
    let (author_a, _) = authors()[0];
    let (mut lm, _store) = new_lane_manager(name, ".db_test_cursor_install_payload");
    let chain_a = direct_chain(&mut lm, author_a, 1).await;
    let blocks = lm.blocks_handle();
    let mut cursor = new_cursor(&lm);

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

#[tokio::test]
async fn forked_lane_does_not_wedge_the_cursor() {
    let (name, _) = authors()[3];
    let (forker, _) = authors()[0];
    let (honest, _) = authors()[1];
    let (mut lm, _store) = new_lane_manager(name, ".db_test_cursor_forked_lane");

    let original = direct_chain(&mut lm, forker, 1).await;
    let honest_1 = direct_chain(&mut lm, honest, 1).await;
    let mut cursor = new_cursor(&lm);
    let c = vec![block_ref(&original[0])];
    cursor.on_completed(1, c.clone(), Vec::new());
    cursor.on_sealed(1, Outcome::Full(c, Vec::new()));
    assert_eq!(cursor.next_view(), 2);
    assert_eq!(cursor.forked_dropped(), 0);

    let sid = lm.sid().clone();
    let sibling = tagged_header(forker, 1, lm.genesis().clone(), sid.clone(), 7);
    let on_fork = tagged_header(forker, 2, sibling.id.clone(), sid, 9);
    lm.process_publish(forker, sibling.clone()).await;
    lm.process_publish(forker, on_fork.clone()).await;

    let honest_2 = direct_chain(&mut lm, honest, 2).await;
    let c2 = sorted_manifest(vec![block_ref(&on_fork), block_ref(&honest_2[1])]);
    cursor.on_completed(2, c2.clone(), Vec::new());
    cursor.on_sealed(2, Outcome::Full(c2, Vec::new()));

    assert_eq!(
        cursor.next_view(),
        3,
        "the cursor must advance past a view containing a forked lane"
    );
    assert_eq!(
        cursor.forked_dropped(),
        1,
        "exactly the forking author's entry is dropped"
    );
    let log = cursor.output_log();
    assert!(
        log.contains(&honest_2[1].id),
        "the honest author's block must still be delivered"
    );
    assert!(
        !log.contains(&on_fork.id) && !log.contains(&sibling.id),
        "no block from the forked branch may be delivered"
    );
    assert!(
        log.contains(&original[0].id) && log.contains(&honest_1[0].id),
        "view 1's output is untouched"
    );
}

#[tokio::test]
async fn install_and_execution_agree_on_watermarks_across_a_fork() {
    let (name, _) = authors()[3];
    let (forker, _) = authors()[0];
    let (honest, _) = authors()[1];
    let (mut lm, _store) = new_lane_manager(name, ".db_test_cursor_wm_fork");

    let forked_a = direct_chain(&mut lm, forker, 1).await;
    let honest_chain = direct_chain(&mut lm, honest, 3).await;

    let sid = lm.sid().clone();
    let b1 = tagged_header(forker, 1, lm.genesis().clone(), sid.clone(), 7);
    let b2 = tagged_header(forker, 2, b1.id.clone(), sid.clone(), 8);
    let b3 = tagged_header(forker, 3, b2.id.clone(), sid, 9);
    for h in [&b1, &b2, &b3] {
        lm.process_publish(forker, h.clone()).await;
    }

    let mut executing = new_cursor(&lm);
    let mut installing = new_cursor(&lm);
    let c1 = sorted_manifest(vec![block_ref(&forked_a[0]), block_ref(&honest_chain[0])]);
    for cursor in [&mut executing, &mut installing] {
        cursor.on_completed(1, c1.clone(), Vec::new());
        cursor.on_sealed(1, Outcome::Full(c1.clone(), Vec::new()));
    }

    let c2 = sorted_manifest(vec![block_ref(&b2), block_ref(&honest_chain[1])]);
    executing.on_completed(2, c2.clone(), Vec::new());
    executing.on_sealed(2, Outcome::Full(c2.clone(), Vec::new()));
    let delta2 = vec![honest_chain[1].id.clone()];
    installing
        .install(
            2,
            SequenceOutcome::Full {
                c: c2,
                t: Vec::new(),
            },
            &delta2,
            usize::MAX,
        )
        .expect("install view 2");

    let c3 = sorted_manifest(vec![block_ref(&b3), block_ref(&honest_chain[2])]);
    for cursor in [&mut executing, &mut installing] {
        cursor.on_completed(3, c3.clone(), Vec::new());
        cursor.on_sealed(3, Outcome::Full(c3.clone(), Vec::new()));
    }

    assert_eq!(
        executing.output_log(),
        installing.output_log(),
        "an installing node and an executing node must deliver identical logs"
    );
    for (label, cursor) in [("executing", &executing), ("installing", &installing)] {
        assert!(
            !cursor.output_log().contains(&b3.id),
            "{label} must not deliver a block on the forked branch"
        );
    }
}

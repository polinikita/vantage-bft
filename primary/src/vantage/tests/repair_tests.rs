// PHASE3-SPEC.md §7 "Repair (N6/N7)".
use super::common::*;
use crate::messages::Header;
use crate::vantage::block::{genesis_digest, session_id};
use crate::vantage::lanes::BlockCache;
use crate::vantage::repair::{Repairer, FANOUT_FIRST};
use crate::vantage::Effect;
use config::Committee;
use crypto::{Digest, PublicKey};
use std::collections::BTreeMap;
use std::sync::Arc;

fn new_standalone_repairer(name: PublicKey) -> Repairer {
    let committee = test_committee();
    let sid = session_id(&committee);
    let genesis = genesis_digest(&sid);
    Repairer::new(
        name,
        committee,
        sid,
        genesis,
        MAX_BLOCK_PAYLOAD,
        Arc::new(parking_lot::Mutex::new(BlockCache::new())),
    )
}

fn requests_for(effects: &[Effect]) -> Vec<(PublicKey, Digest)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::RequestTo(peer, h) => Some((*peer, h.clone())),
            _ => None,
        })
        .collect()
}

/// P1-2: an unsolicited serve -- hash-correct, `BlockOK`, but for a digest we never
/// requested -- changes no state at all. The normative gate is "on serve(h,b) for a
/// requested h"; without it a peer could bulk-inject unbounded valid blocks of its own
/// lane into our cache for free.
#[tokio::test]
async fn unsolicited_serve_changes_no_state() {
    let all = authors();
    let (watcher, _) = all[0];
    let (author, _) = all[1];
    let mut repairer = new_standalone_repairer(watcher);
    let sid = session_id(&test_committee());
    let genesis = genesis_digest(&sid);

    let block = Header::new_vantage(author, 1, BTreeMap::new(), genesis, sid);
    let h = block.id.clone();

    // Never authorized/requested `h` at all.
    let effects = repairer.on_serve(block);
    assert!(effects.is_empty());
    assert!(!repairer_blocks(&repairer).contains(&h));
    assert_eq!(repairer.requested_count(), 0);
}

/// N6: recursive walk to genesis, where each ancestor is only requested (and hence
/// only acceptable via `on_serve`, P1-2) once the walk actually reaches it -- serving
/// arrives in the causally correct order the walk itself drives: h3 first (the only
/// hash requested up front), which on being served triggers the request for h2, which
/// on being served triggers the request for h1.
#[tokio::test]
async fn recursive_walk_over_served_blocks() {
    let all = authors();
    let (watcher, _) = all[0];
    let (author, _) = all[1];
    let mut repairer = new_standalone_repairer(watcher);
    let sid = session_id(&test_committee());
    let genesis = genesis_digest(&sid);

    let h1 = Header::new_vantage(author, 1, BTreeMap::new(), genesis, sid.clone());
    let h2 = Header::new_vantage(author, 2, BTreeMap::new(), h1.id.clone(), sid.clone());
    let h3 = Header::new_vantage(author, 3, BTreeMap::new(), h2.id.clone(), sid);

    let r3 = (author, 3, h3.id.clone());
    let n_peers = test_committee().others_primaries(&watcher).len();
    let effects = repairer.authorize(r3.clone());
    assert_eq!(requests_for(&effects).len(), n_peers); // only h3 requested so far

    // Serving h3 advances the walk one step: it recurses into (author, 2, h2), which
    // isn't cached yet, so *that* is what triggers h2's own request fan-out.
    let effects = repairer.on_serve(h3.clone());
    assert!(requests_for(&effects).iter().all(|(_, h)| h == &h2.id));
    assert_eq!(requests_for(&effects).len(), n_peers);

    // Serving h2 similarly triggers h1's request.
    let effects = repairer.on_serve(h2.clone());
    assert!(requests_for(&effects).iter().all(|(_, h)| h == &h1.id));
    assert_eq!(requests_for(&effects).len(), n_peers);

    // Serving h1 (whose parent is genesis) completes the whole walk down to genesis in
    // one final step -- every block in the prefix is now retained.
    let effects = repairer.on_serve(h1.clone());
    assert!(effects.iter().all(|e| !matches!(e, Effect::ServeTo(_, _)))); // no pending requesters

    let blocks = repairer_blocks(&repairer);
    assert!(blocks.get(&h1.id).unwrap().retained);
    assert!(blocks.get(&h2.id).unwrap().retained);
    assert!(blocks.get(&h3.id).unwrap().retained);
}

/// N6/D2: request fan-out is to all other parties, and at most once per (peer, hash)
/// ever, even across repeated `authorize` calls for the same tuple.
#[tokio::test]
async fn request_fanout_all_parties_once() {
    let all = authors();
    let (watcher, _) = all[0];
    let (author, _) = all[1];
    let mut repairer = new_standalone_repairer(watcher);
    let r = (author, 5u64, Digest([9u8; 32]));

    let first = repairer.authorize(r.clone());
    let n_peers = test_committee().others_primaries(&watcher).len();
    assert_eq!(requests_for(&first).len(), n_peers);

    let second = repairer.authorize(r);
    assert_eq!(requests_for(&second).len(), 0);
}

/// N6: a false-coordinate serve (a genuinely valid block, but not matching the
/// coordinate we authorized it under) is cached but does not advance that walk; a
/// later authorize under the *real* (exact) coordinate consumes the cached body
/// without issuing a new request.
#[tokio::test]
async fn false_coordinate_cached_not_advanced() {
    let all = authors();
    let (watcher, _) = all[0];
    let (real_author, _) = all[1];
    let (fake_author, _) = all[2];
    let mut repairer = new_standalone_repairer(watcher);
    let sid = session_id(&test_committee());
    let genesis = genesis_digest(&sid);

    let block = Header::new_vantage(real_author, 1, BTreeMap::new(), genesis, sid);
    let h = block.id.clone();

    // Authorize under a coordinate that does NOT match the block's real author/height.
    let fake_ref = (fake_author, 7u64, h.clone());
    repairer.authorize(fake_ref.clone());
    let n_peers = test_committee().others_primaries(&watcher).len();
    assert_eq!(repairer.requested_count(), n_peers);

    let effects = repairer.on_serve(block.clone());
    // Cached...
    assert!(repairer_blocks(&repairer).contains(&h));
    // ...but the fake-coordinate walk did not complete (no serve to anyone -- there
    // were no pending requesters -- and, more importantly, no crash/false success).
    assert!(effects
        .iter()
        .all(|e| !matches!(e, Effect::RequestTo(_, _))));

    // A later, exact-coordinate authorize consumes the cached body: no new request.
    let real_ref = (real_author, 1u64, h);
    let before = repairer.requested_count();
    let effects = repairer.authorize(real_ref);
    assert_eq!(repairer.requested_count(), before);
    assert!(requests_for(&effects).is_empty());
}

/// N6: a corrupted serve (declared id doesn't match the recomputed digest, i.e. a
/// hash/body mismatch) is ignored outright -- no state change, the hash stays
/// un-obtained. Requests the corrupted `id` first (P1-2's requested-hash gate must not
/// be what rejects this -- the point is `BlockOK`'s hash-consistency check).
#[tokio::test]
async fn corrupted_serve_ignored() {
    let all = authors();
    let (watcher, _) = all[0];
    let (author, _) = all[1];
    let mut repairer = new_standalone_repairer(watcher);
    let sid = session_id(&test_committee());
    let genesis = genesis_digest(&sid);

    let mut block = Header::new_vantage(author, 1, BTreeMap::new(), genesis, sid);
    let corrupted_id = Digest([0xFFu8; 32]);
    block.id = corrupted_id.clone(); // corrupt: id no longer matches digest()

    repairer.authorize((author, 1, corrupted_id.clone()));
    let effects = repairer.on_serve(block);
    assert!(effects.is_empty());
    assert!(!repairer_blocks(&repairer).contains(&corrupted_id));
}

/// N7: `pendingReq` is recorded before we hold the block; once retained, we answer
/// exactly once, and a repeated request from the same peer never gets a second answer.
#[tokio::test]
async fn pending_request_answered_once_on_retention() {
    let all = authors();
    let (watcher, _) = all[0];
    let (author, _) = all[1];
    let (requester, _) = all[2];
    let mut repairer = new_standalone_repairer(watcher);
    let sid = session_id(&test_committee());
    let genesis = genesis_digest(&sid);

    let block = Header::new_vantage(author, 1, BTreeMap::new(), genesis, sid);
    let h = block.id.clone();

    // Request arrives before we hold the block: no serve yet.
    let effects = repairer.on_request(requester, h.clone());
    assert!(effects.is_empty());

    // Once the block arrives (authorized + served in), the pending request is
    // answered exactly once.
    repairer.authorize((author, 1, h.clone()));
    let effects = repairer.on_serve(block);
    assert_eq!(
        effects
            .iter()
            .filter(|e| matches!(e, Effect::ServeTo(p, _) if *p == requester))
            .count(),
        1
    );

    // A repeated request from the same peer for the same hash never gets answered
    // again.
    let effects = repairer.on_request(requester, h);
    assert!(effects.iter().all(|e| !matches!(e, Effect::ServeTo(_, _))));
}

/// PHASE6-SPEC.md §9 gate amendment, R1: `settled` ⇒ retained ⇒ servable (N7/N8
/// unchanged) -- and the ref moves out of `pending_settle` into `settled` exactly when
/// the walk to genesis first succeeds.
#[tokio::test]
async fn settled_ref_is_retained_and_servable_and_leaves_pending() {
    let all = authors();
    let (watcher, _) = all[0];
    let (author, _) = all[1];
    let (requester, _) = all[2];
    let mut repairer = new_standalone_repairer(watcher);
    let sid = session_id(&test_committee());
    let genesis = genesis_digest(&sid);

    let block = Header::new_vantage(author, 1, BTreeMap::new(), genesis, sid);
    let h = block.id.clone();
    let r = (author, 1u64, h.clone());

    repairer.authorize(r.clone());
    assert!(repairer.is_pending_settle(&r));
    assert!(!repairer.is_settled(&r));

    repairer.on_serve(block);
    assert!(repairer.is_settled(&r));
    assert!(!repairer.is_pending_settle(&r));
    assert!(repairer_blocks(&repairer).get(&h).unwrap().retained);

    // Servable: a request arriving after settlement is answered.
    let effects = repairer.on_request(requester, h.clone());
    assert_eq!(
        effects
            .iter()
            .filter(|e| matches!(e, Effect::ServeTo(p, _) if *p == requester))
            .count(),
        1
    );
}

/// PHASE6-SPEC.md §9 gate amendment, R1: a cached block under the WRONG coordinate
/// keeps its ref pending (never settled) -- `settle`'s memoization must not falsely
/// mark a fake-coordinate authorize as settled just because a block with that hash
/// happens to be cached under a different, real coordinate.
#[tokio::test]
async fn wrong_coordinate_cached_block_keeps_ref_pending() {
    let all = authors();
    let (watcher, _) = all[0];
    let (real_author, _) = all[1];
    let (fake_author, _) = all[2];
    let mut repairer = new_standalone_repairer(watcher);
    let sid = session_id(&test_committee());
    let genesis = genesis_digest(&sid);

    let block = Header::new_vantage(real_author, 1, BTreeMap::new(), genesis, sid);
    let h = block.id.clone();
    let fake_ref = (fake_author, 7u64, h.clone());

    repairer.authorize(fake_ref.clone());
    repairer.on_serve(block);

    assert!(repairer.is_pending_settle(&fake_ref));
    assert!(!repairer.is_settled(&fake_ref));
}

/// PHASE6-SPEC.md §9 gate amendment, R1(c): `on_block_available` iterating only
/// `pending_settle` still propagates retention across a chain of refs authorized
/// separately (repro of the exact scenario `on_block_available`'s original doc comment
/// worried about: h3 authorized/served first, h2 and h1 authorized only afterward as
/// the walk discovers them) -- the whole prefix ends up settled+retained regardless of
/// arrival order across separate `authorize`/`on_serve` calls.
#[tokio::test]
async fn recursive_walk_settles_and_retains_whole_prefix_via_pending_only_sweep() {
    let all = authors();
    let (watcher, _) = all[0];
    let (author, _) = all[1];
    let mut repairer = new_standalone_repairer(watcher);
    let sid = session_id(&test_committee());
    let genesis = genesis_digest(&sid);

    let h1 = Header::new_vantage(author, 1, BTreeMap::new(), genesis, sid.clone());
    let h2 = Header::new_vantage(author, 2, BTreeMap::new(), h1.id.clone(), sid.clone());
    let h3 = Header::new_vantage(author, 3, BTreeMap::new(), h2.id.clone(), sid);

    let r1 = (author, 1u64, h1.id.clone());
    let r2 = (author, 2u64, h2.id.clone());
    let r3 = (author, 3u64, h3.id.clone());

    repairer.authorize(r3.clone());
    repairer.on_serve(h3.clone());
    repairer.on_serve(h2.clone());
    repairer.on_serve(h1.clone());

    for r in [&r1, &r2, &r3] {
        assert!(repairer.is_settled(r), "{:?} should be settled", r);
        assert!(!repairer.is_pending_settle(r));
    }
    let blocks = repairer_blocks(&repairer);
    assert!(blocks.get(&h1.id).unwrap().retained);
    assert!(blocks.get(&h2.id).unwrap().retained);
    assert!(blocks.get(&h3.id).unwrap().retained);
}

fn repairer_blocks(repairer: &Repairer) -> parking_lot::MutexGuard<'_, BlockCache> {
    // `Repairer` doesn't expose its `SharedBlocks` field directly (it isn't needed in
    // production code, where `LaneManager` owns the canonical handle); tests construct
    // repairers standalone, so they need a matching accessor.
    repairer.blocks_for_test()
}

/// `settle`'s peer fan-out is gated on `requested_hashes.insert(h)` rather than run
/// unconditionally (the 2026-08-08 straggler fix; it is what makes a repeated miss on the
/// same digest O(1), which `on_block_available`'s walks depend on).
///
/// That gate's soundness condition CHANGED with the 2026-08-07 bounded fan-out. It used to
/// be "`requested_hashes.contains(h)` implies `requested` already holds `(p, h)` for every
/// other primary" -- true only while the first round asked everyone. Now the first round
/// asks `FANOUT_FIRST` peers, so the condition is weaker but must still exclude the
/// dangerous case: a gated digest whose coverage is INCOMPLETE and which is no longer
/// scheduled to widen would never be asked of the remaining peers, silently losing N6's
/// eventual-coverage guarantee. So the invariant is: gated implies complete-or-escalating.
///
/// Checked at n=10, where the first round is genuinely partial -- at n=4 there are 3 peers,
/// below `FANOUT_FIRST`, so coverage completes immediately and the property is vacuous.
#[tokio::test]
async fn requested_hashes_set_implies_coverage_complete_or_escalating() {
    let (committee, keys) = Committee::local_benchmark(10, 1, 26_000);
    let (name, author) = (keys[0].name, keys[1].name);
    let peers: Vec<PublicKey> = committee
        .others_primaries(&name)
        .into_iter()
        .map(|(pk, _)| pk)
        .collect();
    let mut repairer = wide_repairer(name, &committee);
    let h = Header::default().id.clone();

    let requested = requests_for(&repairer.authorize((author, 1, h.clone())));
    assert!(repairer.was_requested_hash(&h), "the hash gate must be set");

    // Partial coverage now -- and therefore an escalation must be scheduled.
    assert_eq!(requested.len(), FANOUT_FIRST);
    assert!(requested.len() < peers.len());
    assert_eq!(repairer.fanout_asked_for_test(&h), Some(FANOUT_FIRST));
    assert!(
        repairer.is_escalating_for_test(&h),
        "gated with partial coverage but NOT queued to widen -- the remaining peers \
         would never be asked and N6's eventual guarantee would be lost"
    );

    // Re-authorizing the same still-missing ref emits nothing (the gate short-circuits),
    // which is the property the 2026-08-08 fix relies on for its cost reduction.
    let again = repairer.authorize((author, 1, h.clone()));
    assert!(
        requests_for(&again).is_empty(),
        "a repeat authorize on a still-missing digest must emit no new requests"
    );

    // Drive escalation to completion: every peer is in `requested`, and only then is the
    // per-digest state allowed to disappear.
    let mut all = requested;
    for _ in 0..8 {
        all.extend(requests_for(&repairer.retry_requests()));
    }
    for peer in &peers {
        assert!(
            repairer.was_requested(peer, &h),
            "peer {peer} was never asked even after full escalation"
        );
    }
    assert_eq!(all.len(), peers.len(), "no peer may be asked twice");
    assert!(
        repairer.fanout_asked_for_test(&h).is_none() && !repairer.is_escalating_for_test(&h),
        "state must be dropped only once coverage is complete"
    );
}

/// n=100 straggler fix (2026-08-08): `on_block_available` is digest-indexed. A block
/// nobody is waiting on must cost nothing -- before this, EVERY arrival re-walked all
/// of `pending_settle`, which on the failing n=100 run produced 612,424,724 `settle`
/// calls against 60,262 received blocks (a ratio of 10,163).
#[tokio::test]
async fn an_arrival_nobody_waits_on_settles_nothing() {
    let all = authors();
    let (watcher, _) = all[0];
    let (author, _) = all[1];
    let mut repairer = new_standalone_repairer(watcher);
    let sid = session_id(&test_committee());
    let genesis = genesis_digest(&sid);

    let wanted = Header::new_vantage(author, 1, BTreeMap::new(), genesis.clone(), sid.clone());
    let unrelated = Header::new_vantage(author, 7, BTreeMap::new(), genesis, sid);

    repairer.authorize((author, 1, wanted.id.clone()));
    let before = repairer.settle_calls_for_test();
    // An unrelated digest: no ref is blocked on it.
    let effects = repairer.on_block_available(unrelated.id.clone());
    assert!(effects.is_empty());
    assert_eq!(
        repairer.settle_calls_for_test(),
        before,
        "an arrival with an empty wait-bucket must not call settle at all"
    );
}

/// The bucket must wake exactly the refs waiting on that digest, and the walk must
/// still advance -- i.e. the index is a filter on the old sweep, not a change to it.
#[tokio::test]
async fn an_arrival_wakes_only_the_refs_waiting_on_it() {
    let all = authors();
    let (watcher, _) = all[0];
    let (a1, _) = all[1];
    let (a2, _) = all[2];
    let mut repairer = new_standalone_repairer(watcher);
    let sid = session_id(&test_committee());
    let genesis = genesis_digest(&sid);
    let n_peers = test_committee().others_primaries(&watcher).len();

    // Two independent lanes, each authorized at height 2 so each blocks on its own h1.
    let l1h1 = Header::new_vantage(a1, 1, BTreeMap::new(), genesis.clone(), sid.clone());
    let l1h2 = Header::new_vantage(a1, 2, BTreeMap::new(), l1h1.id.clone(), sid.clone());
    let l2h1 = Header::new_vantage(a2, 1, BTreeMap::new(), genesis, sid.clone());
    let l2h2 = Header::new_vantage(a2, 2, BTreeMap::new(), l2h1.id.clone(), sid);

    repairer.authorize((a1, 2, l1h2.id.clone()));
    repairer.authorize((a2, 2, l2h2.id.clone()));

    // Serving lane 1's head advances lane 1 only: the new requests are all for l1h1.
    let effects = repairer.on_serve(l1h2.clone());
    let reqs = requests_for(&effects);
    assert_eq!(reqs.len(), n_peers);
    assert!(
        reqs.iter().all(|(_, h)| h == &l1h1.id),
        "lane 2 must not have been touched by lane 1's arrival"
    );

    // And lane 2 still advances when its own block arrives.
    let effects = repairer.on_serve(l2h2.clone());
    let reqs = requests_for(&effects);
    assert_eq!(reqs.len(), n_peers);
    assert!(reqs.iter().all(|(_, h)| h == &l2h1.id));
}

/// Re-blocking moves a ref between buckets rather than leaving it in both, so the
/// index cannot accumulate duplicates as a walk descends a deep gap one level per
/// arrival. Without this the buckets would grow like the set we just stopped scanning.
#[tokio::test]
async fn a_reblocked_ref_leaves_its_previous_bucket() {
    let all = authors();
    let (watcher, _) = all[0];
    let (author, _) = all[1];
    let mut repairer = new_standalone_repairer(watcher);
    let sid = session_id(&test_committee());
    let genesis = genesis_digest(&sid);

    let h1 = Header::new_vantage(author, 1, BTreeMap::new(), genesis, sid.clone());
    let h2 = Header::new_vantage(author, 2, BTreeMap::new(), h1.id.clone(), sid.clone());
    let h3 = Header::new_vantage(author, 3, BTreeMap::new(), h2.id.clone(), sid);
    let r3 = (author, 3, h3.id.clone());

    repairer.authorize(r3.clone());
    assert_eq!(
        repairer.blocked_on_len_for_test(&h3.id),
        1,
        "r3 blocks on h3"
    );

    // h3 arrives: r3's walk descends and now blocks on h2 instead.
    repairer.on_serve(h3.clone());
    assert_eq!(
        repairer.blocked_on_len_for_test(&h3.id),
        0,
        "the h3 bucket must be emptied, not left holding a stale r3"
    );
    assert!(
        repairer.blocked_on_len_for_test(&h2.id) >= 1,
        "r3 now blocks on h2"
    );

    // The whole chain still settles once the rest arrives -- behaviour unchanged.
    repairer.on_serve(h2.clone());
    repairer.on_serve(h1.clone());
    assert!(
        repairer.is_settled(&r3),
        "the chain must still settle end to end"
    );
    assert_eq!(repairer.blocked_on_len_for_test(&h1.id), 0);
    assert_eq!(repairer.blocked_on_len_for_test(&h2.id), 0);
}

// --- n=100 recovery fix (2026-08-07): bounded, escalating, height-prioritised fan-out.
//
// The n=4 fixture above cannot exercise any of this: it has 3 peers, below
// `FANOUT_FIRST`, so its first round already covers the committee and the staged path
// never runs. These use a 10-party committee (9 peers) instead.

/// A standalone `Repairer` over an arbitrary committee -- `new_standalone_repairer`'s
/// generalization, same rationale as `new_agb_engine_with_committee`.
fn wide_repairer(name: PublicKey, committee: &Committee) -> Repairer {
    let sid = session_id(committee);
    let genesis = genesis_digest(&sid);
    Repairer::new(
        name,
        committee.clone(),
        sid,
        genesis,
        MAX_BLOCK_PAYLOAD,
        Arc::new(parking_lot::Mutex::new(BlockCache::new())),
    )
}

/// The first round asks `FANOUT_FIRST` peers, NOT all n-1. On the failing n=100 run every
/// stalled node's `vantage_repairs_requested` was an exact multiple of 99 (node 72:
/// 5,133,249 = 51,851 distinct digests x 99 peers), and the 99 answers per digest
/// overflowed the bulk inbound queue -- 663,546 drops versus 186 on a healthy node --
/// so the body the node needed was itself dropped.
#[tokio::test]
async fn fanout_first_round_is_bounded() {
    let (committee, keys) = Committee::local_benchmark(10, 1, 21_000);
    let (watcher, author) = (keys[0].name, keys[1].name);
    let n_peers = committee.others_primaries(&watcher).len();
    assert!(
        n_peers > FANOUT_FIRST,
        "fixture must exceed the first-round width to test anything"
    );
    let mut rep = wide_repairer(watcher, &committee);

    let effects = rep.authorize((author, 5u64, Digest([7u8; 32])));
    assert_eq!(requests_for(&effects).len(), FANOUT_FIRST);
}

/// N6's guarantee is EVENTUAL full coverage, and it must be full rather than a quorum:
/// the holder set is only guaranteed f+1 stake, so worst case exactly one member is
/// correct and its identity is unknown. Escalation must therefore reach every peer, and
/// must never ask the same peer twice.
#[tokio::test]
async fn fanout_escalates_to_every_peer_without_repeating_one() {
    let (committee, keys) = Committee::local_benchmark(10, 1, 22_000);
    let (watcher, author) = (keys[0].name, keys[1].name);
    let n_peers = committee.others_primaries(&watcher).len();
    let mut rep = wide_repairer(watcher, &committee);
    let h = Digest([7u8; 32]);

    let mut asked = requests_for(&rep.authorize((author, 5u64, h.clone())));
    // 4 -> 8 -> 16(clamped): full coverage inside a handful of ticks.
    for _ in 0..8 {
        asked.extend(requests_for(&rep.retry_requests()));
    }
    let distinct: std::collections::HashSet<_> = asked.iter().map(|(p, _)| *p).collect();
    assert_eq!(
        distinct.len(),
        n_peers,
        "every peer must eventually be asked"
    );
    assert_eq!(
        asked.len(),
        n_peers,
        "no (peer, digest) may be requested twice -- N6 says at most once, ever"
    );
    assert!(asked.iter().all(|(_, d)| d == &h));
}

/// Escalation stops as soon as the digest is in hand: a node that has what it asked for
/// must not keep widening the fan-out, or the bounded first round buys nothing over time.
#[tokio::test]
async fn fanout_stops_escalating_once_the_digest_arrives() {
    let (committee, keys) = Committee::local_benchmark(10, 1, 23_000);
    let (watcher, author) = (keys[0].name, keys[1].name);
    let sid = session_id(&committee);
    let genesis = genesis_digest(&sid);
    let mut rep = wide_repairer(watcher, &committee);

    let h1 = Header::new_vantage(author, 1, BTreeMap::new(), genesis, sid);
    let first = requests_for(&rep.authorize((author, 1, h1.id.clone())));
    assert_eq!(first.len(), FANOUT_FIRST);

    rep.on_serve(h1.clone());
    for _ in 0..4 {
        assert!(
            requests_for(&rep.retry_requests()).is_empty(),
            "a digest already served must never be re-fanned"
        );
    }
}

/// Different digests must start at different peers. With a fixed start, every node's
/// first `FANOUT_FIRST` requests land on the same few peers, so at n=100 a handful of
/// nodes would serve the committee's entire repair load while the rest served none.
#[tokio::test]
async fn fanout_start_is_spread_across_digests() {
    let (committee, keys) = Committee::local_benchmark(10, 1, 24_000);
    let (watcher, author) = (keys[0].name, keys[1].name);
    let mut rep = wide_repairer(watcher, &committee);

    let mut firsts = std::collections::HashSet::new();
    for i in 0..16u8 {
        let effects = rep.authorize((author, 5u64, Digest([i; 32])));
        if let Some((peer, _)) = requests_for(&effects).first() {
            firsts.insert(*peer);
        }
    }
    assert!(
        firsts.len() > 1,
        "all {} digests began their fan-out at the same peer -- load is not spread",
        16
    );
}

/// Escalation is ordered by the missing block's HEIGHT, lowest first. Repair is parallel
/// (the failing nodes had tens of thousands of digests outstanding) but output is strictly
/// serial -- `Cursor::pump` only advances `next_view` -- so budget spent on a high digest
/// is budget spent on something the node provably cannot use yet.
#[tokio::test]
async fn fanout_escalates_the_lowest_height_first() {
    let (committee, keys) = Committee::local_benchmark(10, 1, 25_000);
    let (watcher, author) = (keys[0].name, keys[1].name);
    let mut rep = wide_repairer(watcher, &committee);

    let (low, high) = (Digest([1u8; 32]), Digest([2u8; 32]));
    // Authorize the HIGH one first, so insertion order and height order disagree.
    rep.authorize((author, 900u64, high.clone()));
    rep.authorize((author, 5u64, low.clone()));

    let order = requests_for(&rep.retry_requests());
    let first_low = order.iter().position(|(_, d)| d == &low);
    let first_high = order.iter().position(|(_, d)| d == &high);
    assert!(
        first_low < first_high,
        "the lower height must escalate first: {first_low:?} vs {first_high:?}"
    );
}

/// Target choice: the FIRST round must go to peers known to hold this author's lane at this
/// height, not to peers picked by hashing the digest.
///
/// This is the second half of the n=100 recovery fix. Bounding the width stopped the flood
/// (bulk drops 639,851 -> 0) but 11 of 100 nodes still lagged, and the counters said why:
/// `escalations` 6,101 against `fanout_pending` 6,109, i.e. essentially every outstanding
/// digest needed a round beyond the first four peers. Four peers was enough; four peers
/// chosen by digest hash was not.
#[tokio::test]
async fn first_round_prefers_peers_known_to_hold_the_lane() {
    let (committee, keys) = Committee::local_benchmark(10, 1, 27_000);
    let (watcher, author) = (keys[0].name, keys[1].name);
    let mut rep = wide_repairer(watcher, &committee);

    // Exactly FANOUT_FIRST peers have confirmed author's lane at/above height 5, so the
    // round is filled entirely from the holder index and the fallback rotation is not
    // consulted at all -- which is what makes the exclusion below meaningful. (With FEWER
    // known holders than the width, the fill loop must pick arbitrary extra peers, and a
    // below-height peer being among them is correct behaviour, not a preference.)
    let holders = [keys[6].name, keys[7].name, keys[8].name, keys[9].name];
    assert_eq!(holders.len(), FANOUT_FIRST);
    for (i, p) in holders.iter().enumerate() {
        rep.note_holder(*p, author, 5 + i as u64);
    }
    rep.note_holder(keys[5].name, author, 4); // too low for height 5

    let asked = requests_for(&rep.authorize((author, 5u64, Digest([7u8; 32]))));
    assert_eq!(asked.len(), FANOUT_FIRST);
    let asked_peers: std::collections::HashSet<_> = asked.iter().map(|(p, _)| *p).collect();
    for p in &holders {
        assert!(
            asked_peers.contains(p),
            "a peer known to hold the lane was not in the first round"
        );
    }
    assert!(
        !asked_peers.contains(&keys[5].name),
        "a peer whose confirmed height is BELOW the missing block displaced a known holder"
    );
}

/// Holder preference must not break coverage: escalation still reaches every peer exactly
/// once, including the ones the holder index knew nothing about.
#[tokio::test]
async fn holder_preference_still_reaches_full_coverage_exactly_once() {
    let (committee, keys) = Committee::local_benchmark(10, 1, 28_000);
    let (watcher, author) = (keys[0].name, keys[1].name);
    let n_peers = committee.others_primaries(&watcher).len();
    let mut rep = wide_repairer(watcher, &committee);
    rep.note_holder(keys[9].name, author, 99);
    let h = Digest([7u8; 32]);

    let mut all = requests_for(&rep.authorize((author, 5u64, h.clone())));
    for _ in 0..8 {
        all.extend(requests_for(&rep.retry_requests()));
    }
    let distinct: std::collections::HashSet<_> = all.iter().map(|(p, _)| *p).collect();
    assert_eq!(
        distinct.len(),
        n_peers,
        "every peer must eventually be asked"
    );
    assert_eq!(
        all.len(),
        n_peers,
        "no peer twice -- N6 allows one request each"
    );
}

/// `note_holder` is monotone: a stale, lower credit must never lower a peer's confirmed
/// height, or the fan-out would stop preferring a peer that demonstrably has the data.
#[tokio::test]
async fn note_holder_never_regresses() {
    let (committee, keys) = Committee::local_benchmark(10, 1, 29_000);
    let (watcher, author) = (keys[0].name, keys[1].name);
    let mut rep = wide_repairer(watcher, &committee);

    rep.note_holder(keys[5].name, author, 50);
    rep.note_holder(keys[5].name, author, 3); // stale credit arriving late

    // Still preferred for a height it confirmed earlier.
    let asked = requests_for(&rep.authorize((author, 40u64, Digest([1u8; 32]))));
    assert!(
        asked.iter().any(|(p, _)| *p == keys[5].name),
        "a stale lower credit regressed the confirmed height"
    );
}

/// The recovery budget must DEFER, never DROP. `requested`/`requested_hashes` are permanent
/// one-shot records, so a request denied after being recorded would mean that peer is never
/// asked again -- a liveness bug dressed as congestion control. A denied digest must keep its
/// state and be picked up by the next tick.
#[tokio::test]
async fn recovery_budget_defers_requests_instead_of_dropping_them() {
    use crate::vantage::repair::RECOVERY_EMIT_START;
    let (committee, keys) = Committee::local_benchmark(10, 1, 30_000);
    let (watcher, author) = (keys[0].name, keys[1].name);
    let mut rep = wide_repairer(watcher, &committee);

    // Spend the whole tick's allowance on distinct digests (FANOUT_FIRST requests each).
    let mut emitted = 0usize;
    let mut h = 0u8;
    while emitted < RECOVERY_EMIT_START && h < 255 {
        let d = Digest([h; 32]);
        emitted += requests_for(&rep.authorize((author, 1 + h as u64, d))).len();
        h += 1;
    }
    assert!(emitted <= RECOVERY_EMIT_START, "budget must cap emission");

    // One more digest while the budget is spent: nothing emitted, but the digest must NOT be
    // marked as requested (that would let an unsolicited serve through P1-2's gate).
    let starved = Digest([0xAB; 32]);
    let none = requests_for(&rep.authorize((author, 900u64, starved.clone())));
    if emitted >= RECOVERY_EMIT_START {
        assert!(none.is_empty(), "budget spent -- nothing may be emitted");
        assert!(
            !rep.was_requested_hash(&starved),
            "a digest we never actually asked for must not pass the serve gate"
        );
        // Next tick refills, and the deferred digest is served first (lowest height wins,
        // but it is present either way).
        let after = requests_for(&rep.retry_requests());
        assert!(
            !after.is_empty(),
            "the next tick must pick up work deferred by the budget"
        );
    }
}

// --- BlockCache eviction floor (2026-08-07). `BlockCache` kept "every block this node has
// ever obtained" with no eviction: 2.504 MB/s/node at n=30, ~4,286 B per entry, growing at
// the committee's block rate -- OOM against 8 GiB in ~8-10 min at n=100.
//
// The floor is "EVERY peer has confirmed holding this lane at or above h". These tests pin
// the safety side, because an over-eager floor drops a block a peer still needs and starves
// exactly the repair path this module was rewritten to fix.

/// A lane nobody has reported on must never be evictable. `None`, not zero: an incomplete
/// picture has to BLOCK eviction, not authorise it from height 0.
#[tokio::test]
async fn eviction_floor_is_unknown_until_every_peer_has_reported() {
    let (committee, keys) = Committee::local_benchmark(10, 1, 31_000);
    let (watcher, author) = (keys[0].name, keys[1].name);
    let n_peers = committee.others_primaries(&watcher).len();
    let mut rep = wide_repairer(watcher, &committee);

    assert_eq!(
        rep.universally_held_below(&author),
        None,
        "no reports at all"
    );

    // All but one peer reports a high height -- still not evictable.
    for k in keys.iter().skip(1).take(n_peers - 1) {
        rep.note_holder(k.name, author, 500);
    }
    assert_eq!(
        rep.universally_held_below(&author),
        None,
        "one silent peer must pin the lane, not be treated as height 0"
    );
}

/// With every peer reporting, the floor is the MINIMUM -- the slowest peer decides. Taking a
/// median or a quorum here would drop blocks the laggard still needs.
#[tokio::test]
async fn eviction_floor_is_the_slowest_peer_not_a_quorum() {
    let (committee, keys) = Committee::local_benchmark(10, 1, 32_000);
    let (watcher, author) = (keys[0].name, keys[1].name);
    let n_peers = committee.others_primaries(&watcher).len();
    let mut rep = wide_repairer(watcher, &committee);

    // One peer is far behind and STAYS behind -- note_holder is monotone, so a laggard has
    // to be given its low height and never raised (trying to lower it later is correctly
    // ignored, which is itself pinned by `note_holder_never_regresses`).
    let laggard = keys[1].name;
    for (i, k) in keys.iter().skip(1).take(n_peers).enumerate() {
        let height = if k.name == laggard {
            7
        } else {
            1000 + i as u64
        };
        rep.note_holder(k.name, author, height);
    }
    assert_eq!(
        rep.universally_held_below(&author),
        Some(7),
        "the floor must be the minimum across peers, not a quorum or a median"
    );
}

/// `evict_author_below` drops strictly below the cut, keeps the rest, and touches no other
/// author's lane.
#[tokio::test]
async fn eviction_drops_only_below_the_cut_and_only_that_author() {
    use crate::vantage::lanes::BlockCache;
    let all = authors();
    let (a1, _) = all[1];
    let (a2, _) = all[2];
    let sid = session_id(&test_committee());
    let genesis = genesis_digest(&sid);
    let mut cache = BlockCache::new();

    let mut mk = |author, height| {
        let h = Header::new_vantage(
            author,
            height,
            BTreeMap::new(),
            genesis.clone(),
            sid.clone(),
        );
        let id = h.id.clone();
        cache.upsert(h, true, false, true, true);
        id
    };
    let ids1: Vec<_> = (1..=10).map(|k| mk(a1, k)).collect();
    let ids2: Vec<_> = (1..=10).map(|k| mk(a2, k)).collect();
    assert_eq!(cache.len(), 20);

    let dropped = cache.evict_author_below(&a1, 6);
    assert_eq!(dropped, 5, "heights 1..=5 of a1");
    for (k, id) in ids1.iter().enumerate() {
        let height = k as u64 + 1;
        assert_eq!(
            cache.contains(id),
            height >= 6,
            "a1 height {height} retention is wrong after evicting below 6"
        );
    }
    for id in &ids2 {
        assert!(
            cache.contains(id),
            "another author's lane must be untouched"
        );
    }
    assert_eq!(cache.len(), 15);
}

/// Evicting a lane with no cached blocks, or below a cut under everything held, is a no-op
/// rather than a panic -- the driver calls this once per known author every tick.
#[tokio::test]
async fn eviction_of_an_absent_or_untouched_lane_is_a_noop() {
    use crate::vantage::lanes::BlockCache;
    let all = authors();
    let (a1, _) = all[1];
    let (absent, _) = all[3];
    let sid = session_id(&test_committee());
    let genesis = genesis_digest(&sid);
    let mut cache = BlockCache::new();
    cache.upsert(
        Header::new_vantage(a1, 9, BTreeMap::new(), genesis, sid),
        true,
        false,
        true,
        true,
    );

    assert_eq!(cache.evict_author_below(&absent, 100), 0);
    assert_eq!(
        cache.evict_author_below(&a1, 9),
        0,
        "cut equals the only height"
    );
    assert_eq!(cache.len(), 1);
}

/// The recovery ceiling must be CONDITIONAL, not constant.
///
/// A fixed ceiling was a measured regression (n=100 at 43f4f0c): node 96 deferred 5,425 and
/// 9,224 requests across two attempts, healthy nodes deferred 0, and its own
/// `vantage_bulk_inbound_dropped_total` was ZERO the whole time. There was no congestion --
/// the constant simply refused work to the one node that needed to recover, leaving its
/// cursor 481/452 views behind and failing a 200-view gate the previous build passed at 74.
///
/// So: double on a clean tick, halve on a tick that saw new bulk drops, clamped both ways.
#[tokio::test]
async fn recovery_ceiling_grows_when_clean_and_backs_off_on_drops() {
    use crate::vantage::repair::{RECOVERY_EMIT_MAX, RECOVERY_EMIT_MIN, RECOVERY_EMIT_START};
    let (committee, keys) = Committee::local_benchmark(10, 1, 33_000);
    let registry = prometheus::Registry::new();
    // `Metrics::new` already hands back an `Arc<Metrics>`.
    let (metrics, _reporter) = metrics::Metrics::new(&registry);
    let mut rep = wide_repairer(keys[0].name, &committee).with_metrics(metrics.clone());

    let ceiling = || {
        registry
            .gather()
            .iter()
            .find(|f| f.get_name() == "vantage_repair_emit_ceiling")
            .and_then(|f| {
                f.get_metric()
                    .first()
                    .map(|m| m.get_gauge().get_value() as usize)
            })
            .unwrap_or(0)
    };

    // Clean ticks: the ceiling climbs, so a node with headroom is allowed to use it.
    rep.retry_requests();
    assert_eq!(
        ceiling(),
        RECOVERY_EMIT_START * 2,
        "a clean tick must loosen"
    );
    for _ in 0..20 {
        rep.retry_requests();
    }
    assert_eq!(
        ceiling(),
        RECOVERY_EMIT_MAX,
        "clean ticks must reach the cap, not overshoot"
    );

    // A tick that saw new drops halves it -- reacting to the DELTA, not the total.
    metrics.vantage_bulk_inbound_dropped_total.inc_by(1);
    rep.retry_requests();
    assert_eq!(ceiling(), RECOVERY_EMIT_MAX / 2, "new drops must back off");

    // No further drops: the same total must not keep throttling (the absolute counter never
    // decreases, so reacting to it would pin a node down forever after one bad moment).
    rep.retry_requests();
    assert_eq!(
        ceiling(),
        RECOVERY_EMIT_MAX,
        "a stale drop total must not keep throttling"
    );

    // Sustained drops floor out rather than reaching zero: congestion control must never
    // starve recovery completely.
    for _ in 0..40 {
        metrics.vantage_bulk_inbound_dropped_total.inc_by(1);
        rep.retry_requests();
    }
    assert_eq!(
        ceiling(),
        RECOVERY_EMIT_MIN,
        "must floor, never starve to zero"
    );
}

/// A deep-but-not-overflowing core queue must NOT throttle recovery -- the `adc6048`
/// regression, reproduced exactly.
///
/// Measured on the 2026-08-07 n=100 run: all four degraded nodes sat at `emit_ceiling` =
/// `RECOVERY_EMIT_MIN` deferring 38-61% of their repair demand (deferred 282-299/s against
/// requested 192-245/s) while `vantage_bulk_inbound_dropped_total` was ZERO on all 100 nodes.
/// The main queue really was deep -- but from availability crediting (128,942 refs/s), which
/// repair cannot influence, against repair's own 193 requests/s. Backing off freed nothing and
/// left a ~5,000-block gap unable to close.
///
/// So a busy main queue must be ignored by this loop, and only NEAR-OVERFLOW (where consensus
/// traffic is about to be shed) may back it off. The escalation-width cap keeps the lower
/// threshold, because the duplicates IT suppresses genuinely are repair's own doing.
#[tokio::test]
async fn a_busy_core_queue_does_not_throttle_recovery_but_near_overflow_does() {
    use crate::vantage::repair::{
        CORE_QUEUE_CONGESTED, CORE_QUEUE_NEAR_OVERFLOW, RECOVERY_EMIT_MAX, RECOVERY_EMIT_MIN,
    };
    let (committee, keys) = Committee::local_benchmark(10, 1, 33_100);
    let registry = prometheus::Registry::new();
    let (metrics, _reporter) = metrics::Metrics::new(&registry);
    let mut rep = wide_repairer(keys[0].name, &committee).with_metrics(metrics.clone());

    let ceiling = || {
        registry
            .gather()
            .iter()
            .find(|f| f.get_name() == "vantage_repair_emit_ceiling")
            .and_then(|f| {
                f.get_metric()
                    .first()
                    .map(|m| m.get_gauge().get_value() as usize)
            })
            .unwrap_or(0)
    };

    // The adc6048 state: queue deep enough to have tripped the old threshold, zero drops.
    rep.observe_core_queue(CORE_QUEUE_CONGESTED);
    for _ in 0..24 {
        rep.retry_requests();
    }
    assert_eq!(
        ceiling(),
        RECOVERY_EMIT_MAX,
        "a busy-but-not-overflowing queue must not throttle: repair is ~0.15% of that load, \
         so backing off cannot drain it and only blocks recovery"
    );

    // Near overflow, consensus traffic is about to be shed -- back off whatever the cause.
    rep.observe_core_queue(CORE_QUEUE_NEAR_OVERFLOW);
    for _ in 0..40 {
        rep.retry_requests();
    }
    assert_eq!(
        ceiling(),
        RECOVERY_EMIT_MIN,
        "the near-overflow backstop must still engage"
    );

    // And it must recover once the queue drains, or one bad moment pins the node forever.
    rep.observe_core_queue(0);
    for _ in 0..24 {
        rep.retry_requests();
    }
    assert_eq!(
        ceiling(),
        RECOVERY_EMIT_MAX,
        "backstop must release when the queue drains"
    );
}

/// The IN-FLIGHT window bounds inbound, which a rate limit alone does not.
///
/// Measured on the 2026-08-07 n=100 run: 3,420 outstanding digests asked of ~49 peers each
/// = ~167,000 invited answers, each costing a block_ok verify, a cache insert and a settle
/// walk on the single-threaded core. That pinned `core_queue_length` at its 1000-slot cap
/// while `bulk_inbound_dropped` read 0 -- the flood was on the MAIN queue, and self-invited.
/// An answer can only arrive for something we asked for, so capping outstanding asks caps
/// arrivals.
///
/// Also pins the release path: every request counted into the window must be released when
/// its digest arrives, or the window latches shut and the node stops asking forever.
#[tokio::test]
async fn in_flight_window_caps_outstanding_requests_and_is_released_on_arrival() {
    use crate::vantage::repair::RECOVERY_IN_FLIGHT_MAX;
    let (committee, keys) = Committee::local_benchmark(10, 1, 34_000);
    let (watcher, author) = (keys[0].name, keys[1].name);
    let sid = session_id(&committee);
    let genesis = genesis_digest(&sid);
    let mut rep = wide_repairer(watcher, &committee);

    // Ask for many distinct digests; emission must stop at the window.
    let mut total = 0usize;
    for k in 1..=400u64 {
        total += requests_for(&rep.authorize((author, k, Digest([(k % 251) as u8; 32])))).len();
    }
    assert!(
        total <= RECOVERY_IN_FLIGHT_MAX,
        "emitted {total} requests, window is {RECOVERY_IN_FLIGHT_MAX}"
    );

    // A real arrival must return slots to the window, or recovery deadlocks. Fresh repairer:
    // the loop above deliberately saturated the window, so a request made now would emit
    // nothing and there would be nothing to release -- which would make this pass vacuously.
    let mut rep2 = wide_repairer(watcher, &committee);
    let h1 = Header::new_vantage(author, 1, BTreeMap::new(), genesis, sid);
    rep2.authorize((author, 1, h1.id.clone()));
    let before = rep2.in_flight_for_test();
    assert_eq!(
        before, FANOUT_FIRST,
        "the first round must occupy the window"
    );
    rep2.on_serve(h1.clone());
    assert_eq!(
        rep2.in_flight_for_test(),
        0,
        "arrival must release every slot the digest held, or the window latches shut"
    );
}

/// Under congestion, one digest must not be escalated to the whole committee. Widening to
/// ~49 peers (the measured value) invites 49 copies of the same block into the queue that is
/// already the bottleneck.
#[tokio::test]
async fn escalation_width_is_capped_while_the_core_queue_is_congested() {
    use crate::vantage::repair::{CORE_QUEUE_CONGESTED, ESCALATE_WIDTH_MAX};
    let (committee, keys) = Committee::local_benchmark(20, 1, 35_000);
    let (watcher, author) = (keys[0].name, keys[1].name);
    let n_peers = committee.others_primaries(&watcher).len();
    assert!(n_peers > ESCALATE_WIDTH_MAX, "fixture must exceed the cap");
    let mut rep = wide_repairer(watcher, &committee);
    let h = Digest([5u8; 32]);

    rep.observe_core_queue(CORE_QUEUE_CONGESTED);
    let mut asked = requests_for(&rep.authorize((author, 5u64, h.clone())));
    for _ in 0..10 {
        asked.extend(requests_for(&rep.retry_requests()));
    }
    let distinct: std::collections::HashSet<_> = asked.iter().map(|(p, _)| *p).collect();
    assert!(
        distinct.len() <= ESCALATE_WIDTH_MAX,
        "congested: asked {} peers, cap is {ESCALATE_WIDTH_MAX}",
        distinct.len()
    );

    // Once the queue drains, coverage must resume widening -- the cap delays N6's eventual
    // coverage, it must not abandon it.
    rep.observe_core_queue(0);
    for _ in 0..10 {
        asked.extend(requests_for(&rep.retry_requests()));
    }
    let distinct: std::collections::HashSet<_> = asked.iter().map(|(p, _)| *p).collect();
    assert_eq!(
        distinct.len(),
        n_peers,
        "coverage must resume to every peer once uncongested"
    );
}

/// The in-flight window must NOT be an absorbing state: a round that is never answered has
/// its slots reclaimed after `ASK_TIMEOUT_TICKS`, so repair resumes.
///
/// Without the reclaim, `room == 0` is terminal. A slot frees only when the digest arrives or
/// the digest retires, and retirement needs emission progress, which needs `room > 0` -- so
/// `RECOVERY_IN_FLIGHT_MAX` asks that will never be answered halt repair on that node
/// forever. That is reachable with NO adversary: `Effect::RequestTo` rides `HeadersRequest`,
/// which `Inbound::is_bulk` sends to the bulk queue where a full channel `try_send`-DROPS it
/// silently, and N6 forbids ever re-asking that peer -- so each drop permanently burns a
/// `(peer, digest)` pair.
///
/// Pins the three properties that make the reclaim safe rather than a retransmission:
/// coverage (`asked`) never shrinks, N6's `requested` set is untouched, and the freed slots
/// become usable.
#[tokio::test]
async fn unanswered_asks_are_reclaimed_so_the_window_cannot_latch_shut() {
    use crate::vantage::repair::{ASK_TIMEOUT_TICKS, RECOVERY_IN_FLIGHT_MAX};
    let (committee, keys) = Committee::local_benchmark(20, 1, 33_700);
    let (watcher, author) = (keys[0].name, keys[1].name);
    let registry = prometheus::Registry::new();
    let (metrics, _r) = metrics::Metrics::new(&registry);
    let mut rep = wide_repairer(watcher, &committee).with_metrics(metrics.clone());

    // Fill the window with asks nobody will ever answer.
    let mut h = 0u8;
    while rep.in_flight_for_test() < RECOVERY_IN_FLIGHT_MAX && h < 250 {
        rep.authorize((author, 1_000 + h as u64, Digest([h; 32])));
        h += 1;
    }
    let filled = rep.in_flight_for_test();
    assert!(filled > 0, "fixture must actually put asks in flight");

    // Ticks before the deadline must NOT reclaim -- otherwise a slow-but-alive peer's
    // answer race would be cut short and the window would lose its closed-loop property.
    for _ in 0..(ASK_TIMEOUT_TICKS - 1) {
        let early = requests_for(&rep.retry_requests());
        assert!(
            early.is_empty(),
            "before the deadline the window is full, so no request may go out -- \
             emitting here would mean the window is not bounding at all"
        );
    }
    assert_eq!(
        rep.in_flight_for_test(),
        filled,
        "must not reclaim before ASK_TIMEOUT_TICKS"
    );

    // At the deadline the slots come back AND are immediately reused. Note what must NOT
    // be asserted here: that `in_flight` drops. The reclaim frees slots and the very same
    // tick spends them on peers not yet asked, so a saturated window legitimately reads
    // saturated again afterwards -- the observable recovery is that requests flow at all,
    // which is precisely what the absorbing state prevented.
    let resumed = requests_for(&rep.retry_requests());
    assert!(
        !resumed.is_empty(),
        "after the timeout the node must be able to ask again; it asked nobody, so the \
         window is still latched shut"
    );

    // N6 intact: reclaiming is window accounting, NOT a retransmission. Every (peer, digest)
    // ever asked must still be recorded, so no peer is ever asked twice.
    let peers = committee.others_primaries(&watcher).len();
    assert!(peers > 0);
    let mut still_recorded = 0;
    for (i, k) in keys.iter().enumerate() {
        if k.name == watcher {
            continue;
        }
        for d in 0..h {
            if rep.was_requested(&k.name, &Digest([d; 32])) {
                still_recorded += 1;
            }
        }
        let _ = i;
    }
    assert!(
        still_recorded > 0,
        "the requested set must survive a reclaim, or N6 is violated"
    );

    // And the reclaim is observable, so a run can tell asks are being burned.
    let reclaimed = registry
        .gather()
        .iter()
        .find(|f| f.get_name() == "vantage_repair_asks_reclaimed_total")
        .map(|f| f.get_metric()[0].get_counter().get_value())
        .unwrap_or(0.0);
    assert!(reclaimed > 0.0, "reclaim must be counted");
}

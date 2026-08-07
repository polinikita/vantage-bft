// PHASE3-SPEC.md §7 "Repair (N6/N7)".
use super::common::*;
use crate::messages::Header;
use crate::vantage::block::{genesis_digest, session_id};
use crate::vantage::lanes::BlockCache;
use crate::vantage::repair::Repairer;
use crate::vantage::Effect;
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

/// n=100 straggler fix (2026-08-08): `settle`'s peer fan-out is gated on
/// `requested_hashes.insert(h)` rather than run unconditionally. The gate is only sound
/// because `requested_hashes.contains(h)` implies `requested` already holds `(p, h)`
/// for EVERY other primary -- if some path ever inserted into `requested_hashes`
/// without also filling `requested`, the gate would silently suppress real requests and
/// the block would never be asked for from anyone.
///
/// This pins that invariant directly, so such a path cannot be added unnoticed.
#[tokio::test]
async fn requested_hashes_membership_implies_every_peer_was_requested() {
    let name = crate::common::keys()[0].0;
    let mut repairer = new_standalone_repairer(name);
    let committee = test_committee();
    let author = crate::common::keys()[1].0;
    let header = Header::default();
    let h = header.id.clone();

    let effects = repairer.authorize((author, 1, h.clone()));
    let requested = requests_for(&effects);

    // One request per OTHER primary, exactly once each, all for this digest.
    let peers: Vec<PublicKey> = committee
        .others_primaries(&name)
        .into_iter()
        .map(|(pk, _)| pk)
        .collect();
    assert_eq!(requested.len(), peers.len());
    for peer in &peers {
        assert!(
            requested.contains(&(*peer, h.clone())),
            "peer {peer} was not asked for the missing digest"
        );
    }
    assert!(repairer.was_requested_hash(&h), "the hash gate must be set");
    for peer in &peers {
        assert!(
            repairer.was_requested(peer, &h),
            "requested_hashes is set but peer {peer} is missing from `requested` -- \
             settle's gate would now suppress this peer's request forever"
        );
    }

    // Re-authorizing the same still-missing ref emits nothing (the gate short-circuits),
    // which is the property the fix relies on for its ~99x cost reduction.
    let again = repairer.authorize((author, 1, h.clone()));
    assert!(
        requests_for(&again).is_empty(),
        "a repeat authorize on a still-missing digest must emit no new requests"
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

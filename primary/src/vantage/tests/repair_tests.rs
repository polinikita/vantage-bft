// PHASE3-SPEC.md §7 "Repair (N6/N7)".
use super::common::*;
use crate::messages::Header;
use crate::vantage::block::{genesis_digest, session_id};
use crate::vantage::lanes::BlockCache;
use crate::vantage::repair::Repairer;
use crate::vantage::Effect;
use crypto::{Digest, PublicKey};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

fn new_standalone_repairer(name: PublicKey) -> Repairer {
    let committee = test_committee();
    let sid = session_id(&committee);
    let genesis = genesis_digest(&sid);
    Repairer::new(name, committee, sid, genesis, MAX_BLOCK_PAYLOAD, Arc::new(Mutex::new(BlockCache::new())))
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
    assert!(effects.iter().all(|e| !matches!(e, Effect::RequestTo(_, _))));

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
        effects.iter().filter(|e| matches!(e, Effect::ServeTo(p, _) if *p == requester)).count(),
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
    assert_eq!(effects.iter().filter(|e| matches!(e, Effect::ServeTo(p, _) if *p == requester)).count(), 1);
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

fn repairer_blocks(repairer: &Repairer) -> std::sync::MutexGuard<'_, BlockCache> {
    // `Repairer` doesn't expose its `SharedBlocks` field directly (it isn't needed in
    // production code, where `LaneManager` owns the canonical handle); tests construct
    // repairers standalone, so they need a matching accessor.
    repairer.blocks_for_test()
}

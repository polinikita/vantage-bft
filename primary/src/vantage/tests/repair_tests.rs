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

    let effects = repairer.on_serve(block);
    assert!(effects.is_empty());
    assert!(!repairer_blocks(&repairer).contains(&h));
    assert_eq!(repairer.requested_count(), 0);
}

#[test]
fn sequence_serve_uses_its_own_request_authorization() {
    let all = authors();
    let (watcher, _) = all[0];
    let (author, _) = all[1];
    let mut repairer = new_standalone_repairer(watcher);
    let sid = session_id(&test_committee());
    let genesis = genesis_digest(&sid);
    let block = Header::new_vantage(author, 1, BTreeMap::new(), genesis, sid);
    let digest = block.id.clone();
    let mut corrupted = block.clone();
    corrupted.id = Digest([0xFE; 32]);

    let effects = repairer.on_sequence_serve(block);

    assert!(effects
        .iter()
        .any(|effect| matches!(effect, Effect::BlockCached(d) if d == &digest)));
    assert!(repairer_blocks(&repairer).contains(&digest));
    assert!(repairer.on_sequence_serve(corrupted.clone()).is_empty());
    assert!(!repairer_blocks(&repairer).contains(&corrupted.id));
}

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
    assert_eq!(requests_for(&effects).len(), n_peers);

    let effects = repairer.on_serve(h3.clone());
    assert!(requests_for(&effects).iter().all(|(_, h)| h == &h2.id));
    assert_eq!(requests_for(&effects).len(), n_peers);

    let effects = repairer.on_serve(h2.clone());
    assert!(requests_for(&effects).iter().all(|(_, h)| h == &h1.id));
    assert_eq!(requests_for(&effects).len(), n_peers);

    let effects = repairer.on_serve(h1.clone());
    assert!(effects.iter().all(|e| !matches!(e, Effect::ServeTo(_, _))));

    let blocks = repairer_blocks(&repairer);
    assert!(blocks.get(&h1.id).unwrap().retained);
    assert!(blocks.get(&h2.id).unwrap().retained);
    assert!(blocks.get(&h3.id).unwrap().retained);
}

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

    let fake_ref = (fake_author, 7u64, h.clone());
    repairer.authorize(fake_ref.clone());
    let n_peers = test_committee().others_primaries(&watcher).len();
    assert_eq!(repairer.requested_count(), n_peers);

    let effects = repairer.on_serve(block.clone());
    assert!(repairer_blocks(&repairer).contains(&h));
    assert!(effects
        .iter()
        .all(|e| !matches!(e, Effect::RequestTo(_, _))));

    let real_ref = (real_author, 1u64, h);
    let before = repairer.requested_count();
    let effects = repairer.authorize(real_ref);
    assert_eq!(repairer.requested_count(), before);
    assert!(requests_for(&effects).is_empty());
}

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
    block.id = corrupted_id.clone();

    repairer.authorize((author, 1, corrupted_id.clone()));
    let effects = repairer.on_serve(block);
    assert!(effects.is_empty());
    assert!(!repairer_blocks(&repairer).contains(&corrupted_id));
}

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

    let effects = repairer.on_request(requester, h.clone());
    assert!(effects.is_empty());

    repairer.authorize((author, 1, h.clone()));
    let effects = repairer.on_serve(block);
    assert_eq!(
        effects
            .iter()
            .filter(|e| matches!(e, Effect::ServeTo(p, _) if *p == requester))
            .count(),
        1
    );

    let effects = repairer.on_request(requester, h);
    assert!(effects.iter().all(|e| !matches!(e, Effect::ServeTo(_, _))));
}

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

    let effects = repairer.on_request(requester, h.clone());
    assert_eq!(
        effects
            .iter()
            .filter(|e| matches!(e, Effect::ServeTo(p, _) if *p == requester))
            .count(),
        1
    );
}

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
    repairer.blocks_for_test()
}

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

    assert_eq!(requested.len(), FANOUT_FIRST);
    assert!(requested.len() < peers.len());
    assert_eq!(repairer.fanout_asked_for_test(&h), Some(FANOUT_FIRST));
    assert!(
        repairer.is_escalating_for_test(&h),
        "gated with partial coverage but NOT queued to widen -- the remaining peers \
         would never be asked and N6's eventual guarantee would be lost"
    );

    let again = repairer.authorize((author, 1, h.clone()));
    assert!(
        requests_for(&again).is_empty(),
        "a repeat authorize on a still-missing digest must emit no new requests"
    );

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
    let effects = repairer.on_block_available(unrelated.id.clone());
    assert!(effects.is_empty());
    assert_eq!(
        repairer.settle_calls_for_test(),
        before,
        "an arrival with an empty wait-bucket must not call settle at all"
    );
}

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

    let l1h1 = Header::new_vantage(a1, 1, BTreeMap::new(), genesis.clone(), sid.clone());
    let l1h2 = Header::new_vantage(a1, 2, BTreeMap::new(), l1h1.id.clone(), sid.clone());
    let l2h1 = Header::new_vantage(a2, 1, BTreeMap::new(), genesis, sid.clone());
    let l2h2 = Header::new_vantage(a2, 2, BTreeMap::new(), l2h1.id.clone(), sid);

    repairer.authorize((a1, 2, l1h2.id.clone()));
    repairer.authorize((a2, 2, l2h2.id.clone()));

    let effects = repairer.on_serve(l1h2.clone());
    let reqs = requests_for(&effects);
    assert_eq!(reqs.len(), n_peers);
    assert!(
        reqs.iter().all(|(_, h)| h == &l1h1.id),
        "lane 2 must not have been touched by lane 1's arrival"
    );

    let effects = repairer.on_serve(l2h2.clone());
    let reqs = requests_for(&effects);
    assert_eq!(reqs.len(), n_peers);
    assert!(reqs.iter().all(|(_, h)| h == &l2h1.id));
}

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

    repairer.on_serve(h2.clone());
    repairer.on_serve(h1.clone());
    assert!(
        repairer.is_settled(&r3),
        "the chain must still settle end to end"
    );
    assert_eq!(repairer.blocked_on_len_for_test(&h1.id), 0);
    assert_eq!(repairer.blocked_on_len_for_test(&h2.id), 0);
}

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

#[tokio::test]
async fn fanout_escalates_to_every_peer_without_repeating_one() {
    let (committee, keys) = Committee::local_benchmark(10, 1, 22_000);
    let (watcher, author) = (keys[0].name, keys[1].name);
    let n_peers = committee.others_primaries(&watcher).len();
    let mut rep = wide_repairer(watcher, &committee);
    let h = Digest([7u8; 32]);

    let mut asked = requests_for(&rep.authorize((author, 5u64, h.clone())));
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

#[tokio::test]
async fn fanout_escalates_the_lowest_height_first() {
    let (committee, keys) = Committee::local_benchmark(10, 1, 25_000);
    let (watcher, author) = (keys[0].name, keys[1].name);
    let mut rep = wide_repairer(watcher, &committee);

    let (low, high) = (Digest([1u8; 32]), Digest([2u8; 32]));
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

#[tokio::test]
async fn first_round_prefers_peers_known_to_hold_the_lane() {
    let (committee, keys) = Committee::local_benchmark(10, 1, 27_000);
    let (watcher, author) = (keys[0].name, keys[1].name);
    let mut rep = wide_repairer(watcher, &committee);

    let holders = [keys[6].name, keys[7].name, keys[8].name, keys[9].name];
    assert_eq!(holders.len(), FANOUT_FIRST);
    for (i, p) in holders.iter().enumerate() {
        rep.note_holder(*p, author, 5 + i as u64);
    }
    rep.note_holder(keys[5].name, author, 4);

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

#[tokio::test]
async fn note_holder_never_regresses() {
    let (committee, keys) = Committee::local_benchmark(10, 1, 29_000);
    let (watcher, author) = (keys[0].name, keys[1].name);
    let mut rep = wide_repairer(watcher, &committee);

    rep.note_holder(keys[5].name, author, 50);
    rep.note_holder(keys[5].name, author, 3);

    let asked = requests_for(&rep.authorize((author, 40u64, Digest([1u8; 32]))));
    assert!(
        asked.iter().any(|(p, _)| *p == keys[5].name),
        "a stale lower credit regressed the confirmed height"
    );
}

#[tokio::test]
async fn recovery_budget_defers_requests_instead_of_dropping_them() {
    use crate::vantage::repair::RECOVERY_EMIT_START;
    let (committee, keys) = Committee::local_benchmark(10, 1, 30_000);
    let (watcher, author) = (keys[0].name, keys[1].name);
    let mut rep = wide_repairer(watcher, &committee);

    let mut emitted = 0usize;
    let mut h = 0u8;
    while emitted < RECOVERY_EMIT_START && h < 255 {
        let d = Digest([h; 32]);
        emitted += requests_for(&rep.authorize((author, 1 + h as u64, d))).len();
        h += 1;
    }
    assert!(emitted <= RECOVERY_EMIT_START, "budget must cap emission");

    let starved = Digest([0xAB; 32]);
    let none = requests_for(&rep.authorize((author, 900u64, starved.clone())));
    if emitted >= RECOVERY_EMIT_START {
        assert!(none.is_empty(), "budget spent -- nothing may be emitted");
        assert!(
            !rep.was_requested_hash(&starved),
            "a digest we never actually asked for must not pass the serve gate"
        );
        let after = requests_for(&rep.retry_requests());
        assert!(
            !after.is_empty(),
            "the next tick must pick up work deferred by the budget"
        );
    }
}

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

    for k in keys.iter().skip(1).take(n_peers - 1) {
        rep.note_holder(k.name, author, 500);
    }
    assert_eq!(
        rep.universally_held_below(&author),
        None,
        "one silent peer must pin the lane, not be treated as height 0"
    );
}

#[tokio::test]
async fn eviction_floor_is_the_slowest_peer_not_a_quorum() {
    let (committee, keys) = Committee::local_benchmark(10, 1, 32_000);
    let (watcher, author) = (keys[0].name, keys[1].name);
    let n_peers = committee.others_primaries(&watcher).len();
    let mut rep = wide_repairer(watcher, &committee);

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

#[tokio::test]
async fn recovery_ceiling_grows_when_clean_and_backs_off_on_drops() {
    use crate::vantage::repair::{RECOVERY_EMIT_MAX, RECOVERY_EMIT_MIN, RECOVERY_EMIT_START};
    let (committee, keys) = Committee::local_benchmark(10, 1, 33_000);
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

    metrics.vantage_bulk_inbound_dropped_total.inc_by(1);
    rep.retry_requests();
    assert_eq!(ceiling(), RECOVERY_EMIT_MAX / 2, "new drops must back off");

    rep.retry_requests();
    assert_eq!(
        ceiling(),
        RECOVERY_EMIT_MAX,
        "a stale drop total must not keep throttling"
    );

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

#[tokio::test]
async fn sustained_core_queue_pressure_does_not_throttle_recovery_ceiling() {
    use crate::vantage::repair::RECOVERY_EMIT_MAX;
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

    rep.observe_core_queue(usize::MAX);
    for _ in 0..24 {
        rep.retry_requests();
    }
    assert_eq!(
        ceiling(),
        RECOVERY_EMIT_MAX,
        "core-queue pressure is not an attributed emit-ceiling signal in this arm"
    );
    assert_eq!(
        metrics.vantage_repair_ceiling_halved_by_queue.get(),
        0,
        "legacy counter must prove queue backoff is disabled"
    );

    metrics.vantage_bulk_inbound_dropped_total.inc();
    rep.retry_requests();
    assert_eq!(
        ceiling(),
        RECOVERY_EMIT_MAX / 2,
        "a new bulk drop must remain the backoff signal"
    );

    rep.retry_requests();
    assert_eq!(
        ceiling(),
        RECOVERY_EMIT_MAX,
        "the queue must not latch recovery"
    );
}

#[tokio::test]
async fn in_flight_window_caps_outstanding_requests_and_is_released_on_arrival() {
    use crate::vantage::repair::RECOVERY_IN_FLIGHT_MAX;
    let (committee, keys) = Committee::local_benchmark(10, 1, 34_000);
    let (watcher, author) = (keys[0].name, keys[1].name);
    let sid = session_id(&committee);
    let genesis = genesis_digest(&sid);
    let mut rep = wide_repairer(watcher, &committee);

    let mut total = 0usize;
    for k in 1..=400u64 {
        total += requests_for(&rep.authorize((author, k, Digest([(k % 251) as u8; 32])))).len();
    }
    assert!(
        total <= RECOVERY_IN_FLIGHT_MAX,
        "emitted {total} requests, window is {RECOVERY_IN_FLIGHT_MAX}"
    );

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

#[tokio::test]
async fn unanswered_asks_are_reclaimed_so_the_window_cannot_latch_shut() {
    use crate::vantage::repair::{ASK_TIMEOUT_TICKS, RECOVERY_IN_FLIGHT_MAX};
    let (committee, keys) = Committee::local_benchmark(20, 1, 33_700);
    let (watcher, author) = (keys[0].name, keys[1].name);
    let registry = prometheus::Registry::new();
    let (metrics, _r) = metrics::Metrics::new(&registry);
    let mut rep = wide_repairer(watcher, &committee).with_metrics(metrics.clone());

    let mut h = 0u8;
    while rep.in_flight_for_test() < RECOVERY_IN_FLIGHT_MAX && h < 250 {
        rep.authorize((author, 1_000 + h as u64, Digest([h; 32])));
        h += 1;
    }
    let filled = rep.in_flight_for_test();
    assert!(filled > 0, "fixture must actually put asks in flight");

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

    let resumed = requests_for(&rep.retry_requests());
    assert!(
        !resumed.is_empty(),
        "after the timeout the node must be able to ask again; it asked nobody, so the \
         window is still latched shut"
    );

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

    let reclaimed = registry
        .gather()
        .iter()
        .find(|f| f.get_name() == "vantage_repair_asks_reclaimed_total")
        .map(|f| f.get_metric()[0].get_counter().get_value())
        .unwrap_or(0.0);
    assert!(reclaimed > 0.0, "reclaim must be counted");
}

use crate::vantage::agb::{ResolutionEntry, ViewProposal};
use crate::vantage::claim::{manifest_refs, AvailClaim, ClaimRef, ShortClaim};
use crate::vantage::BlockRef;
use crypto::{Digest, PublicKey};

fn key(b: u8) -> PublicKey {
    let mut raw = [0u8; 32];
    raw[0] = b;
    PublicKey(raw)
}

fn dig(b: u8) -> Digest {
    Digest([b; 32])
}

fn r(author: u8, height: u64, d: u8) -> BlockRef {
    (key(author), height, dig(d))
}

fn proposal(c: Vec<BlockRef>, t: Vec<BlockRef>, m: Option<ResolutionEntry>) -> ViewProposal {
    ViewProposal { view: 7, c, t, m }
}

#[test]
fn manifest_refs_orders_c_then_t_then_m() {
    let p = proposal(
        vec![r(1, 10, 1)],
        vec![r(2, 20, 2)],
        Some(ResolutionEntry::Full(
            5,
            vec![r(3, 30, 3)],
            vec![r(4, 40, 4)],
        )),
    );
    let refs = manifest_refs(&p);
    assert_eq!(
        refs.iter().map(|x| x.1).collect::<Vec<_>>(),
        vec![10, 20, 30, 40],
        "order must be C, T, then M's C, then M's T"
    );
}

#[test]
fn manifest_refs_skip_entry_contributes_no_lanes() {
    let base = proposal(vec![r(1, 10, 1)], vec![r(2, 20, 2)], None);
    let skip = proposal(
        vec![r(1, 10, 1)],
        vec![r(2, 20, 2)],
        Some(ResolutionEntry::Skip(5)),
    );
    assert_eq!(manifest_refs(&base).len(), 2);
    assert_eq!(
        manifest_refs(&skip).len(),
        2,
        "a Skip entry must not shift lane indices"
    );
}

#[test]
fn statements_map_at_tip_and_short_claims_to_anchors() {
    let p = proposal(vec![r(1, 10, 1), r(2, 20, 2)], vec![r(3, 30, 3)], None);
    let refs = manifest_refs(&p);
    let mut claim = AvailClaim::with_capacity(refs.len());
    claim.set_at_tip(0);
    assert!(claim.push_short(2, 4));

    let got = claim.statements(&refs);
    assert_eq!(got.len(), 2);
    assert_eq!(
        got[0],
        ClaimRef::Exact(r(1, 10, 1)),
        "at-tip is the entry verbatim"
    );
    assert_eq!(
        got[1],
        ClaimRef::Ancestor {
            anchor: r(3, 30, 3),
            delta: 4,
        },
        "short claim carries only its proposal anchor and distance"
    );
}

#[test]
fn statements_drop_ill_formed_claims() {
    let p = proposal(vec![r(1, 10, 1)], vec![], None);
    let refs = manifest_refs(&p);

    let mut over = AvailClaim::with_capacity(refs.len());
    over.short.push(ShortClaim { lane: 0, delta: 10 });
    assert!(
        over.statements(&refs).is_empty(),
        "delta == height must be dropped, not wrapped"
    );
    let mut past = AvailClaim::with_capacity(refs.len());
    past.short.push(ShortClaim { lane: 0, delta: 99 });
    assert!(
        past.statements(&refs).is_empty(),
        "delta > height must be dropped"
    );

    let mut oob = AvailClaim::with_capacity(refs.len());
    oob.short.push(ShortClaim { lane: 7, delta: 1 });
    assert!(
        oob.statements(&refs).is_empty(),
        "out-of-range lane must be dropped"
    );

    let mut both = AvailClaim::with_capacity(refs.len());
    both.set_at_tip(0);
    both.short.push(ShortClaim { lane: 0, delta: 1 });
    let got = both.statements(&refs);
    assert_eq!(
        got.len(),
        1,
        "a doubly-claimed lane must yield one reference"
    );
    assert_eq!(got[0], ClaimRef::Exact(r(1, 10, 1)));
}

#[test]
fn statements_reject_oversized_bitmaps_and_sparse_vectors() {
    let p = proposal(vec![r(1, 10, 1)], vec![], None);
    let refs = manifest_refs(&p);

    let extra_word = AvailClaim {
        at_tip: vec![1, 0],
        short: Vec::new(),
    };
    assert!(extra_word.statements(&refs).is_empty());

    let unused_bit = AvailClaim {
        at_tip: vec![1 << 63],
        short: Vec::new(),
    };
    assert!(unused_bit.statements(&refs).is_empty());

    let too_many_sparse = AvailClaim {
        at_tip: vec![0],
        short: vec![
            ShortClaim { lane: 0, delta: 1 },
            ShortClaim { lane: 0, delta: 2 },
        ],
    };
    assert!(too_many_sparse.statements(&refs).is_empty());
}

#[test]
fn push_short_rejects_contradictory_and_zero_claims() {
    let mut c = AvailClaim::with_capacity(4);
    c.set_at_tip(1);
    assert!(!c.push_short(1, 3), "cannot be at-tip AND short");
    assert!(!c.push_short(2, 0), "delta 0 is at-tip, not short");
    assert!(c.push_short(2, 3));
    assert!(!c.push_short(2, 4), "one integer per lane position");
    assert_eq!(c.claimed(), 2);
}

#[test]
fn bitmap_addresses_every_lane_across_word_boundaries() {
    let n = 200;
    let mut c = AvailClaim::with_capacity(n);
    assert_eq!(c.at_tip.len(), 4, "200 lanes need ceil(200/64) = 4 words");
    for lane in [0usize, 63, 64, 127, 128, 199] {
        c.set_at_tip(lane);
    }
    for lane in 0..n {
        let expect = [0usize, 63, 64, 127, 128, 199].contains(&lane);
        assert_eq!(c.is_at_tip(lane), expect, "lane {lane}");
    }
    assert_eq!(c.claimed(), 6);
}

#[test]
fn encoded_claim_is_tens_of_bytes_not_kilobytes() {
    let mut c = AvailClaim::with_capacity(200);
    for lane in 0..200 {
        c.set_at_tip(lane);
    }
    let bytes = bincode::serialize(&c).expect("AvailClaim serializes");
    assert!(
        bytes.len() < 64,
        "200 fully-claimed lanes took {} B; a reference-per-lane encoding would be \
         200 * 72 = 14,400 B",
        bytes.len()
    );

    c.push_short(0, 0); // A zero delta is invalid.
    let mut d = AvailClaim::with_capacity(8);
    d.set_at_tip(3);
    assert!(d.push_short(5, 2));
    let back: AvailClaim = bincode::deserialize(&bincode::serialize(&d).expect("ser")).expect("de");
    assert_eq!(back, d, "wire round-trip must be exact");
}

use super::common::*;
use crate::vantage::avail::AvailResolver;

fn statements(p: &ViewProposal, c: &AvailClaim) -> Vec<ClaimRef> {
    let refs = manifest_refs(p);
    c.statements(&refs)
}

#[tokio::test]
async fn sender_claims_at_tip_short_and_stays_silent() {
    let a = authors();
    let (me, held, partial, unknown) = (a[0].0, a[1].0, a[2].0, a[3].0);
    let (mut lm, _s) = new_lane_manager(me, ".db_test_claim_sender");

    let held_chain = direct_chain(&mut lm, held, 3).await;
    let mut partial_chain = direct_chain(&mut lm, partial, 2).await;
    let mut parent = partial_chain[1].id.clone();
    for height in 3..=5 {
        let repaired = crate::messages::Header::new_vantage(
            partial,
            height,
            std::collections::BTreeMap::new(),
            parent,
            lm.sid().clone(),
        );
        lm.process_publish(me, repaired.clone()).await;
        parent = repaired.id.clone();
        partial_chain.push(repaired);
    }

    let p = proposal(
        vec![
            (held, 3, held_chain[2].id.clone()),
            (partial, 5, partial_chain[4].id.clone()),
            (unknown, 4, dig(201)),
        ],
        vec![],
        None,
    );
    let claim = lm.build_avail_claim(&p);

    assert!(
        claim.is_at_tip(0),
        "lane we hold at the named height => at-tip"
    );
    assert!(
        !claim.is_at_tip(1),
        "lane we hold only partly is not at-tip"
    );
    assert_eq!(claim.short.len(), 1, "exactly one short claim");
    assert_eq!(claim.short[0].lane, 1);
    assert_eq!(claim.short[0].delta, 3, "named 5, hold 2 => delta 3");
    assert_eq!(claim.claimed(), 2, "the unknown author must draw no claim");
}

#[tokio::test]
async fn only_an_exact_claim_quorum_promotes_a_core_candidate() {
    use crate::vantage::lanes::AckAggregator;
    let a = authors();
    let (me, target) = (a[0].0, a[1].0);
    let (mut lm, _s) = new_lane_manager(me, ".db_test_claim_exact_core_promotion");
    let direct = direct_chain(&mut lm, target, 1).await;
    let target_ref = block_ref(&direct[0]);
    let repaired_child = crate::messages::Header::new_vantage(
        target,
        2,
        std::collections::BTreeMap::new(),
        target_ref.2.clone(),
        lm.sid().clone(),
    );
    lm.process_publish(me, repaired_child.clone()).await;
    let relative = ClaimRef::Ancestor {
        anchor: block_ref(&repaired_child),
        delta: 1,
    };

    let mut aggregate = AckAggregator::new(test_committee());
    aggregate.record_ack(me, target_ref.clone());
    for sender in [a[1].0, a[2].0] {
        let refs = lm.note_claim(sender, std::slice::from_ref(&relative));
        assert_eq!(refs, vec![target_ref.clone()]);
        for r in refs {
            if let Some(availability) = aggregate.record_ack(sender, r).availability {
                lm.process_claim_availability(availability);
            }
        }
    }
    assert!(lm.is_q_available(&target_ref, test_committee().quorum_threshold()));
    assert!(
        lm.c_candidate(&target).is_none(),
        "an integer-derived quorum is eventual evidence, not bounded-common core evidence"
    );
    assert_eq!(
        lm.confirmation_candidate(&target),
        Some(target_ref.clone()),
        "the same coordinate must remain available for an exact confirmation ECHO"
    );

    for sender in [a[1].0, a[2].0] {
        lm.note_claim(sender, &[ClaimRef::Exact(target_ref.clone())]);
    }
    assert!(
        lm.c_candidate(&target).is_none(),
        "direct receipt is not an exact ECHO claim from the local party"
    );
    lm.note_claim(me, &[ClaimRef::Exact(target_ref.clone())]);
    assert_eq!(
        lm.c_candidate(&target),
        Some(target_ref),
        "a quorum of exact-position claims becomes common within one delay"
    );
}

#[tokio::test]
async fn sender_stays_silent_on_a_forked_lane_at_the_named_height() {
    let a = authors();
    let (me, forker) = (a[0].0, a[1].0);
    let (mut lm, _s) = new_lane_manager(me, ".db_test_claim_fork");
    let ours = direct_chain(&mut lm, forker, 3).await;

    let p = proposal(vec![(forker, 3, dig(250))], vec![], None);
    let claim = lm.build_avail_claim(&p);
    assert_eq!(
        claim.claimed(),
        0,
        "we hold height 3 for this author, but on another chain ({:?}) -- no claim",
        ours[2].id
    );
}

fn resolver(lm: &crate::vantage::lanes::LaneManager) -> AvailResolver {
    AvailResolver::new(
        test_committee(),
        lm.sid().clone(),
        lm.genesis().clone(),
        MAX_BLOCK_PAYLOAD,
        lm.blocks_handle(),
    )
}

#[tokio::test]
async fn avail_height_is_the_quorum_order_statistic() {
    let a = authors();
    let (me, target) = (a[0].0, a[1].0);
    let (mut lm, _s) = new_lane_manager(me, ".db_test_claim_quorum");
    let chain = direct_chain(&mut lm, target, 5).await;
    let mut res = resolver(&lm);

    for (i, h) in [(0usize, 5u64), (1, 4), (2, 3), (3, 1)] {
        let p = proposal(
            vec![(target, h, chain[(h - 1) as usize].id.clone())],
            vec![],
            None,
        );
        let mut c = AvailClaim::with_capacity(1);
        c.set_at_tip(0);
        res.note_claim(a[i].0, &statements(&p, &c));
    }
    assert_eq!(
        res.avail_height(&target),
        3,
        "quorum height is the 2f+1-th largest claim"
    );
    assert_eq!(res.avail_height(&me), 0, "no claims for a lane => 0");
}

#[tokio::test]
async fn note_claim_is_monotone_per_sender() {
    let a = authors();
    let (me, target) = (a[0].0, a[1].0);
    let (mut lm, _s) = new_lane_manager(me, ".db_test_claim_monotone");
    let chain = direct_chain(&mut lm, target, 4).await;
    let mut res = resolver(&lm);

    let at = |h: u64| {
        let p = proposal(
            vec![(target, h, chain[(h - 1) as usize].id.clone())],
            vec![],
            None,
        );
        let mut c = AvailClaim::with_capacity(1);
        c.set_at_tip(0);
        (p, c)
    };
    let (p4, c4) = at(4);
    assert_eq!(
        res.note_claim(a[1].0, &statements(&p4, &c4))
            .references
            .len(),
        4,
        "a direct-prefix claim credits every newly derived ancestor"
    );
    let (p2, c2) = at(2);
    assert_eq!(
        res.note_claim(a[1].0, &statements(&p2, &c2)).references,
        vec![(target, 2, chain[1].id.clone())],
        "a distinct exact-position ACK counts even below the sender's watermark"
    );
    assert!(
        res.note_claim(a[1].0, &statements(&p4, &c4))
            .references
            .is_empty(),
        "an equal re-claim must be dropped"
    );
    assert_eq!(
        res.claimed_len_for_test(),
        1,
        "one entry per (author, sender)"
    );
    assert_eq!(res.avail_height(&target), 0, "one claim is not a quorum");
}

#[tokio::test]
async fn short_claims_need_verifiable_linkage() {
    let a = authors();
    let (me, target) = (a[0].0, a[1].0);
    let (mut lm, _s) = new_lane_manager(me, ".db_test_claim_linkage");
    let chain = direct_chain(&mut lm, target, 4).await;
    let mut res = resolver(&lm);

    let p = proposal(vec![(target, 4, chain[3].id.clone())], vec![], None);

    let mut good = AvailClaim::with_capacity(1);
    assert!(good.push_short(0, 2));
    let got = res.note_claim(a[1].0, &statements(&p, &good));
    assert_eq!(
        got.references,
        vec![
            (target, 1, chain[0].id.clone()),
            (target, 2, chain[1].id.clone()),
        ]
    );

    let mut bogus = AvailClaim::with_capacity(1);
    assert!(bogus.push_short(0, 4));
    assert!(
        res.note_claim(a[2].0, &statements(&p, &bogus))
            .references
            .is_empty(),
        "an invalid distance must be dropped"
    );
}

#[tokio::test]
async fn short_claim_waits_for_anchor_ancestry_then_derives_the_digest() {
    let a = authors();
    let (me, target) = (a[0].0, a[1].0);
    let (mut lm, _s) = new_lane_manager(me, ".db_test_claim_pending_ancestry");
    let mut parent = lm.genesis().clone();
    let mut chain = Vec::new();
    for height in 1..=4 {
        let header = crate::messages::Header::new_vantage(
            target,
            height,
            std::collections::BTreeMap::new(),
            parent,
            lm.sid().clone(),
        );
        parent = header.id.clone();
        chain.push(header);
    }
    let p = proposal(vec![(target, 4, chain[3].id.clone())], vec![], None);
    let mut claim = AvailClaim::with_capacity(1);
    assert!(claim.push_short(0, 2));
    let mut res = resolver(&lm);

    assert!(res
        .note_claim(a[2].0, &statements(&p, &claim))
        .references
        .is_empty());
    assert_eq!(res.pending_relative_len_for_test(), 1);

    for header in &chain {
        lm.process_publish(me, header.clone()).await;
    }
    let retried = res.retry_pending_avail(&chain[3].id);
    assert_eq!(
        retried,
        vec![
            (a[2].0, (target, 1, chain[0].id.clone())),
            (a[2].0, (target, 2, chain[1].id.clone())),
        ]
    );
    assert_eq!(res.pending_relative_len_for_test(), 0);
}

#[tokio::test]
async fn pending_sparse_claims_are_tuple_specific_across_anchors() {
    let a = authors();
    let (me, sender, target) = (a[0].0, a[1].0, a[2].0);
    let (lm, _s) = new_lane_manager(me, ".db_test_claim_pending_distinct");
    let mut res = resolver(&lm);

    for (height, digest) in [(4, 201), (5, 202)] {
        let p = proposal(vec![(target, height, dig(digest))], vec![], None);
        let mut claim = AvailClaim::with_capacity(1);
        assert!(claim.push_short(0, 1));
        assert!(res
            .note_claim(sender, &statements(&p, &claim))
            .references
            .is_empty());
    }
    assert_eq!(
        res.pending_relative_len_for_test(),
        2,
        "one unresolved fork must not overwrite another first-hand claim"
    );
}

#[tokio::test]
async fn sparse_target_counts_after_the_prefix_cursor_advanced_on_another_fork() {
    let a = authors();
    let (me, sender, target) = (a[0].0, a[1].0, a[2].0);
    let (mut lm, _s) = new_lane_manager(me, ".db_test_claim_sparse_cross_fork");
    let sid = lm.sid().clone();
    let genesis = lm.genesis().clone();
    let left1 = tagged_header(target, 1, genesis.clone(), sid.clone(), 1);
    let left2 = tagged_header(target, 2, left1.id.clone(), sid.clone(), 2);
    let right1 = tagged_header(target, 1, genesis, sid.clone(), 3);
    let right2 = tagged_header(target, 2, right1.id.clone(), sid, 4);
    for header in [&left1, &left2, &right1, &right2] {
        lm.process_publish(target, header.clone()).await;
    }

    let mut res = resolver(&lm);
    assert_eq!(
        res.note_claim(sender, &[ClaimRef::Exact(block_ref(&left2))])
            .references
            .len(),
        2
    );
    let relative = ClaimRef::Ancestor {
        anchor: block_ref(&right2),
        delta: 1,
    };
    assert_eq!(
        res.note_claim(sender, &[relative]).references,
        vec![block_ref(&right1)],
        "the monotone backfill cursor must not erase an exact tuple on another fork"
    );
}

#[tokio::test]
async fn short_claim_with_a_forked_anchor_is_not_retained_pending() {
    let a = authors();
    let (me, target) = (a[0].0, a[1].0);
    let (mut lm, _s) = new_lane_manager(me, ".db_test_claim_forked_anchor");
    let chain = direct_chain(&mut lm, target, 4).await;
    let p = proposal(vec![(target, 4, chain[2].id.clone())], vec![], None);
    let mut claim = AvailClaim::with_capacity(1);
    assert!(claim.push_short(0, 2));
    let mut res = resolver(&lm);

    assert!(res
        .note_claim(a[2].0, &statements(&p, &claim))
        .references
        .is_empty());
    assert_eq!(res.pending_relative_len_for_test(), 0);
}

#[tokio::test]
async fn note_claim_ignores_non_members() {
    let a = authors();
    let (me, target) = (a[0].0, a[1].0);
    let (mut lm, _s) = new_lane_manager(me, ".db_test_claim_nonmember");
    let chain = direct_chain(&mut lm, target, 2).await;
    let mut res = resolver(&lm);
    let p = proposal(vec![(target, 2, chain[1].id.clone())], vec![], None);
    let mut c = AvailClaim::with_capacity(1);
    c.set_at_tip(0);
    assert!(
        res.note_claim(key(250), &statements(&p, &c))
            .references
            .is_empty(),
        "a non-committee sender must be ignored"
    );
    assert_eq!(res.claimed_len_for_test(), 0);
}

#[test]
fn to_digest_carries_the_claim() {
    let a = authors();
    let p = proposal(vec![r(1, 10, 1), r(2, 20, 2)], vec![], None);
    let mut c = AvailClaim::with_capacity(2);
    c.set_at_tip(0);
    assert!(c.push_short(1, 5));

    let echo = crate::vantage::agb::Echo {
        proposal: p,
        grade: 1,
        sender: a[0].0,
        wish: 3,
        origin: None,
        avail: Some(c.clone()),
    };
    let d = echo.to_digest(&test_sid());
    assert_eq!(
        d.avail.as_ref(),
        Some(&c),
        "the digest-named encoding must carry the claim, or the optimization is dead in \
         exactly the configuration production runs"
    );

    let back: crate::vantage::agb::EchoDigest =
        bincode::deserialize(&bincode::serialize(&d).expect("ser")).expect("de");
    assert_eq!(back.avail, Some(c));
}

#[tokio::test]
async fn on_echo_emits_availclaimed_with_at_tip_marks() {
    use crate::vantage::Effect;
    let a = authors();
    let (me, target) = (a[0].0, a[1].0);
    let (mut lm, _s) = new_lane_manager(me, ".db_test_claim_effect");
    let chain = direct_chain(&mut lm, target, 4).await;

    let p = proposal(vec![(target, 4, chain[3].id.clone())], vec![], None);
    let mut c = AvailClaim::with_capacity(1);
    c.set_at_tip(0);
    let echo = crate::vantage::agb::Echo {
        proposal: p,
        grade: 1,
        sender: a[1].0,
        wish: 0,
        origin: None,
        avail: Some(c),
    };

    let committee = test_committee();
    let mut agb = new_agb_engine(me);
    let mut rep = crate::vantage::repair::Repairer::new(
        me,
        committee,
        test_sid(),
        lm.genesis().clone(),
        MAX_BLOCK_PAYLOAD,
        lm.blocks_handle(),
    );
    let effects = agb.on_echo(echo, &mut rep);
    let claimed: Vec<_> = effects
        .iter()
        .filter_map(|e| match e {
            Effect::AvailClaimed(s, refs) => Some((*s, refs.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(claimed.len(), 1, "exactly one AvailClaimed effect");
    assert_eq!(claimed[0].0, a[1].0, "attributed to the echo's sender");
    assert_eq!(claimed[0].1.len(), 1);
    assert_eq!(
        claimed[0].1[0],
        ClaimRef::Exact((target, 4, chain[3].id.clone()))
    );
}

#[tokio::test]
async fn only_the_first_echo_envelope_from_a_sender_carries_claims() {
    use crate::vantage::Effect;
    let a = authors();
    let (me, sender, target) = (a[0].0, a[1].0, a[2].0);
    let (mut lm, _s) = new_lane_manager(me, ".db_test_claim_first_echo_only");
    let chain = direct_chain(&mut lm, target, 2).await;
    let p = proposal(vec![block_ref(&chain[1])], vec![], None);

    let echo = |claim: AvailClaim| crate::vantage::agb::Echo {
        proposal: p.clone(),
        grade: 1,
        sender,
        wish: 0,
        origin: None,
        avail: Some(claim),
    };
    let mut first = AvailClaim::with_capacity(1);
    first.set_at_tip(0);
    let mut variant = AvailClaim::with_capacity(1);
    assert!(variant.push_short(0, 1));

    let mut agb = new_agb_engine(me);
    let mut rep = crate::vantage::repair::Repairer::new(
        me,
        test_committee(),
        test_sid(),
        lm.genesis().clone(),
        MAX_BLOCK_PAYLOAD,
        lm.blocks_handle(),
    );
    assert!(agb
        .on_echo(echo(first), &mut rep)
        .iter()
        .any(|effect| matches!(effect, Effect::AvailClaimed(_, _))));
    assert!(
        agb.on_echo(echo(variant), &mut rep)
            .iter()
            .all(|effect| !matches!(effect, Effect::AvailClaimed(_, _))),
        "a Byzantine duplicate must not turn one ECHO into several claim envelopes"
    );
}

#[tokio::test]
async fn on_echo_without_a_claim_emits_nothing_extra() {
    use crate::vantage::Effect;
    let a = authors();
    let me = a[0].0;
    let (lm, _s) = new_lane_manager(me, ".db_test_claim_absent");
    let p = proposal(vec![r(2, 1, 5)], vec![], None);
    let echo = crate::vantage::agb::Echo {
        proposal: p,
        grade: 1,
        sender: a[1].0,
        wish: 0,
        origin: None,
        avail: None,
    };
    let committee = test_committee();
    let mut agb = new_agb_engine(me);
    let mut rep = crate::vantage::repair::Repairer::new(
        me,
        committee,
        test_sid(),
        lm.genesis().clone(),
        MAX_BLOCK_PAYLOAD,
        lm.blocks_handle(),
    );
    let effects = agb.on_echo(echo, &mut rep);
    assert!(
        !effects
            .iter()
            .any(|e| matches!(e, Effect::AvailClaimed(_, _))),
        "flag off => no claim => no AvailClaimed effect"
    );
}

#[tokio::test]
async fn sparse_echo_claim_authorizes_its_anchor_walk() {
    use crate::vantage::Effect;
    let a = authors();
    let (me, sender, target) = (a[0].0, a[1].0, a[2].0);
    let (lm, _s) = new_lane_manager(me, ".db_test_claim_sparse_authorizes");
    let anchor = (target, 4, dig(77));
    let p = proposal(vec![anchor.clone()], vec![], None);
    let mut c = AvailClaim::with_capacity(1);
    assert!(c.push_short(0, 1));
    let echo = crate::vantage::agb::Echo {
        proposal: p,
        grade: 1,
        sender,
        wish: 0,
        origin: None,
        avail: Some(c),
    };

    let mut agb = new_agb_engine(me);
    let mut rep = crate::vantage::repair::Repairer::new(
        me,
        test_committee(),
        test_sid(),
        lm.genesis().clone(),
        MAX_BLOCK_PAYLOAD,
        lm.blocks_handle(),
    );
    let effects = agb.on_echo(echo, &mut rep);
    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::AvailClaimed(s, claims)
            if *s == sender
                && claims == &vec![ClaimRef::Ancestor { anchor: anchor.clone(), delta: 1 }]
    )));
    assert!(
        effects
            .iter()
            .any(|effect| matches!(effect, Effect::RequestTo(_, digest) if digest == &anchor.2)),
        "a verified formed carrier must authorize repair of a pending integer claim's anchor"
    );
}

#[tokio::test]
async fn exact_echo_claim_authorizes_ancestor_backfill() {
    use crate::vantage::Effect;
    let a = authors();
    let (me, sender, target) = (a[0].0, a[1].0, a[2].0);
    let (lm, _s) = new_lane_manager(me, ".db_test_claim_exact_authorizes");
    let anchor = (target, 4, dig(78));
    let p = proposal(vec![anchor.clone()], vec![], None);
    let mut c = AvailClaim::with_capacity(1);
    c.set_at_tip(0);
    let echo = crate::vantage::agb::Echo {
        proposal: p,
        grade: 1,
        sender,
        wish: 0,
        origin: None,
        avail: Some(c),
    };

    let mut agb = new_agb_engine(me);
    let mut rep = crate::vantage::repair::Repairer::new(
        me,
        test_committee(),
        test_sid(),
        lm.genesis().clone(),
        MAX_BLOCK_PAYLOAD,
        lm.blocks_handle(),
    );
    let effects = agb.on_echo(echo, &mut rep);
    assert!(effects
        .iter()
        .any(|effect| matches!(effect, Effect::RequestTo(_, digest) if digest == &anchor.2)));
}

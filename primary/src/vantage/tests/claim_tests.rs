use crate::vantage::agb::{ResolutionEntry, ViewProposal};
use crate::vantage::claim::{manifest_refs, AvailClaim, ShortClaim};
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
fn resolve_maps_at_tip_and_short_claims_to_references() {
    let p = proposal(vec![r(1, 10, 1), r(2, 20, 2)], vec![r(3, 30, 3)], None);
    let refs = manifest_refs(&p);
    let mut claim = AvailClaim::with_capacity(refs.len());
    claim.set_at_tip(0);
    assert!(claim.push_short(2, 4, dig(9)));

    let got = claim.resolve(&refs);
    assert_eq!(got.len(), 2);
    assert_eq!(got[0], r(1, 10, 1), "at-tip is the entry verbatim");
    assert_eq!(
        got[1],
        (key(3), 26, dig(9)),
        "short claim is height-delta at its own anchor digest"
    );
}

#[test]
fn resolve_drops_ill_formed_claims() {
    let p = proposal(vec![r(1, 10, 1)], vec![], None);
    let refs = manifest_refs(&p);

    let mut over = AvailClaim::with_capacity(refs.len());
    over.short.push(ShortClaim {
        lane: 0,
        delta: 10,
        head: dig(9),
    });
    assert!(
        over.resolve(&refs).is_empty(),
        "delta == height must be dropped, not wrapped"
    );
    let mut past = AvailClaim::with_capacity(refs.len());
    past.short.push(ShortClaim {
        lane: 0,
        delta: 99,
        head: dig(9),
    });
    assert!(
        past.resolve(&refs).is_empty(),
        "delta > height must be dropped"
    );

    let mut oob = AvailClaim::with_capacity(refs.len());
    oob.short.push(ShortClaim {
        lane: 7,
        delta: 1,
        head: dig(9),
    });
    assert!(
        oob.resolve(&refs).is_empty(),
        "out-of-range lane must be dropped"
    );

    let mut both = AvailClaim::with_capacity(refs.len());
    both.set_at_tip(0);
    both.short.push(ShortClaim {
        lane: 0,
        delta: 1,
        head: dig(9),
    });
    let got = both.resolve(&refs);
    assert_eq!(
        got.len(),
        1,
        "a doubly-claimed lane must yield one reference"
    );
    assert_eq!(got[0], r(1, 10, 1));
}

#[test]
fn push_short_rejects_contradictory_and_zero_claims() {
    let mut c = AvailClaim::with_capacity(4);
    c.set_at_tip(1);
    assert!(!c.push_short(1, 3, dig(1)), "cannot be at-tip AND short");
    assert!(!c.push_short(2, 0, dig(1)), "delta 0 is at-tip, not short");
    assert!(c.push_short(2, 3, dig(1)));
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

    c.push_short(0, 0, dig(1)); // A zero delta is invalid.
    let mut d = AvailClaim::with_capacity(8);
    d.set_at_tip(3);
    assert!(d.push_short(5, 2, dig(7)));
    let back: AvailClaim = bincode::deserialize(&bincode::serialize(&d).expect("ser")).expect("de");
    assert_eq!(back, d, "wire round-trip must be exact");
}

use super::common::*;
use crate::vantage::avail::AvailResolver;

fn resolved(p: &ViewProposal, c: &AvailClaim) -> Vec<(BlockRef, bool)> {
    let refs = manifest_refs(p);
    let at_tip: std::collections::HashSet<Digest> = refs
        .iter()
        .enumerate()
        .filter(|(j, _)| c.is_at_tip(*j))
        .map(|(_, r)| r.2.clone())
        .collect();
    c.resolve(&refs)
        .into_iter()
        .map(|r| {
            let tip = at_tip.contains(&r.2);
            (r, tip)
        })
        .collect()
}

#[tokio::test]
async fn sender_claims_at_tip_short_and_stays_silent() {
    let a = authors();
    let (me, held, partial, unknown) = (a[0].0, a[1].0, a[2].0, a[3].0);
    let (mut lm, _s) = new_lane_manager(me, ".db_test_claim_sender");

    let held_chain = direct_chain(&mut lm, held, 3).await;
    let partial_chain = direct_chain(&mut lm, partial, 2).await;

    let p = proposal(
        vec![
            (held, 3, held_chain[2].id.clone()),
            (partial, 5, dig(200)),
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
    assert_eq!(
        claim.short[0].head, partial_chain[1].id,
        "short claim carries OUR head digest at our frontier"
    );
    assert_eq!(claim.claimed(), 2, "the unknown author must draw no claim");
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
        res.note_claim(a[i].0, &resolved(&p, &c));
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
        res.note_claim(a[1].0, &resolved(&p4, &c4)).len(),
        1,
        "first claim counts"
    );
    let (p2, c2) = at(2);
    assert!(
        res.note_claim(a[1].0, &resolved(&p2, &c2)).is_empty(),
        "a lower re-claim must be dropped"
    );
    assert!(
        res.note_claim(a[1].0, &resolved(&p4, &c4)).is_empty(),
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
    assert!(good.push_short(0, 2, chain[1].id.clone()));
    let got = res.note_claim(a[1].0, &resolved(&p, &good));
    assert_eq!(got.len(), 1);
    assert_eq!(got[0], (target, 2, chain[1].id.clone()));

    let mut bogus = AvailClaim::with_capacity(1);
    assert!(bogus.push_short(0, 1, dig(199)));
    assert!(
        res.note_claim(a[2].0, &resolved(&p, &bogus)).is_empty(),
        "unverifiable linkage must be dropped"
    );
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
        res.note_claim(key(250), &resolved(&p, &c)).is_empty(),
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
    assert!(c.push_short(1, 5, dig(9)));

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
    assert_eq!(claimed[0].1[0].0, (target, 4, chain[3].id.clone()));
    assert!(claimed[0].1[0].1, "an at-tip bit must be marked as such");
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

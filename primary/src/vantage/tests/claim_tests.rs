// AVAIL-ECHO-SPEC.md step 2: the positional availability claim encoding
// (`vantage::claim`) -- wire shape, canonical lane indexing, and the Byzantine
// well-formedness rules `resolve` enforces.
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

/// Lane indices address `C`, then `T`, then `M`'s manifests -- in that exact order.
///
/// AVAIL-ECHO-SPEC §6.1: manifests reach `Cursor` through three paths (agb's seal ->
/// C and T, control's `derive_anchor` -> inside M, cursor's gopen path -> C), so a
/// vector over `C` alone would silently fail to acknowledge every anchor-resolved view.
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

/// `Skip` contributes nothing, so indices stay stable across resolution variants.
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

/// An at-tip bit denotes the proposal's entry verbatim; a short claim denotes the same
/// lane at `height - delta` with its own anchoring digest.
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

/// Every ill-formed claim shape is DROPPED, not an error: a Byzantine sender may put
/// anything on the wire, and a dropped claim costs it only its own acknowledgment.
#[test]
fn resolve_drops_ill_formed_claims() {
    let p = proposal(vec![r(1, 10, 1)], vec![], None);
    let refs = manifest_refs(&p);

    // delta >= height would name a non-positive height. `Height` is unsigned, so this
    // must be rejected BEFORE subtracting or it wraps to a huge height.
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

    // A lane index past the proposal's own vector.
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

    // One lane claimed BOTH at-tip and short: two different heights for one lane. The
    // at-tip claim stands and the short one is dropped, so exactly one survives.
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

/// `push_short` refuses the shapes `resolve` would drop, so an honest sender cannot
/// build one by accident.
#[test]
fn push_short_rejects_contradictory_and_zero_claims() {
    let mut c = AvailClaim::with_capacity(4);
    c.set_at_tip(1);
    assert!(!c.push_short(1, 3, dig(1)), "cannot be at-tip AND short");
    assert!(!c.push_short(2, 0, dig(1)), "delta 0 is at-tip, not short");
    assert!(c.push_short(2, 3, dig(1)));
    assert_eq!(c.claimed(), 2);
}

/// The bit vector must address every lane at n=100 and beyond, across word boundaries.
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

/// Wire size: the whole point is a bit per lane instead of a reference per lane.
///
/// Measured baseline for the mechanism this replaces (2026-08-07 n=100, per node):
/// `VantageAvail` 18.330 MB/s of a 19.880 MB/s total, i.e. 92.2%, at 9,258 B per
/// message. A full at-tip claim over 200 lanes must stay in the low tens of bytes.
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

    // Round-trip, including exceptions.
    c.push_short(0, 0, dig(1)); // rejected (delta 0), must not appear
    let mut d = AvailClaim::with_capacity(8);
    d.set_at_tip(3);
    assert!(d.push_short(5, 2, dig(7)));
    let back: AvailClaim = bincode::deserialize(&bincode::serialize(&d).expect("ser")).expect("de");
    assert_eq!(back, d, "wire round-trip must be exact");
}

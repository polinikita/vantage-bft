use super::common::*;
use crate::vantage::index::{ByRef, CommitteeIndex, MemberSet};
use crypto::Digest;

/// Committees wider than the inline bitmap keep their remaining positions, so no
/// acknowledgment from a high position is silently dropped.
#[test]
fn member_set_spans_the_inline_boundary() {
    let mut set = MemberSet::default();
    for position in [3usize, 63, 64, 255, 256, 400] {
        assert!(set.insert(position), "position {position} counted twice");
        assert!(!set.insert(position), "position {position} counted twice");
    }
    for position in [0usize, 62, 257] {
        assert!(!set.contains(position));
    }
    assert_eq!(
        set.iter().collect::<Vec<_>>(),
        vec![3, 63, 64, 255, 256, 400]
    );
    assert!(set.remove(255) && set.remove(400));
    assert!(!set.remove(255) && !set.remove(400));
    assert_eq!(set.iter().collect::<Vec<_>>(), vec![3, 63, 64, 256]);
}

/// Two digests at one `(author, height)` are equivocation: each keeps its own state.
#[test]
fn by_ref_separates_equivocating_digests() {
    let committee = test_committee();
    let index = CommitteeIndex::new(&committee);
    let author = index.at(1);
    let outside = index.slot(&crypto::PublicKey([9u8; 32]));
    let (left, right) = (Digest([1u8; 32]), Digest([2u8; 32]));
    let mut refs: ByRef<u64> = ByRef::new(&index);

    for (lane, digest, value) in [
        (&author, &left, 1u64),
        (&author, &right, 2),
        (&outside, &left, 3),
        (&outside, &right, 4),
    ] {
        let (held, created) = refs.entry(lane, 7, digest);
        assert!(created);
        *held = value;
    }
    assert!(!refs.entry(&author, 7, &right).1, "recreated a known fork");

    assert_eq!(refs.get(&author, 7, &left), Some(&1));
    assert_eq!(refs.get(&author, 7, &right), Some(&2));
    assert_eq!(refs.get(&outside, 7, &left), Some(&3));
    assert_eq!(refs.get(&author, 8, &left), None);
    assert_eq!(refs.get(&index.at(2), 7, &left), None);

    let mut drained = refs.drain_author(&author);
    drained.sort_unstable();
    assert_eq!(drained, vec![1, 2]);
    assert_eq!(refs.get(&author, 7, &left), None);
    assert_eq!(refs.get(&outside, 7, &right), Some(&4));

    let mut drained = refs.drain_author(&outside);
    drained.sort_unstable();
    assert_eq!(drained, vec![3, 4]);
    assert_eq!(refs.get(&outside, 7, &right), None);
}

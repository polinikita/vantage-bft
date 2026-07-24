use super::*;
use crate::generate_keypair;
use rand::rngs::StdRng;
use rand::SeedableRng as _;

fn keys(n: usize) -> Vec<PublicKey> {
    let mut rng = StdRng::from_seed([9; 32]);
    (0..n).map(|_| generate_keypair(&mut rng).0).collect()
}

#[test]
fn tag_for_matches_verify_from_the_other_side() {
    let secret = MacSecret::generate();
    let members = keys(4);
    let (a, b) = (members[0], members[1]);

    let a_keys = PairwiseKeys::build(&secret, a, members.iter().cloned());
    let b_keys = PairwiseKeys::build(&secret, b, members.iter().cloned());

    let payload = b"a well-formed committee message";
    let tag = a_keys
        .tag_for(&b, payload)
        .expect("b is a committee member");

    // b, receiving a message claiming to be from a, verifies with k_{a,b} == k_{b,a}.
    assert!(b_keys.verify(&a, payload, &tag));
}

#[test]
fn verify_rejects_wrong_sender_claim() {
    let secret = MacSecret::generate();
    let members = keys(4);
    let (a, b, c) = (members[0], members[1], members[2]);

    let a_keys = PairwiseKeys::build(&secret, a, members.iter().cloned());
    let b_keys = PairwiseKeys::build(&secret, b, members.iter().cloned());

    let payload = b"a well-formed committee message";
    // a genuinely produced this tag for b.
    let tag = a_keys.tag_for(&b, payload).unwrap();

    // A Byzantine c cannot relabel a's own message as if it came from c: b's
    // verification, keyed on the CLAIMED sender c, must fail (c doesn't hold k_{a,b}).
    assert!(!b_keys.verify(&c, payload, &tag));
}

#[test]
fn verify_rejects_tampered_payload() {
    let secret = MacSecret::generate();
    let members = keys(3);
    let (a, b) = (members[0], members[1]);

    let a_keys = PairwiseKeys::build(&secret, a, members.iter().cloned());
    let b_keys = PairwiseKeys::build(&secret, b, members.iter().cloned());

    let tag = a_keys.tag_for(&b, b"original payload").unwrap();
    assert!(!b_keys.verify(&a, b"tampered payload!", &tag));
}

#[test]
fn verify_rejects_non_member_sender() {
    let secret = MacSecret::generate();
    let members = keys(3);
    let a = members[0];
    let b = members[1];

    let a_keys = PairwiseKeys::build(&secret, a, members.iter().cloned());
    let b_keys = PairwiseKeys::build(&secret, b, members.iter().cloned());
    let payload = b"msg";
    let tag = a_keys.tag_for(&b, payload).unwrap();

    // b has no key at all for a party outside `members` -- verify must reject, not
    // panic or fall back to some default key.
    let fabricated_sender = {
        let mut rng = StdRng::from_seed([201; 32]);
        generate_keypair(&mut rng).0
    };
    assert!(!b_keys.verify(&fabricated_sender, payload, &tag));
}

#[test]
fn different_secrets_never_agree() {
    let members = keys(2);
    let (a, b) = (members[0], members[1]);
    let secret1 = MacSecret::generate();
    let secret2 = MacSecret::generate();

    let a_keys = PairwiseKeys::build(&secret1, a, members.iter().cloned());
    let b_keys = PairwiseKeys::build(&secret2, b, members.iter().cloned());

    let tag = a_keys.tag_for(&b, b"msg").unwrap();
    assert!(!b_keys.verify(&a, b"msg", &tag));
}

#[test]
fn tag_unverified_is_destination_independent_and_present() {
    let secret = MacSecret::generate();
    let members = keys(3);
    let me = members[0];
    let k = PairwiseKeys::build(&secret, me, members.iter().cloned());

    // Same payload, no destination argument: always identical (used as a broadcast
    // placeholder, computed once regardless of fan-out).
    assert_eq!(k.tag_unverified(b"x"), k.tag_unverified(b"x"));
    assert_ne!(k.tag_unverified(b"x"), k.tag_unverified(b"y"));
}

#[test]
fn split_tag_roundtrips_and_rejects_short_frames() {
    let secret = MacSecret::generate();
    let members = keys(2);
    let (a, b) = (members[0], members[1]);
    let a_keys = PairwiseKeys::build(&secret, a, members.iter().cloned());

    let payload = b"hello world".to_vec();
    let tag = a_keys.tag_for(&b, &payload).unwrap();
    let mut frame = payload.clone();
    frame.extend_from_slice(&tag);

    let (recovered_payload, recovered_tag) = split_tag(&frame).expect("long enough");
    assert_eq!(recovered_payload, payload.as_slice());
    assert_eq!(recovered_tag, tag);

    assert!(split_tag(&[0u8; TAG_LEN - 1]).is_none());
    assert!(split_tag(&[]).is_none());
}

#[test]
fn pairwise_key_derivation_is_order_independent() {
    let secret = MacSecret::generate();
    let members = keys(2);
    let (a, b) = (members[0], members[1]);
    assert_eq!(
        derive_pairwise_key(&secret, &a, &b),
        derive_pairwise_key(&secret, &b, &a)
    );
}

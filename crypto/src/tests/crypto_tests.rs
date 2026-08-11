// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use rand::rngs::StdRng;
use rand::SeedableRng as _;

// Test-only convenience hasher for arbitrary messages.
impl Hash for &[u8] {
    fn digest(&self) -> Digest {
        let mut hasher = Blake3Hasher::new();
        hasher.update(self);
        Digest(hasher.finalize().into())
    }
}

impl PartialEq for SecretKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.encode_base64())
    }
}

pub fn keys() -> Vec<(PublicKey, SecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| generate_keypair(&mut rng)).collect()
}

#[test]
fn import_export_public_key() {
    let (public_key, _) = keys().pop().unwrap();
    let export = public_key.encode_base64();
    let import = PublicKey::decode_base64(&export);
    assert!(import.is_ok());
    assert_eq!(import.unwrap(), public_key);
}

#[test]
fn import_export_secret_key() {
    let (_, secret_key) = keys().pop().unwrap();
    let export = secret_key.encode_base64();
    let import = SecretKey::decode_base64(&export);
    assert!(import.is_ok());
    assert_eq!(import.unwrap(), secret_key);
}

#[test]
fn public_key_index_encoding_is_scoped_and_roundtrips() {
    let public_keys: Vec<_> = keys().into_iter().map(|(key, _)| key).collect();
    let codec = PublicKeyIndexCodec::new(public_keys.iter().copied()).unwrap();
    let key = public_keys[2];

    let legacy = bincode::serialize(&key).unwrap();
    assert_eq!(legacy.len(), 52);

    let compact = with_public_key_index_codec(&codec, || bincode::serialize(&key)).unwrap();
    assert_eq!(compact, vec![2]);
    let decoded: PublicKey =
        with_public_key_index_codec(&codec, || bincode::deserialize(&compact)).unwrap();
    assert_eq!(decoded, key);

    assert_eq!(bincode::serialize(&key).unwrap(), legacy);
}

#[test]
fn public_key_index_encoding_rejects_unknown_values() {
    let codec = PublicKeyIndexCodec::new([PublicKey([1; 32])]).unwrap();

    let encode = with_public_key_index_codec(&codec, || bincode::serialize(&PublicKey([2; 32])));
    assert!(encode.is_err());

    let decode: bincode::Result<PublicKey> =
        with_public_key_index_codec(&codec, || bincode::deserialize(&[1]));
    assert!(decode.is_err());
}

#[test]
fn public_key_index_encoding_supports_exactly_256_keys() {
    let keys_256 = (0..=u8::MAX).map(|index| {
        let mut key = [0; 32];
        key[0] = index;
        PublicKey(key)
    });
    assert_eq!(PublicKeyIndexCodec::new(keys_256).unwrap().len(), 256);

    let keys_257 = (0..257).map(|index| {
        let mut key = [0; 32];
        key[..2].copy_from_slice(&(index as u16).to_le_bytes());
        PublicKey(key)
    });
    assert_eq!(
        PublicKeyIndexCodec::new(keys_257).unwrap_err(),
        PublicKeyIndexError::TooManyKeys(257)
    );
}

#[test]
fn verify_valid_signature() {
    // Get a keypair.
    let (public_key, secret_key) = keys().pop().unwrap();

    // Make signature.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let signature = Signature::new(&digest, &secret_key);

    // Verify the signature.
    assert!(signature.verify(&digest, &public_key).is_ok());
}

#[test]
fn verify_invalid_signature() {
    // Get a keypair.
    let (public_key, secret_key) = keys().pop().unwrap();

    // Make signature.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let signature = Signature::new(&digest, &secret_key);

    // Verify the signature.
    let bad_message: &[u8] = b"Bad message!";
    let digest = bad_message.digest();
    assert!(signature.verify(&digest, &public_key).is_err());
}

#[test]
fn verify_valid_batch() {
    // Make signatures.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let mut keys = keys();
    let signatures: Vec<_> = (0..3)
        .map(|_| {
            let (public_key, secret_key) = keys.pop().unwrap();
            (public_key, Signature::new(&digest, &secret_key))
        })
        .collect();

    // Verify the batch.
    assert!(Signature::verify_batch(&digest, &signatures).is_ok());
}

#[test]
fn verify_invalid_batch() {
    // Make 2 valid signatures.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let mut keys = keys();
    let mut signatures: Vec<_> = (0..2)
        .map(|_| {
            let (public_key, secret_key) = keys.pop().unwrap();
            (public_key, Signature::new(&digest, &secret_key))
        })
        .collect();

    // Add an invalid signature.
    let (public_key, _) = keys.pop().unwrap();
    signatures.push((public_key, Signature::default()));

    // Verify the batch.
    assert!(Signature::verify_batch(&digest, &signatures).is_err());
}

#[tokio::test]
async fn signature_service() {
    // Get a keypair.
    let (public_key, secret_key) = keys().pop().unwrap();

    // Spawn the signature service.
    let mut service = SignatureService::new(secret_key);

    // Request signature from the service.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let signature = service.request_signature(digest.clone()).await;

    // Verify the signature we received.
    assert!(signature.verify(&digest, &public_key).is_ok());
}

// Copyright(C) Facebook, Inc. and its affiliates.
use base64::prelude::{Engine as _, BASE64_STANDARD};
use ed25519_dalek as dalek;
use ed25519_dalek::ed25519;
use ed25519_dalek::Signer as _;
use rand::rngs::OsRng;
use rand::{CryptoRng, RngCore};
use serde::{de, ser, Deserialize, Serialize};
use std::array::TryFromSliceError;
use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;

#[cfg(test)]
#[path = "tests/crypto_tests.rs"]
pub mod crypto_tests;

pub type CryptoError = ed25519::Error;

/// The hasher used for all content digests. Blake3 produces a
/// 32-byte output directly, matching `Digest`'s width with no truncation.
pub type Blake3Hasher = blake3::Hasher;

/// Represents a hash digest (32 bytes).
#[derive(Hash, PartialEq, Default, Eq, Clone, Deserialize, Serialize, Ord, PartialOrd)]
pub struct Digest(pub [u8; 32]);

impl Digest {
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    pub fn size(&self) -> usize {
        self.0.len()
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", BASE64_STANDARD.encode(self.0))
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", BASE64_STANDARD.encode(self.0).get(0..16).unwrap())
    }
}

impl AsRef<[u8]> for Digest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl TryFrom<&[u8]> for Digest {
    type Error = TryFromSliceError;
    fn try_from(item: &[u8]) -> Result<Self, Self::Error> {
        Ok(Digest(item.try_into()?))
    }
}

/// This trait is implemented by all messages that can be hashed.
pub trait Hash {
    fn digest(&self) -> Digest;
}

/// Represents a public key (in bytes).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Default)]
pub struct PublicKey(pub [u8; 32]);

#[derive(Debug)]
struct PublicKeyIndexInner {
    keys: Vec<PublicKey>,
    indices: HashMap<PublicKey, u8>,
}

/// Maps committee public keys to one-byte wire identifiers.
#[derive(Clone, Debug)]
pub struct PublicKeyIndexCodec(Arc<PublicKeyIndexInner>);

/// Invalid committee mapping for one-byte identifiers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PublicKeyIndexError {
    TooManyKeys(usize),
    DuplicateKey(PublicKey),
}

impl fmt::Display for PublicKeyIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyKeys(count) => {
                write!(
                    f,
                    "one-byte committee identifiers support at most 256 keys, got {count}"
                )
            }
            Self::DuplicateKey(key) => write!(f, "duplicate committee key {key}"),
        }
    }
}

impl std::error::Error for PublicKeyIndexError {}

impl PublicKeyIndexCodec {
    pub fn new(keys: impl IntoIterator<Item = PublicKey>) -> Result<Self, PublicKeyIndexError> {
        let keys: Vec<_> = keys.into_iter().collect();
        if keys.len() > u8::MAX as usize + 1 {
            return Err(PublicKeyIndexError::TooManyKeys(keys.len()));
        }
        let mut indices = HashMap::with_capacity(keys.len());
        for (index, key) in keys.iter().copied().enumerate() {
            if indices.insert(key, index as u8).is_some() {
                return Err(PublicKeyIndexError::DuplicateKey(key));
            }
        }
        Ok(Self(Arc::new(PublicKeyIndexInner { keys, indices })))
    }

    pub fn len(&self) -> usize {
        self.0.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.keys.is_empty()
    }

    pub fn index(&self, key: &PublicKey) -> Option<u8> {
        self.0.indices.get(key).copied()
    }

    pub fn key(&self, index: u8) -> Option<PublicKey> {
        self.0.keys.get(index as usize).copied()
    }
}

thread_local! {
    static PUBLIC_KEY_INDEX_SCOPES: RefCell<Vec<PublicKeyIndexCodec>> =
        const { RefCell::new(Vec::new()) };
}

struct PublicKeyIndexScope;

impl Drop for PublicKeyIndexScope {
    fn drop(&mut self) {
        PUBLIC_KEY_INDEX_SCOPES.with(|scopes| {
            scopes.borrow_mut().pop();
        });
    }
}

/// Applies one-byte public-key encoding only during `operation`.
///
/// The operation must be synchronous: the scope is local to the current thread.
pub fn with_public_key_index_codec<T>(
    codec: &PublicKeyIndexCodec,
    operation: impl FnOnce() -> T,
) -> T {
    PUBLIC_KEY_INDEX_SCOPES.with(|scopes| scopes.borrow_mut().push(codec.clone()));
    let _scope = PublicKeyIndexScope;
    operation()
}

fn active_public_key_index(key: &PublicKey) -> Option<Option<u8>> {
    PUBLIC_KEY_INDEX_SCOPES.with(|scopes| scopes.borrow().last().map(|codec| codec.index(key)))
}

fn active_public_key(index: u8) -> Option<Option<PublicKey>> {
    PUBLIC_KEY_INDEX_SCOPES.with(|scopes| scopes.borrow().last().map(|codec| codec.key(index)))
}

impl PublicKey {
    pub fn encode_base64(&self) -> String {
        BASE64_STANDARD.encode(&self.0[..])
    }

    pub fn decode_base64(s: &str) -> Result<Self, base64::DecodeError> {
        let bytes = BASE64_STANDARD.decode(s)?;
        let array = bytes[..32]
            .try_into()
            .map_err(|_| base64::DecodeError::InvalidLength(bytes.len()))?;
        Ok(Self(array))
    }
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.encode_base64())
    }
}

impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.encode_base64().get(0..16).unwrap())
    }
}

impl Serialize for PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        if let Some(index) = active_public_key_index(self) {
            return serializer
                .serialize_u8(index.ok_or_else(|| {
                    ser::Error::custom("public key is not in the wire committee")
                })?);
        }
        serializer.serialize_str(&self.encode_base64())
    }
}

impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        if PUBLIC_KEY_INDEX_SCOPES.with(|scopes| !scopes.borrow().is_empty()) {
            let index = u8::deserialize(deserializer)?;
            return active_public_key(index)
                .flatten()
                .ok_or_else(|| de::Error::custom(format!("unknown committee index {index}")));
        }
        let s = String::deserialize(deserializer)?;
        let value = Self::decode_base64(&s).map_err(|e| de::Error::custom(e.to_string()))?;
        Ok(value)
    }
}

impl AsRef<[u8]> for PublicKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Represents a secret key (in bytes).
pub struct SecretKey([u8; 64]);

impl SecretKey {
    pub fn encode_base64(&self) -> String {
        BASE64_STANDARD.encode(&self.0[..])
    }

    pub fn decode_base64(s: &str) -> Result<Self, base64::DecodeError> {
        let bytes = BASE64_STANDARD.decode(s)?;
        let array = bytes[..64]
            .try_into()
            .map_err(|_| base64::DecodeError::InvalidLength(bytes.len()))?;
        Ok(Self(array))
    }
}

impl Serialize for SecretKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.encode_base64())
    }
}

impl<'de> Deserialize<'de> for SecretKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let value = Self::decode_base64(&s).map_err(|e| de::Error::custom(e.to_string()))?;
        Ok(value)
    }
}

impl Drop for SecretKey {
    fn drop(&mut self) {
        self.0.iter_mut().for_each(|x| *x = 0);
    }
}

pub fn generate_production_keypair() -> (PublicKey, SecretKey) {
    generate_keypair(&mut OsRng)
}

pub fn generate_keypair<R>(csprng: &mut R) -> (PublicKey, SecretKey)
where
    R: CryptoRng + RngCore,
{
    let keypair = dalek::SigningKey::generate(csprng);
    let public = PublicKey(keypair.verifying_key().to_bytes());
    let secret = SecretKey(keypair.to_keypair_bytes());
    (public, secret)
}

/// Represents an ed25519 signature.
#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct Signature {
    part1: [u8; 32],
    part2: [u8; 32],
}

impl Signature {
    pub fn new(digest: &Digest, secret: &SecretKey) -> Self {
        let keypair =
            dalek::SigningKey::from_keypair_bytes(&secret.0).expect("Unable to load secret key");
        let sig = keypair.sign(&digest.0).to_bytes();
        let part1 = sig[..32].try_into().expect("Unexpected signature length");
        let part2 = sig[32..64].try_into().expect("Unexpected signature length");
        Signature { part1, part2 }
    }

    fn flatten(&self) -> [u8; 64] {
        [self.part1, self.part2]
            .concat()
            .try_into()
            .expect("Unexpected signature length")
    }

    pub fn verify(&self, digest: &Digest, public_key: &PublicKey) -> Result<(), CryptoError> {
        let signature = dalek::Signature::from_bytes(&self.flatten());
        let key = dalek::VerifyingKey::from_bytes(&public_key.0)?;
        key.verify_strict(&digest.0, &signature)
    }

    pub fn verify_batch<'a, I>(digest: &Digest, votes: I) -> Result<(), CryptoError>
    where
        I: IntoIterator<Item = &'a (PublicKey, Signature)>,
    {
        let mut messages: Vec<&[u8]> = Vec::new();
        let mut signatures: Vec<dalek::Signature> = Vec::new();
        let mut keys: Vec<dalek::VerifyingKey> = Vec::new();
        for (key, sig) in votes.into_iter() {
            messages.push(&digest.0[..]);
            signatures.push(dalek::Signature::from_bytes(&sig.flatten()));
            keys.push(dalek::VerifyingKey::from_bytes(&key.0)?);
        }
        dalek::verify_batch(&messages[..], &signatures[..], &keys[..])
    }

    pub fn verify_batch_multi<'a, I>(digests: &[Digest], votes: I) -> Result<(), CryptoError>
    where
        I: IntoIterator<Item = &'a (PublicKey, Signature)>,
    {
        let mut messages: Vec<&[u8]> = Vec::new();
        let mut signatures: Vec<dalek::Signature> = Vec::new();
        let mut keys: Vec<dalek::VerifyingKey> = Vec::new();
        for (i, (key, sig)) in votes.into_iter().enumerate() {
            messages.push(&digests[i].0[..]);
            signatures.push(dalek::Signature::from_bytes(&sig.flatten()));
            keys.push(dalek::VerifyingKey::from_bytes(&key.0)?);
        }
        dalek::verify_batch(&messages[..], &signatures[..], &keys[..])
    }
}

/// This service holds the node's private key. It takes digests as input and returns a signature
/// over the digest (through a oneshot channel).
#[derive(Clone)]
pub struct SignatureService {
    channel: Sender<(Digest, oneshot::Sender<Signature>)>,
}

impl SignatureService {
    pub fn new(secret: SecretKey) -> Self {
        let (tx, mut rx): (Sender<(_, oneshot::Sender<_>)>, _) = channel(100);
        tokio::spawn(async move {
            while let Some((digest, sender)) = rx.recv().await {
                let signature = Signature::new(&digest, &secret);
                let _ = sender.send(signature);
            }
        });
        Self { channel: tx }
    }

    pub async fn request_signature(&mut self, digest: Digest) -> Signature {
        let (sender, receiver): (oneshot::Sender<_>, oneshot::Receiver<_>) = oneshot::channel();
        if let Err(e) = self.channel.send((digest, sender)).await {
            panic!("Failed to send message Signature Service: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive signature from Signature Service")
    }
}

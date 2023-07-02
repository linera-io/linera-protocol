// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use ed25519_dalek::{self as dalek, Signer, Verifier};
use generic_array::typenum::Unsigned;
use serde::{Deserialize, Serialize};
use std::{num::ParseIntError, str::FromStr};
use thiserror::Error;

use crate::doc_scalar;

#[cfg(any(test, feature = "test"))]
use {
    proptest::{
        collection::{vec, VecStrategy},
        prelude::Arbitrary,
        strategy::{self, Strategy},
    },
    std::ops::RangeInclusive,
};

/// A signature key-pair.
pub struct KeyPair(dalek::Keypair);

/// A signature public key.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash)]
pub struct PublicKey(pub [u8; dalek::PUBLIC_KEY_LENGTH]);

type HasherOutputSize = <sha3::Sha3_256 as sha3::digest::OutputSizeUser>::OutputSize;
type HasherOutput = generic_array::GenericArray<u8, HasherOutputSize>;

/// A Sha3-256 value.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Copy, Hash)]
#[cfg_attr(any(test, feature = "test"), derive(Default))]
pub struct CryptoHash(HasherOutput);

/// A signature value.
#[derive(Eq, PartialEq, Copy, Clone)]
pub struct Signature(pub dalek::Signature);

#[derive(Error, Debug)]
/// Error type for cryptographic errors.
pub enum CryptoError {
    #[error("Signature for object {type_name} is not valid: {error}")]
    InvalidSignature { error: String, type_name: String },
    #[error("String contains non-hexadecimal digits")]
    NonHexDigits(#[from] hex::FromHexError),
    #[error(
        "Byte slice has length {0} but a `CryptoHash` requires exactly {expected} bytes",
        expected = HasherOutputSize::to_usize(),
    )]
    IncorrectHashSize(usize),
    #[error(
        "Byte slice has length {0} but a `PublicKey` requires exactly {expected} bytes",
        expected = dalek::PUBLIC_KEY_LENGTH,
    )]
    IncorrectPublicKeySize(usize),
    #[error("Could not parse integer")]
    ParseIntError(#[from] ParseIntError),
}

impl PublicKey {
    #[cfg(any(test, feature = "test"))]
    pub fn debug(name: u8) -> PublicKey {
        let addr = [name; dalek::PUBLIC_KEY_LENGTH];
        PublicKey(addr)
    }
}

impl KeyPair {
    /// Generates a new key-pair.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn generate() -> Self {
        let mut csprng = rand07::rngs::OsRng;
        let keypair = dalek::Keypair::generate(&mut csprng);
        KeyPair(keypair)
    }

    /// Obtains the public key of a key-pair.
    pub fn public(&self) -> PublicKey {
        PublicKey(self.0.public.to_bytes())
    }

    /// Copies the key-pair, **including the secret key**.
    ///
    /// The `Clone` and `Copy` traits are deliberately not implemented for `KeyPair` to prevent
    /// accidental copies of secret keys.
    pub fn copy(&self) -> KeyPair {
        KeyPair(dalek::Keypair {
            secret: dalek::SecretKey::from_bytes(self.0.secret.as_bytes()).unwrap(),
            public: self.0.public,
        })
    }
}

impl Serialize for PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            serializer.serialize_newtype_struct("PublicKey", &self.0)
        }
    }
}

impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            let value = Self::from_str(&s).map_err(serde::de::Error::custom)?;
            Ok(value)
        } else {
            #[derive(Deserialize)]
            #[serde(rename = "PublicKey")]
            struct Foo([u8; dalek::PUBLIC_KEY_LENGTH]);

            let value = Foo::deserialize(deserializer)?;
            Ok(Self(value.0))
        }
    }
}

impl Serialize for CryptoHash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            serializer.serialize_newtype_struct("CryptoHash", &self.0)
        }
    }
}

impl<'de> Deserialize<'de> for CryptoHash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            let value = Self::from_str(&s).map_err(serde::de::Error::custom)?;
            Ok(value)
        } else {
            #[derive(Deserialize)]
            #[serde(rename = "CryptoHash")]
            struct Foo(HasherOutput);

            let value = Foo::deserialize(deserializer)?;
            Ok(Self(value.0))
        }
    }
}

impl Serialize for KeyPair {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        // This is only used for JSON configuration.
        assert!(serializer.is_human_readable());
        serializer.serialize_str(&hex::encode(self.0.to_bytes()))
    }
}

impl<'de> Deserialize<'de> for KeyPair {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        // This is only used for JSON configuration.
        assert!(deserializer.is_human_readable());
        let s = String::deserialize(deserializer)?;
        let value = hex::decode(s).map_err(serde::de::Error::custom)?;
        let key = dalek::Keypair::from_bytes(&value).map_err(serde::de::Error::custom)?;
        Ok(KeyPair(key))
    }
}

impl Serialize for Signature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&hex::encode(self.0.to_bytes()))
        } else {
            serializer.serialize_newtype_struct("Signature", &self.0)
        }
    }
}

impl<'de> Deserialize<'de> for Signature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            let value = hex::decode(s).map_err(serde::de::Error::custom)?;
            let sig =
                dalek::Signature::try_from(value.as_slice()).map_err(serde::de::Error::custom)?;
            Ok(Signature(sig))
        } else {
            #[derive(Deserialize)]
            #[serde(rename = "Signature")]
            struct Foo(dalek::Signature);

            let value = Foo::deserialize(deserializer)?;
            Ok(Self(value.0))
        }
    }
}

impl FromStr for PublicKey {
    type Err = CryptoError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value = hex::decode(s)?;
        (value.as_slice()).try_into()
    }
}

impl TryFrom<&[u8]> for PublicKey {
    type Error = CryptoError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != dalek::PUBLIC_KEY_LENGTH {
            return Err(CryptoError::IncorrectPublicKeySize(value.len()));
        }
        let mut pubkey = [0u8; dalek::PUBLIC_KEY_LENGTH];
        pubkey.copy_from_slice(value);
        Ok(PublicKey(pubkey))
    }
}

impl FromStr for CryptoHash {
    type Err = CryptoError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value = hex::decode(s)?;
        (value.as_slice()).try_into()
    }
}

impl TryFrom<&[u8]> for CryptoHash {
    type Error = CryptoError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != HasherOutputSize::to_usize() {
            return Err(CryptoError::IncorrectHashSize(value.len()));
        }
        let mut bytes = HasherOutput::default();
        bytes.copy_from_slice(value);
        Ok(Self(bytes))
    }
}

impl From<[u64; 4]> for CryptoHash {
    fn from(integers: [u64; 4]) -> Self {
        let mut bytes = [0u8; 32];

        bytes[0..8].copy_from_slice(&integers[0].to_le_bytes());
        bytes[8..16].copy_from_slice(&integers[1].to_le_bytes());
        bytes[16..24].copy_from_slice(&integers[2].to_le_bytes());
        bytes[24..32].copy_from_slice(&integers[3].to_le_bytes());

        CryptoHash(bytes.into())
    }
}

impl From<CryptoHash> for [u64; 4] {
    fn from(crypto_hash: CryptoHash) -> Self {
        let bytes = crypto_hash.0;
        let mut integers = [0u64; 4];

        integers[0] = u64::from_le_bytes(bytes[0..8].try_into().expect("incorrect indices"));
        integers[1] = u64::from_le_bytes(bytes[8..16].try_into().expect("incorrect indices"));
        integers[2] = u64::from_le_bytes(bytes[16..24].try_into().expect("incorrect indices"));
        integers[3] = u64::from_le_bytes(bytes[24..32].try_into().expect("incorrect indices"));

        integers
    }
}

impl std::fmt::Display for Signature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let s = hex::encode(self.0.to_bytes());
        write!(f, "{}", s)
    }
}

impl std::fmt::Display for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(&self.0[..]))
    }
}

impl std::fmt::Display for CryptoHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let prec = f.precision().unwrap_or(self.0.len() * 2);
        hex::encode(&self.0[..((prec + 1) / 2)]).fmt(f)
    }
}

impl std::fmt::Debug for Signature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{}", hex::encode(&self.0.to_bytes()[0..8]))
    }
}

impl std::fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{}", hex::encode(&self.0[..8]))
    }
}

impl std::fmt::Debug for CryptoHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{}", hex::encode(&self.0[..8]))
    }
}

/// Something that we know how to hash.
pub trait Hashable<Hasher> {
    fn write(&self, hasher: &mut Hasher);
}

/// Something that we know how to hash and sign.
pub trait HasTypeName {
    fn type_name() -> &'static str;
}

/// Activate the blanket implementation of `Hashable` based on serde and BCS.
/// * We use `serde_name` to extract a seed from the name of structs and enums.
/// * We use `BCS` to generate canonical bytes suitable for hashing.
pub trait BcsHashable: Serialize + serde::de::DeserializeOwned {}

/// Activate the blanket implementation of `Signable` based on serde and BCS.
/// * We use `serde_name` to extract a seed from the name of structs and enums.
/// * We use `BCS` to generate canonical bytes suitable for signing.
pub trait BcsSignable: Serialize + serde::de::DeserializeOwned {}

impl<T: BcsSignable> BcsHashable for T {}

impl<T, Hasher> Hashable<Hasher> for T
where
    T: BcsHashable,
    Hasher: std::io::Write,
{
    fn write(&self, hasher: &mut Hasher) {
        let name = <Self as HasTypeName>::type_name();
        // Note: This assumes that names never contain the separator `::`.
        write!(hasher, "{}::", name).expect("Hasher should not fail");
        bcs::serialize_into(hasher, &self).expect("Message serialization should not fail");
    }
}

impl<Hasher> Hashable<Hasher> for [u8]
where
    Hasher: std::io::Write,
{
    fn write(&self, hasher: &mut Hasher) {
        hasher.write_all(self).expect("Hasher should not fail");
    }
}

impl<T> HasTypeName for T
where
    T: BcsHashable,
{
    fn type_name() -> &'static str {
        serde_name::trace_name::<Self>().expect("Self must be a struct or an enum")
    }
}

impl CryptoHash {
    pub fn new<T: ?Sized>(value: &T) -> Self
    where
        T: BcsHashable,
    {
        use sha3::digest::Digest;

        let mut hasher = sha3::Sha3_256::default();
        value.write(&mut hasher);
        CryptoHash(hasher.finalize())
    }

    pub fn as_bytes(&self) -> &HasherOutput {
        &self.0
    }
}

impl Signature {
    pub fn new<T>(value: &T, secret: &KeyPair) -> Self
    where
        T: BcsSignable,
    {
        let mut message = Vec::new();
        value.write(&mut message);
        let signature = secret.0.sign(&message);
        Signature(signature)
    }

    fn check_internal<T>(&self, value: &T, author: PublicKey) -> Result<(), dalek::SignatureError>
    where
        T: BcsSignable,
    {
        let mut message = Vec::new();
        value.write(&mut message);
        let public_key = dalek::PublicKey::from_bytes(&author.0)?;
        public_key.verify(&message, &self.0)
    }

    pub fn check<T>(&self, value: &T, author: PublicKey) -> Result<(), CryptoError>
    where
        T: BcsSignable + std::fmt::Debug,
    {
        self.check_internal(value, author)
            .map_err(|error| CryptoError::InvalidSignature {
                error: error.to_string(),
                type_name: T::type_name().to_string(),
            })
    }

    fn verify_batch_internal<'a, T, I>(value: &'a T, votes: I) -> Result<(), dalek::SignatureError>
    where
        T: BcsSignable,
        I: IntoIterator<Item = (&'a PublicKey, &'a Signature)>,
    {
        let mut msg = Vec::new();
        value.write(&mut msg);
        let mut messages: Vec<&[u8]> = Vec::new();
        let mut signatures: Vec<dalek::Signature> = Vec::new();
        let mut public_keys: Vec<dalek::PublicKey> = Vec::new();
        for (addr, sig) in votes.into_iter() {
            messages.push(&msg);
            signatures.push(sig.0);
            public_keys.push(dalek::PublicKey::from_bytes(&addr.0)?);
        }
        dalek::verify_batch(&messages[..], &signatures[..], &public_keys[..])
    }

    pub fn verify_batch<'a, T, I>(value: &'a T, votes: I) -> Result<(), CryptoError>
    where
        T: BcsSignable,
        I: IntoIterator<Item = (&'a PublicKey, &'a Signature)>,
    {
        Signature::verify_batch_internal(value, votes).map_err(|error| {
            CryptoError::InvalidSignature {
                error: format!("batched {}", error),
                type_name: T::type_name().to_string(),
            }
        })
    }
}

#[cfg(any(test, feature = "test"))]
impl Arbitrary for CryptoHash {
    type Parameters = ();
    type Strategy = strategy::Map<VecStrategy<RangeInclusive<u8>>, fn(Vec<u8>) -> CryptoHash>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        vec(u8::MIN..=u8::MAX, HasherOutputSize::to_usize()).prop_map(|vector| {
            CryptoHash(generic_array::GenericArray::clone_from_slice(&vector[..]))
        })
    }
}

impl BcsHashable for PublicKey {}

doc_scalar!(CryptoHash, "A Sha3-256 value");
doc_scalar!(PublicKey, "A signature public key");
doc_scalar!(Signature, "A signature value");

#[test]
#[allow(clippy::disallowed_names)]
fn test_signatures() {
    #[derive(Debug, Serialize, Deserialize)]
    struct Foo(String);

    impl BcsSignable for Foo {}

    #[derive(Debug, Serialize, Deserialize)]
    struct Bar(String);

    impl BcsSignable for Bar {}

    let key1 = KeyPair::generate();
    let addr1 = key1.public();
    let key2 = KeyPair::generate();
    let addr2 = key2.public();

    let foo = Foo("hello".into());
    let foox = Foo("hellox".into());
    let bar = Bar("hello".into());

    let s = Signature::new(&foo, &key1);
    assert!(s.check(&foo, addr1).is_ok());
    assert!(s.check(&foo, addr2).is_err());
    assert!(s.check(&foox, addr1).is_err());
    assert!(s.check(&bar, addr1).is_err());
}

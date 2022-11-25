// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use ed25519_dalek as dalek;
use ed25519_dalek::{Signer, Verifier};
use generic_array::typenum::Unsigned;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use thiserror::Error;

#[cfg(any(test, feature = "test"))]
use {
    proptest::{
        collection::{vec, VecStrategy},
        prelude::Arbitrary,
        strategy::{self, Strategy},
    },
    std::ops::RangeInclusive,
};

#[cfg(test)]
#[path = "unit_tests/crypto_tests.rs"]
mod crypto_tests;

/// A signature key-pair.
pub struct KeyPair(dalek::Keypair);

/// A signature public key.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash)]
pub struct PublicKey(pub [u8; dalek::PUBLIC_KEY_LENGTH]);

/// A Sha512 value.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Copy, Hash)]
pub struct HashValue(generic_array::GenericArray<u8, <sha2::Sha512 as sha2::Digest>::OutputSize>);

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
        "Byte slice has length {0} but a `HashValue` requires exactly {expected} bytes",
        expected = <sha2::Sha512 as sha2::Digest>::OutputSize::to_usize(),
    )]
    IncorrectHashSize(usize),
    #[error(
    "Byte slice has length {0} but a `PublicKey` requires exactly {expected} bytes",
    expected = dalek::PUBLIC_KEY_LENGTH,
    )]
    IncorrectPublicKeySize(usize),
}

impl PublicKey {
    #[cfg(any(test, feature = "test"))]
    pub fn debug(name: u8) -> PublicKey {
        let addr = [name; dalek::PUBLIC_KEY_LENGTH];
        PublicKey(addr)
    }
}

impl KeyPair {
    /// Generate a new key-pair.
    pub fn generate() -> Self {
        let mut csprng = OsRng;
        let keypair = dalek::Keypair::generate(&mut csprng);
        KeyPair(keypair)
    }

    /// Obtain the public key of a key-pair.
    pub fn public(&self) -> PublicKey {
        PublicKey(self.0.public.to_bytes())
    }

    /// Avoid implementing `clone` on secret keys to prevent mistakes.
    pub fn copy(&self) -> KeyPair {
        KeyPair(dalek::Keypair {
            secret: dalek::SecretKey::from_bytes(self.0.secret.as_bytes()).unwrap(),
            public: dalek::PublicKey::from_bytes(self.0.public.as_bytes()).unwrap(),
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
            let value =
                Self::from_str(&s).map_err(|err| serde::de::Error::custom(err.to_string()))?;
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

impl Serialize for HashValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            serializer.serialize_newtype_struct("HashValue", &self.0)
        }
    }
}

impl<'de> Deserialize<'de> for HashValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            let value =
                Self::from_str(&s).map_err(|err| serde::de::Error::custom(err.to_string()))?;
            Ok(value)
        } else {
            #[derive(Deserialize)]
            #[serde(rename = "HashValue")]
            struct Foo(generic_array::GenericArray<u8, <sha2::Sha512 as sha2::Digest>::OutputSize>);

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
        let value = hex::decode(&s).map_err(|err| serde::de::Error::custom(err.to_string()))?;
        let key = dalek::Keypair::from_bytes(&value)
            .map_err(|err| serde::de::Error::custom(err.to_string()))?;
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
            let value = hex::decode(&s).map_err(|err| serde::de::Error::custom(err.to_string()))?;
            let sig = dalek::Signature::from_bytes(&value)
                .map_err(|err| serde::de::Error::custom(err.to_string()))?;
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
        pubkey.copy_from_slice(&value[..dalek::PUBLIC_KEY_LENGTH]);
        Ok(PublicKey(pubkey))
    }
}

impl FromStr for HashValue {
    type Err = CryptoError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value = hex::decode(s)?;
        (value.as_slice()).try_into()
    }
}

impl TryFrom<&[u8]> for HashValue {
    type Error = CryptoError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != <sha2::Sha512 as sha2::Digest>::output_size() {
            return Err(CryptoError::IncorrectHashSize(value.len()));
        }
        let mut bytes =
            generic_array::GenericArray::<u8, <sha2::Sha512 as sha2::Digest>::OutputSize>::default(
            );
        bytes.copy_from_slice(&value[..<sha2::Sha512 as sha2::Digest>::output_size()]);
        Ok(Self(bytes))
    }
}

/// Error when attempting to convert a string into a [`HashValue`].
#[derive(Clone, Copy, Debug, Error)]
pub enum HashFromStrError {
    #[error("Invalid length for hex-encoded hash value")]
    InvalidLength,

    #[error("String contains non-hexadecimal digits")]
    NonHexDigits(#[from] hex::FromHexError),
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

impl std::fmt::Display for HashValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(&self.0[..]))
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

impl std::fmt::Debug for HashValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{}", hex::encode(&self.0[..8]))
    }
}

/// Something that we know how to hash and sign.
pub trait Signable<Hasher> {
    fn write(&self, hasher: &mut Hasher);

    fn type_name() -> &'static str;
}

/// Activate the blanket implementation of `Signable` based on serde and BCS.
/// * We use `serde_name` to extract a seed from the name of structs and enums.
/// * We use `BCS` to generate canonical bytes suitable for hashing and signing.
pub trait BcsSignable: Serialize + serde::de::DeserializeOwned {}

impl<T, Hasher> Signable<Hasher> for T
where
    T: BcsSignable,
    Hasher: std::io::Write,
{
    fn write(&self, hasher: &mut Hasher) {
        let name = <Self as Signable<Hasher>>::type_name();
        // Note: This assumes that names never contain the separator `::`.
        write!(hasher, "{}::", name).expect("Hasher should not fail");
        bcs::serialize_into(hasher, &self).expect("Message serialization should not fail");
    }

    fn type_name() -> &'static str {
        serde_name::trace_name::<Self>().expect("Self must be a struct or an enum")
    }
}

impl HashValue {
    pub fn new<T>(value: &T) -> Self
    where
        T: Signable<sha2::Sha512>,
    {
        use sha2::Digest;

        let mut hasher = sha2::Sha512::default();
        value.write(&mut hasher);
        HashValue(hasher.finalize())
    }

    pub fn as_bytes(
        &self,
    ) -> &generic_array::GenericArray<u8, <sha2::Sha512 as sha2::Digest>::OutputSize> {
        &self.0
    }
}

impl Signature {
    pub fn new<T>(value: &T, secret: &KeyPair) -> Self
    where
        T: Signable<Vec<u8>>,
    {
        let mut message = Vec::new();
        value.write(&mut message);
        let signature = secret.0.sign(&message);
        Signature(signature)
    }

    fn check_internal<T>(&self, value: &T, author: PublicKey) -> Result<(), dalek::SignatureError>
    where
        T: Signable<Vec<u8>>,
    {
        let mut message = Vec::new();
        value.write(&mut message);
        let public_key = dalek::PublicKey::from_bytes(&author.0)?;
        public_key.verify(&message, &self.0)
    }

    pub fn check<T>(&self, value: &T, author: PublicKey) -> Result<(), CryptoError>
    where
        T: Signable<Vec<u8>> + std::fmt::Debug,
    {
        self.check_internal(value, author)
            .map_err(|error| CryptoError::InvalidSignature {
                error: error.to_string(),
                type_name: T::type_name().to_string(),
            })
    }

    fn verify_batch_internal<'a, T, I>(value: &'a T, votes: I) -> Result<(), dalek::SignatureError>
    where
        T: Signable<Vec<u8>>,
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
        T: Signable<Vec<u8>>,
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
impl Arbitrary for HashValue {
    type Parameters = ();
    type Strategy = strategy::Map<VecStrategy<RangeInclusive<u8>>, fn(Vec<u8>) -> HashValue>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        vec(u8::MIN..=u8::MAX, 64).prop_map(|vector| {
            let bytes: [u8; 64] = vector.try_into().expect("Incorrect vector size");

            HashValue(generic_array::GenericArray::clone_from_slice(&bytes))
        })
    }
}

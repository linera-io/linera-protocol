// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Defines Ed25519 signature primitives used by the Linera protocol.

use std::{borrow::Cow, fmt, str::FromStr};

use ed25519_dalek::{self as dalek, Signer, Verifier};
use linera_witty::{
    GuestPointer, HList, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitLoad, WitStore, WitType,
};
use serde::{Deserialize, Serialize};

use super::{
    le_bytes_to_u64_array, u64_array_to_le_bytes, BcsHashable, BcsSignable, CryptoError,
    CryptoHash, HasTypeName, Hashable,
};
use crate::{doc_scalar, identifiers::Owner};

/// The label for the Ed25519 scheme.
const ED25519_SCHEME_LABEL: &str = "Ed25519";

/// An Ed25519 secret key.
pub struct Ed25519SecretKey(pub(crate) dalek::SigningKey);

/// An Ed25519 signature public key.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash)]
pub struct Ed25519PublicKey(pub [u8; dalek::PUBLIC_KEY_LENGTH]);

/// An Ed25519 signature.
#[derive(Eq, PartialEq, Copy, Clone)]
pub struct Ed25519Signature(pub dalek::Signature);

impl Ed25519SecretKey {
    #[cfg(all(with_getrandom, with_testing))]
    /// Generates a new key pair using the operating system's RNG.
    ///
    /// If you want control over the RNG, use `generate_from`[Ed25519SecretKey::generate_from].
    pub fn generate() -> Self {
        let mut rng = rand::rngs::OsRng;
        Self::generate_from(&mut rng)
    }

    #[cfg(with_getrandom)]
    /// Generates a new key pair from the given RNG. Use with care.
    pub fn generate_from<R: super::CryptoRng>(rng: &mut R) -> Self {
        let keypair = dalek::SigningKey::generate(rng);
        Ed25519SecretKey(keypair)
    }

    /// Obtains the public key of a key pair.
    pub fn public(&self) -> Ed25519PublicKey {
        Ed25519PublicKey(self.0.verifying_key().to_bytes())
    }

    /// Copies the key pair, **including the secret key**.
    ///
    /// The `Clone` and `Copy` traits are deliberately not implemented for `KeyPair` to prevent
    /// accidental copies of secret keys.
    pub fn copy(&self) -> Ed25519SecretKey {
        Ed25519SecretKey(self.0.clone())
    }
}

impl Ed25519PublicKey {
    /// A fake public key used for testing.
    #[cfg(with_testing)]
    pub fn test_key(name: u8) -> Ed25519PublicKey {
        let addr = [name; dalek::PUBLIC_KEY_LENGTH];
        Ed25519PublicKey(addr)
    }

    /// Returns bytes of the public key.
    pub fn as_bytes(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    /// Parses bytes to a public key.
    ///
    /// Returns error if input bytes are not of the correct length.
    pub fn from_slice(bytes: &[u8]) -> Result<Self, CryptoError> {
        let key = bytes
            .try_into()
            .map_err(|_| CryptoError::IncorrectPublicKeySize {
                scheme: ED25519_SCHEME_LABEL,
                len: bytes.len(),
                expected: dalek::PUBLIC_KEY_LENGTH,
            })?;
        Ok(Ed25519PublicKey(key))
    }
}

impl Serialize for Ed25519PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            serializer.serialize_newtype_struct("Ed25519PublicKey", &self.0)
        }
    }
}

impl<'de> Deserialize<'de> for Ed25519PublicKey {
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
            #[serde(rename = "Ed25519PublicKey")]
            struct PublicKey([u8; dalek::PUBLIC_KEY_LENGTH]);

            let value = PublicKey::deserialize(deserializer)?;
            Ok(Self(value.0))
        }
    }
}

impl Serialize for Ed25519SecretKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        // This is only used for JSON configuration.
        assert!(serializer.is_human_readable());
        serializer.serialize_str(&hex::encode(self.0.to_bytes()))
    }
}

impl<'de> Deserialize<'de> for Ed25519SecretKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        // This is only used for JSON configuration.
        assert!(deserializer.is_human_readable());
        let s = String::deserialize(deserializer)?;
        let value = hex::decode(s).map_err(serde::de::Error::custom)?;
        let key =
            dalek::SigningKey::from_bytes(value[..].try_into().map_err(serde::de::Error::custom)?);
        Ok(Ed25519SecretKey(key))
    }
}

impl FromStr for Ed25519PublicKey {
    type Err = CryptoError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value = hex::decode(s)?;
        (value.as_slice()).try_into()
    }
}

impl TryFrom<&[u8]> for Ed25519PublicKey {
    type Error = CryptoError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != dalek::PUBLIC_KEY_LENGTH {
            return Err(CryptoError::IncorrectPublicKeySize {
                scheme: ED25519_SCHEME_LABEL,
                len: value.len(),
                expected: dalek::PUBLIC_KEY_LENGTH,
            });
        }
        let mut pubkey = [0u8; dalek::PUBLIC_KEY_LENGTH];
        pubkey.copy_from_slice(value);
        Ok(Ed25519PublicKey(pubkey))
    }
}

impl From<[u64; 4]> for Ed25519PublicKey {
    fn from(integers: [u64; 4]) -> Self {
        Ed25519PublicKey(u64_array_to_le_bytes(integers))
    }
}

impl From<Ed25519PublicKey> for [u64; 4] {
    fn from(pub_key: Ed25519PublicKey) -> Self {
        le_bytes_to_u64_array(&pub_key.0)
    }
}

impl fmt::Display for Ed25519PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0[..]))
    }
}

impl fmt::Debug for Ed25519PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0[..8]))
    }
}

impl WitType for Ed25519PublicKey {
    const SIZE: u32 = <(u64, u64, u64, u64) as WitType>::SIZE;
    type Layout = <(u64, u64, u64, u64) as WitType>::Layout;
    type Dependencies = HList![];

    fn wit_type_name() -> Cow<'static, str> {
        "ed25519-public-key".into()
    }

    fn wit_type_declaration() -> Cow<'static, str> {
        concat!(
            "    record ed25519-public-key {\n",
            "        part1: u64,\n",
            "        part2: u64,\n",
            "        part3: u64,\n",
            "        part4: u64,\n",
            "    }\n",
        )
        .into()
    }
}

impl WitLoad for Ed25519PublicKey {
    fn load<Instance>(
        memory: &Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let (part1, part2, part3, part4) = WitLoad::load(memory, location)?;
        Ok(Self::from([part1, part2, part3, part4]))
    }

    fn lift_from<Instance>(
        flat_layout: <Self::Layout as Layout>::Flat,
        memory: &Memory<'_, Instance>,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let (part1, part2, part3, part4) = WitLoad::lift_from(flat_layout, memory)?;
        Ok(Self::from([part1, part2, part3, part4]))
    }
}

impl WitStore for Ed25519PublicKey {
    fn store<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<(), RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let [part1, part2, part3, part4] = (*self).into();
        (part1, part2, part3, part4).store(memory, location)
    }

    fn lower<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<<Self::Layout as Layout>::Flat, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let [part1, part2, part3, part4] = (*self).into();
        (part1, part2, part3, part4).lower(memory)
    }
}

impl<'de> BcsHashable<'de> for Ed25519PublicKey {}

impl Ed25519Signature {
    /// Computes a signature.
    pub fn new<'de, T>(value: &T, secret: &Ed25519SecretKey) -> Self
    where
        T: BcsSignable<'de>,
    {
        let mut message = Vec::new();
        value.write(&mut message);
        let signature = secret.0.sign(&message);
        Ed25519Signature(signature)
    }

    /// Parses bytes to a signature.
    ///
    /// Returns error if input slice is not 64 bytes.
    pub fn from_slice(bytes: &[u8]) -> Result<Self, CryptoError> {
        let sig = dalek::Signature::from_slice(bytes).map_err(|_| {
            CryptoError::IncorrectSignatureBytes {
                scheme: ED25519_SCHEME_LABEL,
                len: bytes.len(),
                expected: dalek::SIGNATURE_LENGTH,
            }
        })?;
        Ok(Ed25519Signature(sig))
    }

    fn check_internal<'de, T>(
        &self,
        value: &T,
        author: Ed25519PublicKey,
    ) -> Result<(), dalek::SignatureError>
    where
        T: BcsSignable<'de>,
    {
        let mut message = Vec::new();
        value.write(&mut message);
        let public_key = dalek::VerifyingKey::from_bytes(&author.0)?;
        public_key.verify(&message, &self.0)
    }

    /// Checks a signature.
    pub fn check<'de, T>(&self, value: &T, author: Ed25519PublicKey) -> Result<(), CryptoError>
    where
        T: BcsSignable<'de> + fmt::Debug,
    {
        self.check_internal(value, author)
            .map_err(|error| CryptoError::InvalidSignature {
                error: error.to_string(),
                type_name: T::type_name().to_string(),
            })
    }

    fn verify_batch_internal<'a, 'de, T, I>(
        value: &'a T,
        votes: I,
    ) -> Result<(), dalek::SignatureError>
    where
        T: BcsSignable<'de>,
        I: IntoIterator<Item = (&'a Ed25519PublicKey, &'a Ed25519Signature)>,
    {
        let mut msg = Vec::new();
        value.write(&mut msg);
        let mut messages = Vec::new();
        let mut signatures = Vec::new();
        let mut public_keys = Vec::new();
        for (addr, sig) in votes.into_iter() {
            messages.push(msg.as_slice());
            signatures.push(sig.0);
            public_keys.push(dalek::VerifyingKey::from_bytes(&addr.0)?);
        }
        dalek::verify_batch(&messages[..], &signatures[..], &public_keys[..])
    }

    /// Verifies a batch of signatures.
    // NOTE: This is unused now since we don't use ed25519 in consensus layer.
    #[allow(unused)]
    pub fn verify_batch<'a, 'de, T, I>(value: &'a T, votes: I) -> Result<(), CryptoError>
    where
        T: BcsSignable<'de>,
        I: IntoIterator<Item = (&'a Ed25519PublicKey, &'a Ed25519Signature)>,
    {
        Ed25519Signature::verify_batch_internal(value, votes).map_err(|error| {
            CryptoError::InvalidSignature {
                error: format!("batched {}", error),
                type_name: T::type_name().to_string(),
            }
        })
    }

    /// Returns bytes of the signature.
    pub fn as_bytes(&self) -> Vec<u8> {
        self.0.to_bytes().to_vec()
    }
}

impl Serialize for Ed25519Signature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&hex::encode(self.0.to_bytes()))
        } else {
            serializer.serialize_newtype_struct("Ed25519Signature", &self.0)
        }
    }
}

impl<'de> Deserialize<'de> for Ed25519Signature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            let value = hex::decode(s).map_err(serde::de::Error::custom)?;
            let sig =
                dalek::Signature::try_from(value.as_slice()).map_err(serde::de::Error::custom)?;
            Ok(Ed25519Signature(sig))
        } else {
            #[derive(Deserialize)]
            #[serde(rename = "Ed25519Signature")]
            struct Signature(dalek::Signature);

            let value = Signature::deserialize(deserializer)?;
            Ok(Self(value.0))
        }
    }
}

impl fmt::Display for Ed25519Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = hex::encode(self.0.to_bytes());
        write!(f, "{}", s)
    }
}

impl fmt::Debug for Ed25519Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0.to_bytes()[0..8]))
    }
}

impl From<Ed25519PublicKey> for Owner {
    fn from(value: Ed25519PublicKey) -> Self {
        Self(CryptoHash::new(&value))
    }
}

impl From<&Ed25519PublicKey> for Owner {
    fn from(value: &Ed25519PublicKey) -> Self {
        Self(CryptoHash::new(value))
    }
}

doc_scalar!(Ed25519Signature, "An Ed25519 signature value");
doc_scalar!(Ed25519PublicKey, "A Ed25519 signature public key");

#[cfg(with_testing)]
mod tests {
    #[test]
    fn test_signatures() {
        use serde::{Deserialize, Serialize};

        use crate::crypto::{
            ed25519::{Ed25519SecretKey, Ed25519Signature},
            BcsSignable, TestString,
        };

        #[derive(Debug, Serialize, Deserialize)]
        struct Foo(String);

        impl<'de> BcsSignable<'de> for Foo {}

        let key1 = Ed25519SecretKey::generate();
        let addr1 = key1.public();
        let key2 = Ed25519SecretKey::generate();
        let addr2 = key2.public();

        let ts = TestString("hello".into());
        let tsx = TestString("hellox".into());
        let foo = Foo("hello".into());

        let s = Ed25519Signature::new(&ts, &key1);
        assert!(s.check(&ts, addr1).is_ok());
        assert!(s.check(&ts, addr2).is_err());
        assert!(s.check(&tsx, addr1).is_err());
        assert!(s.check(&foo, addr1).is_err());
    }

    #[test]
    fn test_public_key_serialization() {
        use crate::crypto::ed25519::Ed25519PublicKey;
        let key_in = Ed25519PublicKey::test_key(0);
        let s = serde_json::to_string(&key_in).unwrap();
        let key_out: Ed25519PublicKey = serde_json::from_str(&s).unwrap();
        assert_eq!(key_out, key_in);

        let s = bcs::to_bytes(&key_in).unwrap();
        let key_out: Ed25519PublicKey = bcs::from_bytes(&s).unwrap();
        assert_eq!(key_out, key_in);
    }

    #[test]
    fn test_secret_key_serialization() {
        use crate::crypto::ed25519::Ed25519SecretKey;
        let key_in = Ed25519SecretKey::generate();
        let s = serde_json::to_string(&key_in).unwrap();
        let key_out: Ed25519SecretKey = serde_json::from_str(&s).unwrap();
        assert_eq!(key_out.0.to_bytes(), key_in.0.to_bytes());
    }
}

// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Cow, fmt, str::FromStr};

use ed25519_dalek::{self as dalek};
use linera_witty::{
    GuestPointer, HList, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitLoad, WitStore, WitType,
};
use serde::{Deserialize, Serialize};

use super::{le_bytes_to_u64_array, u64_array_to_le_bytes, BcsHashable, CryptoError};
use crate::doc_scalar;

/// A signature key-pair.
pub struct Ed25519SecretKey(pub(crate) dalek::SigningKey);

/// A signature public key.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash)]
pub struct PublicKey(pub [u8; dalek::PUBLIC_KEY_LENGTH]);

impl Ed25519SecretKey {
    #[cfg(all(with_getrandom, with_testing))]
    /// Generates a new key-pair.
    pub fn generate() -> Self {
        let mut rng = rand::rngs::OsRng;
        Self::generate_from(&mut rng)
    }

    #[cfg(with_getrandom)]
    /// Generates a new key-pair from the given RNG. Use with care.
    pub fn generate_from<R: super::CryptoRng>(rng: &mut R) -> Self {
        let keypair = dalek::SigningKey::generate(rng);
        Ed25519SecretKey(keypair)
    }

    /// Obtains the public key of a key-pair.
    pub fn public(&self) -> PublicKey {
        PublicKey(self.0.verifying_key().to_bytes())
    }

    /// Copies the key-pair, **including the secret key**.
    ///
    /// The `Clone` and `Copy` traits are deliberately not implemented for `KeyPair` to prevent
    /// accidental copies of secret keys.
    pub fn copy(&self) -> Ed25519SecretKey {
        Ed25519SecretKey(self.0.clone())
    }
}

impl PublicKey {
    /// A fake public key used for testing.
    #[cfg(with_testing)]
    pub fn test_key(name: u8) -> PublicKey {
        let addr = [name; dalek::PUBLIC_KEY_LENGTH];
        PublicKey(addr)
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

impl From<[u64; 4]> for PublicKey {
    fn from(integers: [u64; 4]) -> Self {
        PublicKey(u64_array_to_le_bytes(integers))
    }
}

impl From<PublicKey> for [u64; 4] {
    fn from(pub_key: PublicKey) -> Self {
        le_bytes_to_u64_array(&pub_key.0)
    }
}

impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0[..]))
    }
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0[..8]))
    }
}

impl WitType for PublicKey {
    const SIZE: u32 = <(u64, u64, u64, u64) as WitType>::SIZE;
    type Layout = <(u64, u64, u64, u64) as WitType>::Layout;
    type Dependencies = HList![];

    fn wit_type_name() -> Cow<'static, str> {
        "public-key".into()
    }

    fn wit_type_declaration() -> Cow<'static, str> {
        concat!(
            "    record public-key {\n",
            "        part1: u64,\n",
            "        part2: u64,\n",
            "        part3: u64,\n",
            "        part4: u64,\n",
            "    }\n",
        )
        .into()
    }
}

impl WitLoad for PublicKey {
    fn load<Instance>(
        memory: &Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let (part1, part2, part3, part4) = WitLoad::load(memory, location)?;
        Ok(PublicKey::from([part1, part2, part3, part4]))
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
        Ok(PublicKey::from([part1, part2, part3, part4]))
    }
}

impl WitStore for PublicKey {
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

impl<'de> BcsHashable<'de> for PublicKey {}

doc_scalar!(PublicKey, "A signature public key");

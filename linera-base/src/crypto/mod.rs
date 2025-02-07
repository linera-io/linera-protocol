// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Define the cryptographic primitives used by the Linera protocol.

mod hash;
mod signature;
use std::{borrow::Cow, fmt, io, num::ParseIntError, str::FromStr};

use alloy_primitives::FixedBytes;
use ed25519_dalek::{self as dalek};
pub use hash::*;
use linera_witty::{
    GuestPointer, HList, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitLoad, WitStore, WitType,
};
use serde::{Deserialize, Serialize};
pub use signature::*;
use thiserror::Error;

use crate::doc_scalar;

/// A signature key-pair.
pub struct KeyPair(dalek::SigningKey);

/// A signature public key.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash)]
pub struct PublicKey(pub [u8; dalek::PUBLIC_KEY_LENGTH]);

/// Error type for cryptographic errors.
#[derive(Error, Debug)]
#[allow(missing_docs)]
pub enum CryptoError {
    #[error("Signature for object {type_name} is not valid: {error}")]
    InvalidSignature { error: String, type_name: String },
    #[error("Signature for object {type_name} is missing")]
    MissingSignature { type_name: String },
    #[error(transparent)]
    NonHexDigits(#[from] hex::FromHexError),
    #[error(
        "Byte slice has length {0} but a `CryptoHash` requires exactly {expected} bytes",
        expected = FixedBytes::<32>::len_bytes(),
    )]
    IncorrectHashSize(usize),
    #[error(
        "Byte slice has length {0} but a `PublicKey` requires exactly {expected} bytes",
        expected = dalek::PUBLIC_KEY_LENGTH,
    )]
    IncorrectPublicKeySize(usize),
    #[error("Could not parse integer: {0}")]
    ParseIntError(#[from] ParseIntError),
}

impl PublicKey {
    /// A fake public key used for testing.
    #[cfg(with_testing)]
    pub fn test_key(name: u8) -> PublicKey {
        let addr = [name; dalek::PUBLIC_KEY_LENGTH];
        PublicKey(addr)
    }
}

#[cfg(with_getrandom)]
/// Wrapper around [`rand::CryptoRng`] and [`rand::RngCore`].
pub trait CryptoRng: rand::CryptoRng + rand::RngCore + Send + Sync {}

#[cfg(with_getrandom)]
impl<T: rand::CryptoRng + rand::RngCore + Send + Sync> CryptoRng for T {}

#[cfg(with_getrandom)]
impl From<Option<u64>> for Box<dyn CryptoRng> {
    fn from(seed: Option<u64>) -> Self {
        use rand::SeedableRng;

        match seed {
            Some(seed) => Box::new(rand::rngs::StdRng::seed_from_u64(seed)),
            None => Box::new(rand::rngs::OsRng),
        }
    }
}

impl KeyPair {
    #[cfg(all(with_getrandom, with_testing))]
    /// Generates a new key-pair.
    pub fn generate() -> Self {
        let mut rng = rand::rngs::OsRng;
        Self::generate_from(&mut rng)
    }

    #[cfg(with_getrandom)]
    /// Generates a new key-pair from the given RNG. Use with care.
    pub fn generate_from<R: CryptoRng>(rng: &mut R) -> Self {
        let keypair = dalek::SigningKey::generate(rng);
        KeyPair(keypair)
    }

    /// Obtains the public key of a key-pair.
    pub fn public(&self) -> PublicKey {
        PublicKey(self.0.verifying_key().to_bytes())
    }

    /// Copies the key-pair, **including the secret key**.
    ///
    /// The `Clone` and `Copy` traits are deliberately not implemented for `KeyPair` to prevent
    /// accidental copies of secret keys.
    pub fn copy(&self) -> KeyPair {
        KeyPair(self.0.clone())
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
        let key =
            dalek::SigningKey::from_bytes(value[..].try_into().map_err(serde::de::Error::custom)?);
        Ok(KeyPair(key))
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

/// Something that we know how to hash.
pub trait Hashable<Hasher> {
    /// Send the content of `Self` to the given hasher.
    fn write(&self, hasher: &mut Hasher);
}

/// Something that we know how to hash and sign.
pub trait HasTypeName {
    /// The name of the type.
    fn type_name() -> &'static str;
}

/// Activate the blanket implementation of `Hashable` based on serde and BCS.
/// * We use `serde_name` to extract a seed from the name of structs and enums.
/// * We use `BCS` to generate canonical bytes suitable for hashing.
pub trait BcsHashable<'de>: Serialize + Deserialize<'de> {}

/// Activate the blanket implementation of `Signable` based on serde and BCS.
/// * We use `serde_name` to extract a seed from the name of structs and enums.
/// * We use `BCS` to generate canonical bytes suitable for signing.
pub trait BcsSignable<'de>: Serialize + Deserialize<'de> {}

impl<'de, T: BcsSignable<'de>> BcsHashable<'de> for T {}

impl<'de, T, Hasher> Hashable<Hasher> for T
where
    T: BcsHashable<'de>,
    Hasher: io::Write,
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
    Hasher: io::Write,
{
    fn write(&self, hasher: &mut Hasher) {
        hasher.write_all(self).expect("Hasher should not fail");
    }
}

impl<'de, T> HasTypeName for T
where
    T: BcsHashable<'de>,
{
    fn type_name() -> &'static str {
        serde_name::trace_name::<Self>().expect("Self must be a struct or an enum")
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

/// Reads the `bytes` as four little-endian unsigned 64-bit integers and returns them.
pub(crate) fn le_bytes_to_u64_array(bytes: &[u8]) -> [u64; 4] {
    let mut integers = [0u64; 4];

    integers[0] = u64::from_le_bytes(bytes[0..8].try_into().expect("incorrect indices"));
    integers[1] = u64::from_le_bytes(bytes[8..16].try_into().expect("incorrect indices"));
    integers[2] = u64::from_le_bytes(bytes[16..24].try_into().expect("incorrect indices"));
    integers[3] = u64::from_le_bytes(bytes[24..32].try_into().expect("incorrect indices"));

    integers
}

/// Reads the `bytes` as four big-endian unsigned 64-bit integers and returns them.
pub(crate) fn be_bytes_to_u64_array(bytes: &[u8]) -> [u64; 4] {
    let mut integers = [0u64; 4];

    integers[0] = u64::from_be_bytes(bytes[0..8].try_into().expect("incorrect indices"));
    integers[1] = u64::from_be_bytes(bytes[8..16].try_into().expect("incorrect indices"));
    integers[2] = u64::from_be_bytes(bytes[16..24].try_into().expect("incorrect indices"));
    integers[3] = u64::from_be_bytes(bytes[24..32].try_into().expect("incorrect indices"));

    integers
}

/// Returns the bytes that represent the `integers` in little-endian.
pub(crate) fn u64_array_to_le_bytes(integers: [u64; 4]) -> [u8; 32] {
    let mut bytes = [0u8; 32];

    bytes[0..8].copy_from_slice(&integers[0].to_le_bytes());
    bytes[8..16].copy_from_slice(&integers[1].to_le_bytes());
    bytes[16..24].copy_from_slice(&integers[2].to_le_bytes());
    bytes[24..32].copy_from_slice(&integers[3].to_le_bytes());

    bytes
}

/// Returns the bytes that represent the `integers` in big-endian.
pub(crate) fn u64_array_to_be_bytes(integers: [u64; 4]) -> [u8; 32] {
    let mut bytes = [0u8; 32];

    bytes[0..8].copy_from_slice(&integers[0].to_be_bytes());
    bytes[8..16].copy_from_slice(&integers[1].to_be_bytes());
    bytes[16..24].copy_from_slice(&integers[2].to_be_bytes());
    bytes[24..32].copy_from_slice(&integers[3].to_be_bytes());

    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u64_array_to_be_bytes() {
        let input = [
            0x0123456789ABCDEF,
            0xFEDCBA9876543210,
            0x0011223344556677,
            0x8899AABBCCDDEEFF,
        ];
        let expected_output = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54,
            0x32, 0x10, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB,
            0xCC, 0xDD, 0xEE, 0xFF,
        ];

        let output = u64_array_to_be_bytes(input);
        assert_eq!(output, expected_output);
        assert_eq!(input, be_bytes_to_u64_array(&u64_array_to_be_bytes(input)));
    }

    #[test]
    fn test_u64_array_to_le_bytes() {
        let input = [
            0x0123456789ABCDEF,
            0xFEDCBA9876543210,
            0x0011223344556677,
            0x8899AABBCCDDEEFF,
        ];
        let expected_output = [
            0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01, 0x10, 0x32, 0x54, 0x76, 0x98, 0xBA,
            0xDC, 0xFE, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00, 0xFF, 0xEE, 0xDD, 0xCC,
            0xBB, 0xAA, 0x99, 0x88,
        ];

        let output = u64_array_to_le_bytes(input);
        assert_eq!(output, expected_output);
        assert_eq!(input, le_bytes_to_u64_array(&u64_array_to_le_bytes(input)));
    }
}

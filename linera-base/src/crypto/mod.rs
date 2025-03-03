// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Define the cryptographic primitives used by the Linera protocol.

mod ed25519;
mod hash;
#[allow(dead_code)]
mod secp256k1;
use std::{fmt::Display, io, num::ParseIntError, str::FromStr};

use alloy_primitives::FixedBytes;
use custom_debug_derive::Debug;
pub use hash::*;
use linera_witty::{WitLoad, WitStore, WitType};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::identifiers::Owner;

/// The public key of a validator.
pub type ValidatorPublicKey = secp256k1::Secp256k1PublicKey;
/// The private key of a validator.
pub type ValidatorSecretKey = secp256k1::Secp256k1SecretKey;
/// The signature of a validator.
pub type ValidatorSignature = secp256k1::Secp256k1Signature;
/// The key pair of a validator.
pub type ValidatorKeypair = secp256k1::Secp256k1KeyPair;

/// The public key of a chain owner.
/// The corresponding private key is allowed to propose blocks
/// on the chain and transfer account's tokens.
#[derive(
    Serialize,
    Deserialize,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Copy,
    Clone,
    Hash,
    WitType,
    WitLoad,
    WitStore,
)]
pub struct AccountPublicKey(ed25519::Ed25519PublicKey);

/// The private key of a chain owner.
#[derive(Serialize, Deserialize)]
pub struct AccountSecretKey(ed25519::Ed25519SecretKey);

/// The signature of a chain owner.
#[derive(Eq, PartialEq, Copy, Clone, Debug, Serialize, Deserialize)]
pub struct AccountSignature(ed25519::Ed25519Signature);

impl AccountSecretKey {
    #[cfg(all(with_getrandom, with_testing))]
    /// Generates a new `AccountSecretKey`.
    pub fn generate() -> Self {
        Self(ed25519::Ed25519SecretKey::generate())
    }

    #[cfg(with_getrandom)]
    /// Returns a new `AccountSecretKey` generated from the given `seed`.
    pub fn generate_from<R: CryptoRng>(rng: &mut R) -> Self {
        let secret_key = ed25519::Ed25519SecretKey::generate_from(rng);
        AccountSecretKey(secret_key)
    }

    /// Returns the public key corresponding to this secret key.
    pub fn public(&self) -> AccountPublicKey {
        AccountPublicKey(self.0.public())
    }

    /// Copies the secret key.
    pub fn copy(&self) -> Self {
        AccountSecretKey(self.0.copy())
    }
}

impl AccountPublicKey {
    #[cfg(with_testing)]
    /// Constructs a test key from a seed.
    pub fn test_key(seed: u8) -> Self {
        Self(ed25519::Ed25519PublicKey::test_key(seed))
    }

    /// Returns the byte representation of the public key.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.as_bytes()
    }

    /// Parses the byte representation of the public key.
    ///
    /// Returns error if the byte slice has incorrect length.
    pub fn from_slice(bytes: &[u8]) -> Result<Self, CryptoError> {
        Ok(Self(ed25519::Ed25519PublicKey::from_slice(bytes)?))
    }
}

impl AccountSignature {
    /// Creates a signature for the `value` using provided `secret`.
    pub fn new<'de, T>(value: &T, secret: &AccountSecretKey) -> Self
    where
        T: BcsSignable<'de>,
    {
        let signature = ed25519::Ed25519Signature::new(value, &secret.0);
        AccountSignature(signature)
    }

    /// Verifies the signature for the `value` using the provided `public_key`.
    pub fn verify<'de, T>(&self, value: &T, author: AccountPublicKey) -> Result<(), CryptoError>
    where
        T: BcsSignable<'de> + std::fmt::Debug,
    {
        self.0.check(value, author.0)
    }

    /// Returns byte representation of the signatures.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0 .0.to_vec()
    }

    /// Parses the byte representation of the signature.
    pub fn from_slice(_bytes: &[u8]) -> Result<Self, CryptoError> {
        Ok(Self(ed25519::Ed25519Signature::from_slice(_bytes)?))
    }
}

impl FromStr for AccountPublicKey {
    type Err = CryptoError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value = hex::decode(s)?;
        Ok(AccountPublicKey((value.as_slice()).try_into()?))
    }
}

impl Display for AccountPublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<AccountPublicKey> for Owner {
    fn from(public_key: AccountPublicKey) -> Self {
        public_key.0.into()
    }
}

impl From<&AccountPublicKey> for Owner {
    fn from(public_key: &AccountPublicKey) -> Self {
        public_key.0.into()
    }
}

impl TryFrom<&[u8]> for AccountSignature {
    type Error = CryptoError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        AccountSignature::from_slice(bytes)
    }
}

/// Error type for cryptographic errors.
#[derive(Error, Debug)]
#[allow(missing_docs)]
pub enum CryptoError {
    #[error("Signature for object {type_name} is not valid: {error}")]
    InvalidSignature { error: String, type_name: String },
    #[error("Signature from validator is missing")]
    MissingValidatorSignature,
    #[error(transparent)]
    NonHexDigits(#[from] hex::FromHexError),
    #[error(
        "Byte slice has length {0} but a `CryptoHash` requires exactly {expected} bytes",
        expected = FixedBytes::<32>::len_bytes(),
    )]
    IncorrectHashSize(usize),
    #[error(
        "Byte slice has length {len} but a {scheme} `PublicKey` requires exactly {expected} bytes"
    )]
    IncorrectPublicKeySize {
        scheme: &'static str,
        len: usize,
        expected: usize,
    },
    #[error(
        "byte slice has length {len} but a {scheme} `Signature` requires exactly {expected} bytes"
    )]
    IncorrectSignatureBytes {
        scheme: &'static str,
        len: usize,
        expected: usize,
    },
    #[error("Could not parse integer: {0}")]
    ParseIntError(#[from] ParseIntError),
    #[error("secp256k1 error: {0}")]
    Secp256k1Error(k256::ecdsa::Error),
    #[error("could not parse public key: {0}: point at infinity")]
    Secp256k1PointAtInfinity(String),
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

/// A BCS-signable struct for testing.
#[cfg(with_testing)]
#[derive(Debug, Serialize, Deserialize)]
pub struct TestString(pub String);

#[cfg(with_testing)]
impl TestString {
    /// Creates a new `TestString` with the given string.
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

#[cfg(with_testing)]
impl<'de> BcsSignable<'de> for TestString {}

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

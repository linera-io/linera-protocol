// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Define the cryptographic primitives used by the Linera protocol.

mod ed25519;
mod hash;
#[allow(dead_code)]
mod secp256k1;
use std::{io, num::ParseIntError};

use alloy_primitives::FixedBytes;
pub use ed25519::{
    Ed25519PublicKey as PublicKey, Ed25519SecretKey as SigningKey, Ed25519Signature as Signature,
};
use ed25519_dalek::{self as dalek};
pub use hash::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;

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

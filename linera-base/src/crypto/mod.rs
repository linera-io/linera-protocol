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
use ed25519::{Ed25519PublicKey, Ed25519Signature};
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

/// Signature scheme used for the public key.
#[derive(Serialize, Deserialize, Debug, Copy, Clone, Eq, PartialEq)]
pub enum SignatureScheme {
    /// Ed25519
    Ed25519,
    /// secp256k1
    Secp256k1,
}

impl SignatureScheme {
    /// Returns the flag of the signature scheme.
    pub fn flag(&self) -> u8 {
        match self {
            SignatureScheme::Ed25519 => 0x00,
            SignatureScheme::Secp256k1 => 0x01,
        }
    }

    /// Returns the signature scheme of the public key based on the flag.
    ///
    /// Returns error if the flag is not recognized.
    pub fn from_flag_byte(flag: u8) -> Result<Self, CryptoError> {
        match flag {
            0x00 => Ok(SignatureScheme::Ed25519),
            0x01 => Ok(SignatureScheme::Secp256k1),
            _ => Err(CryptoError::InvalidSchemeFlag(flag)),
        }
    }
}

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
pub enum AccountPublicKey {
    /// Ed25519 public key.
    Ed25519(ed25519::Ed25519PublicKey),
    /// secp256k1 public key.
    Secp256k1(secp256k1::Secp256k1PublicKey),
}

/// The private key of a chain owner.
#[derive(Serialize, Deserialize)]
pub enum AccountSecretKey {
    /// Ed25519 secret key.
    Ed25519(ed25519::Ed25519SecretKey),
    /// secp256k1 secret key.
    Secp256k1(secp256k1::Secp256k1SecretKey),
}

/// The signature of a chain owner.
#[derive(Eq, PartialEq, Copy, Clone, Debug, Serialize, Deserialize)]
pub enum AccountSignature {
    /// Ed25519 signature.
    Ed25519(ed25519::Ed25519Signature),
    /// secp256k1 signature.
    Secp256k1(secp256k1::Secp256k1Signature),
}

impl AccountSecretKey {
    /// Returns the public key corresponding to this secret key.
    pub fn public(&self) -> AccountPublicKey {
        match self {
            AccountSecretKey::Ed25519(secret) => AccountPublicKey::Ed25519(secret.public()),
            AccountSecretKey::Secp256k1(secret) => AccountPublicKey::Secp256k1(secret.public()),
        }
    }

    /// Copies the secret key.
    pub fn copy(&self) -> Self {
        match self {
            AccountSecretKey::Ed25519(secret) => AccountSecretKey::Ed25519(secret.copy()),
            AccountSecretKey::Secp256k1(secret) => AccountSecretKey::Secp256k1(secret.copy()),
        }
    }

    /// Creates a signature for the `value` using provided `secret`.
    pub fn sign<'de, T>(&self, value: &T) -> AccountSignature
    where
        T: BcsSignable<'de>,
    {
        match self {
            AccountSecretKey::Ed25519(secret) => {
                let signature = Ed25519Signature::new(value, secret);
                AccountSignature::Ed25519(signature)
            }
            AccountSecretKey::Secp256k1(secret) => {
                let signature = secp256k1::Secp256k1Signature::new(value, secret);
                AccountSignature::Secp256k1(signature)
            }
        }
    }

    #[cfg(all(with_getrandom, with_testing))]
    /// Generates a new key pair.
    ///
    /// Uses `OsRng` for that. If you want control over the RNG, use `generate_from`.
    pub fn generate() -> Self {
        use rand::RngCore;
        let mut rng = rand::rngs::OsRng;
        if rng.next_u32() % u32::MAX == 0 {
            AccountSecretKey::Ed25519(ed25519::Ed25519SecretKey::generate())
        } else {
            AccountSecretKey::Secp256k1(secp256k1::Secp256k1KeyPair::generate().secret_key)
        }
    }

    #[cfg(with_getrandom)]
    /// Generates a new key pair from the given RNG. Use with care.
    pub fn generate_from<R: CryptoRng>(rng: &mut R) -> Self {
        //TODO: Use a better way to choose between the two schemes.
        if rng.next_u32() % u32::MAX == 0 {
            AccountSecretKey::Ed25519(ed25519::Ed25519SecretKey::generate_from(rng))
        } else {
            AccountSecretKey::Secp256k1(secp256k1::Secp256k1KeyPair::generate_from(rng).secret_key)
        }
    }
}

impl AccountPublicKey {
    /// Returns the signature scheme of the public key.
    pub fn scheme(&self) -> SignatureScheme {
        match self {
            AccountPublicKey::Ed25519(_) => SignatureScheme::Ed25519,
            AccountPublicKey::Secp256k1(_) => SignatureScheme::Secp256k1,
        }
    }

    /// Returns the byte representation of the public key.
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();
        bytes.push(self.scheme().flag());

        match self {
            AccountPublicKey::Ed25519(public_key) => {
                bytes.extend_from_slice(&public_key.as_bytes())
            }
            AccountPublicKey::Secp256k1(public_key) => {
                bytes.extend_from_slice(&public_key.as_bytes())
            }
        }

        bytes
    }

    /// Parses the byte representation of the public key.
    ///
    /// Returns error if the byte slice has incorrect length or the flag is not recognized.
    pub fn from_slice(bytes: &[u8]) -> Result<Self, CryptoError> {
        let flag = bytes[0];
        let scheme = SignatureScheme::from_flag_byte(flag)?;
        match scheme {
            SignatureScheme::Ed25519 => {
                let pk = Ed25519PublicKey::from_slice(&bytes[1..])?;
                Ok(AccountPublicKey::Ed25519(pk))
            }
            SignatureScheme::Secp256k1 => {
                let pk = secp256k1::Secp256k1PublicKey::from_bytes(&bytes[1..])?;
                Ok(AccountPublicKey::Secp256k1(pk))
            }
        }
    }

    /// A fake public key used for testing.
    #[cfg(with_testing)]
    pub fn test_key(name: u8) -> Self {
        AccountPublicKey::Ed25519(Ed25519PublicKey::test_key(name))
    }
}

impl AccountSignature {
    /// Verifies the signature for the `value` using the provided `public_key`.
    pub fn verify<'de, T>(&self, value: &T, author: AccountPublicKey) -> Result<(), CryptoError>
    where
        T: BcsSignable<'de> + std::fmt::Debug,
    {
        match self {
            AccountSignature::Ed25519(signature) => {
                let public_key = match author {
                    AccountPublicKey::Ed25519(public_key) => public_key,
                    _ => {
                        return Err(CryptoError::InvalidSignature {
                            error: "invalid public key type".to_string(),
                            type_name: std::any::type_name::<T>().to_string(),
                        })
                    }
                };
                signature.check(value, public_key)
            }
            AccountSignature::Secp256k1(signature) => {
                let public_key = match author {
                    AccountPublicKey::Secp256k1(public_key) => public_key,
                    _ => {
                        return Err(CryptoError::InvalidSignature {
                            error: "invalid public key type".to_string(),
                            type_name: std::any::type_name::<T>().to_string(),
                        })
                    }
                };
                signature.check(value, &public_key)
            }
        }
    }

    /// Returns byte representation of the signatures.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();

        match self {
            AccountSignature::Ed25519(signature) => {
                bytes.push(SignatureScheme::Ed25519.flag());
                bytes.extend_from_slice(&signature.as_bytes());
            }
            AccountSignature::Secp256k1(signature) => {
                bytes.push(SignatureScheme::Secp256k1.flag());
                bytes.extend_from_slice(&signature.as_bytes());
            }
        }

        bytes
    }

    /// Parses the byte representation of the signature.
    pub fn from_slice(bytes: &[u8]) -> Result<Self, CryptoError> {
        let flag = bytes[0];
        let scheme = SignatureScheme::from_flag_byte(flag)?;
        match scheme {
            SignatureScheme::Ed25519 => {
                let signature = Ed25519Signature::from_slice(&bytes[1..])?;
                Ok(AccountSignature::Ed25519(signature))
            }
            SignatureScheme::Secp256k1 => {
                let signature = secp256k1::Secp256k1Signature::from_slice(&bytes[1..])?;
                Ok(AccountSignature::Secp256k1(signature))
            }
        }
    }
}

impl FromStr for AccountPublicKey {
    type Err = CryptoError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value = hex::decode(s)?;
        AccountPublicKey::from_slice(value.as_slice())
    }
}

impl Display for AccountPublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.as_bytes()))
    }
}

impl From<AccountPublicKey> for Owner {
    fn from(public_key: AccountPublicKey) -> Self {
        match public_key {
            AccountPublicKey::Ed25519(public_key) => public_key.into(),
            AccountPublicKey::Secp256k1(public_key) => public_key.into(),
        }
    }
}

impl From<&AccountPublicKey> for Owner {
    fn from(public_key: &AccountPublicKey) -> Self {
        match public_key {
            AccountPublicKey::Ed25519(public_key) => public_key.into(),
            AccountPublicKey::Secp256k1(public_key) => public_key.into(),
        }
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
    #[error("unrecognized signature scheme flag: {0}")]
    InvalidSchemeFlag(u8),
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
    use crate::crypto::{ed25519::Ed25519SecretKey, secp256k1::Secp256k1KeyPair};

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

    #[test]
    fn roundtrip_account_pk_bytes_repr() {
        fn roundtrip_test(secret: AccountSecretKey) {
            let public = secret.public();
            let bytes = public.as_bytes();
            let parsed = AccountPublicKey::from_slice(&bytes).unwrap();
            assert_eq!(public, parsed);
        }
        roundtrip_test(AccountSecretKey::Ed25519(Ed25519SecretKey::generate()));
        roundtrip_test(AccountSecretKey::Secp256k1(
            Secp256k1KeyPair::generate().secret_key,
        ));
    }

    #[test]
    fn roundtrip_signature_bytes_repr() {
        fn roundtrip_test(secret: AccountSecretKey) {
            let test_string = TestString::new("test");
            let signature = secret.sign(&test_string);
            let bytes = signature.to_bytes();
            let parsed = AccountSignature::from_slice(&bytes).unwrap();
            assert_eq!(signature, parsed);
        }
        roundtrip_test(AccountSecretKey::Ed25519(Ed25519SecretKey::generate()));
        roundtrip_test(AccountSecretKey::Secp256k1(
            Secp256k1KeyPair::generate().secret_key,
        ));
    }

    #[test]
    fn fail_invalid_flag_byte() {
        // 0x02 is not a valid flag for any scheme.
        let invalid_scheme_flag: u8 = 0x02;
        let bytes = vec![invalid_scheme_flag, 0x01, 0x02, 0x03, 0x04];
        let result: Result<AccountPublicKey, CryptoError> = AccountPublicKey::from_slice(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn roundtrip_display_from_str_pk() {
        fn test(secret: AccountSecretKey) {
            let public = secret.public();
            let display = public.to_string();
            let parsed = AccountPublicKey::from_str(&display).unwrap();
            assert_eq!(public, parsed);
        }
        test(AccountSecretKey::Ed25519(Ed25519SecretKey::generate()));
        test(AccountSecretKey::Secp256k1(
            Secp256k1KeyPair::generate().secret_key,
        ));
    }
}

// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Define the cryptographic primitives used by the Linera protocol.

use std::{borrow::Cow, fmt, io, num::ParseIntError, str::FromStr};

use ed25519_dalek::{self as dalek, Signer, Verifier};
use generic_array::typenum::Unsigned;
use linera_witty::{
    GuestPointer, HList, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitLoad, WitStore, WitType,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
#[cfg(with_testing)]
use {
    proptest::{
        collection::{vec, VecStrategy},
        prelude::Arbitrary,
        strategy::{self, Strategy},
    },
    std::ops::RangeInclusive,
};

use crate::doc_scalar;

/// A signature key-pair.
pub struct KeyPair(dalek::SigningKey);

/// A signature public key.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash)]
pub struct PublicKey(pub [u8; dalek::PUBLIC_KEY_LENGTH]);

type HasherOutputSize = <sha3::Sha3_256 as sha3::digest::OutputSizeUser>::OutputSize;
type HasherOutput = generic_array::GenericArray<u8, HasherOutputSize>;

/// A Sha3-256 value.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Copy, Hash)]
#[cfg_attr(with_testing, derive(Default))]
pub struct CryptoHash(HasherOutput);

/// A vector of cryptographic hashes.
/// This is used to represent a hash of a list of hashes.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Hash, Serialize, Deserialize)]
#[cfg_attr(with_testing, derive(Default))]
pub struct CryptoHashVec(pub Vec<CryptoHash>);

impl<'de> BcsHashable<'de> for CryptoHashVec {}

/// A signature value.
#[derive(Eq, PartialEq, Copy, Clone)]
pub struct Signature(pub dalek::Signature);

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
        expected = HasherOutputSize::to_usize(),
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
        let key =
            dalek::SigningKey::from_bytes(value[..].try_into().map_err(serde::de::Error::custom)?);
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
        CryptoHash(u64_array_to_le_bytes(integers).into())
    }
}

impl From<CryptoHash> for [u64; 4] {
    fn from(crypto_hash: CryptoHash) -> Self {
        le_bytes_to_u64_array(&crypto_hash.0)
    }
}

impl fmt::Display for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = hex::encode(self.0.to_bytes());
        write!(f, "{}", s)
    }
}

impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0[..]))
    }
}

impl fmt::Display for CryptoHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prec = f.precision().unwrap_or(self.0.len() * 2);
        hex::encode(&self.0[..((prec + 1) / 2)]).fmt(f)
    }
}

impl fmt::Debug for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0.to_bytes()[0..8]))
    }
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0[..8]))
    }
}

impl fmt::Debug for CryptoHash {
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

impl CryptoHash {
    /// Computes a hash.
    pub fn new<'de, T: BcsHashable<'de>>(value: &T) -> Self {
        use sha3::digest::Digest;

        let mut hasher = sha3::Sha3_256::default();
        value.write(&mut hasher);
        CryptoHash(hasher.finalize())
    }

    /// Reads the bytes of the hash value.
    pub fn as_bytes(&self) -> &HasherOutput {
        &self.0
    }

    /// Returns the hash of `TestString(s)`, for testing purposes.
    #[cfg(with_testing)]
    pub fn test_hash(s: impl Into<String>) -> Self {
        CryptoHash::new(&TestString::new(s))
    }
}

impl Signature {
    /// Computes a signature.
    pub fn new<'de, T>(value: &T, secret: &KeyPair) -> Self
    where
        T: BcsSignable<'de>,
    {
        let mut message = Vec::new();
        value.write(&mut message);
        let signature = secret.0.sign(&message);
        Signature(signature)
    }

    fn check_internal<'de, T>(
        &self,
        value: &T,
        author: PublicKey,
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
    pub fn check<'de, T>(&self, value: &T, author: PublicKey) -> Result<(), CryptoError>
    where
        T: BcsSignable<'de> + fmt::Debug,
    {
        self.check_internal(value, author)
            .map_err(|error| CryptoError::InvalidSignature {
                error: error.to_string(),
                type_name: T::type_name().to_string(),
            })
    }

    /// Checks an optional signature.
    pub fn check_optional_signature<'de, T>(
        signature: Option<&Self>,
        value: &T,
        author: &PublicKey,
    ) -> Result<(), CryptoError>
    where
        T: BcsSignable<'de> + fmt::Debug,
    {
        match signature {
            Some(sig) => sig.check(value, *author),
            None => Err(CryptoError::MissingSignature {
                type_name: T::type_name().to_string(),
            }),
        }
    }

    fn verify_batch_internal<'a, 'de, T, I>(
        value: &'a T,
        votes: I,
    ) -> Result<(), dalek::SignatureError>
    where
        T: BcsSignable<'de>,
        I: IntoIterator<Item = (&'a PublicKey, &'a Signature)>,
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
    pub fn verify_batch<'a, 'de, T, I>(value: &'a T, votes: I) -> Result<(), CryptoError>
    where
        T: BcsSignable<'de>,
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

impl WitType for CryptoHash {
    const SIZE: u32 = <(u64, u64, u64, u64) as WitType>::SIZE;
    type Layout = <(u64, u64, u64, u64) as WitType>::Layout;
    type Dependencies = HList![];

    fn wit_type_name() -> Cow<'static, str> {
        "crypto-hash".into()
    }

    fn wit_type_declaration() -> Cow<'static, str> {
        concat!(
            "    record crypto-hash {\n",
            "        part1: u64,\n",
            "        part2: u64,\n",
            "        part3: u64,\n",
            "        part4: u64,\n",
            "    }\n",
        )
        .into()
    }
}

impl WitLoad for CryptoHash {
    fn load<Instance>(
        memory: &Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let (part1, part2, part3, part4) = WitLoad::load(memory, location)?;
        Ok(CryptoHash::from([part1, part2, part3, part4]))
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
        Ok(CryptoHash::from([part1, part2, part3, part4]))
    }
}

impl WitStore for CryptoHash {
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

#[cfg(with_testing)]
impl Arbitrary for CryptoHash {
    type Parameters = ();
    type Strategy = strategy::Map<VecStrategy<RangeInclusive<u8>>, fn(Vec<u8>) -> CryptoHash>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        vec(u8::MIN..=u8::MAX, HasherOutputSize::to_usize()).prop_map(|vector| {
            CryptoHash(generic_array::GenericArray::clone_from_slice(&vector[..]))
        })
    }
}

impl<'de> BcsHashable<'de> for PublicKey {}

doc_scalar!(CryptoHash, "A Sha3-256 value");
doc_scalar!(PublicKey, "A signature public key");
doc_scalar!(Signature, "A signature value");

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

#[cfg(with_getrandom)]
#[test]
fn test_signatures() {
    #[derive(Debug, Serialize, Deserialize)]
    struct Foo(String);

    impl<'de> BcsSignable<'de> for Foo {}

    let key1 = KeyPair::generate();
    let addr1 = key1.public();
    let key2 = KeyPair::generate();
    let addr2 = key2.public();

    let ts = TestString("hello".into());
    let tsx = TestString("hellox".into());
    let foo = Foo("hello".into());

    let s = Signature::new(&ts, &key1);
    assert!(s.check(&ts, addr1).is_ok());
    assert!(s.check(&ts, addr2).is_err());
    assert!(s.check(&tsx, addr1).is_err());
    assert!(s.check(&foo, addr1).is_err());
}

/// Reads the `bytes` as four little-endian unsigned 64-bit integers and returns them.
fn le_bytes_to_u64_array(bytes: &[u8]) -> [u64; 4] {
    let mut integers = [0u64; 4];

    integers[0] = u64::from_le_bytes(bytes[0..8].try_into().expect("incorrect indices"));
    integers[1] = u64::from_le_bytes(bytes[8..16].try_into().expect("incorrect indices"));
    integers[2] = u64::from_le_bytes(bytes[16..24].try_into().expect("incorrect indices"));
    integers[3] = u64::from_le_bytes(bytes[24..32].try_into().expect("incorrect indices"));

    integers
}

/// Returns the bytes that represent the `integers` in little-endian.
fn u64_array_to_le_bytes(integers: [u64; 4]) -> [u8; 32] {
    let mut bytes = [0u8; 32];

    bytes[0..8].copy_from_slice(&integers[0].to_le_bytes());
    bytes[8..16].copy_from_slice(&integers[1].to_le_bytes());
    bytes[16..24].copy_from_slice(&integers[2].to_le_bytes());
    bytes[24..32].copy_from_slice(&integers[3].to_le_bytes());

    bytes
}

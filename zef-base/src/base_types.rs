// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::error::Error;
use ed25519_dalek as dalek;
use ed25519_dalek::{Signer, Verifier};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use std::{
    convert::{TryFrom, TryInto},
    str::FromStr,
};

#[cfg(test)]
#[path = "unit_tests/base_types_tests.rs"]
mod base_types_tests;

/// A non-negative amount of money to be transferred.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Serialize, Deserialize,
)]
pub struct Amount(u64);

/// The balance of a chain.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Serialize, Deserialize,
)]
pub struct Balance(u128);

/// A block height to identify blocks in a chain.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Serialize, Deserialize,
)]
pub struct BlockHeight(pub u64);

/// A number to identify successive attempts to decide a value in a consensus protocol.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Serialize, Deserialize,
)]
pub struct RoundNumber(pub u64);

/// Optional user message attached to a transfer.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Hash, Default, Debug, Serialize, Deserialize)]
pub struct UserData(pub Option<[u8; 32]>);

/// A signature key-pair.
pub struct KeyPair(dalek::Keypair);

/// A signature public key.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash)]
pub struct PublicKey(pub [u8; dalek::PUBLIC_KEY_LENGTH]);

/// The index of an operation in a chain.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize)]
pub struct OperationId {
    pub chain_id: ChainId,
    pub height: BlockHeight,
}

/// How to create a chain.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize)]
pub enum ChainDescription {
    /// The chain was created by the genesis configuration.
    Root(usize),
    /// The chain was created by an operation from another chain.
    Child(OperationId),
}

impl BcsSignable for ChainDescription {}

/// The unique identifier (UID) of a chain. This is the hash value of a ChainDescription.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Serialize, Deserialize)]
pub struct ChainId(pub HashValue);

/// A Sha512 value.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Copy, Hash)]
pub struct HashValue(generic_array::GenericArray<u8, <sha2::Sha512 as sha2::Digest>::OutputSize>);

/// Alias for the identity of a validator.
pub type ValidatorName = PublicKey;

/// Alias for the authentication method of a chain.
pub type Owner = PublicKey;

/// A signature value.
#[derive(Eq, PartialEq, Copy, Clone)]
pub struct Signature(dalek::Signature);

impl PublicKey {
    // For testing only
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
        serializer.serialize_str(&hex::encode(&self.0.to_bytes()))
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
            serializer.serialize_str(&hex::encode(&self.0.to_bytes()))
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

impl FromStr for PublicKey {
    type Err = failure::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value = hex::decode(s)?;
        if value.len() != dalek::PUBLIC_KEY_LENGTH {
            failure::bail!("Invalid length for hex-encoded public key");
        }
        let mut pubkey = [0u8; dalek::PUBLIC_KEY_LENGTH];
        pubkey.copy_from_slice(&value[..dalek::PUBLIC_KEY_LENGTH]);
        Ok(PublicKey(pubkey))
    }
}

impl FromStr for HashValue {
    type Err = failure::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value = hex::decode(s)?;
        if value.len() != <sha2::Sha512 as sha2::Digest>::output_size() {
            failure::bail!("Invalid length for hex-encoded hash value");
        }
        let mut bytes =
            generic_array::GenericArray::<u8, <sha2::Sha512 as sha2::Digest>::OutputSize>::default(
            );
        bytes.copy_from_slice(&value[..<sha2::Sha512 as sha2::Digest>::output_size()]);
        Ok(Self(bytes))
    }
}

impl std::fmt::Display for ChainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for ChainId {
    type Err = failure::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ChainId(HashValue::from_str(s)?))
    }
}

impl std::fmt::Display for Signature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let s = hex::encode(&self.0.to_bytes());
        write!(f, "{}", s)
    }
}

impl std::fmt::Debug for Signature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{}", self)
    }
}

impl std::fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{}", self)
    }
}

impl std::fmt::Debug for HashValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{}", self)
    }
}

impl std::fmt::Debug for ChainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{}", self)
    }
}

impl From<ChainDescription> for ChainId {
    fn from(description: ChainDescription) -> Self {
        Self(HashValue::new(&description))
    }
}

impl ChainId {
    pub fn root(index: usize) -> Self {
        Self(HashValue::new(&ChainDescription::Root(index)))
    }

    pub fn child(id: OperationId) -> Self {
        Self(HashValue::new(&ChainDescription::Child(id)))
    }
}

impl Amount {
    #[inline]
    pub fn zero() -> Self {
        Amount(0)
    }

    #[inline]
    pub fn try_add(self, other: Self) -> Result<Self, Error> {
        let val = self.0.checked_add(other.0).ok_or(Error::AmountOverflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_sub(self, other: Self) -> Result<Self, Error> {
        let val = self.0.checked_sub(other.0).ok_or(Error::AmountUnderflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_add_assign(&mut self, other: Self) -> Result<(), Error> {
        self.0 = self.0.checked_add(other.0).ok_or(Error::AmountOverflow)?;
        Ok(())
    }

    #[inline]
    pub fn try_sub_assign(&mut self, other: Self) -> Result<(), Error> {
        self.0 = self.0.checked_sub(other.0).ok_or(Error::AmountUnderflow)?;
        Ok(())
    }
}

impl Balance {
    #[inline]
    pub fn zero() -> Self {
        Balance(0)
    }

    #[inline]
    pub fn max() -> Self {
        Balance(std::u128::MAX)
    }

    #[inline]
    pub fn try_add(self, other: Self) -> Result<Self, Error> {
        let val = self.0.checked_add(other.0).ok_or(Error::BalanceOverflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_sub(self, other: Self) -> Result<Self, Error> {
        let val = self.0.checked_sub(other.0).ok_or(Error::BalanceUnderflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_add_assign(&mut self, other: Self) -> Result<(), Error> {
        self.0 = self.0.checked_add(other.0).ok_or(Error::BalanceOverflow)?;
        Ok(())
    }

    #[inline]
    pub fn try_sub_assign(&mut self, other: Self) -> Result<(), Error> {
        self.0 = self.0.checked_sub(other.0).ok_or(Error::BalanceUnderflow)?;
        Ok(())
    }
}

impl std::fmt::Display for Balance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for BlockHeight {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for Amount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for Balance {
    type Err = std::num::ParseIntError;

    fn from_str(src: &str) -> Result<Self, Self::Err> {
        Ok(Self(u128::from_str(src)?))
    }
}

impl std::str::FromStr for Amount {
    type Err = std::num::ParseIntError;

    fn from_str(src: &str) -> Result<Self, Self::Err> {
        Ok(Self(u64::from_str(src)?))
    }
}

impl From<Amount> for u64 {
    fn from(val: Amount) -> Self {
        val.0
    }
}

impl From<Amount> for Balance {
    fn from(val: Amount) -> Self {
        Balance(val.0 as u128)
    }
}

impl TryFrom<Balance> for Amount {
    type Error = std::num::TryFromIntError;

    fn try_from(val: Balance) -> Result<Self, Self::Error> {
        Ok(Amount(val.0.try_into()?))
    }
}

impl BlockHeight {
    #[inline]
    pub fn new() -> Self {
        BlockHeight(0)
    }

    #[inline]
    pub fn max() -> Self {
        BlockHeight(0x7fff_ffff_ffff_ffff)
    }

    #[inline]
    pub fn try_add_one(self) -> Result<BlockHeight, Error> {
        let val = self.0.checked_add(1).ok_or(Error::SequenceOverflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_sub_one(self) -> Result<BlockHeight, Error> {
        let val = self.0.checked_sub(1).ok_or(Error::SequenceUnderflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_add_assign_one(&mut self) -> Result<(), Error> {
        self.0 = self.0.checked_add(1).ok_or(Error::SequenceOverflow)?;
        Ok(())
    }

    #[inline]
    pub fn try_sub_assign_one(&mut self) -> Result<(), Error> {
        self.0 = self.0.checked_sub(1).ok_or(Error::SequenceUnderflow)?;
        Ok(())
    }
}

impl RoundNumber {
    #[inline]
    pub fn new() -> Self {
        RoundNumber(0)
    }

    #[inline]
    pub fn max() -> Self {
        RoundNumber(0x7fff_ffff_ffff_ffff)
    }

    #[inline]
    pub fn try_add_one(self) -> Result<RoundNumber, Error> {
        let val = self.0.checked_add(1).ok_or(Error::SequenceOverflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_sub_one(self) -> Result<RoundNumber, Error> {
        let val = self.0.checked_sub(1).ok_or(Error::SequenceUnderflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_add_assign_one(&mut self) -> Result<(), Error> {
        self.0 = self.0.checked_add(1).ok_or(Error::SequenceOverflow)?;
        Ok(())
    }

    #[inline]
    pub fn try_sub_assign_one(&mut self) -> Result<(), Error> {
        self.0 = self.0.checked_sub(1).ok_or(Error::SequenceUnderflow)?;
        Ok(())
    }
}

impl From<BlockHeight> for u64 {
    fn from(val: BlockHeight) -> Self {
        val.0
    }
}

impl From<u64> for Amount {
    fn from(value: u64) -> Self {
        Amount(value)
    }
}

impl From<u128> for Balance {
    fn from(value: u128) -> Self {
        Balance(value)
    }
}

impl From<u64> for BlockHeight {
    fn from(value: u64) -> Self {
        BlockHeight(value)
    }
}

impl From<BlockHeight> for usize {
    fn from(value: BlockHeight) -> Self {
        value.0 as usize
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
        self.0.as_ref().try_into().expect("unexpected size")
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

    pub fn check<T>(&self, value: &T, author: PublicKey) -> Result<(), Error>
    where
        T: Signable<Vec<u8>> + std::fmt::Debug,
    {
        self.check_internal(value, author)
            .map_err(|error| Error::InvalidSignature {
                error: error.to_string(),
                type_name: T::type_name().to_string(),
            })
    }

    fn verify_batch_internal<'a, T, I>(value: &'a T, votes: I) -> Result<(), dalek::SignatureError>
    where
        T: Signable<Vec<u8>>,
        I: IntoIterator<Item = &'a (PublicKey, Signature)>,
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

    pub fn verify_batch<'a, T, I>(value: &'a T, votes: I) -> Result<(), Error>
    where
        T: Signable<Vec<u8>>,
        I: IntoIterator<Item = &'a (PublicKey, Signature)>,
    {
        Signature::verify_batch_internal(value, votes).map_err(|error| Error::InvalidSignature {
            error: format!("batched {}", error),
            type_name: T::type_name().to_string(),
        })
    }
}

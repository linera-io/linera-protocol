// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::crypto::{BcsHashable, CryptoHash, PublicKey};
use serde::{Deserialize, Serialize};
use std::{str::FromStr, time::SystemTime};
use thiserror::Error;

use crate::crypto::CryptoError;
#[cfg(not(target_arch = "wasm32"))]
use std::fmt;
#[cfg(any(test, feature = "test"))]
use test_strategy::Arbitrary;

#[cfg(not(target_arch = "wasm32"))]
use chrono::NaiveDateTime;

/// A block height to identify blocks in a chain.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Serialize, Deserialize,
)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary))]
pub struct BlockHeight(pub u64);

/// A number to identify successive attempts to decide a value in a consensus protocol.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Serialize, Deserialize,
)]
pub struct RoundNumber(pub u64);

/// A timestamp, in microseconds since the Unix epoch.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Serialize, Deserialize,
)]
pub struct Timestamp(u64);

impl Timestamp {
    /// Returns the current time according to the system clock.
    pub fn now() -> Timestamp {
        Timestamp(
            SystemTime::UNIX_EPOCH
                .elapsed()
                .expect("system time should be after Unix epoch")
                .as_micros()
                .try_into()
                .unwrap_or(u64::MAX),
        )
    }

    /// Returns the number of microseconds since the Unix epoch.
    pub fn micros(&self) -> u64 {
        self.0
    }

    /// Returns the number of microseconds from `other` until `self`, or `0` if `other` is not
    /// earlier than `self`.
    pub fn saturating_diff_micros(&self, other: Timestamp) -> u64 {
        self.0.saturating_sub(other.0)
    }
}

impl From<u64> for Timestamp {
    fn from(t: u64) -> Timestamp {
        Timestamp(t)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(date_time) = NaiveDateTime::from_timestamp_opt(
            (self.0 / 1_000_000) as i64,
            ((self.0 % 1_000_000) * 1_000) as u32,
        ) {
            return date_time.fmt(f);
        }
        self.0.fmt(f)
    }
}

/// The owner of a chain. This is currently the hash of the owner's public key used to
/// verify signatures.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize)]
pub struct Owner(pub CryptoHash);

/// How to create a chain.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize)]
pub enum ChainDescription {
    /// The chain was created by the genesis configuration.
    Root(usize),
    /// The chain was created by an effect from another chain.
    Child(EffectId),
}

/// The unique identifier (UID) of a chain. This is currently computed as the hash value
/// of a [`ChainDescription`].
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary))]
pub struct ChainId(pub CryptoHash);

/// The index of an effect in a chain.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize)]
pub struct EffectId {
    pub chain_id: ChainId,
    pub height: BlockHeight,
    pub index: usize,
}

#[derive(Debug, Error)]
/// An error type for arithmetic errors.
pub enum ArithmeticError {
    #[error("Number overflow")]
    Overflow,
    #[error("Number underflow")]
    Underflow,
}

impl std::fmt::Display for BlockHeight {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for Owner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl BlockHeight {
    #[inline]
    pub fn max() -> Self {
        BlockHeight(0x7fff_ffff_ffff_ffff)
    }

    #[inline]
    pub fn try_add_one(self) -> Result<BlockHeight, ArithmeticError> {
        let val = self.0.checked_add(1).ok_or(ArithmeticError::Overflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_sub_one(self) -> Result<BlockHeight, ArithmeticError> {
        let val = self.0.checked_sub(1).ok_or(ArithmeticError::Underflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_add_assign_one(&mut self) -> Result<(), ArithmeticError> {
        self.0 = self.0.checked_add(1).ok_or(ArithmeticError::Overflow)?;
        Ok(())
    }

    #[inline]
    pub fn try_sub_assign_one(&mut self) -> Result<(), ArithmeticError> {
        self.0 = self.0.checked_sub(1).ok_or(ArithmeticError::Underflow)?;
        Ok(())
    }
}

impl RoundNumber {
    #[inline]
    pub fn max() -> Self {
        RoundNumber(0x7fff_ffff_ffff_ffff)
    }

    #[inline]
    pub fn try_add_one(self) -> Result<RoundNumber, ArithmeticError> {
        let val = self.0.checked_add(1).ok_or(ArithmeticError::Overflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_sub_one(self) -> Result<RoundNumber, ArithmeticError> {
        let val = self.0.checked_sub(1).ok_or(ArithmeticError::Underflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_add_assign_one(&mut self) -> Result<(), ArithmeticError> {
        self.0 = self.0.checked_add(1).ok_or(ArithmeticError::Overflow)?;
        Ok(())
    }

    #[inline]
    pub fn try_sub_assign_one(&mut self) -> Result<(), ArithmeticError> {
        self.0 = self.0.checked_sub(1).ok_or(ArithmeticError::Underflow)?;
        Ok(())
    }
}

impl From<BlockHeight> for u64 {
    fn from(val: BlockHeight) -> Self {
        val.0
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

impl From<PublicKey> for Owner {
    fn from(value: PublicKey) -> Self {
        Self(CryptoHash::new(&value))
    }
}

impl From<&PublicKey> for Owner {
    fn from(value: &PublicKey) -> Self {
        Self(CryptoHash::new(value))
    }
}

impl std::str::FromStr for Owner {
    type Err = CryptoError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Owner(CryptoHash::from_str(s)?))
    }
}

impl std::fmt::Display for ChainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for ChainId {
    type Err = CryptoError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ChainId(CryptoHash::from_str(s)?))
    }
}

impl TryFrom<&[u8]> for ChainId {
    type Error = CryptoError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(ChainId(CryptoHash::try_from(value)?))
    }
}

impl std::fmt::Debug for ChainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{:?}", self.0)
    }
}

impl From<ChainDescription> for ChainId {
    fn from(description: ChainDescription) -> Self {
        Self(CryptoHash::new(&description))
    }
}

impl ChainId {
    pub fn root(index: usize) -> Self {
        Self(CryptoHash::new(&ChainDescription::Root(index)))
    }

    pub fn child(id: EffectId) -> Self {
        Self(CryptoHash::new(&ChainDescription::Child(id)))
    }
}

impl BcsHashable for ChainDescription {}

#[test]
fn test_max_block_height() {
    let max = BlockHeight::max();
    assert_eq!(max.0 * 2 + 1, std::u64::MAX);
}

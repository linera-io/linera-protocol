// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::crypto::{BcsSignable, HashValue, PublicKey};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use thiserror::Error;

use crate::crypto::CryptoError;
#[cfg(any(test, feature = "test"))]
use test_strategy::Arbitrary;

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

/// The identity of a validator.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize)]
pub struct ValidatorName(pub PublicKey);

/// The owner of a chain.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize)]
pub struct Owner(pub PublicKey);

/// A number identifying the configuration of the chain (aka the committee).
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Serialize, Deserialize,
)]
pub struct Epoch(pub u64);

/// How to create a chain.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize)]
pub enum ChainDescription {
    /// The chain was created by the genesis configuration.
    Root(usize),
    /// The chain was created by an effect from another chain.
    Child(EffectId),
}

/// The unique identifier (UID) of a chain. This is the hash value of a ChainDescription.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary))]
pub struct ChainId(pub HashValue);

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
    #[error("Sequence number overflow")]
    SequenceOverflow,
    #[error("Sequence number underflow")]
    SequenceUnderflow,
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

impl std::fmt::Display for ValidatorName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for ValidatorName {
    type Err = CryptoError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ValidatorName(PublicKey::from_str(s)?))
    }
}

impl BlockHeight {
    #[inline]
    pub fn max() -> Self {
        BlockHeight(0x7fff_ffff_ffff_ffff)
    }

    #[inline]
    pub fn try_add_one(self) -> Result<BlockHeight, ArithmeticError> {
        let val = self
            .0
            .checked_add(1)
            .ok_or(ArithmeticError::SequenceOverflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_sub_one(self) -> Result<BlockHeight, ArithmeticError> {
        let val = self
            .0
            .checked_sub(1)
            .ok_or(ArithmeticError::SequenceUnderflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_add_assign_one(&mut self) -> Result<(), ArithmeticError> {
        self.0 = self
            .0
            .checked_add(1)
            .ok_or(ArithmeticError::SequenceOverflow)?;
        Ok(())
    }

    #[inline]
    pub fn try_sub_assign_one(&mut self) -> Result<(), ArithmeticError> {
        self.0 = self
            .0
            .checked_sub(1)
            .ok_or(ArithmeticError::SequenceUnderflow)?;
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
        let val = self
            .0
            .checked_add(1)
            .ok_or(ArithmeticError::SequenceOverflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_sub_one(self) -> Result<RoundNumber, ArithmeticError> {
        let val = self
            .0
            .checked_sub(1)
            .ok_or(ArithmeticError::SequenceUnderflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_add_assign_one(&mut self) -> Result<(), ArithmeticError> {
        self.0 = self
            .0
            .checked_add(1)
            .ok_or(ArithmeticError::SequenceOverflow)?;
        Ok(())
    }

    #[inline]
    pub fn try_sub_assign_one(&mut self) -> Result<(), ArithmeticError> {
        self.0 = self
            .0
            .checked_sub(1)
            .ok_or(ArithmeticError::SequenceUnderflow)?;
        Ok(())
    }
}

impl Epoch {
    #[inline]
    pub fn try_add_one(self) -> Result<Self, ArithmeticError> {
        let val = self
            .0
            .checked_add(1)
            .ok_or(ArithmeticError::SequenceOverflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_add_assign_one(&mut self) -> Result<(), ArithmeticError> {
        self.0 = self
            .0
            .checked_add(1)
            .ok_or(ArithmeticError::SequenceOverflow)?;
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

impl From<u64> for Epoch {
    fn from(value: u64) -> Self {
        Epoch(value)
    }
}

impl From<PublicKey> for ValidatorName {
    fn from(value: PublicKey) -> Self {
        Self(value)
    }
}

impl From<PublicKey> for Owner {
    fn from(value: PublicKey) -> Self {
        Self(value)
    }
}

impl std::str::FromStr for Owner {
    type Err = CryptoError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Owner(PublicKey::from_str(s)?))
    }
}

impl std::fmt::Display for ChainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for ChainId {
    type Err = CryptoError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ChainId(HashValue::from_str(s)?))
    }
}

impl TryFrom<&[u8]> for ChainId {
    type Error = CryptoError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(ChainId(HashValue::try_from(value)?))
    }
}

impl std::fmt::Debug for ChainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{:?}", self.0)
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

    pub fn child(id: EffectId) -> Self {
        Self(HashValue::new(&ChainDescription::Child(id)))
    }
}

impl BcsSignable for ChainDescription {}

#[test]
fn test_max_block_height() {
    let max = BlockHeight::max();
    assert_eq!(max.0 * 2 + 1, std::u64::MAX);
}

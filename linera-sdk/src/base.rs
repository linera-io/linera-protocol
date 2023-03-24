// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

#[cfg(not(target_arch = "wasm32"))]
pub use linera_base::crypto::BcsSignable;

/// Activate the blanket implementation of `Signable` based on serde and BCS.
/// * We use `serde_name` to extract a seed from the name of structs and enums.
/// * We use `BCS` to generate canonical bytes suitable for hashing and signing.
#[cfg(target_arch = "wasm32")]
pub trait BcsSignable: Serialize + serde::de::DeserializeOwned {}

/// The index of an effect in a chain.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Deserialize, Serialize)]
pub struct EffectId {
    pub chain_id: ChainId,
    pub height: BlockHeight,
    pub index: u64,
}

/// The unique identifier (UID) of a chain. This is the hash value of a `ChainDescription`.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Deserialize, Serialize)]
pub struct ChainId(pub CryptoHash);

/// The owner of an account (aka address).
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Deserialize, Serialize)]
pub struct Owner(pub CryptoHash);

/// The name of a subscription channel.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct ChannelName(pub Vec<u8>);

/// A block height to identify blocks in a chain.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Deserialize, Serialize,
)]
pub struct BlockHeight(pub u64);

/// A Sha3-256 value.
#[serde_as]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct CryptoHash(#[serde_as(as = "[_; 32]")] [u8; 32]);

impl From<[u64; 4]> for CryptoHash {
    fn from(integers: [u64; 4]) -> Self {
        let mut bytes = [0u8; 32];

        bytes[0..8].copy_from_slice(&integers[0].to_le_bytes());
        bytes[8..16].copy_from_slice(&integers[1].to_le_bytes());
        bytes[16..24].copy_from_slice(&integers[2].to_le_bytes());
        bytes[24..32].copy_from_slice(&integers[3].to_le_bytes());

        CryptoHash(bytes)
    }
}

impl From<CryptoHash> for [u64; 4] {
    fn from(crypto_hash: CryptoHash) -> Self {
        let bytes = crypto_hash.0;
        let mut integers = [0u64; 4];

        integers[0] = u64::from_le_bytes(bytes[0..8].try_into().expect("incorrect indices"));
        integers[1] = u64::from_le_bytes(bytes[8..16].try_into().expect("incorrect indices"));
        integers[2] = u64::from_le_bytes(bytes[16..24].try_into().expect("incorrect indices"));
        integers[3] = u64::from_le_bytes(bytes[24..32].try_into().expect("incorrect indices"));

        integers
    }
}

/// The destination of a message, relative to a particular application.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Deserialize, Serialize)]
pub enum Destination {
    /// Direct message to a chain.
    Recipient(ChainId),
    /// Broadcast to the current subscribers of our channel.
    Subscribers(ChannelName),
}

impl From<ChainId> for Destination {
    fn from(chain_id: ChainId) -> Self {
        Destination::Recipient(chain_id)
    }
}

/// A unique identifier for an application.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Deserialize, Serialize)]
pub struct ApplicationId {
    /// The bytecode to use for the application.
    pub bytecode: BytecodeId,
    /// The unique ID of the application's creation.
    pub creation: EffectId,
}

/// A unique identifier for an application bytecode.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct BytecodeId(pub EffectId);

/// The identifier of a session.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize)]
pub struct SessionId {
    /// The application that runs the session.
    pub application_id: ApplicationId,
    /// User-defined tag.
    pub kind: u64,
    /// Unique index set by the runtime.
    pub index: u64,
}

/// The balance of a chain.
#[derive(Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize)]
pub struct SystemBalance(pub u128);

/// A timestamp, in microseconds since the Unix epoch.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Deserialize, Serialize,
)]
pub struct Timestamp(u64);

impl Timestamp {
    /// Returns the number of microseconds since the Unix epoch.
    pub fn micros(&self) -> u64 {
        self.0
    }
}

impl From<u64> for Timestamp {
    fn from(t: u64) -> Timestamp {
        Timestamp(t)
    }
}

#[derive(Default, Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Amount(u64);

#[derive(Debug, thiserror::Error)]
/// An error type for arithmetic errors.
pub enum ArithmeticError {
    #[error("Number overflow")]
    Overflow,
    #[error("Number underflow")]
    Underflow,
}

impl Amount {
    pub fn zero() -> Self {
        Amount(0)
    }

    pub fn try_add(self, other: Self) -> Result<Self, ArithmeticError> {
        let val = self
            .0
            .checked_add(other.0)
            .ok_or(ArithmeticError::Overflow)?;
        Ok(Self(val))
    }

    pub fn saturating_add(self, other: Self) -> Self {
        let val = self.0.saturating_add(other.0);
        Self(val)
    }

    pub fn try_sub(self, other: Self) -> Result<Self, ArithmeticError> {
        let val = self
            .0
            .checked_sub(other.0)
            .ok_or(ArithmeticError::Underflow)?;
        Ok(Self(val))
    }

    pub fn saturating_add_assign(&mut self, other: Self) {
        self.0 = self.0.saturating_add(other.0);
    }

    pub fn try_sub_assign(&mut self, other: Self) -> Result<(), ArithmeticError> {
        self.0 = self
            .0
            .checked_sub(other.0)
            .ok_or(ArithmeticError::Underflow)?;
        Ok(())
    }
}

impl From<Amount> for u64 {
    fn from(val: Amount) -> Self {
        val.0
    }
}

impl From<u64> for Amount {
    fn from(value: u64) -> Self {
        Amount(value)
    }
}

impl<'a> std::iter::Sum<&'a Amount> for Amount {
    fn sum<I: Iterator<Item = &'a Self>>(iter: I) -> Self {
        iter.fold(Self::zero(), |a, b| Amount(a.0 + b.0))
    }
}

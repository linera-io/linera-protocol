// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    bcs_scalar,
    crypto::{BcsHashable, CryptoError, CryptoHash, PublicKey},
    data_types::BlockHeight,
    doc_scalar,
};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[cfg(any(test, feature = "test"))]
use test_strategy::Arbitrary;

/// The owner of a chain. This is currently the hash of the owner's public key used to
/// verify signatures.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Default))]
pub struct Owner(pub CryptoHash);

/// How to create a chain.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize)]
pub enum ChainDescription {
    /// The chain was created by the genesis configuration.
    Root(u32),
    /// The chain was created by an effect from another chain.
    Child(EffectId),
}

/// The unique identifier (UID) of a chain. This is currently computed as the hash value
/// of a [`ChainDescription`].
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary, Default))]
pub struct ChainId(pub CryptoHash);

/// The index of an effect in a chain.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Default))]
pub struct EffectId {
    pub chain_id: ChainId,
    pub height: BlockHeight,
    pub index: u32,
}

/// A unique identifier for a user application.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Default))]
#[serde(rename = "UserApplicationId")]
pub struct ApplicationId {
    /// The bytecode to use for the application.
    pub bytecode_id: BytecodeId,
    /// The unique ID of the application's creation.
    pub creation: EffectId,
}

/// A unique identifier for an application bytecode.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[cfg_attr(any(test, feature = "test"), derive(Default))]
pub struct BytecodeId(pub EffectId);

/// The identifier of a session.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct SessionId {
    /// The user application that runs the session.
    pub application_id: ApplicationId,
    /// User-defined tag.
    pub kind: u64,
    /// Unique index set by the runtime.
    pub index: u64,
}

/// The name of a subscription channel.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ChannelName(#[serde(with = "serde_bytes")] Vec<u8>);

/// The destination of a message, relative to a particular application.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
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

impl AsRef<[u8]> for ChannelName {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for ChannelName {
    fn from(name: Vec<u8>) -> Self {
        ChannelName(name)
    }
}

impl ChannelName {
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl From<EffectId> for BytecodeId {
    fn from(effect_id: EffectId) -> Self {
        BytecodeId(effect_id)
    }
}

impl std::fmt::Display for Owner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        self.0.fmt(f)
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
    pub fn root(index: u32) -> Self {
        Self(CryptoHash::new(&ChainDescription::Root(index)))
    }

    pub fn child(id: EffectId) -> Self {
        Self(CryptoHash::new(&ChainDescription::Child(id)))
    }
}

impl BcsHashable for ChainDescription {}

bcs_scalar!(ApplicationId, "A unique identifier for a user application");
bcs_scalar!(
    BytecodeId,
    "A unique identifier for an application bytecode"
);
doc_scalar!(ChainDescription, "How to create a chain");
doc_scalar!(
    ChainId,
    "The unique identifier (UID) of a chain. This is currently computed as the hash value of a \
    ChainDescription."
);
doc_scalar!(ChannelName, "The name of a subscription channel");
bcs_scalar!(EffectId, "The index of an effect in a chain");
doc_scalar!(
    Owner,
    "The owner of a chain. This is currently the hash of the owner's public key used to verify \
    signatures."
);

#[cfg(test)]
mod tests {
    use super::ChainId;

    /// Verifies that chain IDs that are explicitly used in some example and test scripts don't
    /// change.
    #[test]
    fn chain_ids() {
        assert_eq!(
            &ChainId::root(0).to_string(),
            "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65"
        );
        assert_eq!(
            &ChainId::root(9).to_string(),
            "256e1dbc00482ddd619c293cc0df94d366afe7980022bb22d99e33036fd465dd"
        );
        assert_eq!(
            &ChainId::root(999).to_string(),
            "9c8a838e8f7b63194f6c7585455667a8379d2b5db19a3300e9961f0b1e9091ea"
        );
    }
}

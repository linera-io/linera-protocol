// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    bcs_scalar,
    crypto::{BcsHashable, CryptoError, CryptoHash, PublicKey},
    data_types::BlockHeight,
    doc_scalar,
};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Display},
    hash::{Hash, Hasher},
    str::FromStr,
};

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
    /// The chain was created by a message from another chain.
    Child(MessageId),
}

/// The unique identifier (UID) of a chain. This is currently computed as the hash value
/// of a [`ChainDescription`].
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary, Default))]
pub struct ChainId(pub CryptoHash);

/// The index of a message in a chain.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Default))]
pub struct MessageId {
    pub chain_id: ChainId,
    pub height: BlockHeight,
    pub index: u32,
}

/// A unique identifier for a user application.
#[derive(Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Default))]
#[serde(rename = "UserApplicationId")]
pub struct ApplicationId<A = ()> {
    /// The bytecode to use for the application.
    pub bytecode_id: BytecodeId<A>,
    /// The unique ID of the application's creation.
    pub creation: MessageId,
}

/// A unique identifier for an application bytecode.
#[cfg_attr(any(test, feature = "test"), derive(Default))]
pub struct BytecodeId<A = ()> {
    pub message_id: MessageId,
    _phantom: std::marker::PhantomData<A>,
}

/// The identifier of a session.
pub struct SessionId<A = ()> {
    /// The user application that runs the session.
    pub application_id: ApplicationId<A>,
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

// Cannot use #[derive(Clone)] because it requires `A: Copy`.
impl<A> Clone for BytecodeId<A> {
    fn clone(&self) -> Self {
        Self {
            message_id: self.message_id,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<A> Copy for BytecodeId<A> {}

impl<A: PartialEq> PartialEq for BytecodeId<A> {
    fn eq(&self, other: &Self) -> bool {
        let BytecodeId {
            message_id,
            _phantom,
        } = other;
        self.message_id == *message_id
    }
}

impl<A: Eq> Eq for BytecodeId<A> {}

impl<A: PartialOrd> PartialOrd for BytecodeId<A> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let BytecodeId {
            message_id,
            _phantom,
        } = other;
        self.message_id.partial_cmp(message_id)
    }
}

impl<A: Ord> Ord for BytecodeId<A> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let BytecodeId {
            message_id,
            _phantom,
        } = other;
        self.message_id.cmp(message_id)
    }
}

impl<A> Hash for BytecodeId<A> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let BytecodeId {
            message_id,
            _phantom,
        } = self;
        message_id.hash(state);
    }
}

impl<A> Debug for BytecodeId<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let BytecodeId {
            message_id,
            _phantom,
        } = self;
        f.debug_struct("BytecodeId")
            .field("message_id", message_id)
            .finish()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "BytecodeId")]
struct SerializableBytecodeId {
    message_id: MessageId,
}

impl<A> Serialize for BytecodeId<A> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        if serializer.is_human_readable() {
            let bytes = bcs::to_bytes(&self.message_id).map_err(serde::ser::Error::custom)?;
            serializer.serialize_str(&hex::encode(bytes))
        } else {
            SerializableBytecodeId::serialize(
                &SerializableBytecodeId {
                    message_id: self.message_id,
                },
                serializer,
            )
        }
    }
}

impl<'de, A> Deserialize<'de> for BytecodeId<A> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            let message_id_bytes = hex::decode(s).map_err(serde::de::Error::custom)?;
            let message_id =
                bcs::from_bytes(&message_id_bytes).map_err(serde::de::Error::custom)?;
            Ok(BytecodeId {
                message_id,
                _phantom: std::marker::PhantomData,
            })
        } else {
            let value = SerializableBytecodeId::deserialize(deserializer)?;
            Ok(BytecodeId {
                message_id: value.message_id,
                _phantom: std::marker::PhantomData,
            })
        }
    }
}

impl BytecodeId {
    pub fn new(message_id: MessageId) -> Self {
        BytecodeId {
            message_id,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn with_abi<A>(self) -> BytecodeId<A> {
        BytecodeId {
            message_id: self.message_id,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<A> BytecodeId<A> {
    pub fn forget_abi(self) -> BytecodeId {
        BytecodeId {
            message_id: self.message_id,
            _phantom: std::marker::PhantomData,
        }
    }
}

// Cannot use #[derive(Clone)] because it requires `A: Copy`.
impl<A> Clone for ApplicationId<A> {
    fn clone(&self) -> Self {
        Self {
            bytecode_id: self.bytecode_id,
            creation: self.creation,
        }
    }
}

impl<A> Copy for ApplicationId<A> {}

impl<A: PartialEq> PartialEq for ApplicationId<A> {
    fn eq(&self, other: &Self) -> bool {
        let ApplicationId {
            bytecode_id,
            creation,
        } = other;
        self.bytecode_id == *bytecode_id && self.creation == *creation
    }
}

impl<A: Eq> Eq for ApplicationId<A> {}

impl<A: PartialOrd> PartialOrd for ApplicationId<A> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let ApplicationId {
            bytecode_id,
            creation,
        } = other;
        match self.bytecode_id.partial_cmp(bytecode_id) {
            Some(std::cmp::Ordering::Equal) => self.creation.partial_cmp(creation),
            result => result,
        }
    }
}

impl<A: Ord> Ord for ApplicationId<A> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let ApplicationId {
            bytecode_id,
            creation,
        } = other;
        match self.bytecode_id.cmp(bytecode_id) {
            std::cmp::Ordering::Equal => self.creation.cmp(creation),
            result => result,
        }
    }
}

impl<A> Hash for ApplicationId<A> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let ApplicationId {
            bytecode_id,
            creation,
        } = self;
        bytecode_id.hash(state);
        creation.hash(state);
    }
}

impl<A> Debug for ApplicationId<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ApplicationId {
            bytecode_id,
            creation,
        } = self;
        f.debug_struct("ApplicationId")
            .field("bytecode_id", bytecode_id)
            .field("creation", creation)
            .finish()
    }
}

impl ApplicationId {
    pub fn with_abi<A>(self) -> ApplicationId<A> {
        ApplicationId {
            bytecode_id: self.bytecode_id.with_abi(),
            creation: self.creation,
        }
    }
}

impl<A> ApplicationId<A> {
    pub fn forget_abi(self) -> ApplicationId {
        ApplicationId {
            bytecode_id: self.bytecode_id.forget_abi(),
            creation: self.creation,
        }
    }
}

// Cannot use #[derive(Clone)] because it requires `A: Copy`.
impl<A> Clone for SessionId<A> {
    fn clone(&self) -> Self {
        Self {
            application_id: self.application_id,
            index: self.index,
        }
    }
}

impl<A> Copy for SessionId<A> {}

impl<A: PartialEq> PartialEq for SessionId<A> {
    fn eq(&self, other: &Self) -> bool {
        let SessionId {
            application_id,
            index,
        } = other;
        self.application_id == *application_id && self.index == *index
    }
}

impl<A: Eq> Eq for SessionId<A> {}

impl<A: PartialOrd> PartialOrd for SessionId<A> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let SessionId {
            application_id,
            index,
        } = other;
        match self.application_id.partial_cmp(application_id) {
            Some(std::cmp::Ordering::Equal) => self.index.partial_cmp(index),
            result => result,
        }
    }
}

impl<A: Ord> Ord for SessionId<A> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let SessionId {
            application_id,
            index,
        } = other;
        match self.application_id.cmp(application_id) {
            std::cmp::Ordering::Equal => self.index.cmp(index),
            result => result,
        }
    }
}

impl<A> Debug for SessionId<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let SessionId {
            application_id,
            index,
        } = self;
        f.debug_struct("SessionId")
            .field("application_id", application_id)
            .field("index", index)
            .finish()
    }
}

impl SessionId {
    pub fn with_abi<A>(self) -> SessionId<A> {
        SessionId {
            application_id: self.application_id.with_abi(),
            index: self.index,
        }
    }
}

impl<A> SessionId<A> {
    pub fn forget_abi(self) -> SessionId {
        SessionId {
            application_id: self.application_id.forget_abi(),
            index: self.index,
        }
    }
}

impl Display for Owner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        Display::fmt(&self.0, f)
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

impl Display for ChainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
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

    pub fn child(id: MessageId) -> Self {
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
bcs_scalar!(MessageId, "The index of a message in a chain");
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

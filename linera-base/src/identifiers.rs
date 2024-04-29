// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Core identifiers used by the Linera protocol.

use std::{
    fmt::{Debug, Display},
    hash::{Hash, Hasher},
    str::FromStr,
};

use anyhow::{anyhow, Context};
use linera_witty::{WitLoad, WitStore, WitType};
use serde::{Deserialize, Serialize};

use crate::{
    bcs_scalar,
    crypto::{BcsHashable, CryptoError, CryptoHash, PublicKey},
    data_types::BlockHeight,
    doc_scalar,
};

/// The owner of a chain. This is currently the hash of the owner's public key used to
/// verify signatures.
#[derive(
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Copy,
    Clone,
    Hash,
    Debug,
    Serialize,
    Deserialize,
    WitLoad,
    WitStore,
    WitType,
)]
#[cfg_attr(with_testing, derive(Default, test_strategy::Arbitrary))]
pub struct Owner(pub CryptoHash);

/// An account owner.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AccountOwner {
    /// An account owned by a user.
    User(Owner),
    /// An account for an application.
    Application(ApplicationId),
}

/// A system account.
#[derive(
    Debug, PartialEq, Eq, Hash, Copy, Clone, Serialize, Deserialize, WitLoad, WitStore, WitType,
)]
pub struct Account {
    /// The chain of the account.
    pub chain_id: ChainId,
    /// The owner of the account, or `None` for the chain balance.
    pub owner: Option<Owner>,
}

impl Account {
    /// Creates an Account with a ChainId
    pub fn chain(chain_id: ChainId) -> Self {
        Account {
            chain_id,
            owner: None,
        }
    }

    /// Creates an Account with a ChainId and an Owner
    pub fn owner(chain_id: ChainId, owner: Owner) -> Self {
        Account {
            chain_id,
            owner: Some(owner),
        }
    }
}

impl std::fmt::Display for Account {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.owner {
            Some(owner) => write!(f, "{}:{}", self.chain_id, owner),
            None => write!(f, "{}", self.chain_id),
        }
    }
}

impl FromStr for Account {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.split(':').collect::<Vec<_>>();
        anyhow::ensure!(
            parts.len() <= 2,
            "Expecting format `chain-id:address` or `chain-id`"
        );
        if parts.len() == 1 {
            Ok(Account::chain(s.parse()?))
        } else {
            let chain_id = parts[0].parse()?;
            let owner = parts[1].parse()?;
            Ok(Account::owner(chain_id, owner))
        }
    }
}

/// How to create a chain.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize)]
pub enum ChainDescription {
    /// The chain was created by the genesis configuration.
    Root(u32),
    /// The chain was created by a message from another chain.
    Child(MessageId),
}

impl ChainDescription {
    /// Whether the chain was created by another chain.
    pub fn is_child(&self) -> bool {
        matches!(self, ChainDescription::Child(_))
    }
}

/// The unique identifier (UID) of a chain. This is currently computed as the hash value
/// of a [`ChainDescription`].
#[derive(
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Copy,
    Clone,
    Hash,
    Serialize,
    Deserialize,
    WitLoad,
    WitStore,
    WitType,
)]
#[cfg_attr(with_testing, derive(test_strategy::Arbitrary))]
#[cfg_attr(with_testing, derive(Default))]
pub struct ChainId(pub CryptoHash);

/// The index of a message in a chain.
#[derive(
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Copy,
    Clone,
    Hash,
    Debug,
    Serialize,
    Deserialize,
    WitLoad,
    WitStore,
    WitType,
)]
#[cfg_attr(with_testing, derive(Default))]
pub struct MessageId {
    /// The chain ID that created the message.
    pub chain_id: ChainId,
    /// The height of the block that created the message.
    pub height: BlockHeight,
    /// The index of the message inside the block.
    pub index: u32,
}

/// A unique identifier for a user application.
#[derive(WitLoad, WitStore, WitType)]
#[cfg_attr(with_testing, derive(Default))]
pub struct ApplicationId<A = ()> {
    /// The bytecode to use for the application.
    pub bytecode_id: BytecodeId<A>,
    /// The unique ID of the application's creation.
    pub creation: MessageId,
}

/// A unique identifier for an application.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize)]
pub enum GenericApplicationId {
    /// The system application.
    System,
    /// A user application.
    User(ApplicationId),
}

impl GenericApplicationId {
    /// Returns the `ApplicationId`, or `None` if it is `System`.
    pub fn user_application_id(&self) -> Option<&ApplicationId> {
        if let GenericApplicationId::User(app_id) = self {
            Some(app_id)
        } else {
            None
        }
    }
}

impl From<ApplicationId> for GenericApplicationId {
    fn from(user_application_id: ApplicationId) -> Self {
        GenericApplicationId::User(user_application_id)
    }
}

/// A unique identifier for an application bytecode.
#[derive(WitLoad, WitStore, WitType)]
#[cfg_attr(with_testing, derive(Default))]
pub struct BytecodeId<Abi = (), Parameters = (), InstantiationArgument = ()> {
    /// The message ID that published the bytecode.
    pub message_id: MessageId,
    #[witty(skip)]
    _phantom: std::marker::PhantomData<(Abi, Parameters, InstantiationArgument)>,
}

/// The name of a subscription channel.
#[derive(
    Clone,
    Debug,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    Deserialize,
    WitLoad,
    WitStore,
    WitType,
)]
pub struct ChannelName(#[serde(with = "serde_bytes")] Vec<u8>);

/// The destination of a message, relative to a particular application.
#[derive(
    Clone,
    Debug,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    Deserialize,
    WitLoad,
    WitStore,
    WitType,
)]
pub enum Destination {
    /// Direct message to a chain.
    Recipient(ChainId),
    /// Broadcast to the current subscribers of our channel.
    Subscribers(ChannelName),
}

impl Destination {
    /// Whether the destination is a broadcast channel.
    pub fn is_channel(&self) -> bool {
        matches!(self, Destination::Subscribers(_))
    }
}

impl From<ChainId> for Destination {
    fn from(chain_id: ChainId) -> Self {
        Destination::Recipient(chain_id)
    }
}

impl From<ChannelName> for Destination {
    fn from(channel_name: ChannelName) -> Self {
        Destination::Subscribers(channel_name)
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
    /// Turns the channel into bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

// Cannot use #[derive(Clone)] because it requires `A: Clone`.
impl<Abi, Parameters, InstantiationArgument> Clone
    for BytecodeId<Abi, Parameters, InstantiationArgument>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<Abi, Parameters, InstantiationArgument> Copy
    for BytecodeId<Abi, Parameters, InstantiationArgument>
{
}

impl<Abi: PartialEq, Parameters, InstantiationArgument> PartialEq
    for BytecodeId<Abi, Parameters, InstantiationArgument>
{
    fn eq(&self, other: &Self) -> bool {
        let BytecodeId {
            message_id,
            _phantom,
        } = other;
        self.message_id == *message_id
    }
}

impl<Abi: Eq, Parameters, InstantiationArgument> Eq
    for BytecodeId<Abi, Parameters, InstantiationArgument>
{
}

impl<Abi: PartialOrd, Parameters, InstantiationArgument> PartialOrd
    for BytecodeId<Abi, Parameters, InstantiationArgument>
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let BytecodeId {
            message_id,
            _phantom,
        } = other;
        self.message_id.partial_cmp(message_id)
    }
}

impl<Abi: Ord, Parameters, InstantiationArgument> Ord
    for BytecodeId<Abi, Parameters, InstantiationArgument>
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let BytecodeId {
            message_id,
            _phantom,
        } = other;
        self.message_id.cmp(message_id)
    }
}

impl<Abi, Parameters, InstantiationArgument> Hash
    for BytecodeId<Abi, Parameters, InstantiationArgument>
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        let BytecodeId {
            message_id,
            _phantom,
        } = self;
        message_id.hash(state);
    }
}

impl<Abi, Parameters, InstantiationArgument> Debug
    for BytecodeId<Abi, Parameters, InstantiationArgument>
{
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

impl<Abi, Parameters, InstantiationArgument> Serialize
    for BytecodeId<Abi, Parameters, InstantiationArgument>
{
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

impl<'de, Abi, Parameters, InstantiationArgument> Deserialize<'de>
    for BytecodeId<Abi, Parameters, InstantiationArgument>
{
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
    /// Creates a bytecode ID from a message ID.
    pub fn new(message_id: MessageId) -> Self {
        BytecodeId {
            message_id,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Specializes a bytecode ID for a given ABI.
    pub fn with_abi<Abi, Parameters, InstantiationArgument>(
        self,
    ) -> BytecodeId<Abi, Parameters, InstantiationArgument> {
        BytecodeId {
            message_id: self.message_id,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<Abi, Parameters, InstantiationArgument> BytecodeId<Abi, Parameters, InstantiationArgument> {
    /// Forgets the ABI of a bytecode ID (if any).
    pub fn forget_abi(self) -> BytecodeId {
        BytecodeId {
            message_id: self.message_id,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Leaves just the ABI of a bytecode ID (if any).
    pub fn just_abi(self) -> BytecodeId<Abi> {
        BytecodeId {
            message_id: self.message_id,
            _phantom: std::marker::PhantomData,
        }
    }
}

// Cannot use #[derive(Clone)] because it requires `A: Clone`.
impl<A> Clone for ApplicationId<A> {
    fn clone(&self) -> Self {
        *self
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

#[derive(Serialize, Deserialize)]
#[serde(rename = "ApplicationId")]
struct SerializableApplicationId {
    pub bytecode_id: BytecodeId,
    pub creation: MessageId,
}

impl<A> Serialize for ApplicationId<A> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        if serializer.is_human_readable() {
            let bytes = bcs::to_bytes(&SerializableApplicationId {
                bytecode_id: self.bytecode_id.forget_abi(),
                creation: self.creation,
            })
            .map_err(serde::ser::Error::custom)?;
            serializer.serialize_str(&hex::encode(bytes))
        } else {
            SerializableApplicationId::serialize(
                &SerializableApplicationId {
                    bytecode_id: self.bytecode_id.forget_abi(),
                    creation: self.creation,
                },
                serializer,
            )
        }
    }
}

impl<'de, A> Deserialize<'de> for ApplicationId<A> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            let application_id_bytes = hex::decode(s).map_err(serde::de::Error::custom)?;
            let application_id: SerializableApplicationId =
                bcs::from_bytes(&application_id_bytes).map_err(serde::de::Error::custom)?;
            Ok(ApplicationId {
                bytecode_id: application_id.bytecode_id.with_abi(),
                creation: application_id.creation,
            })
        } else {
            let value = SerializableApplicationId::deserialize(deserializer)?;
            Ok(ApplicationId {
                bytecode_id: value.bytecode_id.with_abi(),
                creation: value.creation,
            })
        }
    }
}

impl ApplicationId {
    /// Specializes an application ID for a given ABI.
    pub fn with_abi<A>(self) -> ApplicationId<A> {
        ApplicationId {
            bytecode_id: self.bytecode_id.with_abi(),
            creation: self.creation,
        }
    }
}

impl<A> ApplicationId<A> {
    /// Forgets the ABI of a bytecode ID (if any).
    pub fn forget_abi(self) -> ApplicationId {
        ApplicationId {
            bytecode_id: self.bytecode_id.forget_abi(),
            creation: self.creation,
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

#[derive(Serialize, Deserialize)]
#[serde(rename = "AccountOwner")]
enum SerializableAccountOwner {
    User(Owner),
    Application(ApplicationId),
}

impl Serialize for AccountOwner {
    fn serialize<S: serde::ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            match self {
                AccountOwner::Application(app_id) => SerializableAccountOwner::Application(*app_id),
                AccountOwner::User(owner) => SerializableAccountOwner::User(*owner),
            }
            .serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for AccountOwner {
    fn deserialize<D: serde::de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            let value = Self::from_str(&s).map_err(serde::de::Error::custom)?;
            Ok(value)
        } else {
            let value = SerializableAccountOwner::deserialize(deserializer)?;
            match value {
                SerializableAccountOwner::Application(app_id) => {
                    Ok(AccountOwner::Application(app_id))
                }
                SerializableAccountOwner::User(owner) => Ok(AccountOwner::User(owner)),
            }
        }
    }
}

impl Display for AccountOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AccountOwner::User(owner) => write!(f, "User:{}", owner)?,
            AccountOwner::Application(app_id) => write!(f, "Application:{}", app_id)?,
        };

        Ok(())
    }
}

impl FromStr for AccountOwner {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(owner) = s.strip_prefix("User:") {
            Ok(AccountOwner::User(
                Owner::from_str(owner).context("Getting Owner should not fail")?,
            ))
        } else if let Some(app_id) = s.strip_prefix("Application:") {
            Ok(AccountOwner::Application(
                ApplicationId::from_str(app_id).context("Getting ApplicationId should not fail")?,
            ))
        } else {
            Err(anyhow!("Invalid enum! Enum: {}", s))
        }
    }
}

impl<T> From<T> for AccountOwner
where
    T: Into<Owner>,
{
    fn from(owner: T) -> Self {
        AccountOwner::User(owner.into())
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
    /// The chain ID representing the N-th chain created at genesis time.
    pub fn root(index: u32) -> Self {
        Self(CryptoHash::new(&ChainDescription::Root(index)))
    }

    /// The chain ID representing the chain created by the given message.
    pub fn child(id: MessageId) -> Self {
        Self(CryptoHash::new(&ChainDescription::Child(id)))
    }
}

impl BcsHashable for ChainDescription {}

bcs_scalar!(ApplicationId, "A unique identifier for a user application");
doc_scalar!(
    GenericApplicationId,
    "A unique identifier for a user application or for the system application"
);
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
doc_scalar!(
    Destination,
    "The destination of a message, relative to a particular application."
);
doc_scalar!(AccountOwner, "An owner of an account.");
doc_scalar!(Account, "An account");

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

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Core identifiers used by the Linera protocol.

use std::{
    fmt::{self, Display, Formatter},
    hash::{Hash, Hasher},
    marker::PhantomData,
    str::FromStr,
};

use anyhow::{anyhow, Context};
use async_graphql::SimpleObject;
use custom_debug_derive::Debug;
use linera_witty::{WitLoad, WitStore, WitType};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    bcs_scalar,
    crypto::{BcsHashable, CryptoError, CryptoHash},
    data_types::BlockHeight,
    doc_scalar, hex_debug,
    vm::VmRuntime,
};

/// The owner of a chain. This is currently the hash of the owner's public key used to
/// verify signatures.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, WitLoad, WitStore, WitType)]
#[cfg_attr(with_testing, derive(Default, test_strategy::Arbitrary))]
pub struct Owner(pub CryptoHash);

/// An account owner.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, WitLoad, WitStore, WitType)]
#[cfg_attr(with_testing, derive(test_strategy::Arbitrary))]
pub enum AccountOwner {
    /// An account owned by a user.
    User(Owner),
    /// An account for an application.
    Application(ApplicationId),
    /// Chain account.
    Chain,
}

/// A system account.
#[derive(
    Debug, PartialEq, Eq, Hash, Copy, Clone, Serialize, Deserialize, WitLoad, WitStore, WitType,
)]
pub struct Account {
    /// The chain of the account.
    pub chain_id: ChainId,
    /// The owner of the account, or `None` for the chain balance.
    pub owner: AccountOwner,
}

impl Account {
    /// Creates an [`Account`] representing the balance shared by a chain's owners.
    pub fn chain(chain_id: ChainId) -> Self {
        Account {
            chain_id,
            owner: AccountOwner::Chain,
        }
    }

    /// Creates an [`Account`] for a specific [`Owner`] on a chain.
    pub fn owner(chain_id: ChainId, owner: impl Into<AccountOwner>) -> Self {
        Account {
            chain_id,
            owner: owner.into(),
        }
    }
}

impl Display for Account {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.chain_id, self.owner)
    }
}

impl FromStr for Account {
    type Err = anyhow::Error;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        let mut parts = string.splitn(2, ':');

        let chain_id = parts
            .next()
            .context(
                "Expecting an account formatted as `chain-id` or `chain-id:owner-type:address`",
            )?
            .parse()?;

        if let Some(owner_string) = parts.next() {
            let owner = owner_string.parse::<AccountOwner>()?;
            Ok(Account::owner(chain_id, owner))
        } else {
            Ok(Account::chain(chain_id))
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

/// The type of the blob.
/// Should be a 1:1 mapping of the types in `Blob`.
#[derive(
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Clone,
    Copy,
    Hash,
    Debug,
    Serialize,
    Deserialize,
    WitType,
    WitStore,
    WitLoad,
    Default,
)]
#[cfg_attr(with_testing, derive(test_strategy::Arbitrary))]
pub enum BlobType {
    /// A generic data blob.
    #[default]
    Data,
    /// A blob containing compressed contract bytecode.
    ContractBytecode,
    /// A blob containing compressed service bytecode.
    ServiceBytecode,
    /// A blob containing an application description.
    ApplicationDescription,
    /// A blob containing a committee of validators.
    Committee,
}

impl Display for BlobType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match serde_json::to_string(self) {
            Ok(s) => write!(f, "{}", s),
            Err(_) => Err(fmt::Error),
        }
    }
}

impl FromStr for BlobType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).with_context(|| format!("Invalid BlobType: {}", s))
    }
}

/// A content-addressed blob ID i.e. the hash of the `BlobContent`.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Copy, Hash, Debug, WitType, WitStore, WitLoad)]
#[cfg_attr(with_testing, derive(test_strategy::Arbitrary, Default))]
pub struct BlobId {
    /// The type of the blob.
    pub blob_type: BlobType,
    /// The hash of the blob.
    pub hash: CryptoHash,
}

impl BlobId {
    /// Creates a new `BlobId` from a `CryptoHash`. This must be a hash of the blob's bytes!
    pub fn new(hash: CryptoHash, blob_type: BlobType) -> Self {
        Self { hash, blob_type }
    }
}

impl Display for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.blob_type, self.hash)?;
        Ok(())
    }
}

impl FromStr for BlobId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.split(':').collect::<Vec<_>>();
        if parts.len() == 2 {
            let blob_type = BlobType::from_str(parts[0]).context("Invalid BlobType!")?;
            Ok(BlobId {
                hash: CryptoHash::from_str(parts[1]).context("Invalid hash!")?,
                blob_type,
            })
        } else {
            Err(anyhow!("Invalid blob ID: {}", s))
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "BlobId")]
struct BlobIdHelper {
    hash: CryptoHash,
    blob_type: BlobType,
}

impl Serialize for BlobId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            let helper = BlobIdHelper {
                hash: self.hash,
                blob_type: self.blob_type,
            };
            helper.serialize(serializer)
        }
    }
}

impl<'a> Deserialize<'a> for BlobId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'a>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            Self::from_str(&s).map_err(serde::de::Error::custom)
        } else {
            let helper = BlobIdHelper::deserialize(deserializer)?;
            Ok(BlobId::new(helper.hash, helper.blob_type))
        }
    }
}

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
#[cfg_attr(with_testing, derive(Default, test_strategy::Arbitrary))]
pub struct MessageId {
    /// The chain ID that created the message.
    pub chain_id: ChainId,
    /// The height of the block that created the message.
    pub height: BlockHeight,
    /// The index of the message inside the block.
    pub index: u32,
}

/// A unique identifier for a user application from a blob.
#[derive(Debug, WitLoad, WitStore, WitType)]
#[cfg_attr(with_testing, derive(Default, test_strategy::Arbitrary))]
pub struct ApplicationId<A = ()> {
    /// The hash of the `ApplicationDescription` this refers to.
    pub application_description_hash: CryptoHash,
    #[witty(skip)]
    #[debug(skip)]
    _phantom: PhantomData<A>,
}

/// A unique identifier for an application.
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

/// A unique identifier for a module.
#[derive(Debug, WitLoad, WitStore, WitType)]
#[cfg_attr(with_testing, derive(Default, test_strategy::Arbitrary))]
pub struct ModuleId<Abi = (), Parameters = (), InstantiationArgument = ()> {
    /// The hash of the blob containing the contract bytecode.
    pub contract_blob_hash: CryptoHash,
    /// The hash of the blob containing the service bytecode.
    pub service_blob_hash: CryptoHash,
    /// The virtual machine being used.
    pub vm_runtime: VmRuntime,
    #[witty(skip)]
    #[debug(skip)]
    _phantom: PhantomData<(Abi, Parameters, InstantiationArgument)>,
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
pub struct ChannelName(
    #[serde(with = "serde_bytes")]
    #[debug(with = "hex_debug")]
    Vec<u8>,
);

#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
/// A channel name together with its application ID.
pub struct ChannelFullName {
    /// The application owning the channel.
    pub application_id: GenericApplicationId,
    /// The name of the channel.
    pub name: ChannelName,
}

impl fmt::Display for ChannelFullName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = hex::encode(&self.name);
        match self.application_id {
            GenericApplicationId::System => write!(f, "system channel {name}"),
            GenericApplicationId::User(app_id) => write!(f, "user channel {name} for app {app_id}"),
        }
    }
}

impl ChannelFullName {
    /// Creates a full system channel name.
    pub fn system(name: ChannelName) -> Self {
        Self {
            application_id: GenericApplicationId::System,
            name,
        }
    }

    /// Creates a full user channel name.
    pub fn user(name: ChannelName, application_id: ApplicationId) -> Self {
        Self {
            application_id: application_id.into(),
            name,
        }
    }
}

/// The name of an event stream.
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
pub struct StreamName(
    #[serde(with = "serde_bytes")]
    #[debug(with = "hex_debug")]
    pub Vec<u8>,
);

impl<T> From<T> for StreamName
where
    T: Into<Vec<u8>>,
{
    fn from(name: T) -> Self {
        StreamName(name.into())
    }
}

/// An event stream ID.
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
    SimpleObject,
)]
pub struct StreamId {
    /// The application that can add events to this stream.
    pub application_id: GenericApplicationId,
    /// The name of this stream: an application can have multiple streams with different names.
    pub stream_name: StreamName,
}

impl StreamId {
    /// Creates a system stream ID with the given name.
    pub fn system(name: impl Into<StreamName>) -> Self {
        StreamId {
            application_id: GenericApplicationId::System,
            stream_name: name.into(),
        }
    }
}

/// An event identifier.
#[derive(
    Debug,
    PartialEq,
    Eq,
    Hash,
    Clone,
    Serialize,
    Deserialize,
    WitLoad,
    WitStore,
    WitType,
    SimpleObject,
)]
pub struct EventId {
    /// The ID of the chain that generated this event.
    pub chain_id: ChainId,
    /// The ID of the stream this event belongs to.
    pub stream_id: StreamId,
    /// The event key.
    pub key: Vec<u8>,
}

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
    /// Turns the channel name into bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl StreamName {
    /// Turns the stream name into bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

// Cannot use #[derive(Clone)] because it requires `A: Clone`.
impl<Abi, Parameters, InstantiationArgument> Clone
    for ModuleId<Abi, Parameters, InstantiationArgument>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<Abi, Parameters, InstantiationArgument> Copy
    for ModuleId<Abi, Parameters, InstantiationArgument>
{
}

impl<Abi, Parameters, InstantiationArgument> PartialEq
    for ModuleId<Abi, Parameters, InstantiationArgument>
{
    fn eq(&self, other: &Self) -> bool {
        let ModuleId {
            contract_blob_hash,
            service_blob_hash,
            vm_runtime,
            _phantom,
        } = other;
        self.contract_blob_hash == *contract_blob_hash
            && self.service_blob_hash == *service_blob_hash
            && self.vm_runtime == *vm_runtime
    }
}

impl<Abi, Parameters, InstantiationArgument> Eq
    for ModuleId<Abi, Parameters, InstantiationArgument>
{
}

impl<Abi, Parameters, InstantiationArgument> PartialOrd
    for ModuleId<Abi, Parameters, InstantiationArgument>
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<Abi, Parameters, InstantiationArgument> Ord
    for ModuleId<Abi, Parameters, InstantiationArgument>
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let ModuleId {
            contract_blob_hash,
            service_blob_hash,
            vm_runtime,
            _phantom,
        } = other;
        (
            self.contract_blob_hash,
            self.service_blob_hash,
            self.vm_runtime,
        )
            .cmp(&(*contract_blob_hash, *service_blob_hash, *vm_runtime))
    }
}

impl<Abi, Parameters, InstantiationArgument> Hash
    for ModuleId<Abi, Parameters, InstantiationArgument>
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        let ModuleId {
            contract_blob_hash: contract_blob_id,
            service_blob_hash: service_blob_id,
            vm_runtime: vm_runtime_id,
            _phantom,
        } = self;
        contract_blob_id.hash(state);
        service_blob_id.hash(state);
        vm_runtime_id.hash(state);
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "ModuleId")]
struct SerializableModuleId {
    contract_blob_hash: CryptoHash,
    service_blob_hash: CryptoHash,
    vm_runtime: VmRuntime,
}

impl<Abi, Parameters, InstantiationArgument> Serialize
    for ModuleId<Abi, Parameters, InstantiationArgument>
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        let serializable_module_id = SerializableModuleId {
            contract_blob_hash: self.contract_blob_hash,
            service_blob_hash: self.service_blob_hash,
            vm_runtime: self.vm_runtime,
        };
        if serializer.is_human_readable() {
            let bytes =
                bcs::to_bytes(&serializable_module_id).map_err(serde::ser::Error::custom)?;
            serializer.serialize_str(&hex::encode(bytes))
        } else {
            SerializableModuleId::serialize(&serializable_module_id, serializer)
        }
    }
}

impl<'de, Abi, Parameters, InstantiationArgument> Deserialize<'de>
    for ModuleId<Abi, Parameters, InstantiationArgument>
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            let module_id_bytes = hex::decode(s).map_err(serde::de::Error::custom)?;
            let serializable_module_id: SerializableModuleId =
                bcs::from_bytes(&module_id_bytes).map_err(serde::de::Error::custom)?;
            Ok(ModuleId {
                contract_blob_hash: serializable_module_id.contract_blob_hash,
                service_blob_hash: serializable_module_id.service_blob_hash,
                vm_runtime: serializable_module_id.vm_runtime,
                _phantom: PhantomData,
            })
        } else {
            let serializable_module_id = SerializableModuleId::deserialize(deserializer)?;
            Ok(ModuleId {
                contract_blob_hash: serializable_module_id.contract_blob_hash,
                service_blob_hash: serializable_module_id.service_blob_hash,
                vm_runtime: serializable_module_id.vm_runtime,
                _phantom: PhantomData,
            })
        }
    }
}

impl ModuleId {
    /// Creates a module ID from contract/service hashes and the VM runtime to use.
    pub fn new(
        contract_blob_hash: CryptoHash,
        service_blob_hash: CryptoHash,
        vm_runtime: VmRuntime,
    ) -> Self {
        ModuleId {
            contract_blob_hash,
            service_blob_hash,
            vm_runtime,
            _phantom: PhantomData,
        }
    }

    /// Specializes a module ID for a given ABI.
    pub fn with_abi<Abi, Parameters, InstantiationArgument>(
        self,
    ) -> ModuleId<Abi, Parameters, InstantiationArgument> {
        ModuleId {
            contract_blob_hash: self.contract_blob_hash,
            service_blob_hash: self.service_blob_hash,
            vm_runtime: self.vm_runtime,
            _phantom: PhantomData,
        }
    }
}

impl<Abi, Parameters, InstantiationArgument> ModuleId<Abi, Parameters, InstantiationArgument> {
    /// Forgets the ABI of a module ID (if any).
    pub fn forget_abi(self) -> ModuleId {
        ModuleId {
            contract_blob_hash: self.contract_blob_hash,
            service_blob_hash: self.service_blob_hash,
            vm_runtime: self.vm_runtime,
            _phantom: PhantomData,
        }
    }

    /// Leaves just the ABI of a module ID (if any).
    pub fn just_abi(self) -> ModuleId<Abi> {
        ModuleId {
            contract_blob_hash: self.contract_blob_hash,
            service_blob_hash: self.service_blob_hash,
            vm_runtime: self.vm_runtime,
            _phantom: PhantomData,
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
        self.application_description_hash == other.application_description_hash
    }
}

impl<A: Eq> Eq for ApplicationId<A> {}

impl<A: PartialOrd> PartialOrd for ApplicationId<A> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.application_description_hash
            .partial_cmp(&other.application_description_hash)
    }
}

impl<A: Ord> Ord for ApplicationId<A> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.application_description_hash
            .cmp(&other.application_description_hash)
    }
}

impl<A> Hash for ApplicationId<A> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.application_description_hash.hash(state);
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "ApplicationId")]
struct SerializableApplicationId {
    pub application_description_hash: CryptoHash,
}

impl<A> Serialize for ApplicationId<A> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        if serializer.is_human_readable() {
            let bytes = bcs::to_bytes(&SerializableApplicationId {
                application_description_hash: self.application_description_hash,
            })
            .map_err(serde::ser::Error::custom)?;
            serializer.serialize_str(&hex::encode(bytes))
        } else {
            SerializableApplicationId::serialize(
                &SerializableApplicationId {
                    application_description_hash: self.application_description_hash,
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
                application_description_hash: application_id.application_description_hash,
                _phantom: PhantomData,
            })
        } else {
            let value = SerializableApplicationId::deserialize(deserializer)?;
            Ok(ApplicationId {
                application_description_hash: value.application_description_hash,
                _phantom: PhantomData,
            })
        }
    }
}

impl ApplicationId {
    /// Creates an application ID from the application description hash.
    pub fn new(application_description_hash: CryptoHash) -> Self {
        ApplicationId {
            application_description_hash,
            _phantom: PhantomData,
        }
    }

    /// Specializes an application ID for a given ABI.
    pub fn with_abi<A>(self) -> ApplicationId<A> {
        ApplicationId {
            application_description_hash: self.application_description_hash,
            _phantom: PhantomData,
        }
    }
}

impl<A> ApplicationId<A> {
    /// Forgets the ABI of a module ID (if any).
    pub fn forget_abi(self) -> ApplicationId {
        ApplicationId {
            application_description_hash: self.application_description_hash,
            _phantom: PhantomData,
        }
    }

    /// Converts the application ID to the ID of the blob containing the
    /// `ApplicationDescription`.
    pub fn description_blob_id(self) -> BlobId {
        BlobId::new(
            self.application_description_hash,
            BlobType::ApplicationDescription,
        )
    }
}

impl Display for Owner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl std::str::FromStr for Owner {
    type Err = CryptoError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Owner(CryptoHash::from_str(s)?))
    }
}

impl Serialize for Owner {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            serializer.serialize_newtype_struct("Owner", &self.0)
        }
    }
}

impl<'de> Deserialize<'de> for Owner {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let string = String::deserialize(deserializer)?;
            Self::from_str(&string).map_err(serde::de::Error::custom)
        } else {
            deserializer.deserialize_newtype_struct("Owner", OwnerVisitor)
        }
    }
}

struct OwnerVisitor;

impl<'de> serde::de::Visitor<'de> for OwnerVisitor {
    type Value = Owner;

    fn expecting(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "an owner represented as a `CryptoHash`")
    }

    fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Owner(CryptoHash::deserialize(deserializer)?))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "AccountOwner")]
enum SerializableAccountOwner {
    User(Owner),
    Application(ApplicationId),
    Chain,
}

impl Serialize for AccountOwner {
    fn serialize<S: serde::ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            match self {
                AccountOwner::Application(app_id) => SerializableAccountOwner::Application(*app_id),
                AccountOwner::User(owner) => SerializableAccountOwner::User(*owner),
                AccountOwner::Chain => SerializableAccountOwner::Chain,
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
                SerializableAccountOwner::Chain => Ok(AccountOwner::Chain),
            }
        }
    }
}

impl Display for AccountOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AccountOwner::User(owner) => write!(f, "User:{}", owner)?,
            AccountOwner::Application(app_id) => write!(f, "Application:{}", app_id)?,
            AccountOwner::Chain => write!(f, "Chain")?,
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
        } else if s.strip_prefix("Chain").is_some() {
            Ok(AccountOwner::Chain)
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

impl fmt::Debug for ChainId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
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

impl<'de> BcsHashable<'de> for ChainDescription {}

bcs_scalar!(ApplicationId, "A unique identifier for a user application");
doc_scalar!(
    GenericApplicationId,
    "A unique identifier for a user application or for the system application"
);
bcs_scalar!(ModuleId, "A unique identifier for an application module");
doc_scalar!(ChainDescription, "How to create a chain");
doc_scalar!(
    ChainId,
    "The unique identifier (UID) of a chain. This is currently computed as the hash value of a \
    ChainDescription."
);
doc_scalar!(ChannelName, "The name of a subscription channel");
doc_scalar!(StreamName, "The name of an event stream");
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
doc_scalar!(
    BlobId,
    "A content-addressed blob ID i.e. the hash of the `BlobContent`"
);
doc_scalar!(
    ChannelFullName,
    "A channel name together with its application ID."
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
            "aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8"
        );
        assert_eq!(
            &ChainId::root(1).to_string(),
            "a3edc33d8e951a1139333be8a4b56646b5598a8f51216e86592d881808972b07"
        );
        assert_eq!(
            &ChainId::root(2).to_string(),
            "678e9f66507069d38955b593e93ddf192a23a4087225fd307eadad44e5544ae3"
        );
        assert_eq!(
            &ChainId::root(9).to_string(),
            "63620ea465af9e9e0e8e4dd8d21593cc3a719feac5f096df8440f90738f4dbd8"
        );
        assert_eq!(
            &ChainId::root(999).to_string(),
            "5487b70625ce71f7ee29154ad32aefa1c526cb483bdb783dea2e1d17bc497844"
        );
    }
}

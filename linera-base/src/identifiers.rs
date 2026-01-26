// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Core identifiers used by the Linera protocol.

use std::{
    fmt,
    hash::{Hash, Hasher},
    marker::PhantomData,
};

use allocative::Allocative;
#[cfg(with_revm)]
use alloy_primitives::{Address, B256};
use anyhow::{anyhow, Context};
use async_graphql::{InputObject, SimpleObject};
use custom_debug_derive::Debug;
use derive_more::{Display, FromStr};
use linera_witty::{WitLoad, WitStore, WitType};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    bcs_scalar,
    crypto::{
        AccountPublicKey, CryptoError, CryptoHash, Ed25519PublicKey, EvmPublicKey,
        Secp256k1PublicKey,
    },
    data_types::{BlobContent, ChainDescription},
    doc_scalar, hex_debug,
    vm::VmRuntime,
};

/// An account owner.
#[derive(
    Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, WitLoad, WitStore, WitType, Allocative,
)]
#[cfg_attr(with_testing, derive(test_strategy::Arbitrary))]
// TODO(#5166) we can be more specific here
#[cfg_attr(
    web,
    derive(tsify::Tsify),
    tsify(from_wasm_abi, into_wasm_abi, type = "string")
)]
pub enum AccountOwner {
    /// Short addresses reserved for the protocol.
    Reserved(u8),
    /// 32-byte account address.
    Address32(CryptoHash),
    /// 20-byte account EVM-compatible address.
    Address20([u8; 20]),
}

impl fmt::Debug for AccountOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reserved(byte) => f.debug_tuple("Reserved").field(byte).finish(),
            Self::Address32(hash) => write!(f, "Address32({:?})", hash),
            Self::Address20(bytes) => write!(f, "Address20({})", hex::encode(bytes)),
        }
    }
}

impl AccountOwner {
    /// Returns the default chain address.
    pub const CHAIN: AccountOwner = AccountOwner::Reserved(0);

    /// Tests if the account is the chain address.
    pub fn is_chain(&self) -> bool {
        self == &AccountOwner::CHAIN
    }

    /// The size of the `AccountOwner`.
    pub fn size(&self) -> u32 {
        match self {
            AccountOwner::Reserved(_) => 1,
            AccountOwner::Address32(_) => 32,
            AccountOwner::Address20(_) => 20,
        }
    }

    /// Gets the EVM address if possible
    #[cfg(with_revm)]
    pub fn to_evm_address(&self) -> Option<Address> {
        match self {
            AccountOwner::Address20(address) => Some(Address::from(address)),
            _ => None,
        }
    }
}

#[cfg(with_revm)]
impl From<Address> for AccountOwner {
    fn from(address: Address) -> Self {
        let address = address.into_array();
        AccountOwner::Address20(address)
    }
}

#[cfg(with_testing)]
impl From<CryptoHash> for AccountOwner {
    fn from(address: CryptoHash) -> Self {
        AccountOwner::Address32(address)
    }
}

/// An account.
#[derive(
    Debug,
    PartialEq,
    Eq,
    Hash,
    Copy,
    Clone,
    Serialize,
    Deserialize,
    WitLoad,
    WitStore,
    WitType,
    SimpleObject,
    InputObject,
    Allocative,
)]
#[graphql(name = "AccountOutput", input_name = "Account")]
#[cfg_attr(web, derive(tsify::Tsify), tsify(from_wasm_abi, into_wasm_abi))]
pub struct Account {
    /// The chain of the account.
    pub chain_id: ChainId,
    /// The owner of the account.
    pub owner: AccountOwner,
}

impl Account {
    /// Creates a new [`Account`] with the given chain ID and owner.
    pub fn new(chain_id: ChainId, owner: AccountOwner) -> Self {
        Self { chain_id, owner }
    }

    /// Creates an [`Account`] representing the balance shared by a chain's owners.
    pub fn chain(chain_id: ChainId) -> Self {
        Account {
            chain_id,
            owner: AccountOwner::CHAIN,
        }
    }

    /// An address used exclusively for tests
    #[cfg(with_testing)]
    pub fn burn_address(chain_id: ChainId) -> Self {
        let hash = CryptoHash::test_hash("burn");
        Account {
            chain_id,
            owner: hash.into(),
        }
    }
}

impl fmt::Display for Account {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.owner, self.chain_id)
    }
}

impl std::str::FromStr for Account {
    type Err = anyhow::Error;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        if let Some((owner_string, chain_string)) = string.rsplit_once('@') {
            let owner = owner_string.parse::<AccountOwner>()?;
            let chain_id = chain_string.parse()?;
            Ok(Account::new(chain_id, owner))
        } else {
            let chain_id = string
                .parse()
                .context("Expecting an account formatted as `chain-id` or `owner@chain-id`")?;
            Ok(Account::chain(chain_id))
        }
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
    Allocative,
)]
#[cfg_attr(with_testing, derive(test_strategy::Arbitrary))]
#[cfg_attr(with_testing, derive(Default))]
#[cfg_attr(web, derive(tsify::Tsify), tsify(from_wasm_abi, into_wasm_abi))]
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
    Allocative,
)]
#[cfg_attr(with_testing, derive(test_strategy::Arbitrary))]
pub enum BlobType {
    /// A generic data blob.
    #[default]
    Data,
    /// A blob containing compressed contract Wasm bytecode.
    ContractBytecode,
    /// A blob containing compressed service Wasm bytecode.
    ServiceBytecode,
    /// A blob containing compressed EVM bytecode.
    EvmBytecode,
    /// A blob containing an application description.
    ApplicationDescription,
    /// A blob containing a committee of validators.
    Committee,
    /// A blob containing a chain description.
    ChainDescription,
}

impl BlobType {
    /// Returns whether the blob is of [`BlobType::Committee`] variant.
    pub fn is_committee_blob(&self) -> bool {
        match self {
            BlobType::Data
            | BlobType::ContractBytecode
            | BlobType::ServiceBytecode
            | BlobType::EvmBytecode
            | BlobType::ApplicationDescription
            | BlobType::ChainDescription => false,
            BlobType::Committee => true,
        }
    }
}

impl fmt::Display for BlobType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::str::FromStr for BlobType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(&format!("\"{s}\""))
            .with_context(|| format!("Invalid BlobType: {}", s))
    }
}

/// A content-addressed blob ID i.e. the hash of the `BlobContent`.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Clone, Copy, Hash, Debug, WitType, WitStore, WitLoad, Allocative,
)]
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

impl fmt::Display for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.blob_type, self.hash)?;
        Ok(())
    }
}

impl std::str::FromStr for BlobId {
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

/// Hash of a data blob.
#[derive(
    Eq, Hash, PartialEq, Debug, Serialize, Deserialize, Clone, Copy, WitType, WitLoad, WitStore,
)]
pub struct DataBlobHash(pub CryptoHash);

impl From<DataBlobHash> for BlobId {
    fn from(hash: DataBlobHash) -> BlobId {
        BlobId::new(hash.0, BlobType::Data)
    }
}

// TODO(#5166) we can be more specific here (and also more generic)
#[cfg_attr(web, wasm_bindgen::prelude::wasm_bindgen(typescript_custom_section))]
const _: &str = "export type ApplicationId = string;";

/// A unique identifier for a user application from a blob.
#[derive(Debug, WitLoad, WitStore, WitType, Allocative)]
#[cfg_attr(with_testing, derive(Default, test_strategy::Arbitrary))]
#[allocative(bound = "A")]
pub struct ApplicationId<A = ()> {
    /// The hash of the `ApplicationDescription` this refers to.
    pub application_description_hash: CryptoHash,
    #[witty(skip)]
    #[debug(skip)]
    #[allocative(skip)]
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
    Allocative,
)]
#[cfg_attr(web, derive(tsify::Tsify), tsify(from_wasm_abi, into_wasm_abi))]
pub enum GenericApplicationId {
    /// The system application.
    System,
    /// A user application.
    User(ApplicationId),
}

impl fmt::Display for GenericApplicationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GenericApplicationId::System => Display::fmt("System", f),
            GenericApplicationId::User(application_id) => {
                Display::fmt("User:", f)?;
                Display::fmt(&application_id, f)
            }
        }
    }
}

impl std::str::FromStr for GenericApplicationId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "System" {
            return Ok(GenericApplicationId::System);
        }
        if let Some(result) = s.strip_prefix("User:") {
            let application_id = ApplicationId::from_str(result)?;
            return Ok(GenericApplicationId::User(application_id));
        }
        Err(anyhow!("Invalid parsing of GenericApplicationId"))
    }
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

impl<A> From<ApplicationId<A>> for AccountOwner {
    fn from(app_id: ApplicationId<A>) -> Self {
        if app_id.is_evm() {
            let hash_bytes = app_id.application_description_hash.as_bytes();
            AccountOwner::Address20(hash_bytes[..20].try_into().unwrap())
        } else {
            AccountOwner::Address32(app_id.application_description_hash)
        }
    }
}

impl From<AccountPublicKey> for AccountOwner {
    fn from(public_key: AccountPublicKey) -> Self {
        match public_key {
            AccountPublicKey::Ed25519(public_key) => public_key.into(),
            AccountPublicKey::Secp256k1(public_key) => public_key.into(),
            AccountPublicKey::EvmSecp256k1(public_key) => public_key.into(),
        }
    }
}

impl From<ApplicationId> for GenericApplicationId {
    fn from(application_id: ApplicationId) -> Self {
        GenericApplicationId::User(application_id)
    }
}

impl From<Secp256k1PublicKey> for AccountOwner {
    fn from(public_key: Secp256k1PublicKey) -> Self {
        AccountOwner::Address32(CryptoHash::new(&public_key))
    }
}

impl From<Ed25519PublicKey> for AccountOwner {
    fn from(public_key: Ed25519PublicKey) -> Self {
        AccountOwner::Address32(CryptoHash::new(&public_key))
    }
}

impl From<EvmPublicKey> for AccountOwner {
    fn from(public_key: EvmPublicKey) -> Self {
        AccountOwner::Address20(alloy_primitives::Address::from_public_key(&public_key.0).into())
    }
}

/// A unique identifier for a module.
#[derive(Debug, WitLoad, WitStore, WitType, Allocative)]
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
    Allocative,
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

impl fmt::Display for StreamName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&hex::encode(&self.0), f)
    }
}

impl std::str::FromStr for StreamName {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let vec = hex::decode(s)?;
        Ok(StreamName(vec))
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
    InputObject,
    Allocative,
)]
#[graphql(input_name = "StreamIdInput")]
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

/// The result of an `events_from_index`.
#[derive(
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Clone,
    Hash,
    Serialize,
    Deserialize,
    WitLoad,
    WitStore,
    WitType,
    SimpleObject,
)]
pub struct IndexAndEvent {
    /// The index of the found event.
    pub index: u32,
    /// The event being returned.
    pub event: Vec<u8>,
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.application_id, f)?;
        Display::fmt(":", f)?;
        Display::fmt(&self.stream_name, f)
    }
}

impl std::str::FromStr for StreamId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.rsplit_once(":");
        if let Some((part0, part1)) = parts {
            let application_id =
                GenericApplicationId::from_str(part0).context("Invalid GenericApplicationId!")?;
            let stream_name = StreamName::from_str(part1).context("Invalid StreamName!")?;
            Ok(StreamId {
                application_id,
                stream_name,
            })
        } else {
            Err(anyhow!("Invalid blob ID: {}", s))
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
    Allocative,
)]
pub struct EventId {
    /// The ID of the chain that generated this event.
    pub chain_id: ChainId,
    /// The ID of the stream this event belongs to.
    pub stream_id: StreamId,
    /// The event index, i.e. the number of events in the stream before this one.
    pub index: u32,
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}", self.chain_id, self.stream_id, self.index)
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

    /// Gets the `BlobId` of the contract
    pub fn contract_bytecode_blob_id(&self) -> BlobId {
        match self.vm_runtime {
            VmRuntime::Wasm => BlobId::new(self.contract_blob_hash, BlobType::ContractBytecode),
            VmRuntime::Evm => BlobId::new(self.contract_blob_hash, BlobType::EvmBytecode),
        }
    }

    /// Gets the `BlobId` of the service
    pub fn service_bytecode_blob_id(&self) -> BlobId {
        match self.vm_runtime {
            VmRuntime::Wasm => BlobId::new(self.service_blob_hash, BlobType::ServiceBytecode),
            VmRuntime::Evm => BlobId::new(self.contract_blob_hash, BlobType::EvmBytecode),
        }
    }

    /// Gets all bytecode `BlobId`s of the module
    pub fn bytecode_blob_ids(&self) -> Vec<BlobId> {
        match self.vm_runtime {
            VmRuntime::Wasm => vec![
                BlobId::new(self.contract_blob_hash, BlobType::ContractBytecode),
                BlobId::new(self.service_blob_hash, BlobType::ServiceBytecode),
            ],
            VmRuntime::Evm => vec![BlobId::new(self.contract_blob_hash, BlobType::EvmBytecode)],
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

    /// Converts the application ID to the ID of the blob containing the
    /// `ApplicationDescription`.
    pub fn description_blob_id(self) -> BlobId {
        BlobId::new(
            self.application_description_hash,
            BlobType::ApplicationDescription,
        )
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
    /// Forgets the ABI of an application ID (if any).
    pub fn forget_abi(self) -> ApplicationId {
        ApplicationId {
            application_description_hash: self.application_description_hash,
            _phantom: PhantomData,
        }
    }
}

impl<A> ApplicationId<A> {
    /// Returns whether the `ApplicationId` is the one of an EVM application.
    pub fn is_evm(&self) -> bool {
        let bytes = self.application_description_hash.as_bytes();
        bytes.0[20..] == [0; 12]
    }
}

#[cfg(with_revm)]
impl From<Address> for ApplicationId {
    fn from(address: Address) -> ApplicationId {
        let mut arr = [0_u8; 32];
        arr[..20].copy_from_slice(address.as_slice());
        ApplicationId {
            application_description_hash: arr.into(),
            _phantom: PhantomData,
        }
    }
}

#[cfg(with_revm)]
impl<A> ApplicationId<A> {
    /// Converts the `ApplicationId` into an Ethereum Address.
    pub fn evm_address(&self) -> Address {
        let bytes = self.application_description_hash.as_bytes();
        let bytes = bytes.0.as_ref();
        Address::from_slice(&bytes[0..20])
    }

    /// Converts the `ApplicationId` into an Ethereum-compatible 32-byte array.
    pub fn bytes32(&self) -> B256 {
        *self.application_description_hash.as_bytes()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "AccountOwner")]
enum SerializableAccountOwner {
    Reserved(u8),
    Address32(CryptoHash),
    Address20([u8; 20]),
}

impl Serialize for AccountOwner {
    fn serialize<S: serde::ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            match self {
                AccountOwner::Reserved(value) => SerializableAccountOwner::Reserved(*value),
                AccountOwner::Address32(value) => SerializableAccountOwner::Address32(*value),
                AccountOwner::Address20(value) => SerializableAccountOwner::Address20(*value),
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
                SerializableAccountOwner::Reserved(value) => Ok(AccountOwner::Reserved(value)),
                SerializableAccountOwner::Address32(value) => Ok(AccountOwner::Address32(value)),
                SerializableAccountOwner::Address20(value) => Ok(AccountOwner::Address20(value)),
            }
        }
    }
}

impl fmt::Display for AccountOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AccountOwner::Reserved(value) => {
                write!(f, "0x{}", hex::encode(&value.to_be_bytes()[..]))?
            }
            AccountOwner::Address32(value) => write!(f, "0x{}", value)?,
            AccountOwner::Address20(value) => write!(f, "0x{}", hex::encode(&value[..]))?,
        };

        Ok(())
    }
}

impl std::str::FromStr for AccountOwner {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(s) = s.strip_prefix("0x") {
            if s.len() == 64 {
                if let Ok(hash) = CryptoHash::from_str(s) {
                    return Ok(AccountOwner::Address32(hash));
                }
            } else if s.len() == 40 {
                let address = hex::decode(s)?;
                if address.len() != 20 {
                    anyhow::bail!("Invalid address length: {s}");
                }
                let address = <[u8; 20]>::try_from(address.as_slice()).unwrap();
                return Ok(AccountOwner::Address20(address));
            }
            if s.len() == 2 {
                let bytes = hex::decode(s)?;
                if bytes.len() == 1 {
                    let value = u8::from_be_bytes(bytes.try_into().expect("one byte"));
                    return Ok(AccountOwner::Reserved(value));
                }
            }
        }
        anyhow::bail!("Invalid address value: {s}");
    }
}

impl fmt::Display for ChainId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl std::str::FromStr for ChainId {
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

impl<'a> From<&'a ChainDescription> for ChainId {
    fn from(description: &'a ChainDescription) -> Self {
        Self(CryptoHash::new(&BlobContent::new_chain_description(
            description,
        )))
    }
}

impl From<ChainDescription> for ChainId {
    fn from(description: ChainDescription) -> Self {
        From::from(&description)
    }
}

bcs_scalar!(ApplicationId, "A unique identifier for a user application");
doc_scalar!(DataBlobHash, "Hash of a Data Blob");
doc_scalar!(
    GenericApplicationId,
    "A unique identifier for a user application or for the system application"
);
bcs_scalar!(ModuleId, "A unique identifier for an application module");
doc_scalar!(
    ChainId,
    "The unique identifier (UID) of a chain. This is currently computed as the hash value of a \
    ChainDescription."
);
doc_scalar!(StreamName, "The name of an event stream");

doc_scalar!(
    AccountOwner,
    "A unique identifier for a user or an application."
);
doc_scalar!(
    BlobId,
    "A content-addressed blob ID i.e. the hash of the `BlobContent`"
);

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use assert_matches::assert_matches;

    use super::{AccountOwner, BlobType};
    use crate::{
        data_types::{Amount, ChainDescription, ChainOrigin, Epoch, InitialChainConfig, Timestamp},
        identifiers::{ApplicationId, CryptoHash, GenericApplicationId, StreamId, StreamName},
        ownership::ChainOwnership,
    };

    /// Verifies that the way of computing chain IDs doesn't change.
    #[test]
    fn chain_id_computing() {
        let example_chain_origin = ChainOrigin::Root(0);
        let example_chain_config = InitialChainConfig {
            epoch: Epoch::ZERO,
            ownership: ChainOwnership::single(AccountOwner::Reserved(0)),
            balance: Amount::ZERO,
            min_active_epoch: Epoch::ZERO,
            max_active_epoch: Epoch::ZERO,
            application_permissions: Default::default(),
        };
        let description = ChainDescription::new(
            example_chain_origin,
            example_chain_config,
            Timestamp::from(0),
        );
        assert_eq!(
            description.id().to_string(),
            "76e3a8c7b2449e6bc238642ac68b4311a809cb57328bea0a1ef9122f08a0053d"
        );
    }

    #[test]
    fn blob_types() {
        assert_eq!("ContractBytecode", BlobType::ContractBytecode.to_string());
        assert_eq!(
            BlobType::ContractBytecode,
            BlobType::from_str("ContractBytecode").unwrap()
        );
    }

    #[test]
    fn addresses() {
        assert_eq!(&AccountOwner::Reserved(0).to_string(), "0x00");
        assert_eq!(AccountOwner::from_str("0x00").unwrap(), AccountOwner::CHAIN);

        let address = AccountOwner::from_str("0x10").unwrap();
        assert_eq!(address, AccountOwner::Reserved(16));
        assert_eq!(address.to_string(), "0x10");

        let address = AccountOwner::from_str(
            "0x5487b70625ce71f7ee29154ad32aefa1c526cb483bdb783dea2e1d17bc497844",
        )
        .unwrap();
        assert_matches!(address, AccountOwner::Address32(_));
        assert_eq!(
            address.to_string(),
            "0x5487b70625ce71f7ee29154ad32aefa1c526cb483bdb783dea2e1d17bc497844"
        );

        let address = AccountOwner::from_str("0x6E0ab7F37b667b7228D3a03116Ca21Be83213823").unwrap();
        assert_matches!(address, AccountOwner::Address20(_));
        assert_eq!(
            address.to_string(),
            "0x6e0ab7f37b667b7228d3a03116ca21be83213823"
        );

        assert!(AccountOwner::from_str("0x5487b7").is_err());
        assert!(AccountOwner::from_str("0").is_err());
        assert!(AccountOwner::from_str(
            "5487b70625ce71f7ee29154ad32aefa1c526cb483bdb783dea2e1d17bc497844"
        )
        .is_err());
    }

    #[test]
    fn accounts() {
        use super::{Account, ChainId};

        const CHAIN: &str = "76e3a8c7b2449e6bc238642ac68b4311a809cb57328bea0a1ef9122f08a0053d";
        const OWNER: &str = "0x5487b70625ce71f7ee29154ad32aefa1c526cb483bdb783dea2e1d17bc497844";

        let chain_id = ChainId::from_str(CHAIN).unwrap();
        let owner = AccountOwner::from_str(OWNER).unwrap();

        // Chain-only account.
        let account = Account::from_str(CHAIN).unwrap();
        assert_eq!(
            account,
            Account::from_str(&format!("0x00@{CHAIN}")).unwrap()
        );
        assert_eq!(account, Account::chain(chain_id));
        assert_eq!(account.to_string(), format!("0x00@{CHAIN}"));

        // Account with owner.
        let account = Account::from_str(&format!("{OWNER}@{CHAIN}")).unwrap();
        assert_eq!(account, Account::new(chain_id, owner));
        assert_eq!(account.to_string(), format!("{OWNER}@{CHAIN}"));
    }

    #[test]
    fn stream_name() {
        let vec = vec![32, 54, 120, 234];
        let stream_name1 = StreamName(vec);
        let stream_name2 = StreamName::from_str(&format!("{stream_name1}")).unwrap();
        assert_eq!(stream_name1, stream_name2);
    }

    fn test_generic_application_id(application_id: GenericApplicationId) {
        let application_id2 = GenericApplicationId::from_str(&format!("{application_id}")).unwrap();
        assert_eq!(application_id, application_id2);
    }

    #[test]
    fn generic_application_id() {
        test_generic_application_id(GenericApplicationId::System);
        let hash = CryptoHash::test_hash("test case");
        let application_id = ApplicationId::new(hash);
        test_generic_application_id(GenericApplicationId::User(application_id));
    }

    #[test]
    fn stream_id() {
        let hash = CryptoHash::test_hash("test case");
        let application_id = ApplicationId::new(hash);
        let application_id = GenericApplicationId::User(application_id);
        let vec = vec![32, 54, 120, 234];
        let stream_name = StreamName(vec);

        let stream_id1 = StreamId {
            application_id,
            stream_name,
        };
        let stream_id2 = StreamId::from_str(&format!("{stream_id1}")).unwrap();
        assert_eq!(stream_id1, stream_id2);
    }

    #[cfg(with_revm)]
    #[test]
    fn test_address_account_owner() {
        use alloy_primitives::Address;
        let mut vec = Vec::new();
        for i in 0..20 {
            vec.push(i as u8);
        }
        let address1 = Address::from_slice(&vec);
        let account_owner = AccountOwner::from(address1);
        let address2 = account_owner.to_evm_address().unwrap();
        assert_eq!(address1, address2);
    }
}

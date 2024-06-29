// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module manages the execution of the system application and the user applications in a Linera chain.

#![deny(clippy::large_futures)]

mod applications;
pub mod committee;
mod execution;
mod execution_state_actor;
mod graphql;
mod policy;
mod resources;
mod runtime;
pub mod system;
#[cfg(with_testing)]
pub mod test_utils;
mod util;
mod wasm;

use std::{fmt, str::FromStr, sync::Arc};

use async_graphql::SimpleObject;
use async_trait::async_trait;
use custom_debug_derive::Debug;
use dashmap::DashMap;
use derive_more::Display;
use linera_base::{
    abi::Abi,
    crypto::CryptoHash,
    data_types::{
        Amount, ApplicationPermissions, ArithmeticError, BlockHeight, Resources,
        SendMessageRequest, Timestamp,
    },
    doc_scalar, hex_debug,
    identifiers::{
        Account, ApplicationId, BytecodeId, ChainId, ChannelName, Destination,
        GenericApplicationId, MessageId, Owner,
    },
    ownership::ChainOwnership,
};
use linera_views::{batch::Batch, views::ViewError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[cfg(with_testing)]
pub use crate::applications::ApplicationRegistry;
use crate::runtime::ContractSyncRuntime;
#[cfg(all(with_testing, with_wasm_runtime))]
pub use crate::wasm::test as wasm_test;
#[cfg(with_wasm_runtime)]
pub use crate::wasm::{
    ContractEntrypoints, ContractSystemApi, ServiceEntrypoints, ServiceSystemApi, SystemApiData,
    ViewSystemApi, WasmContractModule, WasmExecutionError, WasmServiceModule,
};
pub use crate::{
    applications::{
        ApplicationRegistryView, BytecodeLocation, UserApplicationDescription, UserApplicationId,
    },
    execution::ExecutionStateView,
    execution_state_actor::ExecutionRequest,
    policy::ResourceControlPolicy,
    resources::{ResourceController, ResourceTracker},
    runtime::{
        ContractSyncRuntimeHandle, ServiceRuntimeRequest, ServiceSyncRuntime,
        ServiceSyncRuntimeHandle,
    },
    system::{
        SystemExecutionError, SystemExecutionStateView, SystemMessage, SystemOperation,
        SystemQuery, SystemResponse,
    },
};

/// An implementation of [`UserContractModule`].
pub type UserContractCode = Arc<dyn UserContractModule + Send + Sync + 'static>;

/// An implementation of [`UserServiceModule`].
pub type UserServiceCode = Arc<dyn UserServiceModule + Send + Sync + 'static>;

/// An implementation of [`UserContract`].
pub type UserContractInstance = Box<dyn UserContract + 'static>;

/// An implementation of [`UserService`].
pub type UserServiceInstance = Box<dyn UserService + 'static>;

/// A factory trait to obtain a [`UserContract`] from a [`UserContractModule`]
pub trait UserContractModule {
    fn instantiate(
        &self,
        runtime: ContractSyncRuntimeHandle,
    ) -> Result<UserContractInstance, ExecutionError>;
}

/// A factory trait to obtain a [`UserService`] from a [`UserServiceModule`]
pub trait UserServiceModule {
    fn instantiate(
        &self,
        runtime: ServiceSyncRuntimeHandle,
    ) -> Result<UserServiceInstance, ExecutionError>;
}

/// A type for errors happening during execution.
#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error(transparent)]
    ViewError(#[from] ViewError),
    #[error(transparent)]
    ArithmeticError(#[from] ArithmeticError),
    #[error(transparent)]
    SystemError(#[from] SystemExecutionError),
    #[error("User application reported an error: {0}")]
    UserError(String),
    #[cfg(any(with_wasmer, with_wasmtime))]
    #[error(transparent)]
    WasmError(#[from] WasmExecutionError),
    #[error(transparent)]
    JoinError(#[from] tokio::task::JoinError),
    #[error("The given promise is invalid or was polled once already")]
    InvalidPromise,

    #[error("Attempted to perform a reentrant call to application {0}")]
    ReentrantCall(UserApplicationId),
    #[error(
        "Application {caller_id} attempted to perform a cross-application to {callee_id} call \
        from `finalize`"
    )]
    CrossApplicationCallInFinalize {
        caller_id: Box<UserApplicationId>,
        callee_id: Box<UserApplicationId>,
    },
    #[error("Attempt to write to storage from a contract")]
    ServiceWriteAttempt,
    #[error("Failed to load bytecode from storage {0:?}")]
    ApplicationBytecodeNotFound(Box<UserApplicationDescription>),

    #[error("Excessive number of bytes read from storage")]
    ExcessiveRead,
    #[error("Excessive number of bytes written to storage")]
    ExcessiveWrite,
    #[error("Runtime failed to respond to application")]
    MissingRuntimeResponse,
    #[error("Bytecode ID {0:?} is invalid")]
    InvalidBytecodeId(BytecodeId),
    #[error("Owner is None")]
    OwnerIsNone,
    #[error("Application is not authorized to perform system operations on this chain: {0:}")]
    UnauthorizedApplication(UserApplicationId),
    #[error("Failed to make network reqwest")]
    ReqwestError(#[from] reqwest::Error),
    #[error("Encountered IO error")]
    IoError(#[from] std::io::Error),
    #[error("No recorded response for oracle query")]
    MissingOracleResponse,
    #[error("Invalid JSON: {}", .0)]
    Json(#[from] serde_json::Error),
    #[error("Recorded response for oracle query has the wrong type")]
    OracleResponseMismatch,
    #[error("Assertion failed: local time {local_time} is not earlier than {timestamp}")]
    AssertBefore {
        timestamp: Timestamp,
        local_time: Timestamp,
    },
}

/// The public entry points provided by the contract part of an application.
pub trait UserContract {
    /// Instantiate the application state on the chain that owns the application.
    fn instantiate(
        &mut self,
        context: OperationContext,
        argument: Vec<u8>,
    ) -> Result<(), ExecutionError>;

    /// Applies an operation from the current block.
    fn execute_operation(
        &mut self,
        context: OperationContext,
        operation: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError>;

    /// Applies a message originating from a cross-chain message.
    fn execute_message(
        &mut self,
        context: MessageContext,
        message: Vec<u8>,
    ) -> Result<(), ExecutionError>;

    /// Finishes execution of the current transaction.
    fn finalize(&mut self, context: FinalizeContext) -> Result<(), ExecutionError>;
}

/// The public entry points provided by the service part of an application.
pub trait UserService {
    /// Executes unmetered read-only queries on the state of this application.
    fn handle_query(
        &mut self,
        context: QueryContext,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError>;
}

/// The result of calling into a user application.
#[derive(Default)]
pub struct ApplicationCallOutcome {
    /// The return value.
    pub value: Vec<u8>,
    /// The externally-visible result.
    pub execution_outcome: RawExecutionOutcome<Vec<u8>>,
}

impl ApplicationCallOutcome {
    /// Adds a `message` to this [`ApplicationCallOutcome`].
    pub fn with_message(mut self, message: RawOutgoingMessage<Vec<u8>>) -> Self {
        self.execution_outcome.messages.push(message);
        self
    }
}

/// Configuration options for the execution runtime available to applications.
#[derive(Clone, Copy, Default)]
pub struct ExecutionRuntimeConfig {}

/// Requirements for the `extra` field in our state views (and notably the
/// [`ExecutionStateView`]).
#[async_trait]
pub trait ExecutionRuntimeContext {
    fn chain_id(&self) -> ChainId;

    fn execution_runtime_config(&self) -> ExecutionRuntimeConfig;

    fn user_contracts(&self) -> &Arc<DashMap<UserApplicationId, UserContractCode>>;

    fn user_services(&self) -> &Arc<DashMap<UserApplicationId, UserServiceCode>>;

    async fn get_user_contract(
        &self,
        description: &UserApplicationDescription,
    ) -> Result<UserContractCode, ExecutionError>;

    async fn get_user_service(
        &self,
        description: &UserApplicationDescription,
    ) -> Result<UserServiceCode, ExecutionError>;
}

#[derive(Clone, Copy, Debug)]
pub struct OperationContext {
    /// The current chain ID.
    pub chain_id: ChainId,
    /// The authenticated signer of the operation, if any.
    pub authenticated_signer: Option<Owner>,
    /// `None` if this is the transaction entrypoint or the caller doesn't want this particular
    /// call to be authenticated (e.g. for safety reasons).
    pub authenticated_caller_id: Option<UserApplicationId>,
    /// The current block height.
    pub height: BlockHeight,
    /// The current index of the operation.
    pub index: Option<u32>,
    /// The index of the next message to be created.
    pub next_message_index: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct MessageContext {
    /// The current chain ID.
    pub chain_id: ChainId,
    /// Whether the message was rejected by the original receiver and is now bouncing back.
    pub is_bouncing: bool,
    /// The authenticated signer of the operation that created the message, if any.
    pub authenticated_signer: Option<Owner>,
    /// Where to send a refund for the unused part of each grant after execution, if any.
    pub refund_grant_to: Option<Account>,
    /// The current block height.
    pub height: BlockHeight,
    /// The hash of the remote certificate that created the message.
    pub certificate_hash: CryptoHash,
    /// The ID of the message (based on the operation height and index in the remote
    /// certificate).
    pub message_id: MessageId,
    /// The index of the next message to be created.
    pub next_message_index: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct FinalizeContext {
    /// The current chain ID.
    pub chain_id: ChainId,
    /// The authenticated signer of the operation, if any.
    pub authenticated_signer: Option<Owner>,
    /// The current block height.
    pub height: BlockHeight,
    /// The index of the next message to be created.
    pub next_message_index: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct QueryContext {
    /// The current chain ID.
    pub chain_id: ChainId,
    /// The height of the next block on this chain.
    pub next_block_height: BlockHeight,
    /// The local time in the node executing the query.
    pub local_time: Timestamp,
}

pub trait BaseRuntime {
    type Read: fmt::Debug + Send + Sync;
    type ContainsKey: fmt::Debug + Send + Sync;
    type ReadMultiValuesBytes: fmt::Debug + Send + Sync;
    type ReadValueBytes: fmt::Debug + Send + Sync;
    type FindKeysByPrefix: fmt::Debug + Send + Sync;
    type FindKeyValuesByPrefix: fmt::Debug + Send + Sync;

    /// The current chain ID.
    fn chain_id(&mut self) -> Result<ChainId, ExecutionError>;

    /// The current block height.
    fn block_height(&mut self) -> Result<BlockHeight, ExecutionError>;

    /// The current application ID.
    fn application_id(&mut self) -> Result<UserApplicationId, ExecutionError>;

    /// The current application parameters.
    fn application_parameters(&mut self) -> Result<Vec<u8>, ExecutionError>;

    /// Reads the system timestamp.
    fn read_system_timestamp(&mut self) -> Result<Timestamp, ExecutionError>;

    /// Reads the balance of the chain.
    fn read_chain_balance(&mut self) -> Result<Amount, ExecutionError>;

    /// Reads the owner balance.
    fn read_owner_balance(&mut self, owner: Owner) -> Result<Amount, ExecutionError>;

    /// Reads the balances from all owners.
    fn read_owner_balances(&mut self) -> Result<Vec<(Owner, Amount)>, ExecutionError>;

    /// Reads balance owners.
    fn read_balance_owners(&mut self) -> Result<Vec<Owner>, ExecutionError>;

    /// Reads the current ownership configuration for this chain.
    fn chain_ownership(&mut self) -> Result<ChainOwnership, ExecutionError>;

    /// Tests whether a key exists in the key-value store
    #[cfg(feature = "test")]
    fn contains_key(&mut self, key: Vec<u8>) -> Result<bool, ExecutionError> {
        let promise = self.contains_key_new(key)?;
        self.contains_key_wait(&promise)
    }

    /// Tests whether a key exists in the key-value store (new)
    fn contains_key_new(&mut self, key: Vec<u8>) -> Result<Self::ContainsKey, ExecutionError>;

    /// Tests whether a key exists in the key-value store (wait)
    fn contains_key_wait(&mut self, promise: &Self::ContainsKey) -> Result<bool, ExecutionError>;

    /// Reads several keys from the key-value store
    #[cfg(feature = "test")]
    fn read_multi_values_bytes(
        &mut self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ExecutionError> {
        let promise = self.read_multi_values_bytes_new(keys)?;
        self.read_multi_values_bytes_wait(&promise)
    }

    /// Reads several keys from the key-value store (new)
    fn read_multi_values_bytes_new(
        &mut self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Self::ReadMultiValuesBytes, ExecutionError>;

    /// Reads several keys from the key-value store (wait)
    fn read_multi_values_bytes_wait(
        &mut self,
        promise: &Self::ReadMultiValuesBytes,
    ) -> Result<Vec<Option<Vec<u8>>>, ExecutionError>;

    /// Reads the key from the key-value store
    #[cfg(feature = "test")]
    fn read_value_bytes(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, ExecutionError> {
        let promise = self.read_value_bytes_new(key)?;
        self.read_value_bytes_wait(&promise)
    }

    /// Reads the key from the key-value store (new)
    fn read_value_bytes_new(
        &mut self,
        key: Vec<u8>,
    ) -> Result<Self::ReadValueBytes, ExecutionError>;

    /// Reads the key from the key-value store (wait)
    fn read_value_bytes_wait(
        &mut self,
        promise: &Self::ReadValueBytes,
    ) -> Result<Option<Vec<u8>>, ExecutionError>;

    /// Reads the data from the keys having a specific prefix (new).
    fn find_keys_by_prefix_new(
        &mut self,
        key_prefix: Vec<u8>,
    ) -> Result<Self::FindKeysByPrefix, ExecutionError>;

    /// Reads the data from the keys having a specific prefix (wait).
    fn find_keys_by_prefix_wait(
        &mut self,
        promise: &Self::FindKeysByPrefix,
    ) -> Result<Vec<Vec<u8>>, ExecutionError>;

    /// Reads the data from the key/values having a specific prefix.
    #[cfg(feature = "test")]
    #[allow(clippy::type_complexity)]
    fn find_key_values_by_prefix(
        &mut self,
        key_prefix: Vec<u8>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError> {
        let promise = self.find_key_values_by_prefix_new(key_prefix)?;
        self.find_key_values_by_prefix_wait(&promise)
    }

    /// Reads the data from the key/values having a specific prefix (new).
    fn find_key_values_by_prefix_new(
        &mut self,
        key_prefix: Vec<u8>,
    ) -> Result<Self::FindKeyValuesByPrefix, ExecutionError>;

    /// Reads the data from the key/values having a specific prefix (wait).
    #[allow(clippy::type_complexity)]
    fn find_key_values_by_prefix_wait(
        &mut self,
        promise: &Self::FindKeyValuesByPrefix,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError>;

    /// Queries a service.
    fn query_service(
        &mut self,
        application_id: ApplicationId,
        query: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError>;

    /// Makes a POST request to the given URL and returns the answer, if any.
    fn http_post(
        &mut self,
        url: &str,
        content_type: String,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError>;

    /// Ensures that the current time at block validation is `< timestamp`. Note that block
    /// validation happens at or after the block timestamp, but isn't necessarily the same.
    ///
    /// Cannot be used in fast blocks: A block using this call should be proposed by a regular
    /// owner, not a super owner.
    fn assert_before(&mut self, timestamp: Timestamp) -> Result<(), ExecutionError>;
}

pub trait ServiceRuntime: BaseRuntime {
    /// Queries another application.
    fn try_query_application(
        &mut self,
        queried_id: UserApplicationId,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError>;

    /// Fetches blob of bytes from an arbitrary URL.
    fn fetch_url(&mut self, url: &str) -> Result<Vec<u8>, ExecutionError>;
}

pub trait ContractRuntime: BaseRuntime {
    /// The authenticated signer for this execution, if there is one.
    fn authenticated_signer(&mut self) -> Result<Option<Owner>, ExecutionError>;

    /// The current message ID, if there is one.
    fn message_id(&mut self) -> Result<Option<MessageId>, ExecutionError>;

    /// If the current message (if there is one) was rejected by its destination and is now
    /// bouncing back.
    fn message_is_bouncing(&mut self) -> Result<Option<bool>, ExecutionError>;

    /// The optional authenticated caller application ID, if it was provided and if there is one
    /// based on the execution context.
    fn authenticated_caller_id(&mut self) -> Result<Option<UserApplicationId>, ExecutionError>;

    /// Returns the amount of execution fuel remaining before execution is aborted.
    fn remaining_fuel(&mut self) -> Result<u64, ExecutionError>;

    /// Consumes some of the execution fuel.
    fn consume_fuel(&mut self, fuel: u64) -> Result<(), ExecutionError>;

    /// Schedules a message to be sent.
    fn send_message(&mut self, message: SendMessageRequest<Vec<u8>>) -> Result<(), ExecutionError>;

    /// Schedules to subscribe to some `channel` on a `chain`.
    fn subscribe(&mut self, chain: ChainId, channel: ChannelName) -> Result<(), ExecutionError>;

    /// Schedules to unsubscribe to some `channel` on a `chain`.
    fn unsubscribe(&mut self, chain: ChainId, channel: ChannelName) -> Result<(), ExecutionError>;

    /// Transfers amount from source to destination.
    fn transfer(
        &mut self,
        source: Option<Owner>,
        destination: Account,
        amount: Amount,
    ) -> Result<(), ExecutionError>;

    /// Claims amount from source to destination.
    fn claim(
        &mut self,
        source: Account,
        destination: Account,
        amount: Amount,
    ) -> Result<(), ExecutionError>;

    /// Calls another application. Forwarded sessions will now be visible to
    /// `callee_id` (but not to the caller any more).
    fn try_call_application(
        &mut self,
        authenticated: bool,
        callee_id: UserApplicationId,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError>;

    /// Opens a new chain.
    fn open_chain(
        &mut self,
        ownership: ChainOwnership,
        application_permissions: ApplicationPermissions,
        balance: Amount,
    ) -> Result<ChainId, ExecutionError>;

    /// Closes the current chain.
    fn close_chain(&mut self) -> Result<(), ExecutionError>;

    /// Writes a batch of changes.
    fn write_batch(&mut self, batch: Batch) -> Result<(), ExecutionError>;
}

/// An operation to be executed in a block.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Operation {
    /// A system operation.
    System(SystemOperation),
    /// A user operation (in serialized form).
    User {
        application_id: UserApplicationId,
        #[serde(with = "serde_bytes")]
        #[debug(with = "hex_debug")]
        bytes: Vec<u8>,
    },
}

/// A message to be sent and possibly executed in the receiver's block.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Message {
    /// A system message.
    System(SystemMessage),
    /// A user message (in serialized form).
    User {
        application_id: UserApplicationId,
        #[serde(with = "serde_bytes")]
        #[debug(with = "hex_debug")]
        bytes: Vec<u8>,
    },
}

/// An query to be sent and possibly executed in the receiver's block.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Query {
    /// A system query.
    System(SystemQuery),
    /// A user query (in serialized form).
    User {
        application_id: UserApplicationId,
        #[serde(with = "serde_bytes")]
        #[debug(with = "hex_debug")]
        bytes: Vec<u8>,
    },
}

/// The response to a query.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Response {
    /// A system response.
    System(SystemResponse),
    /// A user response (in serialized form).
    User(
        #[serde(with = "serde_bytes")]
        #[debug(with = "hex_debug")]
        Vec<u8>,
    ),
}

/// A message together with routing information.
#[derive(Clone, Debug)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct RawOutgoingMessage<Message, Grant = Resources> {
    /// The destination of the message.
    pub destination: Destination,
    /// Whether the message is authenticated.
    pub authenticated: bool,
    /// The grant needed for message execution, typically specified as an `Amount` or as `Resources`.
    pub grant: Grant,
    /// The kind of outgoing message being sent.
    pub kind: MessageKind,
    /// The message itself.
    pub message: Message,
}

impl<Message> From<SendMessageRequest<Message>> for RawOutgoingMessage<Message, Resources> {
    fn from(request: SendMessageRequest<Message>) -> Self {
        let SendMessageRequest {
            destination,
            authenticated,
            grant,
            is_tracked,
            message,
        } = request;

        let kind = if is_tracked {
            MessageKind::Tracked
        } else {
            MessageKind::Simple
        };

        RawOutgoingMessage {
            destination,
            authenticated,
            grant,
            kind,
            message,
        }
    }
}

/// The kind of outgoing message being sent.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Copy)]
pub enum MessageKind {
    /// The message can be skipped or rejected. No receipt is requested.
    Simple,
    /// The message cannot be skipped nor rejected. No receipt is requested.
    /// This only concerns certain system messages that cannot fail.
    Protected,
    /// The message cannot be skipped but can be rejected. A receipt must be sent
    /// when the message is rejected in a block of the receiver.
    Tracked,
    /// This event is a receipt automatically created when the original event was rejected.
    Bouncing,
}

/// Externally visible results of an execution. These results are meant in the context of
/// the application that created them.
#[derive(Debug)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct RawExecutionOutcome<Message, Grant = Resources> {
    /// The signer who created the messages.
    pub authenticated_signer: Option<Owner>,
    /// Where to send a refund for the unused part of each grant after execution, if any.
    pub refund_grant_to: Option<Account>,
    /// Sends messages to the given destinations, possibly forwarding the authenticated
    /// signer and including grant with the refund policy described above.
    pub messages: Vec<RawOutgoingMessage<Message, Grant>>,
    /// Subscribe chains to channels.
    pub subscribe: Vec<(ChannelName, ChainId)>,
    /// Unsubscribe chains to channels.
    pub unsubscribe: Vec<(ChannelName, ChainId)>,
}

/// The identifier of a channel, relative to a particular application.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Debug, Clone, Hash, Serialize, Deserialize, SimpleObject,
)]
pub struct ChannelSubscription {
    /// The chain ID broadcasting on this channel.
    pub chain_id: ChainId,
    /// The name of the channel.
    pub name: ChannelName,
}

/// Externally visible results of an execution, tagged by their application.
#[derive(Debug)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
#[allow(clippy::large_enum_variant)]
pub enum ExecutionOutcome {
    System(RawExecutionOutcome<SystemMessage, Amount>),
    User(UserApplicationId, RawExecutionOutcome<Vec<u8>, Amount>),
}

impl ExecutionOutcome {
    pub fn application_id(&self) -> GenericApplicationId {
        match self {
            ExecutionOutcome::System(_) => GenericApplicationId::System,
            ExecutionOutcome::User(app_id, _) => GenericApplicationId::User(*app_id),
        }
    }
}

impl<Message, Grant> RawExecutionOutcome<Message, Grant> {
    pub fn with_authenticated_signer(mut self, authenticated_signer: Option<Owner>) -> Self {
        self.authenticated_signer = authenticated_signer;
        self
    }

    pub fn with_refund_grant_to(mut self, refund_grant_to: Option<Account>) -> Self {
        self.refund_grant_to = refund_grant_to;
        self
    }

    /// Adds a `message` to this [`RawExecutionOutcome`].
    pub fn with_message(mut self, message: RawOutgoingMessage<Message, Grant>) -> Self {
        self.messages.push(message);
        self
    }
}

impl<Message, Grant> Default for RawExecutionOutcome<Message, Grant> {
    fn default() -> Self {
        Self {
            authenticated_signer: None,
            refund_grant_to: None,
            messages: Vec::new(),
            subscribe: Vec::new(),
            unsubscribe: Vec::new(),
        }
    }
}

impl<Message> RawOutgoingMessage<Message, Resources> {
    pub fn into_priced(
        self,
        policy: &ResourceControlPolicy,
    ) -> Result<RawOutgoingMessage<Message, Amount>, ArithmeticError> {
        let RawOutgoingMessage {
            destination,
            authenticated,
            grant,
            kind,
            message,
        } = self;
        Ok(RawOutgoingMessage {
            destination,
            authenticated,
            grant: policy.total_price(&grant)?,
            kind,
            message,
        })
    }
}

impl<Message> RawExecutionOutcome<Message, Resources> {
    pub fn into_priced(
        self,
        policy: &ResourceControlPolicy,
    ) -> Result<RawExecutionOutcome<Message, Amount>, ArithmeticError> {
        let RawExecutionOutcome {
            authenticated_signer,
            refund_grant_to,
            messages,
            subscribe,
            unsubscribe,
        } = self;
        let messages = messages
            .into_iter()
            .map(|message| message.into_priced(policy))
            .collect::<Result<_, _>>()?;
        Ok(RawExecutionOutcome {
            authenticated_signer,
            refund_grant_to,
            messages,
            subscribe,
            unsubscribe,
        })
    }
}

impl OperationContext {
    fn refund_grant_to(&self) -> Option<Account> {
        Some(Account {
            chain_id: self.chain_id,
            owner: self.authenticated_signer,
        })
    }

    fn next_message_id(&self) -> MessageId {
        MessageId {
            chain_id: self.chain_id,
            height: self.height,
            index: self.next_message_index,
        }
    }
}

#[cfg(with_testing)]
#[derive(Clone)]
pub struct TestExecutionRuntimeContext {
    chain_id: ChainId,
    execution_runtime_config: ExecutionRuntimeConfig,
    user_contracts: Arc<DashMap<UserApplicationId, UserContractCode>>,
    user_services: Arc<DashMap<UserApplicationId, UserServiceCode>>,
}

#[cfg(with_testing)]
impl TestExecutionRuntimeContext {
    pub fn new(chain_id: ChainId, execution_runtime_config: ExecutionRuntimeConfig) -> Self {
        Self {
            chain_id,
            execution_runtime_config,
            user_contracts: Arc::default(),
            user_services: Arc::default(),
        }
    }
}

#[cfg(with_testing)]
#[async_trait]
impl ExecutionRuntimeContext for TestExecutionRuntimeContext {
    fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    fn execution_runtime_config(&self) -> ExecutionRuntimeConfig {
        self.execution_runtime_config
    }

    fn user_contracts(&self) -> &Arc<DashMap<UserApplicationId, UserContractCode>> {
        &self.user_contracts
    }

    fn user_services(&self) -> &Arc<DashMap<UserApplicationId, UserServiceCode>> {
        &self.user_services
    }

    async fn get_user_contract(
        &self,
        description: &UserApplicationDescription,
    ) -> Result<UserContractCode, ExecutionError> {
        let application_id = description.into();
        Ok(self
            .user_contracts()
            .get(&application_id)
            .ok_or_else(|| {
                ExecutionError::ApplicationBytecodeNotFound(Box::new(description.clone()))
            })?
            .clone())
    }

    async fn get_user_service(
        &self,
        description: &UserApplicationDescription,
    ) -> Result<UserServiceCode, ExecutionError> {
        let application_id = description.into();
        Ok(self
            .user_services()
            .get(&application_id)
            .ok_or_else(|| {
                ExecutionError::ApplicationBytecodeNotFound(Box::new(description.clone()))
            })?
            .clone())
    }
}

impl From<SystemOperation> for Operation {
    fn from(operation: SystemOperation) -> Self {
        Operation::System(operation)
    }
}

impl Operation {
    pub fn system(operation: SystemOperation) -> Self {
        Operation::System(operation)
    }

    pub fn user<A: Abi>(
        application_id: UserApplicationId<A>,
        operation: &A::Operation,
    ) -> Result<Self, bcs::Error> {
        let application_id = application_id.forget_abi();
        let bytes = bcs::to_bytes(&operation)?;
        Ok(Operation::User {
            application_id,
            bytes,
        })
    }

    pub fn application_id(&self) -> GenericApplicationId {
        match self {
            Self::System(_) => GenericApplicationId::System,
            Self::User { application_id, .. } => GenericApplicationId::User(*application_id),
        }
    }
}

impl From<SystemMessage> for Message {
    fn from(message: SystemMessage) -> Self {
        Message::System(message)
    }
}

impl Message {
    pub fn system(message: SystemMessage) -> Self {
        Message::System(message)
    }

    pub fn user<A: Abi, M: Serialize>(
        application_id: UserApplicationId<A>,
        message: &M,
    ) -> Result<Self, bcs::Error> {
        let application_id = application_id.forget_abi();
        let bytes = bcs::to_bytes(&message)?;
        Ok(Message::User {
            application_id,
            bytes,
        })
    }

    pub fn application_id(&self) -> GenericApplicationId {
        match self {
            Self::System(_) => GenericApplicationId::System,
            Self::User { application_id, .. } => GenericApplicationId::User(*application_id),
        }
    }
}

impl From<SystemQuery> for Query {
    fn from(query: SystemQuery) -> Self {
        Query::System(query)
    }
}

impl Query {
    pub fn system(query: SystemQuery) -> Self {
        Query::System(query)
    }

    pub fn user<A: Abi>(
        application_id: UserApplicationId<A>,
        query: &A::Query,
    ) -> Result<Self, serde_json::Error> {
        let application_id = application_id.forget_abi();
        let bytes = serde_json::to_vec(&query)?;
        Ok(Query::User {
            application_id,
            bytes,
        })
    }

    pub fn application_id(&self) -> GenericApplicationId {
        match self {
            Self::System(_) => GenericApplicationId::System,
            Self::User { application_id, .. } => GenericApplicationId::User(*application_id),
        }
    }
}

impl From<SystemResponse> for Response {
    fn from(response: SystemResponse) -> Self {
        Response::System(response)
    }
}

impl From<Vec<u8>> for Response {
    fn from(response: Vec<u8>) -> Self {
        Response::User(response)
    }
}

/// A WebAssembly module's bytecode.
#[derive(Clone, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Bytecode {
    #[serde(with = "serde_bytes")]
    bytes: Vec<u8>,
}

impl Bytecode {
    /// Creates a new [`Bytecode`] instance using the provided `bytes`.
    #[allow(dead_code)]
    pub(crate) fn new(bytes: Vec<u8>) -> Self {
        Bytecode { bytes }
    }

    #[cfg(with_fs)]
    /// Load bytecode from a Wasm module file.
    pub async fn load_from_file(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let bytes = tokio::fs::read(path).await?;
        Ok(Bytecode { bytes })
    }
}

impl AsRef<[u8]> for Bytecode {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

impl std::fmt::Debug for Bytecode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_tuple("Bytecode").finish()
    }
}

/// The runtime to use for running the application.
#[derive(Clone, Copy, Display)]
#[cfg_attr(with_wasm_runtime, derive(Debug, Default))]
pub enum WasmRuntime {
    #[cfg(with_wasmer)]
    #[default]
    #[display(fmt = "wasmer")]
    Wasmer,
    #[cfg(with_wasmtime)]
    #[cfg_attr(not(with_wasmer), default)]
    #[display(fmt = "wasmtime")]
    Wasmtime,
    #[cfg(with_wasmer)]
    WasmerWithSanitizer,
    #[cfg(with_wasmtime)]
    WasmtimeWithSanitizer,
}

/// Trait used to select a default WasmRuntime, if one is available.
pub trait WithWasmDefault {
    fn with_wasm_default(self) -> Self;
}

impl WasmRuntime {
    #[cfg(with_wasm_runtime)]
    pub fn default_with_sanitizer() -> Self {
        cfg_if::cfg_if! {
            if #[cfg(with_wasmer)] {
                WasmRuntime::WasmerWithSanitizer
            } else if #[cfg(with_wasmtime)] {
                WasmRuntime::WasmtimeWithSanitizer
            } else {
                compile_error!("BUG: Wasm runtime unhandled in `WasmRuntime::default_with_sanitizer`")
            }
        }
    }

    pub fn needs_sanitizer(self) -> bool {
        match self {
            #[cfg(with_wasmer)]
            WasmRuntime::WasmerWithSanitizer => true,
            #[cfg(with_wasmtime)]
            WasmRuntime::WasmtimeWithSanitizer => true,
            #[cfg(with_wasm_runtime)]
            _ => false,
        }
    }
}

impl WithWasmDefault for Option<WasmRuntime> {
    fn with_wasm_default(self) -> Self {
        #[cfg(with_wasm_runtime)]
        {
            Some(self.unwrap_or_default())
        }
        #[cfg(not(with_wasm_runtime))]
        {
            None
        }
    }
}

impl FromStr for WasmRuntime {
    type Err = InvalidWasmRuntime;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        match string {
            #[cfg(with_wasmer)]
            "wasmer" => Ok(WasmRuntime::Wasmer),
            #[cfg(with_wasmtime)]
            "wasmtime" => Ok(WasmRuntime::Wasmtime),
            unknown => Err(InvalidWasmRuntime(unknown.to_owned())),
        }
    }
}

/// Attempts to create an invalid [`WasmRuntime`] instance from a string.
#[derive(Clone, Debug, Error)]
#[error("{0:?} is not a valid WebAssembly runtime")]
pub struct InvalidWasmRuntime(String);

doc_scalar!(Operation, "An operation to be executed in a block");
doc_scalar!(
    Message,
    "An message to be sent and possibly executed in the receiver's block."
);
doc_scalar!(MessageKind, "The kind of outgoing message being sent");

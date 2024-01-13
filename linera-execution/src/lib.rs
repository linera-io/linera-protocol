// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module manages the execution of the system application and the user applications in a Linera chain.

mod applications;
pub mod committee;
mod execution;
mod execution_state_actor;
mod graphql;
mod ownership;
pub mod policy;
mod resources;
mod runtime;
pub mod system;
mod util;
mod wasm;

pub use crate::runtime::{ContractSyncRuntime, ServiceSyncRuntime};
pub use applications::{
    ApplicationRegistryView, BytecodeLocation, GenericApplicationId, UserApplicationDescription,
    UserApplicationId,
};
pub use execution::ExecutionStateView;
pub use ownership::{ChainOwnership, TimeoutConfig};
pub use resources::ResourceTracker;
pub use system::{
    SystemExecutionError, SystemExecutionStateView, SystemMessage, SystemOperation, SystemQuery,
    SystemResponse,
};
#[cfg(all(
    any(test, feature = "test"),
    any(feature = "wasmer", feature = "wasmtime")
))]
pub use wasm::test as wasm_test;
#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
pub use wasm::{WasmContractModule, WasmExecutionError, WasmServiceModule};
#[cfg(any(test, feature = "test"))]
pub use {applications::ApplicationRegistry, system::SystemExecutionState};

use crate::policy::PricingError;
use async_graphql::SimpleObject;
use async_trait::async_trait;
use custom_debug_derive::Debug;
use dashmap::DashMap;
use derive_more::Display;
use linera_base::{
    abi::Abi,
    crypto::CryptoHash,
    data_types::{Amount, ArithmeticError, BlockHeight, Timestamp},
    doc_scalar, hex_debug,
    identifiers::{BytecodeId, ChainId, ChannelName, Destination, MessageId, Owner, SessionId},
};
use linera_views::{batch::Batch, views::ViewError};
use serde::{Deserialize, Serialize};
use std::{fmt, io, path::Path, str::FromStr, sync::Arc};
use thiserror::Error;

/// An implementation of [`UserContractModule`].
pub type UserContractCode = Arc<dyn UserContractModule + Send + Sync + 'static>;

/// An implementation of [`UserServiceModule`].
pub type UserServiceCode = Arc<dyn UserServiceModule + Send + Sync + 'static>;

/// An implementation of [`UserContract`].
pub type UserContractInstance = Box<dyn UserContract + Send + Sync + 'static>;

/// An implementation of [`UserService`].
pub type UserServiceInstance = Box<dyn UserService + Send + Sync + 'static>;

/// A factory trait to obtain a [`UserContract`] from a [`UserContractModule`]
pub trait UserContractModule {
    fn instantiate(
        &self,
        runtime: ContractSyncRuntime,
    ) -> Result<UserContractInstance, ExecutionError>;
}

/// A factory trait to obtain a [`UserService`] from a [`UserServiceModule`]
pub trait UserServiceModule {
    fn instantiate(
        &self,
        runtime: ServiceSyncRuntime,
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
    #[cfg(any(feature = "wasmer", feature = "wasmtime"))]
    #[error(transparent)]
    WasmError(#[from] WasmExecutionError),
    #[error(transparent)]
    JoinError(#[from] tokio::task::JoinError),
    #[error("The given promise is invalid or was polled once already")]
    InvalidPromise,

    #[error("Session {0} does not exist or was already closed")]
    InvalidSession(SessionId),
    #[error("Attempted to call or forward an active session {0}")]
    SessionIsInUse(SessionId),
    #[error("Attempted to save a session {0} but it is not locked")]
    SessionStateNotLocked(SessionId),
    #[error("Session {session_id} is owned by {owned_by} but was accessed by {accessed_by}")]
    InvalidSessionOwner {
        session_id: Box<SessionId>,
        accessed_by: Box<UserApplicationId>,
        owned_by: Box<UserApplicationId>,
    },
    #[error("Session {0} is still opened at the end of a transaction")]
    SessionWasNotClosed(SessionId),

    #[error("Attempted to perform a reentrant call to application {0}")]
    ReentrantCall(UserApplicationId),
    #[error("Failed to load bytecode from storage {0:?}")]
    ApplicationBytecodeNotFound(Box<UserApplicationDescription>),

    #[error("Pricing error: {0}")]
    PricingError(#[from] PricingError),
    #[error("Excessive number of read queries from storage")]
    ExcessiveNumReads,
    #[error("Excessive number of bytes read from storage")]
    ExcessiveRead,
    #[error("Excessive number of bytes written to storage")]
    ExcessiveWrite,
    #[error("Runtime failed to respond to application")]
    MissingRuntimeResponse,
    #[error("Bytecode ID {0:?} is invalid")]
    InvalidBytecodeId(BytecodeId),
}

impl ExecutionError {
    fn invalid_session_owner(
        session_id: SessionId,
        accessed_by: UserApplicationId,
        owned_by: UserApplicationId,
    ) -> Self {
        Self::InvalidSessionOwner {
            session_id: Box::new(session_id),
            accessed_by: Box::new(accessed_by),
            owned_by: Box::new(owned_by),
        }
    }
}

/// The public entry points provided by the contract part of an application.
pub trait UserContract {
    /// Initializes the application state on the chain that owns the application.
    fn initialize(
        &mut self,
        context: OperationContext,
        argument: Vec<u8>,
    ) -> Result<RawExecutionOutcome<Vec<u8>>, ExecutionError>;

    /// Applies an operation from the current block.
    fn execute_operation(
        &mut self,
        context: OperationContext,
        operation: Vec<u8>,
    ) -> Result<RawExecutionOutcome<Vec<u8>>, ExecutionError>;

    /// Applies a message originating from a cross-chain message.
    fn execute_message(
        &mut self,
        context: MessageContext,
        message: Vec<u8>,
    ) -> Result<RawExecutionOutcome<Vec<u8>>, ExecutionError>;

    /// Executes a call from another application.
    ///
    /// When an application is executing an operation or a message it may call other applications,
    /// which can in turn call other applications.
    fn handle_application_call(
        &mut self,
        context: CalleeContext,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallOutcome, ExecutionError>;

    /// Executes a call from another application into a session created by this application.
    fn handle_session_call(
        &mut self,
        context: CalleeContext,
        session_state: Vec<u8>,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<(SessionCallOutcome, Vec<u8>), ExecutionError>;
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
    /// The states of the new sessions to be created, if any.
    pub create_sessions: Vec<Vec<u8>>,
}

impl ApplicationCallOutcome {
    /// Adds a `message` to this [`ApplicationCallOutcome`].
    pub fn with_message(mut self, message: RawOutgoingMessage<Vec<u8>>) -> Self {
        self.execution_outcome.messages.push(message);
        self
    }

    /// Registers a new session to be created with the provided `session_state`.
    pub fn with_new_session(mut self, session_state: Vec<u8>) -> Self {
        self.create_sessions.push(session_state);
        self
    }
}

/// The result of calling into a session.
#[derive(Default)]
pub struct SessionCallOutcome {
    /// The application result.
    pub inner: ApplicationCallOutcome,
    /// If true, the session should be terminated.
    pub close_session: bool,
}

/// System runtime implementation in use.
#[derive(Default, Clone, Copy)]
pub enum ExecutionRuntimeConfig {
    #[default]
    Synchronous,
}

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
    /// The current chain id.
    pub chain_id: ChainId,
    /// The authenticated signer of the operation, if any.
    pub authenticated_signer: Option<Owner>,
    /// The current block height.
    pub height: BlockHeight,
    /// The current index of the operation.
    pub index: u32,
    /// The index of the next message to be created.
    pub next_message_index: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct MessageContext {
    /// The current chain id.
    pub chain_id: ChainId,
    /// Whether the message was rejected by the original receiver and is now bouncing back.
    pub is_bouncing: bool,
    /// The authenticated signer of the operation that created the message, if any.
    pub authenticated_signer: Option<Owner>,
    /// The current block height.
    pub height: BlockHeight,
    /// The hash of the remote certificate that created the message.
    pub certificate_hash: CryptoHash,
    /// The id of the message (based on the operation height and index in the remote
    /// certificate).
    pub message_id: MessageId,
}

#[derive(Clone, Copy, Debug)]
pub struct CalleeContext {
    /// The current chain id.
    pub chain_id: ChainId,
    /// The authenticated signer for the execution thread, if any.
    pub authenticated_signer: Option<Owner>,
    /// `None` if the caller doesn't want this particular call to be authenticated (e.g.
    /// for safety reasons).
    pub authenticated_caller_id: Option<UserApplicationId>,
}

#[derive(Clone, Copy, Debug)]
pub struct QueryContext {
    /// The current chain id.
    pub chain_id: ChainId,
}

pub trait BaseRuntime {
    type Read: fmt::Debug + Send;
    type ContainsKey: fmt::Debug + Send;
    type ReadMultiValuesBytes: fmt::Debug + Send;
    type ReadValueBytes: fmt::Debug + Send;
    type FindKeysByPrefix: fmt::Debug + Send;
    type FindKeyValuesByPrefix: fmt::Debug + Send;

    /// The current chain id.
    fn chain_id(&mut self) -> Result<ChainId, ExecutionError>;

    /// The current application id.
    fn application_id(&mut self) -> Result<UserApplicationId, ExecutionError>;

    /// The current application parameters.
    fn application_parameters(&mut self) -> Result<Vec<u8>, ExecutionError>;

    /// Reads the system balance.
    fn read_system_balance(&mut self) -> Result<Amount, ExecutionError>;

    /// Reads the system timestamp.
    fn read_system_timestamp(&mut self) -> Result<Timestamp, ExecutionError>;

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

    /// Writes a batch of changes.
    ///
    /// Hack: This fails for services.
    fn write_batch(&mut self, batch: Batch) -> Result<(), ExecutionError>;

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
}

pub trait ServiceRuntime: BaseRuntime {
    /// Queries another application.
    fn try_query_application(
        &mut self,
        queried_id: UserApplicationId,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError>;
}

/// The result of calling into an application or a session.
pub struct CallOutcome {
    /// The return value.
    pub value: Vec<u8>,
    /// The new sessions now visible to the caller.
    pub sessions: Vec<SessionId>,
}

pub trait ContractRuntime: BaseRuntime {
    /// Returns the amount of execution fuel remaining before execution is aborted.
    fn remaining_fuel(&mut self) -> Result<u64, ExecutionError>;

    /// Sets the amount of execution fuel remaining before execution is aborted.
    fn set_remaining_fuel(&mut self, remaining_fuel: u64) -> Result<(), ExecutionError>;

    /// Calls another application. Forwarded sessions will now be visible to
    /// `callee_id` (but not to the caller any more).
    fn try_call_application(
        &mut self,
        authenticated: bool,
        callee_id: UserApplicationId,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<CallOutcome, ExecutionError>;

    /// Calls into a session that is in our scope. Forwarded sessions will be visible to
    /// the application that runs `session_id`.
    fn try_call_session(
        &mut self,
        authenticated: bool,
        session_id: SessionId,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<CallOutcome, ExecutionError>;
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
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct RawOutgoingMessage<Message> {
    /// The destination of the message.
    pub destination: Destination,
    /// Whether the message is authenticated.
    pub authenticated: bool,
    /// The kind of outgoing message being sent.
    pub kind: MessageKind,
    /// The message itself.
    pub message: Message,
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
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct RawExecutionOutcome<Message> {
    /// The signer who created the messages.
    pub authenticated_signer: Option<Owner>,
    /// Sends messages to the given destinations, possibly forwarding the authenticated
    /// signer.
    pub messages: Vec<RawOutgoingMessage<Message>>,
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
    /// The chain id broadcasting on this channel.
    pub chain_id: ChainId,
    /// The name of the channel.
    pub name: ChannelName,
}

/// Externally visible results of an execution, tagged by their application.
#[derive(Debug)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
#[allow(clippy::large_enum_variant)]
pub enum ExecutionOutcome {
    System(RawExecutionOutcome<SystemMessage>),
    User(UserApplicationId, RawExecutionOutcome<Vec<u8>>),
}

impl ExecutionOutcome {
    pub fn application_id(&self) -> GenericApplicationId {
        match self {
            ExecutionOutcome::System(_) => GenericApplicationId::System,
            ExecutionOutcome::User(app_id, _) => GenericApplicationId::User(*app_id),
        }
    }
}

impl<Message> RawExecutionOutcome<Message> {
    pub fn with_authenticated_signer(mut self, authenticated_signer: Option<Owner>) -> Self {
        self.authenticated_signer = authenticated_signer;
        self
    }

    /// Adds a `message` to this [`RawExecutionOutcome`].
    pub fn with_message(mut self, message: RawOutgoingMessage<Message>) -> Self {
        self.messages.push(message);
        self
    }
}

impl<Message> Default for RawExecutionOutcome<Message> {
    fn default() -> Self {
        Self {
            authenticated_signer: None,
            messages: Vec::new(),
            subscribe: Vec::new(),
            unsubscribe: Vec::new(),
        }
    }
}

impl OperationContext {
    fn next_message_id(&self) -> MessageId {
        MessageId {
            chain_id: self.chain_id,
            height: self.height,
            index: self.next_message_index,
        }
    }
}

#[cfg(any(test, feature = "test"))]
#[derive(Clone)]
pub struct TestExecutionRuntimeContext {
    chain_id: ChainId,
    execution_runtime_config: ExecutionRuntimeConfig,
    user_contracts: Arc<DashMap<UserApplicationId, UserContractCode>>,
    user_services: Arc<DashMap<UserApplicationId, UserServiceCode>>,
}

#[cfg(any(test, feature = "test"))]
impl TestExecutionRuntimeContext {
    fn new(chain_id: ChainId, execution_runtime_config: ExecutionRuntimeConfig) -> Self {
        Self {
            chain_id,
            execution_runtime_config,
            user_contracts: Arc::default(),
            user_services: Arc::default(),
        }
    }
}

#[cfg(any(test, feature = "test"))]
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

    pub fn user<A: Abi>(
        application_id: UserApplicationId<A>,
        message: &A::Message,
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
    #[cfg(any(feature = "wasmer", feature = "wasmtime"))]
    pub(crate) fn new(bytes: Vec<u8>) -> Self {
        Bytecode { bytes }
    }

    /// Load bytecode from a Wasm module file.
    pub async fn load_from_file(path: impl AsRef<Path>) -> Result<Self, io::Error> {
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
#[cfg_attr(any(feature = "wasmtime", feature = "wasmer"), derive(Debug, Default))]
pub enum WasmRuntime {
    #[cfg(feature = "wasmer")]
    #[default]
    #[display(fmt = "wasmer")]
    Wasmer,
    #[cfg(feature = "wasmtime")]
    #[cfg_attr(not(feature = "wasmer"), default)]
    #[display(fmt = "wasmtime")]
    Wasmtime,
    #[cfg(feature = "wasmer")]
    WasmerWithSanitizer,
    #[cfg(feature = "wasmtime")]
    WasmtimeWithSanitizer,
}

/// Trait used to select a default WasmRuntime, if one is available.
pub trait WithWasmDefault {
    fn with_wasm_default(self) -> Self;
}

impl WasmRuntime {
    #[cfg(any(feature = "wasmer", feature = "wasmtime"))]
    pub fn default_with_sanitizer() -> Self {
        #[cfg(feature = "wasmer")]
        {
            WasmRuntime::WasmerWithSanitizer
        }
        #[cfg(not(feature = "wasmer"))]
        {
            WasmRuntime::WasmtimeWithSanitizer
        }
    }

    pub fn needs_sanitizer(self) -> bool {
        match self {
            #[cfg(feature = "wasmer")]
            WasmRuntime::WasmerWithSanitizer => true,
            #[cfg(feature = "wasmtime")]
            WasmRuntime::WasmtimeWithSanitizer => true,
            #[cfg(any(feature = "wasmtime", feature = "wasmer"))]
            _ => false,
        }
    }
}

impl WithWasmDefault for Option<WasmRuntime> {
    #[cfg(any(feature = "wasmer", feature = "wasmtime"))]
    fn with_wasm_default(self) -> Self {
        Some(self.unwrap_or_default())
    }

    #[cfg(not(any(feature = "wasmer", feature = "wasmtime")))]
    fn with_wasm_default(self) -> Self {
        None
    }
}

impl FromStr for WasmRuntime {
    type Err = InvalidWasmRuntime;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        match string {
            #[cfg(feature = "wasmer")]
            "wasmer" => Ok(WasmRuntime::Wasmer),
            #[cfg(feature = "wasmtime")]
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

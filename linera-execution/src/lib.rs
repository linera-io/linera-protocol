// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module manages the execution of the system application and the user applications in a Linera chain.

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

pub use crate::runtime::{ContractSyncRuntime, ServiceSyncRuntime};
#[cfg(with_testing)]
pub use applications::ApplicationRegistry;
pub use applications::{
    ApplicationRegistryView, BytecodeLocation, GenericApplicationId, UserApplicationDescription,
    UserApplicationId,
};
pub use execution::ExecutionStateView;
pub use linera_base::execution::{
    CalleeContext, MessageContext, MessageKind, OperationContext, QueryContext,
    RawExecutionOutcome, RawOutgoingMessage,
};
pub use policy::{IntoPriced, ResourceControlPolicy};
pub use resources::{ResourceController, ResourceTracker};
pub use system::{
    SystemExecutionError, SystemExecutionStateView, SystemMessage, SystemOperation, SystemQuery,
    SystemResponse,
};
#[cfg(all(with_testing, any(with_wasmer, with_wasmtime)))]
pub use wasm::test as wasm_test;
#[cfg(with_wasm_runtime)]
pub use wasm::{WasmContractModule, WasmExecutionError, WasmServiceModule};

use async_graphql::SimpleObject;
use async_trait::async_trait;
use custom_debug_derive::Debug;
use dashmap::DashMap;
use derive_more::Display;
use linera_base::{
    abi::Abi,
    data_types::{Amount, ArithmeticError, Resources, Timestamp},
    doc_scalar, hex_debug,
    identifiers::{Account, BytecodeId, ChainId, ChannelName, Destination, Owner, SessionId},
    ownership::ChainOwnership,
};
use linera_views::{batch::Batch, views::ViewError};
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr, sync::Arc};
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
    #[cfg(any(with_wasmer, with_wasmtime))]
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

    /// Reads the balance of the chain.
    fn read_chain_balance(&mut self) -> Result<Amount, ExecutionError>;

    /// Reads the owner balance.
    fn read_owner_balance(&mut self, owner: Owner) -> Result<Amount, ExecutionError>;

    /// Reads the system timestamp.
    fn read_system_timestamp(&mut self) -> Result<Timestamp, ExecutionError>;

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

    /// Consumes some of the execution fuel.
    fn consume_fuel(&mut self, fuel: u64) -> Result<(), ExecutionError>;

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

    /// Opens a new chain.
    fn open_chain(
        &mut self,
        ownership: ChainOwnership,
        balance: Amount,
    ) -> Result<ChainId, ExecutionError>;
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

impl<Message> IntoPriced for RawExecutionOutcome<Message, Resources> {
    type Output = RawExecutionOutcome<Message, Amount>;

    fn into_priced(self, policy: &ResourceControlPolicy) -> Result<Self::Output, ArithmeticError> {
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
    pub fn new(chain_id: ChainId, execution_runtime_config: ExecutionRuntimeConfig) -> Self {
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
    #[cfg(any(test, with_wasm_runtime))]
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

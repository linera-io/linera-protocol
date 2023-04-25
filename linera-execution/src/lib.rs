// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod applications;
pub mod committee;
mod execution;
mod graphql;
mod ownership;
mod runtime;
pub mod system;
mod wasm;

pub use applications::{
    ApplicationId, ApplicationRegistryView, BytecodeLocation, UserApplicationDescription,
    UserApplicationId,
};
pub use execution::ExecutionStateView;
pub use ownership::ChainOwnership;
pub use system::{
    SystemEffect, SystemExecutionError, SystemExecutionStateView, SystemOperation, SystemQuery,
    SystemResponse,
};
#[cfg(all(
    any(test, feature = "test"),
    any(feature = "wasmer", feature = "wasmtime")
))]
pub use wasm::test as wasm_test;
#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
pub use wasm::{WasmApplication, WasmExecutionError};
#[cfg(any(test, feature = "test"))]
pub use {applications::ApplicationRegistry, system::SystemExecutionState};

use async_graphql::SimpleObject;
use async_trait::async_trait;
use custom_debug_derive::Debug;
use dashmap::DashMap;
use derive_more::Display;
use linera_base::{
    crypto::CryptoHash,
    data_types::{ArithmeticError, Balance, BlockHeight, Timestamp},
    hex_debug,
    identifiers::{BytecodeId, ChainId, ChannelName, Destination, EffectId, Owner, SessionId},
};
use linera_views::{batch::Batch, views::ViewError};
use serde::{Deserialize, Serialize};
use std::{io, path::Path, str::FromStr, sync::Arc};
use thiserror::Error;

/// An implementation of [`UserApplication`]
pub type UserApplicationCode = Arc<dyn UserApplication + Send + Sync + 'static>;

#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error(transparent)]
    ViewError(ViewError),
    #[error(transparent)]
    ArithmeticError(#[from] ArithmeticError),
    #[error(transparent)]
    SystemError(#[from] SystemExecutionError),
    #[error("User application reported an error: {0}")]
    UserError(String),
    #[cfg(any(feature = "wasmer", feature = "wasmtime"))]
    #[error(transparent)]
    WasmError(#[from] WasmExecutionError),

    #[error("A session is still opened at the end of a transaction")]
    SessionWasNotClosed,
    #[error("Invalid operation for this application")]
    InvalidOperation,
    #[error("Invalid effect for this application")]
    InvalidEffect,
    #[error("Invalid query for this application")]
    InvalidQuery,
    #[error("Can't call another application during a query")]
    CallApplicationFromQuery,
    #[error("Queries can't change application state")]
    LockStateFromQuery,
    #[error("Session does not exist or was already closed")]
    InvalidSession,
    #[error("Attempted to call or forward an active session")]
    SessionIsInUse,
    #[error("Session is not accessible by this owner")]
    InvalidSessionOwner,
    #[error("Attempted to call an application while the state is locked")]
    ApplicationIsInUse,
    #[error("Attempted to get an entry that is not locked")]
    ApplicationStateNotLocked,

    #[error("Bytecode ID {0:?} is invalid")]
    InvalidBytecodeId(BytecodeId),
    #[error("Failed to load bytecode from storage {0:?}")]
    ApplicationBytecodeNotFound(Box<UserApplicationDescription>),
}

impl From<ViewError> for ExecutionError {
    fn from(error: ViewError) -> Self {
        match error {
            ViewError::TryLockError(_) => ExecutionError::ApplicationIsInUse,
            error => ExecutionError::ViewError(error),
        }
    }
}

/// The public entry points provided by an application.
#[async_trait]
pub trait UserApplication {
    /// Initializes the application state on the chain that owns the application.
    async fn initialize(
        &self,
        context: &OperationContext,
        runtime: &dyn ContractRuntime,
        argument: &[u8],
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError>;

    /// Applies an operation from the current block.
    async fn execute_operation(
        &self,
        context: &OperationContext,
        runtime: &dyn ContractRuntime,
        operation: &[u8],
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError>;

    /// Applies an effect originating from a cross-chain message.
    async fn execute_effect(
        &self,
        context: &EffectContext,
        runtime: &dyn ContractRuntime,
        effect: &[u8],
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError>;

    /// Executes a call from another application.
    ///
    /// When an application is executing an operation or an effect it may call other applications,
    /// which can in turn call other applications.
    async fn handle_application_call(
        &self,
        context: &CalleeContext,
        runtime: &dyn ContractRuntime,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, ExecutionError>;

    /// Executes a call from another application into a session created by this application.
    async fn handle_session_call(
        &self,
        context: &CalleeContext,
        runtime: &dyn ContractRuntime,
        session_kind: u64,
        session_data: &mut Vec<u8>,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallResult, ExecutionError>;

    /// Executes unmetered read-only queries on the state of this application.
    ///
    /// # Note
    ///
    /// This is not meant to be metered and may not be exposed by all validators.
    async fn query_application(
        &self,
        context: &QueryContext,
        runtime: &dyn ServiceRuntime,
        argument: &[u8],
    ) -> Result<Vec<u8>, ExecutionError>;
}

/// The result of calling into a user application.
#[derive(Default)]
pub struct ApplicationCallResult {
    /// The return value.
    pub value: Vec<u8>,
    /// The externally-visible result.
    pub execution_result: RawExecutionResult<Vec<u8>>,
    /// The new sessions that were just created by the callee for us.
    pub create_sessions: Vec<NewSession>,
}

/// The result of calling into a session.
#[derive(Default)]
pub struct SessionCallResult {
    /// The application result.
    pub inner: ApplicationCallResult,
    /// If `call_session` was called, this tells the system to clean up the session.
    pub close_session: bool,
}

/// Syscall to request creating a new session.
#[derive(Default)]
pub struct NewSession {
    /// A kind provided by the creator (meant to be visible to other applications).
    pub kind: u64,
    /// The data associated to the session.
    pub data: Vec<u8>,
}

/// Requirements for the `extra` field in our state views (and notably the
/// [`ExecutionStateView`]).
#[async_trait]
pub trait ExecutionRuntimeContext {
    fn chain_id(&self) -> ChainId;

    fn user_applications(&self) -> &Arc<DashMap<UserApplicationId, UserApplicationCode>>;

    async fn get_user_application(
        &self,
        description: &UserApplicationDescription,
    ) -> Result<UserApplicationCode, ExecutionError>;
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
}

#[derive(Clone, Copy, Debug)]
pub struct EffectContext {
    /// The current chain id.
    pub chain_id: ChainId,
    /// The authenticated signer of the operation that created the effect, if any.
    pub authenticated_signer: Option<Owner>,
    /// The current block height.
    pub height: BlockHeight,
    /// The hash of the remote certificate that created the effect.
    pub certificate_hash: CryptoHash,
    /// The id of the effect (based on the operation height and index in the remote
    /// certificate).
    pub effect_id: EffectId,
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

#[async_trait]
pub trait BaseRuntime: Send + Sync {
    /// The current chain id.
    fn chain_id(&self) -> ChainId;

    /// The current application id.
    fn application_id(&self) -> UserApplicationId;

    /// The current application parameters.
    fn application_parameters(&self) -> Vec<u8>;

    /// Reads the system balance.
    fn read_system_balance(&self) -> Balance;

    /// Reads the system timestamp.
    fn read_system_timestamp(&self) -> Timestamp;

    /// Reads the application state.
    async fn try_read_my_state(&self) -> Result<Vec<u8>, ExecutionError>;

    /// Locks the view user state and prevents further reading/loading
    async fn lock_view_user_state(&self) -> Result<(), ExecutionError>;

    /// Unlocks the view user state and allows reading/loading again
    async fn unlock_view_user_state(&self) -> Result<(), ExecutionError>;

    /// Reads the key from the KV store
    async fn read_key_bytes(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, ExecutionError>;

    /// Reads the data from the keys having a specific prefix.
    async fn find_keys_by_prefix(
        &self,
        key_prefix: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, ExecutionError>;

    /// Reads the data from the key/values having a specific prefix.
    async fn find_key_values_by_prefix(
        &self,
        key_prefix: Vec<u8>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError>;
}

#[async_trait]
pub trait ServiceRuntime: BaseRuntime {
    /// Queries another application.
    async fn try_query_application(
        &self,
        queried_id: UserApplicationId,
        argument: &[u8],
    ) -> Result<Vec<u8>, ExecutionError>;
}

/// The result of calling into an application or a session.
pub struct CallResult {
    /// The return value.
    pub value: Vec<u8>,
    /// The new sessions now visible to the caller.
    pub sessions: Vec<SessionId>,
}

#[async_trait]
pub trait ContractRuntime: BaseRuntime {
    /// Returns the amount of execution fuel remaining before execution is aborted.
    fn remaining_fuel(&self) -> u64;

    /// Sets the amount of execution fuel remaining before execution is aborted.
    fn set_remaining_fuel(&self, remaining_fuel: u64);

    /// Reads the application state and prevents further reading/loading until the state is saved.
    async fn try_read_and_lock_my_state(&self) -> Result<Vec<u8>, ExecutionError>;

    /// Saves the application state and allows reading/loading the state again.
    fn save_and_unlock_my_state(&self, state: Vec<u8>) -> Result<(), ExecutionError>;

    /// Allows reading/loading the state again (without saving anything).
    fn unlock_my_state(&self);

    /// Writes the batch and then unlock
    async fn write_batch_and_unlock(&self, batch: Batch) -> Result<(), ExecutionError>;

    /// Calls another application. Forwarded sessions will now be visible to
    /// `callee_id` (but not to the caller any more).
    async fn try_call_application(
        &self,
        authenticated: bool,
        callee_id: UserApplicationId,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<CallResult, ExecutionError>;

    /// Calls into a session that is in our scope. Forwarded sessions will be visible to
    /// the application that runs `session_id`.
    async fn try_call_session(
        &self,
        authenticated: bool,
        session_id: SessionId,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<CallResult, ExecutionError>;
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

/// An effect to be sent and possibly executed in the receiver's block.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Effect {
    /// A system effect.
    System(SystemEffect),
    /// A user effect (in serialized form).
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

/// Externally visible results of an execution. These results are meant in the context of
/// the application that created them.
#[derive(Debug)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct RawExecutionResult<Effect> {
    /// The signer who created the effects.
    pub authenticated_signer: Option<Owner>,
    /// Sends messages to the given destinations, possibly forwarding the authenticated
    /// signer.
    pub effects: Vec<(Destination, bool, Effect)>,
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
pub enum ExecutionResult {
    System(RawExecutionResult<SystemEffect>),
    User(UserApplicationId, RawExecutionResult<Vec<u8>>),
}

impl ExecutionResult {
    pub fn application_id(&self) -> ApplicationId {
        match self {
            ExecutionResult::System(_) => ApplicationId::System,
            ExecutionResult::User(app_id, _) => ApplicationId::User(*app_id),
        }
    }
}

impl<Effect> RawExecutionResult<Effect> {
    pub fn with_authenticated_signer(mut self, authenticated_signer: Option<Owner>) -> Self {
        self.authenticated_signer = authenticated_signer;
        self
    }
}

impl<Effect> Default for RawExecutionResult<Effect> {
    fn default() -> Self {
        Self {
            authenticated_signer: None,
            effects: Vec::new(),
            subscribe: Vec::new(),
            unsubscribe: Vec::new(),
        }
    }
}

impl From<OperationContext> for EffectId {
    fn from(context: OperationContext) -> Self {
        Self {
            chain_id: context.chain_id,
            height: context.height,
            index: context.index,
        }
    }
}

#[cfg(any(test, feature = "test"))]
#[derive(Clone)]
pub struct TestExecutionRuntimeContext {
    chain_id: ChainId,
    user_applications: Arc<DashMap<UserApplicationId, UserApplicationCode>>,
}

#[cfg(any(test, feature = "test"))]
impl TestExecutionRuntimeContext {
    fn new(chain_id: ChainId) -> Self {
        Self {
            chain_id,
            user_applications: Arc::default(),
        }
    }
}

#[cfg(any(test, feature = "test"))]
#[async_trait]
impl ExecutionRuntimeContext for TestExecutionRuntimeContext {
    fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    fn user_applications(&self) -> &Arc<DashMap<UserApplicationId, UserApplicationCode>> {
        &self.user_applications
    }

    async fn get_user_application(
        &self,
        description: &UserApplicationDescription,
    ) -> Result<UserApplicationCode, ExecutionError> {
        let application_id = description.into();
        Ok(self
            .user_applications()
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
    pub fn application_id(&self) -> ApplicationId {
        match self {
            Self::System(_) => ApplicationId::System,
            Self::User { application_id, .. } => ApplicationId::User(*application_id),
        }
    }
}

impl From<SystemEffect> for Effect {
    fn from(effect: SystemEffect) -> Self {
        Effect::System(effect)
    }
}

impl Effect {
    pub fn application_id(&self) -> ApplicationId {
        match self {
            Self::System(_) => ApplicationId::System,
            Self::User { application_id, .. } => ApplicationId::User(*application_id),
        }
    }
}

impl From<SystemQuery> for Query {
    fn from(query: SystemQuery) -> Self {
        Query::System(query)
    }
}

impl Query {
    pub fn application_id(&self) -> ApplicationId {
        match self {
            Self::System(_) => ApplicationId::System,
            Self::User { application_id, .. } => ApplicationId::User(*application_id),
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

    /// Load bytecode from a WASM module file.
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

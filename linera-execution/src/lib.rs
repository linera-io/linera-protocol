// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod applications;
mod execution;
mod ownership;
mod runtime;
pub mod system;
mod wasm;

pub use applications::{
    ApplicationDescription, ApplicationId, ApplicationRegistryView, BytecodeId, BytecodeLocation,
    UserApplicationDescription, UserApplicationId,
};
pub use execution::ExecutionStateView;
pub use ownership::ChainOwnership;
#[cfg(any(test, feature = "test"))]
pub use system::SystemExecutionState;
pub use system::{
    SystemEffect, SystemExecutionError, SystemExecutionStateView, SystemOperation, SystemQuery,
    SystemResponse,
};
#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
pub use wasm::{WasmApplication, WasmExecutionError};

use async_trait::async_trait;
use dashmap::DashMap;
use linera_base::data_types::{BlockHeight, ChainId, EffectId};
use linera_views::views::ViewError;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

/// An implementation of [`UserApplication`]
pub type UserApplicationCode = Arc<dyn UserApplication + Send + Sync + 'static>;

#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error(transparent)]
    ViewError(ViewError),
    #[error(transparent)]
    SystemError(#[from] SystemExecutionError),
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

    #[error("Bytecode ID {0:?} is invalid")]
    InvalidBytecodeId(BytecodeId),
    #[error("Attempt to create an application using unknown bytecode {0:?}")]
    UnknownBytecode(BytecodeId),
    #[error("Application {0:?} is not known by the chain")]
    UnknownApplication(Box<UserApplicationId>),
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
    /// Initialize the application state on the chain that owns the application.
    async fn initialize(
        &self,
        context: &OperationContext,
        storage: &dyn WritableStorage,
        argument: &[u8],
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError>;

    /// Apply an operation from the current block.
    async fn execute_operation(
        &self,
        context: &OperationContext,
        storage: &dyn WritableStorage,
        operation: &[u8],
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError>;

    /// Apply an effect originating from a cross-chain message.
    async fn execute_effect(
        &self,
        context: &EffectContext,
        storage: &dyn WritableStorage,
        effect: &[u8],
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError>;

    /// Allow an operation or an effect of other applications to call into this
    /// user application.
    async fn call_application(
        &self,
        context: &CalleeContext,
        storage: &dyn WritableStorage,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, ExecutionError>;

    /// Allow an operation or an effect of other applications to call into a session that
    /// we previously created.
    async fn call_session(
        &self,
        context: &CalleeContext,
        storage: &dyn WritableStorage,
        session_kind: u64,
        session_data: &mut Vec<u8>,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallResult, ExecutionError>;

    /// Allow an end user to execute read-only queries on the state of this application.
    /// NOTE: This is not meant to be metered and may not be exposed by all validators.
    async fn query_application(
        &self,
        context: &QueryContext,
        storage: &dyn QueryableStorage,
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
    /// The current block height.
    pub height: BlockHeight,
    /// The current index of the operation.
    pub index: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct EffectContext {
    /// The current chain id.
    pub chain_id: ChainId,
    /// The current block height.
    pub height: BlockHeight,
    /// The id of the effect (based on the operation height and index in the remote
    /// chain that created the effect).
    pub effect_id: EffectId,
}

#[derive(Clone, Copy, Debug)]
pub struct CalleeContext {
    /// The current chain id.
    pub chain_id: ChainId,
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
pub trait ReadableStorage: Send + Sync {
    /// The current chain id.
    fn chain_id(&self) -> ChainId;

    /// The current application id.
    fn application_id(&self) -> UserApplicationId;

    /// Read the system balance.
    fn read_system_balance(&self) -> crate::system::Balance;

    /// Read the application state.
    async fn try_read_my_state(&self) -> Result<Vec<u8>, ExecutionError>;
}

#[async_trait]
pub trait QueryableStorage: ReadableStorage {
    /// Query another application.
    async fn try_query_application(
        &self,
        queried_id: UserApplicationId,
        argument: &[u8],
    ) -> Result<Vec<u8>, ExecutionError>;
}

/// The identifier of a session.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct SessionId {
    /// The application that runs the session.
    pub application_id: UserApplicationId,
    /// User-defined tag.
    pub kind: u64,
    /// Unique index set by the runtime.
    index: u64,
}

/// The result of calling into an application or a session.
pub struct CallResult {
    /// The return value.
    pub value: Vec<u8>,
    /// The new sessions now visible to the caller.
    pub sessions: Vec<SessionId>,
}

#[async_trait]
pub trait WritableStorage: ReadableStorage {
    /// Read the application state and prevent further reading/loading until the state is saved.
    async fn try_read_and_lock_my_state(&self) -> Result<Vec<u8>, ExecutionError>;

    /// Save the application state and allow reading/loading the state again.
    fn save_and_unlock_my_state(&self, state: Vec<u8>) -> Result<(), ApplicationStateNotLocked>;

    /// Allow reading/loading the state again (without saving anything).
    fn unlock_my_state(&self);

    /// Call another application. Forwarded sessions will now be visible to
    /// `callee_id` (but not to the caller any more).
    async fn try_call_application(
        &self,
        authenticated: bool,
        callee_id: UserApplicationId,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<CallResult, ExecutionError>;

    /// Call into a session that is in our scope. Forwarded sessions will be visible to
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
    User(Vec<u8>),
}

/// An effect to be sent and possibly executed in the receiver's block.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Effect {
    /// A system effect.
    System(SystemEffect),
    /// A user effect (in serialized form).
    User(Vec<u8>),
}

/// An query to be sent and possibly executed in the receiver's block.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Query {
    /// A system query.
    System(SystemQuery),
    /// A user query (in serialized form).
    User(Vec<u8>),
}

/// The response to a query.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Response {
    /// A system response.
    System(SystemResponse),
    /// A user response (in serialized form).
    User(Vec<u8>),
}

/// Externally visible results of an execution. These results are meant in the context of
/// the application that created them.
#[derive(Debug)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct RawExecutionResult<Effect> {
    /// Send messages to the given destinations.
    pub effects: Vec<(Destination, Effect)>,
    /// Subscribe chains to channels.
    pub subscribe: Vec<(ChannelName, ChainId)>,
    /// Unsubscribe chains to channels.
    pub unsubscribe: Vec<(ChannelName, ChainId)>,
}

/// The name of a subscription channel.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ChannelName(Vec<u8>);

impl From<Vec<u8>> for ChannelName {
    fn from(name: Vec<u8>) -> Self {
        ChannelName(name)
    }
}

impl AsRef<[u8]> for ChannelName {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// The identifier of a channel, relative to a particular application.
#[derive(Eq, PartialEq, Ord, PartialOrd, Debug, Clone, Hash, Serialize, Deserialize)]
pub struct ChannelId {
    pub chain_id: ChainId,
    pub name: ChannelName,
}

/// The destination of a message, relative to a particular application.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Destination {
    /// Direct message to a chain.
    Recipient(ChainId),
    /// Broadcast to the current subscribers of our channel.
    Subscribers(ChannelName),
}

/// Externally visible results of an execution, tagged by their application.
#[derive(Debug)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub enum ExecutionResult {
    System {
        result: RawExecutionResult<SystemEffect>,
        new_application: Option<NewApplication>,
    },

    User(UserApplicationId, RawExecutionResult<Vec<u8>>),
}

/// A request to create a new application.
#[derive(Clone, Debug)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct NewApplication {
    id: UserApplicationId,
    initialization_argument: Vec<u8>,
}

impl<Effect> Default for RawExecutionResult<Effect> {
    fn default() -> Self {
        Self {
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
            .ok_or_else(|| ExecutionError::UnknownApplication(Box::new(application_id)))?
            .clone())
    }
}

impl From<SystemOperation> for Operation {
    fn from(operation: SystemOperation) -> Self {
        Operation::System(operation)
    }
}

impl From<Vec<u8>> for Operation {
    fn from(operation: Vec<u8>) -> Self {
        Operation::User(operation)
    }
}

impl From<SystemEffect> for Effect {
    fn from(effect: SystemEffect) -> Self {
        Effect::System(effect)
    }
}

impl From<Vec<u8>> for Effect {
    fn from(effect: Vec<u8>) -> Self {
        Effect::User(effect)
    }
}

impl From<SystemQuery> for Query {
    fn from(query: SystemQuery) -> Self {
        Query::System(query)
    }
}

impl From<Vec<u8>> for Query {
    fn from(query: Vec<u8>) -> Self {
        Query::User(query)
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

#[derive(Clone, Copy, Debug, Error)]
#[error("The application state can not be saved because it was not locked for writing")]
pub struct ApplicationStateNotLocked;

/// A WebAssembly module's bytecode.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Bytecode(Vec<u8>);

impl AsRef<[u8]> for Bytecode {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

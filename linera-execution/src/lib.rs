// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod execution;
mod ownership;
mod runtime;
pub mod system;

pub use execution::{ExecutionStateView, ExecutionStateViewContext};
pub use ownership::ChainOwnership;
#[cfg(any(test, feature = "test"))]
pub use system::SystemExecutionState;
pub use system::{
    SystemEffect, SystemExecutionStateView, SystemExecutionStateViewContext, SystemOperation,
    SystemQuery, SystemResponse,
};

use async_trait::async_trait;
use dashmap::DashMap;
use linera_base::{
    error::Error,
    messages::{ApplicationId, BlockHeight, ChainId, Destination, EffectId},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// An implementation of [`UserApplication`]
pub type UserApplicationCode = Arc<dyn UserApplication + Send + Sync + 'static>;

/// The public entry points provided by an application.
#[async_trait]
pub trait UserApplication {
    /// Apply an operation from the current block.
    async fn apply_operation(
        &self,
        context: &OperationContext,
        storage: &dyn WritableStorage,
        operation: &[u8],
    ) -> Result<RawApplicationResult<Vec<u8>>, Error>;

    /// Apply an effect originating from a cross-chain message.
    async fn apply_effect(
        &self,
        context: &EffectContext,
        storage: &dyn WritableStorage,
        effect: &[u8],
    ) -> Result<RawApplicationResult<Vec<u8>>, Error>;

    /// Allow an operation or an effect of other applications to call into this
    /// application.
    async fn call_application(
        &self,
        context: &CalleeContext,
        storage: &dyn WritableStorage,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<RawCallResult, Error>;

    /// Allow an operation or an effect of other applications to call into a session that we previously created.
    async fn call_session(
        &self,
        context: &CalleeContext,
        storage: &dyn WritableStorage,
        session_kind: u64,
        session_data: &mut Vec<u8>,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<RawCallResult, Error>;

    /// Allow an end user to execute read-only queries on the state of this application.
    /// NOTE: This is not meant to be metered and may not be exposed by all validators.
    async fn query_application(
        &self,
        context: &QueryContext,
        storage: &dyn QueryableStorage,
        argument: &[u8],
    ) -> Result<Vec<u8>, Error>;
}

/// The result of calling into an application (or one of its open sessions).
#[derive(Default)]
pub struct RawCallResult {
    /// The return value.
    pub value: Vec<u8>,
    /// The externally-visible result.
    pub application_result: RawApplicationResult<Vec<u8>>,
    /// The new sessions that were just created by the callee for us.
    pub create_sessions: Vec<NewSession>,
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
pub trait ExecutionRuntimeContext {
    #[cfg(any(test, feature = "test"))]
    fn new(chain_id: ChainId) -> Self;

    fn chain_id(&self) -> ChainId;

    fn user_applications(&self) -> &Arc<DashMap<ApplicationId, UserApplicationCode>>;

    fn get_user_application(
        &self,
        application_id: ApplicationId,
    ) -> Result<UserApplicationCode, Error> {
        Ok(self
            .user_applications()
            .get(&application_id)
            .ok_or(Error::UnknownApplication)?
            .clone())
    }
}

#[derive(Debug, Clone)]
pub struct OperationContext {
    /// The current chain id.
    pub chain_id: ChainId,
    /// The current block height.
    pub height: BlockHeight,
    /// The current index of the operation.
    pub index: usize,
}

#[derive(Debug, Clone)]
pub struct EffectContext {
    /// The current chain id.
    pub chain_id: ChainId,
    /// The current block height.
    pub height: BlockHeight,
    /// The id of the effect (based on the operation height and index in the remote
    /// chain that created the effect).
    pub effect_id: EffectId,
}

#[derive(Debug, Clone)]
pub struct CalleeContext {
    /// The current chain id.
    pub chain_id: ChainId,
    /// `None` if the caller doesn't want this particular call to be authenticated (e.g.
    /// for safety reasons).
    pub authenticated_caller_id: Option<ApplicationId>,
}

#[derive(Debug, Clone)]
pub struct QueryContext {
    /// The current chain id.
    pub chain_id: ChainId,
}

#[async_trait]
pub trait ReadableStorage: Send + Sync {
    /// The current chain id.
    fn chain_id(&self) -> ChainId;

    /// The current application id.
    fn application_id(&self) -> ApplicationId;

    /// Read the system balance.
    fn read_system_balance(&self) -> crate::system::Balance;

    /// Read the application state.
    async fn try_read_my_state(&self) -> Result<Vec<u8>, Error>;
}

#[async_trait]
pub trait QueryableStorage: ReadableStorage {
    /// Query another application.
    async fn try_query_application(
        &self,
        queried_id: ApplicationId,
        argument: &[u8],
    ) -> Result<Vec<u8>, Error>;
}

/// The identifier of a session.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct SessionId {
    /// The application that runs the session.
    pub application_id: ApplicationId,
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
    async fn try_read_and_lock_my_state(&self) -> Result<Vec<u8>, Error>;

    /// Save the application state and allow reading/loading the state again.
    fn save_and_unlock_my_state(&self, state: Vec<u8>);

    /// Allow reading/loading the state again (without saving anything).
    fn unlock_my_state(&self);

    /// Call another application. Forwarded sessions will now be visible to
    /// `callee_id` (but not to the caller any more).
    async fn try_call_application(
        &self,
        authenticated: bool,
        callee_id: ApplicationId,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<CallResult, Error>;

    /// Call into a session that is in our scope. Forwarded sessions will be visible to
    /// the application that runs `session_id`.
    async fn try_call_session(
        &self,
        authenticated: bool,
        session_id: SessionId,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<CallResult, Error>;
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
pub struct RawApplicationResult<Effect> {
    /// Send messages to the given destinations.
    pub effects: Vec<(Destination, Effect)>,
    /// Subscribe chains to channels.
    pub subscribe: Vec<(String, ChainId)>,
    /// Unsubscribe chains to channels.
    pub unsubscribe: Vec<(String, ChainId)>,
}

/// Externally visible results of an execution, tagged by their application.
#[derive(Debug)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub enum ApplicationResult {
    System(RawApplicationResult<SystemEffect>),
    User(ApplicationId, RawApplicationResult<Vec<u8>>),
}

impl<Effect> Default for RawApplicationResult<Effect> {
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
    user_applications: Arc<DashMap<ApplicationId, UserApplicationCode>>,
}

#[cfg(any(test, feature = "test"))]
impl ExecutionRuntimeContext for TestExecutionRuntimeContext {
    fn new(chain_id: ChainId) -> Self {
        Self {
            chain_id,
            user_applications: Arc::default(),
        }
    }

    fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    fn user_applications(&self) -> &Arc<DashMap<ApplicationId, UserApplicationCode>> {
        &self.user_applications
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

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
};

use async_trait::async_trait;
use dashmap::DashMap;
use linera_base::{
    error::Error,
    messages::{ApplicationId, BlockHeight, ChainId, Destination, EffectId},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub type UserApplicationCode = Arc<dyn UserApplication + Send + Sync + 'static>;

#[async_trait]
pub trait UserApplication {
    /// Apply an operation from the current block.
    async fn apply_operation(
        &self,
        context: &OperationContext,
        storage: &dyn CallableStorageContext,
        operation: &[u8],
    ) -> Result<RawApplicationResult<Vec<u8>>, Error>;

    /// Apply an effect originating from a cross-chain message.
    async fn apply_effect(
        &self,
        context: &EffectContext,
        storage: &dyn CallableStorageContext,
        effect: &[u8],
    ) -> Result<RawApplicationResult<Vec<u8>>, Error>;

    /// Allow an operation or an effect of other applications to call into this
    /// application.
    async fn call(
        &self,
        context: &CalleeContext,
        storage: &dyn CallableStorageContext,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<RawCallResult, Error>;

    /// Allow an operation or an effect of other applications to call into a session that we previously created.
    async fn call_session(
        &self,
        context: &CalleeContext,
        storage: &dyn CallableStorageContext,
        session_kind: u64,
        session_data: &mut Vec<u8>,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<RawCallResult, Error>;

    /// Allow an end user to execute read-only queries on the state of this application.
    /// NOTE: This is not meant to be metered and may not be exposed by all validators.
    async fn query(
        &self,
        context: &QueryContext,
        storage: &dyn QueryableStorageContext,
        argument: &[u8],
    ) -> Result<Vec<u8>, Error>;
}

/// The result of calling into an application (or one of its open sessions).
pub struct RawCallResult {
    return_value: Vec<u8>,
    chain_effect: RawApplicationResult<Vec<u8>>,
    new_sessions: Vec<NewSession>,
    /// If `call_session` was called, this tells the system to clean up the session.
    close_session: bool,
}

/// Syscall to request creating a new session.
pub struct NewSession {
    /// A kind provided by the creator (meant to be visible to other applications).
    kind: u64,
    /// The data associated to the session.
    data: Vec<u8>,
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
    pub chain_id: ChainId,
    pub height: BlockHeight,
    pub index: usize,
}

#[derive(Debug, Clone)]
pub struct EffectContext {
    pub chain_id: ChainId,
    pub height: BlockHeight,
    pub effect_id: EffectId,
}

#[derive(Debug, Clone)]
pub struct CalleeContext {
    pub chain_id: ChainId,
    /// `None` if the caller doesn't want this particular call to be authenticated (e.g.
    /// for safety reasons).
    pub authenticated_caller_id: Option<ApplicationId>,
}

#[derive(Debug, Clone)]
pub struct QueryContext {
    pub chain_id: ChainId,
}

#[async_trait]
pub trait StorageContext: Send + Sync {
    /// Read the system balance.
    async fn try_read_system_balance(&self) -> Result<crate::system::Balance, Error>;

    /// Read the application state.
    async fn try_read_my_state(&self) -> Result<Vec<u8>, Error>;
}

#[async_trait]
pub trait QueryableStorageContext: StorageContext {
    /// Query another application.
    async fn try_query_application(
        &self,
        callee_id: ApplicationId,
        argument: &[u8],
    ) -> Result<Vec<u8>, Error>;
}

/// The identifier of a session.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct SessionId {
    pub application_id: ApplicationId,
    pub kind: u64,
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
pub trait CallableStorageContext: StorageContext {
    /// Read the application state and prevent further reading/loading until the state is saved.
    async fn try_load_my_state(&self) -> Result<Vec<u8>, Error>;

    /// Save the application state and allow reading/loading the state again.
    async fn try_save_my_state(&self, state: Vec<u8>) -> Result<(), Error>;

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

#[derive(Debug)]
pub struct RawApplicationResult<Effect> {
    pub effects: Vec<(Destination, Effect)>,
    pub subscribe: Vec<(String, ChainId)>,
    pub unsubscribe: Vec<(String, ChainId)>,
}

#[derive(Debug)]
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

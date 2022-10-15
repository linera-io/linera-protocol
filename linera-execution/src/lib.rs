// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod execution;
mod ownership;
pub mod system;

pub use execution::{ExecutionStateView, ExecutionStateViewContext};
pub use ownership::ChainOwnership;
#[cfg(any(test, feature = "test"))]
pub use system::SystemExecutionState;
pub use system::{
    SystemEffect, SystemExecutionStateView, SystemExecutionStateViewContext, SystemOperation,
};

use async_trait::async_trait;
use linera_base::{
    error::Error,
    messages::{ApplicationId, BlockHeight, ChainId, Destination, EffectId},
};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// Temporary map to hold a fixed set of prototyped smart-contracts.
static USER_APPLICATIONS: Lazy<
    Mutex<HashMap<ApplicationId, Arc<dyn UserApplication + Send + Sync + 'static>>>,
> = Lazy::new(|| {
    let m = HashMap::new();
    Mutex::new(m)
});

fn get_user_application(
    application_id: ApplicationId,
) -> Result<Arc<dyn UserApplication + Send + Sync + 'static>, Error> {
    let applications = USER_APPLICATIONS.lock().unwrap();
    Ok(applications
        .get(&application_id)
        .ok_or(Error::UnknownApplication)?
        .clone())
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

#[async_trait]
trait UserApplication {
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
        name: &str,
        argument: &[u8],
    ) -> Result<(Vec<u8>, RawApplicationResult<Vec<u8>>), Error>;

    /// Allow an end user to execute read-only queries on the state of this application.
    /// NOTE: This is not meant to be metered and may not be exposed by validators.
    async fn query(
        &self,
        context: &QueryContext,
        storage: &dyn QueryableStorageContext,
        name: &str,
        argument: &[u8],
    ) -> Result<Vec<u8>, Error>;
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
        name: &str,
        argument: &[u8],
    ) -> Result<Vec<u8>, Error>;
}

#[async_trait]
pub trait CallableStorageContext: StorageContext {
    /// Read the application state and prevent further reading/loading until the state is saved.
    async fn try_load_my_state(&self) -> Result<Vec<u8>, Error>;

    /// Save the application state and allow reading/loading the state again.
    async fn try_save_my_state(&self, state: Vec<u8>) -> Result<(), Error>;

    /// Call another application.
    async fn try_call_application(
        &self,
        authenticated: bool,
        callee_id: ApplicationId,
        name: &str,
        argument: &[u8],
    ) -> Result<Vec<u8>, Error>;
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

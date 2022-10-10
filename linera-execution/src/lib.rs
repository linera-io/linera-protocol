// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod execution;
mod system;

pub use execution::{ExecutionStateView, ExecutionStateViewContext};
#[cfg(any(test, feature = "test"))]
pub use system::SystemExecutionState;
pub use system::{SystemExecutionStateView, SystemExecutionStateViewContext};

use async_trait::async_trait;
use linera_base::{error::Error, messages::*, system::SystemEffect};
use once_cell::sync::Lazy;
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

#[async_trait]
trait UserApplication {
    /// Apply an operation from the current block.
    async fn apply_operation(
        &self,
        context: &OperationContext,
        state: &mut Vec<u8>,
        operation: &[u8],
    ) -> Result<RawApplicationResult<Vec<u8>>, Error>;

    /// Apply an effect originating from a cross-chain message.
    async fn apply_effect(
        &self,
        context: &EffectContext,
        state: &mut Vec<u8>,
        effect: &[u8],
    ) -> Result<RawApplicationResult<Vec<u8>>, Error>;

    /// Allow an operation or an effect of other applications to call into this
    /// application.
    async fn call(
        &self,
        context: &CalleeContext,
        state: &mut Vec<u8>,
        name: &str,
        argument: &[u8],
    ) -> Result<RawApplicationResult<Vec<u8>>, Error>;

    /// Allow an end user to execute read-only queries on the state of this application.
    /// NOTE: This is not meant to be metered and may not be exposed by validators.
    async fn query(
        &self,
        context: &QueryContext,
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

impl From<OperationContext> for EffectId {
    fn from(context: OperationContext) -> Self {
        Self {
            chain_id: context.chain_id,
            height: context.height,
            index: context.index,
        }
    }
}

#[derive(Debug)]
pub struct RawApplicationResult<Effect> {
    pub effects: Vec<(Destination, Effect)>,
    pub subscribe: Option<(String, ChainId)>,
    pub unsubscribe: Option<(String, ChainId)>,
}

#[derive(Debug)]
pub enum ApplicationResult {
    System(RawApplicationResult<SystemEffect>),
    User(RawApplicationResult<Vec<u8>>),
}

impl<Effect> Default for RawApplicationResult<Effect> {
    fn default() -> Self {
        Self {
            effects: Vec::new(),
            subscribe: None,
            unsubscribe: None,
        }
    }
}

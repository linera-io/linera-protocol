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
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, TryLockError};

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
        storage: StorageContext<'_, true>,
        operation: &[u8],
    ) -> Result<RawApplicationResult<Vec<u8>>, Error>;

    /// Apply an effect originating from a cross-chain message.
    async fn apply_effect(
        &self,
        context: &EffectContext,
        storage: StorageContext<'_, true>,
        effect: &[u8],
    ) -> Result<RawApplicationResult<Vec<u8>>, Error>;

    /// Allow an operation or an effect of other applications to call into this
    /// application.
    async fn call(
        &self,
        context: &CalleeContext,
        storage: StorageContext<'_, true>,
        name: &str,
        argument: &[u8],
    ) -> Result<(Vec<u8>, RawApplicationResult<Vec<u8>>), Error>;

    /// Allow an end user to execute read-only queries on the state of this application.
    /// NOTE: This is not meant to be metered and may not be exposed by validators.
    async fn query(
        &self,
        context: &QueryContext,
        storage: StorageContext<'_, false>,
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

#[derive(Debug, Clone)]
pub struct StorageContext<'a, const WRITABLE: bool> {
    chain_id: ChainId,
    application_id: ApplicationId,
    // TODO: use a proper shared collection view
    states: &'a HashMap<ApplicationId, Arc<RwLock<Vec<u8>>>>,
    results: Arc<Mutex<Vec<ApplicationResult>>>,
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

impl<'a, const W: bool> StorageContext<'a, W> {
    pub fn new(
        chain_id: ChainId,
        application_id: ApplicationId,
        states: &'a HashMap<ApplicationId, Arc<RwLock<Vec<u8>>>>,
        results: Arc<Mutex<Vec<ApplicationResult>>>,
    ) -> Self {
        Self {
            chain_id,
            application_id,
            states,
            results,
        }
    }

    pub fn try_read_my_state(&self) -> Result<OwnedRwLockReadGuard<Vec<u8>>, TryLockError> {
        let rc = self
            .states
            .get(&self.application_id)
            .expect("active applications should have a state in the map")
            .clone();
        rc.try_read_owned()
    }
}

impl<'a> StorageContext<'a, false> {
    /// Note that queries are not available from writable contexts.
    pub async fn try_query_application(
        &self,
        callee_id: ApplicationId,
        name: &str,
        argument: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let application = get_user_application(callee_id)?;
        let query_context = QueryContext {
            chain_id: self.chain_id,
        };
        let value = application
            .query(&query_context, self.clone(), name, argument)
            .await?;
        Ok(value)
    }
}

impl<'a> StorageContext<'a, true> {
    pub fn try_write_my_state(&self) -> Result<OwnedRwLockWriteGuard<Vec<u8>>, TryLockError> {
        let rc = self
            .states
            .get(&self.application_id)
            .expect("active applications should have a state in the map")
            .clone();
        rc.try_write_owned()
    }

    pub async fn try_call_application(
        &self,
        authenticated: bool,
        callee_id: ApplicationId,
        name: &str,
        argument: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let authenticated_caller_id = authenticated.then_some(self.application_id);
        let application = get_user_application(callee_id)?;
        let callee_context = CalleeContext {
            chain_id: self.chain_id,
            authenticated_caller_id,
        };
        let (value, result) = application
            .call(&callee_context, self.clone(), name, argument)
            .await?;
        self.results
            .try_lock()
            .unwrap()
            .push(ApplicationResult::User(callee_id, result));
        Ok(value)
    }
}

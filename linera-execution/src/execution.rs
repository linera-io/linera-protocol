// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    system::{SystemExecutionStateView, SystemExecutionStateViewContext, SYSTEM},
    ApplicationResult, CallableStorageContext, Effect, EffectContext, Operation, OperationContext,
    QueryableStorageContext,
};
use async_trait::async_trait;
use linera_base::{
    error::Error,
    messages::{ApplicationId, ChainId},
};
use linera_views::{
    impl_view,
    views::{
        CollectionOperations, ReentrantCollectionView, RegisterOperations, RegisterView, ScopedView,
    },
};
use tokio::sync::OwnedMutexGuard;

#[cfg(any(test, feature = "test"))]
use {
    crate::system::SystemExecutionState, linera_views::memory::MemoryContext,
    linera_views::views::View, std::collections::BTreeMap, std::sync::Arc, tokio::sync::Mutex,
};

/// A view accessing the execution state of a chain.
#[derive(Debug)]
pub struct ExecutionStateView<C> {
    /// System application.
    pub system: ScopedView<0, SystemExecutionStateView<C>>,
    /// User applications.
    pub users: ScopedView<1, ReentrantCollectionView<C, ApplicationId, RegisterView<C, Vec<u8>>>>,
}

impl_view!(
    ExecutionStateView {
        system,
        users,
    };
    SystemExecutionStateViewContext,
    RegisterOperations<Vec<u8>>,
    CollectionOperations<ApplicationId>,
);

#[cfg(any(test, feature = "test"))]
impl ExecutionStateView<MemoryContext<ChainId>> {
    /// Create an in-memory view where the system state is set. This is used notably to
    /// generate state hashes in tests.
    pub async fn from_system_state(state: SystemExecutionState) -> Self {
        let guard = Arc::new(Mutex::new(BTreeMap::new())).lock_owned().await;
        let extra = state
            .description
            .expect("Chain description should be set")
            .into();
        let context = MemoryContext::new(guard, extra);
        let mut view = Self::load(context)
            .await
            .expect("Loading from memory should work");
        view.system.description.set(state.description);
        view.system.epoch.set(state.epoch);
        view.system.admin_id.set(state.admin_id);
        for channel_id in state.subscriptions {
            view.system.subscriptions.insert(channel_id, ());
        }
        view.system.committees.set(state.committees);
        view.system.ownership.set(state.ownership);
        view.system.balance.set(state.balance);
        view
    }
}

enum UserAction<'a> {
    Operation(&'a OperationContext, &'a [u8]),
    Effect(&'a EffectContext, &'a [u8]),
}

impl<C> ExecutionStateView<C>
where
    C: ExecutionStateViewContext<Extra = ChainId>,
    Error: From<C::Error>,
{
    async fn run_user_action(
        &mut self,
        application_id: ApplicationId,
        chain_id: ChainId,
        action: UserAction<'_>,
    ) -> Result<Vec<ApplicationResult>, Error> {
        let application = crate::get_user_application(application_id)?;
        let mut results = Vec::new();
        let storage_context = StorageContext::new(chain_id, application_id, self, &mut results);
        let result = match action {
            UserAction::Operation(context, operation) => {
                application
                    .apply_operation(context, &storage_context, operation)
                    .await?
            }
            UserAction::Effect(context, effect) => {
                application
                    .apply_effect(context, &storage_context, effect)
                    .await?
            }
        };
        results.push(ApplicationResult::User(application_id, result));
        Ok(results)
    }

    pub async fn apply_operation(
        &mut self,
        application_id: ApplicationId,
        context: &OperationContext,
        operation: &Operation,
    ) -> Result<Vec<ApplicationResult>, Error> {
        if application_id == SYSTEM {
            match operation {
                Operation::System(op) => {
                    let result = self.system.apply_operation(context, op).await?;
                    Ok(vec![ApplicationResult::System(result)])
                }
                _ => Err(Error::InvalidOperation),
            }
        } else {
            match operation {
                Operation::System(_) => Err(Error::InvalidOperation),
                Operation::User(operation) => {
                    self.run_user_action(
                        application_id,
                        context.chain_id,
                        UserAction::Operation(context, operation),
                    )
                    .await
                }
            }
        }
    }

    pub async fn apply_effect(
        &mut self,
        application_id: ApplicationId,
        context: &EffectContext,
        effect: &Effect,
    ) -> Result<Vec<ApplicationResult>, Error> {
        if application_id == SYSTEM {
            match effect {
                Effect::System(effect) => {
                    let result = self.system.apply_effect(context, effect)?;
                    Ok(vec![ApplicationResult::System(result)])
                }
                _ => Err(Error::InvalidEffect),
            }
        } else {
            match effect {
                Effect::System(_) => Err(Error::InvalidEffect),
                Effect::User(effect) => {
                    self.run_user_action(
                        application_id,
                        context.chain_id,
                        UserAction::Effect(context, effect),
                    )
                    .await
                }
            }
        }
    }
}

type ActiveUserStates<C> = BTreeMap<ApplicationId, OwnedMutexGuard<RegisterView<C, Vec<u8>>>>;

#[derive(Debug, Clone)]
pub struct StorageContext<'a, C, const WRITABLE: bool> {
    chain_id: ChainId,
    application_id: ApplicationId,
    execution_state: Arc<Mutex<&'a mut ExecutionStateView<C>>>,
    active_user_states: Arc<Mutex<ActiveUserStates<C>>>,
    results: Arc<Mutex<&'a mut Vec<ApplicationResult>>>,
}

impl<'a, C, const W: bool> StorageContext<'a, C, W> {
    pub fn new(
        chain_id: ChainId,
        application_id: ApplicationId,
        execution_state: &'a mut ExecutionStateView<C>,
        results: &'a mut Vec<ApplicationResult>,
    ) -> Self {
        Self {
            chain_id,
            application_id,
            execution_state: Arc::new(Mutex::new(execution_state)),
            active_user_states: Arc::default(),
            results: Arc::new(Mutex::new(results)),
        }
    }
}

#[async_trait]
impl<'a, C, const W: bool> crate::StorageContext for StorageContext<'a, C, W>
where
    C: ExecutionStateViewContext<Extra = ChainId>,
    Error: From<C::Error>,
{
    async fn try_read_system_balance(&self) -> Result<crate::system::Balance, Error> {
        let value = *self
            .execution_state
            .try_lock()
            .map_err(C::Error::from)?
            .system
            .balance
            .get();
        Ok(value)
    }

    async fn try_read_my_state(&self) -> Result<Vec<u8>, Error> {
        let state = self
            .execution_state
            .try_lock()
            .map_err(C::Error::from)?
            .users
            .try_load_entry(self.application_id)
            .await?
            .get()
            .to_vec();
        Ok(state)
    }
}

#[async_trait]
impl<'a, C> QueryableStorageContext for StorageContext<'a, C, false>
where
    C: ExecutionStateViewContext<Extra = ChainId>,
    Error: From<C::Error>,
{
    /// Note that queries are not available from writable contexts.
    async fn try_query_application(
        &self,
        callee_id: ApplicationId,
        name: &str,
        argument: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let application = crate::get_user_application(callee_id)?;
        let query_context = crate::QueryContext {
            chain_id: self.chain_id,
        };
        let value = application
            .query(&query_context, self, name, argument)
            .await?;
        Ok(value)
    }
}

#[async_trait]
impl<'a, C> CallableStorageContext for StorageContext<'a, C, true>
where
    C: ExecutionStateViewContext<Extra = ChainId>,
    Error: From<C::Error>,
{
    async fn try_load_my_state(&self) -> Result<Vec<u8>, Error> {
        let view = self
            .execution_state
            .try_lock()
            .map_err(C::Error::from)?
            .users
            .try_load_entry(self.application_id)
            .await?;
        let state = view.get().to_vec();
        // Remember the view. This will prevent reentrancy.
        self.active_user_states
            .try_lock()
            .map_err(C::Error::from)?
            .insert(self.application_id, view);
        Ok(state)
    }

    async fn try_save_my_state(&self, state: Vec<u8>) -> Result<(), Error> {
        if let Some(mut view) = self
            .active_user_states
            .try_lock()
            .map_err(C::Error::from)?
            .remove(&self.application_id)
        {
            view.set(state);
        }
        Ok(())
    }

    async fn try_call_application(
        &self,
        authenticated: bool,
        callee_id: ApplicationId,
        name: &str,
        argument: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let authenticated_caller_id = authenticated.then_some(self.application_id);
        let application = crate::get_user_application(callee_id)?;
        let callee_context = crate::CalleeContext {
            chain_id: self.chain_id,
            authenticated_caller_id,
        };
        let (value, result) = application
            .call(&callee_context, self, name, argument)
            .await?;
        self.results
            .try_lock()
            .expect("Execution should be single-threaded")
            .push(ApplicationResult::User(callee_id, result));
        Ok(value)
    }
}

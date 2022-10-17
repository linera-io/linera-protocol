// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    runtime::{ExecutionRuntime, SessionManager},
    system::{SystemExecutionStateView, SystemExecutionStateViewContext, SYSTEM},
    ApplicationResult, Effect, EffectContext, ExecutionRuntimeContext, Operation, OperationContext,
};
use linera_base::{
    ensure,
    error::Error,
    messages::{ApplicationId, ChainId},
};
use linera_views::{
    impl_view,
    views::{
        CollectionOperations, ReentrantCollectionView, RegisterOperations, RegisterView,
        ScopedView, View,
    },
};

#[cfg(any(test, feature = "test"))]
use {
    crate::system::SystemExecutionState, linera_views::memory::MemoryContext,
    std::collections::BTreeMap, std::sync::Arc, tokio::sync::Mutex,
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
impl<R> ExecutionStateView<MemoryContext<R>>
where
    R: ExecutionRuntimeContext,
    MemoryContext<R>: ExecutionStateViewContext,
{
    /// Create an in-memory view where the system state is set. This is used notably to
    /// generate state hashes in tests.
    pub async fn from_system_state(state: SystemExecutionState) -> Self {
        let guard = Arc::new(Mutex::new(BTreeMap::new())).lock_owned().await;
        let extra = ExecutionRuntimeContext::new(
            state
                .description
                .expect("Chain description should be set")
                .into(),
        );
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
    C: ExecutionStateViewContext,
    C::Extra: ExecutionRuntimeContext,
    Error: From<C::Error>,
{
    async fn run_user_action(
        &mut self,
        application_id: ApplicationId,
        chain_id: ChainId,
        action: UserAction<'_>,
    ) -> Result<Vec<ApplicationResult>, Error> {
        let application = self
            .context()
            .extra()
            .get_user_application(application_id)?;
        let mut session_manager = SessionManager::default();
        let mut results = Vec::new();
        let runtime = ExecutionRuntime::new(
            chain_id,
            application_id,
            self,
            &mut session_manager,
            &mut results,
        );
        let result = match action {
            UserAction::Operation(context, operation) => {
                application
                    .apply_operation(context, &runtime, operation)
                    .await?
            }
            UserAction::Effect(context, effect) => {
                application.apply_effect(context, &runtime, effect).await?
            }
        };
        results.push(ApplicationResult::User(application_id, result));
        ensure!(
            session_manager.states.is_empty(),
            Error::SessionWasNotClosed
        );
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

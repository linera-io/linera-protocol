// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::system::SystemExecutionState;
use linera_base::{
    error::Error,
    execution::{ApplicationResult, EffectContext, OperationContext, SYSTEM},
    messages::*,
};
use linera_views::{
    impl_view,
    views::{CollectionOperations, CollectionView, RegisterOperations, RegisterView, ScopedView},
};
#[cfg(any(test, feature = "test"))]
use {
    linera_views::memory::MemoryContext, linera_views::views::View, std::collections::BTreeMap,
    std::sync::Arc, tokio::sync::Mutex,
};

/// A view accessing the execution state of a chain.
#[derive(Debug)]
pub struct ExecutionStateView<C> {
    /// System application.
    pub system: ScopedView<0, RegisterView<C, SystemExecutionState>>,
    /// User applications.
    pub users: ScopedView<1, CollectionView<C, ApplicationId, RegisterView<C, Vec<u8>>>>,
}

impl_view!(
    ExecutionStateView {
        system,
        users,
    };
    RegisterOperations<SystemExecutionState>,
    RegisterOperations<Vec<u8>>,
    CollectionOperations<ApplicationId>,
);

#[cfg(any(test, feature = "test"))]
impl ExecutionStateView<MemoryContext<ChainId>> {
    /// Create an in-memory view where the system state is set. This is used notably to
    /// generate state hashes in tests.
    pub async fn from_system_state(system_state: SystemExecutionState) -> Self {
        let guard = Arc::new(Mutex::new(BTreeMap::new())).lock_owned().await;
        let extra = system_state
            .description
            .expect("Chain description should be set")
            .into();
        let context = MemoryContext::new(guard, extra);
        let mut view = Self::load(context)
            .await
            .expect("Loading from memory should work");
        view.system.set(system_state);
        view
    }
}

impl<C> ExecutionStateView<C>
where
    C: ExecutionStateViewContext<Extra = ChainId>,
    Error: From<C::Error>,
{
    pub async fn apply_operation(
        &mut self,
        application_id: ApplicationId,
        context: &OperationContext,
        operation: &Operation,
    ) -> Result<ApplicationResult, Error> {
        if application_id == SYSTEM {
            match operation {
                Operation::System(op) => {
                    let result = self.system.get_mut().apply_operation(context, op)?;
                    Ok(ApplicationResult::System(result))
                }
                _ => Err(Error::InvalidOperation),
            }
        } else {
            let application = linera_base::execution::get_user_application(application_id)?;
            let state = self.users.load_entry(application_id).await?;
            match operation {
                Operation::System(_) => Err(Error::InvalidOperation),
                Operation::User(operation) => {
                    let result =
                        application.apply_operation(context, state.get_mut(), operation)?;
                    Ok(ApplicationResult::User(result))
                }
            }
        }
    }

    pub async fn apply_effect(
        &mut self,
        application_id: ApplicationId,
        context: &EffectContext,
        effect: &Effect,
    ) -> Result<ApplicationResult, Error> {
        if application_id == SYSTEM {
            match effect {
                Effect::System(effect) => {
                    let result = self.system.get_mut().apply_effect(context, effect)?;
                    Ok(ApplicationResult::System(result))
                }
                _ => Err(Error::InvalidEffect),
            }
        } else {
            let application = linera_base::execution::get_user_application(application_id)?;
            let state = self.users.load_entry(application_id).await?;
            match effect {
                Effect::System(_) => Err(Error::InvalidEffect),
                Effect::User(effect) => {
                    let result = application.apply_effect(context, state.get_mut(), effect)?;
                    Ok(ApplicationResult::User(result))
                }
            }
        }
    }
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    system::{SystemExecutionStateView, SystemExecutionStateViewContext},
    ApplicationResult, EffectContext, OperationContext, StorageContext,
};
use linera_base::{error::Error, messages::*, system::SYSTEM};
use linera_views::{
    impl_view,
    views::{CollectionOperations, CollectionView, RegisterOperations, RegisterView, ScopedView},
};
use std::collections::HashMap;
use tokio::sync::RwLock;

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
    pub users: ScopedView<1, CollectionView<C, ApplicationId, RegisterView<C, Vec<u8>>>>,
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
        view.system.manager.set(state.manager);
        view.system.balance.set(state.balance);
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
            let application = crate::get_user_application(application_id)?;
            let state = self.users.load_entry(application_id).await?;
            // TODO: use a proper shared collection.
            let mut map = [(application_id, Arc::new(RwLock::new(state.get().clone())))]
                .into_iter()
                .collect::<HashMap<_, _>>();
            let results = Arc::default();
            let storage_context =
                StorageContext::new(context.chain_id, application_id, &map, Arc::clone(&results));
            match operation {
                Operation::System(_) => Err(Error::InvalidOperation),
                Operation::User(operation) => {
                    let result = application
                        .apply_operation(context, storage_context, operation)
                        .await?;
                    state.set(
                        Arc::try_unwrap(map.remove(&application_id).unwrap())
                            .unwrap()
                            .into_inner(),
                    );
                    let mut results = Arc::try_unwrap(results).unwrap().into_inner().unwrap();
                    results.push(ApplicationResult::User(application_id, result));
                    Ok(results)
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
            let application = crate::get_user_application(application_id)?;
            let state = self.users.load_entry(application_id).await?;
            // TODO: use a proper shared collection.
            let mut map = [(application_id, Arc::new(RwLock::new(state.get().clone())))]
                .into_iter()
                .collect::<HashMap<_, _>>();
            let results = Arc::default();
            let storage_context =
                StorageContext::new(context.chain_id, application_id, &map, Arc::clone(&results));
            match effect {
                Effect::System(_) => Err(Error::InvalidEffect),
                Effect::User(effect) => {
                    let result = application
                        .apply_effect(context, storage_context, effect)
                        .await?;
                    state.set(
                        Arc::try_unwrap(map.remove(&application_id).unwrap())
                            .unwrap()
                            .into_inner(),
                    );
                    let mut results = Arc::try_unwrap(results).unwrap().into_inner().unwrap();
                    results.push(ApplicationResult::User(application_id, result));
                    Ok(results)
                }
            }
        }
    }
}

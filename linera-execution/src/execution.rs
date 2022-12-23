// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    runtime::{ExecutionRuntime, SessionManager},
    system::SystemExecutionStateView,
    ApplicationDescription, ApplicationId, ApplicationRegistryView, Effect, EffectContext,
    ExecutionError, ExecutionResult, ExecutionRuntimeContext, Operation, OperationContext, Query,
    QueryContext, Response,
};
use linera_base::{ensure, messages::ChainId};
use linera_views::{
    collection_view::ReentrantCollectionView,
    impl_view,
    register_view::RegisterView,
    scoped_view::ScopedView,
    views::{View, ViewError},
};

#[cfg(any(test, feature = "test"))]
use {
    crate::{system::SystemExecutionState, TestExecutionRuntimeContext},
    linera_views::{common::Context, memory::MemoryContext},
    std::collections::BTreeMap,
    std::sync::Arc,
    tokio::sync::Mutex,
};

/// A view accessing the execution state of a chain.
#[derive(Debug)]
pub struct ExecutionStateView<C> {
    /// System application.
    pub system: ScopedView<0, SystemExecutionStateView<C>>,
    /// User applications.
    pub users: ScopedView<1, ReentrantCollectionView<C, ApplicationId, RegisterView<C, Vec<u8>>>>,
}

impl_view!(ExecutionStateView { system, users });

#[cfg(any(test, feature = "test"))]
impl ExecutionStateView<MemoryContext<TestExecutionRuntimeContext>>
where
    MemoryContext<TestExecutionRuntimeContext>: Context + Clone + Send + Sync + 'static,
    ViewError:
        From<<MemoryContext<TestExecutionRuntimeContext> as linera_views::common::Context>::Error>,
{
    /// Create an in-memory view where the system state is set. This is used notably to
    /// generate state hashes in tests.
    pub async fn from_system_state(state: SystemExecutionState) -> Self {
        let guard = Arc::new(Mutex::new(BTreeMap::new())).lock_owned().await;
        let extra = TestExecutionRuntimeContext::new(
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
            view.system
                .subscriptions
                .insert(&channel_id, ())
                .expect("serialization of channel_id should not fail");
        }
        view.system.committees.set(state.committees);
        view.system.ownership.set(state.ownership);
        view.system.balance.set(state.balance);
        view
    }
}

enum UserAction<'a> {
    Initialize(&'a OperationContext, &'a [u8]),
    Operation(&'a OperationContext, &'a [u8]),
    Effect(&'a EffectContext, &'a [u8]),
}

impl<C> ExecutionStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
    C::Extra: ExecutionRuntimeContext,
{
    async fn run_user_action(
        &mut self,
        application: &ApplicationDescription,
        chain_id: ChainId,
        action: UserAction<'_>,
        applications: &mut ApplicationRegistryView<C>,
    ) -> Result<Vec<ExecutionResult>, ExecutionError> {
        // Load the application.
        let application_id = ApplicationId::from(application);
        let application = self
            .context()
            .extra()
            .get_user_application(application)
            .await?;
        // Create the execution runtime for this transaction.
        let mut session_manager = SessionManager::default();
        let mut results = Vec::new();
        let mut application_ids = vec![application_id];
        let runtime = ExecutionRuntime::new(
            chain_id,
            applications,
            &mut application_ids,
            self,
            &mut session_manager,
            &mut results,
        );
        // Make the call to user code.
        let result = match action {
            UserAction::Initialize(context, argument) => {
                application.initialize(context, &runtime, argument).await?
            }
            UserAction::Operation(context, operation) => {
                application
                    .execute_operation(context, &runtime, operation)
                    .await?
            }
            UserAction::Effect(context, effect) => {
                application
                    .execute_effect(context, &runtime, effect)
                    .await?
            }
        };
        assert_eq!(application_ids, vec![application_id]);
        // Update externally-visible results.
        results.push(ExecutionResult::User(application_id, result));
        // Check that all sessions were properly closed.
        ensure!(
            session_manager.states.is_empty(),
            ExecutionError::SessionWasNotClosed
        );
        Ok(results)
    }

    pub async fn execute_operation(
        &mut self,
        application: &ApplicationDescription,
        context: &OperationContext,
        operation: &Operation,
        applications: &mut ApplicationRegistryView<C>,
    ) -> Result<Vec<ExecutionResult>, ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        if let ApplicationDescription::System = application {
            match operation {
                Operation::System(op) => {
                    let result = self.system.execute_operation(context, op).await?;
                    let mut results = vec![result];
                    results.extend(
                        self.try_initialize_new_application(context, &results[0], applications)
                            .await?,
                    );
                    Ok(results)
                }
                _ => Err(ExecutionError::InvalidOperation),
            }
        } else {
            match operation {
                Operation::System(_) => Err(ExecutionError::InvalidOperation),
                Operation::User(operation) => {
                    self.run_user_action(
                        application,
                        context.chain_id,
                        UserAction::Operation(context, operation),
                        applications,
                    )
                    .await
                }
            }
        }
    }

    /// Call a newly created application's [`initialize`][crate::UserApplication::initialize]
    /// method, so that it can initialize its state using the initialization argument.
    async fn try_initialize_new_application(
        &mut self,
        context: &OperationContext,
        result: &ExecutionResult,
        applications: &mut ApplicationRegistryView<C>,
    ) -> Result<Vec<ExecutionResult>, ExecutionError> {
        if let ExecutionResult::System {
            new_application: Some(new_application),
            ..
        } = result
        {
            let user_action =
                UserAction::Initialize(context, &new_application.initialization_argument);
            let application = applications
                .register_new_application(new_application.clone())
                .await?;
            let results = self
                .run_user_action(&application, context.chain_id, user_action, applications)
                .await?;
            Ok(results)
        } else {
            Ok(vec![])
        }
    }

    pub async fn execute_effect(
        &mut self,
        application: &ApplicationDescription,
        context: &EffectContext,
        effect: &Effect,
        applications: &mut ApplicationRegistryView<C>,
    ) -> Result<Vec<ExecutionResult>, ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        if let ApplicationDescription::System = application {
            match effect {
                Effect::System(effect) => {
                    let result = self.system.execute_effect(context, effect)?;
                    Ok(vec![ExecutionResult::System {
                        result,
                        new_application: None,
                    }])
                }
                _ => Err(ExecutionError::InvalidEffect),
            }
        } else {
            match effect {
                Effect::System(_) => Err(ExecutionError::InvalidEffect),
                Effect::User(effect) => {
                    self.run_user_action(
                        application,
                        context.chain_id,
                        UserAction::Effect(context, effect),
                        applications,
                    )
                    .await
                }
            }
        }
    }

    pub async fn query_application(
        &mut self,
        application: &ApplicationDescription,
        context: &QueryContext,
        query: &Query,
        applications: &mut ApplicationRegistryView<C>,
    ) -> Result<Response, ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        if let ApplicationDescription::System = application {
            match query {
                Query::System(query) => {
                    let response = self.system.query_application(context, query).await?;
                    Ok(Response::System(response))
                }
                _ => Err(ExecutionError::InvalidQuery),
            }
        } else {
            match query {
                Query::System(_) => Err(ExecutionError::InvalidQuery),
                Query::User(query) => {
                    // Load the application.
                    let application_id = ApplicationId::from(application);
                    let application = self
                        .context()
                        .extra()
                        .get_user_application(application)
                        .await?;
                    // Create the execution runtime for this transaction.
                    let mut session_manager = SessionManager::default();
                    let mut results = Vec::new();
                    let mut application_ids = vec![application_id];
                    let runtime = ExecutionRuntime::new(
                        context.chain_id,
                        applications,
                        &mut application_ids,
                        self,
                        &mut session_manager,
                        &mut results,
                    );
                    // Run the query.
                    let response = application
                        .query_application(context, &runtime, query)
                        .await?;
                    assert_eq!(application_ids, vec![application_id]);
                    Ok(Response::User(response))
                }
            }
        }
    }
}

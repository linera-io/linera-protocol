// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    runtime::{ExecutionRuntime, SessionManager},
    system::SystemExecutionStateView,
    ApplicationDescription, ApplicationRegistryView, Effect, EffectContext, ExecutionError,
    ExecutionResult, ExecutionRuntimeContext, NewApplication, Operation, OperationContext, Query,
    QueryContext, RawExecutionResult, Response, SystemEffect, UserApplicationDescription,
    UserApplicationId,
};
use linera_base::{data_types::ChainId, ensure};
use linera_views::{
    collection_view::ReentrantCollectionView,
    register_view::RegisterView,
    views::{View, ViewError},
};
use linera_views_macro::HashableContainerView;

#[cfg(any(test, feature = "test"))]
use {
    crate::{system::SystemExecutionState, TestExecutionRuntimeContext},
    linera_views::{common::Context, memory::MemoryContext},
    std::collections::BTreeMap,
    std::sync::Arc,
    tokio::sync::Mutex,
};

/// A view accessing the execution state of a chain.
#[derive(Debug, HashableContainerView)]
pub struct ExecutionStateView<C> {
    /// System application.
    pub system: SystemExecutionStateView<C>,
    /// User applications.
    pub users: ReentrantCollectionView<C, UserApplicationId, RegisterView<C, Vec<u8>>>,
}

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
                .insert(&channel_id)
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
        application_description: &UserApplicationDescription,
        chain_id: ChainId,
        action: UserAction<'_>,
        applications: &mut ApplicationRegistryView<C>,
    ) -> Result<Vec<ExecutionResult>, ExecutionError> {
        // Try to load the application. This may fail if the corresponding
        // bytecode-publishing certificate doesn't exist yet on this validator.
        let application_id = UserApplicationId::from(application_description);
        let application = self
            .context()
            .extra()
            .get_user_application(application_description)
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
        // Make sure to declare the application first for all recipients of the user
        // execution result.
        let mut system_result = RawExecutionResult::default();
        for effect in &result.effects {
            system_result.effects.push((
                effect.0.clone(),
                SystemEffect::DeclareApplication {
                    application: application_description.clone(),
                },
            ));
        }
        if !system_result.effects.is_empty() {
            results.push(ExecutionResult::System(system_result));
        }
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
        match (application, operation) {
            (ApplicationDescription::System, Operation::System(op)) => {
                let (result, new_application) = self.system.execute_operation(context, op).await?;
                let mut results = vec![ExecutionResult::System(result)];
                if let Some(new_application) = new_application {
                    results.extend(
                        self.initialize_new_application(context, new_application, applications)
                            .await?,
                    );
                }
                Ok(results)
            }
            (ApplicationDescription::User(application), Operation::User(operation)) => {
                self.run_user_action(
                    application,
                    context.chain_id,
                    UserAction::Operation(context, operation),
                    applications,
                )
                .await
            }
            _ => Err(ExecutionError::InvalidOperation),
        }
    }

    /// Call a newly created application's [`initialize`][crate::UserApplication::initialize]
    /// method, so that it can initialize its state using the initialization argument.
    async fn initialize_new_application(
        &mut self,
        context: &OperationContext,
        new_application: NewApplication,
        applications: &mut ApplicationRegistryView<C>,
    ) -> Result<Vec<ExecutionResult>, ExecutionError> {
        let application = applications
            .register_new_application(new_application)
            .await?;
        let user_action = UserAction::Initialize(context, &application.initialization_argument);
        let results = self
            .run_user_action(&application, context.chain_id, user_action, applications)
            .await?;
        Ok(results)
    }

    pub async fn execute_effect(
        &mut self,
        application: &ApplicationDescription,
        context: &EffectContext,
        effect: &Effect,
        applications: &mut ApplicationRegistryView<C>,
    ) -> Result<Vec<ExecutionResult>, ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        match (application, effect) {
            (ApplicationDescription::System, Effect::System(effect)) => {
                let result = self.system.execute_effect(applications, context, effect)?;
                Ok(vec![ExecutionResult::System(result)])
            }
            (ApplicationDescription::User(application), Effect::User(effect)) => {
                self.run_user_action(
                    application,
                    context.chain_id,
                    UserAction::Effect(context, effect),
                    applications,
                )
                .await
            }
            _ => Err(ExecutionError::InvalidEffect),
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
        match (application, query) {
            (ApplicationDescription::System, Query::System(query)) => {
                let response = self.system.query_application(context, query).await?;
                Ok(Response::System(response))
            }
            (ApplicationDescription::User(application), Query::User(query)) => {
                // Load the application.
                let application_id = UserApplicationId::from(application);
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
            _ => Err(ExecutionError::InvalidQuery),
        }
    }
}

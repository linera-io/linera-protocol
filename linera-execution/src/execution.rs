// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    runtime::{ApplicationStatus, ExecutionRuntime, SessionManager},
    system::SystemExecutionStateView,
    ApplicationId, Effect, EffectContext, ExecutionError, ExecutionResult, ExecutionRuntimeContext,
    Operation, OperationContext, Query, QueryContext, RawExecutionResult, Response, SystemEffect,
    UserApplicationId,
};
use linera_base::{
    data_types::{ChainId, Owner},
    ensure,
};
use linera_views::{
    key_value_store_view::KeyValueStoreView,
    reentrant_collection_view::ReentrantCollectionView,
    register_view::RegisterView,
    views::{View, ViewError},
};
use linera_views_derive::CryptoHashView;

#[cfg(any(test, feature = "test"))]
use {
    crate::{
        system::SystemExecutionState, TestExecutionRuntimeContext, UserApplicationCode,
        UserApplicationDescription,
    },
    async_lock::Mutex,
    linera_views::{common::Context, memory::MemoryContext},
    std::collections::BTreeMap,
    std::sync::Arc,
};

/// A view accessing the execution state of a chain.
#[derive(Debug, CryptoHashView)]
pub struct ExecutionStateView<C> {
    /// System application.
    pub system: SystemExecutionStateView<C>,
    /// User applications (Simple based).
    pub simple_users: ReentrantCollectionView<C, UserApplicationId, RegisterView<C, Vec<u8>>>,
    /// User applications (View based).
    pub view_users: ReentrantCollectionView<C, UserApplicationId, KeyValueStoreView<C>>,
    /// Fuel available for running applications.
    pub available_fuel: RegisterView<C, u64>,
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
        // Destructure, to make sure we don't miss any fields.
        let SystemExecutionState {
            description,
            epoch,
            admin_id,
            subscriptions,
            committees,
            ownership,
            balance,
            balances,
            timestamp,
            registry,
        } = state;
        let guard = Arc::new(Mutex::new(BTreeMap::new())).lock_arc().await;
        let extra = TestExecutionRuntimeContext::new(
            description.expect("Chain description should be set").into(),
        );
        let context = MemoryContext::new(guard, extra);
        let mut view = Self::load(context)
            .await
            .expect("Loading from memory should work");
        view.system.description.set(description);
        view.system.epoch.set(epoch);
        view.system.admin_id.set(admin_id);
        for channel_id in subscriptions {
            view.system
                .subscriptions
                .insert(&channel_id)
                .expect("serialization of channel_id should not fail");
        }
        view.system.committees.set(committees);
        view.system.ownership.set(ownership);
        view.system.balance.set(balance);
        for (owner, balance) in balances {
            view.system
                .balances
                .insert(&owner, balance)
                .expect("insertion of balances should not fail");
        }
        view.system.timestamp.set(timestamp);
        view.system
            .registry
            .import(registry)
            .expect("serialization of registry components should not fail");
        view
    }

    /// Sets the amount of `available_fuel` using the builder pattern.
    pub fn with_fuel(mut self, fuel: u64) -> Self {
        self.available_fuel.set(fuel);
        self
    }

    /// Simulates the initialization of an application.
    pub async fn simulate_initialization(
        &mut self,
        application: UserApplicationCode,
        application_description: UserApplicationDescription,
        initialization_argument: Vec<u8>,
    ) -> Result<(), ExecutionError> {
        let chain_id = application_description.creation.chain_id;
        let context = OperationContext {
            chain_id,
            authenticated_signer: None,
            height: application_description.creation.height,
            index: application_description.creation.index,
        };

        let action = UserAction::Initialize(&context, initialization_argument);

        let application_id = self
            .system
            .registry
            .register_application(application_description)
            .await?;

        self.context()
            .extra()
            .user_applications()
            .insert(application_id, application);

        self.run_user_action(application_id, chain_id, action)
            .await?;

        Ok(())
    }
}

enum UserAction<'a> {
    Initialize(&'a OperationContext, Vec<u8>),
    Operation(&'a OperationContext, &'a [u8]),
    Effect(&'a EffectContext, &'a [u8]),
}

impl<'a> UserAction<'a> {
    fn signer(&self) -> Option<Owner> {
        use UserAction::*;
        match self {
            Initialize(context, _) => context.authenticated_signer,
            Operation(context, _) => context.authenticated_signer,
            Effect(context, _) => context.authenticated_signer,
        }
    }
}

impl<C> ExecutionStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
    C::Extra: ExecutionRuntimeContext,
{
    /// Adds `fuel` to the amount of `available_fuel` in the chain.
    pub fn add_fuel(&mut self, fuel: u64) {
        let updated_amount = self.available_fuel.get().saturating_add(fuel);
        self.available_fuel.set(updated_amount);
    }

    async fn run_user_action(
        &mut self,
        application_id: UserApplicationId,
        chain_id: ChainId,
        action: UserAction<'_>,
    ) -> Result<Vec<ExecutionResult>, ExecutionError> {
        // Try to load the application. This may fail if the corresponding
        // bytecode-publishing certificate doesn't exist yet on this validator.
        let description = self
            .system
            .registry
            .describe_application(application_id)
            .await?;
        let application = self
            .context()
            .extra()
            .get_user_application(&description)
            .await?;
        let signer = action.signer();
        // Create the execution runtime for this transaction.
        let mut session_manager = SessionManager::default();
        let mut results = Vec::new();
        let mut applications = vec![ApplicationStatus {
            id: application_id,
            parameters: description.parameters,
            signer,
        }];
        let runtime = ExecutionRuntime::new(
            chain_id,
            &mut applications,
            self,
            &mut session_manager,
            &mut results,
            10_000_000,
        );
        // Make the call to user code.
        let mut result = match action {
            UserAction::Initialize(context, argument) => {
                application.initialize(context, &runtime, &argument).await?
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
        // Set the authenticated signer to be used in outgoing effects.
        result.authenticated_signer = signer;
        // Check that applications were correctly stacked and unstacked.
        assert_eq!(applications.len(), 1);
        assert_eq!(applications[0].id, application_id);
        // Make sure to declare the application first for all recipients of the user
        // execution result.
        let mut system_result = RawExecutionResult::default();
        let applications = self
            .system
            .registry
            .describe_applications_with_dependencies(vec![application_id], &Default::default())
            .await?;
        for effect in &result.effects {
            system_result.effects.push((
                effect.0.clone(),
                false,
                SystemEffect::RegisterApplications {
                    applications: applications.clone(),
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
        application_id: ApplicationId,
        context: &OperationContext,
        operation: &Operation,
    ) -> Result<Vec<ExecutionResult>, ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        match (application_id, operation) {
            (ApplicationId::System, Operation::System(op)) => {
                let (mut result, new_application) =
                    self.system.execute_operation(context, op).await?;
                result.authenticated_signer = context.authenticated_signer;
                let mut results = vec![ExecutionResult::System(result)];
                if let Some((application_id, argument)) = new_application {
                    let user_action = UserAction::Initialize(context, argument);
                    results.extend(
                        self.run_user_action(application_id, context.chain_id, user_action)
                            .await?,
                    );
                }
                Ok(results)
            }
            (ApplicationId::User(application_id), Operation::User(operation)) => {
                self.run_user_action(
                    application_id,
                    context.chain_id,
                    UserAction::Operation(context, operation),
                )
                .await
            }
            _ => Err(ExecutionError::InvalidOperation),
        }
    }

    pub async fn execute_effect(
        &mut self,
        application_id: ApplicationId,
        context: &EffectContext,
        effect: &Effect,
    ) -> Result<Vec<ExecutionResult>, ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        match (application_id, effect) {
            (ApplicationId::System, Effect::System(effect)) => {
                let result = self.system.execute_effect(context, effect).await?;
                Ok(vec![ExecutionResult::System(result)])
            }
            (ApplicationId::User(application_id), Effect::User(effect)) => {
                self.run_user_action(
                    application_id,
                    context.chain_id,
                    UserAction::Effect(context, effect),
                )
                .await
            }
            _ => Err(ExecutionError::InvalidEffect),
        }
    }

    pub async fn query_application(
        &mut self,
        application_id: ApplicationId,
        context: &QueryContext,
        query: &Query,
    ) -> Result<Response, ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        match (application_id, query) {
            (ApplicationId::System, Query::System(query)) => {
                let response = self.system.query_application(context, query).await?;
                Ok(Response::System(response))
            }
            (ApplicationId::User(application_id), Query::User(query)) => {
                // Load the application.
                let description = self
                    .system
                    .registry
                    .describe_application(application_id)
                    .await?;
                let application = self
                    .context()
                    .extra()
                    .get_user_application(&description)
                    .await?;
                // Create the execution runtime for this transaction.
                let mut session_manager = SessionManager::default();
                let mut results = Vec::new();
                let mut applications = vec![ApplicationStatus {
                    id: application_id,
                    parameters: description.parameters,
                    signer: None,
                }];
                let runtime = ExecutionRuntime::new(
                    context.chain_id,
                    &mut applications,
                    self,
                    &mut session_manager,
                    &mut results,
                    0,
                );
                // Run the query.
                let response = application
                    .query_application(context, &runtime, query)
                    .await?;
                // Check that applications were correctly stacked and unstacked.
                assert_eq!(applications.len(), 1);
                assert_eq!(applications[0].id, application_id);
                Ok(Response::User(response))
            }
            _ => Err(ExecutionError::InvalidQuery),
        }
    }

    pub async fn list_applications(
        &self,
    ) -> Result<Vec<(UserApplicationId, UserApplicationDescription)>, ExecutionError> {
        let mut applications = vec![];
        for index in self.system.registry.known_applications.indices().await? {
            let application_description =
                self.system.registry.known_applications.get(&index).await?;

            if let Some(application_description) = application_description {
                applications.push((index, application_description));
            }
        }
        Ok(applications)
    }
}

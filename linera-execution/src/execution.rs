// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    policy::ResourceControlPolicy,
    resources::{ResourceTracker, RuntimeLimits},
    runtime::{ApplicationStatus, ExecutionRuntime, SessionManager},
    system::SystemExecutionStateView,
    ContractSyncRuntime, ExecutionError, ExecutionResult, ExecutionRuntimeConfig,
    ExecutionRuntimeContext, Message, MessageContext, Operation, OperationContext, Query,
    QueryContext, RawExecutionResult, RawOutgoingMessage, Response, ServiceSyncRuntime,
    SystemMessage, UserApplicationDescription, UserApplicationId,
};
use futures::StreamExt;
use linera_base::identifiers::{ChainId, Owner};
use linera_views::{
    common::Context,
    key_value_store_view::KeyValueStoreView,
    reentrant_collection_view::ReentrantCollectionView,
    register_view::RegisterView,
    views::{View, ViewError},
};
use linera_views_derive::CryptoHashView;

#[cfg(any(test, feature = "test"))]
use {
    crate::{system::SystemExecutionState, TestExecutionRuntimeContext, UserContractCode},
    async_lock::Mutex,
    linera_views::memory::{MemoryContext, TEST_MEMORY_MAX_STREAM_QUERIES},
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
}

#[cfg(any(test, feature = "test"))]
impl ExecutionStateView<MemoryContext<TestExecutionRuntimeContext>>
where
    MemoryContext<TestExecutionRuntimeContext>: Context + Clone + Send + Sync + 'static,
    ViewError:
        From<<MemoryContext<TestExecutionRuntimeContext> as linera_views::common::Context>::Error>,
{
    /// Creates an in-memory view where the system state is set. This is used notably to
    /// generate state hashes in tests.
    pub async fn from_system_state(
        state: SystemExecutionState,
        execution_runtime_config: ExecutionRuntimeConfig,
    ) -> Self {
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
            execution_runtime_config,
        );
        let context = MemoryContext::new(guard, TEST_MEMORY_MAX_STREAM_QUERIES, extra);
        let mut view = Self::load(context)
            .await
            .expect("Loading from memory should work");
        view.system.description.set(description);
        view.system.epoch.set(epoch);
        view.system.admin_id.set(admin_id);
        for subscription in subscriptions {
            view.system
                .subscriptions
                .insert(&subscription)
                .expect("serialization of subscription should not fail");
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

    /// Simulates the initialization of an application.
    #[cfg(any(test, feature = "test"))]
    pub async fn simulate_initialization(
        &mut self,
        contract: UserContractCode,
        application_description: UserApplicationDescription,
        initialization_argument: Vec<u8>,
    ) -> Result<(), ExecutionError> {
        let chain_id = application_description.creation.chain_id;
        let context = OperationContext {
            chain_id,
            authenticated_signer: None,
            height: application_description.creation.height,
            index: application_description.creation.index,
            next_message_index: 0,
        };

        let action = UserAction::Initialize(context, initialization_argument);

        let application_id = self
            .system
            .registry
            .register_application(application_description)
            .await?;

        self.context()
            .extra()
            .user_contracts()
            .insert(application_id, contract);

        let mut tracker = ResourceTracker::default();
        let policy = ResourceControlPolicy::default();
        self.run_user_action(application_id, chain_id, action, &policy, &mut tracker)
            .await?;

        Ok(())
    }
}

pub enum UserAction {
    Initialize(OperationContext, Vec<u8>),
    Operation(OperationContext, Vec<u8>),
    Message(MessageContext, Vec<u8>),
}

impl UserAction {
    pub(crate) fn signer(&self) -> Option<Owner> {
        use UserAction::*;
        match self {
            Initialize(context, _) => context.authenticated_signer,
            Operation(context, _) => context.authenticated_signer,
            Message(context, _) => context.authenticated_signer,
        }
    }
}

impl<C> ExecutionStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
    C::Extra: ExecutionRuntimeContext,
{
    async fn run_user_action(
        &mut self,
        application_id: UserApplicationId,
        chain_id: ChainId,
        action: UserAction,
        policy: &ResourceControlPolicy,
        tracker: &mut ResourceTracker,
    ) -> Result<Vec<ExecutionResult>, ExecutionError> {
        let execution_results = match self.context().extra().execution_runtime_config() {
            ExecutionRuntimeConfig::Actor => {
                self.run_user_action_with_actor_runtime(
                    application_id,
                    chain_id,
                    action,
                    policy,
                    tracker,
                )
                .await?
            }
            ExecutionRuntimeConfig::Synchronous => {
                self.run_user_action_with_synchronous_runtime(
                    application_id,
                    chain_id,
                    action,
                    policy,
                    tracker,
                )
                .await?
            }
        };
        self.update_execution_results_with_app_registrations(execution_results)
            .await
    }

    async fn run_user_action_with_actor_runtime(
        &mut self,
        application_id: UserApplicationId,
        chain_id: ChainId,
        action: UserAction,
        policy: &ResourceControlPolicy,
        tracker: &mut ResourceTracker,
    ) -> Result<Vec<ExecutionResult>, ExecutionError> {
        let balance = self.system.balance.get();
        let runtime_limits = tracker.limits(policy, balance);
        let initial_remaining_fuel = policy.remaining_fuel(*balance);
        // Try to load the contract. This may fail if the corresponding
        // bytecode-publishing certificate doesn't exist yet on this validator.
        let description = self
            .system
            .registry
            .describe_application(application_id)
            .await?;
        let contract = self
            .context()
            .extra()
            .get_user_contract(&description)
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
            initial_remaining_fuel,
            runtime_limits,
        );
        // Make the call to user code.
        let (runtime_actor, runtime_sender) = runtime.contract_runtime_actor();
        let mut contract = contract.instantiate_with_actor_runtime(runtime_sender)?;
        let execution_result_future = tokio::task::spawn_blocking(move || match action {
            UserAction::Initialize(context, argument) => contract.initialize(context, argument),
            UserAction::Operation(context, operation) => {
                contract.execute_operation(context, operation)
            }
            UserAction::Message(context, message) => contract.execute_message(context, message),
        });
        runtime_actor.run().await?;
        let mut execution_result = execution_result_future.await??;

        // Set the authenticated signer to be used in outgoing messages.
        execution_result.authenticated_signer = signer;
        let runtime_counts = runtime.runtime_counts().await;
        let balance = self.system.balance.get_mut();
        tracker.update_limits(balance, policy, runtime_counts)?;

        // Check that applications were correctly stacked and unstacked.
        assert_eq!(applications.len(), 1);
        assert_eq!(applications[0].id, application_id);

        // Update externally-visible results.
        results.push(ExecutionResult::User(application_id, execution_result));
        // Check that all sessions were properly closed.
        if let Some(session_id) = session_manager.states.keys().next() {
            return Err(ExecutionError::SessionWasNotClosed(*session_id));
        }
        Ok(results)
    }

    async fn run_user_action_with_synchronous_runtime(
        &mut self,
        application_id: UserApplicationId,
        chain_id: ChainId,
        action: UserAction,
        policy: &ResourceControlPolicy,
        tracker: &mut ResourceTracker,
    ) -> Result<Vec<ExecutionResult>, ExecutionError> {
        let balance = self.system.balance.get();
        let runtime_limits = tracker.limits(policy, balance);
        let initial_remaining_fuel = policy.remaining_fuel(*balance);
        let (execution_state_sender, mut execution_state_receiver) =
            futures::channel::mpsc::unbounded();
        let execution_results_future = tokio::task::spawn_blocking(move || {
            ContractSyncRuntime::run_action(
                execution_state_sender,
                application_id,
                chain_id,
                runtime_limits,
                initial_remaining_fuel,
                action,
            )
        });
        while let Some(request) = execution_state_receiver.next().await {
            self.handle_request(request).await?;
        }
        let (execution_results, runtime_counts) = execution_results_future.await??;
        let balance = self.system.balance.get_mut();
        tracker.update_limits(balance, policy, runtime_counts)?;
        Ok(execution_results)
    }

    async fn update_execution_results_with_app_registrations(
        &self,
        mut results: Vec<ExecutionResult>,
    ) -> Result<Vec<ExecutionResult>, ExecutionError> {
        // TODO(#1417): It looks like we are forgetting about the recipients of messages
        // sent by earlier entries in `results` (i.e. application calls).
        let Some(ExecutionResult::User(application_id, result)) = results.last() else {
            return Ok(results);
        };
        // Make sure to declare applications before recipients execute messages produced
        // by the user execution results.
        let mut system_result = RawExecutionResult::default();
        let applications = self
            .system
            .registry
            .describe_applications_with_dependencies(vec![*application_id], &Default::default())
            .await?;
        for message in &result.messages {
            system_result.messages.push(RawOutgoingMessage {
                destination: message.destination.clone(),
                authenticated: false,
                is_protected: false,
                message: SystemMessage::RegisterApplications {
                    applications: applications.clone(),
                },
            });
        }
        if !system_result.messages.is_empty() {
            results.insert(0, ExecutionResult::System(system_result));
        }
        Ok(results)
    }

    pub async fn execute_operation(
        &mut self,
        context: OperationContext,
        operation: Operation,
        policy: &ResourceControlPolicy,
        tracker: &mut ResourceTracker,
    ) -> Result<Vec<ExecutionResult>, ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        match operation {
            Operation::System(op) => {
                let (mut result, new_application) =
                    self.system.execute_operation(context, op).await?;
                result.authenticated_signer = context.authenticated_signer;
                let mut results = vec![ExecutionResult::System(result)];
                if let Some((application_id, argument)) = new_application {
                    let user_action = UserAction::Initialize(context, argument);
                    results.extend(
                        self.run_user_action(
                            application_id,
                            context.chain_id,
                            user_action,
                            policy,
                            tracker,
                        )
                        .await?,
                    );
                }
                Ok(results)
            }
            Operation::User {
                application_id,
                bytes,
            } => {
                self.run_user_action(
                    application_id,
                    context.chain_id,
                    UserAction::Operation(context, bytes),
                    policy,
                    tracker,
                )
                .await
            }
        }
    }

    pub async fn execute_message(
        &mut self,
        context: MessageContext,
        message: Message,
        policy: &ResourceControlPolicy,
        tracker: &mut ResourceTracker,
    ) -> Result<Vec<ExecutionResult>, ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        match message {
            Message::System(message) => {
                let result = self.system.execute_message(context, message).await?;
                Ok(vec![ExecutionResult::System(result)])
            }
            Message::User {
                application_id,
                bytes,
            } => {
                self.run_user_action(
                    application_id,
                    context.chain_id,
                    UserAction::Message(context, bytes),
                    policy,
                    tracker,
                )
                .await
            }
        }
    }

    pub async fn query_application(
        &mut self,
        context: QueryContext,
        query: Query,
    ) -> Result<Response, ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        match query {
            Query::System(query) => {
                let response = self.system.handle_query(context, query).await?;
                Ok(Response::System(response))
            }
            Query::User {
                application_id,
                bytes,
            } => {
                let response = match self.context().extra().execution_runtime_config() {
                    ExecutionRuntimeConfig::Actor => {
                        self.query_application_with_actor_runtime(application_id, context, bytes)
                            .await?
                    }
                    ExecutionRuntimeConfig::Synchronous => {
                        self.query_application_with_sync_runtime(application_id, context, bytes)
                            .await?
                    }
                };
                Ok(Response::User(response))
            }
        }
    }

    async fn query_application_with_actor_runtime(
        &mut self,
        application_id: UserApplicationId,
        context: QueryContext,
        query: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        // Load the service.
        let description = self
            .system
            .registry
            .describe_application(application_id)
            .await?;
        let service = self
            .context()
            .extra()
            .get_user_service(&description)
            .await?;
        // Create the execution runtime for this transaction.
        let mut session_manager = SessionManager::default();
        let mut results = Vec::new();
        let mut applications = vec![ApplicationStatus {
            id: application_id,
            parameters: description.parameters,
            signer: None,
        }];
        let runtime_limits = RuntimeLimits::default();
        let remaining_fuel = 0;
        let runtime = ExecutionRuntime::new(
            context.chain_id,
            &mut applications,
            self,
            &mut session_manager,
            &mut results,
            remaining_fuel,
            runtime_limits,
        );
        let (runtime_actor, runtime_sender) = runtime.service_runtime_actor();
        let mut service = service.instantiate_with_actor_runtime(runtime_sender)?;
        // Run the query.
        let response_future =
            tokio::task::spawn_blocking(move || service.handle_query(context, query));
        runtime_actor.run().await?;
        let response = response_future.await??;

        // Check that applications were correctly stacked and unstacked.
        assert_eq!(applications.len(), 1);
        assert_eq!(applications[0].id, application_id);
        Ok(response)
    }

    async fn query_application_with_sync_runtime(
        &mut self,
        application_id: UserApplicationId,
        context: QueryContext,
        query: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        let (execution_state_sender, mut execution_state_receiver) =
            futures::channel::mpsc::unbounded();
        let query_result_future = tokio::task::spawn_blocking(move || {
            ServiceSyncRuntime::run_query(execution_state_sender, application_id, context, query)
        });
        while let Some(request) = execution_state_receiver.next().await {
            self.handle_request(request).await?;
        }
        query_result_future.await?
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

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    policy::ResourceControlPolicy,
    resources::{ResourceTracker, RuntimeLimits},
    runtime::{ApplicationStatus, ExecutionRuntime, SessionManager},
    system::SystemExecutionStateView,
    ContractRuntime, ExecutionError, ExecutionResult, ExecutionRuntimeContext, Message,
    MessageContext, Operation, OperationContext, Query, QueryContext, RawExecutionResult,
    RawOutgoingMessage, Response, SystemMessage, UserApplicationDescription, UserApplicationId,
};
use linera_base::{
    ensure,
    identifiers::{ChainId, Owner},
};
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

enum UserAction {
    Initialize(OperationContext, Vec<u8>),
    Operation(OperationContext, Vec<u8>),
    Message(MessageContext, Vec<u8>),
}

impl UserAction {
    fn signer(&self) -> Option<Owner> {
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
        let call_result_future = match action {
            UserAction::Initialize(context, argument) => {
                contract.initialize(context, runtime_sender, argument)
            }
            UserAction::Operation(context, operation) => {
                contract.execute_operation(context, runtime_sender, operation)
            }
            UserAction::Message(context, message) => {
                contract.execute_message(context, runtime_sender, message)
            }
        };
        let (runtime_result, call_result) =
            futures::future::join(runtime_actor.run(), call_result_future).await;
        runtime_result?;

        // TODO(#989): Make user errors fail blocks again.
        let mut result = if let Err(ExecutionError::UserError(message)) = &call_result {
            tracing::error!("Ignoring error reported by user application: {message}");
            RawExecutionResult::default()
        } else {
            call_result?
        };
        // Set the authenticated signer to be used in outgoing messages.
        result.authenticated_signer = signer;
        let runtime_counts = runtime.runtime_counts();
        let balance = self.system.balance.get_mut();
        tracker.update_limits(balance, policy, runtime_counts)?;

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
        for message in &result.messages {
            system_result.messages.push(RawOutgoingMessage {
                destination: message.destination.clone(),
                authenticated: false,
                is_skippable: true,
                message: SystemMessage::RegisterApplications {
                    applications: applications.clone(),
                },
            });
        }
        if !system_result.messages.is_empty() {
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
                // Run the query.
                let response_future = service.handle_query(context, runtime_sender, bytes);
                let (runtime_result, response) =
                    futures::future::join(runtime_actor.run(), response_future).await;
                runtime_result?;
                let response = response?;

                // Check that applications were correctly stacked and unstacked.
                assert_eq!(applications.len(), 1);
                assert_eq!(applications[0].id, application_id);
                Ok(Response::User(response))
            }
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

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    resources::ResourceController, system::SystemExecutionStateView, ContractSyncRuntime,
    ExecutionError, ExecutionOutcome, ExecutionRuntimeConfig, ExecutionRuntimeContext, Message,
    MessageContext, MessageKind, Operation, OperationContext, Query, QueryContext,
    RawExecutionOutcome, RawOutgoingMessage, Response, ServiceSyncRuntime, SystemMessage,
    UserApplicationDescription, UserApplicationId,
};
use futures::{stream::FuturesUnordered, StreamExt, TryStreamExt};
use linera_base::identifiers::{ChainId, Destination, Owner};
use linera_views::{
    common::Context,
    key_value_store_view::KeyValueStoreView,
    reentrant_collection_view::ReentrantCollectionView,
    views::{View, ViewError},
};
use linera_views_derive::CryptoHashView;
use std::collections::{BTreeSet, HashMap};

#[cfg(any(test, feature = "test"))]
use {
    crate::{
        policy::ResourceControlPolicy, system::SystemExecutionState, ResourceTracker,
        TestExecutionRuntimeContext, UserContractCode,
    },
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
    /// User applications.
    pub users: ReentrantCollectionView<C, UserApplicationId, KeyValueStoreView<C>>,
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
    ) -> Result<Vec<ExecutionOutcome>, ExecutionError> {
        let execution_outcomes = match self.context().extra().execution_runtime_config() {
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
        self.update_execution_outcomes_with_app_registrations(execution_outcomes)
            .await
    }

    async fn run_user_action_with_synchronous_runtime(
        &mut self,
        application_id: UserApplicationId,
        chain_id: ChainId,
        action: UserAction,
        policy: &ResourceControlPolicy,
        tracker: &mut ResourceTracker,
    ) -> Result<Vec<ExecutionOutcome>, ExecutionError> {
        let resource_controller = ResourceController {
            policy: Arc::new(policy.clone()),
            tracker: *tracker,
            balance: *self.system.balance.get(),
        };
        let (execution_state_sender, mut execution_state_receiver) =
            futures::channel::mpsc::unbounded();
        let execution_outcomes_future = tokio::task::spawn_blocking(move || {
            ContractSyncRuntime::run_action(
                execution_state_sender,
                application_id,
                chain_id,
                resource_controller,
                action,
            )
        });
        while let Some(request) = execution_state_receiver.next().await {
            self.handle_request(request).await?;
        }
        let (execution_outcomes, resource_controller) = execution_outcomes_future.await??;
        *self.system.balance.get_mut() = resource_controller.balance;
        *tracker = resource_controller.tracker;
        Ok(execution_outcomes)
    }

    /// Schedules application registration messages when needed.
    ///
    /// Ensures that the outgoing messages in `results` are preceded by a system message that
    /// registers the application that will handle the messages.
    async fn update_execution_outcomes_with_app_registrations(
        &self,
        mut results: Vec<ExecutionOutcome>,
    ) -> Result<Vec<ExecutionOutcome>, ExecutionError> {
        let user_application_outcomes = results.iter().filter_map(|outcome| match outcome {
            ExecutionOutcome::User(application_id, result) => Some((application_id, result)),
            _ => None,
        });

        let mut applications_to_register_per_destination = HashMap::<_, BTreeSet<_>>::new();

        for (application_id, result) in user_application_outcomes {
            for message in &result.messages {
                applications_to_register_per_destination
                    .entry(&message.destination)
                    .or_default()
                    .insert(*application_id);
            }
        }

        if applications_to_register_per_destination.is_empty() {
            return Ok(results);
        }

        let messages = applications_to_register_per_destination
            .into_iter()
            .map(|(destination, applications_to_describe)| async {
                let applications = self
                    .system
                    .registry
                    .describe_applications_with_dependencies(
                        applications_to_describe.into_iter().collect(),
                        &HashMap::new(),
                    )
                    .await?;

                Ok::<_, ExecutionError>(RawOutgoingMessage {
                    destination: destination.clone(),
                    authenticated: false,
                    kind: MessageKind::Simple,
                    message: SystemMessage::RegisterApplications { applications },
                })
            })
            .collect::<FuturesUnordered<_>>()
            .try_collect::<Vec<_>>()
            .await?;

        let system_outcome = RawExecutionOutcome {
            messages,
            ..RawExecutionOutcome::default()
        };

        results.insert(0, ExecutionOutcome::System(system_outcome));

        Ok(results)
    }

    pub async fn execute_operation(
        &mut self,
        context: OperationContext,
        operation: Operation,
        policy: &ResourceControlPolicy,
        tracker: &mut ResourceTracker,
    ) -> Result<Vec<ExecutionOutcome>, ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        match operation {
            Operation::System(op) => {
                let (mut result, new_application) =
                    self.system.execute_operation(context, op).await?;
                result.authenticated_signer = context.authenticated_signer;
                let mut outcomes = vec![ExecutionOutcome::System(result)];
                if let Some((application_id, argument)) = new_application {
                    let user_action = UserAction::Initialize(context, argument);
                    outcomes.extend(
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
                Ok(outcomes)
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
    ) -> Result<Vec<ExecutionOutcome>, ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        match message {
            Message::System(message) => {
                let outcome = self.system.execute_message(context, message).await?;
                Ok(vec![ExecutionOutcome::System(outcome)])
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

    pub async fn bounce_message(
        &self,
        context: MessageContext,
        message: Message,
    ) -> Result<Vec<ExecutionOutcome>, ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        match message {
            Message::System(message) => {
                let mut outcome = RawExecutionOutcome {
                    authenticated_signer: context.authenticated_signer,
                    ..Default::default()
                };
                outcome.messages.push(RawOutgoingMessage {
                    destination: Destination::Recipient(context.message_id.chain_id),
                    authenticated: true,
                    kind: MessageKind::Bouncing,
                    message,
                });
                Ok(vec![ExecutionOutcome::System(outcome)])
            }
            Message::User {
                application_id,
                bytes,
            } => {
                let mut outcome = RawExecutionOutcome {
                    authenticated_signer: context.authenticated_signer,
                    ..Default::default()
                };
                outcome.messages.push(RawOutgoingMessage {
                    destination: Destination::Recipient(context.message_id.chain_id),
                    authenticated: true,
                    kind: MessageKind::Bouncing,
                    message: bytes,
                });
                Ok(vec![ExecutionOutcome::User(application_id, outcome)])
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
                    ExecutionRuntimeConfig::Synchronous => {
                        self.query_application_with_sync_runtime(application_id, context, bytes)
                            .await?
                    }
                };
                Ok(Response::User(response))
            }
        }
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

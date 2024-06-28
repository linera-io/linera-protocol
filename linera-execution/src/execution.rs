// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashMap};

use futures::{stream::FuturesUnordered, StreamExt, TryStreamExt};
use linera_base::{
    data_types::{Amount, BlockHeight, OracleRecord, Timestamp},
    identifiers::{Account, ChainId, Destination, Owner},
};
use linera_views::{
    common::Context,
    key_value_store_view::KeyValueStoreView,
    reentrant_collection_view::HashedReentrantCollectionView,
    views::{ClonableView, View, ViewError},
};
use linera_views_derive::CryptoHashView;
#[cfg(with_testing)]
use {
    crate::{
        ResourceControlPolicy, ResourceTracker, TestExecutionRuntimeContext, UserContractCode,
    },
    linera_views::memory::MemoryContext,
    std::sync::Arc,
};

use crate::{
    resources::ResourceController, system::SystemExecutionStateView, ContractSyncRuntime,
    ExecutionError, ExecutionOutcome, ExecutionRuntimeConfig, ExecutionRuntimeContext, Message,
    MessageContext, MessageKind, Operation, OperationContext, Query, QueryContext,
    RawExecutionOutcome, RawOutgoingMessage, Response, ServiceSyncRuntime, SystemMessage,
    UserApplicationDescription, UserApplicationId,
};

/// A view accessing the execution state of a chain.
#[derive(Debug, ClonableView, CryptoHashView)]
pub struct ExecutionStateView<C> {
    /// System application.
    pub system: SystemExecutionStateView<C>,
    /// User applications.
    pub users: HashedReentrantCollectionView<C, UserApplicationId, KeyValueStoreView<C>>,
}

#[cfg(with_testing)]
impl ExecutionStateView<MemoryContext<TestExecutionRuntimeContext>>
where
    MemoryContext<TestExecutionRuntimeContext>: Context + Clone + Send + Sync + 'static,
    ViewError:
        From<<MemoryContext<TestExecutionRuntimeContext> as linera_views::common::Context>::Error>,
{
    /// Simulates the instantiation of an application.
    pub async fn simulate_instantiation(
        &mut self,
        contract: UserContractCode,
        local_time: Timestamp,
        application_description: UserApplicationDescription,
        instantiation_argument: Vec<u8>,
    ) -> Result<(), ExecutionError> {
        let chain_id = application_description.creation.chain_id;
        let context = OperationContext {
            chain_id,
            authenticated_signer: None,
            authenticated_caller_id: None,
            height: application_description.creation.height,
            index: Some(application_description.creation.index),
            next_message_index: 0,
        };

        let action = UserAction::Instantiate(context, instantiation_argument);

        let application_id = self
            .system
            .registry
            .register_application(application_description)
            .await?;

        self.context()
            .extra()
            .user_contracts()
            .insert(application_id, contract);

        let tracker = ResourceTracker::default();
        let policy = ResourceControlPolicy::default();
        let mut resource_controller = ResourceController {
            policy: Arc::new(policy),
            tracker,
            account: None,
        };
        self.run_user_action(
            application_id,
            chain_id,
            local_time,
            action,
            context.refund_grant_to(),
            None,
            None,
            &mut resource_controller,
        )
        .await?;

        Ok(())
    }
}

pub enum UserAction {
    Instantiate(OperationContext, Vec<u8>),
    Operation(OperationContext, Vec<u8>),
    Message(MessageContext, Vec<u8>),
}

impl UserAction {
    pub(crate) fn signer(&self) -> Option<Owner> {
        use UserAction::*;
        match self {
            Instantiate(context, _) => context.authenticated_signer,
            Operation(context, _) => context.authenticated_signer,
            Message(context, _) => context.authenticated_signer,
        }
    }

    pub(crate) fn height(&self) -> BlockHeight {
        match self {
            UserAction::Instantiate(context, _) => context.height,
            UserAction::Operation(context, _) => context.height,
            UserAction::Message(context, _) => context.height,
        }
    }

    pub(crate) fn next_message_index(&self) -> u32 {
        match self {
            UserAction::Instantiate(context, _) => context.next_message_index,
            UserAction::Operation(context, _) => context.next_message_index,
            UserAction::Message(context, _) => context.next_message_index,
        }
    }
}

impl<C> ExecutionStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
    C::Extra: ExecutionRuntimeContext,
{
    #[allow(clippy::too_many_arguments)]
    async fn run_user_action(
        &mut self,
        application_id: UserApplicationId,
        chain_id: ChainId,
        local_time: Timestamp,
        action: UserAction,
        refund_grant_to: Option<Account>,
        grant: Option<&mut Amount>,
        oracle_record: Option<OracleRecord>,
        resource_controller: &mut ResourceController<Option<Owner>>,
    ) -> Result<(Vec<ExecutionOutcome>, OracleRecord), ExecutionError> {
        let ExecutionRuntimeConfig {} = self.context().extra().execution_runtime_config();
        let (execution_outcomes, oracle_record) = self
            .run_user_action_with_runtime(
                application_id,
                chain_id,
                local_time,
                action,
                refund_grant_to,
                grant,
                oracle_record,
                resource_controller,
            )
            .await?;
        let execution_outcomes = self
            .update_execution_outcomes_with_app_registrations(execution_outcomes)
            .await?;
        Ok((execution_outcomes, oracle_record))
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_user_action_with_runtime(
        &mut self,
        application_id: UserApplicationId,
        chain_id: ChainId,
        local_time: Timestamp,
        action: UserAction,
        refund_grant_to: Option<Account>,
        grant: Option<&mut Amount>,
        oracle_record: Option<OracleRecord>,
        resource_controller: &mut ResourceController<Option<Owner>>,
    ) -> Result<(Vec<ExecutionOutcome>, OracleRecord), ExecutionError> {
        let mut cloned_grant = grant.as_ref().map(|x| **x);
        let initial_balance = resource_controller
            .with_state_and_grant(self, cloned_grant.as_mut())
            .await?
            .balance()?;
        let controller = ResourceController {
            policy: resource_controller.policy.clone(),
            tracker: resource_controller.tracker,
            account: initial_balance,
        };
        let (execution_state_sender, mut execution_state_receiver) =
            futures::channel::mpsc::unbounded();
        let execution_outcomes_future = tokio::task::spawn_blocking(move || {
            ContractSyncRuntime::run_action(
                execution_state_sender,
                application_id,
                chain_id,
                local_time,
                refund_grant_to,
                controller,
                action,
                oracle_record,
            )
        });
        while let Some(request) = execution_state_receiver.next().await {
            self.handle_request(request).await?;
        }
        let (execution_outcomes, oracle_record, controller) = execution_outcomes_future.await??;
        resource_controller
            .with_state_and_grant(self, grant)
            .await?
            .merge_balance(initial_balance, controller.balance()?)?;
        resource_controller.tracker = controller.tracker;
        Ok((execution_outcomes, oracle_record))
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

        let mut applications_to_register_per_destination = BTreeMap::<_, BTreeSet<_>>::new();

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
                    grant: Amount::ZERO,
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
        local_time: Timestamp,
        operation: Operation,
        oracle_record: Option<OracleRecord>,
        resource_controller: &mut ResourceController<Option<Owner>>,
    ) -> Result<(Vec<ExecutionOutcome>, OracleRecord), ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        match operation {
            Operation::System(op) => {
                let (mut result, new_application, returned_oracle_record) =
                    self.system.execute_operation(context, op).await?;
                result.authenticated_signer = context.authenticated_signer;
                result.refund_grant_to = context.refund_grant_to();
                let mut outcomes = vec![ExecutionOutcome::System(result)];
                if let Some((application_id, argument)) = new_application {
                    let user_action = UserAction::Instantiate(context, argument);
                    let (user_outcomes, oracle_record) = self
                        .run_user_action(
                            application_id,
                            context.chain_id,
                            local_time,
                            user_action,
                            context.refund_grant_to(),
                            None,
                            oracle_record,
                            resource_controller,
                        )
                        .await?;
                    outcomes.extend(user_outcomes);
                    return Ok((outcomes, oracle_record));
                }
                Ok((outcomes, returned_oracle_record))
            }
            Operation::User {
                application_id,
                bytes,
            } => {
                self.run_user_action(
                    application_id,
                    context.chain_id,
                    local_time,
                    UserAction::Operation(context, bytes),
                    context.refund_grant_to(),
                    None,
                    oracle_record,
                    resource_controller,
                )
                .await
            }
        }
    }

    pub async fn execute_message(
        &mut self,
        context: MessageContext,
        local_time: Timestamp,
        message: Message,
        grant: Option<&mut Amount>,
        oracle_record: Option<OracleRecord>,
        resource_controller: &mut ResourceController<Option<Owner>>,
    ) -> Result<(Vec<ExecutionOutcome>, OracleRecord), ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        match message {
            Message::System(message) => {
                let outcome = self.system.execute_message(context, message).await?;
                Ok((
                    vec![ExecutionOutcome::System(outcome)],
                    OracleRecord::default(),
                ))
            }
            Message::User {
                application_id,
                bytes,
            } => {
                self.run_user_action(
                    application_id,
                    context.chain_id,
                    local_time,
                    UserAction::Message(context, bytes),
                    context.refund_grant_to,
                    grant,
                    oracle_record,
                    resource_controller,
                )
                .await
            }
        }
    }

    pub async fn bounce_message(
        &self,
        context: MessageContext,
        grant: Amount,
        refund_grant_to: Option<Account>,
        message: Message,
    ) -> Result<Vec<ExecutionOutcome>, ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        match message {
            Message::System(message) => {
                let mut outcome = RawExecutionOutcome {
                    authenticated_signer: context.authenticated_signer,
                    refund_grant_to,
                    ..Default::default()
                };
                outcome.messages.push(RawOutgoingMessage {
                    destination: Destination::Recipient(context.message_id.chain_id),
                    authenticated: true,
                    grant,
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
                    refund_grant_to,
                    ..Default::default()
                };
                outcome.messages.push(RawOutgoingMessage {
                    destination: Destination::Recipient(context.message_id.chain_id),
                    authenticated: true,
                    grant,
                    kind: MessageKind::Bouncing,
                    message: bytes,
                });
                Ok(vec![ExecutionOutcome::User(application_id, outcome)])
            }
        }
    }

    pub async fn send_refund(
        &self,
        context: MessageContext,
        amount: Amount,
        account: Account,
    ) -> Result<ExecutionOutcome, ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        let mut outcome = RawExecutionOutcome::default();
        let message = RawOutgoingMessage {
            destination: Destination::Recipient(account.chain_id),
            authenticated: false,
            grant: Amount::ZERO,
            kind: MessageKind::Tracked,
            message: SystemMessage::Credit {
                amount,
                source: context.authenticated_signer,
                target: account.owner,
            },
        };
        outcome.messages.push(message);
        Ok(ExecutionOutcome::System(outcome))
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
                let ExecutionRuntimeConfig {} = self.context().extra().execution_runtime_config();
                let response = self
                    .query_user_application(application_id, context, bytes)
                    .await?;
                Ok(Response::User(response))
            }
        }
    }

    async fn query_user_application(
        &mut self,
        application_id: UserApplicationId,
        context: QueryContext,
        query: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        let (execution_state_sender, mut execution_state_receiver) =
            futures::channel::mpsc::unbounded();
        let query_result_future = tokio::task::spawn_blocking(move || {
            ServiceSyncRuntime::new(execution_state_sender, context)
                .run_query(application_id, query)
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

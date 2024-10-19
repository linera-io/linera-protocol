// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::mem;

use futures::{FutureExt, StreamExt};
use linera_base::{
    data_types::{Amount, BlockHeight, OracleResponse, Timestamp},
    identifiers::{Account, BlobId, BlobType, ChainId, Destination, Owner},
};
use linera_views::{
    context::Context,
    key_value_store_view::KeyValueStoreView,
    reentrant_collection_view::HashedReentrantCollectionView,
    views::{ClonableView, View},
};
use linera_views_derive::CryptoHashView;
#[cfg(with_testing)]
use {
    crate::{
        ResourceControlPolicy, ResourceTracker, TestExecutionRuntimeContext,
        UserApplicationDescription, UserContractCode,
    },
    linera_base::data_types::Blob,
    linera_views::context::MemoryContext,
    std::{collections::BTreeMap, sync::Arc},
};

use super::{runtime::ServiceRuntimeRequest, ExecutionRequest};
use crate::{
    resources::ResourceController, system::SystemExecutionStateView, ContractSyncRuntime,
    ExecutionError, ExecutionRuntimeConfig, ExecutionRuntimeContext, Message, MessageContext,
    MessageKind, Operation, OperationContext, Query, QueryContext, RawExecutionOutcome,
    RawOutgoingMessage, Response, ServiceSyncRuntime, SystemMessage, TransactionTracker,
    UserApplicationId,
};

/// A view accessing the execution state of a chain.
#[derive(Debug, ClonableView, CryptoHashView)]
pub struct ExecutionStateView<C> {
    /// System application.
    pub system: SystemExecutionStateView<C>,
    /// User applications.
    pub users: HashedReentrantCollectionView<C, UserApplicationId, KeyValueStoreView<C>>,
}

/// How to interact with a long-lived service runtime.
pub struct ServiceRuntimeEndpoint {
    /// How to receive requests.
    pub incoming_execution_requests: futures::channel::mpsc::UnboundedReceiver<ExecutionRequest>,
    /// How to query the runtime.
    pub runtime_request_sender: std::sync::mpsc::Sender<ServiceRuntimeRequest>,
}

#[cfg(with_testing)]
impl ExecutionStateView<MemoryContext<TestExecutionRuntimeContext>>
where
    MemoryContext<TestExecutionRuntimeContext>: Context + Clone + Send + Sync + 'static,
{
    /// Simulates the instantiation of an application.
    pub async fn simulate_instantiation(
        &mut self,
        contract: UserContractCode,
        local_time: Timestamp,
        application_description: UserApplicationDescription,
        instantiation_argument: Vec<u8>,
        contract_blob: Blob,
        service_blob: Blob,
    ) -> Result<(), ExecutionError> {
        let chain_id = application_description.creator_chain_id;
        let context = OperationContext {
            chain_id,
            authenticated_signer: None,
            authenticated_caller_id: None,
            height: application_description.block_height,
            txn_index: Some(0),
            operation_index: Some(0),
        };

        let action = UserAction::Instantiate(context, instantiation_argument);
        let application_id = UserApplicationId::from(&application_description);
        let application_blob = Blob::new_application_description(application_description.clone());

        self.context()
            .extra()
            .user_contracts()
            .insert(application_id, contract);

        self.context()
            .extra()
            .add_blobs(vec![contract_blob, service_blob, application_blob]);

        let tracker = ResourceTracker::default();
        let policy = ResourceControlPolicy::default();
        let mut resource_controller = ResourceController {
            policy: Arc::new(policy),
            tracker,
            account: None,
        };

        let mut pending_applications = BTreeMap::new();
        pending_applications.insert(application_id, application_description);

        let mut txn_tracker = TransactionTracker::new(0, None, Arc::new(pending_applications));
        self.run_user_action(
            application_id,
            chain_id,
            local_time,
            action,
            context.refund_grant_to(),
            None,
            &mut txn_tracker,
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
}

impl<C> ExecutionStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    #[expect(clippy::too_many_arguments)]
    async fn run_user_action(
        &mut self,
        application_id: UserApplicationId,
        chain_id: ChainId,
        local_time: Timestamp,
        action: UserAction,
        refund_grant_to: Option<Account>,
        grant: Option<&mut Amount>,
        txn_tracker: &mut TransactionTracker,
        resource_controller: &mut ResourceController<Option<Owner>>,
    ) -> Result<(), ExecutionError> {
        match action {
            UserAction::Instantiate(_, _) => {
                self.system
                    .check_and_record_bytecode_blobs(&application_id.bytecode_id, txn_tracker)
                    .await?;
                txn_tracker.replay_oracle_response(OracleResponse::Blob(BlobId::new(
                    application_id.application_description_hash,
                    BlobType::ApplicationDescription,
                )))?;
            }
            UserAction::Operation(_, _) | UserAction::Message(_, _) => {
                self.system
                    .check_and_record_application_blob(&application_id, txn_tracker)
                    .await?;
            }
        }

        let ExecutionRuntimeConfig {} = self.context().extra().execution_runtime_config();
        self.run_user_action_with_runtime(
            application_id,
            chain_id,
            local_time,
            action,
            refund_grant_to,
            grant,
            txn_tracker,
            resource_controller,
        )
        .await?;
        Ok(())
    }

    #[expect(clippy::too_many_arguments)]
    async fn run_user_action_with_runtime(
        &mut self,
        application_id: UserApplicationId,
        chain_id: ChainId,
        local_time: Timestamp,
        action: UserAction,
        refund_grant_to: Option<Account>,
        grant: Option<&mut Amount>,
        txn_tracker: &mut TransactionTracker,
        resource_controller: &mut ResourceController<Option<Owner>>,
    ) -> Result<(), ExecutionError> {
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
        let txn_tracker_moved = mem::take(txn_tracker);
        let execution_outcomes_future = linera_base::task::spawn_blocking(move || {
            ContractSyncRuntime::run_action(
                execution_state_sender,
                application_id,
                chain_id,
                local_time,
                refund_grant_to,
                controller,
                action,
                txn_tracker_moved,
            )
        });
        while let Some(request) = execution_state_receiver.next().await {
            self.handle_request(request).await?;
        }

        let (controller, txn_tracker_moved) = execution_outcomes_future.await??;
        *txn_tracker = txn_tracker_moved;
        resource_controller
            .with_state_and_grant(self, grant)
            .await?
            .merge_balance(initial_balance, controller.balance()?)?;
        resource_controller.tracker = controller.tracker;
        Ok(())
    }

    pub async fn execute_operation(
        &mut self,
        context: OperationContext,
        local_time: Timestamp,
        operation: Operation,
        txn_tracker: &mut TransactionTracker,
        resource_controller: &mut ResourceController<Option<Owner>>,
    ) -> Result<(), ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        match operation {
            Operation::System(op) => {
                let new_application = self
                    .system
                    .execute_operation(context, op, txn_tracker)
                    .await?;
                if let Some((application_id, argument)) = new_application {
                    let user_action = UserAction::Instantiate(context, argument);
                    self.run_user_action(
                        application_id,
                        context.chain_id,
                        local_time,
                        user_action,
                        context.refund_grant_to(),
                        None,
                        txn_tracker,
                        resource_controller,
                    )
                    .await?;
                }
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
                    txn_tracker,
                    resource_controller,
                )
                .await?;
            }
        }
        Ok(())
    }

    pub async fn execute_message(
        &mut self,
        context: MessageContext,
        local_time: Timestamp,
        message: Message,
        grant: Option<&mut Amount>,
        txn_tracker: &mut TransactionTracker,
        resource_controller: &mut ResourceController<Option<Owner>>,
    ) -> Result<(), ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        match message {
            Message::System(message) => {
                let outcome = self.system.execute_message(context, message).await?;
                txn_tracker.add_system_outcome(outcome)?;
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
                    txn_tracker,
                    resource_controller,
                )
                .await?;
            }
        }
        Ok(())
    }

    pub async fn bounce_message(
        &self,
        context: MessageContext,
        grant: Amount,
        message: Message,
        txn_tracker: &mut TransactionTracker,
    ) -> Result<(), ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        match message {
            Message::System(message) => {
                let mut outcome = RawExecutionOutcome {
                    authenticated_signer: context.authenticated_signer,
                    refund_grant_to: context.refund_grant_to,
                    ..Default::default()
                };
                outcome.messages.push(RawOutgoingMessage {
                    destination: Destination::Recipient(context.message_id.chain_id),
                    authenticated: true,
                    grant,
                    kind: MessageKind::Bouncing,
                    message,
                });
                txn_tracker.add_system_outcome(outcome)?;
            }
            Message::User {
                application_id,
                bytes,
            } => {
                let mut outcome = RawExecutionOutcome {
                    authenticated_signer: context.authenticated_signer,
                    refund_grant_to: context.refund_grant_to,
                    ..Default::default()
                };
                outcome.messages.push(RawOutgoingMessage {
                    destination: Destination::Recipient(context.message_id.chain_id),
                    authenticated: true,
                    grant,
                    kind: MessageKind::Bouncing,
                    message: bytes,
                });
                txn_tracker.add_user_outcome(application_id, outcome)?;
            }
        }
        Ok(())
    }

    pub async fn send_refund(
        &self,
        context: MessageContext,
        amount: Amount,
        account: Account,
        txn_tracker: &mut TransactionTracker,
    ) -> Result<(), ExecutionError> {
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
        txn_tracker.add_system_outcome(outcome)?;
        Ok(())
    }

    pub async fn query_application(
        &mut self,
        context: QueryContext,
        query: Query,
        endpoint: Option<&mut ServiceRuntimeEndpoint>,
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
                let response = match endpoint {
                    Some(endpoint) => {
                        self.query_user_application_with_long_lived_service(
                            application_id,
                            context,
                            bytes,
                            &mut endpoint.incoming_execution_requests,
                            &mut endpoint.runtime_request_sender,
                        )
                        .await?
                    }
                    None => {
                        self.query_user_application(application_id, context, bytes)
                            .await?
                    }
                };
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
        let execution_outcomes_future = linera_base::task::spawn_blocking(move || {
            let mut runtime = ServiceSyncRuntime::new(execution_state_sender, context);
            runtime.run_query(application_id, query)
        });
        while let Some(request) = execution_state_receiver.next().await {
            self.handle_request(request).await?;
        }

        let response = execution_outcomes_future.await??;
        Ok(response)
    }

    async fn query_user_application_with_long_lived_service(
        &mut self,
        application_id: UserApplicationId,
        context: QueryContext,
        query: Vec<u8>,
        incoming_execution_requests: &mut futures::channel::mpsc::UnboundedReceiver<
            ExecutionRequest,
        >,
        runtime_request_sender: &mut std::sync::mpsc::Sender<ServiceRuntimeRequest>,
    ) -> Result<Vec<u8>, ExecutionError> {
        let (response_sender, response_receiver) = oneshot::channel();
        let mut response_receiver = response_receiver.fuse();

        runtime_request_sender
            .send(ServiceRuntimeRequest::Query {
                application_id,
                context,
                query,
                callback: response_sender,
            })
            .expect("Service runtime thread should only stop when `request_sender` is dropped");

        loop {
            futures::select! {
                maybe_request = incoming_execution_requests.next() => {
                    if let Some(request) = maybe_request {
                        self.handle_request(request).await?;
                    }
                }
                response = &mut response_receiver => {
                    return response.map_err(|_| ExecutionError::MissingRuntimeResponse)?;
                }
            }
        }
    }
}

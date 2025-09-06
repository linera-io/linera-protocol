// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, BTreeSet},
    mem, vec,
};

use futures::{FutureExt, StreamExt};
use linera_base::{
    data_types::{Amount, BlockHeight, StreamUpdate},
    identifiers::{Account, AccountOwner, StreamId},
};
use linera_views::{
    context::Context,
    key_value_store_view::KeyValueStoreView,
    map_view::MapView,
    reentrant_collection_view::HashedReentrantCollectionView,
    views::{ClonableView, ReplaceContext, View},
};
use linera_views_derive::CryptoHashView;
#[cfg(with_testing)]
use {
    crate::{
        ResourceControlPolicy, ResourceTracker, TestExecutionRuntimeContext, UserContractCode,
    },
    linera_base::data_types::Blob,
    linera_views::context::MemoryContext,
    std::sync::Arc,
};

use super::{runtime::ServiceRuntimeRequest, ExecutionRequest};
use crate::{
    resources::ResourceController, system::SystemExecutionStateView, ApplicationDescription,
    ApplicationId, ContractSyncRuntime, ExecutionError, ExecutionRuntimeConfig,
    ExecutionRuntimeContext, Message, MessageContext, MessageKind, Operation, OperationContext,
    OutgoingMessage, ProcessStreamsContext, Query, QueryContext, QueryOutcome, ServiceSyncRuntime,
    SystemMessage, Timestamp, TransactionTracker,
};

/// A view accessing the execution state of a chain.
#[derive(Debug, ClonableView, CryptoHashView)]
pub struct ExecutionStateView<C> {
    /// System application.
    pub system: SystemExecutionStateView<C>,
    /// User applications.
    pub users: HashedReentrantCollectionView<C, ApplicationId, KeyValueStoreView<C>>,
    /// The number of events in the streams that this chain is writing to.
    pub stream_event_counts: MapView<C, StreamId, u32>,
}

impl<C: Context, C2: Context> ReplaceContext<C2> for ExecutionStateView<C> {
    type Target = ExecutionStateView<C2>;

    async fn with_context(
        &mut self,
        ctx: impl FnOnce(&Self::Context) -> C2 + Clone,
    ) -> Self::Target {
        ExecutionStateView {
            system: self.system.with_context(ctx.clone()).await,
            users: self.users.with_context(ctx.clone()).await,
            stream_event_counts: self.stream_event_counts.with_context(ctx.clone()).await,
        }
    }
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
        local_time: linera_base::data_types::Timestamp,
        application_description: ApplicationDescription,
        instantiation_argument: Vec<u8>,
        contract_blob: Blob,
        service_blob: Blob,
    ) -> Result<(), ExecutionError> {
        let chain_id = application_description.creator_chain_id;
        assert_eq!(chain_id, self.context().extra().chain_id);
        let context = OperationContext {
            chain_id,
            authenticated_signer: None,
            height: application_description.block_height,
            round: None,
            timestamp: local_time,
        };

        let action = UserAction::Instantiate(context, instantiation_argument);
        let next_application_index = application_description.application_index + 1;
        let next_chain_index = 0;

        let application_id = From::from(&application_description);
        let blob = Blob::new_application_description(&application_description);

        self.system.used_blobs.insert(&blob.id())?;
        self.system.used_blobs.insert(&contract_blob.id())?;
        self.system.used_blobs.insert(&service_blob.id())?;

        self.context()
            .extra()
            .user_contracts()
            .insert(application_id, contract);

        self.context()
            .extra()
            .add_blobs([
                contract_blob,
                service_blob,
                Blob::new_application_description(&application_description),
            ])
            .await?;

        let tracker = ResourceTracker::default();
        let policy = ResourceControlPolicy::no_fees();
        let mut resource_controller = ResourceController::new(Arc::new(policy), tracker, None);
        let mut txn_tracker = TransactionTracker::new(
            local_time,
            0,
            next_application_index,
            next_chain_index,
            None,
            &[],
        );
        txn_tracker.add_created_blob(blob);
        self.run_user_action(
            application_id,
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
    ProcessStreams(ProcessStreamsContext, Vec<StreamUpdate>),
}

impl UserAction {
    pub(crate) fn signer(&self) -> Option<AccountOwner> {
        match self {
            UserAction::Instantiate(context, _) => context.authenticated_signer,
            UserAction::Operation(context, _) => context.authenticated_signer,
            UserAction::ProcessStreams(_, _) => None,
            UserAction::Message(context, _) => context.authenticated_signer,
        }
    }

    pub(crate) fn height(&self) -> BlockHeight {
        match self {
            UserAction::Instantiate(context, _) => context.height,
            UserAction::Operation(context, _) => context.height,
            UserAction::ProcessStreams(context, _) => context.height,
            UserAction::Message(context, _) => context.height,
        }
    }

    pub(crate) fn round(&self) -> Option<u32> {
        match self {
            UserAction::Instantiate(context, _) => context.round,
            UserAction::Operation(context, _) => context.round,
            UserAction::ProcessStreams(context, _) => context.round,
            UserAction::Message(context, _) => context.round,
        }
    }

    pub(crate) fn timestamp(&self) -> Timestamp {
        match self {
            UserAction::Instantiate(context, _) => context.timestamp,
            UserAction::Operation(context, _) => context.timestamp,
            UserAction::ProcessStreams(context, _) => context.timestamp,
            UserAction::Message(context, _) => context.timestamp,
        }
    }
}

impl<C> ExecutionStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    async fn run_user_action(
        &mut self,
        application_id: ApplicationId,
        action: UserAction,
        refund_grant_to: Option<Account>,
        grant: Option<&mut Amount>,
        txn_tracker: &mut TransactionTracker,
        resource_controller: &mut ResourceController<Option<AccountOwner>>,
    ) -> Result<(), ExecutionError> {
        let ExecutionRuntimeConfig {} = self.context().extra().execution_runtime_config();
        self.run_user_action_with_runtime(
            application_id,
            action,
            refund_grant_to,
            grant,
            txn_tracker,
            resource_controller,
        )
        .await
    }

    async fn run_user_action_with_runtime(
        &mut self,
        application_id: ApplicationId,
        action: UserAction,
        refund_grant_to: Option<Account>,
        grant: Option<&mut Amount>,
        txn_tracker: &mut TransactionTracker,
        resource_controller: &mut ResourceController<Option<AccountOwner>>,
    ) -> Result<(), ExecutionError> {
        let chain_id = self.context().extra().chain_id();
        let mut cloned_grant = grant.as_ref().map(|x| **x);
        let initial_balance = resource_controller
            .with_state_and_grant(&mut self.system, cloned_grant.as_mut())
            .await?
            .balance()?;
        let controller = ResourceController::new(
            resource_controller.policy().clone(),
            resource_controller.tracker,
            initial_balance,
        );
        let (execution_state_sender, mut execution_state_receiver) =
            futures::channel::mpsc::unbounded();
        let (code, description) = self.load_contract(application_id, txn_tracker).await?;
        let txn_tracker_moved = mem::take(txn_tracker);
        let contract_runtime_task = linera_base::task::Blocking::spawn(move |mut codes| {
            let runtime = ContractSyncRuntime::new(
                execution_state_sender,
                chain_id,
                refund_grant_to,
                controller,
                &action,
                txn_tracker_moved,
            );

            async move {
                let code = codes.next().await.expect("we send this immediately below");
                runtime.preload_contract(application_id, code, description)?;
                runtime.run_action(application_id, chain_id, action)
            }
        })
        .await;

        contract_runtime_task.send(code)?;

        while let Some(request) = execution_state_receiver.next().await {
            self.handle_request(request, resource_controller).await?;
        }

        let (result, controller, txn_tracker_moved) = contract_runtime_task.join().await?;

        *txn_tracker = txn_tracker_moved;
        txn_tracker.add_operation_result(result);

        resource_controller
            .with_state_and_grant(&mut self.system, grant)
            .await?
            .merge_balance(initial_balance, controller.balance()?)?;
        resource_controller.tracker = controller.tracker;

        Ok(())
    }

    pub async fn execute_operation(
        &mut self,
        context: OperationContext,
        operation: Operation,
        txn_tracker: &mut TransactionTracker,
        resource_controller: &mut ResourceController<Option<AccountOwner>>,
    ) -> Result<(), ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        match operation {
            Operation::System(op) => {
                let new_application = self
                    .system
                    .execute_operation(context, *op, txn_tracker, resource_controller)
                    .await?;
                if let Some((application_id, argument)) = new_application {
                    let user_action = UserAction::Instantiate(context, argument);
                    self.run_user_action(
                        application_id,
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
                    UserAction::Operation(context, bytes),
                    context.refund_grant_to(),
                    None,
                    txn_tracker,
                    resource_controller,
                )
                .await?;
            }
        }
        self.process_subscriptions(txn_tracker, resource_controller, context.into())
            .await?;
        Ok(())
    }

    pub async fn execute_message(
        &mut self,
        context: MessageContext,
        message: Message,
        grant: Option<&mut Amount>,
        txn_tracker: &mut TransactionTracker,
        resource_controller: &mut ResourceController<Option<AccountOwner>>,
    ) -> Result<(), ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        match message {
            Message::System(message) => {
                let outcome = self.system.execute_message(context, message).await?;
                txn_tracker.add_outgoing_messages(outcome);
            }
            Message::User {
                application_id,
                bytes,
            } => {
                self.run_user_action(
                    application_id,
                    UserAction::Message(context, bytes),
                    context.refund_grant_to,
                    grant,
                    txn_tracker,
                    resource_controller,
                )
                .await?;
            }
        }
        self.process_subscriptions(txn_tracker, resource_controller, context.into())
            .await?;
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
        txn_tracker.add_outgoing_message(OutgoingMessage {
            destination: context.origin,
            authenticated_signer: context.authenticated_signer,
            refund_grant_to: context.refund_grant_to.filter(|_| !grant.is_zero()),
            grant,
            kind: MessageKind::Bouncing,
            message,
        });
        Ok(())
    }

    pub async fn send_refund(
        &self,
        context: MessageContext,
        amount: Amount,
        txn_tracker: &mut TransactionTracker,
    ) -> Result<(), ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        if amount.is_zero() {
            return Ok(());
        }
        let Some(account) = context.refund_grant_to else {
            return Err(ExecutionError::InternalError(
                "Messages with grants should have a non-empty `refund_grant_to`",
            ));
        };
        let message = SystemMessage::Credit {
            amount,
            source: context.authenticated_signer.unwrap_or(AccountOwner::CHAIN),
            target: account.owner,
        };
        txn_tracker.add_outgoing_message(
            OutgoingMessage::new(account.chain_id, message).with_kind(MessageKind::Tracked),
        );
        Ok(())
    }

    pub async fn query_application(
        &mut self,
        context: QueryContext,
        query: Query,
        endpoint: Option<&mut ServiceRuntimeEndpoint>,
    ) -> Result<QueryOutcome, ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        match query {
            Query::System(query) => {
                let outcome = self.system.handle_query(context, query).await?;
                Ok(outcome.into())
            }
            Query::User {
                application_id,
                bytes,
            } => {
                let ExecutionRuntimeConfig {} = self.context().extra().execution_runtime_config();
                let outcome = match endpoint {
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
                Ok(outcome.into())
            }
        }
    }

    async fn query_user_application(
        &mut self,
        application_id: ApplicationId,
        context: QueryContext,
        query: Vec<u8>,
    ) -> Result<QueryOutcome<Vec<u8>>, ExecutionError> {
        let (execution_state_sender, mut execution_state_receiver) =
            futures::channel::mpsc::unbounded();
        let (code, description) = self
            .load_service(application_id, &mut TransactionTracker::default())
            .await?;

        let service_runtime_task = linera_base::task::Blocking::spawn(move |mut codes| {
            let mut runtime = ServiceSyncRuntime::new(execution_state_sender, context);

            async move {
                let code = codes.next().await.expect("we send this immediately below");
                runtime.preload_service(application_id, code, description)?;
                runtime.run_query(application_id, query)
            }
        })
        .await;

        service_runtime_task.send(code)?;

        while let Some(request) = execution_state_receiver.next().await {
            self.handle_request(request, &mut ResourceController::default())
                .await?;
        }

        service_runtime_task.join().await
    }

    async fn query_user_application_with_long_lived_service(
        &mut self,
        application_id: ApplicationId,
        context: QueryContext,
        query: Vec<u8>,
        incoming_execution_requests: &mut futures::channel::mpsc::UnboundedReceiver<
            ExecutionRequest,
        >,
        runtime_request_sender: &mut std::sync::mpsc::Sender<ServiceRuntimeRequest>,
    ) -> Result<QueryOutcome<Vec<u8>>, ExecutionError> {
        let (outcome_sender, outcome_receiver) = oneshot::channel();
        let mut outcome_receiver = outcome_receiver.fuse();

        runtime_request_sender
            .send(ServiceRuntimeRequest::Query {
                application_id,
                context,
                query,
                callback: outcome_sender,
            })
            .expect("Service runtime thread should only stop when `request_sender` is dropped");

        loop {
            futures::select! {
                maybe_request = incoming_execution_requests.next() => {
                    if let Some(request) = maybe_request {
                        self.handle_request(request, &mut ResourceController::default()).await?;
                    }
                }
                outcome = &mut outcome_receiver => {
                    return outcome.map_err(|_| ExecutionError::MissingRuntimeResponse)?;
                }
            }
        }
    }

    pub async fn list_applications(
        &self,
    ) -> Result<Vec<(ApplicationId, ApplicationDescription)>, ExecutionError> {
        let mut applications = vec![];
        for app_id in self.users.indices().await? {
            let blob_id = app_id.description_blob_id();
            let blob_content = self.system.read_blob_content(blob_id).await?;
            let application_description = bcs::from_bytes(blob_content.bytes())?;
            applications.push((app_id, application_description));
        }
        Ok(applications)
    }

    /// Calls `process_streams` for all applications that are subscribed to streams with new
    /// events or that have new subscriptions.
    async fn process_subscriptions(
        &mut self,
        txn_tracker: &mut TransactionTracker,
        resource_controller: &mut ResourceController<Option<AccountOwner>>,
        context: ProcessStreamsContext,
    ) -> Result<(), ExecutionError> {
        // Keep track of which streams we have already processed. This is to guard against
        // applications unsubscribing and subscribing in the process_streams call itself.
        let mut processed = BTreeSet::new();
        loop {
            let to_process = txn_tracker
                .take_streams_to_process()
                .into_iter()
                .filter_map(|(app_id, updates)| {
                    let updates = updates
                        .into_iter()
                        .filter_map(|update| {
                            if !processed.insert((
                                app_id,
                                update.chain_id,
                                update.stream_id.clone(),
                            )) {
                                return None;
                            }
                            Some(update)
                        })
                        .collect::<Vec<_>>();
                    if updates.is_empty() {
                        return None;
                    }
                    Some((app_id, updates))
                })
                .collect::<BTreeMap<_, _>>();
            if to_process.is_empty() {
                return Ok(());
            }
            for (app_id, updates) in to_process {
                self.run_user_action(
                    app_id,
                    UserAction::ProcessStreams(context, updates),
                    None,
                    None,
                    txn_tracker,
                    resource_controller,
                )
                .await?;
            }
        }
    }
}

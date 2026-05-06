// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut},
    vec,
};

use allocative::Allocative;
use futures::{FutureExt, StreamExt};
use linera_base::{
    crypto::{BcsHashable, CryptoHash},
    data_types::{BlobContent, BlockHeight, StreamUpdate},
    identifiers::{AccountOwner, BlobId, ChainId, StreamId},
    time::Instant,
};
use linera_views::{
    context::Context,
    historical_hash_wrapper::HistoricallyHashableView,
    key_value_store_view::KeyValueStoreView,
    map_view::MapView,
    reentrant_collection_view::ReentrantCollectionView,
    views::{ClonableView, ReplaceContext, View},
    ViewError,
};
use serde::{Deserialize, Serialize};
#[cfg(with_testing)]
use {
    crate::{
        execution_state_actor::{RuntimeChannels, RuntimeCommand},
        runtime::ContractSyncRuntime,
        FinalizeContext, Message, Operation, ResourceControlPolicy, ResourceTracker,
        TestExecutionRuntimeContext, UserContractCode,
    },
    linera_base::{
        data_types::{Amount, Blob, Timestamp},
        identifiers::Account,
    },
    linera_views::context::MemoryContext,
    std::sync::Arc,
};

use super::{execution_state_actor::ExecutionRequest, runtime::ServiceRuntimeRequest};
use crate::{
    execution_state_actor::ExecutionStateActor, resources::ResourceController,
    system::SystemExecutionStateView, ApplicationDescription, ApplicationId, ExecutionError,
    ExecutionRuntimeContext, JsVec, MessageContext, OperationContext, ProcessStreamsContext, Query,
    QueryContext, QueryOutcome, ServiceSyncRuntime, TransactionTracker,
};

/// An inner view accessing the execution state of a chain, for hashing purposes.
#[derive(Debug, ClonableView, View, Allocative)]
#[allocative(bound = "C")]
pub struct ExecutionStateViewInner<C> {
    /// System application.
    pub system: SystemExecutionStateView<C>,
    /// User applications.
    pub users: ReentrantCollectionView<C, ApplicationId, KeyValueStoreView<C>>,
    /// The heights of previous blocks that sent messages to the same recipients.
    pub previous_message_blocks: MapView<C, ChainId, BlockHeight>,
    /// The heights of previous blocks that published events to the same streams.
    pub previous_event_blocks: MapView<C, StreamId, BlockHeight>,
}

impl<C: Context, C2: Context> ReplaceContext<C2> for ExecutionStateViewInner<C> {
    type Target = ExecutionStateViewInner<C2>;

    async fn with_context(
        &mut self,
        ctx: impl FnOnce(&Self::Context) -> C2 + Clone,
    ) -> Self::Target {
        ExecutionStateViewInner {
            system: self.system.with_context(ctx.clone()).await,
            users: self.users.with_context(ctx.clone()).await,
            previous_message_blocks: self.previous_message_blocks.with_context(ctx.clone()).await,
            previous_event_blocks: self.previous_event_blocks.with_context(ctx.clone()).await,
        }
    }
}

/// A view accessing the execution state of a chain.
#[derive(Debug, ClonableView, View, Allocative)]
#[allocative(bound = "C")]
pub struct ExecutionStateView<C> {
    inner: HistoricallyHashableView<C, ExecutionStateViewInner<C>>,
}

impl<C> Deref for ExecutionStateView<C> {
    type Target = ExecutionStateViewInner<C>;

    fn deref(&self) -> &ExecutionStateViewInner<C> {
        self.inner.deref()
    }
}

impl<C> DerefMut for ExecutionStateView<C> {
    fn deref_mut(&mut self) -> &mut ExecutionStateViewInner<C> {
        self.inner.deref_mut()
    }
}

impl<C> ExecutionStateView<C>
where
    C: Context + Clone + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    pub async fn crypto_hash_mut(&mut self) -> Result<CryptoHash, ViewError> {
        #[derive(Serialize, Deserialize)]
        struct ExecutionStateViewHash([u8; 32]);
        impl BcsHashable<'_> for ExecutionStateViewHash {}
        let hash = self.inner.historical_hash().await?;
        Ok(CryptoHash::new(&ExecutionStateViewHash(hash.into())))
    }
}

impl<C: Context, C2: Context> ReplaceContext<C2> for ExecutionStateView<C> {
    type Target = ExecutionStateView<C2>;

    async fn with_context(
        &mut self,
        ctx: impl FnOnce(&Self::Context) -> C2 + Clone,
    ) -> Self::Target {
        ExecutionStateView {
            inner: self.inner.with_context(ctx.clone()).await,
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
impl<C> ExecutionStateView<C>
where
    C: Context + Clone + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    /// Spawns a block-level contract runtime, runs `f` against an actor connected to it,
    /// then sends `FinalizeAll` and tears the runtime down. Test-only.
    #[allow(clippy::too_many_arguments)]
    async fn with_block_runtime<F, R>(
        &mut self,
        txn_tracker: &mut TransactionTracker,
        resource_controller: &mut ResourceController<Option<AccountOwner>>,
        chain_id: ChainId,
        height: BlockHeight,
        round: Option<u32>,
        timestamp: Timestamp,
        finalize_context: FinalizeContext,
        f: F,
    ) -> Result<R, ExecutionError>
    where
        F: AsyncFnOnce(&mut ExecutionStateActor<'_, C>) -> Result<R, ExecutionError>,
    {
        let (command_tx, command_rx) = std::sync::mpsc::channel::<RuntimeCommand>();
        let (execution_state_sender, mut execution_state_receiver) =
            futures::channel::mpsc::unbounded();

        let allow_application_logs = self
            .context()
            .extra()
            .execution_runtime_config()
            .allow_application_logs;
        let runtime_resource_controller = ResourceController::new(
            resource_controller.policy().clone(),
            ResourceTracker::default(),
            Amount::ZERO,
        );
        let task = self
            .context()
            .extra()
            .thread_pool()
            .run_send((), move |()| async move {
                let runtime = ContractSyncRuntime::new_for_block(
                    execution_state_sender,
                    chain_id,
                    height,
                    round,
                    timestamp,
                    runtime_resource_controller,
                    allow_application_logs,
                );
                runtime.run_block_loop(&command_rx)
            })
            .await;

        let action_result = {
            let mut actor = ExecutionStateActor::with_runtime(
                self,
                txn_tracker,
                resource_controller,
                RuntimeChannels {
                    command_tx: &command_tx,
                    execution_state_receiver: &mut execution_state_receiver,
                },
            );
            f(&mut actor).await
        };

        // Always send `FinalizeAll` so the runtime task terminates, even if `f` failed.
        let initial_balance = match resource_controller.with_state(&mut self.system).await {
            Ok(guard) => guard.balance().unwrap_or(Amount::ZERO),
            Err(_) => Amount::ZERO,
        };
        let pre_finalize_tracker = resource_controller.tracker;
        // Errors here mean the runtime task already exited; we'll surface that
        // via `task.await` below.
        _ = command_tx.send(RuntimeCommand::FinalizeAll {
            context: finalize_context,
            tracker: Box::new(pre_finalize_tracker),
            initial_balance,
        });

        let drain_result = async {
            let mut drain_actor = ExecutionStateActor::new(self, txn_tracker, resource_controller);
            while let Some(request) = execution_state_receiver.next().await {
                if matches!(request, ExecutionRequest::ActionComplete { .. }) {
                    continue;
                }
                drain_actor.handle_request(request).await?;
            }
            Ok::<(), ExecutionError>(())
        }
        .await;

        let runtime_result = task.await;

        let action_value = action_result?;
        drain_result?;
        let runtime_controller =
            runtime_result.map_err(|_| ExecutionError::MissingRuntimeResponse)??;

        resource_controller.tracker = runtime_controller.tracker;
        resource_controller
            .with_state(&mut self.system)
            .await?
            .merge_balance(initial_balance, runtime_controller.account)?;

        Ok(action_value)
    }

    /// Executes an operation through a freshly spawned block-level runtime. Test-only.
    pub async fn execute_operation(
        &mut self,
        txn_tracker: &mut TransactionTracker,
        resource_controller: &mut ResourceController<Option<AccountOwner>>,
        context: OperationContext,
        operation: Operation,
    ) -> Result<(), ExecutionError> {
        let finalize_context = FinalizeContext {
            authenticated_owner: context.authenticated_owner,
            chain_id: context.chain_id,
            height: context.height,
            round: context.round,
        };
        Box::pin(self.with_block_runtime(
            txn_tracker,
            resource_controller,
            context.chain_id,
            context.height,
            context.round,
            context.timestamp,
            finalize_context,
            async move |actor| actor.execute_operation(context, operation).await,
        ))
        .await
    }

    /// Executes an operation through the snapshot-based per-action path. Test-only.
    ///
    /// Mirror of [`Self::execute_operation`] that drives the actor's
    /// `run_user_action_non_threaded_not_shared_memory` path: a fresh worker is
    /// spawned per action by the actor itself, with the supplied snapshot map
    /// passed in and returned (mutated) by the runtime. This lets equivalence
    /// tests exercise the snapshot path on native, side-by-side with the
    /// threaded one.
    pub async fn execute_operation_with_snapshots(
        &mut self,
        txn_tracker: &mut TransactionTracker,
        resource_controller: &mut ResourceController<Option<AccountOwner>>,
        block_snapshots: &mut BTreeMap<ApplicationId, Vec<u8>>,
        context: OperationContext,
        operation: Operation,
    ) -> Result<(), ExecutionError> {
        let mut actor = ExecutionStateActor::with_block_snapshots(
            self,
            txn_tracker,
            resource_controller,
            block_snapshots,
        );
        Box::pin(actor.execute_operation(context, operation)).await
    }

    /// Executes a message through a freshly spawned block-level runtime. Test-only.
    pub async fn execute_message(
        &mut self,
        txn_tracker: &mut TransactionTracker,
        resource_controller: &mut ResourceController<Option<AccountOwner>>,
        context: MessageContext,
        message: Message,
        grant: Option<&mut Amount>,
    ) -> Result<(), ExecutionError> {
        let finalize_context = FinalizeContext {
            authenticated_owner: context.authenticated_owner,
            chain_id: context.chain_id,
            height: context.height,
            round: context.round,
        };
        Box::pin(self.with_block_runtime(
            txn_tracker,
            resource_controller,
            context.chain_id,
            context.height,
            context.round,
            context.timestamp,
            finalize_context,
            async move |actor| actor.execute_message(context, message, grant).await,
        ))
        .await
    }
}

#[cfg(with_testing)]
impl ExecutionStateView<MemoryContext<TestExecutionRuntimeContext>>
where
    MemoryContext<TestExecutionRuntimeContext>: Context + Clone + 'static,
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
            authenticated_owner: None,
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
            .pin()
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
        let finalize_context = FinalizeContext {
            authenticated_owner: context.authenticated_owner,
            chain_id: context.chain_id,
            height: context.height,
            round: context.round,
        };
        let refund_grant_to = context.authenticated_owner.map(|owner| Account {
            chain_id: context.chain_id,
            owner,
        });
        Box::pin(self.with_block_runtime(
            &mut txn_tracker,
            &mut resource_controller,
            context.chain_id,
            context.height,
            context.round,
            context.timestamp,
            finalize_context,
            async move |actor| {
                actor
                    .run_user_action(application_id, action, refund_grant_to, None)
                    .await
            },
        ))
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
            UserAction::Instantiate(context, _) => context.authenticated_owner,
            UserAction::Operation(context, _) => context.authenticated_owner,
            UserAction::ProcessStreams(_, _) => None,
            UserAction::Message(context, _) => context.authenticated_owner,
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

    pub(crate) fn timestamp(&self) -> linera_base::data_types::Timestamp {
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
    C: Context + Clone + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    pub async fn query_application(
        &mut self,
        context: QueryContext,
        query: Query,
        endpoint: Option<&mut ServiceRuntimeEndpoint>,
    ) -> Result<QueryOutcome, ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        match query {
            Query::System(query) => {
                let outcome = self.system.handle_query(context, query);
                Ok(outcome.into())
            }
            Query::User {
                application_id,
                bytes,
            } => {
                let outcome = match endpoint {
                    Some(endpoint) => {
                        self.query_user_application_with_long_lived_service(
                            application_id,
                            context,
                            bytes,
                            &mut endpoint.incoming_execution_requests,
                            &endpoint.runtime_request_sender,
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
        self.query_user_application_with_deadline(
            application_id,
            context,
            query,
            None,
            BTreeMap::new(),
        )
        .await
    }

    pub(crate) async fn query_user_application_with_deadline(
        &mut self,
        application_id: ApplicationId,
        context: QueryContext,
        query: Vec<u8>,
        deadline: Option<Instant>,
        created_blobs: BTreeMap<BlobId, BlobContent>,
    ) -> Result<QueryOutcome<Vec<u8>>, ExecutionError> {
        let (execution_state_sender, mut execution_state_receiver) =
            futures::channel::mpsc::unbounded();
        let mut txn_tracker = TransactionTracker::default().with_blobs(created_blobs);
        let mut resource_controller = ResourceController::default();
        let thread_pool = self.context().extra().thread_pool().clone();
        let mut actor = ExecutionStateActor::new(self, &mut txn_tracker, &mut resource_controller);

        let (codes, descriptions) = actor.service_and_dependencies(application_id).await?;

        let service_runtime_task = thread_pool
            .run_send(JsVec(codes), move |codes| async move {
                let mut runtime = ServiceSyncRuntime::new_with_deadline(
                    execution_state_sender,
                    context,
                    deadline,
                );

                for (code, description) in codes.0.into_iter().zip(descriptions) {
                    runtime.preload_service(ApplicationId::from(&description), code, description);
                }

                runtime.run_query(application_id, query)
            })
            .await;

        while let Some(request) = execution_state_receiver.next().await {
            actor.handle_request(request).await?;
        }

        service_runtime_task.await?
    }

    async fn query_user_application_with_long_lived_service(
        &mut self,
        application_id: ApplicationId,
        context: QueryContext,
        query: Vec<u8>,
        incoming_execution_requests: &mut futures::channel::mpsc::UnboundedReceiver<
            ExecutionRequest,
        >,
        runtime_request_sender: &std::sync::mpsc::Sender<ServiceRuntimeRequest>,
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

        let mut txn_tracker = TransactionTracker::default();
        let mut resource_controller = ResourceController::default();
        let mut actor = ExecutionStateActor::new(self, &mut txn_tracker, &mut resource_controller);

        loop {
            futures::select! {
                maybe_request = incoming_execution_requests.next() => {
                    if let Some(request) = maybe_request {
                        actor.handle_request(request).await?;
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
}

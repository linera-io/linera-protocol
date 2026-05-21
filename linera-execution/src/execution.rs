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
    data_types::{Blob, BlobContent, BlockHeight, OracleResponse, StreamUpdate},
    ensure,
    identifiers::{AccountOwner, BlobId, BlobType, ChainId, StreamId},
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
        ResourceControlPolicy, ResourceTracker, TestExecutionRuntimeContext, UserContractCode,
    },
    linera_views::context::MemoryContext,
    std::sync::Arc,
};

use super::{execution_state_actor::ExecutionRequest, runtime::ServiceRuntimeRequest};
use crate::{
    execution_state_actor::ExecutionStateActor, resources::ResourceController,
    system::SystemExecutionStateView, ApplicationDescription, ApplicationId, ExecutionError,
    ExecutionRuntimeContext, JsVec, MessageContext, OperationContext, ProcessStreamsContext, Query,
    QueryContext, QueryOutcome, ServiceSyncRuntime, Timestamp, TransactionTracker,
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

    /// Validates the execution-state-level preconditions for a `SystemOperation::Checkpoint`
    /// and dumps the inner view's persisted content as one or more [`Blob`]s. The dump is
    /// split into chunks of at most `maximum_blob_size` bytes (from the current epoch's
    /// resource policy) so each chunk respects the per-blob size limit even for very
    /// large states. The blobs are not yet published; the caller is expected to register
    /// them during transaction execution via [`Self::apply_checkpoint`].
    ///
    /// This is a *pre-block* operation: it must run before the block-level setup mutates
    /// the chain state (e.g. setting `system.timestamp`), because `dump_content` reads
    /// from storage and refuses to run with pending in-memory changes. Splitting the
    /// dump out of the operation handler also guarantees the captured bytes represent
    /// the chain's pre-block state, which is exactly what a bootstrapping node will
    /// `restore_from_content` from before re-applying the certified checkpoint block.
    pub async fn prepare_checkpoint(
        &mut self,
        maximum_blob_size: u64,
    ) -> Result<Vec<Blob>, ExecutionError> {
        let mut had_event_block = false;
        self.previous_event_blocks
            .for_each_index_while(|_| {
                had_event_block = true;
                Ok(false)
            })
            .await?;
        ensure!(
            !had_event_block,
            ExecutionError::CheckpointPreconditionFailed("chain has published events")
        );

        let mut had_message_block = false;
        self.previous_message_blocks
            .for_each_index_while(|_| {
                had_message_block = true;
                Ok(false)
            })
            .await?;
        ensure!(
            !had_message_block,
            ExecutionError::CheckpointPreconditionFailed("chain has sent cross-chain messages")
        );

        let (bytes, _content_hash) = self.inner.dump_content().await?;
        let chunk_size = usize::try_from(maximum_blob_size).unwrap_or(usize::MAX);
        Ok(bytes
            .chunks(chunk_size)
            .map(|chunk| {
                Blob::new(BlobContent::new(
                    BlobType::CheckpointExecutionState,
                    chunk.to_vec(),
                ))
            })
            .collect())
    }

    /// Registers the checkpoint blobs (produced by [`Self::prepare_checkpoint`]) with the
    /// transaction tracker and records the matching [`OracleResponse::Checkpoint`].
    pub fn apply_checkpoint(
        &self,
        blobs: Vec<Blob>,
        txn_tracker: &mut TransactionTracker,
    ) -> Result<(), ExecutionError> {
        let execution_state_blobs = blobs.iter().map(|blob| blob.id().hash).collect();
        for blob in blobs {
            txn_tracker.add_created_blob(blob);
        }
        txn_tracker.replay_oracle_response(OracleResponse::Checkpoint {
            execution_state_blobs,
        })?;
        Ok(())
    }

    /// Convenience helper that combines [`Self::prepare_checkpoint`] and
    /// [`Self::apply_checkpoint`] in a single call. Production block-execution code
    /// invokes the two halves separately so the dump runs before any block-level state
    /// mutation; this helper is only used by unit tests that exercise the operation in
    /// isolation.
    #[cfg(test)]
    pub async fn execute_checkpoint(
        &mut self,
        maximum_blob_size: u64,
        txn_tracker: &mut TransactionTracker,
    ) -> Result<(), ExecutionError> {
        let blobs = self.prepare_checkpoint(maximum_blob_size).await?;
        self.apply_checkpoint(blobs, txn_tracker)
    }

    /// Replaces the persisted execution state with the content of a checkpoint blob,
    /// recording the hash of the bytes as the new stored hash. The caller is
    /// contractually obliged to reload the view after this returns.
    pub async fn restore_from_content(&mut self, bytes: &[u8]) -> Result<(), ViewError> {
        self.inner.restore_from_content(bytes).await?;
        Ok(())
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
        Box::pin(
            ExecutionStateActor::new(self, &mut txn_tracker, &mut resource_controller)
                .run_user_action(application_id, action, context.refund_grant_to(), None),
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

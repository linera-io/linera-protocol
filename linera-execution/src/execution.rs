// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, vec};

use allocative::Allocative;
use futures::{FutureExt, StreamExt};
use linera_base::{
    crypto::CryptoHash,
    data_types::{BlobContent, BlockHeight, StreamUpdate},
    identifiers::{AccountOwner, BlobId, StreamId},
    time::Instant,
};
use linera_views::{
    batch::Batch,
    common::HasherOutput,
    context::Context,
    key_value_store_view::KeyValueStoreView,
    map_view::MapView,
    reentrant_collection_view::HashedReentrantCollectionView,
    register_view::RegisterView,
    views::{ClonableView, HashableView, Hasher as _, ReplaceContext, View},
    ViewError,
};
#[cfg(with_testing)]
use {
    crate::{
        ResourceControlPolicy, ResourceTracker, TestExecutionRuntimeContext, UserContractCode,
    },
    linera_base::data_types::Blob,
    linera_views::context::MemoryContext,
    std::sync::Arc,
};

use super::{execution_state_actor::ExecutionRequest, runtime::ServiceRuntimeRequest};
use crate::{
    execution_state_actor::ExecutionStateActor, resources::ResourceController,
    system::SystemExecutionStateView, ApplicationDescription, ApplicationId, BcsHashable,
    Deserialize, ExecutionError, ExecutionRuntimeContext, JsVec, MessageContext, OperationContext,
    ProcessStreamsContext, Query, QueryContext, QueryOutcome, Serialize, ServiceSyncRuntime,
    Timestamp, TransactionTracker, FLAG_HISTORICAL_HASH, FLAG_HISTORICAL_HASH_SHADOW,
    FLAG_ZERO_HASH,
};

/// A view accessing the execution state of a chain.
#[derive(Debug, ClonableView, View, Allocative)]
#[allocative(bound = "C")]
pub struct ExecutionStateView<C> {
    /// System application.
    pub system: SystemExecutionStateView<C>,
    /// User applications.
    pub users: HashedReentrantCollectionView<C, ApplicationId, KeyValueStoreView<C>>,
    /// The number of events in the streams that this chain is writing to.
    pub stream_event_counts: MapView<C, StreamId, u32>,
    /// Rolling *historical* hash of the execution state, used when `FLAG_HISTORICAL_HASH`
    /// (or its shadow variant) is active. `None` until the first block after activation, which
    /// seeds it from the full content hash (via [`HashableView`]); subsequent blocks extend it
    /// cheaply from the batch that block persists. Appended last so that existing chains load it
    /// as `None` without moving the storage keys of the other fields, and deliberately excluded
    /// from the hand-written [`HashableView`] impl below (so it never hashes itself).
    #[allocative(skip)]
    pub historical_hash: RegisterView<C, Option<HasherOutput>>,
}

/// Selects how [`ExecutionStateView::crypto_hash_mut`] computes the execution-state hash,
/// based on the feature flags in the current committee's policy.
#[derive(Clone, Copy, Debug)]
enum HashingMode {
    /// Report an all-zeros hash without hashing the state (`FLAG_ZERO_HASH`).
    Zero,
    /// Full content hash of the state on every block (the default, legacy behavior).
    Legacy,
    /// Rolling historical hash. In shadow mode (`enforce == false`) the hash is computed,
    /// persisted and logged but the reported hash stays all-zeros.
    Historical { enforce: bool },
}

/// Hashes only the content fields of [`ExecutionStateView`], mirroring the `HashableView`
/// derive but omitting the appended `historical_hash` register. Keeping this by hand means the
/// rolling hash never feeds back into the content hash that seeds it.
impl<C> HashableView for ExecutionStateView<C>
where
    C: Context + Clone + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    type Hasher = linera_views::sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<HasherOutput, ViewError> {
        use std::io::Write as _;
        let mut hasher = Self::Hasher::default();
        hasher.write_all(self.system.hash_mut().await?.as_ref())?;
        hasher.write_all(self.users.hash_mut().await?.as_ref())?;
        hasher.write_all(self.stream_event_counts.hash_mut().await?.as_ref())?;
        Ok(hasher.finalize())
    }

    async fn hash(&self) -> Result<HasherOutput, ViewError> {
        use std::io::Write as _;
        let mut hasher = Self::Hasher::default();
        hasher.write_all(self.system.hash().await?.as_ref())?;
        hasher.write_all(self.users.hash().await?.as_ref())?;
        hasher.write_all(self.stream_event_counts.hash().await?.as_ref())?;
        Ok(hasher.finalize())
    }
}

impl<C> ExecutionStateView<C>
where
    C: Context + Clone + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    pub async fn crypto_hash_mut(&mut self) -> Result<CryptoHash, ViewError> {
        match self.hashing_mode().await? {
            HashingMode::Zero => Ok(CryptoHash::from([0; 32])),
            HashingMode::Legacy => {
                let hash = self.hash_mut().await?;
                Ok(Self::wrap_state_hash(hash))
            }
            HashingMode::Historical { enforce } => {
                let hash = self.advance_historical_hash().await?;
                let wrapped = Self::wrap_state_hash(hash);
                if enforce {
                    Ok(wrapped)
                } else {
                    // Shadow mode: populate and surface the rolling hash so it can be compared
                    // across validators, but keep reporting an all-zeros hash so that it is not
                    // yet enforced in consensus.
                    tracing::info!(
                        chain_id = %self.context().extra().chain_id(),
                        historical_state_hash = %wrapped,
                        "computed historical execution-state hash in shadow mode",
                    );
                    Ok(CryptoHash::from([0; 32]))
                }
            }
        }
    }

    /// Decides which hashing scheme applies, based on the feature flags in the current
    /// committee's content policy. With no active committee, falls back to legacy hashing
    /// (matching the previous behavior).
    async fn hashing_mode(&self) -> Result<HashingMode, ViewError> {
        let Some((_epoch, committee)) = self.system.current_committee().await? else {
            return Ok(HashingMode::Legacy);
        };
        let flags = &committee.policy().http_request_allow_list;
        let mode = if flags.contains(FLAG_ZERO_HASH) {
            HashingMode::Zero
        } else if flags.contains(FLAG_HISTORICAL_HASH) {
            HashingMode::Historical { enforce: true }
        } else if flags.contains(FLAG_HISTORICAL_HASH_SHADOW) {
            HashingMode::Historical { enforce: false }
        } else {
            HashingMode::Legacy
        };
        Ok(mode)
    }

    /// Computes the next rolling historical hash and stores it in `historical_hash`.
    ///
    /// On the first block after activation (`historical_hash` is `None`) the hash is seeded from
    /// a full content hash of the state, which — because [`HashableView::hash_mut`] reflects the
    /// in-memory state — already includes this block's changes. Every subsequent block extends
    /// the stored hash with the batch it is about to persist, so the cost is proportional to the
    /// block's writes rather than the whole state.
    async fn advance_historical_hash(&mut self) -> Result<HasherOutput, ViewError> {
        let hash = match *self.historical_hash.get() {
            None => self.hash_mut().await?,
            Some(stored) => {
                // Hash only the content fields' pending writes. The `historical_hash` register is
                // intentionally excluded so the rolling hash never depends on its own value (it
                // may still be a pending, unsaved write at this point). This mirrors `main`, where
                // the wrapper's hash key sits outside the hashed inner view.
                let mut batch = Batch::new();
                self.system.pre_save(&mut batch)?;
                self.users.pre_save(&mut batch)?;
                self.stream_event_counts.pre_save(&mut batch)?;
                Self::make_hash(Some(stored), &batch)?
            }
        };
        self.historical_hash.set(Some(hash));
        Ok(hash)
    }

    /// Extends a stored hash with a batch: `SHA3-256(stored || bcs(batch))`. An empty batch
    /// leaves the hash unchanged. Mirrors `HistoricallyHashableView::make_hash`.
    fn make_hash(stored: Option<HasherOutput>, batch: &Batch) -> Result<HasherOutput, ViewError> {
        let stored = stored.unwrap_or_default();
        if batch.is_empty() {
            return Ok(stored);
        }
        let mut hasher = linera_views::sha3::Sha3_256::default();
        hasher.update_with_bytes(stored.as_ref())?;
        hasher.update_with_bcs_bytes(&batch)?;
        Ok(hasher.finalize())
    }

    /// Wraps a raw 32-byte hash into a domain-separated [`CryptoHash`], matching the convention
    /// used on `main` for the execution-state hash.
    fn wrap_state_hash(hash: HasherOutput) -> CryptoHash {
        #[derive(Serialize, Deserialize)]
        struct ExecutionStateViewHash([u8; 32]);
        impl BcsHashable<'_> for ExecutionStateViewHash {}
        CryptoHash::new(&ExecutionStateViewHash(hash.into()))
    }
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
            historical_hash: self.historical_hash.with_context(ctx.clone()).await,
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
                let outcome = self.system.handle_query(context, query)?;
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

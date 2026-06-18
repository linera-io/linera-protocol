// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The state and functionality of a chain worker.

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::{self, Arc},
};

use futures::future::Either;
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use linera_base::{
    crypto::{CryptoHash, ValidatorPublicKey},
    data_types::{
        ApplicationDescription, ArithmeticError, Blob, BlockHeight, Epoch, OracleResponse, Round,
        Timestamp,
    },
    ensure,
    hashed::Hashed,
    identifiers::{AccountOwner, ApplicationId, BlobId, BlobType, ChainId, EventId, StreamId},
};
use linera_cache::{Arc as CacheArc, UniqueValueCache, ValueCache};
use linera_chain::{
    data_types::{
        BlockProposal, BundleExecutionPolicy, IncomingBundle, MessageAction, MessageBundle,
        OriginalProposal, ProposalContent, ProposedBlock,
    },
    manager::{self, ManagerSafetySnapshot},
    types::{
        Block, ConfirmedBlock, ConfirmedBlockCertificate, TimeoutCertificate,
        ValidatedBlockCertificate,
    },
    ChainError, ChainExecutionContext, ChainIdSet, ChainStateView, ChainTipState,
    ExecutionResultExt as _,
};
use linera_execution::{
    system::{EpochEventData, EventSubscriptions, EPOCH_STREAM_NAME},
    ExecutionRuntimeContext as _, ExecutionStateView, Query, QueryContext, QueryOutcome,
    ResourceTracker, ServiceRuntimeEndpoint,
};
use linera_storage::{Clock as _, Storage};
use linera_views::{
    batch::Batch,
    context::{Context, InactiveContext},
    store::WritableKeyValueStore as _,
    views::{ReplaceContext as _, RootView as _, View as _},
};
use tokio::sync::oneshot;
use tracing::{debug, instrument, trace, warn};

use crate::{
    chain_worker::{handle::AtomicTimestamp, ChainWorkerConfig, DeliveryNotifier},
    client::{ChainModes, ListeningMode},
    data_types::{ChainInfo, ChainInfoQuery, ChainInfoResponse, CrossChainRequest},
    worker::{BatchRequest, NetworkActions, Notification, Reason, WorkerError},
};

/// Type alias for event subscriptions result.
pub(crate) type EventSubscriptionsResult = Vec<((ChainId, StreamId), EventSubscriptions)>;

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{
        exponential_bucket_interval, exponential_bucket_latencies, register_histogram,
        register_histogram_vec,
    };
    use prometheus::{Histogram, HistogramVec};

    pub static CREATE_NETWORK_ACTIONS_LATENCY: LazyLock<Histogram> = LazyLock::new(|| {
        register_histogram(
            "create_network_actions_latency",
            "Time (ms) to create network actions",
            exponential_bucket_latencies(10_000.0),
        )
    });

    pub static NUM_INBOXES: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "num_inboxes",
            "Number of inboxes",
            &[],
            exponential_bucket_interval(1.0, 10_000.0),
        )
    });
}

/// The state of the chain worker.
pub(crate) struct ChainWorkerState<StorageClient>
where
    StorageClient: Storage,
{
    config: ChainWorkerConfig,
    storage: StorageClient,
    chain: ChainStateView<StorageClient::Context>,
    service_runtime_endpoint: Option<ServiceRuntimeEndpoint>,
    /// The background task running the service runtime. Must be kept alive for the
    /// lifetime of the worker: the pool `Guard` wrapper returns the thread-pool slot
    /// when dropped, so dropping this early lets the pool schedule unrelated work on a
    /// thread that is still running the service runtime.
    service_runtime_task: Option<web_thread_pool::Task<()>>,
    /// Timestamp of the last access.
    /// Used by the keep-alive task to determine when the worker has been idle.
    /// Wrapped in `Arc` so the keep-alive task can read it without acquiring
    /// the `RwLock`.
    last_access: Arc<AtomicTimestamp>,
    block_values: Arc<ValueCache<CryptoHash, ConfirmedBlock>>,
    execution_state_cache:
        Option<Arc<UniqueValueCache<CryptoHash, ExecutionStateView<InactiveContext>>>>,
    chain_modes: Option<Arc<sync::RwLock<ChainModes>>>,
    delivery_notifier: DeliveryNotifier,
    knows_chain_is_active: bool,
    /// Set to `true` if a database `save` failure has left storage potentially
    /// inconsistent.
    poisoned: bool,
}

/// The result of processing a cross-chain update.
pub(crate) enum CrossChainUpdateResult {
    /// The update was applied up to the given height. The caller must save.
    Updated(BlockHeight),
    /// All bundles were already received; nothing to do.
    NothingToDo,
    /// A gap was detected in the inbox for messages from `origin`. If
    /// `allow_revert_confirm` is enabled, a `RevertConfirm` request should be sent
    /// to retransmit bundles starting from `retransmit_from`.
    GapDetected {
        origin: ChainId,
        retransmit_from: BlockHeight,
    },
}

/// Whether the block was processed or skipped. Used for metrics.
pub enum BlockOutcome {
    Processed,
    Preprocessed,
    Skipped,
}

/// How to handle a confirmed block.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessConfirmedBlockMode {
    /// Execute the block if it is contiguous (or bridgeable via a checkpoint);
    /// otherwise preprocess it. Used by validators and by any caller that
    /// wants graceful fallback when there's a gap.
    Auto,
    /// Execute the block. Fail with [`WorkerError::InvalidBlockChaining`] if
    /// there's a gap that can't be bridged. Use when the caller knows the
    /// block must be contiguous and wants a hard error otherwise.
    Execute,
    /// Only preprocess the block, never execute it — even if it would be
    /// contiguous. Used by the client for sender-chain blocks it is not
    /// otherwise tracking, since their execution state is irrelevant.
    Preprocess,
}

impl<StorageClient> ChainWorkerState<StorageClient>
where
    StorageClient: Storage + Clone + 'static,
{
    /// Creates a new [`ChainWorkerState`] using the provided `storage` client.
    #[instrument(skip_all, fields(
        chain_id = %chain_id
    ))]
    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn load(
        config: ChainWorkerConfig,
        storage: StorageClient,
        block_values: Arc<ValueCache<CryptoHash, ConfirmedBlock>>,
        execution_state_cache: Option<
            Arc<UniqueValueCache<CryptoHash, ExecutionStateView<InactiveContext>>>,
        >,
        chain_modes: Option<Arc<sync::RwLock<ChainModes>>>,
        delivery_notifier: DeliveryNotifier,
        chain_id: ChainId,
        service_runtime_endpoint: Option<ServiceRuntimeEndpoint>,
        service_runtime_task: Option<web_thread_pool::Task<()>>,
    ) -> Result<Self, WorkerError> {
        let chain = storage.load_chain(chain_id).await?;

        Ok(ChainWorkerState {
            config,
            storage,
            chain,
            service_runtime_endpoint,
            service_runtime_task,
            last_access: Arc::new(AtomicTimestamp::now()),
            block_values,
            execution_state_cache,
            chain_modes,
            delivery_notifier,
            knows_chain_is_active: false,
            poisoned: false,
        })
    }

    /// Returns the [`ChainId`] of the chain handled by this worker.
    fn chain_id(&self) -> ChainId {
        self.chain.chain_id()
    }

    /// Returns a reference to the chain state view.
    pub(crate) fn chain(&self) -> &ChainStateView<StorageClient::Context> {
        &self.chain
    }

    /// Resolves the committee that signed certificates at the given epoch by
    /// walking the admin chain's epoch event stream — works even after the
    /// epoch has been revoked, so long as the admin chain still has the event.
    /// Surfaces `EventsNotFound` (as a chained `ChainError::ExecutionError`)
    /// when the admin event isn't local, so the caller's retry/sync paths can
    /// fetch it before re-asking.
    async fn committee_for_epoch(
        &self,
        epoch: Epoch,
    ) -> Result<linera_execution::committee::Committee, WorkerError> {
        let hash = self
            .chain
            .execution_state
            .context()
            .extra()
            .get_committee_hashes(epoch..=epoch)
            .await
            .map_err(|error| {
                ChainError::ExecutionError(Box::new(error), ChainExecutionContext::Block)
            })?
            .remove(&epoch)
            .ok_or_else(|| {
                ChainError::InternalError(format!(
                    "missing committee for epoch {epoch}; this is a bug"
                ))
            })?;
        let committee = self
            .chain
            .execution_state
            .context()
            .extra()
            .get_or_load_committee_by_hash(hash)
            .await
            .map_err(|error| {
                ChainError::ExecutionError(Box::new(error), ChainExecutionContext::Block)
            })?;
        Ok((*committee).clone())
    }

    /// Filters bundles destined for this chain to drop ones already received and to refuse
    /// ones whose epoch has been revoked on the admin chain.
    ///
    /// A revoked-epoch bundle is still accepted if (a) it has already been executed by
    /// anticipation (`bundle.height <= last_anticipated_block_height`), or (b) a later
    /// bundle in the same batch is in a still-trusted epoch — that bundle's certificate
    /// transitively re-certifies all preceding ones via prev-hash chaining.
    pub(crate) async fn select_message_bundles(
        &self,
        origin: &ChainId,
        next_height_to_receive: BlockHeight,
        last_anticipated_block_height: Option<BlockHeight>,
        mut bundles: Vec<(Epoch, MessageBundle)>,
    ) -> Result<Vec<MessageBundle>, WorkerError> {
        let recipient = self.chain_id();
        let mut latest_height = None;
        let mut skipped_len = 0;
        let mut trusted_len = 0;
        for (i, (epoch, bundle)) in bundles.iter().enumerate() {
            ensure!(
                latest_height <= Some(bundle.height),
                WorkerError::InvalidCrossChainRequest
            );
            latest_height = Some(bundle.height);
            if bundle.height < next_height_to_receive {
                skipped_len = i + 1;
            }
            let is_revoked = self
                .storage
                .is_epoch_revoked(*epoch)
                .await
                .map_err(|error| {
                    WorkerError::ChainError(Box::new(ChainError::ExecutionError(
                        Box::new(error),
                        ChainExecutionContext::Block,
                    )))
                })?;
            if !is_revoked || Some(bundle.height) <= last_anticipated_block_height {
                trusted_len = i + 1;
            }
        }
        if skipped_len > 0 {
            let (_, sample_bundle) = &bundles[skipped_len - 1];
            debug!(
                "Ignoring repeated messages to {recipient:.8} from {origin:} at height {}",
                sample_bundle.height,
            );
        }
        if skipped_len < bundles.len() && trusted_len < bundles.len() {
            let (sample_epoch, sample_bundle) = &bundles[trusted_len];
            warn!(
                "Refusing messages to {recipient:.8} from {origin:} at height {} \
                 because the epoch {} is not trusted any more",
                sample_bundle.height, sample_epoch,
            );
        }
        Ok(if skipped_len < trusted_len {
            bundles
                .drain(skipped_len..trusted_len)
                .map(|(_, bundle)| bundle)
                .collect()
        } else {
            vec![]
        })
    }

    /// Returns whether this chain is known to be active (initialized).
    pub(crate) fn knows_chain_is_active(&self) -> bool {
        self.knows_chain_is_active
    }

    /// Rolls back any uncommitted changes to the chain state.
    pub(crate) fn rollback(&mut self) {
        self.chain.rollback();
    }

    /// Returns `WorkerError::PoisonedWorker` if the worker is poisoned due to a database
    /// `save` failure.
    pub(crate) fn check_not_poisoned(&self) -> Result<(), WorkerError> {
        ensure!(!self.poisoned, WorkerError::PoisonedWorker);
        Ok(())
    }

    /// Updates the last-access timestamp to the current time.
    pub(crate) fn touch(&self) {
        self.last_access.store_now();
    }

    /// Returns a clone of the last-access `Arc`, for use by the keep-alive task.
    pub(crate) fn last_access_arc(&self) -> Arc<AtomicTimestamp> {
        Arc::clone(&self.last_access)
    }

    /// Drops the service runtime endpoint, signaling the runtime task to stop.
    /// Returns the runtime task so the caller can await it outside the lock.
    pub(crate) fn clear_service_runtime(&mut self) -> Option<web_thread_pool::Task<()>> {
        self.service_runtime_endpoint.take();
        self.service_runtime_task.take()
    }

    /// Returns the pending cross-chain network actions if the outbox index is already
    /// reconciled to the current tracked set, or `None` if it must first be reconciled (which
    /// needs a write lock).
    pub(crate) async fn cross_chain_network_actions_if_reconciled(
        &self,
    ) -> Result<Option<NetworkActions>, WorkerError> {
        let tracked = self.tracked_full_chains();
        if !self.chain.outbox_index_is_reconciled(tracked.as_deref()) {
            return Ok(None);
        }
        Ok(Some(
            self.build_network_actions(None, tracked.as_deref().map(|h| h.inner()))
                .await?,
        ))
    }

    /// Reconciles the outbox index with the current tracked set, then returns the pending
    /// cross-chain network actions for this chain, without initializing the chain's execution
    /// state. Intended for callers that only need to re-emit cross-chain requests from the
    /// outbox of a sender chain whose `ChainDescription` we may never have needed.
    ///
    /// This is the slow path of [`Self::cross_chain_network_actions_if_reconciled`].
    #[instrument(skip_all, fields(chain_id = %self.chain_id()))]
    pub(crate) async fn reconcile_and_cross_chain_network_actions(
        &mut self,
    ) -> Result<NetworkActions, WorkerError> {
        let tracked = self.tracked_full_chains();
        self.chain
            .reconcile_outbox_index(tracked.as_deref())
            .await?;
        let actions = self
            .build_network_actions(None, tracked.as_deref().map(|h| h.inner()))
            .await?;
        self.save().await?;
        Ok(actions)
    }

    /// Handles a [`ChainInfoQuery`], potentially voting on the next block.
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, WorkerError> {
        if let Some((height, round)) = query.request_leader_timeout {
            self.vote_for_leader_timeout(height, round).await?;
        }
        if query.request_fallback {
            self.vote_for_fallback().await?;
        }
        self.prepare_chain_info_response(query).await
    }

    /// Returns the requested blob, if it belongs to the current locking block or pending proposal.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        blob_id = %blob_id
    ))]
    pub(crate) async fn download_pending_blob(
        &self,
        blob_id: BlobId,
    ) -> Result<CacheArc<Blob>, WorkerError> {
        if let Some(blob) = self.chain.manager.pending_blob(&blob_id).await? {
            return Ok(self.storage.cache_blob(blob));
        }
        self.storage
            .read_blob(blob_id)
            .await?
            .ok_or(WorkerError::BlobsNotFound(vec![blob_id]))
    }

    /// Reads the blobs from the chain manager or from storage. Returns an error if any are
    /// missing.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id()
    ))]
    async fn get_required_blobs(
        &self,
        required_blob_ids: impl IntoIterator<Item = BlobId>,
        created_blobs: BTreeMap<BlobId, Blob>,
    ) -> Result<BTreeMap<BlobId, Blob>, WorkerError> {
        let maybe_blobs = self
            .maybe_get_required_blobs(required_blob_ids, Some(created_blobs))
            .await?;
        let not_found_blob_ids = missing_blob_ids(&maybe_blobs);
        ensure!(
            not_found_blob_ids.is_empty(),
            WorkerError::BlobsNotFound(not_found_blob_ids)
        );
        Ok(maybe_blobs
            .into_iter()
            .filter_map(|(blob_id, maybe_blob)| Some((blob_id, maybe_blob?)))
            .collect())
    }

    /// Tries to read the blobs from the chain manager or storage. Returns `None` if not found.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id()
    ))]
    async fn maybe_get_required_blobs(
        &self,
        blob_ids: impl IntoIterator<Item = BlobId>,
        mut created_blobs: Option<BTreeMap<BlobId, Blob>>,
    ) -> Result<BTreeMap<BlobId, Option<Blob>>, WorkerError> {
        let maybe_blobs = blob_ids.into_iter().collect::<BTreeSet<_>>();
        let mut maybe_blobs = maybe_blobs
            .into_iter()
            .map(|x| (x, None))
            .collect::<Vec<(BlobId, Option<Blob>)>>();

        if let Some(blob_map) = &mut created_blobs {
            for (blob_id, value) in &mut maybe_blobs {
                if let Some(blob) = blob_map.remove(blob_id) {
                    *value = Some(blob);
                }
            }
        }

        let (missing_indices, missing_blob_ids) = missing_indices_blob_ids(&maybe_blobs);
        let second_block_blobs = self.chain.manager.pending_blobs(&missing_blob_ids).await?;
        for (index, blob) in missing_indices.into_iter().zip(second_block_blobs) {
            maybe_blobs[index].1 = blob;
        }

        let (missing_indices, missing_blob_ids) = missing_indices_blob_ids(&maybe_blobs);
        let third_block_blobs = self
            .chain
            .pending_validated_blobs
            .multi_get(&missing_blob_ids)
            .await?;
        for (index, blob) in missing_indices.into_iter().zip(third_block_blobs) {
            maybe_blobs[index].1 = blob;
        }

        let (missing_indices, missing_blob_ids) = missing_indices_blob_ids(&maybe_blobs);
        if !missing_indices.is_empty() {
            let all_entries_pending_blobs = self
                .chain
                .pending_proposed_blobs
                .try_load_all_entries()
                .await?;
            for (index, blob_id) in missing_indices.into_iter().zip(missing_blob_ids) {
                for (_, pending_blobs) in &all_entries_pending_blobs {
                    if let Some(blob) = pending_blobs.get(&blob_id).await? {
                        maybe_blobs[index].1 = Some(blob);
                        break;
                    }
                }
            }
        }

        let (missing_indices, missing_blob_ids) = missing_indices_blob_ids(&maybe_blobs);
        let fourth_block_blobs = self.storage.read_blobs(&missing_blob_ids).await?;
        for (index, blob) in missing_indices.into_iter().zip(fourth_block_blobs) {
            maybe_blobs[index].1 = blob.map(CacheArc::unwrap_or_clone);
        }
        Ok(maybe_blobs.into_iter().collect())
    }

    /// Creates cross-chain requests for a single recipient from its outbox.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id()
    ))]
    async fn create_cross_chain_actions_for_recipient(
        &self,
        recipient: ChainId,
    ) -> Result<NetworkActions, WorkerError> {
        let outbox = self.chain.outboxes.try_load_entry(&recipient).await?;
        let Some(outbox) = outbox else {
            return Ok(NetworkActions::default());
        };
        let heights = outbox.queue.elements().await?;
        if heights.is_empty() {
            return Ok(NetworkActions::default());
        }
        let heights_by_recipient = BTreeMap::from([(recipient, heights)]);
        let cross_chain_requests = self
            .create_cross_chain_requests(heights_by_recipient)
            .await?;
        Ok(NetworkActions {
            cross_chain_requests,
            notifications: Vec::new(),
        })
    }

    /// Returns the set of chains tracked in full mode together with its memoized hash, or `None` if
    /// every chain is implicitely in full-mode (e.g. on validator).
    fn tracked_full_chains(&self) -> Option<Arc<Hashed<ChainIdSet>>> {
        let chain_modes = self.chain_modes.as_ref()?;
        let full = chain_modes
            .read()
            .expect("Panics should not happen while holding a lock to `chain_modes`")
            .full();
        Some(full)
    }

    /// Returns whether the given chain is tracked in full mode (always `true` on a validator),
    /// i.e. whether its outbox should be indexed.
    fn is_tracked(&self, chain_id: &ChainId) -> bool {
        self.chain_modes.as_ref().is_none_or(|chain_modes| {
            chain_modes
                .read()
                .expect("Panics should not happen while holding a lock to `chain_modes`")
                .get(chain_id)
                .is_some_and(ListeningMode::is_full)
        })
    }

    /// Reconciles the chain's `nonempty_outboxes` and `outbox_counters` indices with the current
    /// set of fully-tracked chains.
    async fn reconcile_tracked_outboxes(
        &mut self,
    ) -> Result<Option<Arc<Hashed<ChainIdSet>>>, WorkerError> {
        let full_chains = self.tracked_full_chains();
        self.chain
            .reconcile_outbox_index(full_chains.as_deref())
            .await?;
        Ok(full_chains)
    }

    /// Reconciles the outbox index with the current tracked set, then loads pending cross-chain
    /// requests and adds `NewRound` notifications where appropriate.
    async fn create_network_actions(
        &mut self,
        old_round: Option<Round>,
    ) -> Result<NetworkActions, WorkerError> {
        // Make the outbox index authoritative for the current tracked set first, so it already
        // holds only `is_full` targets and needs no read-time filtering.
        let tracked = self.reconcile_tracked_outboxes().await?;
        self.build_network_actions(old_round, tracked.as_deref().map(|h| h.inner()))
            .await
    }

    /// Builds the pending cross-chain actions from the already-reconciled outbox index.
    async fn build_network_actions(
        &self,
        old_round: Option<Round>,
        tracked: Option<&ChainIdSet>,
    ) -> Result<NetworkActions, WorkerError> {
        #[cfg(with_metrics)]
        let _latency = metrics::CREATE_NETWORK_ACTIONS_LATENCY.measure_latency();
        let mut heights_by_recipient = BTreeMap::<_, Vec<_>>::new();
        let targets = self.chain.nonempty_outbox_chain_ids();
        if let Some(tracked) = tracked {
            if let Some(target) = targets.iter().find(|target| !tracked.contains(*target)) {
                return Err(ChainError::CorruptedChainState(format!(
                    "outbox index contains untracked target {target}"
                ))
                .into());
            }
        }
        let outboxes = self.chain.load_outboxes(&targets).await?;
        for (target, outbox) in targets.into_iter().zip(outboxes) {
            let heights = outbox.queue.elements().await?;
            heights_by_recipient.insert(target, heights);
        }
        let cross_chain_requests = self
            .create_cross_chain_requests(heights_by_recipient)
            .await?;
        let mut notifications = Vec::new();
        if let Some(old_round) = old_round {
            let round = self.chain.manager.current_round();
            if round > old_round {
                let height = self.chain.tip_state.get().next_block_height;
                notifications.push(Notification {
                    chain_id: self.chain_id(),
                    reason: Reason::NewRound { height, round },
                });
            }
        }
        Ok(NetworkActions {
            cross_chain_requests,
            notifications,
        })
    }

    /// Returns confirmed blocks by hash, checking the cache first and batch-loading the rest
    /// from storage. The order of the returned blocks matches the order of the input hashes.
    async fn read_confirmed_blocks(
        &self,
        hashes: &[CryptoHash],
    ) -> Result<Vec<Option<CacheArc<ConfirmedBlock>>>, WorkerError> {
        let mut blocks = Vec::with_capacity(hashes.len());
        let mut uncached_indices = Vec::new();
        let mut uncached_hashes = Vec::new();

        for (i, hash) in hashes.iter().enumerate() {
            if let Some(block) = self.block_values.get(hash) {
                blocks.push(Some(block));
            } else {
                blocks.push(None);
                uncached_indices.push(i);
                uncached_hashes.push(*hash);
            }
        }

        if !uncached_hashes.is_empty() {
            let from_storage = self.storage.read_confirmed_blocks(uncached_hashes).await?;
            for (i, maybe_block) in uncached_indices.into_iter().zip(from_storage) {
                blocks[i] = maybe_block;
            }
        }

        Ok(blocks)
    }

    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        num_recipients = %heights_by_recipient.len()
    ))]
    async fn create_cross_chain_requests(
        &self,
        heights_by_recipient: BTreeMap<ChainId, Vec<BlockHeight>>,
    ) -> Result<Vec<CrossChainRequest>, WorkerError> {
        // Load all the certificates we will need, regardless of the medium.
        let heights = heights_by_recipient
            .values()
            .flatten()
            .copied()
            .collect::<BTreeSet<_>>();
        let hashes = self
            .chain
            .block_hashes_for_heights(heights.iter().copied())
            .await?;

        let blocks = self.read_confirmed_blocks(&hashes).await?;

        let mut height_to_blocks = HashMap::new();
        for (block, hash) in blocks.into_iter().zip(hashes) {
            let block = block.ok_or_else(|| WorkerError::ReadCertificatesError(vec![hash]))?;
            height_to_blocks.insert(block.height(), block);
        }

        let sender = self.chain.chain_id();
        let mut cross_chain_requests = Vec::new();
        for (recipient, heights) in heights_by_recipient {
            // Extract the predecessor height for this recipient from the first
            // block's `previous_message_blocks`. This lets the recipient detect
            // gaps even before it consumes the missing message.
            let previous_height = heights.first().and_then(|first_height| {
                let block = height_to_blocks.get(first_height)?;
                let (_, prev_height) =
                    block.block().body.previous_message_blocks.get(&recipient)?;
                Some(*prev_height)
            });
            let mut bundles = Vec::new();
            let mut bundles_size = 0;
            for height in heights {
                let Some(confirmed_block) = height_to_blocks.get(&height) else {
                    tracing::warn!(
                        %height,
                        %recipient,
                        "spurious entry in outbox; skipping this and higher sender blocks"
                    );
                    break;
                };
                let new_bundles = confirmed_block
                    .block()
                    .message_bundles_for(recipient, confirmed_block.inner().hash())
                    .collect::<Vec<_>>();
                let new_size = new_bundles
                    .iter()
                    .map(|(_epoch, bundle)| bundle.estimated_size())
                    .sum::<usize>();
                // If adding this block's bundles would exceed the chunk limit,
                // stop here. Always include at least one block's bundles.
                if bundles_size + new_size > self.config.cross_chain_message_chunk_limit {
                    if bundles.is_empty() {
                        warn!(
                            "Single block at height {height} produces an UpdateRecipient \
                            of ~{new_size} bytes, exceeding the chunk limit of {}",
                            self.config.cross_chain_message_chunk_limit
                        );
                    } else {
                        debug!(
                            "Stopping cross-chain batch for {recipient} at height {height}: \
                            adding ~{new_size} bytes would exceed chunk limit of {} \
                            (current batch ~{bundles_size} bytes)",
                            self.config.cross_chain_message_chunk_limit
                        );
                        break;
                    }
                }
                bundles.extend(new_bundles);
                bundles_size += new_size;
            }
            if !bundles.is_empty() {
                cross_chain_requests.push(CrossChainRequest::UpdateRecipient {
                    sender,
                    recipient,
                    bundles,
                    previous_height,
                });
            }
        }
        Ok(cross_chain_requests)
    }

    /// Processes a leader timeout issued for this multi-owner chain.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        height = %certificate.inner().height()
    ))]
    pub(crate) async fn process_timeout(
        &mut self,
        certificate: TimeoutCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        // Check that the chain is active and ready for this timeout.
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        self.initialize_and_save_if_needed().await?;
        let (chain_epoch, committee) = self.chain.current_committee().await?;
        certificate.check(&committee)?;
        if self
            .chain
            .tip_state
            .get()
            .already_validated_block(certificate.inner().height())?
        {
            return Ok((self.chain_info_response().await?, NetworkActions::default()));
        }
        ensure!(
            certificate.inner().epoch() == chain_epoch,
            WorkerError::InvalidEpoch {
                chain_id: certificate.inner().chain_id(),
                chain_epoch,
                epoch: certificate.inner().epoch()
            }
        );
        let old_round = self.chain.manager.current_round();
        self.chain
            .manager
            .handle_timeout_certificate(certificate, self.storage.clock().current_time());
        self.save().await?;
        let actions = self.create_network_actions(Some(old_round)).await?;
        Ok((self.chain_info_response().await?, actions))
    }

    /// Tries to load all blobs published in this proposal.
    ///
    /// If they cannot be found, it creates an entry in `pending_proposed_blobs` so they can be
    /// submitted one by one.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        block_height = %proposal.content.block.height
    ))]
    async fn load_proposal_blobs(
        &mut self,
        proposal: &BlockProposal,
    ) -> Result<Vec<Blob>, WorkerError> {
        let owner = proposal.owner();
        let BlockProposal {
            content:
                ProposalContent {
                    block,
                    round,
                    outcome: _,
                },
            original_proposal,
            signature: _,
        } = proposal;

        let mut maybe_blobs = self
            .maybe_get_required_blobs(proposal.required_blob_ids(), None)
            .await?;
        let missing_blob_ids = missing_blob_ids(&maybe_blobs);
        if !missing_blob_ids.is_empty() {
            let chain = &mut self.chain;
            if chain.ownership().await?.open_multi_leader_rounds {
                // TODO(#3203): Allow multiple pending proposals on permissionless chains.
                chain.pending_proposed_blobs.clear();
            }
            let validated = matches!(original_proposal, Some(OriginalProposal::Regular { .. }));
            chain
                .pending_proposed_blobs
                .try_load_entry_mut(&owner)
                .await?
                .update(*round, validated, maybe_blobs)?;
            self.save().await?;
            return Err(WorkerError::BlobsNotFound(missing_blob_ids));
        }
        let published_blobs = block
            .published_blob_ids()
            .iter()
            .filter_map(|blob_id| maybe_blobs.remove(blob_id).flatten())
            .collect::<Vec<_>>();
        Ok(published_blobs)
    }

    /// Processes a validated block issued for this multi-owner chain.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        block_height = %certificate.block().header.height
    ))]
    pub(crate) async fn process_validated_block(
        &mut self,
        certificate: ValidatedBlockCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions, BlockOutcome), WorkerError> {
        let block = certificate.block();

        let header = &block.header;
        let height = header.height;
        // Check that the chain is active and ready for this validated block.
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        self.initialize_and_save_if_needed().await?;
        let tip_state = self.chain.tip_state.get();
        ensure!(
            header.height == tip_state.next_block_height,
            ChainError::UnexpectedBlockHeight {
                expected_block_height: tip_state.next_block_height,
                found_block_height: header.height,
            }
        );
        let (epoch, committee) = self.chain.current_committee().await?;
        check_block_epoch(epoch, header.chain_id, header.epoch)?;
        certificate.check(&committee)?;
        let already_committed_block = self.chain.tip_state.get().already_validated_block(height)?;
        let should_skip_validated_block = || {
            self.chain
                .manager
                .check_validated_block(&certificate)
                .map(|outcome| outcome == manager::Outcome::Skip)
        };
        if already_committed_block || should_skip_validated_block()? {
            // If we just processed the same pending block, return the chain info unchanged.
            return Ok((
                self.chain_info_response().await?,
                NetworkActions::default(),
                BlockOutcome::Skipped,
            ));
        }

        self.block_values
            .insert_hashed(Cow::Borrowed(certificate.inner().inner()));
        let required_blob_ids = block.required_blob_ids();
        let maybe_blobs = self
            .maybe_get_required_blobs(required_blob_ids, Some(block.created_blobs()))
            .await?;
        let missing_blob_ids = missing_blob_ids(&maybe_blobs);
        if !missing_blob_ids.is_empty() {
            self.chain
                .pending_validated_blobs
                .update(certificate.round, true, maybe_blobs)?;
            self.save().await?;
            return Err(WorkerError::BlobsNotFound(missing_blob_ids));
        }
        let blobs = maybe_blobs
            .into_iter()
            .filter_map(|(blob_id, maybe_blob)| Some((blob_id, maybe_blob?)))
            .collect();
        let old_round = self.chain.manager.current_round();
        self.chain.manager.create_final_vote(
            certificate,
            self.config.key_pair(),
            self.storage.clock().current_time(),
            blobs,
        )?;
        self.save().await?;
        let actions = self.create_network_actions(Some(old_round)).await?;
        Ok((
            self.chain_info_response().await?,
            actions,
            BlockOutcome::Processed,
        ))
    }

    /// Processes a confirmed block (aka a commit).
    #[instrument(skip_all, fields(
        chain_id = %certificate.block().header.chain_id,
        height = %certificate.block().header.height,
        block_hash = %certificate.hash(),
    ))]
    pub(crate) async fn process_confirmed_block(
        &mut self,
        certificate: ConfirmedBlockCertificate,
        mode: ProcessConfirmedBlockMode,
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions, BlockOutcome), WorkerError> {
        let block = certificate.block();
        let block_hash = certificate.hash();
        let height = block.header.height;
        let chain_id = block.header.chain_id;

        // Trust-mark accept path: an earlier checkpoint cert recorded this block's
        // hash in `pre_checkpoint_block_trust`. Removing the trust mark here is
        // only an in-memory change; if anything below fails before `save`, the
        // mark survives on the next reload. The dispatch's `gap` definition
        // (`!=` rather than `<`) routes both above-tip and below-tip trust
        // uploads to `preprocess_certified_block` so the chain can write the
        // cert without advancing the tip.
        let in_trust_set = self
            .chain
            .pre_checkpoint_block_trust
            .contains(&block_hash)
            .await?;
        if in_trust_set {
            self.chain.pre_checkpoint_block_trust.remove(&block_hash)?;
        }

        // Check if we already processed this block.
        let tip = self.chain.tip_state.get().clone();
        if !in_trust_set && tip.next_block_height > height {
            let actions = self.create_network_actions(None).await?;
            self.register_delivery_notifier(height, &actions, notify_when_messages_are_delivered)
                .await;
            return Ok((
                self.chain_info_response().await?,
                actions,
                BlockOutcome::Skipped,
            ));
        }

        // We haven't processed the block - verify the certificate first.
        let committee = self.committee_for_epoch(block.header.epoch).await?;
        certificate.check(&committee)?;

        // Certificate check passed - which means the blobs the block requires are legitimate and
        // we can take note of it, so that if any are missing, we will accept them when the client
        // sends them.
        let required_blob_ids = block.required_blob_ids();
        let blobs_result = self
            .get_required_blobs(required_blob_ids.iter().copied(), block.created_blobs())
            .await
            .map(|blobs| blobs.into_values().collect::<Vec<_>>());

        if let Ok(blobs) = &blobs_result {
            self.storage
                .write_blobs_and_certificate(blobs, &certificate)
                .await?;
            let events = block
                .body
                .events
                .iter()
                .flatten()
                .map(|event| (event.id(chain_id), event.value.clone()));
            self.storage.write_events(events).await?;
        }

        // Update the blob state with last used certificate hash.
        let blob_state = certificate.value().to_blob_state(blobs_result.is_ok());
        let blob_ids = required_blob_ids.into_iter().collect::<Vec<_>>();
        self.storage
            .maybe_write_blob_states(&blob_ids, blob_state)
            .await?;

        let blobs = blobs_result?
            .into_iter()
            .map(|blob| (blob.id(), blob))
            .collect::<BTreeMap<_, _>>();

        // Dispatch on the actual outcome (preprocess / checkpoint-restore-then-execute
        // / contiguous execute):
        //  - `Preprocess` mode, or `Auto` with an unbridgeable gap: preprocess.
        //  - `Execute` mode with an unbridgeable gap: error.
        //  - `Auto`/`Execute` mode + gap + checkpoint block: install the snapshot,
        //    then execute.
        //  - `Auto`/`Execute` mode + contiguous: execute directly.
        use ProcessConfirmedBlockMode::{Auto, Execute, Preprocess};
        let gap = tip.next_block_height != height;
        let starts_with_checkpoint = block.starts_with_checkpoint();
        match (mode, gap, starts_with_checkpoint) {
            (Preprocess, _, _) | (Auto, true, false) => {
                self.preprocess_certified_block(certificate, notify_when_messages_are_delivered)
                    .await
            }
            (Execute, true, false) => Err(WorkerError::InvalidBlockChaining),
            (Auto | Execute, true, true) => {
                self.execute_block_with_checkpoint_restore(
                    certificate,
                    blobs,
                    notify_when_messages_are_delivered,
                )
                .await
            }
            (Auto | Execute, false, _) => {
                self.execute_contiguous_block(
                    certificate,
                    blobs,
                    tip,
                    notify_when_messages_are_delivered,
                )
                .await
            }
        }
    }

    /// Preprocesses a confirmed block: updates outboxes and event streams without
    /// executing it, and does not advance the chain tip.
    async fn preprocess_certified_block(
        &mut self,
        certificate: ConfirmedBlockCertificate,
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions, BlockOutcome), WorkerError> {
        let block_hash = certificate.hash();
        let block = certificate.block();
        let chain_id = block.header.chain_id;
        let height = block.header.height;

        let tracked = self.reconcile_tracked_outboxes().await?;
        let updated_event_streams = self
            .chain
            .preprocess_block(certificate.value(), tracked.as_deref().map(|h| h.inner()))
            .await?;
        self.save().await?;
        let mut actions = self.create_network_actions(None).await?;
        if !updated_event_streams.is_empty() {
            actions.notifications.push(Notification {
                chain_id,
                reason: Reason::NewEvents {
                    height,
                    block_hash,
                    event_streams: updated_event_streams,
                },
            });
        }
        trace!("Preprocessed confirmed block {height}");
        self.register_delivery_notifier(height, &actions, notify_when_messages_are_delivered)
            .await;
        Ok((
            self.chain_info_response().await?,
            actions,
            BlockOutcome::Preprocessed,
        ))
    }

    /// Restores the chain's execution state from the checkpoint blob embedded in
    /// the block's first oracle response, fast-forwards the tip to the block's
    /// height, then executes the block as if it were contiguous. Re-running the
    /// block applies its fees and re-derives any post-checkpoint state.
    async fn execute_block_with_checkpoint_restore(
        &mut self,
        certificate: ConfirmedBlockCertificate,
        blobs: BTreeMap<BlobId, Blob>,
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions, BlockOutcome), WorkerError> {
        let (bytes, chain_id, height, previous_block_hash, outbox_block_hashes, inbox_cursors) = {
            let block = certificate.block();
            let Some(OracleResponse::Checkpoint {
                execution_state_blobs,
                outbox_block_hashes,
                inbox_cursors,
                ..
            }) = block.body.oracle_responses.first().and_then(|r| r.first())
            else {
                return Err(ChainError::InternalError(
                    "Checkpoint block missing OracleResponse::Checkpoint".into(),
                )
                .into());
            };
            let mut bytes = Vec::new();
            let mut missing = Vec::new();
            for hash in execution_state_blobs {
                let blob_id = BlobId::new(*hash, BlobType::CheckpointExecutionState);
                match blobs.get(&blob_id) {
                    Some(blob) => bytes.extend_from_slice(blob.bytes()),
                    None => missing.push(blob_id),
                }
            }
            ensure!(missing.is_empty(), WorkerError::BlobsNotFound(missing));
            (
                bytes,
                block.header.chain_id,
                block.header.height,
                block.header.previous_block_hash,
                outbox_block_hashes.clone(),
                inbox_cursors.clone(),
            )
        };
        // Every pre-checkpoint sender block the oracle response names must already
        // be in storage before we touch any chain state. If some aren't, record
        // their hashes as trusted-by-this-checkpoint and error with `BlocksNotFound`
        // — the client uploads each missing cert (the trust-mark path at the top
        // of `process_confirmed_block` accepts them regardless of their possibly
        // revoked epoch), then retries the checkpoint. This way the worker never
        // exposes a half-restored chain whose outboxes reference unknown blocks.
        let mut missing_blocks = Vec::new();
        for hash in &outbox_block_hashes {
            if !self.storage.contains_certificate(*hash).await? {
                missing_blocks.push(*hash);
            }
        }
        if !missing_blocks.is_empty() {
            for hash in &missing_blocks {
                self.chain.pre_checkpoint_block_trust.insert(hash)?;
            }
            self.save().await?;
            return Err(WorkerError::BlocksNotFound(missing_blocks));
        }
        self.chain
            .execution_state
            .restore_from_content(&bytes)
            .await?;
        // `restore_from_content` writes directly to storage and leaves the
        // in-memory view in an undefined state — reload from storage.
        self.chain = self.storage.load_chain(chain_id).await?;
        // Re-populate `block_hashes` for every pre-checkpoint sender block the
        // chain still needs. The heights live in the just-restored execution
        // state (`unfinalized_message_blocks`); the matching hashes are the
        // ones the producer recorded in the oracle response, certified by the
        // checkpoint cert we already trust. Without this, the next step
        // (re-executing the checkpoint to verify its outcome) would fail
        // because `collect_unfinalized_block_hashes` looks these up.
        let heights = self.chain.collect_unfinalized_heights().await?;
        ensure!(
            heights.len() == outbox_block_hashes.len(),
            ChainError::InternalError(format!(
                "checkpoint oracle response has {} outbox block hashes but the \
                 restored state references {} distinct heights",
                outbox_block_hashes.len(),
                heights.len(),
            ))
        );
        for (height, hash) in heights.into_iter().zip(outbox_block_hashes) {
            self.chain.block_hashes.insert(&height, hash)?;
        }
        // Rebuild the off-chain outbox state (queues, counters,
        // nonempty_outboxes) from the on-chain unfinalized map so that this
        // node can resume pushing pre-checkpoint messages forward. The outbox
        // isn't part of the certified checkpoint blob, so without this a
        // bootstrapped validator would silently stop delivering pending
        // messages.
        let tracked = self.tracked_full_chains();
        self.chain
            .restore_outboxes_from_unfinalized(tracked.as_deref())
            .await?;
        for (origin, cursor) in inbox_cursors {
            let mut inbox = self.chain.inboxes.try_load_entry_mut(&origin).await?;
            inbox.restore_from_checkpoint(cursor).await?;
        }
        // Seed `next_expected_events` from the restored per-stream event counts. Re-executing
        // the checkpoint re-emits each summarized stream's summary event; without this seed
        // the summary's index would look like a gap on the freshly-restored node, so the
        // tracker would never advance and future events on that stream would never be
        // delivered to subscribers. The counts are contiguous, so this matches the producer's
        // tracker as of just before the checkpoint block.
        for (stream_id, counts) in self
            .chain
            .execution_state
            .system
            .stream_event_counts
            .index_values()
            .await?
        {
            self.chain
                .next_expected_events
                .insert(&stream_id, counts.next_index)?;
        }
        // We reset `execution_state` (via restore), `tip_state`, `block_hashes`
        // (for outbox-referenced pre-checkpoint heights), and the outbox views.
        // The other `ChainStateView` fields are either (a) already default for
        // a fresh bootstrap node (`inboxes`, `received_log`, …), (b) about to
        // be overwritten by `apply_confirmed_block` when the cert is applied
        // (`manager`, `block_hashes` for height `height`), or (c) outside the
        // protocol state hash so divergence from the producer is fine
        // (inboxes; subsequent blocks reconcile by anticipation if needed).
        // The `num_*` counters on `ChainTipState` are write-only in current
        // code, so leaving them at zero has no functional impact.
        let new_tip = ChainTipState {
            block_hash: previous_block_hash,
            next_block_height: height,
            ..Default::default()
        };
        self.chain.tip_state.set(new_tip.clone());
        // Installing a snapshot establishes the chain's state from scratch (block 0 is never
        // executed here), so record it as the initialization time for the reset cooldown.
        self.chain
            .chain_initialized_at
            .set(self.storage.clock().current_time());
        self.save().await?;
        self.execute_contiguous_block(
            certificate,
            blobs,
            new_tip,
            notify_when_messages_are_delivered,
        )
        .await
    }

    /// Executes a confirmed block whose height equals `tip.next_block_height`,
    /// updating inboxes, applying the block, and persisting the chain.
    async fn execute_contiguous_block(
        &mut self,
        certificate: ConfirmedBlockCertificate,
        mut blobs: BTreeMap<BlobId, Blob>,
        tip: ChainTipState,
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions, BlockOutcome), WorkerError> {
        let block_hash = certificate.hash();
        let block = certificate.block();
        let chain_id = block.header.chain_id;
        let height = block.header.height;

        // This should always be true for valid certificates.
        ensure!(
            tip.block_hash == block.header.previous_block_hash,
            WorkerError::InvalidBlockChaining
        );

        // Verify that the chain is active and that the epoch we used for verifying
        // the certificate is actually the active one on the chain.
        self.initialize_and_save_if_needed().await?;
        let (epoch, _) = self.chain.current_committee().await?;
        check_block_epoch(epoch, chain_id, block.header.epoch)?;

        let published_blobs = block
            .published_blob_ids()
            .iter()
            .filter_map(|blob_id| blobs.remove(blob_id))
            .collect::<Vec<_>>();

        let local_time = self.storage.clock().current_time();
        if block.header.timestamp.duration_since(local_time) > self.config.block_time_grace_period {
            warn!(
                block_timestamp = %block.header.timestamp,
                %local_time,
                "Confirmed block has a timestamp in the future beyond the block time grace period"
            );
        }
        let tracked = self.reconcile_tracked_outboxes().await?;
        let chain = &mut self.chain;
        chain
            .remove_bundles_from_inboxes(
                block.header.timestamp,
                false,
                block.body.incoming_bundles(),
            )
            .await?;
        let confirmed_block = if let Some(mut execution_state) = self
            .execution_state_cache
            .as_ref()
            .and_then(|cache| cache.remove(&block_hash))
        {
            chain.execution_state = execution_state
                .with_context(|ctx| {
                    chain
                        .execution_state
                        .context()
                        .clone_with_base_key(ctx.base_key().bytes.clone())
                })
                .await;
            certificate.into_value()
        } else {
            let (proposed_block, outcome) = certificate.into_value().into_block().into_proposal();
            let oracle_responses = Some(outcome.oracle_responses.clone());
            let (proposed_block, verified, _resource_tracker, _) = chain
                .execute_block(
                    proposed_block,
                    local_time,
                    None,
                    &published_blobs,
                    oracle_responses,
                    BundleExecutionPolicy::committed(),
                )
                .await?;
            // We should always agree on the messages and state hash.
            if outcome != verified {
                return Err(ChainError::CorruptedChainState(format!(
                    "computed block outcome differs from the certificate.\n\
                    Computed: {verified:#?}\n\
                    Submitted: {outcome:#?}"
                ))
                .into());
            }
            ConfirmedBlock::new(Block::new(proposed_block, verified))
        };

        let updated_streams = chain
            .apply_confirmed_block(
                &confirmed_block,
                local_time,
                tracked.as_deref().map(|h| h.inner()),
            )
            .await?;
        let mut actions = self.create_network_actions(None).await?;
        trace!("Processed confirmed block {height}");
        actions.notifications.push(Notification {
            chain_id,
            reason: Reason::NewBlock {
                height,
                hash: block_hash,
            },
        });
        if !updated_streams.is_empty() {
            actions.notifications.push(Notification {
                chain_id,
                reason: Reason::NewEvents {
                    height,
                    block_hash,
                    event_streams: updated_streams,
                },
            });
        }
        self.save().await?;

        self.block_values
            .insert_hashed(Cow::Owned(confirmed_block.into_inner()));

        self.register_delivery_notifier(height, &actions, notify_when_messages_are_delivered)
            .await;

        Ok((
            self.chain_info_response().await?,
            actions,
            BlockOutcome::Processed,
        ))
    }

    /// Schedules a notification for when cross-chain messages are delivered up to the given
    /// `height`.
    #[instrument(level = "trace", skip(self, notify_when_messages_are_delivered))]
    async fn register_delivery_notifier(
        &self,
        height: BlockHeight,
        actions: &NetworkActions,
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) {
        if let Some(notifier) = notify_when_messages_are_delivered {
            if actions
                .cross_chain_requests
                .iter()
                .any(|request| request.has_messages_lower_or_equal_than(height))
            {
                self.delivery_notifier.register(height, notifier);
            } else {
                // No need to wait. Also, cross-chain requests may not trigger the
                // notifier later, even if we register it.
                if let Err(()) = notifier.send(()) {
                    debug!("Failed to notify message delivery to caller (early case)");
                }
            }
        }
    }

    /// Updates the chain's inboxes, receiving messages from a cross-chain update.
    #[instrument(level = "debug", skip(self, bundles), fields(chain_id = %self.chain_id()))]
    pub(crate) async fn process_cross_chain_update(
        &mut self,
        origin: ChainId,
        bundles: Vec<(Epoch, MessageBundle)>,
        sender_previous_height: Option<BlockHeight>,
    ) -> Result<CrossChainUpdateResult, WorkerError> {
        // Only process certificates with relevant heights and epochs.
        let mut inbox = self.chain.inboxes.try_load_entry_mut(&origin).await?;
        let next_height_to_receive = inbox.next_block_height_to_receive()?;
        let last_anticipated_block_height = inbox
            .removed_bundles
            .back()
            .await?
            .map(|bundle| bundle.height);

        // Proactive gap detection: if the sender declares a predecessor height that
        // we haven't received yet, the inbox has a gap.
        if let Some(prev) = sender_previous_height {
            if prev >= next_height_to_receive {
                let chain_id = self.chain_id();
                if self.config.allow_revert_confirm && self.config.recovery_allowed_for(&chain_id) {
                    warn!(
                        %chain_id,
                        "Inbox gap detected from {origin}: \
                        sender declares previous height {prev} but we only have up to \
                        {next_height_to_receive}; requesting resend",
                    );
                    return Ok(CrossChainUpdateResult::GapDetected {
                        origin,
                        retransmit_from: next_height_to_receive,
                    });
                }
                return Err(ChainError::InboxGapDetected {
                    chain_id,
                    origin,
                    expected_height: prev,
                    actual_height: bundles.first().map(|(_, b)| b.height).unwrap_or_default(),
                }
                .into());
            }
        }

        let bundles = self
            .select_message_bundles(
                &origin,
                next_height_to_receive,
                last_anticipated_block_height,
                bundles,
            )
            .await?;
        let Some(last_updated_height) = bundles.last().map(|bundle| bundle.height) else {
            return Ok(CrossChainUpdateResult::NothingToDo);
        };
        // Process the received messages in certificates.
        let local_time = self.storage.clock().current_time();
        let mut previous_height = None;
        for bundle in bundles {
            let add_to_received_log = previous_height != Some(bundle.height);
            previous_height = Some(bundle.height);
            // Update the staged chain state with the received block.
            self.chain
                .receive_message_bundle_with_inbox(
                    &mut inbox,
                    &origin,
                    bundle,
                    local_time,
                    add_to_received_log,
                )
                .await?;
        }
        inbox.observe_size_metric();
        drop(inbox);
        if !self.config.allow_inactive_chains && !self.chain.is_active().await? {
            // Refuse to create a chain state if the chain is still inactive by
            // now. Accordingly, do not send a confirmation, so that the
            // cross-chain update is retried later.
            warn!(
                chain_id = %self.chain_id(),
                "Refusing to deliver messages from {origin} \
                at height {last_updated_height} because the recipient is still inactive",
            );
            return Ok(CrossChainUpdateResult::NothingToDo);
        }
        Ok(CrossChainUpdateResult::Updated(last_updated_height))
    }

    /// Handles the cross-chain request confirming that the recipient was updated.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        %recipient,
        %latest_height
    ))]
    pub(crate) async fn confirm_updated_recipient(
        &mut self,
        recipient: ChainId,
        latest_height: BlockHeight,
    ) -> Result<bool, WorkerError> {
        // Reconcile the outbox indices with the *current* tracked set before draining the counter
        // and checking delivery.
        let tracked = self.reconcile_tracked_outboxes().await?;
        // The indices are now reconciled to the tracked set, so `all_messages_delivered_up_to`
        // (the `outbox_counters` fast path) is already the complete answer for tracked chains.
        Ok(self
            .chain
            .mark_messages_as_received(
                &recipient,
                latest_height,
                tracked.as_deref().map(|h| h.inner()),
            )
            .await?
            && self.chain.all_messages_delivered_up_to(latest_height))
    }

    /// Notifies delivery waiters that all messages up to `height` have been delivered.
    pub(crate) fn notify_delivery(&self, height: BlockHeight) {
        self.delivery_notifier.notify(height);
    }

    /// Processes a batch of cross-chain requests, performing at most one `save()`.
    ///
    /// Both update and confirmation requests are handled together so that a
    /// single write-lock acquisition covers all pending work for the chain.
    pub(crate) async fn process_batch(&mut self, requests: Vec<BatchRequest>) {
        let mut update_results = Vec::new();
        let mut confirm_results = Vec::new();
        let mut need_save = false;
        let mut need_rollback = false;
        let mut max_delivered_height: Option<BlockHeight> = None;

        for request in requests {
            match request {
                BatchRequest::Update {
                    origin,
                    bundles,
                    previous_height,
                    result_sender,
                } => {
                    if need_rollback {
                        send_result(result_sender, Err(WorkerError::BatchRolledBack));
                        continue;
                    }
                    let result = self
                        .process_cross_chain_update(origin, bundles, previous_height)
                        .await;
                    let update_result = match result {
                        Ok(update_result) => update_result,
                        Err(error) => {
                            need_rollback = true;
                            send_result(result_sender, Err(error));
                            continue;
                        }
                    };
                    match &update_result {
                        CrossChainUpdateResult::Updated(_) => need_save = true,
                        CrossChainUpdateResult::GapDetected { .. }
                        | CrossChainUpdateResult::NothingToDo => {}
                    }
                    update_results.push((result_sender, update_result));
                }
                BatchRequest::Confirm {
                    recipient,
                    latest_height,
                    result_sender,
                } => {
                    if need_rollback {
                        send_result(result_sender, Err(WorkerError::BatchRolledBack));
                        continue;
                    }
                    match self
                        .confirm_updated_recipient(recipient, latest_height)
                        .await
                    {
                        Ok(fully_delivered) => {
                            need_save = true;
                            if fully_delivered {
                                max_delivered_height = Some(
                                    max_delivered_height
                                        .map_or(latest_height, |h| h.max(latest_height)),
                                );
                            }
                            confirm_results.push((result_sender, recipient));
                        }
                        Err(error) => {
                            need_rollback = true;
                            send_result(result_sender, Err(error));
                        }
                    }
                }
            }
        }
        if !need_rollback && need_save {
            if let Err(error) = self.save().await {
                tracing::error!(%error, "failed to save batch; rolling back");
                need_rollback = true;
            }
        }
        if need_rollback {
            for (result_sender, _) in update_results {
                send_result(result_sender, Err(WorkerError::BatchRolledBack));
            }
            for (result_sender, _) in confirm_results {
                send_result(result_sender, Err(WorkerError::BatchRolledBack));
            }
            return;
        }

        if let Some(height) = max_delivered_height {
            self.notify_delivery(height);
        }

        for (result_sender, update_result) in update_results {
            send_result(result_sender, Ok(update_result));
        }
        for (result_sender, recipient) in confirm_results {
            let result = self
                .create_cross_chain_actions_for_recipient(recipient)
                .await;
            send_result(result_sender, result);
        }
    }

    /// Handles a `RevertConfirm` request: walks backward through
    /// `previous_message_blocks` to find all block heights that sent messages to
    /// `recipient` starting from the latest down to `retransmit_from`, re-adds them
    /// to the outbox, and creates cross-chain update actions to resend the bundles.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        %recipient,
        %retransmit_from,
    ))]
    pub(crate) async fn handle_revert_confirm(
        &mut self,
        recipient: ChainId,
        retransmit_from: BlockHeight,
    ) -> Result<NetworkActions, WorkerError> {
        self.reconcile_tracked_outboxes().await?;
        // 1. Walk backward through previous_message_blocks to collect all heights
        //    that sent messages to this recipient, from the latest down to retransmit_from.
        let Some(latest_height) = self
            .chain
            .execution_state
            .previous_message_blocks
            .get(&recipient)
            .await?
        else {
            warn!("RevertConfirm: no record of sending to {recipient}");
            return Ok(NetworkActions::default());
        };

        let mut heights_to_re_add = Vec::new();
        let mut current_height = latest_height;
        while current_height >= retransmit_from {
            // We arrived at current_height via previous_message_blocks links, starting from the
            // chain state and following the links downwards. So these blocks should all be in
            // `block_hashes` already.
            heights_to_re_add.push(current_height);
            // Load the block at current_height to find the previous message block
            let hash = match &*self
                .chain
                .block_hashes_for_heights([current_height])
                .await?
            {
                [hash] => *hash,
                _ => {
                    return Err(WorkerError::BlockHashNotFound {
                        height: current_height,
                        chain_id: self.chain_id(),
                    })
                }
            };
            let block = self
                .read_confirmed_blocks(&[hash])
                .await?
                .pop()
                .flatten()
                .ok_or_else(|| WorkerError::LocalBlockNotFound {
                    height: current_height,
                    chain_id: self.chain_id(),
                })?;
            match block.block().body.previous_message_blocks.get(&recipient) {
                Some((_, prev_height)) if *prev_height >= retransmit_from => {
                    current_height = *prev_height;
                }
                _ => break,
            }
        }

        // 2. Re-add the heights to the outbox.
        let new_heights = self
            .chain
            .outboxes
            .try_load_entry_mut(&recipient)
            .await?
            .revert(&heights_to_re_add)
            .await?;

        if new_heights.is_empty() {
            debug!("RevertConfirm: all heights already in outbox for {recipient}");
            return Ok(NetworkActions::default());
        }

        // 3. Update the indices only for tracked recipients (mirroring `process_outgoing_messages`):
        //    an untracked recipient keeps its re-added outbox queue but is not counted or indexed.
        let new_heights_len = new_heights.len();
        if self.is_tracked(&recipient) {
            for h in new_heights {
                *self.chain.outbox_counters.get_mut().entry(h).or_default() += 1;
            }
            self.chain.nonempty_outboxes.get_mut().insert(recipient);
        }

        // 4. Create cross-chain requests for this recipient.
        let actions = self
            .create_cross_chain_actions_for_recipient(recipient)
            .await?;

        // 5. Save chain state.
        self.save().await?;

        warn!(
            "RevertConfirm: re-added {new_heights_len} heights to outbox for {recipient}, \
            starting from height {retransmit_from}"
        );

        Ok(actions)
    }

    /// If the config enables corruption recovery and the min-duration guard is
    /// satisfied, resets the chain state and re-executes all confirmed blocks.
    /// Returns `RevertConfirm` requests to dispatch, or `None` if no reset happened.
    pub(crate) async fn maybe_reset_corrupted_chain_state(
        &mut self,
    ) -> Result<Option<Vec<CrossChainRequest>>, WorkerError> {
        let Some(min_duration) = self.config.reset_on_corrupted_chain_state else {
            return Ok(None);
        };
        let chain_id = self.chain_id();
        if !self.config.recovery_allowed_for(&chain_id) {
            return Ok(None);
        }
        let local_time = self.storage.clock().current_time();
        let initialized_time = *self.chain.chain_initialized_at.get();
        let elapsed = local_time.duration_since(initialized_time);
        if elapsed < min_duration {
            warn!(
                %chain_id, ?elapsed, ?min_duration,
                "Not resetting corrupted chain state; not enough time elapsed \
                since the chain was last initialized"
            );
            return Ok(None);
        }
        warn!(%chain_id, "Corrupted chain state detected; resetting and re-executing");
        Ok(Some(self.reset_and_reexecute_chain().await?))
    }

    /// Resets the chain state completely and re-executes all confirmed blocks from storage.
    /// Returns a `RevertConfirm` request for every known sender so they resend cross-chain
    /// messages that may have been lost during the reset.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
    ))]
    pub(crate) async fn reset_and_reexecute_chain(
        &mut self,
    ) -> Result<Vec<CrossChainRequest>, WorkerError> {
        let chain_id = self.chain_id();
        let tip_height = self.chain.tip_state.get().next_block_height;

        // 1. Collect all sender chain IDs and block hashes before clearing.
        let sender_ids = self.chain.inboxes.indices().await?;
        let block_hashes = self.chain.block_hashes.index_values().await?;
        // Re-execution starts from the latest checkpoint, if any: replaying the checkpoint
        // block onto the wiped (height-0) chain installs its snapshot, so the blocks below it
        // never need to be replayed. Without a checkpoint, this is `0` and the whole chain is
        // replayed.
        let restore_from =
            (*self.chain.latest_checkpoint_height.get()).unwrap_or(BlockHeight::ZERO);

        // 2. Snapshot safety-critical manager state so that we cannot be tricked
        //    into double-signing if the reset wipes votes we already cast.
        let manager_snapshot = ManagerSafetySnapshot::capture(&self.chain.manager).await?;

        // 3. Wipe every key under this chain's storage root and reload a fresh
        //    `ChainStateView`. `ChainStateView::clear` + `save` would only reset
        //    the in-memory view; `HistoricallyHashableView` deliberately keeps
        //    its `stored_hash` across clears, so the historical hash would carry
        //    over from the pre-reset state and the replayed block outcomes would
        //    never match the original certificates' `state_hash`. Deleting the
        //    storage prefix and reloading is equivalent to creating the chain
        //    from scratch, which is what replay expects.
        self.wipe_and_reload_chain().await?;
        self.knows_chain_is_active = false;
        warn!(
            %chain_id,
            "Cleared chain state up to height {tip_height}; \
            re-executing blocks from height {restore_from}"
        );

        // 4. Re-load and re-process the certificates from the latest checkpoint onward. The
        //    first one lands on the wiped, height-0 chain with a tip gap: if it is a checkpoint
        //    `process_confirmed_block` installs its snapshot before executing, and otherwise
        //    (height 0) it executes directly. The remaining blocks are contiguous.
        for (height, hash) in block_hashes {
            if height < restore_from {
                continue;
            }
            let cert = self
                .storage
                .read_certificate(hash)
                .await?
                .map(CacheArc::unwrap_or_clone)
                .ok_or_else(|| WorkerError::LocalBlockNotFound { height, chain_id })?;
            Box::pin(self.process_confirmed_block(cert, ProcessConfirmedBlockMode::Execute, None))
                .await?;
        }

        // 5. Restore any previously cast votes and locking block so we cannot be
        //    asked to sign a conflicting statement at the same height/round. Votes
        //    in the manager always belong to the pending height (one past the tip),
        //    so restoring is only meaningful if re-execution landed at the same
        //    tip. Otherwise the restored state would refer to a stale pending
        //    height and could only break the manager's invariants without any
        //    safety benefit — so we drop the snapshot in that case.
        let new_tip_height = self.chain.tip_state.get().next_block_height;
        if new_tip_height == tip_height {
            manager_snapshot.restore(&mut self.chain.manager)?;
            self.save().await?;
        } else {
            warn!(
                %tip_height, %new_tip_height,
                "Dropping manager snapshot: pre-reset tip differs from post-reset tip"
            );
        }

        // 6. Build RevertConfirm requests so each sender resends messages we may
        //    have lost during the reset.
        let revert_requests = sender_ids
            .into_iter()
            .map(|sender| CrossChainRequest::RevertConfirm {
                sender,
                recipient: chain_id,
                retransmit_from: BlockHeight::ZERO,
            })
            .collect::<Vec<_>>();

        warn!(
            tip_height = %self.chain.tip_state.get().next_block_height,
            num_revert_confirms = revert_requests.len(),
            "Chain reset and re-executed; sending RevertConfirm to senders"
        );

        Ok(revert_requests)
    }

    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        num_trackers = %new_trackers.len()
    ))]
    pub(crate) async fn update_received_certificate_trackers(
        &mut self,
        new_trackers: BTreeMap<ValidatorPublicKey, u64>,
    ) -> Result<(), WorkerError> {
        self.chain
            .update_received_certificate_trackers(new_trackers);
        self.save().await?;
        Ok(())
    }

    /// Returns the preprocessed block hashes in the given height range.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        start = %start,
        end = %end
    ))]
    pub(crate) async fn get_preprocessed_block_hashes(
        &self,
        start: BlockHeight,
        end: BlockHeight,
    ) -> Result<Vec<CryptoHash>, WorkerError> {
        let mut hashes = Vec::new();
        let mut height = start;
        while height < end {
            match self.chain.block_hashes.get(&height).await? {
                Some(hash) => hashes.push(hash),
                None => break,
            }
            height = height.try_add_one()?;
        }
        Ok(hashes)
    }

    /// Returns the next block height to receive from an inbox.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        origin = %origin
    ))]
    pub(crate) async fn get_inbox_next_height(
        &self,
        origin: ChainId,
    ) -> Result<BlockHeight, WorkerError> {
        Ok(match self.chain.inboxes.try_load_entry(&origin).await? {
            Some(inbox) => inbox.next_block_height_to_receive()?,
            None => BlockHeight::ZERO,
        })
    }

    /// Returns the locking blobs for the given blob IDs.
    /// Returns `Ok(None)` if any of the blobs is not found.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        num_blob_ids = %blob_ids.len()
    ))]
    pub(crate) async fn get_locking_blobs(
        &self,
        blob_ids: Vec<BlobId>,
    ) -> Result<Option<Vec<Blob>>, WorkerError> {
        let results = self
            .chain
            .manager
            .locking_blobs
            .multi_get(&blob_ids)
            .await?;
        Ok(results.into_iter().collect())
    }

    /// Gets block hashes for specified heights.
    pub(crate) async fn get_block_hashes(
        &self,
        heights: Vec<BlockHeight>,
    ) -> Result<Vec<CryptoHash>, WorkerError> {
        Ok(self.chain.block_hashes_for_heights(heights).await?)
    }

    /// Gets proposed blobs from the manager for specified blob IDs.
    pub(crate) async fn get_proposed_blobs(
        &self,
        blob_ids: Vec<BlobId>,
    ) -> Result<Vec<Blob>, WorkerError> {
        let results = self
            .chain
            .manager
            .proposed_blobs
            .multi_get(&blob_ids)
            .await?;
        let mut blobs = Vec::with_capacity(blob_ids.len());
        let mut missing = Vec::new();
        for (blob_id, maybe_blob) in blob_ids.into_iter().zip(results) {
            match maybe_blob {
                Some(blob) => blobs.push(blob),
                None => missing.push(blob_id),
            }
        }
        if !missing.is_empty() {
            return Err(WorkerError::BlobsNotFound(missing));
        }
        Ok(blobs)
    }

    /// Gets event subscriptions.
    pub(crate) async fn get_event_subscriptions(
        &self,
    ) -> Result<EventSubscriptionsResult, WorkerError> {
        Ok(self
            .chain
            .execution_state
            .system
            .event_subscriptions
            .index_values()
            .await?)
    }

    /// Gets the next expected event index for a stream.
    pub(crate) async fn get_next_expected_event(
        &self,
        stream_id: StreamId,
    ) -> Result<Option<u32>, WorkerError> {
        Ok(self.chain.next_expected_events.get(&stream_id).await?)
    }

    /// Gets the lowest readable event index for a stream this chain is writing to, i.e. the
    /// index of the first event published since the most recent checkpoint.
    pub(crate) async fn get_stream_first_index(
        &self,
        stream_id: StreamId,
    ) -> Result<u32, WorkerError> {
        Ok(self
            .chain
            .execution_state
            .system
            .stream_event_counts
            .get(&stream_id)
            .await?
            .map_or(0, |counts| counts.first_index))
    }

    /// Gets the `next_expected_events` indices for the given streams.
    pub(crate) async fn get_next_expected_events(
        &self,
        stream_ids: Vec<StreamId>,
    ) -> Result<BTreeMap<StreamId, u32>, WorkerError> {
        let values = self
            .chain
            .next_expected_events
            .multi_get(&stream_ids)
            .await?;
        Ok(stream_ids
            .into_iter()
            .zip(values)
            .filter_map(|(id, val)| Some((id, val?)))
            .collect())
    }

    /// Gets received certificate trackers.
    pub(crate) async fn get_received_certificate_trackers(
        &self,
    ) -> Result<HashMap<ValidatorPublicKey, u64>, WorkerError> {
        Ok(self.chain.received_certificate_trackers.get().clone())
    }

    /// Gets tip state and outbox info for next_outbox_heights calculation.
    pub(crate) async fn get_tip_state_and_outbox_info(
        &self,
        receiver_id: ChainId,
    ) -> Result<(BlockHeight, Option<BlockHeight>), WorkerError> {
        let next_block_height = self.chain.tip_state.get().next_block_height;
        let next_height_to_schedule = self
            .chain
            .outboxes
            .try_load_entry(&receiver_id)
            .await?
            .map(|outbox| *outbox.next_height_to_schedule.get());
        Ok((next_block_height, next_height_to_schedule))
    }

    /// Gets the next height to preprocess.
    pub(crate) fn get_next_height_to_preprocess(&self) -> BlockHeight {
        *self.chain.next_height_to_preprocess.get()
    }

    /// Attempts to vote for a leader timeout, if possible.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        height = %height,
        round = %round
    ))]
    async fn vote_for_leader_timeout(
        &mut self,
        height: BlockHeight,
        round: Round,
    ) -> Result<(), WorkerError> {
        let chain = &mut self.chain;
        ensure!(
            height == chain.tip_state.get().next_block_height,
            WorkerError::UnexpectedBlockHeight {
                expected_block_height: chain.tip_state.get().next_block_height,
                found_block_height: height
            }
        );
        let epoch = chain.execution_state.system.epoch.get();
        let chain_id = chain.chain_id();
        let key_pair = self.config.key_pair();
        let local_time = self.storage.clock().current_time();
        if chain
            .manager
            .create_timeout_vote(chain_id, height, round, *epoch, key_pair, local_time)?
        {
            self.save().await?;
        }
        Ok(())
    }

    /// Votes for falling back to a public chain.
    ///
    /// Fallback is triggered when the chain is in epoch `e` and epoch `e+1` has been created
    /// on the admin chain longer than the configured `fallback_duration` ago.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id()
    ))]
    async fn vote_for_fallback(&mut self) -> Result<(), WorkerError> {
        let chain = &mut self.chain;
        let epoch = *chain.execution_state.system.epoch.get();
        let Some(admin_chain_id) = chain.execution_state.system.admin_chain_id.get() else {
            return Ok(());
        };

        // Check if epoch e+1 exists on the admin chain and when it was created.
        let next_epoch_index = epoch.0.saturating_add(1);
        let event_id = EventId {
            chain_id: *admin_chain_id,
            stream_id: StreamId::system(EPOCH_STREAM_NAME),
            index: next_epoch_index,
        };

        let Some(event_bytes) = self.storage.read_event(event_id).await? else {
            return Ok(()); // Next epoch doesn't exist yet.
        };

        let event_data: EpochEventData = bcs::from_bytes(&event_bytes)?;
        let elapsed = self
            .storage
            .clock()
            .current_time()
            .delta_since(event_data.timestamp);
        if elapsed >= chain.ownership().await?.timeout_config.fallback_duration {
            let chain_id = chain.chain_id();
            let height = chain.tip_state.get().next_block_height;
            let key_pair = self.config.key_pair();
            if chain
                .manager
                .vote_fallback(chain_id, height, epoch, key_pair)
            {
                self.save().await?;
            }
        }
        Ok(())
    }

    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        blob_id = %blob.id()
    ))]
    pub(crate) async fn handle_pending_blob(
        &mut self,
        blob: Blob,
    ) -> Result<ChainInfoResponse, WorkerError> {
        let mut was_expected = self
            .chain
            .pending_validated_blobs
            .maybe_insert(&blob)
            .await?;
        for (_, mut pending_blobs) in self
            .chain
            .pending_proposed_blobs
            .try_load_all_entries_mut()
            .await?
        {
            if !pending_blobs.validated.get() {
                let (_, committee) = self.chain.current_committee().await?;
                let policy = committee.policy();
                policy
                    .check_blob_size(blob.content())
                    .with_execution_context(ChainExecutionContext::Block)?;
                ensure!(
                    u64::try_from(pending_blobs.pending_blobs.iterative_count().await?)
                        .is_ok_and(|count| count < policy.maximum_published_blobs),
                    WorkerError::TooManyPublishedBlobs(policy.maximum_published_blobs)
                );
            }
            was_expected = was_expected || pending_blobs.maybe_insert(&blob).await?;
        }
        ensure!(was_expected, WorkerError::UnexpectedBlob);
        self.save().await?;
        self.chain_info_response().await
    }

    /// Returns a stored [`Certificate`] for the chain's block at the requested [`BlockHeight`].
    ///
    /// Does not need `&mut self` because the chain is eagerly initialized when the
    /// chain handle is created.
    #[cfg(with_testing)]
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        height = %height
    ))]
    pub(crate) async fn read_certificate(
        &self,
        height: BlockHeight,
    ) -> Result<Option<CacheArc<ConfirmedBlockCertificate>>, WorkerError> {
        let certificate_hash = match self.chain.block_hashes.get(&height).await? {
            Some(hash) => hash,
            None => return Ok(None),
        };
        let certificate = self
            .storage
            .read_certificate(certificate_hash)
            .await?
            .ok_or(WorkerError::BlocksNotFound(vec![certificate_hash]))?;
        Ok(Some(certificate))
    }

    /// Queries an application's state on the chain.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        query_application_id = %query.application_id()
    ))]
    pub(crate) async fn query_application(
        &mut self,
        query: Query,
        block_hash: Option<CryptoHash>,
    ) -> Result<(QueryOutcome, BlockHeight), WorkerError> {
        self.initialize_and_save_if_needed().await?;
        let next_block_height = self.chain.tip_state.get().next_block_height;
        let local_time = self.storage.clock().current_time();
        // Try to use a cached execution state for the requested block.
        // We want to pretend that this block is committed, so we set the next block height.
        let cached_state = block_hash
            .zip(self.execution_state_cache.as_ref())
            .and_then(|(h, cache)| Some(h).zip(cache.remove(&h)));
        if let Some((requested_block, mut state)) = cached_state {
            let next_block_height = next_block_height
                .try_add_one()
                .expect("block height to not overflow");
            let context = QueryContext {
                chain_id: self.chain_id(),
                next_block_height,
                local_time,
            };
            let outcome = state
                .with_context(|ctx| {
                    self.chain
                        .execution_state
                        .context()
                        .clone_with_base_key(ctx.base_key().bytes.clone())
                })
                .await
                .query_application(context, query, self.service_runtime_endpoint.as_mut())
                .await
                .with_execution_context(ChainExecutionContext::Query)?;
            if let Some(cache) = &self.execution_state_cache {
                cache.insert(&requested_block, state);
            }
            Ok((outcome, next_block_height))
        } else {
            if block_hash.is_some() {
                tracing::debug!(
                    "requested block hash not found in cache, querying committed state"
                );
            }
            let outcome = self
                .chain
                .query_application(local_time, query, self.service_runtime_endpoint.as_mut())
                .await?;
            Ok((outcome, next_block_height))
        }
    }

    /// Returns an application's description by reading the blob directly from storage.
    ///
    /// Does not track blob usage (which requires `&mut self`), making it safe for
    /// concurrent reads. Blob tracking is only relevant during block execution and is
    /// always rolled back for read-only queries.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        application_id = %application_id
    ))]
    pub(crate) async fn describe_application_readonly(
        &self,
        application_id: ApplicationId,
    ) -> Result<ApplicationDescription, WorkerError> {
        let blob_id = application_id.description_blob_id();
        let blob = self
            .storage
            .read_blob(blob_id)
            .await?
            .ok_or(WorkerError::BlobsNotFound(vec![blob_id]))?;
        Ok(bcs::from_bytes(blob.bytes())?)
    }

    /// Executes a block without persisting any changes to the state, with a specified
    /// policy for handling bundle failures.
    ///
    /// The block may be modified to reflect the actual executed transactions
    /// (bundles may be rejected or removed based on the policy).
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        block_height = %block.height
    ))]
    pub(crate) async fn stage_block_execution(
        &mut self,
        block: ProposedBlock,
        round: Option<u32>,
        published_blobs: &[Blob],
        policy: BundleExecutionPolicy,
    ) -> Result<
        (
            ProposedBlock,
            Block,
            ChainInfoResponse,
            ResourceTracker,
            HashSet<ChainId>,
        ),
        WorkerError,
    > {
        self.initialize_and_save_if_needed().await?;
        let local_time = self.storage.clock().current_time();
        let (_, committee) = self.chain.current_committee().await?;
        block.check_proposal_size(committee.policy().maximum_block_proposal_size)?;

        self.chain
            .remove_bundles_from_inboxes(block.timestamp, true, block.incoming_bundles())
            .await?;
        let (executed_block, resource_tracker, never_reject_origins) =
            Box::pin(self.execute_block(block, local_time, round, published_blobs, policy)).await?;

        // No need to sign: only used internally.
        let info = ChainInfo::from_chain_view(&mut self.chain).await?;
        let mut response = ChainInfoResponse::new(info, None);
        if let Some(owner) = executed_block.header.authenticated_owner {
            response.info.requested_owner_balance = self
                .chain
                .execution_state
                .system
                .balances
                .get(&owner)
                .await?;
        }

        let (proposed_block, _) = executed_block.clone().into_proposal();
        Ok((
            proposed_block,
            executed_block,
            response,
            resource_tracker,
            never_reject_origins,
        ))
    }

    /// Validates and executes a block proposed to extend this chain.
    ///
    /// Returns network actions alongside the result so the caller can dispatch them
    /// even when the proposal is rejected: a `HasIncompatibleConfirmedVote` rejection
    /// can still advance `current_round` via `update_signed_proposal`, and subscribers
    /// need the resulting `NewRound` notification.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        block_height = %proposal.content.block.height
    ))]
    pub(crate) async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> (Result<ChainInfoResponse, WorkerError>, NetworkActions) {
        let old_round = self.chain.manager.current_round();
        match self.try_handle_block_proposal(proposal).await {
            Ok((response, actions)) => (Ok(response), actions),
            Err(err) => {
                // Even on error, the manager's `current_round` may have advanced
                // (the `HasIncompatibleConfirmedVote` recovery path calls
                // `update_signed_proposal`). Surface the resulting `NewRound`
                // notification so subscribers can react.
                let actions = if self.chain.manager.current_round() != old_round {
                    self.create_network_actions(Some(old_round))
                        .await
                        .unwrap_or_default()
                } else {
                    NetworkActions::default()
                };
                (Err(err), actions)
            }
        }
    }

    async fn try_handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        self.initialize_and_save_if_needed().await?;
        proposal
            .check_invariants()
            .map_err(|msg| WorkerError::InvalidBlockProposal(msg.to_string()))?;
        proposal.check_signature()?;
        let owner = proposal.owner();
        let BlockProposal {
            content,
            original_proposal,
            signature: _,
        } = &proposal;
        let block = &content.block;
        let chain = &self.chain;
        // Check if the chain is ready for this new block proposal.
        chain.tip_state.get().verify_block_chaining(block)?;
        // Check the epoch.
        let (epoch, committee) = chain.current_committee().await?;
        check_block_epoch(epoch, block.chain_id, block.epoch)?;
        let policy = committee.policy().clone();
        block.check_proposal_size(policy.maximum_block_proposal_size)?;
        // Check the authentication of the block.
        ensure!(
            chain.manager.can_propose(&owner, proposal.content.round),
            WorkerError::InvalidOwner
        );
        let old_round = self.chain.manager.current_round();
        match original_proposal {
            None => {
                if let Some(signer) = block.authenticated_owner {
                    // Check the authentication of the operations in the new block.
                    ensure!(signer == owner, WorkerError::InvalidSigner(owner));
                }
            }
            Some(OriginalProposal::Regular { certificate }) => {
                // Verify that this block has been validated by a quorum before.
                certificate.check(&committee)?;
            }
            Some(OriginalProposal::Fast(signature)) => {
                let original_proposal = BlockProposal {
                    content: ProposalContent {
                        block: content.block.clone(),
                        round: Round::Fast,
                        outcome: None,
                    },
                    signature: *signature,
                    original_proposal: None,
                };
                let super_owner = original_proposal.owner();
                ensure!(
                    chain
                        .manager
                        .ownership
                        .get()
                        .super_owners
                        .contains(&super_owner),
                    WorkerError::InvalidOwner
                );
                if let Some(signer) = block.authenticated_owner {
                    // Check the authentication of the operations in the new block.
                    ensure!(signer == super_owner, WorkerError::InvalidSigner(signer));
                }
                original_proposal.check_signature()?;
            }
        }
        let local_time = self.storage.clock().current_time();
        match chain.manager.check_proposed_block(&proposal) {
            Ok(manager::Outcome::Skip) => {
                // We already voted for this block.
                return Ok((self.chain_info_response().await?, NetworkActions::default()));
            }
            Ok(manager::Outcome::Accept) => {}
            Err(err) => {
                // A `HasIncompatibleConfirmedVote` rejection means the proposer is at a
                // round we'd otherwise be happy to sign at; only our prior confirmed vote
                // prevents us from voting. Record the proposal so `current_round` still
                // tracks the round the proposer is in — without it, the chain can wedge.
                // Other rejections (e.g. `WrongRound`, `InsufficientRound`) mean the
                // proposal is not actually valid for the chain's current state, so we
                // shouldn't let it advance our round.
                if matches!(err, ChainError::HasIncompatibleConfirmedVote(_, _))
                    && self
                        .chain
                        .manager
                        .update_signed_proposal(&proposal, local_time)
                {
                    self.save().await?;
                }
                return Err(err.into());
            }
        }

        // Make sure we remember that a proposal was signed, to determine the correct round to
        // propose in.
        if self
            .chain
            .manager
            .update_signed_proposal(&proposal, local_time)
        {
            self.save().await?;
        }

        let published_blobs = self.load_proposal_blobs(&proposal).await?;
        let ProposalContent {
            block,
            round,
            outcome,
        } = content;

        if self.config.key_pair().is_some()
            && block.timestamp.duration_since(local_time) > self.config.block_time_grace_period
        {
            return Err(WorkerError::InvalidTimestamp {
                local_time,
                block_timestamp: block.timestamp,
                block_time_grace_period: self.config.block_time_grace_period,
            });
        }
        // Note: WorkerState::handle_block_proposal delays processing proposals with future
        // timestamps (within the grace period) before acquiring the chain lock. By the time
        // we reach here, the block timestamp should be in the past or very close to current time.

        self.chain
            .remove_bundles_from_inboxes(block.timestamp, true, block.incoming_bundles())
            .await?;
        let block = if let Some(outcome) = outcome {
            outcome.clone().with(proposal.content.block.clone())
        } else {
            let (executed_block, _resource_tracker, _) = Box::pin(self.execute_block(
                block.clone(),
                local_time,
                round.multi_leader(),
                &published_blobs,
                BundleExecutionPolicy::committed(),
            ))
            .await?;
            executed_block
        };

        ensure!(
            !round.is_fast() || !block.has_oracle_responses(),
            WorkerError::FastBlockUsingOracles
        );
        let chain = &mut self.chain;
        // Check if the counters of tip_state would be valid.
        chain
            .tip_state
            .get_mut()
            .update_counters(&block.body.transactions, &block.body.messages)?;
        // Don't save the changes since the block is not confirmed yet.
        chain.rollback();

        // Create the vote and store it in the chain state.
        let blobs = self
            .get_required_blobs(proposal.expected_blob_ids(), block.created_blobs())
            .await?;
        let key_pair = self.config.key_pair();
        let manager = &mut self.chain.manager;
        match manager.create_vote(&proposal, block, key_pair, local_time, blobs)? {
            // Cache the value we voted on, so the client doesn't have to send it again.
            Some(Either::Left(vote)) => {
                self.block_values
                    .insert_hashed(Cow::Borrowed(vote.value.inner()));
            }
            Some(Either::Right(vote)) => {
                self.block_values
                    .insert_hashed(Cow::Borrowed(vote.value.inner()));
            }
            None => (),
        }
        self.save().await?;
        let actions = self.create_network_actions(Some(old_round)).await?;
        Ok((self.chain_info_response().await?, actions))
    }

    /// Prepares a [`ChainInfoResponse`] for a [`ChainInfoQuery`].
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id()
    ))]
    async fn prepare_chain_info_response(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, WorkerError> {
        self.initialize_and_save_if_needed().await?;
        let mut info = ChainInfo::from_chain_view(&mut self.chain).await?;
        let chain = &self.chain;
        if query.request_owner_balance == AccountOwner::CHAIN {
            info.requested_owner_balance = Some(*chain.execution_state.system.balance.get());
        } else {
            info.requested_owner_balance = chain
                .execution_state
                .system
                .balances
                .get(&query.request_owner_balance)
                .await?;
        }
        if let Some(next_block_height) = query.test_next_block_height {
            // If not, send the same error as if a block with next_block_height was proposed.
            ensure!(
                chain.tip_state.get().next_block_height == next_block_height,
                WorkerError::UnexpectedBlockHeight {
                    expected_block_height: chain.tip_state.get().next_block_height,
                    found_block_height: next_block_height,
                }
            );
        }
        if query.request_pending_message_bundles {
            let mut bundles = Vec::new();
            let nonempty_origins: Vec<ChainId> =
                chain.nonempty_inboxes.get().iter().copied().collect();
            #[cfg(with_metrics)]
            metrics::NUM_INBOXES
                .with_label_values(&[])
                .observe(nonempty_origins.len() as f64);
            let action = if *chain.execution_state.system.closed.get() {
                MessageAction::Reject
            } else {
                MessageAction::Accept
            };
            let inboxes = chain.inboxes.try_load_entries(&nonempty_origins).await?;
            for (origin, inbox) in nonempty_origins.into_iter().zip(inboxes) {
                let inbox = inbox.ok_or_else(|| {
                    ChainError::InternalError(format!("Missing inbox for origin {origin}"))
                })?;
                for bundle in inbox.added_bundles.elements().await? {
                    bundles.push(IncomingBundle {
                        origin,
                        bundle,
                        action,
                    });
                }
            }
            info.requested_pending_message_bundles = bundles;
        }
        let hashes = chain
            .block_hashes_for_heights(query.request_sent_certificate_hashes_by_heights)
            .await?;
        info.requested_sent_certificate_hashes = hashes;
        if let Some(start) = query.request_received_log_excluding_first_n {
            let start = usize::try_from(start).map_err(|_| ArithmeticError::Overflow)?;
            let max_received_log_entries = self.config.chain_info_max_received_log_entries;
            let end = start
                .saturating_add(max_received_log_entries)
                .min(chain.received_log.count());
            info.requested_received_log = chain.received_log.read(start..end).await?;
        }
        if query.request_manager_values {
            info.manager.add_values(&chain.manager);
        }
        if !query.request_previous_event_blocks.is_empty() {
            let stream_ids = query.request_previous_event_blocks;
            let heights = chain
                .execution_state
                .previous_event_blocks
                .multi_get(&stream_ids)
                .await?;
            let mut streams_with_heights = Vec::new();
            for (stream_id, height) in stream_ids.into_iter().zip(heights) {
                if let Some(height) = height {
                    streams_with_heights.push((stream_id, height));
                }
            }
            let hashes = chain
                .block_hashes
                .multi_get(streams_with_heights.iter().map(|(_, height)| height))
                .await?;
            for (maybe_hash, (stream_id, height)) in hashes.into_iter().zip(streams_with_heights) {
                let hash = maybe_hash.ok_or_else(|| WorkerError::BlockHashNotFound {
                    height,
                    chain_id: info.chain_id,
                })?;
                info.requested_previous_event_blocks
                    .insert(stream_id, (height, hash));
            }
        }
        if query.request_latest_checkpoint_height {
            info.requested_latest_checkpoint_height = *self.chain.latest_checkpoint_height.get();
        }
        Ok(ChainInfoResponse::new(info, self.config.key_pair()))
    }

    /// Executes a block with a specified policy for handling bundle failures.
    ///
    /// The block may be modified to reflect the actual executed transactions.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        block_height = %block.height
    ))]
    async fn execute_block(
        &mut self,
        block: ProposedBlock,
        local_time: Timestamp,
        round: Option<u32>,
        published_blobs: &[Blob],
        policy: BundleExecutionPolicy,
    ) -> Result<(Block, ResourceTracker, HashSet<ChainId>), WorkerError> {
        let (proposed_block, outcome, resource_tracker, never_reject_origins) = Box::pin(
            self.chain
                .execute_block(block, local_time, round, published_blobs, None, policy),
        )
        .await?;
        let executed_block = Block::new(proposed_block, outcome);
        let block_hash = CryptoHash::new(&executed_block);
        if let Some(cache) = &self.execution_state_cache {
            cache.insert(
                &block_hash,
                Box::pin(
                    self.chain
                        .execution_state
                        .with_context(|ctx| InactiveContext(ctx.base_key().clone())),
                )
                .await,
            );
        }
        Ok((executed_block, resource_tracker, never_reject_origins))
    }

    /// Initializes and saves the current chain if it is not active yet.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id()
    ))]
    pub(crate) async fn initialize_and_save_if_needed(&mut self) -> Result<(), WorkerError> {
        if !self.knows_chain_is_active {
            let local_time = self.storage.clock().current_time();
            self.chain.initialize_if_needed(local_time).await?;
            self.save().await?;
            self.knows_chain_is_active = true;
        }
        Ok(())
    }

    pub(crate) async fn chain_info_response(&mut self) -> Result<ChainInfoResponse, WorkerError> {
        let info = ChainInfo::from_chain_view(&mut self.chain).await?;
        Ok(ChainInfoResponse::new(info, self.config.key_pair()))
    }

    /// Stores the chain state in persistent storage.
    ///
    /// If the save fails, the worker is marked as poisoned and must be reloaded.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id()
    ))]
    pub(crate) async fn save(&mut self) -> Result<(), WorkerError> {
        if let Err(error) = self.chain.save().await {
            if error.must_reload_view() {
                tracing::error!(
                    ?error,
                    chain_id = %self.chain_id(),
                    "Chain save failed with a nonrecoverable error; marking worker as poisoned"
                );
                self.poisoned = true;
            }
            return Err(WorkerError::ViewError(error));
        }
        Ok(())
    }

    /// Deletes every key under this chain's storage root and replaces `self.chain`
    /// with a freshly loaded (empty) view. If either the write or the reload fails,
    /// the worker is marked as poisoned: the partial deletion may have left
    /// storage inconsistent with the in-memory view, so it cannot be reused.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id()
    ))]
    async fn wipe_and_reload_chain(&mut self) -> Result<(), WorkerError> {
        let context = self.chain.context().clone();
        let mut batch = Batch::new();
        batch.delete_key_prefix(Vec::new());
        if let Err(error) = context.store().write_batch(batch).await {
            tracing::error!(
                ?error,
                chain_id = %self.chain_id(),
                "Wiping chain storage failed; marking worker as poisoned"
            );
            self.poisoned = true;
            return Err(WorkerError::PoisonedWorker);
        }
        match ChainStateView::load(context).await {
            Ok(chain) => {
                self.chain = chain;
                Ok(())
            }
            Err(error) => {
                tracing::error!(
                    ?error,
                    chain_id = %self.chain_id(),
                    "Reloading chain after wipe failed; marking worker as poisoned"
                );
                self.poisoned = true;
                Err(WorkerError::PoisonedWorker)
            }
        }
    }
}

/// Sends a result through a oneshot channel, logging at `debug` level if the
/// receiver has been dropped.
pub(crate) fn send_result<T>(sender: oneshot::Sender<T>, value: T) {
    if sender.send(value).is_err() {
        tracing::debug!("cannot send cross-chain result; receiver dropped");
    }
}

/// Returns the missing indices and corresponding blob_ids.
fn missing_indices_blob_ids(maybe_blobs: &[(BlobId, Option<Blob>)]) -> (Vec<usize>, Vec<BlobId>) {
    let mut missing_indices = Vec::new();
    let mut missing_blob_ids = Vec::new();
    for (index, (blob_id, blob)) in maybe_blobs.iter().enumerate() {
        if blob.is_none() {
            missing_indices.push(index);
            missing_blob_ids.push(*blob_id);
        }
    }
    (missing_indices, missing_blob_ids)
}

/// Returns the blob IDs whose corresponding value is `None`.
fn missing_blob_ids<'a>(
    maybe_blobs: impl IntoIterator<Item = (&'a BlobId, &'a Option<Blob>)>,
) -> Vec<BlobId> {
    maybe_blobs
        .into_iter()
        .filter(|(_, maybe_blob)| maybe_blob.is_none())
        .map(|(blob_id, _)| *blob_id)
        .collect()
}

/// Returns an error if the block is not at the expected epoch.
fn check_block_epoch(
    chain_epoch: Epoch,
    block_chain: ChainId,
    block_epoch: Epoch,
) -> Result<(), WorkerError> {
    ensure!(
        block_epoch == chain_epoch,
        WorkerError::InvalidEpoch {
            chain_id: block_chain,
            epoch: block_epoch,
            chain_epoch
        }
    );
    Ok(())
}

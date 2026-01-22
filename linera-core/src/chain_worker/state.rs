// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The state and functionality of a chain worker.

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{self, Arc},
};

use futures::future::Either;
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use linera_base::{
    crypto::{CryptoHash, ValidatorPublicKey},
    data_types::{
        ApplicationDescription, ArithmeticError, Blob, BlockHeight, Epoch, Round, Timestamp,
    },
    ensure,
    hashed::Hashed,
    identifiers::{AccountOwner, ApplicationId, BlobId, ChainId, EventId, StreamId},
};
use linera_chain::{
    data_types::{
        BlockProposal, IncomingBundle, MessageAction, MessageBundle, OriginalProposal,
        ProposalContent, ProposedBlock,
    },
    manager,
    types::{Block, ConfirmedBlockCertificate, TimeoutCertificate, ValidatedBlockCertificate},
    ChainError, ChainExecutionContext, ChainStateView, ExecutionResultExt as _,
};
use linera_execution::{
    system::{EpochEventData, EPOCH_STREAM_NAME},
    Committee, ExecutionRuntimeContext as _, ExecutionStateView, Query, QueryContext, QueryOutcome,
    ResourceTracker, ServiceRuntimeEndpoint,
};
use linera_storage::{Clock as _, ResultReadCertificates, Storage};
use linera_views::{
    context::{Context, InactiveContext},
    views::{ClonableView, ReplaceContext as _, RootView as _, View as _},
};
use tokio::sync::{oneshot, OwnedRwLockReadGuard, RwLock, RwLockWriteGuard};
use tracing::{debug, instrument, trace, warn};

use super::{ChainWorkerConfig, ChainWorkerRequest, DeliveryNotifier, EventSubscriptionsResult};
use crate::{
    client::ListeningMode,
    data_types::{ChainInfo, ChainInfoQuery, ChainInfoResponse, CrossChainRequest},
    value_cache::ValueCache,
    worker::{NetworkActions, Notification, Reason, WorkerError},
};

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{exponential_bucket_latencies, register_histogram};
    use prometheus::Histogram;

    pub static CREATE_NETWORK_ACTIONS_LATENCY: LazyLock<Histogram> = LazyLock::new(|| {
        register_histogram(
            "create_network_actions_latency",
            "Time (ms) to create network actions",
            exponential_bucket_latencies(10_000.0),
        )
    });
}

/// The state of the chain worker.
pub(crate) struct ChainWorkerState<StorageClient>
where
    StorageClient: Storage + Clone + 'static,
{
    config: ChainWorkerConfig,
    storage: StorageClient,
    chain: ChainStateView<StorageClient::Context>,
    shared_chain_view: Option<Arc<RwLock<ChainStateView<StorageClient::Context>>>>,
    service_runtime_endpoint: Option<ServiceRuntimeEndpoint>,
    block_values: Arc<ValueCache<CryptoHash, Hashed<Block>>>,
    execution_state_cache: Arc<ValueCache<CryptoHash, ExecutionStateView<InactiveContext>>>,
    chain_modes: Option<Arc<sync::RwLock<BTreeMap<ChainId, ListeningMode>>>>,
    delivery_notifier: DeliveryNotifier,
    knows_chain_is_active: bool,
}

/// Whether the block was processed or skipped. Used for metrics.
pub enum BlockOutcome {
    Processed,
    Preprocessed,
    Skipped,
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
    pub(super) async fn load(
        config: ChainWorkerConfig,
        storage: StorageClient,
        block_values: Arc<ValueCache<CryptoHash, Hashed<Block>>>,
        execution_state_cache: Arc<ValueCache<CryptoHash, ExecutionStateView<InactiveContext>>>,
        chain_modes: Option<Arc<sync::RwLock<BTreeMap<ChainId, ListeningMode>>>>,
        delivery_notifier: DeliveryNotifier,
        chain_id: ChainId,
        service_runtime_endpoint: Option<ServiceRuntimeEndpoint>,
    ) -> Result<Self, WorkerError> {
        let chain = storage.load_chain(chain_id).await?;

        Ok(ChainWorkerState {
            config,
            storage,
            chain,
            shared_chain_view: None,
            service_runtime_endpoint,
            block_values,
            execution_state_cache,
            chain_modes,
            delivery_notifier,
            knows_chain_is_active: false,
        })
    }

    /// Returns the [`ChainId`] of the chain handled by this worker.
    fn chain_id(&self) -> ChainId {
        self.chain.chain_id()
    }

    /// Handles a request and applies it to the chain state.
    #[instrument(skip_all, fields(chain_id = %self.chain_id()))]
    pub(super) async fn handle_request(
        &mut self,
        request: ChainWorkerRequest<StorageClient::Context>,
    ) {
        tracing::trace!("Handling chain worker request: {request:?}");
        // TODO(#2237): Spawn concurrent tasks for read-only operations
        let responded = match request {
            #[cfg(with_testing)]
            ChainWorkerRequest::ReadCertificate { height, callback } => {
                callback.send(self.read_certificate(height).await).is_ok()
            }
            ChainWorkerRequest::GetChainStateView { callback } => {
                callback.send(self.chain_state_view().await).is_ok()
            }
            ChainWorkerRequest::QueryApplication {
                query,
                block_hash,
                callback,
            } => callback
                .send(self.query_application(query, block_hash).await)
                .is_ok(),
            ChainWorkerRequest::DescribeApplication {
                application_id,
                callback,
            } => callback
                .send(self.describe_application(application_id).await)
                .is_ok(),
            ChainWorkerRequest::StageBlockExecution {
                block,
                round,
                published_blobs,
                callback,
            } => callback
                .send(
                    self.stage_block_execution(block, round, &published_blobs)
                        .await,
                )
                .is_ok(),
            ChainWorkerRequest::ProcessTimeout {
                certificate,
                callback,
            } => callback
                .send(self.process_timeout(certificate).await)
                .is_ok(),
            ChainWorkerRequest::HandleBlockProposal { proposal, callback } => callback
                .send(self.handle_block_proposal(proposal).await)
                .is_ok(),
            ChainWorkerRequest::ProcessValidatedBlock {
                certificate,
                callback,
            } => callback
                .send(self.process_validated_block(certificate).await)
                .is_ok(),
            ChainWorkerRequest::ProcessConfirmedBlock {
                certificate,
                notify_when_messages_are_delivered,
                callback,
            } => callback
                .send(
                    self.process_confirmed_block(certificate, notify_when_messages_are_delivered)
                        .await,
                )
                .is_ok(),
            ChainWorkerRequest::ProcessCrossChainUpdate {
                origin,
                bundles,
                callback,
            } => callback
                .send(self.process_cross_chain_update(origin, bundles).await)
                .is_ok(),
            ChainWorkerRequest::ConfirmUpdatedRecipient {
                recipient,
                latest_height,
                callback,
            } => callback
                .send(
                    self.confirm_updated_recipient(recipient, latest_height)
                        .await,
                )
                .is_ok(),
            ChainWorkerRequest::HandleChainInfoQuery { query, callback } => callback
                .send(self.handle_chain_info_query(query).await)
                .is_ok(),
            ChainWorkerRequest::DownloadPendingBlob { blob_id, callback } => callback
                .send(self.download_pending_blob(blob_id).await)
                .is_ok(),
            ChainWorkerRequest::HandlePendingBlob { blob, callback } => {
                callback.send(self.handle_pending_blob(blob).await).is_ok()
            }
            ChainWorkerRequest::UpdateReceivedCertificateTrackers {
                new_trackers,
                callback,
            } => callback
                .send(
                    self.update_received_certificate_trackers(new_trackers)
                        .await,
                )
                .is_ok(),
            ChainWorkerRequest::GetPreprocessedBlockHashes {
                start,
                end,
                callback,
            } => callback
                .send(self.get_preprocessed_block_hashes(start, end).await)
                .is_ok(),
            ChainWorkerRequest::GetInboxNextHeight { origin, callback } => callback
                .send(self.get_inbox_next_height(origin).await)
                .is_ok(),
            ChainWorkerRequest::GetLockingBlobs { blob_ids, callback } => callback
                .send(self.get_locking_blobs(blob_ids).await)
                .is_ok(),
            ChainWorkerRequest::GetBlockHashes { heights, callback } => {
                callback.send(self.get_block_hashes(heights).await).is_ok()
            }
            ChainWorkerRequest::GetProposedBlobs { blob_ids, callback } => callback
                .send(self.get_proposed_blobs(blob_ids).await)
                .is_ok(),
            ChainWorkerRequest::GetEventSubscriptions { callback } => {
                callback.send(self.get_event_subscriptions().await).is_ok()
            }
            ChainWorkerRequest::GetNextExpectedEvent {
                stream_id,
                callback,
            } => callback
                .send(self.get_next_expected_event(stream_id).await)
                .is_ok(),
            ChainWorkerRequest::GetReceivedCertificateTrackers { callback } => callback
                .send(self.get_received_certificate_trackers().await)
                .is_ok(),
            ChainWorkerRequest::GetTipStateAndOutboxInfo {
                receiver_id,
                callback,
            } => callback
                .send(self.get_tip_state_and_outbox_info(receiver_id).await)
                .is_ok(),
            ChainWorkerRequest::GetNextHeightToPreprocess { callback } => callback
                .send(self.get_next_height_to_preprocess().await)
                .is_ok(),
        };

        if !responded {
            debug!("Callback for `ChainWorkerActor` was dropped before a response was sent");
        }

        // Roll back any unsaved changes to the chain state: If there was an error while trying
        // to handle the request, the chain state might contain unsaved and potentially invalid
        // changes. The next request needs to be applied to the chain state as it is in storage.
        self.chain.rollback();
    }

    /// Returns a read-only view of the [`ChainStateView`].
    ///
    /// The returned view holds a lock on the chain state, which prevents the worker from changing
    /// it.
    async fn chain_state_view(
        &mut self,
    ) -> Result<OwnedRwLockReadGuard<ChainStateView<StorageClient::Context>>, WorkerError> {
        if self.shared_chain_view.is_none() {
            self.shared_chain_view = Some(Arc::new(RwLock::new(self.chain.clone_unchecked()?)));
        }

        Ok(self
            .shared_chain_view
            .as_ref()
            .expect("`shared_chain_view` should be initialized above")
            .clone()
            .read_owned()
            .await)
    }

    /// Clears the shared chain view, and acquires and drops its write lock.
    ///
    /// This is the only place a write lock is acquired, and read locks are acquired in
    /// the `chain_state_view` method, which has a `&mut self` receiver like this one.
    /// That means that when this function returns, no readers will be waiting to acquire
    /// the lock and it is safe to write the chain state to storage without any readers
    /// having a stale view of it.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id()
    ))]
    pub(super) async fn clear_shared_chain_view(&mut self) {
        if let Some(shared_chain_view) = self.shared_chain_view.take() {
            let _: RwLockWriteGuard<_> = shared_chain_view.write().await;
        }
    }

    /// Handles a [`ChainInfoQuery`], potentially voting on the next block.
    #[tracing::instrument(level = "debug", skip(self))]
    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        let create_network_actions = query.create_network_actions;
        if let Some((height, round)) = query.request_leader_timeout {
            self.vote_for_leader_timeout(height, round).await?;
        }
        if query.request_fallback {
            self.vote_for_fallback().await?;
        }
        let response = self.prepare_chain_info_response(query).await?;
        // Trigger any outgoing cross-chain messages that haven't been confirmed yet.
        let actions = if create_network_actions {
            self.create_network_actions(None).await?
        } else {
            NetworkActions::default()
        };
        Ok((response, actions))
    }

    /// Returns the requested blob, if it belongs to the current locking block or pending proposal.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        blob_id = %blob_id
    ))]
    async fn download_pending_blob(&self, blob_id: BlobId) -> Result<Blob, WorkerError> {
        if let Some(blob) = self.chain.manager.pending_blob(&blob_id).await? {
            return Ok(blob);
        }
        let blob = self.storage.read_blob(blob_id).await?;
        blob.ok_or(WorkerError::BlobsNotFound(vec![blob_id]))
    }

    /// Reads the blobs from the chain manager or from storage. Returns an error if any are
    /// missing.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id()
    ))]
    async fn get_required_blobs(
        &self,
        required_blob_ids: impl IntoIterator<Item = BlobId>,
        created_blobs: &BTreeMap<BlobId, Blob>,
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
        created_blobs: Option<&BTreeMap<BlobId, Blob>>,
    ) -> Result<BTreeMap<BlobId, Option<Blob>>, WorkerError> {
        let maybe_blobs = blob_ids.into_iter().collect::<BTreeSet<_>>();
        let mut maybe_blobs = maybe_blobs
            .into_iter()
            .map(|x| (x, None))
            .collect::<Vec<(BlobId, Option<Blob>)>>();

        if let Some(blob_map) = created_blobs {
            for (blob_id, value) in &mut maybe_blobs {
                if let Some(blob) = blob_map.get(blob_id) {
                    *value = Some(blob.clone());
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
            maybe_blobs[index].1 = blob;
        }
        Ok(maybe_blobs.into_iter().collect())
    }

    /// Loads pending cross-chain requests, and adds `NewRound` notifications where appropriate.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id()
    ))]
    async fn create_network_actions(
        &self,
        old_round: Option<Round>,
    ) -> Result<NetworkActions, WorkerError> {
        #[cfg(with_metrics)]
        let _latency = metrics::CREATE_NETWORK_ACTIONS_LATENCY.measure_latency();
        let mut heights_by_recipient = BTreeMap::<_, Vec<_>>::new();
        let mut targets = self.chain.nonempty_outbox_chain_ids();
        if let Some(chain_modes) = self.chain_modes.as_ref() {
            let chain_modes = chain_modes
                .read()
                .expect("Panics should not happen while holding a lock to `chain_modes`");
            // Only process outboxes for full chains (those whose inboxes we update).
            targets.retain(|target| chain_modes.get(target).is_some_and(ListeningMode::is_full));
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

    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        num_recipients = %heights_by_recipient.len()
    ))]
    async fn create_cross_chain_requests(
        &self,
        heights_by_recipient: BTreeMap<ChainId, Vec<BlockHeight>>,
    ) -> Result<Vec<CrossChainRequest>, WorkerError> {
        // Load all the certificates we will need, regardless of the medium.
        let heights = BTreeSet::from_iter(heights_by_recipient.values().flatten().copied());
        let next_block_height = self.chain.tip_state.get().next_block_height;
        let log_heights = heights
            .range(..next_block_height)
            .copied()
            .map(usize::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let mut hashes = self
            .chain
            .confirmed_log
            .multi_get(log_heights)
            .await?
            .into_iter()
            .zip(&heights)
            .map(|(maybe_hash, height)| {
                maybe_hash.ok_or_else(|| WorkerError::ConfirmedLogEntryNotFound {
                    height: *height,
                    chain_id: self.chain_id(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let requested_heights: Vec<BlockHeight> = heights
            .range(next_block_height..)
            .copied()
            .collect::<Vec<BlockHeight>>();
        for (height, hash) in self
            .chain
            .preprocessed_blocks
            .multi_get_pairs(requested_heights)
            .await?
        {
            let hash = hash.ok_or_else(|| WorkerError::PreprocessedBlocksEntryNotFound {
                height,
                chain_id: self.chain_id(),
            })?;
            hashes.push(hash);
        }

        let mut uncached_hashes = Vec::new();
        let mut height_to_blocks: HashMap<BlockHeight, Hashed<Block>> = HashMap::new();

        for hash in hashes {
            if let Some(hashed_block) = self.block_values.get(&hash) {
                height_to_blocks.insert(hashed_block.inner().header.height, hashed_block);
            } else {
                uncached_hashes.push(hash);
            }
        }

        if !uncached_hashes.is_empty() {
            let certificates = self.storage.read_certificates(&uncached_hashes).await?;
            let certificates = match ResultReadCertificates::new(certificates, uncached_hashes) {
                ResultReadCertificates::Certificates(certificates) => certificates,
                ResultReadCertificates::InvalidHashes(hashes) => {
                    return Err(WorkerError::ReadCertificatesError(hashes))
                }
            };

            for cert in certificates {
                let hashed_block = cert.into_value().into_inner();
                let height = hashed_block.inner().header.height;
                self.block_values.insert(Cow::Owned(hashed_block.clone()));
                height_to_blocks.insert(height, hashed_block);
            }
        }

        let mut cross_chain_requests = Vec::new();
        for (recipient, heights) in heights_by_recipient {
            let mut bundles = Vec::new();
            for height in heights {
                let hashed_block = height_to_blocks
                    .get(&height)
                    .ok_or_else(|| ChainError::InternalError("missing block".to_string()))?;
                bundles.extend(
                    hashed_block
                        .inner()
                        .message_bundles_for(recipient, hashed_block.hash()),
                );
            }
            let request = CrossChainRequest::UpdateRecipient {
                sender: self.chain.chain_id(),
                recipient,
                bundles,
            };
            cross_chain_requests.push(request);
        }
        Ok(cross_chain_requests)
    }

    /// Returns true if there are no more outgoing messages in flight up to the given
    /// block height for all tracked chains (those whose inboxes we update).
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        height = %height
    ))]
    async fn all_messages_to_tracked_chains_delivered_up_to(
        &self,
        height: BlockHeight,
    ) -> Result<bool, WorkerError> {
        if self.chain.all_messages_delivered_up_to(height) {
            return Ok(true);
        }
        let Some(chain_modes) = self.chain_modes.as_ref() else {
            return Ok(false);
        };
        let mut targets = self.chain.nonempty_outbox_chain_ids();
        {
            let chain_modes = chain_modes.read().unwrap();
            // Only consider full chains (those whose inboxes we update).
            targets.retain(|target| chain_modes.get(target).is_some_and(ListeningMode::is_full));
        }
        let outboxes = self.chain.load_outboxes(&targets).await?;
        for outbox in outboxes {
            let front = outbox.queue.front().await?;
            if front.is_some_and(|key| key <= height) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Processes a leader timeout issued for this multi-owner chain.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        height = %certificate.inner().height()
    ))]
    async fn process_timeout(
        &mut self,
        certificate: TimeoutCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        // Check that the chain is active and ready for this timeout.
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        self.initialize_and_save_if_needed().await?;
        let (chain_epoch, committee) = self.chain.current_committee()?;
        certificate.check(committee)?;
        if self
            .chain
            .tip_state
            .get()
            .already_validated_block(certificate.inner().height())?
        {
            return Ok((self.chain_info_response(), NetworkActions::default()));
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
        Ok((self.chain_info_response(), actions))
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
            if chain.ownership().open_multi_leader_rounds {
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
    async fn process_validated_block(
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
        let (epoch, committee) = self.chain.current_committee()?;
        check_block_epoch(epoch, header.chain_id, header.epoch)?;
        certificate.check(committee)?;
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
                self.chain_info_response(),
                NetworkActions::default(),
                BlockOutcome::Skipped,
            ));
        }

        self.block_values
            .insert(Cow::Borrowed(certificate.inner().inner()));
        let required_blob_ids = block.required_blob_ids();
        let maybe_blobs = self
            .maybe_get_required_blobs(required_blob_ids, Some(&block.created_blobs()))
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
        Ok((self.chain_info_response(), actions, BlockOutcome::Processed))
    }

    /// Processes a confirmed block (aka a commit).
    #[instrument(skip_all, fields(
        chain_id = %certificate.block().header.chain_id,
        height = %certificate.block().header.height,
        block_hash = %certificate.hash(),
    ))]
    async fn process_confirmed_block(
        &mut self,
        certificate: ConfirmedBlockCertificate,
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions, BlockOutcome), WorkerError> {
        let block = certificate.block();
        let block_hash = certificate.hash();
        let height = block.header.height;
        let chain_id = block.header.chain_id;

        // Check that the chain is active and ready for this confirmation.
        let tip = self.chain.tip_state.get().clone();
        if tip.next_block_height > height {
            // We already processed this block.
            let actions = self.create_network_actions(None).await?;
            self.register_delivery_notifier(height, &actions, notify_when_messages_are_delivered)
                .await;
            return Ok((self.chain_info_response(), actions, BlockOutcome::Skipped));
        }

        // We haven't processed the block - verify the certificate first
        let epoch = block.header.epoch;
        // Get the committee for the block's epoch from storage.
        if let Some(committee) = self
            .chain
            .execution_state
            .system
            .committees
            .get()
            .get(&epoch)
        {
            certificate.check(committee)?;
        } else {
            let committee = self
                .chain
                .execution_state
                .context()
                .extra()
                .get_committees(epoch..=epoch)
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
            certificate.check(&committee)?;
        }

        // Certificate check passed - which means the blobs the block requires are legitimate and
        // we can take note of it, so that if any are missing, we will accept them when the client
        // sends them.
        let required_blob_ids = block.required_blob_ids();
        let created_blobs: BTreeMap<_, _> = block.iter_created_blobs().collect();
        let blobs_result = self
            .get_required_blobs(required_blob_ids.iter().copied(), &created_blobs)
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

        let mut blobs = blobs_result?
            .into_iter()
            .map(|blob| (blob.id(), blob))
            .collect::<BTreeMap<_, _>>();

        // If this block is higher than the next expected block in this chain, we're going
        // to have a gap: do not execute this block, only update the outboxes and return.
        if tip.next_block_height < height {
            // Update the outboxes and event streams.
            let updated_event_streams = self.chain.preprocess_block(certificate.value()).await?;
            // Persist chain.
            self.save().await?;
            let mut actions = self.create_network_actions(None).await?;
            if !updated_event_streams.is_empty() {
                actions.notifications.push(Notification {
                    chain_id,
                    reason: Reason::NewEvents {
                        height,
                        hash: certificate.hash(),
                        event_streams: updated_event_streams,
                    },
                });
            }
            trace!("Preprocessed confirmed block {height} on chain {chain_id:.8}");
            self.register_delivery_notifier(height, &actions, notify_when_messages_are_delivered)
                .await;
            return Ok((
                self.chain_info_response(),
                actions,
                BlockOutcome::Preprocessed,
            ));
        }

        // This should always be true for valid certificates.
        ensure!(
            tip.block_hash == block.header.previous_block_hash,
            WorkerError::InvalidBlockChaining
        );

        // If we got here, `height` is equal to `tip.next_block_height` and the block is
        // properly chained. Verify that the chain is active and that the epoch we used for
        // verifying the certificate is actually the active one on the chain.
        self.initialize_and_save_if_needed().await?;
        let (epoch, _) = self.chain.current_committee()?;
        check_block_epoch(epoch, chain_id, block.header.epoch)?;

        let published_blobs = block
            .published_blob_ids()
            .iter()
            .filter_map(|blob_id| blobs.remove(blob_id))
            .collect::<Vec<_>>();

        // Execute the block and update inboxes.
        let local_time = self.storage.clock().current_time();
        if block.header.timestamp.duration_since(local_time) > self.config.block_time_grace_period {
            warn!(
                block_timestamp = %block.header.timestamp,
                %local_time,
                "Confirmed block has a timestamp in the future beyond the block time grace period"
            );
        }
        let chain = &mut self.chain;
        chain
            .remove_bundles_from_inboxes(
                block.header.timestamp,
                false,
                block.body.incoming_bundles(),
            )
            .await?;
        let oracle_responses = Some(block.body.oracle_responses.clone());
        let (proposed_block, outcome) = block.clone().into_proposal();
        let verified_outcome =
            if let Some(mut execution_state) = self.execution_state_cache.remove(&block_hash) {
                chain.execution_state = execution_state
                    .with_context(|ctx| {
                        chain
                            .execution_state
                            .context()
                            .clone_with_base_key(ctx.base_key().bytes.clone())
                    })
                    .await;
                outcome.clone()
            } else {
                let (verified, _resource_tracker) = chain
                    .execute_block(
                        &proposed_block,
                        local_time,
                        None,
                        &published_blobs,
                        oracle_responses,
                    )
                    .await?;
                verified
            };
        // We should always agree on the messages and state hash.
        ensure!(
            outcome == verified_outcome,
            WorkerError::IncorrectOutcome {
                submitted: Box::new(outcome),
                computed: Box::new(verified_outcome),
            }
        );

        // Update the rest of the chain state.
        let updated_streams = chain
            .apply_confirmed_block(certificate.value(), local_time)
            .await?;
        let mut actions = self.create_network_actions(None).await?;
        trace!("Processed confirmed block {height} on chain {chain_id:.8}");
        let hash = certificate.hash();
        actions.notifications.push(Notification {
            chain_id,
            reason: Reason::NewBlock { height, hash },
        });
        if !updated_streams.is_empty() {
            actions.notifications.push(Notification {
                chain_id,
                reason: Reason::NewEvents {
                    height,
                    hash,
                    event_streams: updated_streams,
                },
            });
        }
        // Persist chain.
        self.save().await?;

        self.block_values
            .insert(Cow::Owned(certificate.into_inner().into_inner()));

        self.register_delivery_notifier(height, &actions, notify_when_messages_are_delivered)
            .await;

        Ok((self.chain_info_response(), actions, BlockOutcome::Processed))
    }

    /// Schedules a notification for when cross-chain messages are delivered up to the given
    /// `height`.
    #[instrument(level = "trace", skip(self, notify_when_messages_are_delivered))]
    async fn register_delivery_notifier(
        &mut self,
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
                    warn!("Failed to notify message delivery to caller");
                }
            }
        }
    }

    /// Updates the chain's inboxes, receiving messages from a cross-chain update.
    #[instrument(level = "trace", skip(self, bundles))]
    async fn process_cross_chain_update(
        &mut self,
        origin: ChainId,
        bundles: Vec<(Epoch, MessageBundle)>,
    ) -> Result<Option<BlockHeight>, WorkerError> {
        // Only process certificates with relevant heights and epochs.
        let next_height_to_receive = self.chain.next_block_height_to_receive(&origin).await?;
        let last_anticipated_block_height =
            self.chain.last_anticipated_block_height(&origin).await?;
        let helper = CrossChainUpdateHelper::new(&self.config, &self.chain);
        let recipient = self.chain_id();
        let bundles = helper.select_message_bundles(
            &origin,
            recipient,
            next_height_to_receive,
            last_anticipated_block_height,
            bundles,
        )?;
        let Some(last_updated_height) = bundles.last().map(|bundle| bundle.height) else {
            return Ok(None);
        };
        // Process the received messages in certificates.
        let local_time = self.storage.clock().current_time();
        let mut previous_height = None;
        for bundle in bundles {
            let add_to_received_log = previous_height != Some(bundle.height);
            previous_height = Some(bundle.height);
            // Update the staged chain state with the received block.
            self.chain
                .receive_message_bundle(&origin, bundle, local_time, add_to_received_log)
                .await?;
        }
        if !self.config.allow_inactive_chains && !self.chain.is_active() {
            // Refuse to create a chain state if the chain is still inactive by
            // now. Accordingly, do not send a confirmation, so that the
            // cross-chain update is retried later.
            warn!(
                "Refusing to deliver messages to {recipient:?} from {origin:?} \
                at height {last_updated_height} because the recipient is still inactive",
            );
            return Ok(None);
        }
        // Save the chain.
        self.save().await?;
        Ok(Some(last_updated_height))
    }

    /// Handles the cross-chain request confirming that the recipient was updated.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        recipient = %recipient,
        latest_height = %latest_height
    ))]
    async fn confirm_updated_recipient(
        &mut self,
        recipient: ChainId,
        latest_height: BlockHeight,
    ) -> Result<(), WorkerError> {
        let fully_delivered = self
            .chain
            .mark_messages_as_received(&recipient, latest_height)
            .await?
            && self
                .all_messages_to_tracked_chains_delivered_up_to(latest_height)
                .await?;

        self.save().await?;

        if fully_delivered {
            self.delivery_notifier.notify(latest_height);
        }

        Ok(())
    }

    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        num_trackers = %new_trackers.len()
    ))]
    async fn update_received_certificate_trackers(
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
    async fn get_preprocessed_block_hashes(
        &self,
        start: BlockHeight,
        end: BlockHeight,
    ) -> Result<Vec<CryptoHash>, WorkerError> {
        let mut hashes = Vec::new();
        let mut height = start;
        while height < end {
            match self.chain.preprocessed_blocks.get(&height).await? {
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
    async fn get_inbox_next_height(&self, origin: ChainId) -> Result<BlockHeight, WorkerError> {
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
    async fn get_locking_blobs(
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
    async fn get_block_hashes(
        &self,
        heights: Vec<BlockHeight>,
    ) -> Result<Vec<CryptoHash>, WorkerError> {
        Ok(self.chain.block_hashes(heights).await?)
    }

    /// Gets proposed blobs from the manager for specified blob IDs.
    async fn get_proposed_blobs(&self, blob_ids: Vec<BlobId>) -> Result<Vec<Blob>, WorkerError> {
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
    async fn get_event_subscriptions(&self) -> Result<EventSubscriptionsResult, WorkerError> {
        Ok(self
            .chain
            .execution_state
            .system
            .event_subscriptions
            .index_values()
            .await?)
    }

    /// Gets the next expected event index for a stream.
    async fn get_next_expected_event(
        &self,
        stream_id: StreamId,
    ) -> Result<Option<u32>, WorkerError> {
        Ok(self.chain.next_expected_events.get(&stream_id).await?)
    }

    /// Gets received certificate trackers.
    async fn get_received_certificate_trackers(
        &self,
    ) -> Result<HashMap<ValidatorPublicKey, u64>, WorkerError> {
        Ok(self.chain.received_certificate_trackers.get().clone())
    }

    /// Gets tip state and outbox info for next_outbox_heights calculation.
    async fn get_tip_state_and_outbox_info(
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
    async fn get_next_height_to_preprocess(&self) -> Result<BlockHeight, WorkerError> {
        Ok(self.chain.next_height_to_preprocess().await?)
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
        if elapsed >= chain.ownership().timeout_config.fallback_duration {
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
    async fn handle_pending_blob(&mut self, blob: Blob) -> Result<ChainInfoResponse, WorkerError> {
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
                let (_, committee) = self.chain.current_committee()?;
                let policy = committee.policy();
                policy
                    .check_blob_size(blob.content())
                    .with_execution_context(ChainExecutionContext::Block)?;
                ensure!(
                    u64::try_from(pending_blobs.pending_blobs.count().await?)
                        .is_ok_and(|count| count < policy.maximum_published_blobs),
                    WorkerError::TooManyPublishedBlobs(policy.maximum_published_blobs)
                );
            }
            was_expected = was_expected || pending_blobs.maybe_insert(&blob).await?;
        }
        ensure!(was_expected, WorkerError::UnexpectedBlob);
        self.save().await?;
        Ok(self.chain_info_response())
    }

    /// Returns a stored [`Certificate`] for the chain's block at the requested [`BlockHeight`].
    #[cfg(with_testing)]
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        height = %height
    ))]
    async fn read_certificate(
        &mut self,
        height: BlockHeight,
    ) -> Result<Option<ConfirmedBlockCertificate>, WorkerError> {
        self.initialize_and_save_if_needed().await?;
        let certificate_hash = match self.chain.confirmed_log.get(height.try_into()?).await? {
            Some(hash) => hash,
            None => return Ok(None),
        };
        let certificate = self
            .storage
            .read_certificate(certificate_hash)
            .await?
            .ok_or_else(|| WorkerError::ReadCertificatesError(vec![certificate_hash]))?;
        Ok(Some(certificate))
    }

    /// Queries an application's state on the chain.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        query_application_id = %query.application_id()
    ))]
    pub(super) async fn query_application(
        &mut self,
        query: Query,
        block_hash: Option<CryptoHash>,
    ) -> Result<QueryOutcome, WorkerError> {
        self.initialize_and_save_if_needed().await?;
        let local_time = self.storage.clock().current_time();
        if let Some(requested_block) = block_hash {
            if let Some(mut state) = self.execution_state_cache.remove(&requested_block) {
                // We try to use a cached execution state for the requested block.
                // We want to pretend that this block is committed, so we set the next block height.
                let next_block_height = self
                    .chain
                    .tip_state
                    .get()
                    .next_block_height
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
                self.execution_state_cache
                    .insert_owned(&requested_block, state);
                Ok(outcome)
            } else {
                tracing::debug!(requested_block = %requested_block, "requested block hash not found in cache, querying committed state");
                let outcome = self
                    .chain
                    .query_application(local_time, query, self.service_runtime_endpoint.as_mut())
                    .await?;
                Ok(outcome)
            }
        } else {
            let outcome = self
                .chain
                .query_application(local_time, query, self.service_runtime_endpoint.as_mut())
                .await?;
            Ok(outcome)
        }
    }

    /// Returns an application's description.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        application_id = %application_id
    ))]
    async fn describe_application(
        &mut self,
        application_id: ApplicationId,
    ) -> Result<ApplicationDescription, WorkerError> {
        self.initialize_and_save_if_needed().await?;
        let response = self.chain.describe_application(application_id).await?;
        Ok(response)
    }

    /// Executes a block without persisting any changes to the state.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        block_height = %block.height
    ))]
    async fn stage_block_execution(
        &mut self,
        block: ProposedBlock,
        round: Option<u32>,
        published_blobs: &[Blob],
    ) -> Result<(Block, ChainInfoResponse, ResourceTracker), WorkerError> {
        self.initialize_and_save_if_needed().await?;
        let local_time = self.storage.clock().current_time();
        let (_, committee) = self.chain.current_committee()?;
        block.check_proposal_size(committee.policy().maximum_block_proposal_size)?;

        self.chain
            .remove_bundles_from_inboxes(block.timestamp, true, block.incoming_bundles())
            .await?;
        let (executed_block, resource_tracker) =
            Box::pin(self.execute_block(&block, local_time, round, published_blobs)).await?;

        // No need to sign: only used internally.
        let mut response = ChainInfoResponse::new(&self.chain, None);
        if let Some(owner) = block.authenticated_owner {
            response.info.requested_owner_balance = self
                .chain
                .execution_state
                .system
                .balances
                .get(&owner)
                .await?;
        }

        Ok((executed_block, response, resource_tracker))
    }

    /// Validates and executes a block proposed to extend this chain.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        block_height = %proposal.content.block.height
    ))]
    async fn handle_block_proposal(
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
        let (epoch, committee) = chain.current_committee()?;
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
                certificate.check(committee)?;
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
        if chain.manager.check_proposed_block(&proposal)? == manager::Outcome::Skip {
            // We already voted for this block.
            return Ok((self.chain_info_response(), NetworkActions::default()));
        }
        let local_time = self.storage.clock().current_time();

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
        // Note: The actor delays processing proposals with future timestamps (within the grace
        // period) so that other requests can be handled in the meantime. By the time we reach
        // here, the block timestamp should be in the past or very close to the current time.

        self.chain
            .remove_bundles_from_inboxes(block.timestamp, true, block.incoming_bundles())
            .await?;
        let block = if let Some(outcome) = outcome {
            outcome.clone().with(proposal.content.block.clone())
        } else {
            let (executed_block, _resource_tracker) = Box::pin(self.execute_block(
                block,
                local_time,
                round.multi_leader(),
                &published_blobs,
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
        let created_blobs: BTreeMap<_, _> = block.iter_created_blobs().collect();
        let blobs = self
            .get_required_blobs(proposal.expected_blob_ids(), &created_blobs)
            .await?;
        let key_pair = self.config.key_pair();
        let manager = &mut self.chain.manager;
        match manager.create_vote(proposal, block, key_pair, local_time, blobs)? {
            // Cache the value we voted on, so the client doesn't have to send it again.
            Some(Either::Left(vote)) => {
                self.block_values.insert(Cow::Borrowed(vote.value.inner()));
            }
            Some(Either::Right(vote)) => {
                self.block_values.insert(Cow::Borrowed(vote.value.inner()));
            }
            None => (),
        }
        self.save().await?;
        let actions = self.create_network_actions(Some(old_round)).await?;
        Ok((self.chain_info_response(), actions))
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
        let chain = &self.chain;
        let mut info = ChainInfo::from(chain);
        if query.request_committees {
            info.requested_committees = Some(chain.execution_state.system.committees.get().clone());
        }
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
            let pairs = chain.inboxes.try_load_all_entries().await?;
            let action = if *chain.execution_state.system.closed.get() {
                MessageAction::Reject
            } else {
                MessageAction::Accept
            };
            for (origin, inbox) in pairs {
                for bundle in inbox.added_bundles.elements().await? {
                    bundles.push(IncomingBundle {
                        origin,
                        bundle,
                        action,
                    });
                }
            }
            bundles.sort_by_key(|b| b.bundle.timestamp);
            info.requested_pending_message_bundles = bundles;
        }
        let hashes = chain
            .block_hashes(query.request_sent_certificate_hashes_by_heights)
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
        Ok(ChainInfoResponse::new(info, self.config.key_pair()))
    }

    /// Executes a block, caches the result, and returns the outcome.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        block_height = %block.height
    ))]
    async fn execute_block(
        &mut self,
        block: &ProposedBlock,
        local_time: Timestamp,
        round: Option<u32>,
        published_blobs: &[Blob],
    ) -> Result<(Block, ResourceTracker), WorkerError> {
        let (outcome, resource_tracker) =
            Box::pin(
                self.chain
                    .execute_block(block, local_time, round, published_blobs, None),
            )
            .await?;
        let block = Block::new(block.clone(), outcome);
        let block_hash = CryptoHash::new(&block);
        self.execution_state_cache.insert_owned(
            &block_hash,
            Box::pin(
                self.chain
                    .execution_state
                    .with_context(|ctx| InactiveContext(ctx.base_key().clone())),
            )
            .await,
        );
        Ok((block, resource_tracker))
    }

    /// Initializes and saves the current chain if it is not active yet.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id()
    ))]
    async fn initialize_and_save_if_needed(&mut self) -> Result<(), WorkerError> {
        if !self.knows_chain_is_active {
            let local_time = self.storage.clock().current_time();
            self.chain.initialize_if_needed(local_time).await?;
            self.save().await?;
            self.knows_chain_is_active = true;
        }
        Ok(())
    }

    fn chain_info_response(&self) -> ChainInfoResponse {
        ChainInfoResponse::new(&self.chain, self.config.key_pair())
    }

    /// Stores the chain state in persistent storage.
    ///
    /// Waits until the [`ChainStateView`] is no longer shared before persisting the changes.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id()
    ))]
    async fn save(&mut self) -> Result<(), WorkerError> {
        self.clear_shared_chain_view().await;
        self.chain.save().await?;
        Ok(())
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

/// Returns the keys whose value is `None`.
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

/// Helper type for handling cross-chain updates.
pub(crate) struct CrossChainUpdateHelper<'a> {
    pub(crate) allow_messages_from_deprecated_epochs: bool,
    pub(crate) current_epoch: Epoch,
    pub(crate) committees: &'a BTreeMap<Epoch, Committee>,
}

impl<'a> CrossChainUpdateHelper<'a> {
    /// Creates a new [`CrossChainUpdateHelper`].
    fn new<C>(config: &ChainWorkerConfig, chain: &'a ChainStateView<C>) -> Self
    where
        C: Context + Clone + 'static,
    {
        CrossChainUpdateHelper {
            allow_messages_from_deprecated_epochs: config.allow_messages_from_deprecated_epochs,
            current_epoch: *chain.execution_state.system.epoch.get(),
            committees: chain.execution_state.system.committees.get(),
        }
    }

    /// Checks basic invariants and deals with repeated heights and deprecated epochs.
    /// * Returns a range of message bundles that are both new to us and not relying on
    ///   an untrusted set of validators.
    /// * In the case of validators, if the epoch(s) of the highest bundles are not
    ///   trusted, we only accept bundles that contain messages that were already
    ///   executed by anticipation (i.e. received in certified blocks).
    /// * Basic invariants are checked for good measure. We still crucially trust
    ///   the worker of the sending chain to have verified and executed the blocks
    ///   correctly.
    pub(crate) fn select_message_bundles(
        &self,
        origin: &'a ChainId,
        recipient: ChainId,
        next_height_to_receive: BlockHeight,
        last_anticipated_block_height: Option<BlockHeight>,
        mut bundles: Vec<(Epoch, MessageBundle)>,
    ) -> Result<Vec<MessageBundle>, WorkerError> {
        let mut latest_height = None;
        let mut skipped_len = 0;
        let mut trusted_len = 0;
        for (i, (epoch, bundle)) in bundles.iter().enumerate() {
            // Make sure that heights are not decreasing.
            ensure!(
                latest_height <= Some(bundle.height),
                WorkerError::InvalidCrossChainRequest
            );
            latest_height = Some(bundle.height);
            // Check if the block has been received already.
            if bundle.height < next_height_to_receive {
                skipped_len = i + 1;
            }
            // Check if the height is trusted or the epoch is trusted.
            if self.allow_messages_from_deprecated_epochs
                || Some(bundle.height) <= last_anticipated_block_height
                || *epoch >= self.current_epoch
                || self.committees.contains_key(epoch)
            {
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
        let bundles = if skipped_len < trusted_len {
            bundles
                .drain(skipped_len..trusted_len)
                .map(|(_, bundle)| bundle)
                .collect()
        } else {
            vec![]
        };
        Ok(bundles)
    }
}

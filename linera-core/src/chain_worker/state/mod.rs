// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The state and functionality of a chain worker.

mod attempted_changes;
mod temporary_changes;

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    iter,
    sync::{self, Arc},
};

use linera_base::{
    crypto::{CryptoHash, ValidatorPublicKey},
    data_types::{ApplicationDescription, Blob, BlockHeight, Epoch},
    ensure,
    hashed::Hashed,
    identifiers::{ApplicationId, BlobId, BlobType, ChainId},
};
use linera_chain::{
    data_types::{BlockExecutionOutcome, BlockProposal, MessageBundle, ProposedBlock},
    manager,
    types::{Block, ConfirmedBlockCertificate, TimeoutCertificate, ValidatedBlockCertificate},
    ChainError, ChainStateView,
};
use linera_execution::{ExecutionStateView, Query, QueryOutcome, ServiceRuntimeEndpoint};
use linera_storage::{Clock as _, ResultReadCertificates, Storage};
use linera_views::views::ClonableView;
use tokio::sync::{oneshot, OwnedRwLockReadGuard, RwLock};
use tracing::{instrument, warn};

#[cfg(test)]
pub(crate) use self::attempted_changes::CrossChainUpdateHelper;
use self::{
    attempted_changes::ChainWorkerStateWithAttemptedChanges,
    temporary_changes::ChainWorkerStateWithTemporaryChanges,
};
use super::{ChainWorkerConfig, ChainWorkerRequest, DeliveryNotifier};
use crate::{
    data_types::{ChainInfoQuery, ChainInfoResponse, CrossChainRequest},
    value_cache::ValueCache,
    worker::{NetworkActions, WorkerError},
};

/// The state of the chain worker.
pub struct ChainWorkerState<StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
{
    config: ChainWorkerConfig,
    storage: StorageClient,
    chain: ChainStateView<StorageClient::Context>,
    shared_chain_view: Option<Arc<RwLock<ChainStateView<StorageClient::Context>>>>,
    service_runtime_endpoint: Option<ServiceRuntimeEndpoint>,
    block_values: Arc<ValueCache<CryptoHash, Hashed<Block>>>,
    execution_state_cache: Arc<ValueCache<CryptoHash, ExecutionStateView<StorageClient::Context>>>,
    tracked_chains: Option<Arc<sync::RwLock<HashSet<ChainId>>>>,
    delivery_notifier: DeliveryNotifier,
    knows_chain_is_active: bool,
}

impl<StorageClient> ChainWorkerState<StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
{
    /// Creates a new [`ChainWorkerState`] using the provided `storage` client.
    #[expect(clippy::too_many_arguments)]
    pub async fn load(
        config: ChainWorkerConfig,
        storage: StorageClient,
        block_values: Arc<ValueCache<CryptoHash, Hashed<Block>>>,
        execution_state_cache: Arc<
            ValueCache<CryptoHash, ExecutionStateView<StorageClient::Context>>,
        >,
        tracked_chains: Option<Arc<sync::RwLock<HashSet<ChainId>>>>,
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
            tracked_chains,
            delivery_notifier,
            knows_chain_is_active: false,
        })
    }

    /// Returns the [`ChainId`] of the chain handled by this worker.
    pub fn chain_id(&self) -> ChainId {
        self.chain.chain_id()
    }

    /// Handles a request and applies it to the chain state.
    #[instrument(skip_all)]
    pub async fn handle_request(&mut self, request: ChainWorkerRequest<StorageClient::Context>) {
        tracing::trace!("Handling chain worker request: {request:?}");
        // TODO(#2237): Spawn concurrent tasks for read-only operations
        let responded = match request {
            #[cfg(with_testing)]
            ChainWorkerRequest::ReadCertificate { height, callback } => {
                callback.send(self.read_certificate(height).await).is_ok()
            }
            #[cfg(with_testing)]
            ChainWorkerRequest::FindBundleInInbox {
                inbox_id,
                certificate_hash,
                height,
                index,
                callback,
            } => callback
                .send(
                    self.find_bundle_in_inbox(inbox_id, certificate_hash, height, index)
                        .await,
                )
                .is_ok(),
            ChainWorkerRequest::GetChainStateView { callback } => {
                callback.send(self.chain_state_view().await).is_ok()
            }
            ChainWorkerRequest::QueryApplication { query, callback } => {
                callback.send(self.query_application(query).await).is_ok()
            }
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
        };

        if !responded {
            warn!("Callback for `ChainWorkerActor` was dropped before a response was sent");
        }
    }

    /// Returns a read-only view of the [`ChainStateView`].
    ///
    /// The returned view holds a lock on the chain state, which prevents the worker from changing
    /// it.
    pub(super) async fn chain_state_view(
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

    /// Returns a stored [`Certificate`] for the chain's block at the requested [`BlockHeight`].
    #[cfg(with_testing)]
    pub(super) async fn read_certificate(
        &mut self,
        height: BlockHeight,
    ) -> Result<Option<ConfirmedBlockCertificate>, WorkerError> {
        ChainWorkerStateWithTemporaryChanges::new(self)
            .await
            .read_certificate(height)
            .await
    }

    /// Searches for a bundle in one of the chain's inboxes.
    #[cfg(with_testing)]
    pub(super) async fn find_bundle_in_inbox(
        &mut self,
        inbox_id: ChainId,
        certificate_hash: CryptoHash,
        height: BlockHeight,
        index: u32,
    ) -> Result<Option<MessageBundle>, WorkerError> {
        ChainWorkerStateWithTemporaryChanges::new(self)
            .await
            .find_bundle_in_inbox(inbox_id, certificate_hash, height, index)
            .await
    }

    /// Queries an application's state on the chain.
    pub(super) async fn query_application(
        &mut self,
        query: Query,
    ) -> Result<QueryOutcome, WorkerError> {
        ChainWorkerStateWithTemporaryChanges::new(self)
            .await
            .query_application(query)
            .await
    }

    /// Returns an application's description.
    pub(super) async fn describe_application(
        &mut self,
        application_id: ApplicationId,
    ) -> Result<ApplicationDescription, WorkerError> {
        ChainWorkerStateWithTemporaryChanges::new(self)
            .await
            .describe_application(application_id)
            .await
    }

    /// Executes a block without persisting any changes to the state.
    pub(super) async fn stage_block_execution(
        &mut self,
        block: ProposedBlock,
        round: Option<u32>,
        published_blobs: &[Blob],
    ) -> Result<(Block, ChainInfoResponse), WorkerError> {
        let (block, response) = ChainWorkerStateWithTemporaryChanges::new(self)
            .await
            .stage_block_execution(block, round, published_blobs)
            .await?;
        Ok((block, response))
    }

    /// Processes a leader timeout issued for this multi-owner chain.
    pub(super) async fn process_timeout(
        &mut self,
        certificate: TimeoutCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        ChainWorkerStateWithAttemptedChanges::new(self)
            .await
            .process_timeout(certificate)
            .await
    }

    /// Handles a proposal for the next block for this chain.
    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        self.ensure_is_active().await?;
        if ChainWorkerStateWithTemporaryChanges::new(&mut *self)
            .await
            .check_proposed_block(&proposal)
            .await?
            == manager::Outcome::Skip
        {
            // Skipping: We already voted for this block.
            let info = ChainInfoResponse::new(&self.chain, self.config.key_pair());
            return Ok((info, NetworkActions::default()));
        };
        let published_blobs = ChainWorkerStateWithAttemptedChanges::new(&mut *self)
            .await
            .load_proposal_blobs(&proposal)
            .await?;
        let validation_outcome = ChainWorkerStateWithTemporaryChanges::new(self)
            .await
            .validate_proposal_content(&proposal.content, &published_blobs)
            .await?;

        let actions = if let Some((outcome, local_time)) = validation_outcome {
            ChainWorkerStateWithAttemptedChanges::new(&mut *self)
                .await
                .vote_for_block_proposal(proposal, outcome, local_time)
                .await?;
            // Trigger any outgoing cross-chain messages that haven't been confirmed yet.
            self.create_network_actions().await?
        } else {
            // If we just processed the same pending block, return the chain info unchanged.
            NetworkActions::default()
        };

        let info = ChainInfoResponse::new(&self.chain, self.config.key_pair());
        Ok((info, actions))
    }

    /// Clears the shared chain view, and acquires and drops its write lock.
    ///
    /// This is the only place a write lock is acquired, and read locks are acquired in
    /// the `chain_state_view` method, which has a `&mut self` receiver like this one.
    /// That means that when this function returns, no readers will be waiting to acquire
    /// the lock and it is safe to write the chain state to storage without any readers
    /// having a stale view of it.
    pub(super) async fn clear_shared_chain_view(&mut self) {
        if let Some(shared_chain_view) = self.shared_chain_view.take() {
            let _ = shared_chain_view.write().await;
        }
    }

    /// Processes a validated block issued for this multi-owner chain.
    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn process_validated_block(
        &mut self,
        certificate: ValidatedBlockCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions, bool), WorkerError> {
        ChainWorkerStateWithAttemptedChanges::new(self)
            .await
            .process_validated_block(certificate)
            .await
    }

    /// Processes a confirmed block (aka a commit).
    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn process_confirmed_block(
        &mut self,
        certificate: ConfirmedBlockCertificate,
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        ChainWorkerStateWithAttemptedChanges::new(self)
            .await
            .process_confirmed_block(certificate, notify_when_messages_are_delivered)
            .await
    }

    /// Updates the chain's inboxes, receiving messages from a cross-chain update.
    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn process_cross_chain_update(
        &mut self,
        origin: ChainId,
        bundles: Vec<(Epoch, MessageBundle)>,
    ) -> Result<Option<BlockHeight>, WorkerError> {
        ChainWorkerStateWithAttemptedChanges::new(self)
            .await
            .process_cross_chain_update(origin, bundles)
            .await
    }

    /// Handles the cross-chain request confirming that the recipient was updated.
    pub(super) async fn confirm_updated_recipient(
        &mut self,
        recipient: ChainId,
        latest_height: BlockHeight,
    ) -> Result<(), WorkerError> {
        ChainWorkerStateWithAttemptedChanges::new(self)
            .await
            .confirm_updated_recipient(recipient, latest_height)
            .await
    }

    /// Handles a [`ChainInfoQuery`], potentially voting on the next block.
    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        if query.request_leader_timeout {
            ChainWorkerStateWithAttemptedChanges::new(&mut *self)
                .await
                .vote_for_leader_timeout()
                .await?;
        }
        if query.request_fallback {
            ChainWorkerStateWithAttemptedChanges::new(&mut *self)
                .await
                .vote_for_fallback()
                .await?;
        }
        let response = ChainWorkerStateWithTemporaryChanges::new(self)
            .await
            .prepare_chain_info_response(query)
            .await?;
        // Trigger any outgoing cross-chain messages that haven't been confirmed yet.
        let actions = self.create_network_actions().await?;
        Ok((response, actions))
    }

    /// Returns the requested blob, if it belongs to the current locking block or pending proposal.
    pub(super) async fn download_pending_blob(&self, blob_id: BlobId) -> Result<Blob, WorkerError> {
        if let Some(blob) = self.chain.manager.pending_blob(&blob_id).await? {
            return Ok(blob);
        }
        let blob = self.storage.read_blob(blob_id).await?;
        blob.ok_or(WorkerError::BlobsNotFound(vec![blob_id]))
    }

    /// Adds the blob to pending blocks or validated block certificates that are missing it.
    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn handle_pending_blob(
        &mut self,
        blob: Blob,
    ) -> Result<ChainInfoResponse, WorkerError> {
        ChainWorkerStateWithAttemptedChanges::new(&mut *self)
            .await
            .handle_pending_blob(blob)
            .await
    }

    /// Ensures that the current chain is active, returning an error otherwise.
    async fn ensure_is_active(&mut self) -> Result<(), WorkerError> {
        if !self.knows_chain_is_active {
            let local_time = self.storage.clock().current_time();
            self.chain.ensure_is_active(local_time).await?;
            self.knows_chain_is_active = true;
        }
        Ok(())
    }

    /// Reads the blobs from the chain manager or from storage. Returns an error if any are
    /// missing.
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
    async fn maybe_get_required_blobs(
        &self,
        blob_ids: impl IntoIterator<Item = BlobId>,
        created_blobs: Option<&BTreeMap<BlobId, Blob>>,
    ) -> Result<BTreeMap<BlobId, Option<Blob>>, WorkerError> {
        let mut maybe_blobs = BTreeMap::from_iter(blob_ids.into_iter().zip(iter::repeat(None)));

        for (blob_id, maybe_blob) in &mut maybe_blobs {
            if let Some(blob) = created_blobs.and_then(|blob_map| blob_map.get(blob_id)) {
                *maybe_blob = Some(blob.clone());
            } else if let Some(blob) = self.chain.manager.pending_blob(blob_id).await? {
                *maybe_blob = Some(blob);
            } else if let Some(blob) = self.chain.pending_validated_blobs.get(blob_id).await? {
                *maybe_blob = Some(blob);
            } else {
                for (_, pending_blobs) in self
                    .chain
                    .pending_proposed_blobs
                    .try_load_all_entries()
                    .await?
                {
                    if let Some(blob) = pending_blobs.get(blob_id).await? {
                        *maybe_blob = Some(blob);
                        break;
                    }
                }
            }
        }
        let missing_blob_ids = missing_blob_ids(&maybe_blobs);
        let blobs_from_storage = self.storage.read_blobs(&missing_blob_ids).await?;
        for (blob_id, maybe_blob) in missing_blob_ids.into_iter().zip(blobs_from_storage) {
            maybe_blobs.insert(blob_id, maybe_blob);
        }
        Ok(maybe_blobs)
    }

    /// Adds any newly created chains to the set of `tracked_chains`, if the parent chain is
    /// also tracked.
    ///
    /// Chains that are not tracked are usually processed only because they sent some message
    /// to one of the tracked chains. In most use cases, their children won't be of interest.
    fn track_newly_created_chains(
        &self,
        proposed_block: &ProposedBlock,
        outcome: &BlockExecutionOutcome,
    ) {
        if let Some(tracked_chains) = self.tracked_chains.as_ref() {
            if !tracked_chains
                .read()
                .expect("Panics should not happen while holding a lock to `tracked_chains`")
                .contains(&proposed_block.chain_id)
            {
                return; // The parent chain is not tracked; don't track the child.
            }
            let new_chain_ids = outcome
                .created_blobs_ids()
                .into_iter()
                .filter(|blob_id| blob_id.blob_type == BlobType::ChainDescription)
                .map(|blob_id| ChainId(blob_id.hash));

            tracked_chains
                .write()
                .expect("Panics should not happen while holding a lock to `tracked_chains`")
                .extend(new_chain_ids);
        }
    }

    /// Loads pending cross-chain requests.
    async fn create_network_actions(&self) -> Result<NetworkActions, WorkerError> {
        let mut heights_by_recipient = BTreeMap::<_, Vec<_>>::new();
        let mut targets = self.chain.nonempty_outbox_chain_ids();
        if let Some(tracked_chains) = self.tracked_chains.as_ref() {
            let tracked_chains = tracked_chains
                .read()
                .expect("Panics should not happen while holding a lock to `tracked_chains`");
            targets.retain(|target| tracked_chains.contains(target));
        }
        let outboxes = self.chain.load_outboxes(&targets).await?;
        for (target, outbox) in targets.into_iter().zip(outboxes) {
            let heights = outbox.queue.elements().await?;
            heights_by_recipient.insert(target, heights);
        }
        self.create_cross_chain_requests(heights_by_recipient).await
    }

    async fn create_cross_chain_requests(
        &self,
        heights_by_recipient: BTreeMap<ChainId, Vec<BlockHeight>>,
    ) -> Result<NetworkActions, WorkerError> {
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
        for height in heights.range(next_block_height..) {
            hashes.push(
                self.chain
                    .preprocessed_blocks
                    .get(height)
                    .await?
                    .ok_or_else(|| WorkerError::PreprocessedBlocksEntryNotFound {
                        height: *height,
                        chain_id: self.chain_id(),
                    })?,
            );
        }
        let certificates = self.storage.read_certificates(hashes.clone()).await?;
        let certificates = match ResultReadCertificates::new(certificates, hashes) {
            ResultReadCertificates::Certificates(certificates) => certificates,
            ResultReadCertificates::InvalidHashes(hashes) => {
                return Err(WorkerError::ReadCertificatesError(hashes))
            }
        };
        let certificates = heights
            .into_iter()
            .zip(certificates)
            .collect::<HashMap<_, _>>();
        // For each medium, select the relevant messages.
        let mut actions = NetworkActions::default();
        for (recipient, heights) in heights_by_recipient {
            let mut bundles = Vec::new();
            for height in heights {
                let cert = certificates
                    .get(&height)
                    .ok_or_else(|| ChainError::InternalError("missing certificates".to_string()))?;
                bundles.extend(cert.message_bundles_for(recipient));
            }
            let request = CrossChainRequest::UpdateRecipient {
                sender: self.chain.chain_id(),
                recipient,
                bundles,
            };
            actions.cross_chain_requests.push(request);
        }
        Ok(actions)
    }

    /// Returns true if there are no more outgoing messages in flight up to the given
    /// block height.
    pub async fn all_messages_to_tracked_chains_delivered_up_to(
        &self,
        height: BlockHeight,
    ) -> Result<bool, WorkerError> {
        if self.chain.all_messages_delivered_up_to(height) {
            return Ok(true);
        }
        let Some(tracked_chains) = self.tracked_chains.as_ref() else {
            return Ok(false);
        };
        let mut targets = self.chain.nonempty_outbox_chain_ids();
        {
            let tracked_chains = tracked_chains.read().unwrap();
            targets.retain(|target| tracked_chains.contains(target));
        }
        let outboxes = self.chain.load_outboxes(&targets).await?;
        for outbox in outboxes {
            let front = outbox.queue.front();
            if front.is_some_and(|key| *key <= height) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Updates the received certificate trackers to at least the given values.
    pub async fn update_received_certificate_trackers(
        &mut self,
        new_trackers: BTreeMap<ValidatorPublicKey, u64>,
    ) -> Result<(), WorkerError> {
        ChainWorkerStateWithAttemptedChanges::new(self)
            .await
            .update_received_certificate_trackers(new_trackers)
            .await
    }
}

/// Returns the keys whose value is `None`.
fn missing_blob_ids(maybe_blobs: &BTreeMap<BlobId, Option<Blob>>) -> Vec<BlobId> {
    maybe_blobs
        .iter()
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

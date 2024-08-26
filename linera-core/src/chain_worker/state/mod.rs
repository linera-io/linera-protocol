// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The state and functionality of a chain worker.

mod attempted_changes;
mod temporary_changes;

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::{self, Arc},
};

use linera_base::{
    crypto::CryptoHash,
    data_types::{Blob, BlockHeight},
    ensure,
    identifiers::{BlobId, ChainId},
};
use linera_chain::{
    data_types::{
        Block, BlockProposal, Certificate, ExecutedBlock, HashedCertificateValue, Medium,
        MessageBundle, Origin, Target,
    },
    ChainError, ChainStateView,
};
use linera_execution::{
    committee::Epoch, ExecutionRequest, Message, Query, QueryContext, Response,
    ServiceRuntimeRequest, SystemMessage, UserApplicationDescription, UserApplicationId,
};
use linera_storage::Storage;
use linera_views::views::{ClonableView, ViewError};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

#[cfg(test)]
pub(crate) use self::attempted_changes::CrossChainUpdateHelper;
use self::{
    attempted_changes::ChainWorkerStateWithAttemptedChanges,
    temporary_changes::ChainWorkerStateWithTemporaryChanges,
};
use super::ChainWorkerConfig;
use crate::{
    data_types::{ChainInfoQuery, ChainInfoResponse, CrossChainRequest},
    value_cache::ValueCache,
    worker::{NetworkActions, WorkerError},
};

/// The state of the chain worker.
pub struct ChainWorkerState<StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
    ViewError: From<StorageClient::StoreError>,
{
    config: ChainWorkerConfig,
    storage: StorageClient,
    chain: ChainStateView<StorageClient::Context>,
    shared_chain_view: Option<Arc<RwLock<ChainStateView<StorageClient::Context>>>>,
    execution_state_receiver: futures::channel::mpsc::UnboundedReceiver<ExecutionRequest>,
    runtime_request_sender: std::sync::mpsc::Sender<ServiceRuntimeRequest>,
    recent_hashed_certificate_values: Arc<ValueCache<CryptoHash, HashedCertificateValue>>,
    recent_blobs: Arc<ValueCache<BlobId, Blob>>,
    tracked_chains: Option<Arc<sync::RwLock<HashSet<ChainId>>>>,
    knows_chain_is_active: bool,
}

impl<StorageClient> ChainWorkerState<StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
    ViewError: From<StorageClient::StoreError>,
{
    /// Creates a new [`ChainWorkerState`] using the provided `storage` client.
    #[allow(clippy::too_many_arguments)]
    pub async fn load(
        config: ChainWorkerConfig,
        storage: StorageClient,
        certificate_value_cache: Arc<ValueCache<CryptoHash, HashedCertificateValue>>,
        blob_cache: Arc<ValueCache<BlobId, Blob>>,
        tracked_chains: Option<Arc<sync::RwLock<HashSet<ChainId>>>>,
        chain_id: ChainId,
        execution_state_receiver: futures::channel::mpsc::UnboundedReceiver<ExecutionRequest>,
        runtime_request_sender: std::sync::mpsc::Sender<ServiceRuntimeRequest>,
    ) -> Result<Self, WorkerError> {
        let chain = storage.load_chain(chain_id).await?;

        Ok(ChainWorkerState {
            config,
            storage,
            chain,
            shared_chain_view: None,
            execution_state_receiver,
            runtime_request_sender,
            recent_hashed_certificate_values: certificate_value_cache,
            recent_blobs: blob_cache,
            tracked_chains,
            knows_chain_is_active: false,
        })
    }

    /// Returns the [`ChainId`] of the chain handled by this worker.
    pub fn chain_id(&self) -> ChainId {
        self.chain.chain_id()
    }

    /// Returns the current [`QueryContext`] for the current chain state.
    pub fn current_query_context(&self) -> QueryContext {
        QueryContext {
            chain_id: self.chain_id(),
            next_block_height: self.chain.tip_state.get().next_block_height,
            local_time: self.storage.clock().current_time(),
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
    ) -> Result<Option<Certificate>, WorkerError> {
        ChainWorkerStateWithTemporaryChanges::new(self)
            .await
            .read_certificate(height)
            .await
    }

    /// Searches for a bundle in one of the chain's inboxes.
    #[cfg(with_testing)]
    pub(super) async fn find_bundle_in_inbox(
        &mut self,
        inbox_id: Origin,
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
    ) -> Result<Response, WorkerError> {
        ChainWorkerStateWithTemporaryChanges::new(self)
            .await
            .query_application(query)
            .await
    }

    /// Returns an application's description.
    pub(super) async fn describe_application(
        &mut self,
        application_id: UserApplicationId,
    ) -> Result<UserApplicationDescription, WorkerError> {
        ChainWorkerStateWithTemporaryChanges::new(self)
            .await
            .describe_application(application_id)
            .await
    }

    /// Executes a block without persisting any changes to the state.
    pub(super) async fn stage_block_execution(
        &mut self,
        block: Block,
    ) -> Result<(ExecutedBlock, ChainInfoResponse), WorkerError> {
        ChainWorkerStateWithTemporaryChanges::new(self)
            .await
            .stage_block_execution(block)
            .await
    }

    /// Processes a leader timeout issued for this multi-owner chain.
    pub(super) async fn process_timeout(
        &mut self,
        certificate: Certificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        ChainWorkerStateWithAttemptedChanges::new(self)
            .await
            .process_timeout(certificate)
            .await
    }

    /// Handles a proposal for the next block for this chain.
    pub(super) async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        let validation_outcome = ChainWorkerStateWithTemporaryChanges::new(self)
            .await
            .validate_block(&proposal)
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

    /// Processes a validated block issued for this multi-owner chain.
    pub(super) async fn process_validated_block(
        &mut self,
        certificate: Certificate,
        blobs: &[Blob],
    ) -> Result<(ChainInfoResponse, NetworkActions, bool), WorkerError> {
        ChainWorkerStateWithAttemptedChanges::new(self)
            .await
            .process_validated_block(certificate, blobs)
            .await
    }

    /// Processes a confirmed block (aka a commit).
    pub(super) async fn process_confirmed_block(
        &mut self,
        certificate: Certificate,
        blobs: &[Blob],
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        ChainWorkerStateWithAttemptedChanges::new(self)
            .await
            .process_confirmed_block(certificate, blobs)
            .await
    }

    /// Updates the chain's inboxes, receiving messages from a cross-chain update.
    pub(super) async fn process_cross_chain_update(
        &mut self,
        origin: Origin,
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
        latest_heights: Vec<(Target, BlockHeight)>,
    ) -> Result<BlockHeight, WorkerError> {
        ChainWorkerStateWithAttemptedChanges::new(self)
            .await
            .confirm_updated_recipient(latest_heights)
            .await
    }

    /// Handles a [`ChainInfoQuery`], potentially voting on the next block.
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

    /// Ensures that the current chain is active, returning an error otherwise.
    fn ensure_is_active(&mut self) -> Result<(), WorkerError> {
        if !self.knows_chain_is_active {
            self.chain.ensure_is_active()?;
            self.knows_chain_is_active = true;
        }
        Ok(())
    }

    /// Returns an error if the block requires a blob we don't have, or if unrelated blobs were provided.
    async fn check_no_missing_blobs(
        &self,
        blobs_in_block: HashSet<BlobId>,
        blobs: &[Blob],
    ) -> Result<(), WorkerError> {
        let missing_blobs = self.get_missing_blobs(blobs_in_block, blobs).await?;

        if missing_blobs.is_empty() {
            return Ok(());
        }

        Err(WorkerError::BlobsNotFound(missing_blobs))
    }

    /// Returns the blobs required by the block that we don't have, or an error if unrelated blobs were provided.
    async fn get_missing_blobs(
        &self,
        mut required_blob_ids: HashSet<BlobId>,
        blobs: &[Blob],
    ) -> Result<Vec<BlobId>, WorkerError> {
        // Find all certificates containing blobs used when executing this block.
        for blob in blobs {
            let blob_id = blob.id();
            ensure!(
                required_blob_ids.remove(&blob_id),
                WorkerError::UnneededBlob { blob_id }
            );
        }

        let pending_blobs = &self.chain.manager.get().pending_blobs;
        let blob_ids = self
            .recent_blobs
            .subtract_cached_items_from::<_, Vec<_>>(required_blob_ids, |id| id)
            .await
            .into_iter()
            .filter(|blob_id| !pending_blobs.contains_key(blob_id))
            .collect::<Vec<_>>();
        Ok(self.storage.missing_blobs(blob_ids.clone()).await?)
    }

    /// Returns the blobs requested by their `blob_ids` that are either in pending in the
    /// chain or in the `recent_blobs` cache.
    async fn get_blobs(&self, blob_ids: HashSet<BlobId>) -> Result<Vec<Blob>, WorkerError> {
        let pending_blobs = &self.chain.manager.get().pending_blobs;
        let (found_blobs, not_found_blobs): (HashMap<BlobId, Blob>, HashSet<BlobId>) =
            self.recent_blobs.try_get_many(blob_ids).await;

        let mut blobs = found_blobs.into_values().collect::<Vec<_>>();
        for blob_id in not_found_blobs {
            if let Some(blob) = pending_blobs.get(&blob_id) {
                blobs.push(blob.clone());
            }
        }

        Ok(blobs)
    }

    /// Inserts a [`Blob`] into the worker's cache.
    async fn cache_recent_blob<'a>(&mut self, blob: Cow<'a, Blob>) -> bool {
        self.recent_blobs.insert(blob).await
    }

    /// Adds any newly created chains to the set of `tracked_chains`.
    fn track_newly_created_chains(&self, block: &ExecutedBlock) {
        if let Some(tracked_chains) = self.tracked_chains.as_ref() {
            let messages = block.messages().iter().flatten();
            let open_chain_message_indices =
                messages
                    .enumerate()
                    .filter_map(|(index, outgoing_message)| match outgoing_message.message {
                        Message::System(SystemMessage::OpenChain(_)) => Some(index),
                        _ => None,
                    });
            let open_chain_message_ids =
                open_chain_message_indices.map(|index| block.message_id(index as u32));
            let new_chain_ids = open_chain_message_ids.map(ChainId::child);

            tracked_chains
                .write()
                .expect("Panics should not happen while holding a lock to `tracked_chains`")
                .extend(new_chain_ids);
        }
    }

    /// Loads pending cross-chain requests.
    async fn create_network_actions(&self) -> Result<NetworkActions, WorkerError> {
        let mut heights_by_recipient: BTreeMap<_, BTreeMap<_, _>> = Default::default();
        let mut targets = self.chain.outboxes.indices().await?;
        if let Some(tracked_chains) = self.tracked_chains.as_ref() {
            let tracked_chains = tracked_chains
                .read()
                .expect("Panics should not happen while holding a lock to `tracked_chains`");
            targets.retain(|target| tracked_chains.contains(&target.recipient));
        }
        let outboxes = self.chain.outboxes.try_load_entries(&targets).await?;
        for (target, outbox) in targets.into_iter().zip(outboxes) {
            let outbox = outbox.expect("Only existing outboxes should be referenced by `indices`");
            let heights = outbox.queue.elements().await?;
            heights_by_recipient
                .entry(target.recipient)
                .or_default()
                .insert(target.medium, heights);
        }
        let mut actions = NetworkActions::default();
        for (recipient, height_map) in heights_by_recipient {
            let request = self
                .create_cross_chain_request(height_map.into_iter().collect(), recipient)
                .await?;
            actions.cross_chain_requests.push(request);
        }
        Ok(actions)
    }

    /// Creates an `UpdateRecipient` request that informs the `recipient` about new
    /// cross-chain messages from this chain.
    async fn create_cross_chain_request(
        &self,
        height_map: Vec<(Medium, Vec<BlockHeight>)>,
        recipient: ChainId,
    ) -> Result<CrossChainRequest, WorkerError> {
        // Load all the certificates we will need, regardless of the medium.
        let heights =
            BTreeSet::from_iter(height_map.iter().flat_map(|(_, heights)| heights).copied());
        let heights_usize = heights
            .iter()
            .copied()
            .map(usize::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let hashes = self
            .chain
            .confirmed_log
            .multi_get(heights_usize.clone())
            .await?
            .into_iter()
            .zip(heights_usize)
            .map(|(maybe_hash, height)| {
                maybe_hash.ok_or_else(|| ViewError::not_found("confirmed log entry", height))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let certificates = self.storage.read_certificates(hashes).await?;
        let certificates = heights
            .into_iter()
            .zip(certificates)
            .collect::<HashMap<_, _>>();
        // For each medium, select the relevant messages.
        let mut bundle_vecs = Vec::new();
        for (medium, heights) in height_map {
            let mut bundles = Vec::new();
            for height in heights {
                let cert = certificates
                    .get(&height)
                    .ok_or_else(|| ChainError::InternalError("missing certificates".to_string()))?;
                bundles.extend(cert.message_bundles_for(&medium, recipient));
            }
            if !bundles.is_empty() {
                bundle_vecs.push((medium, bundles));
            }
        }
        Ok(CrossChainRequest::UpdateRecipient {
            sender: self.chain.chain_id(),
            recipient,
            bundle_vecs,
        })
    }
}

/// Returns an error if the block is not at the expected epoch.
fn check_block_epoch(chain_epoch: Epoch, block: &Block) -> Result<(), WorkerError> {
    ensure!(
        block.epoch == chain_epoch,
        WorkerError::InvalidEpoch {
            chain_id: block.chain_id,
            epoch: block.epoch,
            chain_epoch
        }
    );
    Ok(())
}

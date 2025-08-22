// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The state and functionality of a chain worker.

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    iter,
    sync::{self, Arc},
};

use futures::future::Either;
use linera_base::{
    crypto::{CryptoHash, ValidatorPublicKey},
    data_types::{
        ApplicationDescription, ArithmeticError, Blob, BlockHeight, Epoch, Round, Timestamp,
    },
    ensure,
    hashed::Hashed,
    identifiers::{AccountOwner, ApplicationId, BlobId, BlobType, ChainId, EventId, StreamId},
};
use linera_chain::{
    data_types::{
        BlockExecutionOutcome, BlockProposal, IncomingBundle, MessageAction, MessageBundle,
        OriginalProposal, ProposalContent, ProposedBlock,
    },
    manager,
    types::{Block, ConfirmedBlockCertificate, TimeoutCertificate, ValidatedBlockCertificate},
    ChainError, ChainExecutionContext, ChainStateView, ExecutionResultExt as _,
};
use linera_execution::{
    system::EPOCH_STREAM_NAME, Committee, ExecutionStateView, Query, QueryOutcome,
    ServiceRuntimeEndpoint,
};
use linera_storage::{Clock as _, ResultReadCertificates, Storage};
use linera_views::{
    context::{Context, InactiveContext},
    views::{ClonableView, ReplaceContext as _, RootView as _, View as _},
};
use tokio::sync::{oneshot, OwnedRwLockReadGuard, RwLock, RwLockWriteGuard};
use tracing::{debug, instrument, trace, warn};

use super::{ChainWorkerConfig, ChainWorkerRequest, DeliveryNotifier};
use crate::{
    data_types::{ChainInfo, ChainInfoQuery, ChainInfoResponse, CrossChainRequest},
    value_cache::ValueCache,
    worker::{NetworkActions, Notification, Reason, WorkerError},
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
    execution_state_cache: Arc<ValueCache<CryptoHash, ExecutionStateView<InactiveContext>>>,
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
        execution_state_cache: Arc<ValueCache<CryptoHash, ExecutionStateView<InactiveContext>>>,
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

        // Roll back any unsaved changes to the chain state: If there was an error while trying
        // to handle the request, the chain state might contain unsaved and potentially invalid
        // changes. The next request needs to be applied to the chain state as it is in storage.
        self.chain.rollback();
    }

    /// Returns a read-only view of the [`ChainStateView`].
    ///
    /// The returned view holds a lock on the chain state, which prevents the worker from changing
    /// it.
    pub(super) async fn chain_state_view(
        &mut self,
    ) -> Result<OwnedRwLockReadGuard<ChainStateView<StorageClient::Context>>, WorkerError> {
        if self.shared_chain_view.is_none() {
            self.shared_chain_view = Some(Arc::new(RwLock::new(self.chain.clone_unchecked())));
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
    pub(super) async fn clear_shared_chain_view(&mut self) {
        if let Some(shared_chain_view) = self.shared_chain_view.take() {
            let _: RwLockWriteGuard<_> = shared_chain_view.write().await;
        }
    }

    /// Handles a [`ChainInfoQuery`], potentially voting on the next block.
    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        if let Some((height, round)) = query.request_leader_timeout {
            self.vote_for_leader_timeout(height, round).await?;
        }
        if query.request_fallback {
            self.vote_for_fallback().await?;
        }
        let response = self.prepare_chain_info_response(query).await?;
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

    /// Processes a leader timeout issued for this multi-owner chain.
    pub(super) async fn process_timeout(
        &mut self,
        certificate: TimeoutCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        // Check that the chain is active and ready for this timeout.
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        self.ensure_is_active().await?;
        let (chain_epoch, committee) = self.chain.current_committee()?;
        ensure!(
            certificate.inner().epoch() == chain_epoch,
            WorkerError::InvalidEpoch {
                chain_id: certificate.inner().chain_id(),
                chain_epoch,
                epoch: certificate.inner().epoch()
            }
        );
        certificate.check(committee)?;
        let mut actions = NetworkActions::default();
        if self
            .chain
            .tip_state
            .get()
            .already_validated_block(certificate.inner().height())?
        {
            return Ok((self.chain_info_response(), actions));
        }
        let old_round = self.chain.manager.current_round();
        let timeout_chain_id = certificate.inner().chain_id();
        let timeout_height = certificate.inner().height();
        self.chain
            .manager
            .handle_timeout_certificate(certificate, self.storage.clock().current_time());
        let round = self.chain.manager.current_round();
        if round > old_round {
            actions.notifications.push(Notification {
                chain_id: timeout_chain_id,
                reason: Reason::NewRound {
                    height: timeout_height,
                    round,
                },
            })
        }
        self.save().await?;
        Ok((self.chain_info_response(), actions))
    }

    /// Tries to load all blobs published in this proposal.
    ///
    /// If they cannot be found, it creates an entry in `pending_proposed_blobs` so they can be
    /// submitted one by one.
    pub(super) async fn load_proposal_blobs(
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
                .update(*round, validated, maybe_blobs)
                .await?;
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
    pub(super) async fn process_validated_block(
        &mut self,
        certificate: ValidatedBlockCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions, bool), WorkerError> {
        let block = certificate.block();

        let header = &block.header;
        let height = header.height;
        // Check that the chain is active and ready for this validated block.
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        self.ensure_is_active().await?;
        let (epoch, committee) = self.chain.current_committee()?;
        check_block_epoch(epoch, header.chain_id, header.epoch)?;
        certificate.check(committee)?;
        let mut actions = NetworkActions::default();
        let already_committed_block = self.chain.tip_state.get().already_validated_block(height)?;
        let should_skip_validated_block = || {
            self.chain
                .manager
                .check_validated_block(&certificate)
                .map(|outcome| outcome == manager::Outcome::Skip)
        };
        if already_committed_block || should_skip_validated_block()? {
            // If we just processed the same pending block, return the chain info unchanged.
            return Ok((self.chain_info_response(), actions, true));
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
                .update(certificate.round, true, maybe_blobs)
                .await?;
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
        let round = self.chain.manager.current_round();
        if round > old_round {
            actions.notifications.push(Notification {
                chain_id: self.chain_id(),
                reason: Reason::NewRound { height, round },
            })
        }
        Ok((self.chain_info_response(), actions, false))
    }

    /// Processes a confirmed block (aka a commit).
    pub(super) async fn process_confirmed_block(
        &mut self,
        certificate: ConfirmedBlockCertificate,
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        let block = certificate.block();
        let height = block.header.height;
        let chain_id = block.header.chain_id;

        // Check that the chain is active and ready for this confirmation.
        let tip = self.chain.tip_state.get().clone();
        if tip.next_block_height > height {
            // We already processed this block.
            let actions = self.create_network_actions().await?;
            self.register_delivery_notifier(height, &actions, notify_when_messages_are_delivered)
                .await;
            return Ok((self.chain_info_response(), actions));
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
            let committees = self.storage.committees_for(epoch..=epoch).await?;
            let Some(committee) = committees.get(&epoch) else {
                let net_description = self
                    .storage
                    .read_network_description()
                    .await?
                    .ok_or_else(|| WorkerError::MissingNetworkDescription)?;
                return Err(WorkerError::EventsNotFound(vec![EventId {
                    chain_id: net_description.admin_chain_id,
                    stream_id: StreamId::system(EPOCH_STREAM_NAME),
                    index: epoch.0,
                }]));
            };
            // This line is duplicated, but this avoids cloning and a lifetimes error.
            certificate.check(committee)?;
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
            // Update the outboxes.
            self.chain.preprocess_block(certificate.value()).await?;
            // Persist chain.
            self.save().await?;
            let actions = self.create_network_actions().await?;
            trace!("Preprocessed confirmed block {height} on chain {chain_id:.8}");
            self.register_delivery_notifier(height, &actions, notify_when_messages_are_delivered)
                .await;
            return Ok((self.chain_info_response(), actions));
        }

        // This should always be true for valid certificates.
        ensure!(
            tip.block_hash == block.header.previous_block_hash,
            WorkerError::InvalidBlockChaining
        );

        // If we got here, `height` is equal to `tip.next_block_height` and the block is
        // properly chained. Verify that the chain is active and that the epoch we used for
        // verifying the certificate is actually the active one on the chain.
        self.ensure_is_active().await?;
        let (epoch, _) = self.chain.current_committee()?;
        check_block_epoch(epoch, chain_id, block.header.epoch)?;

        let published_blobs = block
            .published_blob_ids()
            .iter()
            .filter_map(|blob_id| blobs.remove(blob_id))
            .collect::<Vec<_>>();

        // If height is zero, we haven't initialized the chain state or verified the epoch before -
        // do it now.
        // This will fail if the chain description blob is still missing - but that's alright,
        // because we already wrote the blob state above, so the client can now upload the
        // blob, which will get accepted, and retry.
        if height == BlockHeight::ZERO {
            self.ensure_is_active().await?;
            let (epoch, _) = self.chain.current_committee()?;
            check_block_epoch(epoch, chain_id, block.header.epoch)?;
        }

        // Execute the block and update inboxes.
        let local_time = self.storage.clock().current_time();
        let chain = &mut self.chain;
        chain
            .remove_bundles_from_inboxes(block.header.timestamp, block.body.incoming_bundles())
            .await?;
        let oracle_responses = Some(block.body.oracle_responses.clone());
        let (proposed_block, outcome) = block.clone().into_proposal();
        let verified_outcome = if let Some(mut execution_state) =
            self.execution_state_cache.remove(&outcome.state_hash)
        {
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
            chain
                .execute_block(
                    &proposed_block,
                    local_time,
                    None,
                    &published_blobs,
                    oracle_responses,
                )
                .await?
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
        chain
            .apply_confirmed_block(certificate.value(), local_time)
            .await?;
        self.track_newly_created_chains(&proposed_block, &outcome);
        let mut actions = self.create_network_actions().await?;
        trace!("Processed confirmed block {height} on chain {chain_id:.8}");
        let hash = certificate.hash();
        let event_streams = certificate
            .value()
            .block()
            .body
            .events
            .iter()
            .flatten()
            .map(|event| event.stream_id.clone())
            .collect();
        actions.notifications.push(Notification {
            chain_id,
            reason: Reason::NewBlock {
                height,
                hash,
                event_streams,
            },
        });
        // Persist chain.
        self.save().await?;

        self.block_values
            .insert(Cow::Owned(certificate.into_inner().into_inner()));

        self.register_delivery_notifier(height, &actions, notify_when_messages_are_delivered)
            .await;

        Ok((self.chain_info_response(), actions))
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
    pub(super) async fn process_cross_chain_update(
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
    pub(super) async fn confirm_updated_recipient(
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

    pub async fn update_received_certificate_trackers(
        &mut self,
        new_trackers: BTreeMap<ValidatorPublicKey, u64>,
    ) -> Result<(), WorkerError> {
        self.chain
            .update_received_certificate_trackers(new_trackers);
        self.save().await?;
        Ok(())
    }

    /// Attempts to vote for a leader timeout, if possible.
    pub(super) async fn vote_for_leader_timeout(
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
    pub(super) async fn vote_for_fallback(&mut self) -> Result<(), WorkerError> {
        let chain = &mut self.chain;
        if let (epoch, Some(entry)) = (
            chain.execution_state.system.epoch.get(),
            chain.unskippable_bundles.front(),
        ) {
            let elapsed = self.storage.clock().current_time().delta_since(entry.seen);
            if elapsed >= chain.ownership().timeout_config.fallback_duration {
                let chain_id = chain.chain_id();
                let height = chain.tip_state.get().next_block_height;
                let key_pair = self.config.key_pair();
                if chain
                    .manager
                    .vote_fallback(chain_id, height, *epoch, key_pair)
                {
                    self.save().await?;
                }
            }
        }
        Ok(())
    }

    pub(super) async fn handle_pending_blob(
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
    pub(super) async fn read_certificate(
        &mut self,
        height: BlockHeight,
    ) -> Result<Option<ConfirmedBlockCertificate>, WorkerError> {
        self.ensure_is_active().await?;
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
    pub(super) async fn query_application(
        &mut self,
        query: Query,
    ) -> Result<QueryOutcome, WorkerError> {
        self.ensure_is_active().await?;
        let local_time = self.storage.clock().current_time();
        let outcome = self
            .chain
            .query_application(local_time, query, self.service_runtime_endpoint.as_mut())
            .await?;
        Ok(outcome)
    }

    /// Returns an application's description.
    pub(super) async fn describe_application(
        &mut self,
        application_id: ApplicationId,
    ) -> Result<ApplicationDescription, WorkerError> {
        self.ensure_is_active().await?;
        let response = self.chain.describe_application(application_id).await?;
        Ok(response)
    }

    /// Executes a block without persisting any changes to the state.
    pub(super) async fn stage_block_execution(
        &mut self,
        block: ProposedBlock,
        round: Option<u32>,
        published_blobs: &[Blob],
    ) -> Result<(Block, ChainInfoResponse), WorkerError> {
        self.ensure_is_active().await?;
        let local_time = self.storage.clock().current_time();
        let signer = block.authenticated_signer;
        let (_, committee) = self.chain.current_committee()?;
        block.check_proposal_size(committee.policy().maximum_block_proposal_size)?;

        let outcome = self
            .execute_block(&block, local_time, round, published_blobs)
            .await?;

        // No need to sign: only used internally.
        let mut response = ChainInfoResponse::new(&self.chain, None);
        if let Some(signer) = signer {
            response.info.requested_owner_balance = self
                .chain
                .execution_state
                .system
                .balances
                .get(&signer)
                .await?;
        }

        Ok((outcome.with(block), response))
    }

    /// Validates and executes a block proposed to extend this chain.
    pub(super) async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        self.ensure_is_active().await?;
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
        // Check the epoch.
        let (epoch, committee) = chain.current_committee()?;
        check_block_epoch(epoch, block.chain_id, block.epoch)?;
        let policy = committee.policy().clone();
        block.check_proposal_size(policy.maximum_block_proposal_size)?;
        // Check the authentication of the block.
        ensure!(
            chain.manager.verify_owner(&owner, proposal.content.round)?,
            WorkerError::InvalidOwner
        );
        match original_proposal {
            None => {
                if let Some(signer) = block.authenticated_signer {
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
                if let Some(signer) = block.authenticated_signer {
                    // Check the authentication of the operations in the new block.
                    ensure!(signer == super_owner, WorkerError::InvalidSigner(signer));
                }
                original_proposal.check_signature()?;
            }
        }
        // Check if the chain is ready for this new block proposal.
        chain.tip_state.get().verify_block_chaining(block)?;
        if chain.manager.check_proposed_block(&proposal)? == manager::Outcome::Skip {
            // We already voted for this block.
            return Ok((self.chain_info_response(), NetworkActions::default()));
        }

        // Make sure we remember that a proposal was signed, to determine the correct round to
        // propose in.
        if self.chain.manager.update_signed_proposal(&proposal) {
            self.save().await?;
        }

        let published_blobs = self.load_proposal_blobs(&proposal).await?;
        let ProposalContent {
            block,
            round,
            outcome,
        } = content;

        let local_time = self.storage.clock().current_time();
        ensure!(
            block.timestamp.duration_since(local_time) <= self.config.grace_period,
            WorkerError::InvalidTimestamp
        );
        self.storage.clock().sleep_until(block.timestamp).await;
        let local_time = self.storage.clock().current_time();

        self.chain
            .remove_bundles_from_inboxes(block.timestamp, block.incoming_bundles())
            .await?;
        let outcome = if let Some(outcome) = outcome {
            outcome.clone()
        } else {
            self.execute_block(block, local_time, round.multi_leader(), &published_blobs)
                .await?
        };

        ensure!(
            !round.is_fast() || !outcome.has_oracle_responses(),
            WorkerError::FastBlockUsingOracles
        );
        let chain = &mut self.chain;
        // Check if the counters of tip_state would be valid.
        chain
            .tip_state
            .get_mut()
            .update_counters(&block.transactions, &outcome.messages)?;
        // Verify that the resulting chain would have no unconfirmed incoming messages.
        chain.validate_incoming_bundles().await?;
        // Don't save the changes since the block is not confirmed yet.
        chain.rollback();

        // Create the vote and store it in the chain state.
        let block = outcome.with(proposal.content.block.clone());
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
        let actions = self.create_network_actions().await?;
        Ok((self.chain_info_response(), actions))
    }

    /// Prepares a [`ChainInfoResponse`] for a [`ChainInfoQuery`].
    pub(super) async fn prepare_chain_info_response(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, WorkerError> {
        self.ensure_is_active().await?;
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
            ensure!(
                chain.tip_state.get().next_block_height == next_block_height,
                WorkerError::UnexpectedBlockHeight {
                    expected_block_height: next_block_height,
                    found_block_height: chain.tip_state.get().next_block_height
                }
            );
        }
        if query.request_pending_message_bundles {
            let mut messages = Vec::new();
            let pairs = chain.inboxes.try_load_all_entries().await?;
            let action = if *chain.execution_state.system.closed.get() {
                MessageAction::Reject
            } else {
                MessageAction::Accept
            };
            for (origin, inbox) in pairs {
                for bundle in inbox.added_bundles.elements().await? {
                    messages.push(IncomingBundle {
                        origin,
                        bundle,
                        action,
                    });
                }
            }

            info.requested_pending_message_bundles = messages;
        }
        let mut hashes = Vec::new();
        for height in query.request_sent_certificate_hashes_by_heights {
            hashes.extend(chain.block_hashes(height..=height).await?);
        }
        info.requested_sent_certificate_hashes = hashes;
        if let Some(start) = query.request_received_log_excluding_first_n {
            let start = usize::try_from(start).map_err(|_| ArithmeticError::Overflow)?;
            info.requested_received_log = chain.received_log.read(start..).await?;
        }
        if query.request_manager_values {
            info.manager.add_values(&chain.manager);
        }
        Ok(ChainInfoResponse::new(info, self.config.key_pair()))
    }

    /// Executes a block, caches the result, and returns the outcome.
    async fn execute_block(
        &mut self,
        block: &ProposedBlock,
        local_time: Timestamp,
        round: Option<u32>,
        published_blobs: &[Blob],
    ) -> Result<BlockExecutionOutcome, WorkerError> {
        let outcome =
            Box::pin(
                self.chain
                    .execute_block(block, local_time, round, published_blobs, None),
            )
            .await?;
        self.execution_state_cache.insert_owned(
            &outcome.state_hash,
            self.chain
                .execution_state
                .with_context(|ctx| InactiveContext(ctx.base_key().clone()))
                .await,
        );
        Ok(outcome)
    }

    /// Ensures that the current chain is active, returning an error otherwise.
    async fn ensure_is_active(&mut self) -> Result<(), WorkerError> {
        if !self.knows_chain_is_active {
            let local_time = self.storage.clock().current_time();
            self.chain.ensure_is_active(local_time).await?;
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
    async fn save(&mut self) -> Result<(), WorkerError> {
        self.clear_shared_chain_view().await;
        self.chain.save().await?;
        Ok(())
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

/// Helper type for handling cross-chain updates.
pub(crate) struct CrossChainUpdateHelper<'a> {
    pub allow_messages_from_deprecated_epochs: bool,
    pub current_epoch: Epoch,
    pub committees: &'a BTreeMap<Epoch, Committee>,
}

impl<'a> CrossChainUpdateHelper<'a> {
    /// Creates a new [`CrossChainUpdateHelper`].
    pub fn new<C>(config: &ChainWorkerConfig, chain: &'a ChainStateView<C>) -> Self
    where
        C: Context + Clone + Send + Sync + 'static,
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
    pub fn select_message_bundles(
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

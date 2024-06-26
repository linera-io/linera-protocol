// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The state and functionality of a chain worker.

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use futures::{future, FutureExt as _};
use linera_base::{
    crypto::CryptoHash,
    data_types::{ArithmeticError, BlockHeight, HashedBlob, Timestamp},
    ensure,
    identifiers::{BlobId, ChainId},
};
use linera_chain::{
    data_types::{
        Block, BlockExecutionOutcome, BlockProposal, Certificate, CertificateValue, ExecutedBlock,
        HashedCertificateValue, IncomingMessage, Medium, MessageAction, MessageBundle, Origin,
        ProposalContent, Target,
    },
    manager, ChainError, ChainStateView,
};
use linera_execution::{
    committee::{Committee, Epoch},
    BytecodeLocation, Query, Response, UserApplicationDescription, UserApplicationId,
};
use linera_storage::Storage;
use linera_views::{
    common::Context,
    views::{ClonableView, RootView, View, ViewError},
};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};
use tracing::{debug, warn};
#[cfg(with_testing)]
use {linera_base::identifiers::BytecodeId, linera_chain::data_types::Event};

use super::ChainWorkerConfig;
use crate::{
    data_types::{ChainInfo, ChainInfoQuery, ChainInfoResponse, CrossChainRequest},
    value_cache::ValueCache,
    worker::{NetworkActions, Notification, Reason, WorkerError},
};

/// The state of the chain worker.
pub struct ChainWorkerState<StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
    ViewError: From<StorageClient::ContextError>,
{
    config: ChainWorkerConfig,
    storage: StorageClient,
    chain: ChainStateView<StorageClient::Context>,
    shared_chain_view: Option<Arc<RwLock<ChainStateView<StorageClient::Context>>>>,
    recent_hashed_certificate_values: Arc<ValueCache<CryptoHash, HashedCertificateValue>>,
    recent_hashed_blobs: Arc<ValueCache<BlobId, HashedBlob>>,
    knows_chain_is_active: bool,
}

impl<StorageClient> ChainWorkerState<StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
    ViewError: From<StorageClient::ContextError>,
{
    /// Creates a new [`ChainWorkerState`] using the provided `storage` client.
    pub async fn load(
        config: ChainWorkerConfig,
        storage: StorageClient,
        certificate_value_cache: Arc<ValueCache<CryptoHash, HashedCertificateValue>>,
        blob_cache: Arc<ValueCache<BlobId, HashedBlob>>,
        chain_id: ChainId,
    ) -> Result<Self, WorkerError> {
        let chain = storage.load_chain(chain_id).await?;

        Ok(ChainWorkerState {
            config,
            storage,
            chain,
            shared_chain_view: None,
            recent_hashed_certificate_values: certificate_value_cache,
            recent_hashed_blobs: blob_cache,
            knows_chain_is_active: false,
        })
    }

    /// Returns the [`ChainId`] of the chain handled by this worker.
    pub fn chain_id(&self) -> ChainId {
        self.chain.chain_id()
    }

    /// Returns a read-only view of the [`ChainStateView`].
    ///
    /// The returned view holds a lock on the chain state, which prevents the worker from changing
    /// it.
    pub async fn chain_state_view(
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
    pub async fn read_certificate(
        &mut self,
        height: BlockHeight,
    ) -> Result<Option<Certificate>, WorkerError> {
        ChainWorkerStateWithTemporaryChanges(self)
            .read_certificate(height)
            .await
    }

    /// Searches for an event in one of the chain's inboxes.
    #[cfg(with_testing)]
    pub async fn find_event_in_inbox(
        &mut self,
        inbox_id: Origin,
        certificate_hash: CryptoHash,
        height: BlockHeight,
        index: u32,
    ) -> Result<Option<Event>, WorkerError> {
        ChainWorkerStateWithTemporaryChanges(self)
            .find_event_in_inbox(inbox_id, certificate_hash, height, index)
            .await
    }

    /// Queries an application's state on the chain.
    pub async fn query_application(&mut self, query: Query) -> Result<Response, WorkerError> {
        ChainWorkerStateWithTemporaryChanges(self)
            .query_application(query)
            .await
    }

    /// Returns the [`BytecodeLocation`] for the requested [`BytecodeId`], if it is known by the
    /// chain.
    #[cfg(with_testing)]
    pub async fn read_bytecode_location(
        &mut self,
        bytecode_id: BytecodeId,
    ) -> Result<Option<BytecodeLocation>, WorkerError> {
        ChainWorkerStateWithTemporaryChanges(self)
            .read_bytecode_location(bytecode_id)
            .await
    }

    /// Returns an application's description.
    pub async fn describe_application(
        &mut self,
        application_id: UserApplicationId,
    ) -> Result<UserApplicationDescription, WorkerError> {
        ChainWorkerStateWithTemporaryChanges(self)
            .describe_application(application_id)
            .await
    }

    /// Executes a block without persisting any changes to the state.
    pub async fn stage_block_execution(
        &mut self,
        block: Block,
    ) -> Result<(ExecutedBlock, ChainInfoResponse), WorkerError> {
        ChainWorkerStateWithTemporaryChanges(self)
            .stage_block_execution(block)
            .await
    }

    /// Processes a leader timeout issued for this multi-owner chain.
    pub async fn process_timeout(
        &mut self,
        certificate: Certificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        ChainWorkerStateWithAttemptedChanges::from(self)
            .process_timeout(certificate)
            .await
    }

    /// Handles a proposal for the next block for this chain.
    pub async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        let maybe_validation_outcome = ChainWorkerStateWithTemporaryChanges(self)
            .validate_block_proposal(&proposal)
            .await?;

        let actions = if let Some((outcome, local_time)) = maybe_validation_outcome {
            ChainWorkerStateWithAttemptedChanges::from(&mut *self)
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
    pub async fn process_validated_block(
        &mut self,
        certificate: Certificate,
    ) -> Result<(ChainInfoResponse, NetworkActions, bool), WorkerError> {
        ChainWorkerStateWithAttemptedChanges::from(self)
            .process_validated_block(certificate)
            .await
    }

    /// Processes a confirmed block (aka a commit).
    pub async fn process_confirmed_block(
        &mut self,
        certificate: Certificate,
        hashed_certificate_values: &[HashedCertificateValue],
        hashed_blobs: &[HashedBlob],
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        ChainWorkerStateWithAttemptedChanges::from(self)
            .process_confirmed_block(certificate, hashed_certificate_values, hashed_blobs)
            .await
    }

    /// Updates the chain's inboxes, receiving messages from a cross-chain update.
    pub async fn process_cross_chain_update(
        &mut self,
        origin: Origin,
        bundles: Vec<MessageBundle>,
    ) -> Result<Option<BlockHeight>, WorkerError> {
        ChainWorkerStateWithAttemptedChanges::from(self)
            .process_cross_chain_update(origin, bundles)
            .await
    }

    /// Handles the cross-chain request confirming that the recipient was updated.
    pub async fn confirm_updated_recipient(
        &mut self,
        latest_heights: Vec<(Target, BlockHeight)>,
    ) -> Result<BlockHeight, WorkerError> {
        ChainWorkerStateWithAttemptedChanges::from(self)
            .confirm_updated_recipient(latest_heights)
            .await
    }

    /// Handles a [`ChainInfoQuery`], potentially voting on the next block.
    pub async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        if query.request_leader_timeout {
            ChainWorkerStateWithAttemptedChanges::from(&mut *self)
                .vote_for_leader_timeout()
                .await?;
        }
        if query.request_fallback {
            ChainWorkerStateWithAttemptedChanges::from(&mut *self)
                .vote_for_fallback()
                .await?;
        }
        let response = ChainWorkerStateWithTemporaryChanges(self)
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

    /// Returns an error if the block requires bytecode or a blob we don't have, or if unrelated bytecode
    /// hashed certificate values or blobs were provided.
    async fn check_no_missing_blobs(
        &self,
        block: &Block,
        hashed_certificate_values: &[HashedCertificateValue],
        hashed_blobs: &[HashedBlob],
    ) -> Result<(), WorkerError> {
        let missing_bytecodes = self
            .get_missing_bytecodes(block, hashed_certificate_values)
            .await?;
        let missing_blobs = self.get_missing_blobs(block, hashed_blobs).await?;

        if missing_bytecodes.is_empty() && missing_blobs.is_empty() {
            return Ok(());
        }

        Err(WorkerError::ApplicationBytecodesOrBlobsNotFound(
            missing_bytecodes,
            missing_blobs,
        ))
    }

    /// Returns the blobs required by the block that we don't have, or an error if unrelated blobs were provided.
    async fn get_missing_blobs(
        &self,
        block: &Block,
        hashed_blobs: &[HashedBlob],
    ) -> Result<Vec<BlobId>, WorkerError> {
        let mut required_blob_ids = block.blob_ids();
        // Find all certificates containing blobs used when executing this block.
        for hashed_blob in hashed_blobs {
            let blob_id = hashed_blob.id();
            ensure!(
                required_blob_ids.remove(&blob_id),
                WorkerError::UnneededBlob { blob_id }
            );
        }

        let pending_blobs = &self.chain.manager.get().pending_blobs;
        Ok(self
            .recent_hashed_blobs
            .subtract_cached_items_from::<_, Vec<_>>(required_blob_ids, |id| id)
            .await
            .into_iter()
            .filter(|blob_id| !pending_blobs.contains_key(blob_id))
            .collect())
    }

    /// Returns the blobs requested by their `blob_ids` that are either in pending in the
    /// chain or in the `recent_hashed_blobs` cache.
    async fn get_blobs(&self, blob_ids: HashSet<BlobId>) -> Result<Vec<HashedBlob>, WorkerError> {
        let pending_blobs = &self.chain.manager.get().pending_blobs;
        let (found_blobs, not_found_blobs): (HashMap<BlobId, HashedBlob>, HashSet<BlobId>) =
            self.recent_hashed_blobs.try_get_many(blob_ids).await;

        let mut blobs = found_blobs.into_values().collect::<Vec<_>>();
        for blob_id in not_found_blobs {
            if let Some(blob) = pending_blobs.get(&blob_id) {
                blobs.push(blob.clone());
            }
        }

        Ok(blobs)
    }

    /// Returns an error if the block requires bytecode we don't have, or if unrelated bytecode
    /// hashed certificate values were provided.
    async fn get_missing_bytecodes(
        &self,
        block: &Block,
        hashed_certificate_values: &[HashedCertificateValue],
    ) -> Result<Vec<BytecodeLocation>, WorkerError> {
        // Find all certificates containing bytecode used when executing this block.
        let mut required_locations_left: HashMap<_, _> = block
            .bytecode_locations()
            .into_iter()
            .map(|bytecode_location| (bytecode_location.certificate_hash, bytecode_location))
            .collect();
        for value in hashed_certificate_values {
            let value_hash = value.hash();
            ensure!(
                required_locations_left.remove(&value_hash).is_some(),
                WorkerError::UnneededValue { value_hash }
            );
        }
        let tasks = self
            .recent_hashed_certificate_values
            .subtract_cached_items_from::<_, Vec<_>>(
                required_locations_left.into_values(),
                |location| &location.certificate_hash,
            )
            .await
            .into_iter()
            .map(|location| {
                self.storage
                    .contains_hashed_certificate_value(location.certificate_hash)
                    .map(move |result| (location, result))
            })
            .collect::<Vec<_>>();
        let mut missing_locations = vec![];
        for (location, result) in future::join_all(tasks).await {
            match result {
                Ok(true) => {}
                Ok(false) => missing_locations.push(location),
                Err(err) => Err(err)?,
            }
        }

        Ok(missing_locations)
    }

    /// Inserts a [`HashedBlob`] into the worker's cache.
    async fn cache_recent_blob<'a>(&mut self, hashed_blob: Cow<'a, HashedBlob>) -> bool {
        self.recent_hashed_blobs.insert(hashed_blob).await
    }

    /// Loads pending cross-chain requests.
    async fn create_network_actions(&self) -> Result<NetworkActions, WorkerError> {
        let mut heights_by_recipient: BTreeMap<_, BTreeMap<_, _>> = Default::default();
        let targets = self.chain.outboxes.indices().await?;
        let outboxes = self.chain.outboxes.try_load_entries(&targets).await?;
        for (target, outbox) in targets.into_iter().zip(outboxes) {
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

/// Wrapper type that rolls back changes to the `chain` state when dropped.
pub struct ChainWorkerStateWithTemporaryChanges<'state, StorageClient>(
    &'state mut ChainWorkerState<StorageClient>,
)
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
    ViewError: From<StorageClient::ContextError>;

impl<StorageClient> ChainWorkerStateWithTemporaryChanges<'_, StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
    ViewError: From<StorageClient::ContextError>,
{
    /// Returns a stored [`Certificate`] for the chain's block at the requested [`BlockHeight`].
    #[cfg(with_testing)]
    pub async fn read_certificate(
        &mut self,
        height: BlockHeight,
    ) -> Result<Option<Certificate>, WorkerError> {
        self.0.ensure_is_active()?;
        let certificate_hash = match self.0.chain.confirmed_log.get(height.try_into()?).await? {
            Some(hash) => hash,
            None => return Ok(None),
        };
        let certificate = self.0.storage.read_certificate(certificate_hash).await?;
        Ok(Some(certificate))
    }

    /// Searches for an event in one of the chain's inboxes.
    #[cfg(with_testing)]
    pub async fn find_event_in_inbox(
        &mut self,
        inbox_id: Origin,
        certificate_hash: CryptoHash,
        height: BlockHeight,
        index: u32,
    ) -> Result<Option<Event>, WorkerError> {
        self.0.ensure_is_active()?;

        let mut inbox = self.0.chain.inboxes.try_load_entry_mut(&inbox_id).await?;
        let mut events = inbox.added_events.iter_mut().await?;

        Ok(events
            .find(|event| {
                event.certificate_hash == certificate_hash
                    && event.height == height
                    && event.index == index
            })
            .cloned())
    }

    /// Queries an application's state on the chain.
    pub async fn query_application(&mut self, query: Query) -> Result<Response, WorkerError> {
        self.0.ensure_is_active()?;
        let local_time = self.0.storage.clock().current_time();
        let response = self.0.chain.query_application(local_time, query).await?;
        Ok(response)
    }

    /// Returns the [`BytecodeLocation`] for the requested [`BytecodeId`], if it is known by the
    /// chain.
    #[cfg(with_testing)]
    pub async fn read_bytecode_location(
        &mut self,
        bytecode_id: BytecodeId,
    ) -> Result<Option<BytecodeLocation>, WorkerError> {
        self.0.ensure_is_active()?;
        let response = self.0.chain.read_bytecode_location(bytecode_id).await?;
        Ok(response)
    }

    /// Returns an application's description.
    pub async fn describe_application(
        &mut self,
        application_id: UserApplicationId,
    ) -> Result<UserApplicationDescription, WorkerError> {
        self.0.ensure_is_active()?;
        let response = self.0.chain.describe_application(application_id).await?;
        Ok(response)
    }

    /// Executes a block without persisting any changes to the state.
    pub async fn stage_block_execution(
        &mut self,
        block: Block,
    ) -> Result<(ExecutedBlock, ChainInfoResponse), WorkerError> {
        self.0.ensure_is_active()?;

        let local_time = self.0.storage.clock().current_time();
        let signer = block.authenticated_signer;

        let executed_block = Box::pin(self.0.chain.execute_block(&block, local_time, None))
            .await?
            .with(block);

        let mut response = ChainInfoResponse::new(&self.0.chain, None);
        if let Some(signer) = signer {
            response.info.requested_owner_balance = self
                .0
                .chain
                .execution_state
                .system
                .balances
                .get(&signer)
                .await?;
        }

        Ok((executed_block, response))
    }

    /// Validates a proposal for the next block for this chain.
    pub async fn validate_block_proposal(
        &mut self,
        proposal: &BlockProposal,
    ) -> Result<Option<(BlockExecutionOutcome, Timestamp)>, WorkerError> {
        let BlockProposal {
            content:
                ProposalContent {
                    block,
                    round,
                    forced_oracle_records: oracle_records,
                },
            owner,
            hashed_certificate_values,
            hashed_blobs,
            validated_block_certificate,
            signature: _,
        } = proposal;
        ensure!(
            validated_block_certificate.is_some() == oracle_records.is_some(),
            WorkerError::InvalidBlockProposal(
                "Must contain a certificate if and only if it contains oracle records".to_string()
            )
        );
        self.0.ensure_is_active()?;
        // Check the epoch.
        let (epoch, committee) = self
            .0
            .chain
            .execution_state
            .system
            .current_committee()
            .expect("chain is active");
        check_block_epoch(epoch, block)?;
        // Check the authentication of the block.
        let public_key = self
            .0
            .chain
            .manager
            .get()
            .verify_owner(proposal)
            .ok_or(WorkerError::InvalidOwner)?;
        proposal.check_signature(public_key)?;
        if let Some(lite_certificate) = validated_block_certificate {
            // Verify that this block has been validated by a quorum before.
            lite_certificate.check(committee)?;
        } else if let Some(signer) = block.authenticated_signer {
            // Check the authentication of the operations in the new block.
            ensure!(signer == *owner, WorkerError::InvalidSigner(signer));
        }
        // Check if the chain is ready for this new block proposal.
        // This should always pass for nodes without voting key.
        self.0.chain.tip_state.get().verify_block_chaining(block)?;
        if self.0.chain.manager.get().check_proposed_block(proposal)? == manager::Outcome::Skip {
            return Ok(None);
        }
        // Update the inboxes so that we can verify the provided hashed certificate values are
        // legitimately required.
        // Actual execution happens below, after other validity checks.
        self.0.chain.remove_events_from_inboxes(block).await?;
        // Verify that all required bytecode hashed certificate values are available, and no
        // unrelated ones provided.
        self.0
            .check_no_missing_blobs(block, hashed_certificate_values, hashed_blobs)
            .await?;
        // Write the values so that the bytecode is available during execution.
        self.0
            .storage
            .write_hashed_certificate_values(hashed_certificate_values)
            .await?;
        let local_time = self.0.storage.clock().current_time();
        ensure!(
            block.timestamp.duration_since(local_time) <= self.0.config.grace_period,
            WorkerError::InvalidTimestamp
        );
        self.0.storage.clock().sleep_until(block.timestamp).await;
        let local_time = self.0.storage.clock().current_time();
        let outcome = Box::pin(self.0.chain.execute_block(
            block,
            local_time,
            oracle_records.clone(),
        ))
        .await?;
        if let Some(lite_certificate) = &validated_block_certificate {
            let value = HashedCertificateValue::new_validated(outcome.clone().with(block.clone()));
            lite_certificate
                .clone()
                .with_value(value)
                .ok_or_else(|| WorkerError::InvalidLiteCertificate)?;
        }
        if round.is_fast() {
            ensure!(
                outcome
                    .oracle_records
                    .iter()
                    .all(|record| record.is_permitted_in_fast_blocks()),
                WorkerError::FastBlockUsingOracles
            );
        }
        // Check if the counters of tip_state would be valid.
        self.0
            .chain
            .tip_state
            .get()
            .verify_counters(block, &outcome)?;
        // Verify that the resulting chain would have no unconfirmed incoming messages.
        self.0.chain.validate_incoming_messages().await?;
        Ok(Some((outcome, local_time)))
    }

    /// Prepares a [`ChainInfoResponse`] for a [`ChainInfoQuery`].
    pub async fn prepare_chain_info_response(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, WorkerError> {
        let chain = &self.0.chain;
        let mut info = ChainInfo::from(chain);
        if query.request_committees {
            info.requested_committees = Some(chain.execution_state.system.committees.get().clone());
        }
        if let Some(owner) = query.request_owner_balance {
            info.requested_owner_balance =
                chain.execution_state.system.balances.get(&owner).await?;
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
        if query.request_pending_messages {
            let mut messages = Vec::new();
            let origins = chain.inboxes.indices().await?;
            let inboxes = chain.inboxes.try_load_entries(&origins).await?;
            let action = if *chain.execution_state.system.closed.get() {
                MessageAction::Reject
            } else {
                MessageAction::Accept
            };
            for (origin, inbox) in origins.into_iter().zip(inboxes) {
                for event in inbox.added_events.elements().await? {
                    messages.push(IncomingMessage {
                        origin: origin.clone(),
                        event: event.clone(),
                        action,
                    });
                }
            }

            info.requested_pending_messages = messages;
        }
        if let Some(range) = query.request_sent_certificate_hashes_in_range {
            let start: usize = range.start.try_into()?;
            let end = match range.limit {
                None => chain.confirmed_log.count(),
                Some(limit) => start
                    .checked_add(usize::try_from(limit).map_err(|_| ArithmeticError::Overflow)?)
                    .ok_or(ArithmeticError::Overflow)?
                    .min(chain.confirmed_log.count()),
            };
            let keys = chain.confirmed_log.read(start..end).await?;
            info.requested_sent_certificate_hashes = keys;
        }
        if let Some(start) = query.request_received_log_excluding_first_nth {
            let start = usize::try_from(start).map_err(|_| ArithmeticError::Overflow)?;
            info.requested_received_log = chain.received_log.read(start..).await?;
        }
        if query.request_manager_values {
            info.manager.add_values(chain.manager.get());
        }
        Ok(ChainInfoResponse::new(info, self.0.config.key_pair()))
    }
}

impl<StorageClient> Drop for ChainWorkerStateWithTemporaryChanges<'_, StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
    ViewError: From<StorageClient::ContextError>,
{
    fn drop(&mut self) {
        self.0.chain.rollback();
    }
}

/// Wrapper type that tracks if the changes to the `chain` state should be rolled back when
/// dropped.
pub struct ChainWorkerStateWithAttemptedChanges<'state, StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
    ViewError: From<StorageClient::ContextError>,
{
    state: &'state mut ChainWorkerState<StorageClient>,
    succeeded: bool,
}

impl<'state, StorageClient> From<&'state mut ChainWorkerState<StorageClient>>
    for ChainWorkerStateWithAttemptedChanges<'state, StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
    ViewError: From<StorageClient::ContextError>,
{
    fn from(state: &'state mut ChainWorkerState<StorageClient>) -> Self {
        ChainWorkerStateWithAttemptedChanges {
            state,
            succeeded: false,
        }
    }
}

impl<StorageClient> ChainWorkerStateWithAttemptedChanges<'_, StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
    ViewError: From<StorageClient::ContextError>,
{
    /// Processes a leader timeout issued for this multi-owner chain.
    pub async fn process_timeout(
        &mut self,
        certificate: Certificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        let (chain_id, height, epoch) = match certificate.value() {
            CertificateValue::Timeout {
                chain_id,
                height,
                epoch,
                ..
            } => (*chain_id, *height, *epoch),
            _ => panic!("Expecting a leader timeout certificate"),
        };
        // Check that the chain is active and ready for this timeout.
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        self.state.ensure_is_active()?;
        let (chain_epoch, committee) = self
            .state
            .chain
            .execution_state
            .system
            .current_committee()
            .expect("chain is active");
        ensure!(
            epoch == chain_epoch,
            WorkerError::InvalidEpoch {
                chain_id,
                chain_epoch,
                epoch
            }
        );
        certificate.check(committee)?;
        let mut actions = NetworkActions::default();
        if self
            .state
            .chain
            .tip_state
            .get()
            .already_validated_block(height)?
        {
            return Ok((
                ChainInfoResponse::new(&self.state.chain, self.state.config.key_pair()),
                actions,
            ));
        }
        let old_round = self.state.chain.manager.get().current_round;
        self.state
            .chain
            .manager
            .get_mut()
            .handle_timeout_certificate(
                certificate.clone(),
                self.state.storage.clock().current_time(),
            );
        let round = self.state.chain.manager.get().current_round;
        if round > old_round {
            actions.notifications.push(Notification {
                chain_id,
                reason: Reason::NewRound { height, round },
            })
        }
        let info = ChainInfoResponse::new(&self.state.chain, self.state.config.key_pair());
        self.save().await?;
        Ok((info, actions))
    }

    /// Votes for a block proposal for the next block for this chain.
    pub async fn vote_for_block_proposal(
        &mut self,
        proposal: BlockProposal,
        outcome: BlockExecutionOutcome,
        local_time: Timestamp,
    ) -> Result<(), WorkerError> {
        // Create the vote and store it in the chain state.
        let manager = self.state.chain.manager.get_mut();
        manager.create_vote(proposal, outcome, self.state.config.key_pair(), local_time);
        // Cache the value we voted on, so the client doesn't have to send it again.
        if let Some(vote) = manager.pending() {
            self.state
                .recent_hashed_certificate_values
                .insert(Cow::Borrowed(&vote.value))
                .await;
        }
        self.save().await?;
        Ok(())
    }

    /// Processes a validated block issued for this multi-owner chain.
    pub async fn process_validated_block(
        &mut self,
        certificate: Certificate,
    ) -> Result<(ChainInfoResponse, NetworkActions, bool), WorkerError> {
        let block = match certificate.value() {
            CertificateValue::ValidatedBlock {
                executed_block: ExecutedBlock { block, .. },
            } => block,
            _ => panic!("Expecting a validation certificate"),
        };
        let height = block.height;
        // Check that the chain is active and ready for this validated block.
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        self.state.ensure_is_active()?;
        let (epoch, committee) = self
            .state
            .chain
            .execution_state
            .system
            .current_committee()
            .expect("chain is active");
        check_block_epoch(epoch, block)?;
        certificate.check(committee)?;
        let mut actions = NetworkActions::default();
        let already_validated_block = self
            .state
            .chain
            .tip_state
            .get()
            .already_validated_block(height)?;
        let should_skip_validated_block = || {
            self.state
                .chain
                .manager
                .get()
                .check_validated_block(&certificate)
                .map(|outcome| outcome == manager::Outcome::Skip)
        };
        if already_validated_block || should_skip_validated_block()? {
            // If we just processed the same pending block, return the chain info unchanged.
            return Ok((
                ChainInfoResponse::new(&self.state.chain, self.state.config.key_pair()),
                actions,
                true,
            ));
        }
        self.state
            .recent_hashed_certificate_values
            .insert(Cow::Borrowed(&certificate.value))
            .await;
        let old_round = self.state.chain.manager.get().current_round;
        self.state.chain.manager.get_mut().create_final_vote(
            certificate,
            self.state.config.key_pair(),
            self.state.storage.clock().current_time(),
        );
        let info = ChainInfoResponse::new(&self.state.chain, self.state.config.key_pair());
        self.save().await?;
        let round = self.state.chain.manager.get().current_round;
        if round > old_round {
            actions.notifications.push(Notification {
                chain_id: self.state.chain_id(),
                reason: Reason::NewRound { height, round },
            })
        }
        Ok((info, actions, false))
    }

    /// Processes a confirmed block (aka a commit).
    pub async fn process_confirmed_block(
        &mut self,
        certificate: Certificate,
        hashed_certificate_values: &[HashedCertificateValue],
        hashed_blobs: &[HashedBlob],
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        let CertificateValue::ConfirmedBlock { executed_block, .. } = certificate.value() else {
            panic!("Expecting a confirmation certificate");
        };
        let block = &executed_block.block;
        let BlockExecutionOutcome {
            messages,
            state_hash,
            oracle_records,
        } = &executed_block.outcome;
        // Check that the chain is active and ready for this confirmation.
        let tip = self.state.chain.tip_state.get().clone();
        if tip.next_block_height < block.height {
            return Err(WorkerError::MissingEarlierBlocks {
                current_block_height: tip.next_block_height,
            });
        }
        if tip.next_block_height > block.height {
            // Block was already confirmed.
            let info = ChainInfoResponse::new(&self.state.chain, self.state.config.key_pair());
            let actions = self.state.create_network_actions().await?;
            return Ok((info, actions));
        }
        if tip.is_first_block() && !self.state.chain.is_active() {
            let local_time = self.state.storage.clock().current_time();
            for message in &block.incoming_messages {
                if self
                    .state
                    .chain
                    .execute_init_message(
                        message.id(),
                        &message.event.message,
                        message.event.timestamp,
                        local_time,
                    )
                    .await?
                {
                    break;
                }
            }
        }
        self.state.ensure_is_active()?;
        // Verify the certificate.
        let (epoch, committee) = self
            .state
            .chain
            .execution_state
            .system
            .current_committee()
            .expect("chain is active");
        check_block_epoch(epoch, block)?;
        certificate.check(committee)?;
        // This should always be true for valid certificates.
        ensure!(
            tip.block_hash == block.previous_block_hash,
            WorkerError::InvalidBlockChaining
        );
        // Verify that all required bytecode hashed certificate values are available, and no
        // unrelated ones provided.
        self.state
            .check_no_missing_blobs(block, hashed_certificate_values, hashed_blobs)
            .await?;
        // Persist certificate and hashed certificate values.
        self.state
            .recent_hashed_certificate_values
            .insert_all(hashed_certificate_values.iter().map(Cow::Borrowed))
            .await;
        for hashed_blob in hashed_blobs {
            self.state
                .cache_recent_blob(Cow::Borrowed(hashed_blob))
                .await;
        }

        let blobs_in_block = self.state.get_blobs(block.blob_ids()).await?;
        let certificate_hash = certificate.hash();
        let (result_hashed_certificate_value, result_blobs, result_certificate) = tokio::join!(
            self.state
                .storage
                .write_hashed_certificate_values(hashed_certificate_values),
            self.state
                .storage
                .write_hashed_blobs(&blobs_in_block, &certificate_hash),
            self.state.storage.write_certificate(&certificate)
        );
        result_hashed_certificate_value?;
        result_blobs?;
        result_certificate?;
        // Execute the block and update inboxes.
        self.state.chain.remove_events_from_inboxes(block).await?;
        let local_time = self.state.storage.clock().current_time();
        let verified_outcome = Box::pin(self.state.chain.execute_block(
            block,
            local_time,
            Some(oracle_records.clone()),
        ))
        .await?;
        // We should always agree on the messages and state hash.
        ensure!(
            *messages == verified_outcome.messages,
            WorkerError::IncorrectMessages {
                computed: verified_outcome.messages,
                submitted: messages.clone(),
            }
        );
        ensure!(
            *state_hash == verified_outcome.state_hash,
            WorkerError::IncorrectStateHash
        );
        // Advance to next block height.
        let tip = self.state.chain.tip_state.get_mut();
        tip.block_hash = Some(certificate.hash());
        tip.next_block_height.try_add_assign_one()?;
        tip.num_incoming_messages += block.incoming_messages.len() as u32;
        tip.num_operations += block.operations.len() as u32;
        tip.num_outgoing_messages += messages.len() as u32;
        self.state.chain.confirmed_log.push(certificate.hash());
        let info = ChainInfoResponse::new(&self.state.chain, self.state.config.key_pair());
        let mut actions = self.state.create_network_actions().await?;
        actions.notifications.push(Notification {
            chain_id: block.chain_id,
            reason: Reason::NewBlock {
                height: block.height,
                hash: certificate.value.hash(),
            },
        });
        // Persist chain.
        self.save().await?;
        self.state
            .recent_hashed_certificate_values
            .insert(Cow::Owned(certificate.value))
            .await;

        Ok((info, actions))
    }

    /// Updates the chain's inboxes, receiving messages from a cross-chain update.
    pub async fn process_cross_chain_update(
        &mut self,
        origin: Origin,
        bundles: Vec<MessageBundle>,
    ) -> Result<Option<BlockHeight>, WorkerError> {
        // Only process certificates with relevant heights and epochs.
        let next_height_to_receive = self
            .state
            .chain
            .next_block_height_to_receive(&origin)
            .await?;
        let last_anticipated_block_height = self
            .state
            .chain
            .last_anticipated_block_height(&origin)
            .await?;
        let helper = CrossChainUpdateHelper::new(&self.state.config, &self.state.chain);
        let recipient = self.state.chain_id();
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
        let local_time = self.state.storage.clock().current_time();
        for bundle in bundles {
            // Update the staged chain state with the received block.
            self.state
                .chain
                .receive_message_bundle(&origin, bundle, local_time)
                .await?
        }
        if !self.state.config.allow_inactive_chains && !self.state.chain.is_active() {
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
    pub async fn confirm_updated_recipient(
        &mut self,
        latest_heights: Vec<(Target, BlockHeight)>,
    ) -> Result<BlockHeight, WorkerError> {
        let mut height_with_fully_delivered_messages = BlockHeight::ZERO;

        for (target, height) in latest_heights {
            let fully_delivered = self
                .state
                .chain
                .mark_messages_as_received(&target, height)
                .await?
                && self.state.chain.all_messages_delivered_up_to(height);

            if fully_delivered && height > height_with_fully_delivered_messages {
                height_with_fully_delivered_messages = height;
            }
        }

        self.save().await?;

        Ok(height_with_fully_delivered_messages)
    }

    /// Attempts to vote for a leader timeout, if possible.
    pub async fn vote_for_leader_timeout(&mut self) -> Result<(), WorkerError> {
        let chain = &mut self.state.chain;
        if let Some(epoch) = chain.execution_state.system.epoch.get() {
            let chain_id = chain.chain_id();
            let height = chain.tip_state.get().next_block_height;
            let key_pair = self.state.config.key_pair();
            let local_time = self.state.storage.clock().current_time();
            let manager = chain.manager.get_mut();
            if manager.vote_timeout(chain_id, height, *epoch, key_pair, local_time) {
                self.save().await?;
            }
        }
        Ok(())
    }

    /// Votes for falling back to a public chain.
    ///
    pub async fn vote_for_fallback(&mut self) -> Result<(), WorkerError> {
        let chain = &mut self.state.chain;
        if let (Some(epoch), Some(entry)) = (
            chain.execution_state.system.epoch.get(),
            chain.unskippable.front().await?,
        ) {
            let ownership = chain.execution_state.system.ownership.get();
            let elapsed = self
                .state
                .storage
                .clock()
                .current_time()
                .delta_since(entry.seen);
            if elapsed >= ownership.timeout_config.fallback_duration {
                let chain_id = chain.chain_id();
                let height = chain.tip_state.get().next_block_height;
                let key_pair = self.state.config.key_pair();
                let manager = chain.manager.get_mut();
                if manager.vote_fallback(chain_id, height, *epoch, key_pair) {
                    self.save().await?;
                }
            }
        }
        Ok(())
    }

    /// Stores the chain state in persistent storage.
    ///
    /// Waits until the [`ChainStateView`] is no longer shared before persisting the changes.
    async fn save(&mut self) -> Result<(), WorkerError> {
        // SAFETY: this is the only place a write-lock is acquired, and read-locks are acquired in
        // the `chain_state_view` method, which has a `&mut self` receiver like this `save` method.
        // That means that when the write-lock is acquired, no readers will be waiting to acquire
        // the lock. This is important because otherwise readers could have a stale view of the
        // chain state.
        let maybe_shared_chain_view = self.state.shared_chain_view.take();
        let _maybe_write_guard = match &maybe_shared_chain_view {
            Some(shared_chain_view) => Some(shared_chain_view.write().await),
            None => None,
        };

        self.state.chain.save().await?;
        self.succeeded = true;
        Ok(())
    }
}

impl<StorageClient> Drop for ChainWorkerStateWithAttemptedChanges<'_, StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
    ViewError: From<StorageClient::ContextError>,
{
    fn drop(&mut self) {
        if !self.succeeded {
            self.state.chain.rollback();
        }
    }
}

/// Helper type for handling cross-chain updates.
pub(crate) struct CrossChainUpdateHelper<'a> {
    pub allow_messages_from_deprecated_epochs: bool,
    pub current_epoch: Option<Epoch>,
    pub committees: &'a BTreeMap<Epoch, Committee>,
}

impl<'a> CrossChainUpdateHelper<'a> {
    /// Creates a new [`CrossChainUpdateHelper`].
    pub fn new<C>(config: &ChainWorkerConfig, chain: &'a ChainStateView<C>) -> Self
    where
        C: Context + Clone + Send + Sync + 'static,
        ViewError: From<C::Error>,
    {
        CrossChainUpdateHelper {
            allow_messages_from_deprecated_epochs: config.allow_messages_from_deprecated_epochs,
            current_epoch: *chain.execution_state.system.epoch.get(),
            committees: chain.execution_state.system.committees.get(),
        }
    }

    /// Checks basic invariants and deals with repeated heights and deprecated epochs.
    /// * Returns a range of message bundles that are both new to us and not relying on
    /// an untrusted set of validators.
    /// * In the case of validators, if the epoch(s) of the highest bundles are not
    /// trusted, we only accept bundles that contain messages that were already
    /// executed by anticipation (i.e. received in certified blocks).
    /// * Basic invariants are checked for good measure. We still crucially trust
    /// the worker of the sending chain to have verified and executed the blocks
    /// correctly.
    pub fn select_message_bundles(
        &self,
        origin: &'a Origin,
        recipient: ChainId,
        next_height_to_receive: BlockHeight,
        last_anticipated_block_height: Option<BlockHeight>,
        mut bundles: Vec<MessageBundle>,
    ) -> Result<Vec<MessageBundle>, WorkerError> {
        let mut latest_height = None;
        let mut skipped_len = 0;
        let mut trusted_len = 0;
        for (i, bundle) in bundles.iter().enumerate() {
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
                || Some(bundle.epoch) >= self.current_epoch
                || self.committees.contains_key(&bundle.epoch)
            {
                trusted_len = i + 1;
            }
        }
        if skipped_len > 0 {
            let sample_bundle = &bundles[skipped_len - 1];
            debug!(
                "Ignoring repeated messages to {recipient:?} from {origin:?} at height {}",
                sample_bundle.height,
            );
        }
        if skipped_len < bundles.len() && trusted_len < bundles.len() {
            let sample_bundle = &bundles[trusted_len];
            warn!(
                "Refusing messages to {recipient:?} from {origin:?} at height {} \
                 because the epoch {:?} is not trusted any more",
                sample_bundle.height, sample_bundle.epoch,
            );
        }
        let certificates = if skipped_len < trusted_len {
            bundles.drain(skipped_len..trusted_len).collect()
        } else {
            vec![]
        };
        Ok(certificates)
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

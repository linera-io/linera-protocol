// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::data_types::{ChainInfo, ChainInfoQuery, ChainInfoResponse, CrossChainRequest};
use async_trait::async_trait;
use linera_base::{
    committee::Committee,
    crypto::{CryptoHash, KeyPair},
    data_types::{ArithmeticError, BlockHeight, ChainId, Epoch, Owner, Timestamp},
    ensure,
};
use linera_chain::{
    data_types::{
        Block, BlockProposal, Certificate, HashedValue, LiteCertificate, Medium, Message, Origin,
        OutgoingEffect, Target, ValueKind,
    },
    ChainManagerOutcome, ChainStateView,
};
use linera_execution::{
    ApplicationId, Query, Response, UserApplicationDescription, UserApplicationId,
};
use linera_storage::Store;
use linera_views::{
    log_view::LogView,
    views::{RootView, View, ViewError},
};
use lru::LruCache;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
    iter,
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};
use thiserror::Error;

#[cfg(test)]
#[path = "unit_tests/worker_tests.rs"]
mod worker_tests;

/// Interface provided by each physical shard (aka "worker") of a validator or a local node.
/// * All commands return either the current chain info or an error.
/// * Repeating commands produces no changes and returns no error.
/// * Some handlers may return cross-chain requests, that is, messages
///   to be communicated to other workers of the same validator.
#[async_trait]
pub trait ValidatorWorker {
    /// Propose a new block. In case of success, the chain info contains a vote on the new
    /// block.
    async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, WorkerError>;

    /// Process a certificate, e.g. to extend a chain with a confirmed block.
    async fn handle_lite_certificate(
        &mut self,
        certificate: LiteCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError>;

    /// Process a certificate, e.g. to extend a chain with a confirmed block.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
        blobs: Vec<HashedValue>,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError>;

    /// Handle information queries on chains.
    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError>;

    /// Handle a (trusted!) cross-chain request.
    async fn handle_cross_chain_request(
        &mut self,
        request: CrossChainRequest,
    ) -> Result<NetworkActions, WorkerError>;
}

/// Instruct the networking layer to send cross-chain requests and/or push notifications.
#[derive(Default, Debug)]
pub struct NetworkActions {
    /// The cross-chain requests
    pub cross_chain_requests: Vec<CrossChainRequest>,
    /// The push notifications.
    pub notifications: Vec<Notification>,
}

impl NetworkActions {
    fn merge(&mut self, other: NetworkActions) {
        self.cross_chain_requests.extend(other.cross_chain_requests);
        self.notifications.extend(other.notifications);
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Notify that a chain has a new certified block or a new message.
pub struct Notification {
    pub chain_id: ChainId,
    pub reason: Reason,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
/// Reason for the notification.
pub enum Reason {
    NewBlock {
        height: BlockHeight,
    },
    NewMessage {
        application_id: ApplicationId,
        origin: Origin,
        height: BlockHeight,
    },
}

/// Error type for [`ValidatorWorker`].
#[derive(Debug, Error)]
pub enum WorkerError {
    #[error(transparent)]
    CryptoError(#[from] linera_base::crypto::CryptoError),
    #[error(transparent)]
    ArithmeticError(#[from] ArithmeticError),
    #[error(transparent)]
    ViewError(#[from] linera_views::views::ViewError),
    #[error(transparent)]
    ChainError(#[from] Box<linera_chain::ChainError>),

    // Chain access control
    #[error("Block was not signed by an authorized owner")]
    InvalidOwner,

    #[error("Operations in the block are not authenticated by the proper signer")]
    InvalidSigner(Owner),

    // Chaining
    #[error(
        "Was expecting block height {expected_block_height} but found {found_block_height} instead"
    )]
    UnexpectedBlockHeight {
        expected_block_height: BlockHeight,
        found_block_height: BlockHeight,
    },
    #[error("Cannot confirm a block before its predecessors: {current_block_height:?}")]
    MissingEarlierBlocks { current_block_height: BlockHeight },
    #[error("{epoch:?} is not recognized by chain {chain_id:}")]
    InvalidEpoch { chain_id: ChainId, epoch: Epoch },

    // Other server-side errors
    #[error("Invalid cross-chain request")]
    InvalidCrossChainRequest,
    #[error("The block does contain the hash that we expected for the previous block")]
    InvalidBlockChaining,
    #[error("The given state hash is not what we computed after executing the block")]
    IncorrectStateHash,
    #[error("The given effects are not what we computed after executing the block")]
    IncorrectEffects,
    #[error("The timestamp of a Tick operation is in the future.")]
    InvalidTimestamp,
    #[error("We don't have the value for the certificate.")]
    MissingCertificateValue,
    #[error("The hash certificate doesn't match its value.")]
    InvalidLiteCertificate,
    #[error("An additional value was provided that is not required: {value_hash}.")]
    UnneededValue { value_hash: CryptoHash },
}

impl From<linera_chain::ChainError> for WorkerError {
    fn from(chain_error: linera_chain::ChainError) -> Self {
        WorkerError::ChainError(Box::new(chain_error))
    }
}

const DEFAULT_VALUE_CACHE_SIZE: usize = 1000;

/// State of a worker in a validator or a local node.
pub struct WorkerState<StorageClient> {
    /// A name used for logging
    nickname: String,
    /// The signature key pair of the validator. The key may be missing for replicas
    /// without voting rights (possibly with a partial view of chains).
    key_pair: Option<Arc<KeyPair>>,
    /// Access to local persistent storage.
    storage: StorageClient,
    /// Whether inactive chains are allowed in storage.
    allow_inactive_chains: bool,
    /// Whether new messages from deprecated epochs are allowed.
    allow_messages_from_deprecated_epochs: bool,
    /// Blocks with a timestamp this far in the future will still be accepted, but the validator
    /// will wait until that timestamp before voting.
    grace_period_micros: u64,
    /// Cached values by hash.
    recent_values: LruCache<CryptoHash, HashedValue>,
}

impl<Client: Clone> Clone for WorkerState<Client> {
    fn clone(&self) -> Self {
        let mut recent_values = LruCache::new(self.recent_values.cap());
        for (k, v) in &self.recent_values {
            recent_values.push(*k, v.clone());
        }
        WorkerState {
            nickname: self.nickname.clone(),
            key_pair: self.key_pair.clone(),
            storage: self.storage.clone(),
            allow_inactive_chains: self.allow_inactive_chains,
            allow_messages_from_deprecated_epochs: self.allow_messages_from_deprecated_epochs,
            grace_period_micros: self.grace_period_micros,
            recent_values,
        }
    }
}

impl<Client> WorkerState<Client> {
    pub fn new(nickname: String, key_pair: Option<KeyPair>, storage: Client) -> Self {
        WorkerState {
            nickname,
            key_pair: key_pair.map(Arc::new),
            storage,
            allow_inactive_chains: false,
            allow_messages_from_deprecated_epochs: false,
            grace_period_micros: 0,
            recent_values: LruCache::new(NonZeroUsize::try_from(DEFAULT_VALUE_CACHE_SIZE).unwrap()),
        }
    }

    pub fn with_allow_inactive_chains(mut self, value: bool) -> Self {
        self.allow_inactive_chains = value;
        self
    }

    pub fn with_allow_messages_from_deprecated_epochs(mut self, value: bool) -> Self {
        self.allow_messages_from_deprecated_epochs = value;
        self
    }

    pub fn with_value_cache_size(mut self, size: NonZeroUsize) -> Self {
        self.recent_values.resize(size);
        self
    }

    /// Returns an instance with the specified grace period, in microseconds.
    ///
    /// Blocks with a timestamp this far in the future will still be accepted, but the validator
    /// will wait until that timestamp before voting.
    pub fn with_grace_period_micros(mut self, grace_period_micros: u64) -> Self {
        self.grace_period_micros = grace_period_micros;
        self
    }

    pub fn nickname(&self) -> &str {
        &self.nickname
    }

    pub(crate) fn storage_client(&self) -> &Client {
        &self.storage
    }

    pub(crate) fn recent_value(&mut self, hash: &CryptoHash) -> Option<&HashedValue> {
        self.recent_values.get(hash)
    }
}

impl<Client> WorkerState<Client>
where
    Client: Store + Clone + Send + Sync + 'static,
    ViewError: From<Client::ContextError>,
{
    // NOTE: This only works for non-sharded workers!
    #[cfg(test)]
    pub(crate) async fn fully_handle_certificate(
        &mut self,
        certificate: Certificate,
        blobs: Vec<HashedValue>,
    ) -> Result<ChainInfoResponse, WorkerError> {
        self.fully_handle_certificate_with_notifications(certificate, blobs, None)
            .await
    }

    #[inline]
    pub(crate) async fn fully_handle_certificate_with_notifications(
        &mut self,
        certificate: Certificate,
        blobs: Vec<HashedValue>,
        mut notifications: Option<&mut Vec<Notification>>,
    ) -> Result<ChainInfoResponse, WorkerError> {
        let (response, actions) = self.handle_certificate(certificate, blobs).await?;
        let mut requests = VecDeque::from(actions.cross_chain_requests);
        while let Some(request) = requests.pop_front() {
            let actions = self.handle_cross_chain_request(request).await?;
            requests.extend(actions.cross_chain_requests);
            if let Some(notifications) = notifications.as_mut() {
                notifications.extend(actions.notifications);
            }
        }
        Ok(response)
    }

    /// Try to execute a block proposal without any verification other than block execution.
    pub async fn stage_block_execution(
        &mut self,
        block: &Block,
    ) -> Result<(Vec<OutgoingEffect>, ChainInfoResponse), WorkerError> {
        let mut chain = self.storage.load_active_chain(block.chain_id).await?;
        let effects = chain.execute_block(block).await?;
        let info = ChainInfoResponse::new(&chain, None);
        // Do not save the new state.
        Ok((effects, info))
    }

    pub(crate) async fn query_application(
        &mut self,
        chain_id: ChainId,
        application_id: ApplicationId,
        query: &Query,
    ) -> Result<Response, WorkerError> {
        let mut chain = self.storage.load_active_chain(chain_id).await?;
        let response = chain.query_application(application_id, query).await?;
        Ok(response)
    }

    pub(crate) async fn describe_application(
        &mut self,
        chain_id: ChainId,
        application_id: UserApplicationId,
    ) -> Result<UserApplicationDescription, WorkerError> {
        let mut chain = self.storage.load_active_chain(chain_id).await?;
        let response = chain.describe_application(application_id).await?;
        Ok(response)
    }

    /// Get a reference to the [`KeyPair`], if available.
    fn key_pair(&self) -> Option<&KeyPair> {
        self.key_pair.as_ref().map(Arc::as_ref)
    }

    async fn create_cross_chain_request(
        &mut self,
        confirmed_log: &mut LogView<Client::Context, CryptoHash>,
        height_map: Vec<(ApplicationId, Medium, Vec<BlockHeight>)>,
        sender: ChainId,
        recipient: ChainId,
    ) -> Result<CrossChainRequest, WorkerError> {
        let heights = BTreeSet::from_iter(
            height_map
                .iter()
                .flat_map(|(_, _, heights)| heights)
                .copied(),
        );
        let mut keys = Vec::new();
        for height in heights {
            if let Some(key) = confirmed_log.get(usize::from(height)).await? {
                keys.push(key);
            }
        }
        let certificates = self.storage.read_certificates(keys).await?;
        Ok(CrossChainRequest::UpdateRecipient {
            height_map,
            sender,
            recipient,
            certificates,
        })
    }

    /// Load pending cross-chain requests.
    async fn create_network_actions(
        &mut self,
        chain: &mut ChainStateView<Client::Context>,
    ) -> Result<NetworkActions, WorkerError> {
        let mut heights_by_recipient: BTreeMap<_, BTreeMap<_, _>> = Default::default();
        for application_id in chain.communication_states.indices().await? {
            let state = chain
                .communication_states
                .load_entry_mut(&application_id)
                .await?;
            for target in state.outboxes.indices().await? {
                let outbox = state.outboxes.load_entry_mut(&target).await?;
                let heights = outbox.block_heights().await?;
                heights_by_recipient
                    .entry(target.recipient)
                    .or_default()
                    .insert((application_id, target.medium), heights);
            }
        }
        let mut actions = NetworkActions::default();
        let chain_id = chain.chain_id();
        for (recipient, height_map) in heights_by_recipient {
            let request = self
                .create_cross_chain_request(
                    &mut chain.confirmed_log,
                    height_map
                        .into_iter()
                        .map(|((app_id, medium), heights)| (app_id, medium, heights))
                        .collect(),
                    chain_id,
                    recipient,
                )
                .await?;
            actions.cross_chain_requests.push(request);
        }
        Ok(actions)
    }

    /// Process a confirmed block (aka a commit).
    async fn process_confirmed_block(
        &mut self,
        certificate: Certificate,
        blobs: &[HashedValue],
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        assert!(
            certificate.value.is_confirmed(),
            "Expecting a confirmation certificate"
        );
        let block = certificate.value.block();
        let mut chain = self.storage.load_active_chain(block.chain_id).await?;
        // Find all certificates containing bytecode used when executing this block.
        let blob_hashes: HashSet<_> = block
            .required_bytecode(&chain.execution_state.system.registry)
            .await
            .map_err(|err| WorkerError::ChainError(Box::new(err.into())))?
            .into_values()
            .map(|bytecode_location| bytecode_location.certificate_hash)
            .collect();
        for value in blobs {
            let value_hash = value.hash();
            ensure!(
                blob_hashes.contains(&value_hash),
                WorkerError::UnneededValue { value_hash }
            );
        }
        // Write the certificates so that the bytecode is available during execution.
        for value in blobs {
            self.storage.write_value(value.clone()).await?;
        }
        // Check that the chain is active and ready for this confirmation.
        let tip = chain.tip_state.get();
        if tip.next_block_height < block.height {
            return Err(WorkerError::MissingEarlierBlocks {
                current_block_height: tip.next_block_height,
            });
        }
        if tip.next_block_height > block.height {
            // Block was already confirmed.
            let info = ChainInfoResponse::new(&chain, self.key_pair());
            let actions = self.create_network_actions(&mut chain).await?;
            return Ok((info, actions));
        }
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        let epoch = chain
            .execution_state
            .system
            .epoch
            .get()
            .expect("chain is active");
        ensure!(
            block.epoch == epoch,
            WorkerError::InvalidEpoch {
                chain_id: block.chain_id,
                epoch: block.epoch,
            }
        );
        let committee = chain
            .execution_state
            .system
            .committees
            .get()
            .get(&epoch)
            .expect("chain is active");
        certificate.check(committee)?;
        // This should always be true for valid certificates.
        ensure!(
            tip.block_hash == block.previous_block_hash,
            WorkerError::InvalidBlockChaining
        );
        // Persist certificate.
        self.storage.write_certificate(certificate.clone()).await?;
        // Execute the block.
        let verified_effects = chain.execute_block(block).await?;
        ensure!(
            *certificate.value.effects() == verified_effects,
            WorkerError::IncorrectEffects
        );
        // Advance to next block height.
        let tip = chain.tip_state.get_mut();
        tip.block_hash = Some(certificate.value.hash());
        tip.next_block_height.try_add_assign_one()?;
        chain.confirmed_log.push(certificate.value.hash());
        // We should always agree on the state hash.
        ensure!(
            *chain.execution_state_hash.get() == Some(certificate.value.state_hash()),
            WorkerError::IncorrectStateHash
        );
        let info = ChainInfoResponse::new(&chain, self.key_pair());
        let mut actions = self.create_network_actions(&mut chain).await?;
        actions.notifications.push(Notification {
            chain_id: block.chain_id,
            reason: Reason::NewBlock {
                height: block.height,
            },
        });
        // Persist chain.
        chain.save().await?;
        Ok((info, actions))
    }

    /// Process a validated block issued from a multi-owner chain.
    async fn process_validated_block(
        &mut self,
        certificate: Certificate,
    ) -> Result<ChainInfoResponse, WorkerError> {
        let round = match certificate.value.kind() {
            ValueKind::ValidatedBlock { round } => round,
            ValueKind::ConfirmedBlock => panic!("Expecting a validation certificate"),
        };
        let block = certificate.value.block();
        // Check that the chain is active and ready for this confirmation.
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        let mut chain = self.storage.load_active_chain(block.chain_id).await?;
        let epoch = chain
            .execution_state
            .system
            .epoch
            .get()
            .expect("chain is active");
        ensure!(
            block.epoch == epoch,
            WorkerError::InvalidEpoch {
                chain_id: block.chain_id,
                epoch: block.epoch,
            }
        );
        let committee = chain
            .execution_state
            .system
            .committees
            .get()
            .get(&epoch)
            .expect("chain is active");
        certificate.check(committee)?;
        if chain.manager.get_mut().check_validated_block(
            chain.tip_state.get().next_block_height,
            block,
            round,
        )? == ChainManagerOutcome::Skip
        {
            // If we just processed the same pending block, return the chain info
            // unchanged.
            return Ok(ChainInfoResponse::new(&chain, self.key_pair()));
        }
        chain
            .manager
            .get_mut()
            .create_final_vote(certificate, self.key_pair());
        let info = ChainInfoResponse::new(&chain, self.key_pair());
        chain.save().await?;
        Ok(info)
    }

    async fn process_cross_chain_update(
        &mut self,
        application_id: ApplicationId,
        origin: Origin,
        recipient: ChainId,
        certificates: Vec<Certificate>,
    ) -> Result<NetworkActions, WorkerError> {
        let mut chain = self.storage.load_chain(recipient).await?;
        // Only process certificates with relevant heights and epochs.
        let next_height_to_receive = chain
            .next_block_height_to_receive(application_id, origin.clone())
            .await?;
        let last_anticipated_block_height = chain
            .last_anticipated_block_height(application_id, origin.clone())
            .await?;
        let helper = CrossChainUpdateHelper {
            nickname: &self.nickname,
            allow_messages_from_deprecated_epochs: self.allow_messages_from_deprecated_epochs,
            current_epoch: *chain.execution_state.system.epoch.get(),
            committees: chain.execution_state.system.committees.get(),
        };
        let certificates = helper.select_certificates(
            application_id,
            &origin,
            recipient,
            next_height_to_receive,
            last_anticipated_block_height,
            certificates,
        )?;
        // Process the received messages in certificates.
        let mut last_updated_height = None;
        for certificate in certificates {
            let block = certificate.value.block();
            // Update the staged chain state with the received block.
            chain
                .receive_block(
                    application_id,
                    &origin,
                    block.height,
                    block.timestamp,
                    certificate.value.effects().clone(),
                    certificate.value.hash(),
                )
                .await?;
            last_updated_height = Some(block.height);
            self.storage.write_certificate(certificate).await?;
        }
        // Be ready to confirm the highest processed block so far. It could be from a previous update.
        let cross_chain_requests =
            match last_updated_height.or_else(|| next_height_to_receive.try_sub_one().ok()) {
                Some(height) => vec![CrossChainRequest::ConfirmUpdatedRecipient {
                    application_id,
                    origin: origin.clone(),
                    recipient,
                    height,
                }],
                None => vec![],
            };
        // If needed, save the state and return network actions.
        if let Some(height) = last_updated_height {
            if !self.allow_inactive_chains && !chain.is_active() {
                // Refuse to create a chain state if the chain is still inactive by
                // now. Accordingly, do not send a confirmation, so that the
                // cross-chain update is retried later.
                log::warn!(
                    "[{}] Refusing to deliver messages to {recipient:?} from {application_id:?}::{origin:?} \
                     at height {height} because the recipient is still inactive",
                    self.nickname
                );
                return Ok(NetworkActions::default());
            }
            // Save the chain.
            chain.save().await?;
            // Notify subscribers and send a confirmation.
            Ok(NetworkActions {
                cross_chain_requests,
                notifications: vec![Notification {
                    chain_id: recipient,
                    reason: Reason::NewMessage {
                        application_id,
                        origin,
                        height,
                    },
                }],
            })
        } else {
            // Send a confirmation but do not notify subscribers.
            Ok(NetworkActions {
                cross_chain_requests,
                notifications: vec![],
            })
        }
    }

    fn cache_recent_value(&mut self, hash: CryptoHash, value: HashedValue) {
        if value.is_validated() {
            // Cache the corresponding confirmed block, too, in case we get a certificate.
            let conf_value = value.clone().into_confirmed();
            let conf_hash = conf_value.hash();
            self.recent_values.push(conf_hash, conf_value);
        }
        // Cache the certificate so that clients don't have to send the value again.
        self.recent_values.push(hash, value);
    }
}

#[async_trait]
impl<Client> ValidatorWorker for WorkerState<Client>
where
    Client: Store + Clone + Send + Sync + 'static,
    ViewError: From<Client::ContextError>,
{
    async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, WorkerError> {
        log::trace!("{} <-- {:?}", self.nickname, proposal);
        let chain_id = proposal.content.block.chain_id;
        let mut chain = self.storage.load_active_chain(chain_id).await?;
        // Check the epoch.
        let epoch = chain
            .execution_state
            .system
            .epoch
            .get()
            .expect("chain is active");
        ensure!(
            proposal.content.block.epoch == epoch,
            WorkerError::InvalidEpoch { chain_id, epoch }
        );
        // Check the authentication of the block.
        ensure!(
            chain.manager.get().has_owner(&proposal.owner),
            WorkerError::InvalidOwner
        );
        proposal
            .signature
            .check(&proposal.content, proposal.owner.0)?;
        // Check the authentication of the operations in the block.
        if let Some(signer) = proposal.content.block.authenticated_signer {
            ensure!(signer == proposal.owner, WorkerError::InvalidSigner(signer));
        }
        // Check if the chain is ready for this new block proposal.
        // This should always pass for nodes without voting key.
        if chain.manager.get().check_proposed_block(
            chain.tip_state.get().block_hash,
            chain.tip_state.get().next_block_height,
            &proposal.content.block,
            proposal.content.round,
        )? == ChainManagerOutcome::Skip
        {
            // If we just processed the same pending block, return the chain info
            // unchanged.
            return Ok(ChainInfoResponse::new(&chain, self.key_pair()));
        }
        let time_till_block = proposal
            .content
            .block
            .timestamp
            .saturating_diff_micros(Timestamp::now());
        ensure!(
            time_till_block <= self.grace_period_micros,
            WorkerError::InvalidTimestamp
        );
        if time_till_block > 0 {
            tokio::time::sleep(Duration::from_micros(time_till_block)).await;
        }
        let (effects, state_hash) = {
            let effects = chain.execute_block(&proposal.content.block).await?;
            let hash = chain.execution_state_hash.get().expect("was just computed");
            // Verify that the resulting chain would have no unconfirmed incoming
            // messages.
            chain.validate_incoming_messages().await?;
            // Reset all the staged changes as we were only validating things.
            chain.rollback();
            (effects, hash)
        };
        // Create the vote and store it in the chain state.
        let manager = chain.manager.get_mut();
        manager.create_vote(proposal, effects, state_hash, self.key_pair());
        // Cache the value we voted on, so the client doesn't have to send it again.
        if let Some(vote) = manager.pending() {
            self.cache_recent_value(vote.value.lite().value_hash, vote.value.clone());
        }
        let info = ChainInfoResponse::new(&chain, self.key_pair());
        chain.save().await?;
        Ok(info)
    }

    /// Process a certificate, e.g. to extend a chain with a confirmed block.
    async fn handle_lite_certificate(
        &mut self,
        certificate: LiteCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        let value = self
            .recent_values
            .get(&certificate.value.value_hash)
            .ok_or(WorkerError::MissingCertificateValue)?
            .clone();
        let full_cert = certificate
            .with_value(value)
            .ok_or(WorkerError::InvalidLiteCertificate)?;
        self.handle_certificate(full_cert, vec![]).await
    }

    /// Process a certificate.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
        blobs: Vec<HashedValue>,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        log::trace!("{} <-- {:?}", self.nickname, certificate);
        ensure!(
            certificate.value.is_confirmed() || blobs.is_empty(),
            WorkerError::UnneededValue {
                value_hash: blobs[0].hash(),
            }
        );
        let values_to_cache: Vec<_> = blobs
            .iter()
            .chain(iter::once(&certificate.value))
            .filter(|value| !self.recent_values.contains(&value.hash()))
            .cloned()
            .collect();
        let (info, actions) = match certificate.value.kind() {
            ValueKind::ValidatedBlock { .. } => {
                // Confirm the validated block.
                let info = self.process_validated_block(certificate).await?;
                (info, NetworkActions::default())
            }
            ValueKind::ConfirmedBlock => {
                // Execute the confirmed block.
                self.process_confirmed_block(certificate, &blobs).await?
            }
        };
        for value in values_to_cache {
            self.cache_recent_value(value.hash(), value);
        }
        Ok((info, actions))
    }

    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        log::trace!("{} <-- {:?}", self.nickname, query);
        let mut chain = self.storage.load_chain(query.chain_id).await?;
        let mut info = ChainInfo::from(&chain);
        if query.request_committees {
            info.requested_committees = Some(chain.execution_state.system.committees.get().clone());
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
            for application_id in chain.communication_states.indices().await? {
                let state = chain
                    .communication_states
                    .load_entry_mut(&application_id)
                    .await?;
                for origin in state.inboxes.indices().await? {
                    let inbox = state.inboxes.load_entry_mut(&origin).await?;
                    let count = inbox.added_events.count();
                    for event in inbox.added_events.read_front(count).await? {
                        messages.push(Message {
                            application_id,
                            origin: origin.clone(),
                            event: event.clone(),
                        });
                    }
                }
            }
            info.requested_pending_messages = messages;
        }
        if let Some(range) = query.request_sent_certificates_in_range {
            let start = range.start.into();
            let end = match range.limit {
                None => chain.confirmed_log.count(),
                Some(limit) => std::cmp::min(start + limit, chain.confirmed_log.count()),
            };
            let keys = chain.confirmed_log.read(start..end).await?;
            let certs = self.storage.read_certificates(keys).await?;
            info.requested_sent_certificates = certs;
        }
        if let Some(start) = query.request_received_certificates_excluding_first_nth {
            let end = chain.received_log.count();
            let keys = chain.received_log.read(start..end).await?;
            let certs = self.storage.read_certificates(keys).await?;
            info.requested_received_certificates = certs;
        }
        if let Some(hash) = query.request_blob {
            info.requested_blob = Some(self.storage.read_value(hash).await?);
        }
        if query.request_manager_values {
            info.manager.add_values(chain.manager.get());
        }
        let response = ChainInfoResponse::new(info, self.key_pair());
        log::trace!("{} --> {:?}", self.nickname, response);
        // Trigger any outgoing cross-chain messages that haven't been confirmed yet.
        let actions = self.create_network_actions(&mut chain).await?;
        Ok((response, actions))
    }

    async fn handle_cross_chain_request(
        &mut self,
        request: CrossChainRequest,
    ) -> Result<NetworkActions, WorkerError> {
        log::trace!("{} <-- {:?}", self.nickname, request);
        match request {
            CrossChainRequest::UpdateRecipient {
                height_map,
                sender,
                recipient,
                certificates,
            } => {
                let mut actions = NetworkActions::default();
                for (application_id, medium, heights) in height_map {
                    let origin = Origin { sender, medium };
                    let app_certificates = certificates
                        .iter()
                        .filter(|cert| heights.binary_search(&cert.value.block().height).is_ok())
                        .cloned()
                        .collect();
                    actions.merge(
                        self.process_cross_chain_update(
                            application_id,
                            origin.clone(),
                            recipient,
                            app_certificates,
                        )
                        .await?,
                    );
                }
                Ok(actions)
            }
            CrossChainRequest::ConfirmUpdatedRecipient {
                application_id,
                origin: Origin { sender, medium },
                recipient,
                height,
            } => {
                let mut chain = self.storage.load_chain(sender).await?;
                let target = Target { recipient, medium };
                if chain
                    .mark_messages_as_received(application_id, target, height)
                    .await?
                {
                    chain.save().await?;
                }
                Ok(NetworkActions::default())
            }
        }
    }
}

struct CrossChainUpdateHelper<'a> {
    nickname: &'a str,
    allow_messages_from_deprecated_epochs: bool,
    current_epoch: Option<Epoch>,
    committees: &'a BTreeMap<Epoch, Committee>,
}

impl<'a> CrossChainUpdateHelper<'a> {
    /// Check basic invariants and deal with repeated heights and deprecated epochs.
    /// * Return a range of certificates that are both new to us and not relying on an
    /// untrusted set of validators.
    /// * In the case of validators, if the epoch(s) of the highest certificates are not
    /// trusted, we only accept certificates that contain effects that were already
    /// executed by anticipation (i.e. received in certified blocks).
    /// * Basic invariants are checked for good measure. We still crucially trust
    /// the worker of the sending chain to have verified and executed the blocks
    /// correctly.
    fn select_certificates(
        &self,
        application_id: ApplicationId,
        origin: &'a Origin,
        recipient: ChainId,
        next_height_to_receive: BlockHeight,
        last_anticipated_block_height: Option<BlockHeight>,
        mut certificates: Vec<Certificate>,
    ) -> Result<Vec<Certificate>, WorkerError> {
        let mut latest_height = None;
        let mut skipped_len = 0;
        let mut trusted_len = 0;
        for (i, certificate) in certificates.iter().enumerate() {
            // Certificates are confirming blocks.
            if !certificate.value.is_confirmed() {
                return Err(WorkerError::InvalidCrossChainRequest);
            };
            let block = certificate.value.block();
            // Make sure that the chain_id is correct.
            ensure!(
                origin.sender == block.chain_id,
                WorkerError::InvalidCrossChainRequest
            );
            // Make sure that heights are increasing.
            ensure!(
                latest_height < Some(block.height),
                WorkerError::InvalidCrossChainRequest
            );
            latest_height = Some(block.height);
            // Check if the block has been received already.
            if block.height < next_height_to_receive {
                skipped_len = i + 1;
            }
            // Check if the height is trusted or the epoch is trusted.
            if self.allow_messages_from_deprecated_epochs
                || Some(block.height) <= last_anticipated_block_height
                || Some(block.epoch) >= self.current_epoch
                || self.committees.contains_key(&block.epoch)
            {
                trusted_len = i + 1;
            }
        }
        if skipped_len > 0 {
            let sample_block = certificates[skipped_len - 1].value.block();
            log::warn!(
                "[{}] Ignoring repeated messages to {recipient:?} from {application_id:?}::{origin:?} at height {}",
                self.nickname,
                sample_block.height,
            );
        }
        if skipped_len < certificates.len() && trusted_len < certificates.len() {
            let sample_block = certificates[trusted_len].value.block();
            log::warn!(
                "[{}] Refusing messages to {recipient:?} from {application_id:?}::{origin:?} at height {} \
                 because the epoch {:?} is not trusted any more",
                self.nickname,
                sample_block.height,
                sample_block.epoch,
            );
        }
        let certificates = if skipped_len < trusted_len {
            certificates.drain(skipped_len..trusted_len).collect()
        } else {
            vec![]
        };
        Ok(certificates)
    }
}

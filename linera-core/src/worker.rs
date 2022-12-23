// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::messages::{ChainInfo, ChainInfoQuery, ChainInfoResponse, CrossChainRequest};
use async_trait::async_trait;
use linera_base::{
    crypto::{HashValue, KeyPair},
    ensure,
    messages::{ArithmeticError, BlockHeight, ChainId, Epoch, Medium, Origin},
};
use linera_chain::{
    messages::{Block, BlockProposal, Certificate, MessageGroup, Value},
    ChainManagerOutcome, ChainStateView,
};
use linera_execution::ApplicationDescription;
use linera_storage::Store;
use linera_views::{
    log_view::LogView,
    views::{View, ViewError},
};
use std::{collections::VecDeque, sync::Arc};
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
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<(ChainInfoResponse, Vec<CrossChainRequest>), WorkerError>;

    /// Handle information queries on chains.
    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, WorkerError>;

    /// Handle a (trusted!) cross-chain request.
    async fn handle_cross_chain_request(
        &mut self,
        request: CrossChainRequest,
    ) -> Result<Vec<CrossChainRequest>, WorkerError>;
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
}

impl From<linera_chain::ChainError> for WorkerError {
    fn from(chain_error: linera_chain::ChainError) -> Self {
        WorkerError::ChainError(Box::new(chain_error))
    }
}

/// State of a worker in a validator or a local node.
#[derive(Clone)]
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
}

impl<Client> WorkerState<Client> {
    pub fn new(nickname: String, key_pair: Option<KeyPair>, storage: Client) -> Self {
        WorkerState {
            nickname,
            key_pair: key_pair.map(Arc::new),
            storage,
            allow_inactive_chains: false,
            allow_messages_from_deprecated_epochs: false,
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

    pub fn nickname(&self) -> &str {
        &self.nickname
    }

    pub(crate) fn storage_client(&self) -> &Client {
        &self.storage
    }
}

impl<Client> WorkerState<Client>
where
    Client: Store + Clone + Send + Sync + 'static,
    ViewError: From<Client::ContextError>,
{
    // NOTE: This only works for non-sharded workers!
    pub(crate) async fn fully_handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<ChainInfoResponse, WorkerError> {
        let (response, requests) = self.handle_certificate(certificate).await?;
        let mut requests = VecDeque::from(requests);
        while let Some(request) = requests.pop_front() {
            requests.extend(self.handle_cross_chain_request(request).await?);
        }
        Ok(response)
    }

    /// Try to execute a block proposal without any verification other than block execution.
    pub(crate) async fn stage_block_execution(
        &mut self,
        block: &Block,
    ) -> Result<ChainInfoResponse, WorkerError> {
        let mut chain = self.storage.load_active_chain(block.chain_id).await?;
        chain.execute_block(block).await?;
        let info = ChainInfoResponse::new(&chain, None);
        // Do not save the new state.
        Ok(info)
    }

    /// Get a reference to the [`KeyPair`], if available.
    fn key_pair(&self) -> Option<&KeyPair> {
        self.key_pair.as_ref().map(Arc::as_ref)
    }

    async fn make_cross_chain_request(
        &mut self,
        confirmed_log: &mut LogView<Client::Context, HashValue>,
        application: ApplicationDescription,
        origin: Origin,
        recipient: ChainId,
        heights: &[BlockHeight],
    ) -> Result<CrossChainRequest, WorkerError> {
        let mut keys = Vec::new();
        for height in heights {
            if let Some(key) = confirmed_log.get(usize::from(*height)).await? {
                keys.push(key);
            }
        }
        let certificates = self.storage.read_certificates(keys).await?;
        Ok(CrossChainRequest::UpdateRecipient {
            application,
            origin,
            recipient,
            certificates,
        })
    }

    /// Load pending cross-chain requests.
    async fn make_continuation(
        &mut self,
        chain: &mut ChainStateView<Client::Context>,
    ) -> Result<Vec<CrossChainRequest>, WorkerError> {
        let mut continuation = Vec::new();
        let chain_id = chain.chain_id();
        for application_id in chain.communication_states.indices().await? {
            let application = chain.describe_application(application_id).await?;
            let state = chain
                .communication_states
                .load_entry(application_id)
                .await?;
            for recipient in state.outboxes.indices().await? {
                let outbox = state.outboxes.load_entry(recipient).await?;
                let heights = outbox.block_heights().await?;
                let origin = Origin {
                    chain_id,
                    medium: Medium::Direct,
                };
                let request = self
                    .make_cross_chain_request(
                        &mut chain.confirmed_log,
                        application.clone(),
                        origin,
                        recipient,
                        &heights,
                    )
                    .await?;
                continuation.push(request);
            }
            for name in state.channels.indices().await? {
                let channel = state.channels.load_entry(name.clone()).await?;
                for recipient in channel.outboxes.indices().await? {
                    let outbox = channel.outboxes.load_entry(recipient).await?;
                    let heights = outbox.block_heights().await?;
                    let origin = Origin {
                        chain_id,
                        medium: Medium::Channel(name.clone()),
                    };
                    let request = self
                        .make_cross_chain_request(
                            &mut chain.confirmed_log,
                            application.clone(),
                            origin,
                            recipient,
                            &heights,
                        )
                        .await?;
                    continuation.push(request);
                }
            }
        }
        Ok(continuation)
    }

    /// Process a confirmed block (aka a commit).
    async fn process_confirmed_block(
        &mut self,
        certificate: Certificate,
    ) -> Result<(ChainInfoResponse, Vec<CrossChainRequest>), WorkerError> {
        let (block, effects, state_hash) = match &certificate.value {
            Value::ConfirmedBlock {
                block,
                effects,
                state_hash,
            } => (block, effects.clone(), *state_hash),
            _ => panic!("Expecting a confirmation certificate"),
        };
        // Obtain the sender's chain.
        let sender = block.chain_id;
        // Check that the chain is active and ready for this confirmation.
        let mut chain = self.storage.load_active_chain(sender).await?;
        let tip = chain.tip_state.get();
        if tip.next_block_height < block.height {
            return Err(WorkerError::MissingEarlierBlocks {
                current_block_height: tip.next_block_height,
            });
        }
        if tip.next_block_height > block.height {
            // Block was already confirmed.
            let info = ChainInfoResponse::new(&chain, self.key_pair());
            let continuation = self.make_continuation(&mut chain).await?;
            return Ok((info, continuation));
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
                chain_id: sender,
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
        ensure!(effects == verified_effects, WorkerError::IncorrectEffects);
        // Advance to next block height.
        let tip = chain.tip_state.get_mut();
        tip.block_hash = Some(certificate.hash);
        tip.next_block_height.try_add_assign_one()?;
        chain.confirmed_log.push(certificate.hash);
        // We should always agree on the state hash.
        ensure!(
            *chain.execution_state_hash.get() == Some(state_hash),
            WorkerError::IncorrectStateHash
        );
        let info = ChainInfoResponse::new(&chain, self.key_pair());
        let continuation = self.make_continuation(&mut chain).await?;
        // Persist chain.
        chain.save().await?;
        Ok((info, continuation))
    }

    /// Process a validated block issued from a multi-owner chain.
    async fn process_validated_block(
        &mut self,
        certificate: Certificate,
    ) -> Result<ChainInfoResponse, WorkerError> {
        let (block, round, effects, state_hash) = match &certificate.value {
            Value::ValidatedBlock {
                block,
                round,
                effects,
                state_hash,
            } => (block, *round, effects.clone(), *state_hash),
            _ => panic!("Expecting a validation certificate"),
        };
        // Check that the chain is active and ready for this confirmation.
        let mut chain = self.storage.load_active_chain(block.chain_id).await?;
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
        chain.manager.get_mut().create_final_vote(
            block.clone(),
            effects,
            state_hash,
            certificate,
            self.key_pair(),
        );
        let info = ChainInfoResponse::new(&chain, self.key_pair());
        chain.save().await?;
        Ok(info)
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
        // Obtain the sender's chain.
        let sender = proposal.content.block.chain_id;
        let mut chain = self.storage.load_active_chain(sender).await?;
        // Check the epoch.
        let epoch = chain
            .execution_state
            .system
            .epoch
            .get()
            .expect("chain is active");
        ensure!(
            proposal.content.block.epoch == epoch,
            WorkerError::InvalidEpoch {
                chain_id: sender,
                epoch,
            }
        );
        // Check authentication of the block.
        ensure!(
            chain.manager.get().has_owner(&proposal.owner),
            WorkerError::InvalidOwner
        );
        proposal
            .signature
            .check(&proposal.content, proposal.owner.0)?;
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
        chain
            .manager
            .get_mut()
            .create_vote(proposal, effects, state_hash, self.key_pair());
        let info = ChainInfoResponse::new(&chain, self.key_pair());
        chain.save().await?;
        Ok(info)
    }

    /// Process a certificate.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<(ChainInfoResponse, Vec<CrossChainRequest>), WorkerError> {
        log::trace!("{} <-- {:?}", self.nickname, certificate);
        match &certificate.value {
            Value::ValidatedBlock { .. } => {
                // Confirm the validated block.
                let info = self.process_validated_block(certificate).await?;
                Ok((info, Vec::new()))
            }
            Value::ConfirmedBlock { .. } => {
                // Execute the confirmed block.
                self.process_confirmed_block(certificate).await
            }
        }
    }

    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, WorkerError> {
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
            let mut message_groups = Vec::new();
            for application_id in chain.communication_states.indices().await? {
                let state = chain
                    .communication_states
                    .load_entry(application_id)
                    .await?;
                for origin in state.inboxes.indices().await? {
                    let inbox = state.inboxes.load_entry(origin.clone()).await?;
                    let mut effects = Vec::new();
                    let mut current_height = None;
                    let count = inbox.received_events.count();
                    for event in inbox.received_events.read_front(count).await? {
                        match current_height {
                            None => {
                                current_height = Some(event.height);
                            }
                            Some(height) if height != event.height => {
                                // If the height changed, flush the accumulated effects
                                // into a new group.
                                message_groups.push(MessageGroup {
                                    application_id,
                                    origin: origin.clone(),
                                    height,
                                    effects,
                                });
                                effects = Vec::new();
                                current_height = Some(event.height);
                            }
                            _ => {
                                // Otherwise, continue adding effects to the same group.
                            }
                        }
                        effects.push((event.index, event.effect.clone()));
                    }
                    if let Some(height) = current_height {
                        message_groups.push(MessageGroup {
                            application_id,
                            origin: origin.clone(),
                            height,
                            effects,
                        });
                    }
                }
            }
            info.requested_pending_messages = message_groups;
        }
        if let Some(range) = query.request_sent_certificates_in_range {
            let start = range.start.into();
            let end = match range.limit {
                None => chain.confirmed_log.count(),
                Some(limit) => std::cmp::max(start + limit, chain.confirmed_log.count()),
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
        let response = ChainInfoResponse::new(info, self.key_pair());
        log::trace!("{} --> {:?}", self.nickname, response);
        Ok(response)
    }

    async fn handle_cross_chain_request(
        &mut self,
        request: CrossChainRequest,
    ) -> Result<Vec<CrossChainRequest>, WorkerError> {
        log::trace!("{} <-- {:?}", self.nickname, request);
        match request {
            CrossChainRequest::UpdateRecipient {
                application,
                origin,
                recipient,
                certificates,
            } => {
                let mut chain = self.storage.load_chain(recipient).await?;
                let application_id = chain.register_application(application);
                let last_authorized_block_height_for_epoch = {
                    // Start by checking a few invariants and deal with untrusted epochs.
                    // * Note that we still crucially trust the worker of the sending
                    // chain to have verified and executed the blocks correctly.
                    // * If the last epoch is not trusted (validators only), we can only
                    // accept certificates that contain effect that were already executed
                    // by anticipation.
                    let mut last_epoch = None;
                    let mut last_height = None;
                    for certificate in &certificates {
                        match &certificate.value {
                            Value::ConfirmedBlock { block, .. } => {
                                ensure!(
                                    origin.chain_id == block.chain_id,
                                    WorkerError::InvalidCrossChainRequest
                                );
                                ensure!(
                                    last_height < Some(block.height),
                                    WorkerError::InvalidCrossChainRequest
                                );
                                ensure!(
                                    last_epoch <= Some(block.epoch),
                                    WorkerError::InvalidCrossChainRequest
                                );
                                last_epoch = Some(block.epoch);
                                last_height = Some(block.height);
                            }
                            _ => {
                                return Err(WorkerError::InvalidCrossChainRequest);
                            }
                        }
                    }
                    match (last_epoch, last_height) {
                        (Some(epoch), Some(height))
                            if !self.allow_messages_from_deprecated_epochs
                                && Some(epoch) < *chain.execution_state.system.epoch.get()
                                && !chain
                                    .execution_state
                                    .system
                                    .committees
                                    .get()
                                    .contains_key(&epoch) =>
                        {
                            // Refuse to persist the chain state if the latest epoch in
                            // the received blocks from this recipient is not recognized
                            // any more by the receiving chain. (Future epochs are ok.)
                            match chain
                                .last_anticipated_block_height(application_id, origin.clone())
                                .await?
                            {
                                None => {
                                    // Early return because the entire batch of certificates is untrusted.
                                    // Make sure to display the appropriate warning.
                                    let next_height_to_receive = chain
                                        .next_block_height_to_receive(
                                            application_id,
                                            origin.clone(),
                                        )
                                        .await?;
                                    if height >= next_height_to_receive {
                                        log::warn!(
                                            "[{}] Refusing updates to {recipient:?} from untrusted epoch {epoch:?} at {application_id:?}::{origin:?}",
                                            self.nickname,
                                        );
                                    } else {
                                        log::warn!(
                                            "[{}] Ignoring repeated messages to {:?} from {:?}::{:?} at height {}",
                                            self.nickname,
                                            recipient,
                                            application_id,
                                            origin,
                                            height
                                        );
                                    }
                                    return Ok(Vec::new());
                                }
                                Some(last_anticipated_block_height) => {
                                    // Can only process block up to this value.
                                    Some((last_anticipated_block_height, epoch))
                                }
                            }
                        }
                        _ => {
                            // No restriction
                            None
                        }
                    }
                };
                let mut last_updated_height = None;
                for certificate in certificates {
                    let (block, effects) = match &certificate.value {
                        Value::ConfirmedBlock { block, effects, .. } => {
                            (block.clone(), effects.clone())
                        }
                        _ => unreachable!("already checked"),
                    };
                    if let Some((height, epoch)) = last_authorized_block_height_for_epoch {
                        if block.height > height {
                            // Stop processing certificates from untrusted epochs.
                            // Make sure to display the appropriate warning.
                            let next_height_to_receive = chain
                                .next_block_height_to_receive(application_id, origin.clone())
                                .await?;
                            if block.height >= next_height_to_receive {
                                log::warn!(
                                    "[{}] Refusing some updates to {recipient:?} from untrusted epoch {epoch:?} at {application_id:?}::{origin:?}",
                                    self.nickname,
                                );
                            } else {
                                log::warn!(
                                    "[{}] Ignoring repeated messages to {:?} from {:?}::{:?} at height {}",
                                    self.nickname,
                                    recipient,
                                    application_id,
                                    origin,
                                    block.height
                                );
                            }
                            break;
                        }
                    }
                    // Update the staged chain state with the received block.
                    if chain
                        .receive_block(
                            application_id,
                            &origin,
                            block.height,
                            effects,
                            certificate.hash,
                        )
                        .await?
                    {
                        self.storage.write_certificate(certificate).await?;
                        last_updated_height = Some(block.height);
                    } else {
                        log::warn!(
                            "[{}] Ignoring repeated messages to {:?} from {:?}::{:?} at height {}",
                            self.nickname,
                            recipient,
                            application_id,
                            origin,
                            block.height
                        );
                    }
                }
                match last_updated_height {
                    None => {
                        // Nothing happened.
                        return Ok(Vec::new());
                    }
                    Some(height) => {
                        if !self.allow_inactive_chains {
                            // Validator nodes are more strict than clients when it comes to
                            // processing cross-chain messages.
                            if !chain.is_active() {
                                // Refuse to create the chain state if it is still inactive by
                                // now. Accordingly, do not send a confirmation, so that the
                                // message is retried later.
                                log::warn!(
                                    "[{}] Refusing to deliver messages to an inactive chain {recipient:?}",
                                    self.nickname
                                );
                                return Ok(Vec::new());
                            }
                        }
                        chain.save().await?;
                        // Acknowledge the highest processed block height.
                        Ok(vec![CrossChainRequest::ConfirmUpdatedRecipient {
                            application_id,
                            origin,
                            recipient,
                            height,
                        }])
                    }
                }
            }
            CrossChainRequest::ConfirmUpdatedRecipient {
                application_id,
                origin:
                    Origin {
                        chain_id,
                        medium: Medium::Direct,
                    },
                recipient,
                height,
            } => {
                let mut chain = self.storage.load_chain(chain_id).await?;
                if chain
                    .mark_outbox_messages_as_received(application_id, recipient, height)
                    .await?
                {
                    chain.save().await?;
                }
                Ok(Vec::new())
            }
            CrossChainRequest::ConfirmUpdatedRecipient {
                application_id,
                origin:
                    Origin {
                        chain_id,
                        medium: Medium::Channel(name),
                    },
                recipient,
                height,
            } => {
                let mut chain = self.storage.load_chain(chain_id).await?;
                if chain
                    .mark_channel_messages_as_received(&name, application_id, recipient, height)
                    .await?
                {
                    chain.save().await?;
                }
                Ok(Vec::new())
            }
        }
    }
}

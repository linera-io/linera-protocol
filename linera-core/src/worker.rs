// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::data_types::{ChainInfo, ChainInfoQuery, ChainInfoResponse, CrossChainRequest};
use async_trait::async_trait;
use linera_base::{
    committee::Committee,
    crypto::{HashValue, KeyPair},
    data_types::{ArithmeticError, BlockHeight, ChainId, Epoch},
    ensure,
};
use linera_chain::{
    data_types::{Block, BlockProposal, Certificate, Message, Origin, Target, Value},
    ChainManagerOutcome, ChainStateView,
};
use linera_execution::{ApplicationDescription, ApplicationId, Query, Response};
use linera_storage::Store;
use linera_views::{
    log_view::LogView,
    views::{ContainerView, View, ViewError},
};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
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
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError>;

    /// Handle information queries on chains.
    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, WorkerError>;

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

#[derive(Clone, Debug, Eq, PartialEq)]
/// Notify that a chain has a new certified block or a new message.
pub struct Notification {
    pub chain_id: ChainId,
    pub reason: Reason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
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
    #[cfg(test)]
    pub(crate) async fn fully_handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<ChainInfoResponse, WorkerError> {
        self.fully_handle_certificate_with_notifications(certificate, None)
            .await
    }

    #[inline]
    pub(crate) async fn fully_handle_certificate_with_notifications(
        &mut self,
        certificate: Certificate,
        mut notifications: Option<&mut Vec<Notification>>,
    ) -> Result<ChainInfoResponse, WorkerError> {
        let (response, actions) = self.handle_certificate(certificate).await?;
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

    /// Get a reference to the [`KeyPair`], if available.
    fn key_pair(&self) -> Option<&KeyPair> {
        self.key_pair.as_ref().map(Arc::as_ref)
    }

    async fn create_cross_chain_request(
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
    async fn create_network_actions(
        &mut self,
        chain: &mut ChainStateView<Client::Context>,
    ) -> Result<NetworkActions, WorkerError> {
        let mut actions = NetworkActions::default();
        let chain_id = chain.chain_id();
        for application_id in chain.communication_states.indices().await? {
            let application = chain.describe_application(application_id).await?;
            let state = chain
                .communication_states
                .load_entry(application_id)
                .await?;
            for target in state.outboxes.indices().await? {
                let outbox = state.outboxes.load_entry(target.clone()).await?;
                let heights = outbox.block_heights().await?;
                let origin = Origin {
                    sender: chain_id,
                    medium: target.medium,
                };
                let request = self
                    .create_cross_chain_request(
                        &mut chain.confirmed_log,
                        application.clone(),
                        origin,
                        target.recipient,
                        &heights,
                    )
                    .await?;
                actions.cross_chain_requests.push(request);
            }
        }
        Ok(actions)
    }

    /// Process a confirmed block (aka a commit).
    async fn process_confirmed_block(
        &mut self,
        certificate: Certificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        let (block, effects, state_hash) = match &certificate.value {
            Value::ConfirmedBlock {
                block,
                effects,
                state_hash,
            } => (block, effects.clone(), *state_hash),
            _ => panic!("Expecting a confirmation certificate"),
        };
        // Check that the chain is active and ready for this confirmation.
        let mut chain = self.storage.load_active_chain(block.chain_id).await?;
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

    async fn process_cross_chain_update(
        &mut self,
        application: ApplicationDescription,
        origin: Origin,
        recipient: ChainId,
        certificates: Vec<Certificate>,
    ) -> Result<NetworkActions, WorkerError> {
        let mut chain = self.storage.load_chain(recipient).await?;
        let application_id = chain.register_application(application)?;
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
            let Value::ConfirmedBlock { block, effects, .. } = &certificate.value else {
                unreachable!("already checked");
            };
            // Update the staged chain state with the received block.
            chain
                .receive_block(
                    application_id,
                    &origin,
                    block.height,
                    effects.clone(),
                    certificate.hash,
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
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        log::trace!("{} <-- {:?}", self.nickname, certificate);
        match &certificate.value {
            Value::ValidatedBlock { .. } => {
                // Confirm the validated block.
                let info = self.process_validated_block(certificate).await?;
                Ok((info, NetworkActions::default()))
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
            let mut messages = Vec::new();
            for application_id in chain.communication_states.indices().await? {
                let state = chain
                    .communication_states
                    .load_entry(application_id)
                    .await?;
                for origin in state.inboxes.indices().await? {
                    let inbox = state.inboxes.load_entry(origin.clone()).await?;
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
    ) -> Result<NetworkActions, WorkerError> {
        log::trace!("{} <-- {:?}", self.nickname, request);
        match request {
            CrossChainRequest::UpdateRecipient {
                application,
                origin,
                recipient,
                certificates,
            } => {
                self.process_cross_chain_update(application, origin, recipient, certificates)
                    .await
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
            let Value::ConfirmedBlock { block, .. } = &certificate.value else {
                return Err(WorkerError::InvalidCrossChainRequest);
            };
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

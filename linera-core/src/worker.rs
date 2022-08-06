// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use linera_base::{
    chain::{ChainState, OutboxState},
    crypto::*,
    ensure,
    error::Error,
    manager::Outcome,
    messages::*,
};
use linera_storage::Storage;
use std::{collections::VecDeque, sync::Arc};

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
    ) -> Result<ChainInfoResponse, Error>;

    /// Process a certificate, e.g. to extend a chain with a confirmed block.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<(ChainInfoResponse, Vec<CrossChainRequest>), Error>;

    /// Handle information queries on chains.
    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, Error>;

    /// Handle a (trusted!) cross-chain request.
    async fn handle_cross_chain_request(
        &mut self,
        request: CrossChainRequest,
    ) -> Result<Vec<CrossChainRequest>, Error>;
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
}

impl<Client> WorkerState<Client> {
    pub fn new(nickname: String, key_pair: Option<KeyPair>, storage: Client) -> Self {
        WorkerState {
            nickname,
            key_pair: key_pair.map(Arc::new),
            storage,
            allow_inactive_chains: false,
        }
    }

    pub fn allow_inactive_chains(mut self, value: bool) -> Self {
        self.allow_inactive_chains = value;
        self
    }

    pub(crate) fn storage_client(&self) -> &Client {
        &self.storage
    }
}

impl<Client> WorkerState<Client>
where
    Client: Storage + Clone + Send + Sync + 'static,
{
    // NOTE: This only works for non-sharded workers!
    pub(crate) async fn fully_handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<ChainInfoResponse, linera_base::error::Error> {
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
    ) -> Result<ChainInfoResponse, Error> {
        let mut chain = self.storage.read_active_chain(block.chain_id).await?;
        chain.execute_block(block)?;
        let info = chain.make_chain_info(None);
        // Do not save the new state.
        Ok(info)
    }

    /// Get a reference to the [`KeyPair`], if available.
    fn key_pair(&self) -> Option<&KeyPair> {
        self.key_pair.as_ref().map(Arc::as_ref)
    }

    async fn make_cross_chain_request(
        &mut self,
        chain: &ChainState,
        application_id: ApplicationId,
        origin: Origin,
        recipient: ChainId,
        outbox: &OutboxState,
    ) -> Result<CrossChainRequest, Error> {
        let certificates = self
            .storage
            .read_certificates(
                outbox
                    .queue
                    .iter()
                    .map(|height| chain.confirmed_log[usize::from(*height)]),
            )
            .await?;
        Ok(CrossChainRequest::UpdateRecipient {
            application_id,
            origin,
            recipient,
            certificates,
        })
    }

    /// Load pending cross-chain requests.
    async fn make_continuation(
        &mut self,
        chain: &ChainState,
    ) -> Result<Vec<CrossChainRequest>, Error> {
        let mut continuation = Vec::new();
        for (application_id, state) in &chain.communication_states {
            for (&recipient, outbox) in &state.outboxes {
                let origin = Origin::Chain(chain.chain_id());
                let request = self
                    .make_cross_chain_request(chain, *application_id, origin, recipient, outbox)
                    .await?;
                continuation.push(request);
            }
            for (name, channel) in &state.channels {
                for (&recipient, outbox) in &channel.outboxes {
                    let origin = Origin::Channel(ChannelId {
                        chain_id: chain.chain_id(),
                        name: name.into(),
                    });
                    let request = self
                        .make_cross_chain_request(chain, *application_id, origin, recipient, outbox)
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
    ) -> Result<(ChainInfoResponse, Vec<CrossChainRequest>), Error> {
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
        let mut chain = self.storage.read_active_chain(sender).await?;
        if chain.next_block_height < block.height {
            return Err(Error::MissingEarlierBlocks {
                current_block_height: chain.next_block_height,
            });
        }
        if chain.next_block_height > block.height {
            // Block was already confirmed.
            let info = chain.make_chain_info(self.key_pair());
            let continuation = self.make_continuation(&chain).await?;
            return Ok((info, continuation));
        }
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        let epoch = chain.state.epoch.expect("chain is active");
        ensure!(
            block.epoch == epoch,
            Error::InvalidEpoch {
                chain_id: sender,
                epoch: block.epoch,
            }
        );
        let committee = chain.state.committees.get(&epoch).expect("chain is active");
        certificate
            .check(committee)
            .map_err(|_| Error::InvalidCertificate)?;
        // This should always be true for valid certificates.
        ensure!(
            chain.block_hash == block.previous_block_hash,
            Error::InvalidBlockChaining
        );
        // Persist certificate.
        self.storage.write_certificate(certificate.clone()).await?;
        // Make sure temporary manager information are cleared.
        chain.state.manager.reset();
        // Execute the block.
        let verified_effects = chain.execute_block(block)?;
        ensure!(effects == verified_effects, Error::IncorrectEffects);
        // Advance to next block height.
        chain.block_hash = Some(certificate.hash);
        chain.confirmed_log.push(certificate.hash);
        chain.next_block_height.try_add_assign_one()?;
        // We should always agree on the state hash.
        ensure!(chain.state_hash == state_hash, Error::IncorrectStateHash);
        let info = chain.make_chain_info(self.key_pair());
        let continuation = self.make_continuation(&chain).await?;
        // Persist chain.
        self.storage.write_chain(chain).await?;
        Ok((info, continuation))
    }

    /// Process a validated block issued from a multi-owner chain.
    async fn process_validated_block(
        &mut self,
        certificate: Certificate,
    ) -> Result<ChainInfoResponse, Error> {
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
        let mut chain = self.storage.read_active_chain(block.chain_id).await?;
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        let epoch = chain.state.epoch.expect("chain is active");
        ensure!(
            block.epoch == epoch,
            Error::InvalidEpoch {
                chain_id: block.chain_id,
                epoch: block.epoch,
            }
        );
        let committee = chain.state.committees.get(&epoch).expect("chain is active");
        certificate
            .check(committee)
            .map_err(|_| Error::InvalidCertificate)?;
        if chain
            .state
            .manager
            .check_validated_block(chain.next_block_height, block, round)?
            == Outcome::Skip
        {
            // If we just processed the same pending block, return the chain info
            // unchanged.
            return Ok(chain.make_chain_info(self.key_pair()));
        }
        chain.state.manager.create_final_vote(
            block.clone(),
            effects,
            state_hash,
            certificate,
            self.key_pair(),
        );
        let info = chain.make_chain_info(self.key_pair());
        self.storage.write_chain(chain).await?;
        Ok(info)
    }
}

#[async_trait]
impl<Client> ValidatorWorker for WorkerState<Client>
where
    Client: Storage + Clone + Send + Sync + 'static,
{
    async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, Error> {
        log::trace!("{} <-- {:?}", self.nickname, proposal);
        // Obtain the sender's chain.
        let sender = proposal.content.block.chain_id;
        let mut chain = self.storage.read_active_chain(sender).await?;
        // Check the epoch.
        let epoch = chain.state.epoch.expect("chain is active");
        ensure!(
            proposal.content.block.epoch == epoch,
            Error::InvalidEpoch {
                chain_id: sender,
                epoch,
            }
        );
        // Check authentication of the block.
        ensure!(
            chain.state.manager.has_owner(&proposal.owner),
            Error::InvalidOwner
        );
        proposal
            .signature
            .check(&proposal.content, proposal.owner.0)?;
        // Check if the chain is ready for this new block proposal.
        // This should always pass for nodes without voting key.
        if chain.state.manager.check_proposed_block(
            chain.block_hash,
            chain.next_block_height,
            &proposal.content.block,
            proposal.content.round,
        )? == Outcome::Skip
        {
            // If we just processed the same pending block, return the chain info
            // unchanged.
            return Ok(chain.make_chain_info(self.key_pair()));
        }
        let (effects, state_hash) = {
            // Execute the block on a copy of the chain state for validation.
            let mut staged = chain.clone();
            // Make sure the clear round information in the state so that it is not
            // hashed.
            staged.state.manager.reset();
            let effects = staged.execute_block(&proposal.content.block)?;
            // Verify that the resulting chain would have no unconfirmed incoming
            // messages.
            staged.validate_incoming_messages()?;
            (effects, staged.state_hash)
        };
        // Create the vote and store it in the chain state.
        chain
            .state
            .manager
            .create_vote(proposal, effects, state_hash, self.key_pair());
        let info = chain.make_chain_info(self.key_pair());
        self.storage.write_chain(chain).await?;
        Ok(info)
    }

    /// Process a certificate.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<(ChainInfoResponse, Vec<CrossChainRequest>), Error> {
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
    ) -> Result<ChainInfoResponse, Error> {
        log::trace!("{} <-- {:?}", self.nickname, query);
        let chain = self.storage.read_chain_or_default(query.chain_id).await?;
        let mut info = chain.make_chain_info(None).info;
        if query.request_execution_state {
            info.requested_execution_state = Some(chain.state);
        }
        if let Some(next_block_height) = query.test_next_block_height {
            ensure!(
                chain.next_block_height == next_block_height,
                Error::UnexpectedBlockHeight
            );
        }
        if query.request_pending_messages {
            let mut message_groups = Vec::new();
            for (application_id, state) in &chain.communication_states {
                for (origin, inbox) in &state.inboxes {
                    let mut effects = Vec::new();
                    let mut current_height = None;
                    for event in &inbox.received_events {
                        match current_height {
                            None => {
                                current_height = Some(event.height);
                            }
                            Some(height) if height != event.height => {
                                // If the height changed, flush the accumulated effects
                                // into a new group.
                                message_groups.push(MessageGroup {
                                    application_id: *application_id,
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
                            application_id: *application_id,
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
            let keys = chain.confirmed_log[..]
                .iter()
                .skip(range.start.into())
                .cloned();
            let certs = match range.limit {
                None => self.storage.read_certificates(keys).await?,
                Some(count) => self.storage.read_certificates(keys.take(count)).await?,
            };
            info.requested_sent_certificates = certs;
        }
        if let Some(idx) = query.request_received_certificates_excluding_first_nth {
            let keys = chain.received_log[..].iter().skip(idx).cloned();
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
    ) -> Result<Vec<CrossChainRequest>, Error> {
        log::trace!("{} <-- {:?}", self.nickname, request);
        match request {
            CrossChainRequest::UpdateRecipient {
                application_id,
                origin,
                recipient,
                certificates,
            } => {
                let mut chain = self.storage.read_chain_or_default(recipient).await?;
                let mut last_height = None;
                let mut last_epoch = None;
                let mut need_update = false;
                for certificate in certificates {
                    // Start by checking a few invariants. Note that we still crucially
                    // trust the worker of the sending chain to have verified and executed
                    // the blocks correctly.
                    let (block, effects) = match &certificate.value {
                        Value::ConfirmedBlock { block, effects, .. } => {
                            (block.clone(), effects.clone())
                        }
                        _ => {
                            return Err(Error::InvalidCrossChainRequest);
                        }
                    };
                    ensure!(
                        origin.sender() == block.chain_id,
                        Error::InvalidCrossChainRequest
                    );
                    ensure!(
                        last_height < Some(block.height),
                        Error::InvalidCrossChainRequest
                    );
                    ensure!(
                        last_epoch <= Some(block.epoch),
                        Error::InvalidCrossChainRequest
                    );
                    last_height = Some(block.height);
                    last_epoch = Some(block.epoch);
                    // Update the staged chain state with the received block.
                    if chain.receive_block(
                        application_id,
                        &origin,
                        block.height,
                        effects,
                        certificate.hash,
                    )? {
                        self.storage.write_certificate(certificate).await?;
                        need_update = true;
                    }
                }
                if need_update {
                    if !self.allow_inactive_chains {
                        // Validator nodes are more strict than clients when it comes to
                        // processing cross-chain messages.
                        if !chain.is_active() {
                            // Refuse to create the chain state if it is still inactive by
                            // now. Accordingly, do not send a confirmation, so that the
                            // message is retried later.
                            log::warn!(
                                "{}: refusing to store inactive chain {recipient:?}",
                                self.nickname
                            );
                            return Ok(Vec::new());
                        }
                        let epoch = last_epoch.expect("need_update implies epoch.is_some()");
                        if Some(epoch) < chain.state.epoch
                            && !chain.state.committees.contains_key(&epoch)
                        {
                            // Refuse to persist the chain state if the latest epoch in
                            // the received blocks from this recipient is not recognized
                            // any more by the receiving chain. (Future epochs are ok.)
                            log::warn!("Refusing updates from untrusted epoch {epoch:?}");
                            return Ok(Vec::new());
                        }
                    }
                    self.storage.write_chain(chain).await?;
                }
                match last_height {
                    Some(height) => {
                        // We have processed at least one certificate successfully: send back
                        // an acknowledgment.
                        Ok(vec![CrossChainRequest::ConfirmUpdatedRecipient {
                            application_id,
                            origin,
                            recipient,
                            height,
                        }])
                    }
                    None => Ok(Vec::new()),
                }
            }
            CrossChainRequest::ConfirmUpdatedRecipient {
                application_id,
                origin: Origin::Chain(sender),
                recipient,
                height,
            } => {
                let mut chain = self.storage.read_chain_or_default(sender).await?;
                if chain.mark_outbox_messages_as_received(application_id, recipient, height) {
                    self.storage.write_chain(chain).await?;
                }
                Ok(Vec::new())
            }
            CrossChainRequest::ConfirmUpdatedRecipient {
                application_id,
                origin: Origin::Channel(channel_id),
                recipient,
                height,
            } => {
                let mut chain = self
                    .storage
                    .read_chain_or_default(channel_id.chain_id)
                    .await?;
                if chain.mark_channel_messages_as_received(
                    &channel_id.name,
                    application_id,
                    recipient,
                    height,
                ) {
                    self.storage.write_chain(chain).await?;
                }
                Ok(Vec::new())
            }
        }
    }
}

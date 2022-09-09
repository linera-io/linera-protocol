// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use linera_base::{crypto::*, ensure, error::Error, manager::Outcome, messages::*};
use linera_storage2::{
    chain::{ChainStateView, OutboxStateView},
    Store,
};
use linera_views::views::{AppendOnlyLogView, View};
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
    Client: Store + Clone + Send + Sync + 'static,
    Error: From<Client::Error>,
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
        let mut chain = self.storage.load_active_chain(block.chain_id).await?;
        chain.execute_block(block).await?;
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
        confirmed_log: &mut AppendOnlyLogView<Client::Context, HashValue>,
        application_id: ApplicationId,
        origin: Origin,
        recipient: ChainId,
        outbox: &mut OutboxStateView<Client::Context>,
    ) -> Result<CrossChainRequest, Error> {
        let count = outbox.queue.count();
        let heights = outbox.queue.read_front(count).await?;
        let mut keys = Vec::new();
        for height in heights {
            if let Some(key) = confirmed_log.get(usize::from(height)).await? {
                keys.push(key);
            }
        }
        let certificates = self.storage.read_certificates(keys).await?;
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
        chain: &mut ChainStateView<Client::Context>,
    ) -> Result<Vec<CrossChainRequest>, Error> {
        let mut continuation = Vec::new();
        let chain_id = chain.chain_id();
        for application_id in chain.communication_states.indices().await? {
            let mut state = chain
                .communication_states
                .load_entry(application_id)
                .await?;
            for recipient in state.outboxes.indices().await? {
                let mut outbox = state.outboxes.load_entry(recipient).await?;
                let origin = Origin {
                    chain_id,
                    medium: Medium::Direct,
                };
                let request = self
                    .make_cross_chain_request(
                        &mut chain.confirmed_log,
                        application_id,
                        origin,
                        recipient,
                        &mut *outbox,
                    )
                    .await?;
                continuation.push(request);
            }
            for name in state.channels.indices().await? {
                let mut channel = state.channels.load_entry(name.clone()).await?;
                for recipient in channel.outboxes.indices().await? {
                    let mut outbox = channel.outboxes.load_entry(recipient).await?;
                    let origin = Origin {
                        chain_id,
                        medium: Medium::Channel(name.clone()),
                    };
                    let request = self
                        .make_cross_chain_request(
                            &mut chain.confirmed_log,
                            application_id,
                            origin,
                            recipient,
                            &mut *outbox,
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
        let mut chain = self.storage.load_active_chain(sender).await?;
        let tip = chain.tip_state.get();
        if tip.next_block_height < block.height {
            return Err(Error::MissingEarlierBlocks {
                current_block_height: tip.next_block_height,
            });
        }
        if tip.next_block_height > block.height {
            // Block was already confirmed.
            let info = chain.make_chain_info(self.key_pair());
            let continuation = self.make_continuation(&mut chain).await?;
            return Ok((info, continuation));
        }
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        let epoch = chain
            .execution_state
            .get()
            .system
            .epoch
            .expect("chain is active");
        ensure!(
            block.epoch == epoch,
            Error::InvalidEpoch {
                chain_id: sender,
                epoch: block.epoch,
            }
        );
        let committee = chain
            .execution_state
            .get()
            .system
            .committees
            .get(&epoch)
            .expect("chain is active");
        certificate
            .check(committee)
            .map_err(|_| Error::InvalidCertificate)?;
        // This should always be true for valid certificates.
        ensure!(
            tip.block_hash == block.previous_block_hash,
            Error::InvalidBlockChaining
        );
        // Persist certificate.
        self.storage.write_certificate(certificate.clone()).await?;
        // Make sure temporary manager information are cleared.
        chain.execution_state.get_mut().system.manager.reset();
        // Execute the block.
        let verified_effects = chain.execute_block(block).await?;
        ensure!(effects == verified_effects, Error::IncorrectEffects);
        // Advance to next block height.
        let tip = chain.tip_state.get_mut();
        tip.block_hash = Some(certificate.hash);
        tip.next_block_height.try_add_assign_one()?;
        chain.confirmed_log.push(certificate.hash);
        // We should always agree on the state hash.
        ensure!(
            *chain.execution_state_hash.get() == Some(state_hash),
            Error::IncorrectStateHash
        );
        let info = chain.make_chain_info(self.key_pair());
        let continuation = self.make_continuation(&mut chain).await?;
        // Persist chain.
        chain.write_commit().await?;
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
        let mut chain = self.storage.load_active_chain(block.chain_id).await?;
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        let epoch = chain
            .execution_state
            .get()
            .system
            .epoch
            .expect("chain is active");
        ensure!(
            block.epoch == epoch,
            Error::InvalidEpoch {
                chain_id: block.chain_id,
                epoch: block.epoch,
            }
        );
        let committee = chain
            .execution_state
            .get_mut()
            .system
            .committees
            .get(&epoch)
            .expect("chain is active");
        certificate
            .check(committee)
            .map_err(|_| Error::InvalidCertificate)?;
        if chain
            .execution_state
            .get_mut()
            .system
            .manager
            .check_validated_block(chain.tip_state.get().next_block_height, block, round)?
            == Outcome::Skip
        {
            // If we just processed the same pending block, return the chain info
            // unchanged.
            return Ok(chain.make_chain_info(self.key_pair()));
        }
        chain
            .execution_state
            .get_mut()
            .system
            .manager
            .create_final_vote(
                block.clone(),
                effects,
                state_hash,
                certificate,
                self.key_pair(),
            );
        let info = chain.make_chain_info(self.key_pair());
        chain.write_commit().await?;
        Ok(info)
    }
}

#[async_trait]
impl<Client> ValidatorWorker for WorkerState<Client>
where
    Client: Store + Clone + Send + Sync + 'static,
    Error: From<Client::Error>,
{
    async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, Error> {
        log::trace!("{} <-- {:?}", self.nickname, proposal);
        // Obtain the sender's chain.
        let sender = proposal.content.block.chain_id;
        let mut chain = self.storage.load_active_chain(sender).await?;
        // Check the epoch.
        let epoch = chain
            .execution_state
            .get()
            .system
            .epoch
            .expect("chain is active");
        ensure!(
            proposal.content.block.epoch == epoch,
            Error::InvalidEpoch {
                chain_id: sender,
                epoch,
            }
        );
        // Check authentication of the block.
        ensure!(
            chain
                .execution_state
                .get()
                .system
                .manager
                .has_owner(&proposal.owner),
            Error::InvalidOwner
        );
        proposal
            .signature
            .check(&proposal.content, proposal.owner.0)?;
        // Check if the chain is ready for this new block proposal.
        // This should always pass for nodes without voting key.
        if chain
            .execution_state
            .get()
            .system
            .manager
            .check_proposed_block(
                chain.tip_state.get().block_hash,
                chain.tip_state.get().next_block_height,
                &proposal.content.block,
                proposal.content.round,
            )?
            == Outcome::Skip
        {
            // If we just processed the same pending block, return the chain info
            // unchanged.
            return Ok(chain.make_chain_info(self.key_pair()));
        }
        let (effects, state_hash) = {
            // Make sure the clear round information in the state so that it is not
            // hashed.
            chain.execution_state.get_mut().system.manager.reset();
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
        chain.execution_state.get_mut().system.manager.create_vote(
            proposal,
            effects,
            state_hash,
            self.key_pair(),
        );
        let info = chain.make_chain_info(self.key_pair());
        chain.write_commit().await?;
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
        let mut chain = self.storage.load_chain(query.chain_id).await?;
        let mut info = chain.make_chain_info(None).info;
        if query.request_system_execution_state {
            info.requested_system_execution_state =
                Some(chain.execution_state.get().system.clone());
        }
        if let Some(next_block_height) = query.test_next_block_height {
            ensure!(
                chain.tip_state.get().next_block_height == next_block_height,
                Error::UnexpectedBlockHeight {
                    expected_block_height: next_block_height,
                    found_block_height: chain.tip_state.get().next_block_height
                }
            );
        }
        if query.request_pending_messages {
            let mut message_groups = Vec::new();
            for application_id in chain.communication_states.indices().await? {
                let mut state = chain
                    .communication_states
                    .load_entry(application_id)
                    .await?;
                for origin in state.inboxes.indices().await? {
                    let mut inbox = state.inboxes.load_entry(origin.clone()).await?;
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
    ) -> Result<Vec<CrossChainRequest>, Error> {
        log::trace!("{} <-- {:?}", self.nickname, request);
        match request {
            CrossChainRequest::UpdateRecipient {
                application_id,
                origin,
                recipient,
                certificates,
            } => {
                let mut chain = self.storage.load_chain(recipient).await?;
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
                        origin.chain_id == block.chain_id,
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
                        if Some(epoch) < chain.execution_state.get().system.epoch
                            && !chain
                                .execution_state
                                .get()
                                .system
                                .committees
                                .contains_key(&epoch)
                        {
                            // Refuse to persist the chain state if the latest epoch in
                            // the received blocks from this recipient is not recognized
                            // any more by the receiving chain. (Future epochs are ok.)
                            log::warn!("Refusing updates from untrusted epoch {epoch:?}");
                            return Ok(Vec::new());
                        }
                    }
                    chain.write_commit().await?;
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
                    chain.write_commit().await?;
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
                    chain.write_commit().await?;
                }
                Ok(Vec::new())
            }
        }
    }
}

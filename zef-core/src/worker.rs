// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use zef_base::{chain::ChainState, crypto::*, ensure, error::Error, manager::Outcome, messages::*};
use zef_storage::Storage;

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
pub struct WorkerState<StorageClient> {
    /// A name used for logging
    nickname: String,
    /// The signature key pair of the validator. The key may be missing for replicas
    /// without voting rights (possibly with a partial view of chains).
    key_pair: Option<KeyPair>,
    /// Access to local persistent storage.
    storage: StorageClient,
    /// Whether inactive chains are allowed in storage.
    allow_inactive_chains: bool,
}

impl<Client> WorkerState<Client> {
    pub fn new(
        nickname: String,
        key_pair: Option<KeyPair>,
        storage: Client,
        allow_inactive_chains: bool,
    ) -> Self {
        WorkerState {
            nickname,
            key_pair,
            storage,
            allow_inactive_chains,
        }
    }

    pub(crate) fn storage_client(&self) -> &Client {
        &self.storage
    }
}

impl<Client> WorkerState<Client>
where
    Client: Storage + Clone + 'static,
{
    // NOTE: This only works for non-sharded workers!
    pub(crate) async fn fully_handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<ChainInfoResponse, zef_base::error::Error> {
        let (response, mut requests) = self.handle_certificate(certificate).await?;
        while let Some(request) = requests.pop() {
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

    /// Load pending cross-chain requests.
    async fn make_continuation(
        &mut self,
        chain: &ChainState,
    ) -> Result<Vec<CrossChainRequest>, Error> {
        let mut continuation = Vec::new();
        for (&recipient, outbox) in &chain.outboxes {
            let certificates = self
                .storage
                .read_certificates(outbox.queue.iter().map(|(_, hash)| *hash))
                .await?;
            continuation.push(CrossChainRequest::UpdateRecipient {
                sender: chain.state.chain_id,
                recipient,
                certificates,
            })
        }
        log::trace!("{} --> {:?}", self.nickname, continuation);
        Ok(continuation)
    }

    /// (Trusted) Process a confirmed block (aka a commit).
    async fn process_confirmed_block(
        &mut self,
        certificate: Certificate,
    ) -> Result<(ChainInfoResponse, Vec<CrossChainRequest>), Error> {
        let block = certificate
            .value
            .confirmed_block()
            .expect("A confirmation certificate");
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
            let info = chain.make_chain_info(self.key_pair.as_ref());
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
        let notifications = chain.execute_block(block)?;
        // Advance to next block height.
        chain.block_hash = Some(certificate.hash);
        chain.confirmed_log.push(certificate.hash);
        chain.next_block_height.try_add_assign_one()?;
        // We should always agree on the state hash.
        ensure!(
            chain.state_hash == certificate.value.state_hash(),
            Error::IncorrectStateHash
        );
        // Final touch on the sender's chain.
        let info = chain.make_chain_info(self.key_pair.as_ref());
        // Schedule a new cross-chain request to notify each recipient about the given
        // blocks (generally, just this one).
        for (recipient, heights) in notifications {
            if heights.is_empty() {
                // Save a few bytes in the outbox.
                continue;
            }
            let queue = &mut chain.outboxes.entry(recipient).or_default().queue;
            for height in heights {
                let hash = chain.confirmed_log[usize::from(height)];
                queue.push_back((height, hash));
            }
        }
        let continuation = self.make_continuation(&chain).await?;
        // Persist chain.
        self.storage.write_chain(chain).await?;
        Ok((info, continuation))
    }

    /// (Trusted) Process a validated block issued from a multi-owner chain.
    async fn process_validated_block(
        &mut self,
        certificate: Certificate,
    ) -> Result<ChainInfoResponse, Error> {
        let (block, round, state_hash) = match &certificate.value {
            Value::ValidatedBlock {
                block,
                round,
                state_hash,
            } => (block, *round, *state_hash),
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
            return Ok(chain.make_chain_info(self.key_pair.as_ref()));
        }
        chain.state.manager.create_final_vote(
            block.clone(),
            state_hash,
            certificate,
            self.key_pair.as_ref(),
        );
        let info = chain.make_chain_info(self.key_pair.as_ref());
        self.storage.write_chain(chain).await?;
        Ok(info)
    }
}

#[async_trait]
impl<Client> ValidatorWorker for WorkerState<Client>
where
    Client: Storage + Clone + 'static,
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
            return Ok(chain.make_chain_info(self.key_pair.as_ref()));
        }
        let state_hash = {
            // Execute the block on a copy of the chain state for validation.
            let mut staged = chain.clone();
            // Make sure the clear round information in the state so that it is not
            // hashed.
            staged.state.manager.reset();
            staged.execute_block(&proposal.content.block)?;
            // Verify that the resulting chain would have no unconfirmed incoming
            // messages.
            staged.validate_incoming_messages()?;
            staged.state_hash
        };
        // Create the vote and store it in the chain state.
        chain
            .state
            .manager
            .create_vote(proposal, state_hash, self.key_pair.as_ref());
        let info = chain.make_chain_info(self.key_pair.as_ref());
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
        if query.query_committees {
            info.queried_committees = chain.state.committees;
        }
        if let Some(next_block_height) = query.check_next_block_height {
            ensure!(
                chain.next_block_height == next_block_height,
                Error::UnexpectedBlockHeight
            );
        }
        if query.query_pending_messages {
            let mut message_groups = Vec::new();
            for (&sender_id, inbox) in &chain.inboxes {
                let mut operations = Vec::new();
                let mut current_height = None;
                for event in &inbox.received_events {
                    match current_height {
                        None => {
                            current_height = Some(event.height);
                        }
                        Some(height) if height != event.height => {
                            // If the height changed, flush the accumulated operations
                            // into a new group.
                            message_groups.push(MessageGroup {
                                sender_id,
                                height,
                                operations,
                            });
                            operations = Vec::new();
                            current_height = Some(event.height);
                        }
                        _ => {
                            // Otherwise, continue adding operations to the same group.
                        }
                    }
                    operations.push((event.index, event.operation.clone()));
                }
                if let Some(height) = current_height {
                    message_groups.push(MessageGroup {
                        sender_id,
                        height,
                        operations,
                    });
                }
            }
            info.queried_pending_messages = message_groups;
        }
        if let Some(range) = query.query_sent_certificates_in_range {
            let keys = chain.confirmed_log[..]
                .iter()
                .skip(range.start.into())
                .cloned();
            let certs = match range.limit {
                None => self.storage.read_certificates(keys).await?,
                Some(count) => self.storage.read_certificates(keys.take(count)).await?,
            };
            info.queried_sent_certificates = certs;
        }
        if let Some(idx) = query.query_received_certificates_excluding_first_nth {
            let keys = chain.received_log[..].iter().skip(idx).cloned();
            let certs = self.storage.read_certificates(keys).await?;
            info.queried_received_certificates = certs;
        }
        let response = ChainInfoResponse::new(info, self.key_pair.as_ref());
        Ok(response)
    }

    async fn handle_cross_chain_request(
        &mut self,
        request: CrossChainRequest,
    ) -> Result<Vec<CrossChainRequest>, Error> {
        log::trace!("{} <-- {:?}", self.nickname, request);
        match request {
            CrossChainRequest::UpdateRecipient {
                sender,
                recipient,
                certificates,
            } => {
                let mut chain = self.storage.read_chain_or_default(recipient).await?;
                let mut height = None;
                let mut need_update = false;
                for certificate in certificates {
                    let block = certificate
                        .value
                        .confirmed_block()
                        .ok_or(Error::InvalidCrossChainRequest)?;
                    ensure!(block.chain_id == sender, Error::InvalidCrossChainRequest);
                    ensure!(height < Some(block.height), Error::InvalidCrossChainRequest);
                    height = Some(block.height);
                    if chain.receive_block(
                        block.chain_id,
                        block.height,
                        block.operations.clone(),
                        certificate.hash,
                    )? {
                        self.storage.write_certificate(certificate).await?;
                        need_update = true;
                    }
                }
                if need_update {
                    if !self.allow_inactive_chains && !chain.is_active() {
                        // Refuse to create the chain state if it is still inactive by
                        // now. Accordingly, do not send a confirmation, so that the
                        // message is retried later.
                        return Ok(Vec::new());
                    }
                    self.storage.write_chain(chain).await?;
                }
                if let Some(height) = height {
                    let request = CrossChainRequest::ConfirmUpdatedRecipient {
                        sender,
                        recipient,
                        height,
                    };
                    Ok(vec![request])
                } else {
                    Ok(Vec::new())
                }
            }
            CrossChainRequest::ConfirmUpdatedRecipient {
                sender,
                recipient,
                height,
            } => {
                let mut chain = self.storage.read_active_chain(sender).await?;
                if let std::collections::hash_map::Entry::Occupied(mut entry) =
                    chain.outboxes.entry(recipient)
                {
                    let mut updated = false;
                    while let Some((h, _)) = entry.get().queue.front() {
                        if *h > height {
                            break;
                        }
                        entry.get_mut().queue.pop_front().unwrap();
                        updated = true;
                    }
                    if updated {
                        if entry.get().queue.is_empty() {
                            entry.remove();
                        }
                        self.storage.write_chain(chain).await?;
                    }
                }
                Ok(Vec::new())
            }
        }
    }
}

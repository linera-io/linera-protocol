// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    node::{LocalNodeClient, ValidatorNode},
    updater::{communicate_with_quorum, CommunicateAction, ValidatorUpdater},
    worker::WorkerState,
};
use anyhow::{anyhow, bail, ensure, Result};
use async_trait::async_trait;
use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};
use zef_base::{
    committee::Committee,
    crypto::*,
    error::Error,
    execution::{Address, Amount, Balance, Effect, ExecutionState, Operation, UserData},
    manager::ChainManager,
    messages::*,
};
use zef_storage::Storage;

#[cfg(test)]
#[path = "unit_tests/client_tests.rs"]
mod client_tests;

/// How to communicate with a chain across all the validators. As a rule,
/// operations are considered successful (and communication may stop) when they succeeded
/// in gathering a quorum of responses.
#[async_trait]
pub trait ChainClient {
    /// Send money to a chain.
    async fn transfer_to_chain(
        &mut self,
        amount: Amount,
        recipient: ChainId,
        user_data: UserData,
    ) -> Result<Certificate>;

    /// Burn money.
    async fn burn(&mut self, amount: Amount, user_data: UserData) -> Result<Certificate>;

    /// Process confirmed operation for which this chain is a recipient.
    async fn receive_certificate(&mut self, certificate: Certificate) -> Result<()>;

    /// Rotate the key of the chain.
    async fn rotate_key_pair(&mut self, key_pair: KeyPair) -> Result<Certificate>;

    /// Transfer ownership of the chain.
    async fn transfer_ownership(&mut self, new_owner: Owner) -> Result<Certificate>;

    /// Add another owner to the chain.
    async fn share_ownership(&mut self, new_owner: Owner) -> Result<Certificate>;

    /// Open a new chain with a derived UID.
    async fn open_chain(&mut self, owner: Owner) -> Result<(ChainId, Certificate)>;

    /// Close the chain (and lose everything in it!!).
    async fn close_chain(&mut self) -> Result<Certificate>;

    /// Create a new committee (admin chains only).
    async fn stage_new_voting_rights(
        &mut self,
        voting_rights: BTreeMap<ValidatorName, usize>,
    ) -> Result<Certificate>;

    /// Create an empty block to process all incoming messages.
    async fn process_inbox(&mut self) -> Result<Certificate>;

    /// Start listening to the admin chain for new committees. (This is only useful for
    /// other genesis chains.)
    async fn subscribe_to_new_committees(&mut self) -> Result<Certificate>;

    /// Deprecate all the configurations of voting rights but the last one (admin chains
    /// only). Currently, each individual chain is still entitled to wait before accepting
    /// this command. However, it is expected that deprecated validators stop functioning
    /// shortly after such command is issued.
    async fn finalize_voting_rights(&mut self) -> Result<Certificate>;

    /// Send money to a chain.
    /// Do not check balance. (This may block the client)
    /// Do not confirm the transaction.
    async fn transfer_to_chain_unsafe_unconfirmed(
        &mut self,
        amount: Amount,
        recipient: ChainId,
        user_data: UserData,
    ) -> Result<Certificate>;

    /// Attempt to synchronize with validators and re-compute our balance.
    async fn synchronize_balance(&mut self) -> Result<Balance>;

    /// Retry the last pending block
    async fn retry_pending_block(&mut self) -> Result<Option<Certificate>>;

    /// Clear the information on any operation that previously failed.
    async fn clear_pending_block(&mut self);

    /// Return the current local balance.
    async fn local_balance(&mut self) -> Result<Balance>;
}

/// Reference implementation of the `ChainClient` trait using many instances of some
/// `ValidatorNode` implementation for communication, and a client to some (local)
/// storage.
pub struct ChainClientState<ValidatorNode, StorageClient> {
    /// The off-chain chain id.
    chain_id: ChainId,
    /// How to talk to the validators.
    validator_clients: Vec<(ValidatorName, ValidatorNode)>,
    /// Latest block hash, if any.
    block_hash: Option<HashValue>,
    /// Sequence number that we plan to use for the next block.
    /// We track this value outside local storage mainly for security reasons.
    next_block_height: BlockHeight,
    /// Round number that we plan to use for the next block.
    next_round: RoundNumber,
    /// Pending block.
    pending_block: Option<Block>,
    /// Known key pairs from present and past identities.
    known_key_pairs: BTreeMap<Owner, KeyPair>,

    /// Support synchronization of received certificates.
    received_certificate_trackers: HashMap<ValidatorName, usize>,
    /// How much time to wait between attempts when we wait for a cross-chain update.
    cross_chain_delay: Duration,
    /// How many times we are willing to retry a block that depends on cross-chain updates.
    cross_chain_retries: usize,
    /// Local node to manage the execution state and the local storage of the chains that we are
    /// tracking.
    node_client: LocalNodeClient<StorageClient>,
}

impl<A, S> ChainClientState<A, S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chain_id: ChainId,
        known_key_pairs: Vec<KeyPair>,
        validator_clients: Vec<(ValidatorName, A)>,
        storage_client: S,
        block_hash: Option<HashValue>,
        next_block_height: BlockHeight,
        cross_chain_delay: Duration,
        cross_chain_retries: usize,
    ) -> Self {
        let known_key_pairs = known_key_pairs
            .into_iter()
            .map(|kp| (Owner(kp.public()), kp))
            .collect();
        let state = WorkerState::new(format!("Client node {:?}", chain_id), None, storage_client)
            .allow_inactive_chains(true);
        let node_client = LocalNodeClient::new(state);
        Self {
            chain_id,
            validator_clients,
            block_hash,
            next_block_height,
            next_round: RoundNumber::default(),
            pending_block: None,
            known_key_pairs,
            received_certificate_trackers: HashMap::new(),
            cross_chain_delay,
            cross_chain_retries,
            node_client,
        }
    }

    pub fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    pub fn block_hash(&self) -> Option<HashValue> {
        self.block_hash
    }

    pub fn next_block_height(&self) -> BlockHeight {
        self.next_block_height
    }

    pub fn pending_block(&self) -> &Option<Block> {
        &self.pending_block
    }
}

impl<A, S> ChainClientState<A, S>
where
    A: ValidatorNode + Send + Sync + 'static + Clone,
    S: Storage + Clone + 'static,
{
    async fn chain_info(&mut self) -> Result<ChainInfo, Error> {
        let query = ChainInfoQuery::new(self.chain_id);
        let response = self.node_client.handle_chain_info_query(query).await?;
        Ok(response.info)
    }

    async fn pending_messages(&mut self) -> Result<Vec<MessageGroup>, Error> {
        let query = ChainInfoQuery::new(self.chain_id).with_pending_messages();
        let response = self.node_client.handle_chain_info_query(query).await?;
        Ok(response.info.requested_pending_messages)
    }

    async fn committee(&mut self) -> Result<Committee, Error> {
        let mut state = self.execution_state().await?;
        state
            .committees
            .remove(&state.epoch.ok_or(Error::InactiveChain(self.chain_id))?)
            .ok_or(Error::InactiveChain(self.chain_id))
    }

    async fn execution_state(&mut self) -> Result<ExecutionState, Error> {
        let query = ChainInfoQuery::new(self.chain_id).with_execution_state();
        let info = self.node_client.handle_chain_info_query(query).await?.info;
        let state = info
            .requested_execution_state
            .ok_or(Error::InvalidChainInfoResponse)?;
        Ok(state)
    }

    async fn epoch(&mut self) -> Result<Epoch, anyhow::Error> {
        Ok(self
            .chain_info()
            .await?
            .epoch
            .ok_or(Error::InactiveChain(self.chain_id))?)
    }

    async fn next_admin_height(&mut self) -> Result<BlockHeight, Error> {
        Ok(self.execution_state().await?.next_admin_height())
    }

    async fn identity(&mut self) -> Result<Owner, anyhow::Error> {
        match self.chain_info().await?.manager {
            ChainManager::Single(m) => {
                if !self.known_key_pairs.contains_key(&m.owner) {
                    bail!(
                        "No key available to interact with single-owner chain {}",
                        self.chain_id
                    );
                }
                Ok(m.owner)
            }
            ChainManager::Multi(m) => {
                let mut identities = Vec::new();
                for (owner, ()) in &m.owners {
                    if self.known_key_pairs.contains_key(owner) {
                        identities.push(*owner);
                    }
                }
                if identities.is_empty() {
                    bail!(
                        "Cannot find suitable identity to interact with multi-owner chain {}",
                        self.chain_id
                    );
                }
                if identities.len() >= 2 {
                    bail!(
                        "Found several possible identities to interact with multi-owner chain {}",
                        self.chain_id
                    );
                }
                Ok(identities.pop().unwrap())
            }
            ChainManager::None => Err(Error::InactiveChain(self.chain_id).into()),
        }
    }

    pub async fn key_pair(&mut self) -> Result<&KeyPair> {
        let id = self.identity().await?;
        Ok(self
            .known_key_pairs
            .get(&id)
            .expect("key should be known at this point"))
    }
}

impl<A, S> ChainClientState<A, S>
where
    A: ValidatorNode + Send + Sync + 'static + Clone,
    S: Storage + Clone + 'static,
{
    /// Prepare the chain for the next operation.
    async fn prepare_chain(&mut self) -> Result<(), Error> {
        // Verify that our local storage contains enough history compared to the
        // expected block height. Otherwise, download the missing history from the
        // network.
        let mut info = self
            .node_client
            .download_certificates(
                self.validator_clients.clone(),
                self.chain_id,
                self.next_block_height,
            )
            .await?;
        if info.next_block_height == self.next_block_height {
            // Check that our local node has the expected block hash.
            zef_base::ensure!(
                self.block_hash == info.block_hash,
                Error::InvalidBlockChaining
            );
        }
        if matches!(info.manager, ChainManager::Multi(_)) {
            // For multi-owner chains, we could be missing recent certificates created by
            // other owners. Further synchronize blocks from the network. This is a
            // best-effort that depends on network conditions.
            info = self
                .node_client
                .synchronize_chain_state(self.validator_clients.clone(), self.chain_id)
                .await?;
        }
        // Update chain information tracked by the client.
        if (info.next_block_height, info.manager.next_round())
            > (self.next_block_height, self.next_round)
        {
            self.next_block_height = info.next_block_height;
            self.next_round = info.manager.next_round();
            self.block_hash = info.block_hash;
        }
        Ok(())
    }

    /// Broadcast certified blocks and optionally one more block proposal.
    /// The corresponding block heights should be consecutive and increasing.
    async fn communicate_chain_updates(
        &mut self,
        committee: &Committee,
        chain_id: ChainId,
        action: CommunicateAction,
    ) -> Result<Option<Certificate>> {
        let storage_client = self.node_client.storage_client().await;
        let cross_chain_delay = self.cross_chain_delay;
        let cross_chain_retries = self.cross_chain_retries;
        let result = communicate_with_quorum(
            &self.validator_clients,
            committee,
            |value: &Option<Vote>| -> Option<(Vec<Effect>, HashValue)> {
                value
                    .as_ref()
                    .map(|vote| vote.value.effects_and_state_hash())
            },
            |name, client| {
                let mut updater = ValidatorUpdater {
                    name,
                    client,
                    store: storage_client.clone(),
                    delay: cross_chain_delay,
                    retries: cross_chain_retries,
                };
                let action = action.clone();
                Box::pin(async move { updater.send_chain_update(chain_id, action).await })
            },
        )
        .await;
        let (effects_and_state_hash, votes): (Option<_>, Vec<_>) = match result {
            Ok(content) => content,
            Err(Some(Error::InactiveChain(id)))
                if id == chain_id
                    && matches!(action, CommunicateAction::AdvanceToNextBlockHeight(_)) =>
            {
                // The chain is visibly not active (yet or any more) so there is no need
                // to synchronize block heights.
                return Ok(None);
            }
            Err(Some(err)) => bail!("Failed to communicate with a quorum of validators: {}", err),
            Err(None) => {
                bail!("Failed to communicate with a quorum of validators (multiple errors)")
            }
        };
        let signatures: Vec<_> = votes
            .into_iter()
            .filter_map(|vote| match vote {
                Some(vote) => Some((vote.validator, vote.signature)),
                None => None,
            })
            .collect();
        match action {
            CommunicateAction::SubmitBlockForConfirmation(proposal) => {
                let (effects, state_hash) =
                    effects_and_state_hash.expect("this action produces votes");
                let value = Value::ConfirmedBlock {
                    block: proposal.content.block,
                    effects,
                    state_hash,
                };
                let certificate = Certificate::new(value, signatures);
                // Certificate is valid because
                // * `communicate_with_quorum` ensured a sufficient "weight" of
                // (non-error) answers were returned by validators.
                // * each answer is a vote signed by the expected validator.
                Ok(Some(certificate))
            }
            CommunicateAction::SubmitBlockForValidation(proposal) => {
                let (effects, state_hash) =
                    effects_and_state_hash.expect("this action produces votes");
                let BlockAndRound { block, round } = proposal.content;
                let value = Value::ValidatedBlock {
                    block,
                    round,
                    effects,
                    state_hash,
                };
                let certificate = Certificate::new(value, signatures);
                Ok(Some(certificate))
            }
            CommunicateAction::FinalizeBlock(validity_certificate) => {
                let (block, effects, state_hash) = match validity_certificate.value {
                    Value::ValidatedBlock {
                        block,
                        effects,
                        state_hash,
                        ..
                    } => (block, effects, state_hash),
                    _ => unreachable!(),
                };
                let certificate = Certificate::new(
                    Value::ConfirmedBlock {
                        block,
                        effects,
                        state_hash,
                    },
                    signatures,
                );
                Ok(Some(certificate))
            }
            CommunicateAction::AdvanceToNextBlockHeight(_) => Ok(None),
        }
    }

    /// Attempt to download new received certificates.
    ///
    /// This is a best effort: it will only find certificates that have been confirmed
    /// amongst sufficiently many validators of the current committee of the target
    /// chain.
    ///
    /// However, this should be the case whenever a sender's chain is still in use and
    /// is regularly upgraded to new committees.
    async fn find_received_certificates(&mut self) -> Result<()> {
        let chain_id = self.chain_id;
        let state = self.execution_state().await?;
        let admin_id = state.admin_id()?;
        let committees = state.committees;
        let epoch = state.epoch.ok_or(Error::InactiveChain(chain_id))?;
        let committee = committees
            .get(&epoch)
            .ok_or(Error::InactiveChain(chain_id))?;
        let trackers = self.received_certificate_trackers.clone();
        let result = communicate_with_quorum(
            &self.validator_clients,
            committee,
            |_| (),
            |name, mut client| {
                let tracker = *trackers.get(&name).unwrap_or(&0);
                let committees = committees.clone();
                Box::pin(async move {
                    // Retrieve new received certificates from this validator.
                    let query = ChainInfoQuery::new(chain_id)
                        .with_received_certificates_excluding_first_nth(tracker);
                    let response = client.handle_chain_info_query(query).await?;
                    // Responses are authenticated for accountability.
                    response.check(name)?;
                    let mut certificates = Vec::new();
                    let mut new_tracker = tracker;
                    for certificate in response.info.requested_received_certificates {
                        let block = certificate
                            .value
                            .confirmed_block()
                            .ok_or(Error::ClientErrorWhileQueryingCertificate)?;
                        // Check that certificates  are valid w.r.t one of our trusted
                        // committees.
                        if block.chain_id == admin_id {
                            // That is, unless it comes from the admin chain. We don't
                            // have a good way to enforce the policy on the admin chain
                            // yet because subscriptions may accepted in a later epoch.
                            certificates.push(certificate);
                            new_tracker += 1;
                            continue;
                        }
                        if block.epoch > epoch {
                            // We don't accept a certificate from a committee in the
                            // future.
                            log::warn!(
                                "Postponing received certificate from future epoch {:?}",
                                block.epoch
                            );
                            // Stop the synchronization here. Do not incrememt the tracker
                            // so that the certificate can still be downloaded later, once
                            // our committee was updated.
                            break;
                        }
                        match committees.get(&block.epoch) {
                            Some(committee) => {
                                // This epoch is recognized by our chain. Let's verify the
                                // certificate.
                                certificate.check(committee)?;
                                certificates.push(certificate);
                                new_tracker += 1;
                            }
                            None => {
                                // This epoch is not recognized any more. Let's skip the
                                // certificate. If a higher block with a recognized epoch
                                // comes up later from the same chain, the call to
                                // `receive_certificate` below will download the skipped
                                // certificate again.
                                log::warn!(
                                    "Skipping received certificate from past epoch {:?}",
                                    block.epoch
                                );
                                new_tracker += 1;
                            }
                        }
                    }
                    Ok((name, new_tracker, certificates))
                })
            },
        )
        .await;
        let responses = match result {
            Ok(((), responses)) => responses,
            Err(Some(Error::InactiveChain(id))) if id == chain_id => {
                // The chain is visibly not active (yet or any more) so there is no need
                // to synchronize received certificates.
                return Ok(());
            }
            Err(Some(err)) => bail!("Failed to communicate with a quorum of validators: {}", err),
            Err(None) => {
                bail!("Failed to communicate with a quorum of validators (multiple errors)")
            }
        };
        'outer: for (name, tracker, certificates) in responses {
            // Process received certificates.
            for certificate in certificates {
                let hash = certificate.hash;
                if let Err(e) = self.receive_certificate(certificate.clone()).await {
                    log::warn!("Received invalid certificate {hash} from {name}: {e}");
                    // Do not update the validator's tracker in case of error.
                    // Move on to the next validator.
                    continue 'outer;
                }
            }
            // Update tracker.
            self.received_certificate_trackers.insert(name, tracker);
        }
        Ok(())
    }

    /// Send money.
    async fn transfer(
        &mut self,
        amount: Amount,
        recipient: Address,
        user_data: UserData,
    ) -> Result<Certificate> {
        let balance = self.synchronize_balance().await?;
        ensure!(
            Balance::from(amount) <= balance,
            "Transferred amount ({}) is not backed by sufficient funds ({})",
            amount,
            balance
        );
        let block = Block {
            epoch: self.epoch().await?,
            chain_id: self.chain_id,
            incoming_messages: self.pending_messages().await?,
            operations: vec![Operation::Transfer {
                recipient,
                amount,
                user_data,
            }],
            height: self.next_block_height,
            previous_block_hash: self.block_hash,
        };
        let certificate = self
            .propose_block(block, /* with_confirmation */ true)
            .await?;
        Ok(certificate)
    }

    async fn process_certificate(&mut self, certificate: Certificate) -> Result<(), Error> {
        let info = self.node_client.handle_certificate(certificate).await?.info;
        if info.chain_id == self.chain_id
            && (info.next_block_height, info.manager.next_round())
                > (self.next_block_height, self.next_round)
        {
            self.block_hash = info.block_hash;
            self.next_block_height = info.next_block_height;
            self.next_round = info.manager.next_round();
        }
        Ok(())
    }

    /// Execute (or retry) a regular block proposal. Update local balance.
    /// If `with_confirmation` is false, we stop short of executing the finalized block.
    async fn propose_block(
        &mut self,
        block: Block,
        with_confirmation: bool,
    ) -> Result<Certificate> {
        let next_round = self.next_round;
        ensure!(
            matches!(&self.pending_block, None)
                || matches!(&self.pending_block, Some(r) if *r == block),
            "Client state has a different pending block",
        );
        ensure!(
            block.height == self.next_block_height,
            "Unexpected block height"
        );
        ensure!(
            block.previous_block_hash == self.block_hash,
            "Unexpected previous block hash"
        );
        // Remember what we are trying to do
        self.pending_block = Some(block.clone());
        // Build the initial query.
        let key_pair = self.key_pair().await?;
        let proposal = BlockProposal::new(
            BlockAndRound {
                block,
                round: next_round,
            },
            key_pair,
        );
        // Send the query.
        let committee = self.committee().await?;
        let final_certificate = match self.chain_info().await?.manager {
            ChainManager::Multi(_) => {
                // Need two-round trips.
                let certificate = self
                    .communicate_chain_updates(
                        &committee,
                        self.chain_id,
                        CommunicateAction::SubmitBlockForValidation(proposal.clone()),
                    )
                    .await?
                    .expect("a certificate");
                assert_eq!(
                    certificate.value.validated_block(),
                    Some(&proposal.content.block)
                );
                self.communicate_chain_updates(
                    &committee,
                    self.chain_id,
                    CommunicateAction::FinalizeBlock(certificate),
                )
                .await?
                .expect("a certificate")
            }
            ChainManager::Single(_) => {
                // Only one round-trip is needed
                self.communicate_chain_updates(
                    &committee,
                    self.chain_id,
                    CommunicateAction::SubmitBlockForConfirmation(proposal.clone()),
                )
                .await?
                .expect("a certificate")
            }
            ChainManager::None => unreachable!("chain is active"),
        };
        // By now the block should be final.
        ensure!(
            final_certificate.value.confirmed_block() == Some(&proposal.content.block),
            "A different operation was executed in parallel (consider retrying the operation)"
        );
        self.process_certificate(final_certificate.clone()).await?;
        self.pending_block = None;
        // Communicate the new certificate now if needed.
        if with_confirmation {
            self.communicate_chain_updates(
                &committee,
                self.chain_id,
                CommunicateAction::AdvanceToNextBlockHeight(self.next_block_height),
            )
            .await?;
            if let Ok(new_committee) = self.committee().await {
                if new_committee != committee {
                    // If the configuration just changed, communicate to the new committee as well.
                    // (This is actually more important that updating the previous committee.)
                    self.communicate_chain_updates(
                        &new_committee,
                        self.chain_id,
                        CommunicateAction::AdvanceToNextBlockHeight(self.next_block_height),
                    )
                    .await?;
                }
            }
        }
        Ok(final_certificate)
    }
}

#[async_trait]
impl<A, S> ChainClient for ChainClientState<A, S>
where
    A: ValidatorNode + Send + Sync + Clone + 'static,
    S: Storage + Clone + 'static,
{
    async fn local_balance(&mut self) -> Result<Balance> {
        ensure!(
            self.chain_info().await?.next_block_height == self.next_block_height,
            "The local node is behind and needs synchronization"
        );
        let block = Block {
            epoch: self.epoch().await?,
            chain_id: self.chain_id,
            incoming_messages: self.pending_messages().await?,
            operations: Vec::new(),
            previous_block_hash: self.block_hash,
            height: self.next_block_height,
        };
        Ok(self
            .node_client
            .stage_block_execution(&block)
            .await?
            .info
            .balance)
    }

    async fn transfer_to_chain(
        &mut self,
        amount: Amount,
        recipient: ChainId,
        user_data: UserData,
    ) -> Result<Certificate> {
        self.transfer(amount, Address::Account(recipient), user_data)
            .await
    }

    async fn burn(&mut self, amount: Amount, user_data: UserData) -> Result<Certificate> {
        self.transfer(amount, Address::Burn, user_data).await
    }

    async fn synchronize_balance(&mut self) -> Result<Balance> {
        self.find_received_certificates().await?;
        self.prepare_chain().await?;
        self.local_balance().await
    }

    async fn retry_pending_block(&mut self) -> Result<Option<Certificate>> {
        self.find_received_certificates().await?;
        self.prepare_chain().await?;
        match &self.pending_block {
            Some(block) => {
                // Finish executing the previous block.
                let block = block.clone();
                let certificate = self
                    .propose_block(block, /* with_confirmation */ true)
                    .await?;
                Ok(Some(certificate))
            }
            None => Ok(None),
        }
    }

    async fn clear_pending_block(&mut self) {
        self.pending_block = None;
    }

    async fn receive_certificate(&mut self, certificate: Certificate) -> Result<()> {
        let block = certificate
            .value
            .confirmed_block()
            .ok_or_else(|| anyhow!("Was expecting a confirmed chain operation"))?
            .clone();
        let state = self.execution_state().await?;
        if let Some(epoch) = state.epoch {
            if block.chain_id != state.admin_id()? {
                ensure!(
                    block.epoch <= epoch,
                    "Cannot accept a certificate from an unknown committee in the future. Please synchronize the local chain",
                );
                match state.committees.get(&block.epoch) {
                    Some(committee) => {
                        certificate.check(committee)?;
                    }
                    None => bail!("Cannot accept a certificate from a committee that was retired. Try a newer certificate from the same chain"),
                }
            }
        }
        // Recover history from the network.
        self.node_client
            .download_certificates(self.validator_clients.clone(), block.chain_id, block.height)
            .await?;
        // Process the received operation.
        self.process_certificate(certificate).await?;
        // Make sure a quorum of validators (according to our committee) are up-to-date
        // for data availability.
        let committee = self.committee().await?;
        self.communicate_chain_updates(
            &committee,
            block.chain_id,
            CommunicateAction::AdvanceToNextBlockHeight(block.height.try_add_one()?),
        )
        .await?;
        Ok(())
    }

    async fn rotate_key_pair(&mut self, key_pair: KeyPair) -> Result<Certificate> {
        self.prepare_chain().await?;
        let new_owner = Owner(key_pair.public());
        let block = Block {
            epoch: self.epoch().await?,
            chain_id: self.chain_id,
            incoming_messages: self.pending_messages().await?,
            operations: vec![Operation::ChangeOwner { new_owner }],
            previous_block_hash: self.block_hash,
            height: self.next_block_height,
        };
        self.known_key_pairs.insert(new_owner, key_pair);
        let certificate = self
            .propose_block(block, /* with_confirmation */ true)
            .await?;
        Ok(certificate)
    }

    async fn transfer_ownership(&mut self, new_owner: Owner) -> Result<Certificate> {
        self.prepare_chain().await?;
        let block = Block {
            epoch: self.epoch().await?,
            chain_id: self.chain_id,
            incoming_messages: self.pending_messages().await?,
            operations: vec![Operation::ChangeOwner { new_owner }],
            previous_block_hash: self.block_hash,
            height: self.next_block_height,
        };
        let certificate = self
            .propose_block(block, /* with_confirmation */ true)
            .await?;
        Ok(certificate)
    }

    async fn share_ownership(&mut self, new_owner: Owner) -> Result<Certificate> {
        self.prepare_chain().await?;
        let owner = self.identity().await?;
        let block = Block {
            epoch: self.epoch().await?,
            chain_id: self.chain_id,
            incoming_messages: self.pending_messages().await?,
            operations: vec![Operation::ChangeMultipleOwners {
                new_owners: vec![owner, new_owner],
            }],
            previous_block_hash: self.block_hash,
            height: self.next_block_height,
        };
        let certificate = self
            .propose_block(block, /* with_confirmation */ true)
            .await?;
        Ok(certificate)
    }

    async fn open_chain(&mut self, owner: Owner) -> Result<(ChainId, Certificate)> {
        self.prepare_chain().await?;
        let id = ChainId::child(EffectId {
            chain_id: self.chain_id,
            height: self.next_block_height,
            index: 0,
        });
        let state = self.execution_state().await?;
        let admin_id = state.admin_id()?;
        let committees = state.committees;
        let epoch = state.epoch.ok_or(Error::InactiveChain(self.chain_id))?;
        let block = Block {
            epoch,
            chain_id: self.chain_id,
            incoming_messages: self.pending_messages().await?,
            operations: vec![Operation::OpenChain {
                id,
                owner,
                committees,
                admin_id,
                epoch,
                next_admin_height: self.next_admin_height().await?,
            }],
            previous_block_hash: self.block_hash,
            height: self.next_block_height,
        };
        let certificate = self
            .propose_block(block, /* with_confirmation */ true)
            .await?;
        Ok((id, certificate))
    }

    async fn close_chain(&mut self) -> Result<Certificate> {
        self.prepare_chain().await?;
        let block = Block {
            epoch: self.epoch().await?,
            chain_id: self.chain_id,
            incoming_messages: self.pending_messages().await?,
            operations: vec![Operation::CloseChain],
            previous_block_hash: self.block_hash,
            height: self.next_block_height,
        };
        let certificate = self
            .propose_block(block, /* with_confirmation */ true)
            .await?;
        Ok(certificate)
    }

    async fn stage_new_voting_rights(
        &mut self,
        voting_rights: BTreeMap<ValidatorName, usize>,
    ) -> Result<Certificate> {
        self.prepare_chain().await?;
        let committee = Committee::new(voting_rights);
        let epoch = self.epoch().await?;
        let block = Block {
            epoch,
            chain_id: self.chain_id,
            incoming_messages: self.pending_messages().await?,
            operations: vec![Operation::CreateCommittee {
                admin_id: self.chain_id,
                epoch: epoch.try_add_one()?,
                committee,
            }],
            previous_block_hash: self.block_hash,
            height: self.next_block_height,
        };
        let certificate = self
            .propose_block(block, /* with_confirmation */ true)
            .await?;
        Ok(certificate)
    }

    async fn process_inbox(&mut self) -> Result<Certificate> {
        self.prepare_chain().await?;
        let block = Block {
            epoch: self.epoch().await?,
            chain_id: self.chain_id,
            incoming_messages: self.pending_messages().await?,
            operations: Vec::new(),
            previous_block_hash: self.block_hash,
            height: self.next_block_height,
        };
        let certificate = self
            .propose_block(block, /* with_confirmation */ true)
            .await?;
        Ok(certificate)
    }

    async fn subscribe_to_new_committees(&mut self) -> Result<Certificate> {
        self.prepare_chain().await?;
        let admin_id = self.execution_state().await?.admin_id()?;
        let block = Block {
            epoch: self.epoch().await?,
            chain_id: self.chain_id,
            incoming_messages: self.pending_messages().await?,
            operations: vec![Operation::SubscribeToNewCommittees {
                id: self.chain_id,
                admin_id,
                next_admin_height: self.next_admin_height().await?,
            }],
            previous_block_hash: self.block_hash,
            height: self.next_block_height,
        };
        let certificate = self
            .propose_block(block, /* with_confirmation */ true)
            .await?;
        Ok(certificate)
    }

    async fn finalize_voting_rights(&mut self) -> Result<Certificate> {
        self.prepare_chain().await?;
        let state = self.execution_state().await?;
        let admin_id = state.admin_id()?;
        let current_epoch = state.epoch.ok_or(Error::InactiveChain(self.chain_id))?;
        let operations = state
            .committees
            .keys()
            .filter_map(|epoch| {
                if *epoch != current_epoch {
                    Some(Operation::RemoveCommittee {
                        admin_id,
                        epoch: *epoch,
                    })
                } else {
                    None
                }
            })
            .collect();
        let block = Block {
            epoch: current_epoch,
            chain_id: self.chain_id,
            incoming_messages: self.pending_messages().await?,
            operations,
            previous_block_hash: self.block_hash,
            height: self.next_block_height,
        };
        let certificate = self
            .propose_block(block, /* with_confirmation */ true)
            .await?;
        Ok(certificate)
    }

    async fn transfer_to_chain_unsafe_unconfirmed(
        &mut self,
        amount: Amount,
        recipient: ChainId,
        user_data: UserData,
    ) -> Result<Certificate> {
        self.prepare_chain().await?;
        let block = Block {
            epoch: self.epoch().await?,
            chain_id: self.chain_id,
            incoming_messages: self.pending_messages().await?,
            operations: vec![Operation::Transfer {
                recipient: Address::Account(recipient),
                amount,
                user_data,
            }],
            previous_block_hash: self.block_hash,
            height: self.next_block_height,
        };
        let new_certificate = self
            .propose_block(block, /* with_confirmation */ false)
            .await?;
        Ok(new_certificate)
    }
}

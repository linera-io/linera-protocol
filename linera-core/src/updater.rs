// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::node::ValidatorNode;
use futures::{future, StreamExt};
use linera_base::{chain::ChainState, committee::Committee, error::Error, messages::*};
use linera_storage::Storage;
use std::{collections::HashMap, hash::Hash, time::Duration};

/// Used for `communicate_chain_updates`
#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
pub enum CommunicateAction {
    SubmitBlockForConfirmation(BlockProposal),
    SubmitBlockForValidation(BlockProposal),
    FinalizeBlock(Certificate),
    AdvanceToNextBlockHeight(BlockHeight),
}

pub struct ValidatorUpdater<A, S> {
    pub name: ValidatorName,
    pub client: A,
    pub store: S,
    pub delay: Duration,
    pub retries: usize,
}

/// Execute a sequence of actions in parallel for all validators.
/// Try to stop early when a quorum is reached.
pub async fn communicate_with_quorum<'a, A, V, K, F, G>(
    validator_clients: &'a [(ValidatorName, A)],
    committee: &Committee,
    group_by: G,
    execute: F,
) -> Result<(K, Vec<V>), Option<Error>>
where
    A: ValidatorNode + Send + Sync + 'static + Clone,
    F: Fn(ValidatorName, A) -> future::BoxFuture<'a, Result<V, Error>> + Clone,
    G: Fn(&V) -> K,
    K: Hash + PartialEq + Eq + Clone + 'static,
    V: 'static,
{
    let mut responses: futures::stream::FuturesUnordered<_> = validator_clients
        .iter()
        .filter_map(|(name, client)| {
            let client = client.clone();
            let execute = execute.clone();
            if committee.weight(name) > 0 {
                Some(async move { (*name, execute(*name, client).await) })
            } else {
                // This should not happen but better prevent it because certificates
                // are not allowed to include votes with weight 0.
                None
            }
        })
        .collect();

    let mut value_scores = HashMap::new();
    let mut error_scores = HashMap::new();
    while let Some((name, result)) = responses.next().await {
        match result {
            Ok(value) => {
                let key = group_by(&value);
                let entry = value_scores.entry(key.clone()).or_insert((0, Vec::new()));
                entry.0 += committee.weight(&name);
                entry.1.push(value);
                if entry.0 >= committee.quorum_threshold() {
                    // Success!
                    return Ok((key, std::mem::take(&mut entry.1)));
                }
            }
            Err(err) => {
                let entry = error_scores.entry(err.clone()).or_insert(0);
                *entry += committee.weight(&name);
                if *entry >= committee.validity_threshold() {
                    // At least one honest node returned this error.
                    // No quorum can be reached, so return early.
                    return Err(Some(err));
                }
            }
        }
    }

    // No specific error is available to report reliably.
    Err(None)
}

impl<A, S> ValidatorUpdater<A, S>
where
    A: ValidatorNode + Send + Sync + 'static + Clone,
    S: Storage + Clone + 'static,
{
    pub async fn send_certificate(
        &mut self,
        certificate: Certificate,
        retryable: bool,
    ) -> Result<ChainInfo, Error> {
        let mut count = 0;
        loop {
            match self.client.handle_certificate(certificate.clone()).await {
                Ok(response) => {
                    response.check(self.name)?;
                    // Succeed
                    return Ok(response.info);
                }
                Err(Error::InactiveChain(_)) if retryable && count < self.retries => {
                    // Retry
                    tokio::time::sleep(self.delay).await;
                    count += 1;
                    continue;
                }
                Err(e) => {
                    // Fail
                    return Err(e);
                }
            }
        }
    }

    pub async fn send_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfo, Error> {
        let mut count = 0;
        loop {
            match self.client.handle_block_proposal(proposal.clone()).await {
                Ok(response) => {
                    response.check(self.name)?;
                    // Succeed
                    return Ok(response.info);
                }
                Err(Error::InactiveChain(_)) if count < self.retries => {
                    // Retry
                    tokio::time::sleep(self.delay).await;
                    count += 1;
                    continue;
                }
                Err(e) => {
                    // Fail
                    return Err(e);
                }
            }
        }
    }

    pub async fn send_chain_information(
        &mut self,
        mut chain_id: ChainId,
        mut target_block_height: BlockHeight,
    ) -> Result<(), Error> {
        let mut jobs = Vec::new();
        loop {
            // Figure out which certificates this validator is missing.
            let query = ChainInfoQuery::new(chain_id);
            match self.client.handle_chain_info_query(query).await {
                Ok(response) if response.info.description.is_some() => {
                    response.check(self.name)?;
                    jobs.push((
                        chain_id,
                        response.info.next_block_height,
                        target_block_height,
                        false,
                    ));
                    break;
                }
                Ok(response) => {
                    response.check(self.name)?;
                    // Obtain the chain description from our local node.
                    let chain = self.store.read_chain_or_default(chain_id).await?;
                    match chain.description() {
                        Some(ChainDescription::Child(EffectId {
                            chain_id: parent_id,
                            height,
                            index: _,
                        })) => {
                            jobs.push((chain_id, BlockHeight::from(0), target_block_height, true));
                            chain_id = *parent_id;
                            target_block_height = height.try_add_one()?;
                        }
                        _ => {
                            return Err(Error::InactiveChain(chain_id));
                        }
                    }
                }
                Err(e) => return Err(e),
            }
        }
        for (chain_id, initial_block_height, target_block_height, retryable) in
            jobs.into_iter().rev()
        {
            // Obtain chain state.
            let chain = self.store.read_chain_or_default(chain_id).await?;
            // Send the requested certificates in order.
            for number in usize::from(initial_block_height)..usize::from(target_block_height) {
                let key = chain
                    .confirmed_key(number)
                    .expect("certificate should be known locally");
                let cert = self.store.read_certificate(key).await?;
                self.send_certificate(cert, retryable).await?;
            }
        }
        Ok(())
    }

    pub async fn send_chain_information_as_a_receiver(
        &mut self,
        chain_id: ChainId,
    ) -> Result<(), Error> {
        let chain = self.store.read_chain_or_default(chain_id).await?;
        for (origin, inbox) in chain.inboxes.iter() {
            self.send_chain_information(origin.sender(), inbox.next_height_to_receive)
                .await?;
        }
        Ok(())
    }

    pub async fn send_chain_update(
        &mut self,
        chain_id: ChainId,
        action: CommunicateAction,
    ) -> Result<Option<Vote>, Error> {
        let target_block_height = match &action {
            CommunicateAction::SubmitBlockForValidation(proposal)
            | CommunicateAction::SubmitBlockForConfirmation(proposal) => {
                proposal.content.block.height
            }
            CommunicateAction::FinalizeBlock(certificate) => {
                certificate.value.validated_block().unwrap().height
            }
            CommunicateAction::AdvanceToNextBlockHeight(seq) => *seq,
        };
        // Update the validator with missing information, if needed.
        self.send_chain_information(chain_id, target_block_height)
            .await?;
        // Send the block proposal (if any) and return a vote.
        match action {
            CommunicateAction::SubmitBlockForValidation(proposal)
            | CommunicateAction::SubmitBlockForConfirmation(proposal) => {
                let result = self.send_block_proposal(proposal.clone()).await;
                let info = match result {
                    Ok(info) => info,
                    Err(e) if ChainState::is_retriable_validation_error(&e) => {
                        // Some received certificates may be missing for this validator
                        // (e.g. to make the balance sufficient) so we are going to
                        // synchronize them now.
                        self.send_chain_information_as_a_receiver(chain_id).await?;
                        // Now retry the block.
                        self.send_block_proposal(proposal).await?
                    }
                    Err(e) => {
                        return Err(e);
                    }
                };
                match info.manager.pending() {
                    Some(vote) => {
                        vote.check(self.name)?;
                        return Ok(Some(vote.clone()));
                    }
                    None => return Err(Error::ClientErrorWhileProcessingBlockProposal),
                }
            }
            CommunicateAction::FinalizeBlock(certificate) => {
                // The only cause for a retry is that the first certificate of a newly opened chain.
                let retryable = target_block_height == BlockHeight::from(0);
                let info = self.send_certificate(certificate, retryable).await?;
                match info.manager.pending() {
                    Some(vote) => {
                        vote.check(self.name)?;
                        return Ok(Some(vote.clone()));
                    }
                    None => return Err(Error::ClientErrorWhileProcessingBlockProposal),
                }
            }
            CommunicateAction::AdvanceToNextBlockHeight(_) => (),
        }
        Ok(None)
    }
}

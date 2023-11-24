// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    data_types::{ChainInfo, ChainInfoQuery, ChainInfoResponse},
    node::{NodeError, ValidatorNode},
};
use futures::{future, StreamExt};
use linera_base::{
    data_types::{BlockHeight, Round},
    identifiers::{ChainDescription, ChainId, MessageId},
};
use linera_chain::data_types::{BlockProposal, Certificate, CertificateValue, LiteVote};
use linera_execution::committee::{Committee, Epoch, ValidatorName};
use linera_storage::Storage;
use linera_views::views::ViewError;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
    hash::Hash,
    ops::Range,
    time::{Duration, Instant},
};
use thiserror::Error;
use tracing::{error, info, warn};

/// The amount of time we wait for additional validators to contribute to the result, as a fraction
/// of how long it took to reach a quorum.
const GRACE_PERIOD: f64 = 0.2;
/// The maximum timeout for `communicate_with_quorum` if no quorum is reached.
const MAX_TIMEOUT: Duration = Duration::from_secs(60 * 60 * 24); // 1 day.

/// Used for `communicate_chain_updates`
#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
pub enum CommunicateAction {
    SubmitBlock(BlockProposal),
    FinalizeBlock(Certificate),
    AdvanceToNextBlockHeight(BlockHeight),
    RequestLeaderTimeout {
        height: BlockHeight,
        round: Round,
        epoch: Epoch,
    },
}

pub struct ValidatorUpdater<A, S> {
    pub name: ValidatorName,
    pub node: A,
    pub storage: S,
    pub delay: Duration,
    pub retries: usize,
}

/// An error result for [`communicate_with_quorum`].
#[derive(Error, Debug)]
pub enum CommunicationError<E: fmt::Debug> {
    /// A single error that was returned by a sufficient number of nodes to be trusted as
    /// valid.
    #[error("Failed to communicate with a quorum of validators: {0}")]
    Trusted(E),
    /// No single error reached the validity threshold so we're returning a sample of
    /// errors for debugging purposes.
    #[error("Failed to communicate with a quorum of validators:\n{:#?}", .0)]
    Sample(Vec<E>),
}

/// Executes a sequence of actions in parallel for all validators.
///
/// Tries to stop early when a quorum is reached. If `grace_period` is not zero, other validators
/// are given this much additional time to contribute to the result, as a fraction of how long it
/// took to reach the quorum.
pub async fn communicate_with_quorum<'a, A, V, K, F, G>(
    validator_clients: &'a [(ValidatorName, A)],
    committee: &Committee,
    group_by: G,
    execute: F,
) -> Result<(K, Vec<V>), CommunicationError<NodeError>>
where
    A: ValidatorNode + Send + Sync + 'static + Clone,
    F: Fn(ValidatorName, A) -> future::BoxFuture<'a, Result<V, NodeError>> + Clone,
    G: Fn(&V) -> K,
    K: Hash + PartialEq + Eq + Clone + 'static,
    V: 'static,
{
    let mut responses: futures::stream::FuturesUnordered<_> = validator_clients
        .iter()
        .filter_map(|(name, node)| {
            let node = node.clone();
            let execute = execute.clone();
            if committee.weight(name) > 0 {
                Some(async move { (*name, execute(*name, node).await) })
            } else {
                // This should not happen but better prevent it because certificates
                // are not allowed to include votes with weight 0.
                None
            }
        })
        .collect();

    let start_time = Instant::now();
    let mut end_time: Option<Instant> = None;
    let mut remaining_votes = committee.total_votes();
    let mut highest_key_score = 0;
    let mut value_scores = HashMap::new();
    let mut error_scores = HashMap::new();

    while let Ok(Some((name, result))) = tokio::time::timeout(
        end_time.map_or(MAX_TIMEOUT, |t| t.saturating_duration_since(Instant::now())),
        responses.next(),
    )
    .await
    {
        remaining_votes -= committee.weight(&name);
        match result {
            Ok(value) => {
                let key = group_by(&value);
                let entry = value_scores.entry(key.clone()).or_insert((0, Vec::new()));
                entry.0 += committee.weight(&name);
                entry.1.push(value);
                highest_key_score = highest_key_score.max(entry.0);
            }
            Err(err) => {
                let entry = error_scores.entry(err.clone()).or_insert(0);
                *entry += committee.weight(&name);
                if *entry >= committee.validity_threshold() {
                    // At least one honest node returned this error.
                    // No quorum can be reached, so return early.
                    return Err(CommunicationError::Trusted(err));
                }
            }
        }
        // If a key reaches a quorum or it becomes clear that no key can, wait for the grace
        // period to collect more values or error information and then stop.
        if end_time.is_none()
            && (highest_key_score >= committee.quorum_threshold()
                || highest_key_score + remaining_votes < committee.quorum_threshold())
        {
            end_time = Some(Instant::now() + start_time.elapsed().mul_f64(GRACE_PERIOD));
        }
    }

    // If a key has a quorum, return it with its values.
    if let Some((key, (_, values))) = value_scores
        .into_iter()
        .find(|(_, (score, _))| *score >= committee.quorum_threshold())
    {
        return Ok((key, values));
    }

    // No specific error is available to report reliably.
    let mut sample = error_scores.into_iter().collect::<Vec<_>>();
    sample.sort_by_key(|(_, score)| std::cmp::Reverse(*score));
    let sample = sample.into_iter().map(|(error, _)| error).take(4).collect();
    Err(CommunicationError::Sample(sample))
}

impl<A, S> ValidatorUpdater<A, S>
where
    A: ValidatorNode + Clone + Send + Sync + 'static,
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    async fn send_optimized_certificate(
        &mut self,
        certificate: &Certificate,
    ) -> Result<ChainInfoResponse, NodeError> {
        if certificate.is_signed_by(&self.name) {
            let result = self
                .node
                .handle_lite_certificate(certificate.lite_certificate())
                .await;
            match result {
                Err(NodeError::MissingCertificateValue) => {
                    warn!(
                        "Validator {} forgot a certificate value that they signed before",
                        self.name
                    );
                }
                _ => {
                    return result;
                }
            }
        }
        self.node
            .handle_certificate(certificate.clone(), vec![])
            .await
    }

    async fn send_certificate(&mut self, certificate: Certificate) -> Result<ChainInfo, NodeError> {
        let mut result = self.send_optimized_certificate(&certificate).await;

        if let Err(NodeError::ApplicationBytecodesNotFound(locations)) = &result {
            // Find the missing bytecodes locally and retry.
            let required = match certificate.value() {
                CertificateValue::ConfirmedBlock { executed_block, .. }
                | CertificateValue::ValidatedBlock { executed_block, .. } => {
                    executed_block.block.bytecode_locations()
                }
                CertificateValue::LeaderTimeout { .. } => HashMap::new(),
            };
            for location in locations {
                if !required.contains_key(location) {
                    let hash = location.certificate_hash;
                    warn!("validator requested {:?} but it is not required", hash);
                    return Err(NodeError::InvalidChainInfoResponse);
                }
            }
            let unique_locations = locations.iter().cloned().collect::<HashSet<_>>();
            if locations.len() > unique_locations.len() {
                warn!("locations requested by validator contain duplicates");
                return Err(NodeError::InvalidChainInfoResponse);
            }
            let blobs = future::join_all(
                unique_locations
                    .into_iter()
                    .map(|location| self.storage.read_value(location.certificate_hash)),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
            result = self
                .node
                .handle_certificate(certificate.clone(), blobs)
                .await;
        }

        let response = result?;
        response.check(self.name)?;
        // Succeed
        Ok(response.info)
    }

    async fn send_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfo, NodeError> {
        let chain_id = proposal.content.block.chain_id;
        let mut count = 0;
        let mut has_send_chain_information_for_senders = false;
        loop {
            match self.node.handle_block_proposal(proposal.clone()).await {
                Ok(response) => {
                    response.check(self.name)?;
                    // Succeed
                    return Ok(response.info);
                }
                Err(NodeError::MissingCrossChainUpdate { .. })
                | Err(NodeError::ApplicationBytecodesNotFound(_))
                    if !has_send_chain_information_for_senders =>
                {
                    // Some received certificates may be missing for this validator
                    // (e.g. to make the balance sufficient) so we are going to
                    // synchronize them now.
                    self.send_chain_information_for_senders(chain_id).await?;
                    has_send_chain_information_for_senders = true;
                }
                Err(NodeError::InactiveChain(_)) => {
                    if count < self.retries {
                        // `send_chain_information` is always called before
                        // `send_block_proposal` but in the case of new chains, it may
                        // take some time to receive the missing `OpenChain` message: let's
                        // retry.
                        tokio::time::sleep(self.delay).await;
                        count += 1;
                    } else {
                        return Err(NodeError::ProposedBlockToInactiveChain {
                            chain_id,
                            retries: self.retries,
                        });
                    }
                }
                Err(e @ NodeError::MissingCrossChainUpdate { .. }) => {
                    if count < self.retries {
                        // We just called `send_chain_information_for_senders` but it may
                        // take time to receive the missing messages: let's retry.
                        tokio::time::sleep(self.delay).await;
                        count += 1;
                    } else {
                        info!("Missing cross-chain updates: {:?}", e);
                        return Err(NodeError::ProposedBlockWithLaggingMessages {
                            chain_id,
                            retries: self.retries,
                        });
                    }
                }
                Err(NodeError::ApplicationBytecodesNotFound(_)) => {
                    if count < self.retries {
                        tokio::time::sleep(self.delay).await;
                        count += 1;
                    } else {
                        return Err(NodeError::ProposedBlockWithLaggingBytecode {
                            chain_id,
                            retries: self.retries,
                        });
                    }
                }
                Err(e) => {
                    // Fail
                    return Err(e);
                }
            }
        }
    }

    async fn send_chain_information(
        &mut self,
        mut chain_id: ChainId,
        mut target_block_height: BlockHeight,
    ) -> Result<(), NodeError> {
        let mut jobs = Vec::new();
        loop {
            // Figure out which certificates this validator is missing.
            let query = ChainInfoQuery::new(chain_id);
            match self.node.handle_chain_info_query(query).await {
                Ok(response) if response.info.description.is_some() => {
                    response.check(self.name)?;
                    jobs.push((
                        chain_id,
                        response.info.next_block_height,
                        target_block_height,
                    ));
                    break;
                }
                Ok(response) => {
                    response.check(self.name)?;
                    // Obtain the chain description from our local node.
                    let description = *self
                        .storage
                        .load_chain(chain_id)
                        .await?
                        .execution_state
                        .system
                        .description
                        .get();
                    match description {
                        Some(ChainDescription::Child(MessageId {
                            chain_id: parent_id,
                            height,
                            index: _,
                        })) => {
                            jobs.push((chain_id, BlockHeight::ZERO, target_block_height));
                            chain_id = parent_id;
                            target_block_height = height.try_add_one()?;
                        }
                        _ => {
                            return Err(NodeError::InactiveLocalChain(chain_id));
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to query validator {:?} for information about chain {:?}: {}",
                        self.name, chain_id, e
                    );
                    return Err(e);
                }
            }
        }
        for (chain_id, initial_block_height, target_block_height) in jobs.into_iter().rev() {
            // Obtain chain state.
            let range: Range<usize> =
                initial_block_height.try_into()?..target_block_height.try_into()?;
            if !range.is_empty() {
                let keys = {
                    let chain = self.storage.load_chain(chain_id).await?;
                    chain.confirmed_log.read(range).await?
                };
                // Send the requested certificates in order.
                let certs = self.storage.read_certificates(keys.into_iter()).await?;
                for cert in certs {
                    self.send_certificate(cert).await?;
                }
            }
        }
        let manager = self
            .storage
            .load_chain(chain_id)
            .await?
            .manager
            .get()
            .clone();
        if let Some(cert) = manager.locked {
            if cert.value().is_validated() && cert.value().chain_id() == chain_id {
                self.send_certificate(cert).await?;
            }
        }
        if let Some(cert) = manager.leader_timeout {
            if cert.value().is_timeout() && cert.value().chain_id() == chain_id {
                self.send_certificate(cert).await?;
            }
        }
        Ok(())
    }

    async fn send_chain_information_for_senders(
        &mut self,
        chain_id: ChainId,
    ) -> Result<(), NodeError> {
        let mut info = BTreeMap::new();
        {
            let chain = self.storage.load_chain(chain_id).await?;
            let origins = chain.inboxes.indices().await?;
            let inboxes = chain.inboxes.try_load_entries(&origins).await?;
            for (origin, inbox) in origins.into_iter().zip(inboxes) {
                let next_height = info.entry(origin.sender).or_default();
                let inbox_next_height = inbox.next_block_height_to_receive()?;
                if inbox_next_height > *next_height {
                    *next_height = inbox_next_height;
                }
            }
        }
        for (sender, next_height) in info {
            self.send_chain_information(sender, next_height).await?;
        }
        Ok(())
    }

    pub async fn send_chain_update(
        &mut self,
        chain_id: ChainId,
        action: CommunicateAction,
    ) -> Result<Option<LiteVote>, NodeError> {
        let target_block_height = match &action {
            CommunicateAction::SubmitBlock(proposal) => proposal.content.block.height,
            CommunicateAction::FinalizeBlock(certificate) => certificate.value().height(),
            CommunicateAction::AdvanceToNextBlockHeight(height) => *height,
            CommunicateAction::RequestLeaderTimeout { height, .. } => *height,
        };
        // Update the validator with missing information, if needed.
        self.send_chain_information(chain_id, target_block_height)
            .await?;
        // Send the block proposal (if any) and return a vote.
        match action {
            CommunicateAction::SubmitBlock(proposal) => {
                let info = self.send_block_proposal(proposal.clone()).await?;
                match info.manager.pending {
                    Some(vote) if vote.validator == self.name => {
                        vote.check()?;
                        return Ok(Some(vote.clone()));
                    }
                    Some(_) | None => {
                        return Err(NodeError::MissingVoteInValidatorResponse);
                    }
                }
            }
            CommunicateAction::FinalizeBlock(certificate) => {
                let info = self.send_certificate(certificate).await?;
                match info.manager.pending {
                    Some(vote) if vote.validator == self.name => {
                        vote.check()?;
                        return Ok(Some(vote.clone()));
                    }
                    Some(_) | None => {
                        return Err(NodeError::MissingVoteInValidatorResponse);
                    }
                }
            }
            CommunicateAction::RequestLeaderTimeout { .. } => {
                let query = ChainInfoQuery::new(chain_id).with_leader_timeout();
                let info = self.node.handle_chain_info_query(query).await?.info;
                match info.manager.timeout_vote {
                    Some(vote) if vote.validator == self.name => {
                        vote.check()?;
                        return Ok(Some(vote.clone()));
                    }
                    Some(_) | None => {
                        return Err(NodeError::MissingVoteInValidatorResponse);
                    }
                }
            }
            CommunicateAction::AdvanceToNextBlockHeight(_) => (),
        }
        Ok(None)
    }
}

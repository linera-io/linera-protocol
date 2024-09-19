// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    hash::Hash,
    ops::Range,
};

use futures::{Future, StreamExt};
use linera_base::{
    data_types::{BlockHeight, Round},
    ensure,
    identifiers::ChainId,
    time::{timer::timeout, Duration, Instant},
};
use linera_chain::data_types::{BlockProposal, Certificate, LiteVote};
use linera_execution::committee::{Committee, ValidatorName};
use linera_storage::Storage;
use thiserror::Error;
use tracing::{error, warn};

use crate::{
    data_types::{ChainInfo, ChainInfoQuery, ChainInfoResponse},
    local_node::LocalNodeClient,
    node::{CrossChainMessageDelivery, LocalValidatorNode, NodeError},
};

/// The amount of time we wait for additional validators to contribute to the result, as a fraction
/// of how long it took to reach a quorum.
const GRACE_PERIOD: f64 = 0.2;
/// The maximum timeout for `communicate_with_quorum` if no quorum is reached.
const MAX_TIMEOUT: Duration = Duration::from_secs(60 * 60 * 24); // 1 day.

/// Used for `communicate_chain_action`
#[derive(Clone)]
pub enum CommunicateAction {
    SubmitBlock {
        proposal: BlockProposal,
    },
    FinalizeBlock {
        certificate: Certificate,
        delivery: CrossChainMessageDelivery,
    },
    RequestTimeout {
        chain_id: ChainId,
        height: BlockHeight,
        round: Round,
    },
}

impl CommunicateAction {
    /// The round to which this action pertains.
    pub fn round(&self) -> Round {
        match self {
            CommunicateAction::SubmitBlock { proposal } => proposal.content.round,
            CommunicateAction::FinalizeBlock { certificate, .. } => certificate.round,
            CommunicateAction::RequestTimeout { round, .. } => *round,
        }
    }
}

pub struct ValidatorUpdater<A, S>
where
    S: Storage,
{
    pub name: ValidatorName,
    pub node: A,
    pub local_node: LocalNodeClient<S>,
}

/// An error result for [`communicate_with_quorum`].
#[derive(Error, Debug)]
pub enum CommunicationError<E: fmt::Debug> {
    /// No consensus is possible since validators returned different possibilities
    /// for the next block
    #[error("No error but failed to find a consensus block. Consensus threshold: {}, Proposals: {:?}", .0, .1)]
    NoConsensus(u64, Vec<(u64, usize)>),
    /// A single error that was returned by a sufficient number of nodes to be trusted as
    /// valid.
    #[error("Failed to communicate with a quorum of validators: {0}")]
    Trusted(E),
    /// No single error reached the validity threshold so we're returning a sample of
    /// errors for debugging purposes, together with their weight.
    #[error("Failed to communicate with a quorum of validators:\n{:#?}", .0)]
    Sample(Vec<(E, u64)>),
}

/// Executes a sequence of actions in parallel for all validators.
///
/// Tries to stop early when a quorum is reached. If `grace_period` is not zero, other validators
/// are given this much additional time to contribute to the result, as a fraction of how long it
/// took to reach the quorum.
pub async fn communicate_with_quorum<'a, A, V, K, F, R, G>(
    validator_clients: &'a [(ValidatorName, A)],
    committee: &Committee,
    group_by: G,
    execute: F,
) -> Result<(K, Vec<V>), CommunicationError<NodeError>>
where
    A: LocalValidatorNode + Clone + 'static,
    F: Clone + Fn(ValidatorName, A) -> R,
    R: Future<Output = Result<V, NodeError>> + 'a,
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

    while let Ok(Some((name, result))) = timeout(
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

    let scores = value_scores
        .values()
        .map(|(weight, values)| (*weight, values.len()))
        .collect();
    // If a key has a quorum, return it with its values.
    if let Some((key, (_, values))) = value_scores
        .into_iter()
        .find(|(_, (score, _))| *score >= committee.quorum_threshold())
    {
        return Ok((key, values));
    }

    if error_scores.is_empty() {
        return Err(CommunicationError::NoConsensus(
            committee.quorum_threshold(),
            scores,
        ));
    }

    // No specific error is available to report reliably.
    let mut sample = error_scores.into_iter().collect::<Vec<_>>();
    sample.sort_by_key(|(_, score)| std::cmp::Reverse(*score));
    sample.truncate(4);
    Err(CommunicationError::Sample(sample))
}

impl<A, S> ValidatorUpdater<A, S>
where
    A: LocalValidatorNode + Clone + 'static,
    S: Storage + Clone + Send + Sync + 'static,
{
    async fn send_optimized_certificate(
        &mut self,
        certificate: &Certificate,
        delivery: CrossChainMessageDelivery,
    ) -> Result<ChainInfoResponse, NodeError> {
        if certificate.is_signed_by(&self.name) {
            let result = self
                .node
                .handle_lite_certificate(certificate.lite_certificate(), delivery)
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
            .handle_certificate(certificate.clone(), vec![], delivery)
            .await
    }

    async fn send_certificate(
        &mut self,
        certificate: Certificate,
        delivery: CrossChainMessageDelivery,
    ) -> Result<Box<ChainInfo>, NodeError> {
        let result = self
            .send_optimized_certificate(&certificate, delivery)
            .await;

        let response = match &result {
            Err(original_err @ NodeError::BlobsNotFound(blob_ids)) => {
                let blobs = self
                    .local_node
                    .find_missing_blobs(&certificate, blob_ids, certificate.value().chain_id())
                    .await?;
                ensure!(blobs.len() == blob_ids.len(), original_err.clone());
                self.node
                    .handle_certificate(certificate, blobs, delivery)
                    .await
            }
            _ => result,
        }?;

        response.check(&self.name)?;
        // Succeed
        Ok(response.info)
    }

    async fn send_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<Box<ChainInfo>, NodeError> {
        let chain_id = proposal.content.block.chain_id;
        let response = match self.node.handle_block_proposal(proposal.clone()).await {
            Ok(response) => response,
            Err(NodeError::MissingCrossChainUpdate { .. }) | Err(NodeError::InactiveChain(_)) => {
                // Some received certificates may be missing for this validator
                // (e.g. to create the chain or make the balance sufficient) so we are going to
                // synchronize them now and retry.
                self.send_chain_information_for_senders(chain_id).await?;
                self.node.handle_block_proposal(proposal.clone()).await?
            }
            Err(NodeError::BlobNotFoundOnRead(blob_id)) => {
                // For `BlobNotFoundOnRead`, we assume that the local node should already be
                // updated with the needed blobs, so sending the chain information about the
                // certificate that last used the blob to the validator node should be enough.
                let local_storage = self.local_node.storage_client();
                let last_used_by_hash = local_storage.read_blob_state(blob_id).await?.last_used_by;
                let certificate = local_storage.read_certificate(last_used_by_hash).await?;

                let block_chain_id = certificate.value().chain_id();
                let block_height = certificate.value().height();
                self.send_chain_information(
                    block_chain_id,
                    block_height.try_add_one()?,
                    CrossChainMessageDelivery::Blocking,
                )
                .await?;

                self.node.handle_block_proposal(proposal.clone()).await?
            }
            Err(e) => {
                // Fail immediately on other errors.
                return Err(e);
            }
        };
        response.check(&self.name)?;
        Ok(response.info)
    }

    pub async fn send_chain_information(
        &mut self,
        chain_id: ChainId,
        target_block_height: BlockHeight,
        delivery: CrossChainMessageDelivery,
    ) -> Result<(), NodeError> {
        // Figure out which certificates this validator is missing.
        let query = ChainInfoQuery::new(chain_id);
        let initial_block_height = match self.node.handle_chain_info_query(query).await {
            Ok(response) => {
                response.check(&self.name)?;
                response.info.next_block_height
            }
            Err(error) => {
                error!(
                    name = ?self.name, ?chain_id, %error,
                    "Failed to query validator about missing blocks"
                );
                return Err(error);
            }
        };
        // Obtain the missing blocks and the manager state from the local node.
        let range: Range<usize> =
            initial_block_height.try_into()?..target_block_height.try_into()?;
        let (keys, manager) = {
            let chain = self.local_node.chain_state_view(chain_id).await?;
            (
                chain.confirmed_log.read(range).await?,
                chain.manager.get().clone(),
            )
        };
        if !keys.is_empty() {
            // Send the requested certificates in order.
            let storage = self.local_node.storage_client();
            let certs = storage.read_certificates(keys.into_iter()).await?;
            for cert in certs {
                self.send_certificate(cert, delivery).await?;
            }
        }
        if let Some(cert) = manager.timeout {
            if cert.value().is_timeout() && cert.value().chain_id() == chain_id {
                self.send_certificate(cert, CrossChainMessageDelivery::NonBlocking)
                    .await?;
            }
        }
        Ok(())
    }

    async fn send_chain_information_for_senders(
        &mut self,
        chain_id: ChainId,
    ) -> Result<(), NodeError> {
        let mut sender_heights = BTreeMap::new();
        {
            let chain = self.local_node.chain_state_view(chain_id).await?;
            let pairs = chain.inboxes.try_load_all_entries().await?;
            for (origin, inbox) in pairs {
                let next_height = sender_heights.entry(origin.sender).or_default();
                let inbox_next_height = inbox.next_block_height_to_receive()?;
                if inbox_next_height > *next_height {
                    *next_height = inbox_next_height;
                }
            }
        }
        for (sender, next_height) in sender_heights {
            self.send_chain_information(sender, next_height, CrossChainMessageDelivery::Blocking)
                .await?;
        }
        Ok(())
    }

    pub async fn send_chain_update(
        &mut self,
        action: CommunicateAction,
    ) -> Result<LiteVote, NodeError> {
        let (target_block_height, chain_id) = match &action {
            CommunicateAction::SubmitBlock { proposal } => {
                let block = &proposal.content.block;
                (block.height, block.chain_id)
            }
            CommunicateAction::FinalizeBlock { certificate, .. } => {
                let value = certificate.value();
                (value.height(), value.chain_id())
            }
            CommunicateAction::RequestTimeout {
                height, chain_id, ..
            } => (*height, *chain_id),
        };
        // Update the validator with missing information, if needed.
        let delivery = CrossChainMessageDelivery::NonBlocking;
        self.send_chain_information(chain_id, target_block_height, delivery)
            .await?;
        // Send the block proposal, certificate or timeout request and return a vote.
        let vote = match action {
            CommunicateAction::SubmitBlock { proposal } => {
                let info = self.send_block_proposal(proposal.clone()).await?;
                info.manager.pending
            }
            CommunicateAction::FinalizeBlock {
                certificate,
                delivery,
            } => {
                let info = self.send_certificate(certificate, delivery).await?;
                info.manager.pending
            }
            CommunicateAction::RequestTimeout { .. } => {
                let query = ChainInfoQuery::new(chain_id).with_timeout();
                let info = self.node.handle_chain_info_query(query).await?.info;
                info.manager.timeout_vote
            }
        };
        match vote {
            Some(vote) if vote.validator == self.name => {
                vote.check()?;
                Ok(vote)
            }
            Some(_) | None => Err(NodeError::MissingVoteInValidatorResponse),
        }
    }
}

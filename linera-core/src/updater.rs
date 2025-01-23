// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
    hash::Hash,
    ops::Range,
};

use futures::{stream, stream::TryStreamExt, Future, StreamExt};
use linera_base::{
    data_types::{BlockHeight, Round},
    identifiers::{BlobId, ChainId},
    time::{timer::timeout, Duration, Instant},
};
use linera_chain::{
    data_types::{BlockProposal, LiteVote},
    types::{ConfirmedBlock, GenericCertificate, ValidatedBlock, ValidatedBlockCertificate},
};
use linera_execution::committee::Committee;
use linera_storage::Storage;
use thiserror::Error;

use crate::{
    client::ChainClientError,
    data_types::{ChainInfo, ChainInfoQuery},
    local_node::LocalNodeClient,
    node::{CrossChainMessageDelivery, NodeError, ValidatorNode},
    remote_node::RemoteNode,
};

/// The default amount of time we wait for additional validators to contribute
/// to the result, as a fraction of how long it took to reach a quorum.
pub const DEFAULT_GRACE_PERIOD: f64 = 0.2;
/// The maximum timeout for requests to a stake-weighted quorum if no quorum is reached.
const MAX_TIMEOUT: Duration = Duration::from_secs(60 * 60 * 24); // 1 day.

/// Used for `communicate_chain_action`
#[derive(Clone)]
pub enum CommunicateAction {
    SubmitBlock {
        proposal: Box<BlockProposal>,
        blob_ids: HashSet<BlobId>,
    },
    FinalizeBlock {
        certificate: ValidatedBlockCertificate,
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
            CommunicateAction::SubmitBlock { proposal, .. } => proposal.content.round,
            CommunicateAction::FinalizeBlock { certificate, .. } => certificate.round,
            CommunicateAction::RequestTimeout { round, .. } => *round,
        }
    }
}

#[derive(Clone)]
pub struct ValidatorUpdater<A, S>
where
    S: Storage,
{
    pub chain_worker_count: usize,
    pub remote_node: RemoteNode<A>,
    pub local_node: LocalNodeClient<S>,
}

/// An error result for requests to a stake-weighted quorum.
#[derive(Error, Debug)]
pub enum CommunicationError<E: fmt::Debug> {
    /// No consensus is possible since validators returned different possibilities
    /// for the next block
    #[error(
        "No error but failed to find a consensus block. Consensus threshold: {0}, Proposals: {1:?}"
    )]
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
/// Tries to stop early when a quorum is reached. If `grace_period` is specified, other validators
/// are given additional time to contribute to the result. The grace period is calculated as a fraction
/// (defaulting to `DEFAULT_GRACE_PERIOD`) of the time taken to reach quorum.
pub async fn communicate_with_quorum<'a, A, V, K, F, R, G>(
    validator_clients: &'a [RemoteNode<A>],
    committee: &Committee,
    group_by: G,
    execute: F,
    // Grace period as a fraction of time taken to reach quorum
    grace_period: f64,
) -> Result<(K, Vec<V>), CommunicationError<NodeError>>
where
    A: ValidatorNode + Clone + 'static,
    F: Clone + Fn(RemoteNode<A>) -> R,
    R: Future<Output = Result<V, ChainClientError>> + 'a,
    G: Fn(&V) -> K,
    K: Hash + PartialEq + Eq + Clone + 'static,
    V: 'static,
{
    let mut responses: futures::stream::FuturesUnordered<_> = validator_clients
        .iter()
        .filter_map(|remote_node| {
            if committee.weight(&remote_node.name) == 0 {
                // This should not happen but better prevent it because certificates
                // are not allowed to include votes with weight 0.
                return None;
            }
            let execute = execute.clone();
            let remote_node = remote_node.clone();
            Some(async move { (remote_node.name, execute(remote_node).await) })
        })
        .collect();

    let start_time = Instant::now();
    let mut end_time: Option<Instant> = None;
    let mut remaining_votes = committee.total_votes();
    let mut highest_key_score = 0;
    let mut value_scores = HashMap::new();
    let mut error_scores = HashMap::new();

    'vote_wait: while let Ok(Some((name, result))) = timeout(
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
                // TODO(#2857): Handle non-remote errors properly.
                let err = match err {
                    ChainClientError::RemoteNodeError(err) => err,
                    err => NodeError::ResponseHandlingError {
                        error: err.to_string(),
                    },
                };
                let entry = error_scores.entry(err.clone()).or_insert(0);
                *entry += committee.weight(&name);
                if *entry >= committee.validity_threshold() {
                    // At least one honest node returned this error.
                    // No quorum can be reached, so return early.
                    return Err(CommunicationError::Trusted(err));
                }
            }
        }
        // If it becomes clear that no key can reach a quorum, break early.
        if highest_key_score + remaining_votes < committee.quorum_threshold() {
            break 'vote_wait;
        }

        // If a key reaches a quorum, wait for the grace period to collect more values
        // or error information and then stop.
        if end_time.is_none() && highest_key_score >= committee.quorum_threshold() {
            end_time = Some(Instant::now() + start_time.elapsed().mul_f64(grace_period));
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
    A: ValidatorNode + Clone + 'static,
    S: Storage + Clone + Send + Sync + 'static,
{
    async fn send_confirmed_certificate(
        &mut self,
        certificate: GenericCertificate<ConfirmedBlock>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<Box<ChainInfo>, ChainClientError> {
        let result = self
            .remote_node
            .handle_optimized_confirmed_certificate(&certificate, delivery)
            .await;

        Ok(match &result {
            Err(original_err @ NodeError::BlobsNotFound(blob_ids)) => {
                self.remote_node
                    .check_blobs_not_found(&certificate, blob_ids)?;
                // The certificate is confirmed, so the blobs must be in storage.
                let maybe_blobs = self.local_node.read_blobs_from_storage(blob_ids).await?;
                let blobs = maybe_blobs.ok_or_else(|| original_err.clone())?;
                self.remote_node.upload_blobs(blobs.clone()).await?;
                self.remote_node
                    .handle_confirmed_certificate(certificate, delivery)
                    .await
            }
            _ => result,
        }?)
    }

    async fn send_validated_certificate(
        &mut self,
        certificate: GenericCertificate<ValidatedBlock>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<Box<ChainInfo>, ChainClientError> {
        let result = self
            .remote_node
            .handle_optimized_validated_certificate(&certificate, delivery)
            .await;

        Ok(match &result {
            Err(original_err @ NodeError::BlobsNotFound(blob_ids)) => {
                self.remote_node
                    .check_blobs_not_found(&certificate, blob_ids)?;
                let chain_id = certificate.inner().chain_id();
                // The certificate is for a validated block, i.e. for our locked block.
                // Take the missing blobs from our local chain manager.
                let blobs = self
                    .local_node
                    .get_locked_blobs(blob_ids, chain_id)
                    .await?
                    .ok_or_else(|| original_err.clone())?;
                self.remote_node.send_pending_blobs(chain_id, blobs).await?;
                self.remote_node
                    .handle_validated_certificate(certificate)
                    .await
            }
            _ => result,
        }?)
    }

    async fn send_block_proposal(
        &mut self,
        proposal: Box<BlockProposal>,
        mut blob_ids: HashSet<BlobId>,
    ) -> Result<Box<ChainInfo>, ChainClientError> {
        let chain_id = proposal.content.block.chain_id;
        let mut sent_cross_chain_updates = false;
        for blob in &proposal.blobs {
            blob_ids.remove(&blob.id()); // Keep only blobs we may need to resend.
        }
        loop {
            match self
                .remote_node
                .handle_block_proposal(proposal.clone())
                .await
            {
                Ok(info) => return Ok(info),
                Err(NodeError::MissingCrossChainUpdate { .. })
                | Err(NodeError::InactiveChain(_))
                    if !sent_cross_chain_updates =>
                {
                    sent_cross_chain_updates = true;
                    // Some received certificates may be missing for this validator
                    // (e.g. to create the chain or make the balance sufficient) so we are going to
                    // synchronize them now and retry.
                    self.send_chain_information_for_senders(chain_id).await?;
                }
                Err(NodeError::BlobsNotFound(_)) if !blob_ids.is_empty() => {
                    // For `BlobsNotFound`, we assume that the local node should already be
                    // updated with the needed blobs, so sending the chain information about the
                    // certificates that last used the blobs to the validator node should be enough.
                    let blob_ids = blob_ids.drain().collect::<Vec<_>>();
                    let missing_blob_ids = self.remote_node.node.missing_blob_ids(blob_ids).await?;
                    let local_storage = self.local_node.storage_client();
                    let blob_states = local_storage.read_blob_states(&missing_blob_ids).await?;
                    let mut chain_heights = BTreeMap::new();
                    for blob_state in blob_states {
                        let block_chain_id = blob_state.chain_id;
                        let block_height = blob_state.block_height.try_add_one()?;
                        chain_heights
                            .entry(block_chain_id)
                            .and_modify(|h| *h = block_height.max(*h))
                            .or_insert(block_height);
                    }

                    self.send_chain_info_up_to_heights(
                        chain_heights,
                        CrossChainMessageDelivery::NonBlocking,
                    )
                    .await?;
                }
                // Fail immediately on other errors.
                Err(err) => return Err(err.into()),
            }
        }
    }

    pub async fn send_chain_information(
        &mut self,
        chain_id: ChainId,
        target_block_height: BlockHeight,
        delivery: CrossChainMessageDelivery,
    ) -> Result<(), ChainClientError> {
        // Figure out which certificates this validator is missing.
        let query = ChainInfoQuery::new(chain_id);
        let remote_info = self.remote_node.handle_chain_info_query(query).await?;
        let initial_block_height = remote_info.next_block_height;
        // Obtain the missing blocks and the manager state from the local node.
        let range: Range<usize> =
            initial_block_height.try_into()?..target_block_height.try_into()?;
        let (keys, timeout) = {
            let chain = self.local_node.chain_state_view(chain_id).await?;
            (
                chain.confirmed_log.read(range).await?,
                chain.manager.timeout.get().clone(),
            )
        };
        if !keys.is_empty() {
            // Send the requested certificates in order.
            let storage = self.local_node.storage_client();
            let certs = storage.read_certificates(keys.into_iter()).await?;
            for cert in certs {
                self.send_confirmed_certificate(cert, delivery).await?;
            }
        }
        if let Some(cert) = timeout {
            if cert.inner().chain_id == chain_id {
                // Timeouts are small and don't have blobs, so we can call `handle_certificate`
                // directly.
                self.remote_node.handle_timeout_certificate(cert).await?;
            }
        }
        Ok(())
    }

    async fn send_chain_info_up_to_heights(
        &mut self,
        chain_heights: BTreeMap<ChainId, BlockHeight>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<(), ChainClientError> {
        let stream = stream::iter(chain_heights)
            .map(|(chain_id, height)| {
                let mut updater = self.clone();
                async move {
                    updater
                        .send_chain_information(chain_id, height, delivery)
                        .await
                }
            })
            .buffer_unordered(self.chain_worker_count);
        stream.try_collect::<Vec<_>>().await?;
        Ok(())
    }

    async fn send_chain_information_for_senders(
        &mut self,
        chain_id: ChainId,
    ) -> Result<(), ChainClientError> {
        let mut sender_heights = BTreeMap::new();
        {
            let chain = self.local_node.chain_state_view(chain_id).await?;
            let pairs = chain.inboxes.try_load_all_entries().await?;
            for (origin, inbox) in pairs {
                let inbox_next_height = inbox.next_block_height_to_receive()?;
                sender_heights
                    .entry(origin.sender)
                    .and_modify(|h| *h = inbox_next_height.max(*h))
                    .or_insert(inbox_next_height);
            }
        }

        self.send_chain_info_up_to_heights(sender_heights, CrossChainMessageDelivery::Blocking)
            .await?;
        Ok(())
    }

    pub async fn send_chain_update(
        &mut self,
        action: CommunicateAction,
    ) -> Result<LiteVote, ChainClientError> {
        let (target_block_height, chain_id) = match &action {
            CommunicateAction::SubmitBlock { proposal, .. } => {
                let block = &proposal.content.block;
                (block.height, block.chain_id)
            }
            CommunicateAction::FinalizeBlock { certificate, .. } => (
                certificate.inner().block().header.height,
                certificate.inner().block().header.chain_id,
            ),
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
            CommunicateAction::SubmitBlock { proposal, blob_ids } => {
                let info = self.send_block_proposal(proposal, blob_ids).await?;
                info.manager.pending
            }
            CommunicateAction::FinalizeBlock {
                certificate,
                delivery,
            } => {
                let info = self
                    .send_validated_certificate(certificate, delivery)
                    .await?;
                info.manager.pending
            }
            CommunicateAction::RequestTimeout { .. } => {
                let query = ChainInfoQuery::new(chain_id).with_timeout();
                let info = self.remote_node.handle_chain_info_query(query).await?;
                info.manager.timeout_vote
            }
        };
        match vote {
            Some(vote) if vote.validator == self.remote_node.name => {
                vote.check()?;
                Ok(vote)
            }
            Some(_) | None => Err(NodeError::MissingVoteInValidatorResponse.into()),
        }
    }
}

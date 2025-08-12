// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    hash::Hash,
    mem,
};

use futures::{
    stream::{FuturesUnordered, TryStreamExt},
    Future, StreamExt,
};
use linera_base::{
    crypto::ValidatorPublicKey,
    data_types::{BlockHeight, Round},
    ensure,
    identifiers::{BlobId, ChainId, GenericApplicationId},
    time::{timer::timeout, Duration, Instant},
};
use linera_chain::{
    data_types::{BlockProposal, LiteVote},
    types::{ConfirmedBlock, GenericCertificate, ValidatedBlock, ValidatedBlockCertificate},
};
use linera_execution::committee::Committee;
use linera_storage::{ResultReadCertificates, Storage};
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
        blob_ids: Vec<BlobId>,
    },
    FinalizeBlock {
        certificate: Box<ValidatedBlockCertificate>,
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
) -> Result<(K, Vec<(ValidatorPublicKey, V)>), CommunicationError<NodeError>>
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
            if committee.weight(&remote_node.public_key) == 0 {
                // This should not happen but better prevent it because certificates
                // are not allowed to include votes with weight 0.
                return None;
            }
            let execute = execute.clone();
            let remote_node = remote_node.clone();
            Some(async move { (remote_node.public_key, execute(remote_node).await) })
        })
        .collect();

    let start_time = Instant::now();
    let mut end_time: Option<Instant> = None;
    let mut remaining_votes = committee.total_votes();
    let mut highest_key_score = 0;
    let mut value_scores: HashMap<K, (u64, Vec<(ValidatorPublicKey, V)>)> = HashMap::new();
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
                entry.1.push((name, value));
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
                self.remote_node.node.upload_blobs(blobs.clone()).await?;
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
                // The certificate is for a validated block, i.e. for our locking block.
                // Take the missing blobs from our local chain manager.
                let blobs = self
                    .local_node
                    .get_locking_blobs(blob_ids, chain_id)
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
        mut blob_ids: Vec<BlobId>,
    ) -> Result<Box<ChainInfo>, ChainClientError> {
        let chain_id = proposal.content.block.chain_id;
        let mut sent_cross_chain_updates = false;
        let mut publisher_chain_ids_sent = BTreeSet::new();
        loop {
            match self
                .remote_node
                .handle_block_proposal(proposal.clone())
                .await
            {
                Ok(info) => return Ok(info),
                Err(NodeError::WrongRound(_round)) => {
                    // The proposal is for a different round, so we need to update the validator.
                    // TODO: this should probably be more specific as to which rounds are retried.
                    self.send_chain_information(
                        chain_id,
                        proposal.content.block.height,
                        CrossChainMessageDelivery::NonBlocking,
                    )
                    .await?;
                }
                Err(NodeError::UnexpectedBlockHeight {
                    expected_block_height,
                    found_block_height,
                }) if expected_block_height < found_block_height => {
                    // The proposal is for a later block height, so we need to update the validator.
                    self.send_chain_information(
                        chain_id,
                        found_block_height,
                        CrossChainMessageDelivery::NonBlocking,
                    )
                    .await?;
                }
                Err(NodeError::MissingCrossChainUpdate { .. }) if !sent_cross_chain_updates => {
                    sent_cross_chain_updates = true;
                    // Some received certificates may be missing for this validator
                    // (e.g. to create the chain or make the balance sufficient) so we are going to
                    // synchronize them now and retry.
                    self.send_chain_information_for_senders(chain_id).await?;
                }
                Err(NodeError::EventsNotFound(event_ids)) => {
                    let mut publisher_heights = BTreeMap::new();
                    let new_chain_ids = event_ids
                        .iter()
                        .map(|event_id| event_id.chain_id)
                        .filter(|chain_id| !publisher_chain_ids_sent.contains(chain_id))
                        .collect::<BTreeSet<_>>();
                    ensure!(
                        !new_chain_ids.is_empty(),
                        NodeError::EventsNotFound(event_ids)
                    );
                    for chain_id in new_chain_ids {
                        let info = self.local_node.chain_info(chain_id).await?;
                        publisher_heights.insert(chain_id, info.next_block_height);
                        publisher_chain_ids_sent.insert(chain_id);
                    }
                    self.send_chain_info_up_to_heights(
                        publisher_heights,
                        CrossChainMessageDelivery::NonBlocking,
                    )
                    .await?;
                }
                Err(NodeError::BlobsNotFound(_) | NodeError::InactiveChain(_))
                    if !blob_ids.is_empty() =>
                {
                    // For `BlobsNotFound`, we assume that the local node should already be
                    // updated with the needed blobs, so sending the chain information about the
                    // certificates that last used the blobs to the validator node should be enough.
                    let published_blob_ids =
                        BTreeSet::from_iter(proposal.content.block.published_blob_ids());
                    blob_ids.retain(|blob_id| !published_blob_ids.contains(blob_id));
                    let mut published_blobs = Vec::new();
                    {
                        let chain = self.local_node.chain_state_view(chain_id).await?;
                        for blob_id in published_blob_ids {
                            published_blobs
                                .extend(chain.manager.proposed_blobs.get(&blob_id).await?);
                        }
                    }
                    self.remote_node
                        .send_pending_blobs(chain_id, published_blobs)
                        .await?;
                    let missing_blob_ids = self
                        .remote_node
                        .node
                        .missing_blob_ids(mem::take(&mut blob_ids))
                        .await?;
                    let blob_states = self
                        .local_node
                        .read_blob_states_from_storage(&missing_blob_ids)
                        .await?;
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
        // Figure out which certificates this validator is missing. In many cases, it's just the
        // last one, so we optimistically send that one right away.
        let remote_info = if let Ok(height) = target_block_height.try_sub_one() {
            let chain = self.local_node.chain_state_view(chain_id).await?;
            if let Some(hash) = chain
                .block_hashes(height..target_block_height)
                .await?
                .first()
            {
                let certificate = self
                    .local_node
                    .storage_client()
                    .read_certificate(*hash)
                    .await?
                    .ok_or_else(|| ChainClientError::MissingConfirmedBlock(*hash))?;
                match self.send_confirmed_certificate(certificate, delivery).await {
                    Err(ChainClientError::RemoteNodeError(NodeError::EventsNotFound(
                        event_ids,
                    ))) if event_ids.iter().all(|event_id| {
                        event_id.stream_id.application_id == GenericApplicationId::System
                    }) =>
                    {
                        // The chain is missing epoch events. Send all blocks.
                        let query = ChainInfoQuery::new(chain_id);
                        self.remote_node.handle_chain_info_query(query).await?
                    }
                    Err(err) => return Err(err),
                    Ok(info) => info,
                }
            } else {
                // We don't have the block at the specified height. Send all blocks.
                tracing::warn!(
                    "send_chain_information called with height {target_block_height},
                    but {chain_id:.8} does not have that block"
                );
                let query = ChainInfoQuery::new(chain_id);
                self.remote_node.handle_chain_info_query(query).await?
            }
        } else {
            let query = ChainInfoQuery::new(chain_id);
            self.remote_node.handle_chain_info_query(query).await?
        };
        let initial_block_height = remote_info.next_block_height;
        // Obtain the missing blocks and the manager state from the local node.
        let range = initial_block_height..target_block_height;
        let keys = {
            let chain = self.local_node.chain_state_view(chain_id).await?;
            chain.block_hashes(range).await?
        };
        if !keys.is_empty() {
            // Send the requested certificates in order.
            let storage = self.local_node.storage_client();
            let certificates = storage.read_certificates(keys.clone()).await?;
            let certificates = match ResultReadCertificates::new(certificates, keys) {
                ResultReadCertificates::Certificates(certificates) => certificates,
                ResultReadCertificates::InvalidHashes(hashes) => {
                    return Err(ChainClientError::ReadCertificatesError(hashes))
                }
            };
            for certificate in certificates {
                self.send_confirmed_certificate(certificate, delivery)
                    .await?;
            }
        }
        // If the remote node is missing a timeout certificate, send it as well.
        let local_info = self.local_node.chain_info(chain_id).await?;
        if let Some(cert) = local_info.manager.timeout {
            if (local_info.next_block_height, cert.round)
                >= (
                    remote_info.next_block_height,
                    remote_info.manager.current_round,
                )
            {
                self.remote_node.handle_timeout_certificate(*cert).await?;
            }
        }
        Ok(())
    }

    async fn send_chain_info_up_to_heights(
        &mut self,
        chain_heights: BTreeMap<ChainId, BlockHeight>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<(), ChainClientError> {
        let stream =
            FuturesUnordered::from_iter(chain_heights.into_iter().map(|(chain_id, height)| {
                let mut updater = self.clone();
                async move {
                    updater
                        .send_chain_information(chain_id, height, delivery)
                        .await
                }
            }));
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
                    .entry(origin)
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
        let chain_id = match &action {
            CommunicateAction::SubmitBlock { proposal, .. } => proposal.content.block.chain_id,
            CommunicateAction::FinalizeBlock { certificate, .. } => {
                certificate.inner().block().header.chain_id
            }
            CommunicateAction::RequestTimeout { chain_id, .. } => *chain_id,
        };
        // Send the block proposal, certificate or timeout request and return a vote.
        let vote = match action {
            CommunicateAction::SubmitBlock { proposal, blob_ids } => {
                let info = self.send_block_proposal(proposal, blob_ids).await?;
                info.manager.pending.ok_or_else(|| {
                    NodeError::MissingVoteInValidatorResponse("submit a block proposal".into())
                })?
            }
            CommunicateAction::FinalizeBlock {
                certificate,
                delivery,
            } => {
                let info = self
                    .send_validated_certificate(*certificate, delivery)
                    .await?;
                info.manager.pending.ok_or_else(|| {
                    NodeError::MissingVoteInValidatorResponse("finalize a block".into())
                })?
            }
            CommunicateAction::RequestTimeout { round, height, .. } => {
                let query = ChainInfoQuery::new(chain_id).with_timeout(height, round);
                let info = self.remote_node.handle_chain_info_query(query).await?;
                info.manager.timeout_vote.ok_or_else(|| {
                    NodeError::MissingVoteInValidatorResponse("request a timeout".into())
                })?
            }
        };
        vote.check(self.remote_node.public_key)?;
        Ok(vote)
    }
}

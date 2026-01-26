// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    hash::Hash,
    mem,
    sync::Arc,
};

use futures::{
    stream::{FuturesUnordered, TryStreamExt},
    Future, StreamExt,
};
use linera_base::{
    crypto::ValidatorPublicKey,
    data_types::{BlockHeight, Round, TimeDelta},
    ensure,
    identifiers::{BlobId, BlobType, ChainId, StreamId},
    time::{timer::timeout, Duration, Instant},
};
use linera_chain::{
    data_types::{BlockProposal, LiteVote},
    manager::LockingBlock,
    types::{ConfirmedBlock, GenericCertificate, ValidatedBlock, ValidatedBlockCertificate},
};
use linera_execution::{committee::Committee, system::EPOCH_STREAM_NAME};
use linera_storage::{Clock, Storage};
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{instrument, Level};

use crate::{
    client::{chain_client, Client},
    data_types::{ChainInfo, ChainInfoQuery},
    environment::Environment,
    node::{CrossChainMessageDelivery, NodeError, ValidatorNode},
    remote_node::RemoteNode,
    LocalNodeError,
};

/// The default amount of time we wait for additional validators to contribute
/// to the result, as a fraction of how long it took to reach a quorum.
pub const DEFAULT_QUORUM_GRACE_PERIOD: f64 = 0.2;

/// A report of clock skew from a validator, sent before retrying due to `InvalidTimestamp`.
pub type ClockSkewReport = (ValidatorPublicKey, TimeDelta);
/// The maximum timeout for requests to a stake-weighted quorum if no quorum is reached.
const MAX_TIMEOUT: Duration = Duration::from_secs(60 * 60 * 24); // 1 day.

/// Used for `communicate_chain_action`
#[derive(Clone)]
pub enum CommunicateAction {
    SubmitBlock {
        proposal: Box<BlockProposal>,
        blob_ids: Vec<BlobId>,
        /// Channel to report clock skew before sleeping, so the caller can aggregate reports.
        clock_skew_sender: mpsc::UnboundedSender<ClockSkewReport>,
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

pub struct ValidatorUpdater<Env>
where
    Env: Environment,
{
    pub remote_node: RemoteNode<Env::ValidatorNode>,
    pub client: Arc<Client<Env>>,
    pub admin_chain_id: ChainId,
}

impl<Env: Environment> Clone for ValidatorUpdater<Env> {
    fn clone(&self) -> Self {
        ValidatorUpdater {
            remote_node: self.remote_node.clone(),
            client: self.client.clone(),
            admin_chain_id: self.admin_chain_id,
        }
    }
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
/// Tries to stop early when a quorum is reached. If `quorum_grace_period` is specified, other
/// validators are given additional time to contribute to the result. The grace period is
/// calculated as a fraction (defaulting to `DEFAULT_QUORUM_GRACE_PERIOD`) of the time taken to
/// reach quorum.
pub async fn communicate_with_quorum<'a, A, V, K, F, R, G>(
    validator_clients: &'a [RemoteNode<A>],
    committee: &Committee,
    group_by: G,
    execute: F,
    // Grace period as a fraction of time taken to reach quorum.
    quorum_grace_period: f64,
) -> Result<(K, Vec<(ValidatorPublicKey, V)>), CommunicationError<NodeError>>
where
    A: ValidatorNode + Clone + 'static,
    F: Clone + Fn(RemoteNode<A>) -> R,
    R: Future<Output = Result<V, chain_client::Error>> + 'a,
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
                    chain_client::Error::RemoteNodeError(err) => err,
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
            end_time = Some(Instant::now() + start_time.elapsed().mul_f64(quorum_grace_period));
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

impl<Env> ValidatorUpdater<Env>
where
    Env: Environment + 'static,
{
    #[instrument(
        level = "trace", skip_all, err(level = Level::WARN),
        fields(chain_id = %certificate.block().header.chain_id)
    )]
    async fn send_confirmed_certificate(
        &mut self,
        certificate: GenericCertificate<ConfirmedBlock>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<Box<ChainInfo>, chain_client::Error> {
        let mut result = self
            .remote_node
            .handle_optimized_confirmed_certificate(&certificate, delivery)
            .await;

        let mut sent_admin_chain = false;
        let mut sent_blobs = false;
        loop {
            match result {
                Err(NodeError::EventsNotFound(event_ids))
                    if !sent_admin_chain
                        && certificate.inner().chain_id() != self.admin_chain_id
                        && event_ids.iter().all(|event_id| {
                            event_id.stream_id == StreamId::system(EPOCH_STREAM_NAME)
                                && event_id.chain_id == self.admin_chain_id
                        }) =>
                {
                    // The validator doesn't have the committee that signed the certificate.
                    self.update_admin_chain().await?;
                    sent_admin_chain = true;
                }
                Err(NodeError::BlobsNotFound(blob_ids)) if !sent_blobs => {
                    // The validator is missing the blobs required by the certificate.
                    self.remote_node
                        .check_blobs_not_found(&certificate, &blob_ids)?;
                    // The certificate is confirmed, so the blobs must be in storage.
                    let maybe_blobs = self
                        .client
                        .local_node
                        .read_blobs_from_storage(&blob_ids)
                        .await?;
                    let blobs = maybe_blobs.ok_or(NodeError::BlobsNotFound(blob_ids))?;
                    self.remote_node.node.upload_blobs(blobs).await?;
                    sent_blobs = true;
                }
                result => return Ok(result?),
            }
            result = self
                .remote_node
                .handle_confirmed_certificate(certificate.clone(), delivery)
                .await;
        }
    }

    async fn send_validated_certificate(
        &mut self,
        certificate: GenericCertificate<ValidatedBlock>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<Box<ChainInfo>, chain_client::Error> {
        let result = self
            .remote_node
            .handle_optimized_validated_certificate(&certificate, delivery)
            .await;

        let chain_id = certificate.inner().chain_id();
        match &result {
            Err(original_err @ NodeError::BlobsNotFound(blob_ids)) => {
                self.remote_node
                    .check_blobs_not_found(&certificate, blob_ids)?;
                // The certificate is for a validated block, i.e. for our locking block.
                // Take the missing blobs from our local chain manager.
                let blobs = self
                    .client
                    .local_node
                    .get_locking_blobs(blob_ids, chain_id)
                    .await?
                    .ok_or_else(|| original_err.clone())?;
                self.remote_node.send_pending_blobs(chain_id, blobs).await?;
            }
            Err(error) => {
                self.sync_if_needed(
                    chain_id,
                    certificate.round,
                    certificate.block().header.height,
                    error,
                )
                .await?;
            }
            _ => return Ok(result?),
        }
        Ok(self
            .remote_node
            .handle_validated_certificate(certificate)
            .await?)
    }

    /// Requests a vote for a timeout certificate for the given round from the remote node.
    ///
    /// If the remote node is not in that round or at that height yet, sends the chain information
    /// to update it.
    async fn request_timeout(
        &mut self,
        chain_id: ChainId,
        round: Round,
        height: BlockHeight,
    ) -> Result<Box<ChainInfo>, chain_client::Error> {
        let query = ChainInfoQuery::new(chain_id).with_timeout(height, round);
        let result = self
            .remote_node
            .handle_chain_info_query(query.clone())
            .await;
        if let Err(err) = &result {
            self.sync_if_needed(chain_id, round, height, err).await?;
        }
        Ok(result?)
    }

    /// Synchronizes either the local node or the remote node, if one of them is lagging behind.
    async fn sync_if_needed(
        &mut self,
        chain_id: ChainId,
        round: Round,
        height: BlockHeight,
        error: &NodeError,
    ) -> Result<(), chain_client::Error> {
        let address = &self.remote_node.address();
        match error {
            NodeError::WrongRound(validator_round) if *validator_round > round => {
                tracing::debug!(
                    address, %chain_id, %validator_round, %round,
                    "validator is at a higher round; synchronizing",
                );
                self.client
                    .synchronize_chain_state_from(&self.remote_node, chain_id)
                    .await?;
            }
            NodeError::UnexpectedBlockHeight {
                expected_block_height,
                found_block_height,
            } if expected_block_height > found_block_height => {
                tracing::debug!(
                    address,
                    %chain_id,
                    %expected_block_height,
                    %found_block_height,
                    "validator is at a higher height; synchronizing",
                );
                self.client
                    .synchronize_chain_state_from(&self.remote_node, chain_id)
                    .await?;
            }
            NodeError::WrongRound(validator_round) if *validator_round < round => {
                tracing::debug!(
                    address, %chain_id, %validator_round, %round,
                    "validator is at a lower round; sending chain info",
                );
                self.send_chain_information(
                    chain_id,
                    height,
                    CrossChainMessageDelivery::NonBlocking,
                    None,
                )
                .await?;
            }
            NodeError::UnexpectedBlockHeight {
                expected_block_height,
                found_block_height,
            } if expected_block_height < found_block_height => {
                tracing::debug!(
                    address,
                    %chain_id,
                    %expected_block_height,
                    %found_block_height,
                    "Validator is at a lower height; sending chain info.",
                );
                self.send_chain_information(
                    chain_id,
                    height,
                    CrossChainMessageDelivery::NonBlocking,
                    None,
                )
                .await?;
            }
            NodeError::InactiveChain(chain_id) => {
                tracing::debug!(
                    address,
                    %chain_id,
                    "Validator has inactive chain; sending chain info.",
                );
                self.send_chain_information(
                    *chain_id,
                    height,
                    CrossChainMessageDelivery::NonBlocking,
                    None,
                )
                .await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn send_block_proposal(
        &mut self,
        proposal: Box<BlockProposal>,
        mut blob_ids: Vec<BlobId>,
        clock_skew_sender: mpsc::UnboundedSender<ClockSkewReport>,
    ) -> Result<Box<ChainInfo>, chain_client::Error> {
        let chain_id = proposal.content.block.chain_id;
        let mut sent_cross_chain_updates = BTreeMap::new();
        let mut publisher_chain_ids_sent = BTreeSet::new();
        let storage = self.client.local_node.storage_client();
        loop {
            let local_time = storage.clock().current_time();
            match self
                .remote_node
                .handle_block_proposal(proposal.clone())
                .await
            {
                Ok(info) => return Ok(info),
                Err(NodeError::WrongRound(_round)) => {
                    // The proposal is for a different round, so we need to update the validator.
                    // TODO: this should probably be more specific as to which rounds are retried.
                    tracing::debug!(
                        remote_node = self.remote_node.address(),
                        %chain_id,
                        "wrong round; sending chain to validator",
                    );
                    self.send_chain_information(
                        chain_id,
                        proposal.content.block.height,
                        CrossChainMessageDelivery::NonBlocking,
                        None,
                    )
                    .await?;
                }
                Err(NodeError::UnexpectedBlockHeight {
                    expected_block_height,
                    found_block_height,
                }) if expected_block_height < found_block_height
                    && found_block_height == proposal.content.block.height =>
                {
                    tracing::debug!(
                        remote_node = self.remote_node.address(),
                        %chain_id,
                        "wrong height; sending chain to validator",
                    );
                    // The proposal is for a later block height, so we need to update the validator.
                    self.send_chain_information(
                        chain_id,
                        found_block_height,
                        CrossChainMessageDelivery::NonBlocking,
                        None,
                    )
                    .await?;
                }
                Err(NodeError::MissingCrossChainUpdate {
                    chain_id,
                    origin,
                    height,
                }) if chain_id == proposal.content.block.chain_id
                    && sent_cross_chain_updates
                        .get(&origin)
                        .is_none_or(|h| *h < height) =>
                {
                    tracing::debug!(
                        remote_node = %self.remote_node.address(),
                        chain_id = %origin,
                        "Missing cross-chain update; sending chain to validator.",
                    );
                    sent_cross_chain_updates.insert(origin, height);
                    // Some received certificates may be missing for this validator
                    // (e.g. to create the chain or make the balance sufficient) so we are going to
                    // synchronize them now and retry.
                    self.send_chain_information(
                        origin,
                        height.try_add_one()?,
                        CrossChainMessageDelivery::Blocking,
                        None,
                    )
                    .await?;
                }
                Err(NodeError::EventsNotFound(event_ids)) => {
                    let mut publisher_heights = BTreeMap::new();
                    let chain_ids = event_ids
                        .iter()
                        .map(|event_id| event_id.chain_id)
                        .filter(|chain_id| !publisher_chain_ids_sent.contains(chain_id))
                        .collect::<BTreeSet<_>>();
                    tracing::debug!(
                        remote_node = self.remote_node.address(),
                        ?chain_ids,
                        "missing events; sending chains to validator",
                    );
                    ensure!(!chain_ids.is_empty(), NodeError::EventsNotFound(event_ids));
                    for chain_id in chain_ids {
                        let height = self
                            .client
                            .local_node
                            .get_next_height_to_preprocess(chain_id)
                            .await?;
                        publisher_heights.insert(chain_id, height);
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
                    tracing::debug!("Missing blobs");
                    // For `BlobsNotFound`, we assume that the local node should already be
                    // updated with the needed blobs, so sending the chain information about the
                    // certificates that last used the blobs to the validator node should be enough.
                    let published_blob_ids =
                        BTreeSet::from_iter(proposal.content.block.published_blob_ids());
                    blob_ids.retain(|blob_id| !published_blob_ids.contains(blob_id));
                    let published_blobs = self
                        .client
                        .local_node
                        .get_proposed_blobs(chain_id, published_blob_ids.into_iter().collect())
                        .await?;
                    self.remote_node
                        .send_pending_blobs(chain_id, published_blobs)
                        .await?;
                    let missing_blob_ids = self
                        .remote_node
                        .node
                        .missing_blob_ids(mem::take(&mut blob_ids))
                        .await?;

                    tracing::debug!("Sending chains for missing blobs");
                    self.send_chain_info_for_blobs(
                        &missing_blob_ids,
                        CrossChainMessageDelivery::NonBlocking,
                    )
                    .await?;
                }
                Err(NodeError::InvalidTimestamp {
                    block_timestamp,
                    local_time: validator_local_time,
                    ..
                }) => {
                    // The validator's clock is behind the block's timestamp. We need to
                    // wait for two things:
                    // 1. Our clock to reach block_timestamp (in case the block timestamp
                    //    is in the future from our perspective too).
                    // 2. The validator's clock to catch up (in case of clock skew between
                    //    us and the validator).
                    let clock_skew = local_time.delta_since(validator_local_time);
                    tracing::debug!(
                        remote_node = self.remote_node.address(),
                        %chain_id,
                        %block_timestamp,
                        ?clock_skew,
                        "validator's clock is behind; waiting and retrying",
                    );
                    // Report the clock skew before sleeping so the caller can aggregate.
                    // Receiver may have been dropped if the caller is no longer interested.
                    clock_skew_sender
                        .send((self.remote_node.public_key, clock_skew))
                        .ok();
                    storage
                        .clock()
                        .sleep_until(block_timestamp.saturating_add(clock_skew))
                        .await;
                }
                // Fail immediately on other errors.
                Err(err) => return Err(err.into()),
            }
        }
    }

    async fn update_admin_chain(&mut self) -> Result<(), chain_client::Error> {
        let local_admin_info = self
            .client
            .local_node
            .chain_info(self.admin_chain_id)
            .await?;
        Box::pin(self.send_chain_information(
            self.admin_chain_id,
            local_admin_info.next_block_height,
            CrossChainMessageDelivery::NonBlocking,
            None,
        ))
        .await
    }

    /// Sends chain information to bring a validator up to date with a specific chain.
    ///
    /// This method performs a two-phase synchronization:
    /// 1. **Height synchronization**: Ensures the validator has all blocks up to `target_block_height`.
    /// 2. **Round synchronization**: If heights match, ensures the validator has proposals/certificates
    ///    for the current consensus round.
    ///
    /// # Height Sync Strategy
    /// - For existing chains (target_block_height > 0):
    ///   * Optimistically sends the last certificate first (often that's all that's missing).
    ///   * Falls back to full chain query if the validator needs more context.
    ///   * Sends any additional missing certificates in order.
    /// - For new chains (target_block_height == 0):
    ///   * Sends the chain description and dependencies first.
    ///   * Then queries the validator's state.
    ///
    /// # Round Sync Strategy
    /// Once heights match, if the local node is at a higher round, sends the evidence
    /// (proposal, validated block, or timeout certificate) that proves the current round.
    ///
    /// # Parameters
    /// - `chain_id`: The chain to synchronize
    /// - `target_block_height`: The height the validator should reach
    /// - `delivery`: Message delivery mode (blocking or non-blocking)
    /// - `latest_certificate`: Optional certificate at target_block_height - 1 to avoid a storage lookup
    ///
    /// # Returns
    /// - `Ok(())` if synchronization completed successfully or the validator is already up to date
    /// - `Err` if there was a communication or storage error
    #[instrument(level = "trace", skip_all)]
    pub async fn send_chain_information(
        &mut self,
        chain_id: ChainId,
        target_block_height: BlockHeight,
        delivery: CrossChainMessageDelivery,
        latest_certificate: Option<GenericCertificate<ConfirmedBlock>>,
    ) -> Result<(), chain_client::Error> {
        // Phase 1: Height synchronization
        let info = if target_block_height.0 > 0 {
            self.sync_chain_height(chain_id, target_block_height, delivery, latest_certificate)
                .await?
        } else {
            self.initialize_new_chain_on_validator(chain_id).await?
        };

        // Phase 2: Round synchronization (if needed)
        // Height synchronization is complete. Now check if we need to synchronize
        // the consensus round at this height.
        let (remote_height, remote_round) = (info.next_block_height, info.manager.current_round);
        let query = ChainInfoQuery::new(chain_id).with_manager_values();
        let local_info = match self.client.local_node.handle_chain_info_query(query).await {
            Ok(response) => response.info,
            // If we don't have the full chain description locally, we can't help the
            // validator with round synchronization. This is not an error - the validator
            // should retry later once the chain is fully initialized locally.
            Err(LocalNodeError::BlobsNotFound(_)) => {
                tracing::debug!("local chain description not fully available, skipping round sync");
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };

        let manager = local_info.manager;
        if local_info.next_block_height != remote_height || manager.current_round <= remote_round {
            return Ok(());
        }

        // Validator is at our height but behind on consensus round
        self.sync_consensus_round(remote_round, &manager).await
    }

    /// Synchronizes a validator to a specific block height by sending missing certificates.
    ///
    /// Uses an optimistic approach: sends the last certificate first, then fills in any gaps.
    ///
    /// Returns the [`ChainInfo`] from the validator after synchronization.
    async fn sync_chain_height(
        &mut self,
        chain_id: ChainId,
        target_block_height: BlockHeight,
        delivery: CrossChainMessageDelivery,
        latest_certificate: Option<GenericCertificate<ConfirmedBlock>>,
    ) -> Result<Box<ChainInfo>, chain_client::Error> {
        let height = target_block_height.try_sub_one()?;

        // Get the certificate for the last block we want to send
        let certificate = if let Some(cert) = latest_certificate {
            cert
        } else {
            self.read_certificates_for_heights(chain_id, vec![height])
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    chain_client::Error::InternalError(
                        "failed to read latest certificate for height sync",
                    )
                })?
        };

        // Optimistically try sending just the last certificate
        let info = match self.send_confirmed_certificate(certificate, delivery).await {
            Ok(info) => info,
            Err(error) => {
                tracing::debug!(
                    address = self.remote_node.address(), %error,
                    "validator failed to handle confirmed certificate; sending whole chain",
                );
                let query = ChainInfoQuery::new(chain_id);
                self.remote_node.handle_chain_info_query(query).await?
            }
        };

        // Calculate which block heights the validator is still missing
        let heights: Vec<_> = (info.next_block_height.0..target_block_height.0)
            .map(BlockHeight)
            .collect();

        if heights.is_empty() {
            return Ok(info);
        }

        // Send any additional missing certificates in order
        let certificates = self
            .read_certificates_for_heights(chain_id, heights)
            .await?;

        for certificate in certificates {
            self.send_confirmed_certificate(certificate, delivery)
                .await?;
        }

        Ok(info)
    }

    /// Reads certificates for the given heights from storage.
    async fn read_certificates_for_heights(
        &self,
        chain_id: ChainId,
        heights: Vec<BlockHeight>,
    ) -> Result<Vec<GenericCertificate<ConfirmedBlock>>, chain_client::Error> {
        let storage = self.client.local_node.storage_client();

        let certificates_by_height = storage
            .read_certificates_by_heights(chain_id, &heights)
            .await?;

        Ok(certificates_by_height.into_iter().flatten().collect())
    }

    /// Initializes a new chain on the validator by sending the chain description and dependencies.
    ///
    /// This is called when the validator doesn't know about the chain yet.
    ///
    /// Returns the [`ChainInfo`] from the validator after initialization.
    async fn initialize_new_chain_on_validator(
        &mut self,
        chain_id: ChainId,
    ) -> Result<Box<ChainInfo>, chain_client::Error> {
        // Send chain description and all dependency chains
        self.send_chain_info_for_blobs(
            &[BlobId::new(chain_id.0, BlobType::ChainDescription)],
            CrossChainMessageDelivery::NonBlocking,
        )
        .await?;

        // Query the validator's state for this chain
        let query = ChainInfoQuery::new(chain_id);
        let info = self.remote_node.handle_chain_info_query(query).await?;
        Ok(info)
    }

    /// Synchronizes the consensus round state with the validator.
    ///
    /// If the validator is at the same height but an earlier round, sends the evidence
    /// (proposal, validated block, or timeout certificate) that justifies the current round.
    ///
    /// This is a best-effort operation - failures are logged but don't fail the entire sync.
    async fn sync_consensus_round(
        &mut self,
        remote_round: Round,
        manager: &linera_chain::manager::ChainManagerInfo,
    ) -> Result<(), chain_client::Error> {
        // Try to send a proposal for the current round
        for proposal in manager
            .requested_proposed
            .iter()
            .chain(manager.requested_signed_proposal.iter())
        {
            if proposal.content.round == manager.current_round {
                match self
                    .remote_node
                    .handle_block_proposal(proposal.clone())
                    .await
                {
                    Ok(_) => {
                        tracing::debug!("successfully sent block proposal for round sync");
                        return Ok(());
                    }
                    Err(error) => {
                        tracing::debug!(%error, "failed to send block proposal");
                    }
                }
            }
        }

        // Try to send a validated block for the current round
        if let Some(LockingBlock::Regular(validated)) = manager.requested_locking.as_deref() {
            if validated.round == manager.current_round {
                match self
                    .remote_node
                    .handle_optimized_validated_certificate(
                        validated,
                        CrossChainMessageDelivery::NonBlocking,
                    )
                    .await
                {
                    Ok(_) => {
                        tracing::debug!("successfully sent validated block for round sync");
                        return Ok(());
                    }
                    Err(error) => {
                        tracing::debug!(%error, "failed to send validated block");
                    }
                }
            }
        }

        // Try to send a timeout certificate
        if let Some(cert) = &manager.timeout {
            if cert.round >= remote_round {
                match self
                    .remote_node
                    .handle_timeout_certificate(cert.as_ref().clone())
                    .await
                {
                    Ok(_) => {
                        tracing::debug!(round = %cert.round, "successfully sent timeout certificate");
                        return Ok(());
                    }
                    Err(error) => {
                        tracing::debug!(%error, round = %cert.round, "failed to send timeout certificate");
                    }
                }
            }
        }

        // If we reach here, either we had no round sync data to send, or all attempts failed.
        // This is not a fatal error - height sync succeeded which is the primary goal.
        tracing::debug!("round sync not performed: no applicable data or all attempts failed");
        Ok(())
    }

    /// Sends chain information for all chains referenced by the given blobs.
    ///
    /// Reads blob states from storage, determines the specific chain heights needed,
    /// and sends chain information for those heights. With sparse chains, this only
    /// sends the specific blocks containing the blobs, not all blocks up to those heights.
    async fn send_chain_info_for_blobs(
        &mut self,
        blob_ids: &[BlobId],
        delivery: CrossChainMessageDelivery,
    ) -> Result<(), chain_client::Error> {
        let blob_states = self
            .client
            .local_node
            .read_blob_states_from_storage(blob_ids)
            .await?;

        let mut chain_heights: BTreeMap<ChainId, BTreeSet<BlockHeight>> = BTreeMap::new();
        for blob_state in blob_states {
            let block_chain_id = blob_state.chain_id;
            let block_height = blob_state.block_height;
            chain_heights
                .entry(block_chain_id)
                .or_default()
                .insert(block_height);
        }

        self.send_chain_info_at_heights(chain_heights, delivery)
            .await
    }

    /// Sends chain information for specific heights on multiple chains.
    ///
    /// Unlike `send_chain_info_up_to_heights`, this method only sends the blocks at the
    /// specified heights, not all blocks up to those heights. This is more efficient for
    /// sparse chains where only specific blocks are needed.
    async fn send_chain_info_at_heights(
        &mut self,
        chain_heights: impl IntoIterator<Item = (ChainId, BTreeSet<BlockHeight>)>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<(), chain_client::Error> {
        FuturesUnordered::from_iter(chain_heights.into_iter().map(|(chain_id, heights)| {
            let mut updater = self.clone();
            async move {
                // Get all block hashes for this chain at the specified heights in one call
                let heights_vec: Vec<_> = heights.into_iter().collect();
                let certificates = updater
                    .client
                    .local_node
                    .storage_client()
                    .read_certificates_by_heights(chain_id, &heights_vec)
                    .await?
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>();

                // Send each certificate
                for certificate in certificates {
                    updater
                        .send_confirmed_certificate(certificate, delivery)
                        .await?;
                }

                Ok::<_, chain_client::Error>(())
            }
        }))
        .try_collect::<Vec<_>>()
        .await?;
        Ok(())
    }

    async fn send_chain_info_up_to_heights(
        &mut self,
        chain_heights: impl IntoIterator<Item = (ChainId, BlockHeight)>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<(), chain_client::Error> {
        FuturesUnordered::from_iter(chain_heights.into_iter().map(|(chain_id, height)| {
            let mut updater = self.clone();
            async move {
                updater
                    .send_chain_information(chain_id, height, delivery, None)
                    .await
            }
        }))
        .try_collect::<Vec<_>>()
        .await?;
        Ok(())
    }

    pub async fn send_chain_update(
        &mut self,
        action: CommunicateAction,
    ) -> Result<LiteVote, chain_client::Error> {
        let chain_id = match &action {
            CommunicateAction::SubmitBlock { proposal, .. } => proposal.content.block.chain_id,
            CommunicateAction::FinalizeBlock { certificate, .. } => {
                certificate.inner().block().header.chain_id
            }
            CommunicateAction::RequestTimeout { chain_id, .. } => *chain_id,
        };
        // Send the block proposal, certificate or timeout request and return a vote.
        let vote = match action {
            CommunicateAction::SubmitBlock {
                proposal,
                blob_ids,
                clock_skew_sender,
            } => {
                let info = self
                    .send_block_proposal(proposal, blob_ids, clock_skew_sender)
                    .await?;
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
                let info = self.request_timeout(chain_id, round, height).await?;
                info.manager.timeout_vote.ok_or_else(|| {
                    NodeError::MissingVoteInValidatorResponse("request a timeout".into())
                })?
            }
        };
        vote.check(self.remote_node.public_key)?;
        Ok(vote)
    }
}

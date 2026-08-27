// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Telling one validator about a chain, from storage and a payload the caller holds.
//!
//! Everything here needs a [`Storage`] and whatever the caller is trying to send, and nothing
//! else. That is what separates it from [`RemoteNodeUpdater`](crate::updater::RemoteNodeUpdater),
//! which additionally holds a local node and can therefore speak for a chain's consensus state as
//! well as its history.
//!
//! Two repairs a validator may need fall outside that: pushing a consensus round we are ahead on,
//! and knowing which of a publisher chain's blocks we would preprocess. Both read a local chain
//! manager, so they are reported to the caller as [`ProposalOutcome::NeedsLocalRepair`] rather
//! than attempted here. A caller with a local node applies them and asks again; one without --
//! a block exporter, or a proposer delegate -- gives up, which is the correct answer for both.

use std::collections::{BTreeMap, BTreeSet};

use linera_base::{
    crypto::ValidatorPublicKey,
    data_types::{Blob, BlockHeight, Round, TimeDelta},
    ensure,
    identifiers::{BlobId, ChainId},
};
use linera_chain::{
    data_types::BlockProposal,
    types::{ConfirmedBlockCertificate, ValidatedBlockCertificate},
};
use linera_execution::BlobOrigin;
use linera_storage::{Arc as CacheArc, Clock as _, Storage};
use tokio::sync::mpsc;
use tracing::debug;

use crate::{
    client::chain_client,
    data_types::{ChainInfo, ChainInfoQuery},
    node::{CrossChainMessageDelivery, NodeError, ValidatorNode},
    remote_node::RemoteNode,
};

/// Takes the given blobs from what the caller handed us.
///
/// A block that is only proposed or validated is durable nowhere, so there is no storage to fall
/// back on: whoever is offering the block is the only source for its blobs.
fn held_blobs(blob_ids: &BTreeSet<BlobId>, held: &[Blob]) -> Result<Vec<Blob>, NodeError> {
    let mut blobs = Vec::with_capacity(blob_ids.len());
    for blob_id in blob_ids {
        let blob = held
            .iter()
            .find(|blob| blob.id() == *blob_id)
            .ok_or_else(|| NodeError::BlobsNotFound(vec![*blob_id]))?;
        blobs.push(blob.clone());
    }
    Ok(blobs)
}

/// A validator's clock skew relative to ours, as observed while offering it a block.
pub type ClockSkewReport = (ValidatorPublicKey, TimeDelta);

/// What came of offering a proposal or a validated certificate to a validator.
pub(crate) enum ProposalOutcome {
    /// The validator accepted it, and its answer carries the vote.
    Accepted(Box<ChainInfo>),
    /// The validator needs something only a local chain manager can supply: it is ahead of us, or
    /// it rejected the block over consensus state we do not hold, or it is missing a publisher
    /// chain's events. A caller with a local node repairs this and asks again; one without gives
    /// up, which is the right answer when there is nothing of ours to push.
    NeedsLocalRepair(NodeError),
}

/// The one-shot guards for the repairs a proposal upload may attempt.
///
/// Held by the caller so a repair stays one-shot across the calls made either side of a local
/// repair: a validator that keeps reporting the same cause is not making progress.
#[derive(Default)]
pub(crate) struct ProposalRepairs {
    sent_blobs: bool,
    synced_cross_chain_updates: bool,
    synced_round_and_height: bool,
    /// Set by a caller that has pushed its own consensus round state once. Unused here; it lives
    /// with the other guards so one attempt has one place to look.
    pub(crate) synced_consensus_round: bool,
}

/// Tells one validator about a chain, from storage and what the caller hands it.
///
/// It holds no local node, which is what lets three different callers share it: a block exporter,
/// which has none to hold; a client, which has one but does not need it for most of this; and a
/// proposer delegate, which deliberately does without. For the exporter that also keeps the
/// export path off the chain workers, whose TTLs it would otherwise reset.
///
/// Committed blocks and their blobs are durable before anything here sees them
/// (`write_blobs_and_certificate` precedes execution), so history comes out of storage. A block
/// that is only proposed or validated is durable nowhere, so its blobs come from the caller.
pub(crate) struct BlockSender<S, N> {
    pub(crate) remote_node: RemoteNode<N>,
    pub(crate) storage: S,
    pub(crate) certificate_upload_batch_size: u64,
}

impl<S, N> BlockSender<S, N>
where
    S: Storage + Clone + 'static,
    N: ValidatorNode + Clone + 'static,
{
    /// Pushes a block the caller already holds, first closing up to `max_catch_up` of any gap
    /// below it, and returns the height the validator reports afterwards.
    ///
    /// The held block is sent only once the gap is gone: one landing above a gap is silently
    /// preprocessed and never advances the tip.
    pub(crate) async fn send_block(
        &mut self,
        certificate: &CacheArc<ConfirmedBlockCertificate>,
        blobs: &[CacheArc<Blob>],
        destination_next_height: Option<BlockHeight>,
        max_catch_up: u64,
    ) -> Result<BlockHeight, chain_client::Error> {
        let block = certificate.block();
        let (chain_id, height) = (block.header.chain_id, block.header.height);

        let next_height = if destination_next_height == Some(height) {
            height
        } else {
            self.send_missing_blocks(chain_id, height, destination_next_height, max_catch_up)
                .await?
        };
        // Not exactly at this block: either the gap was larger than one chunk, or the validator
        // is already past it — a re-executed chain re-offers its whole history — and in both
        // cases sending would be waste, so report the truth instead.
        if next_height != height {
            return Ok(next_height);
        }
        let info = self.send_confirmed_certificate(certificate, blobs).await?;
        Ok(info.next_block_height)
    }

    /// Sends up to `max_blocks` of the blocks of `chain_id` the validator is missing below
    /// `target_next_height`, returning the height it reports afterwards.
    ///
    /// Bounded so a validator that just joined converges over rounds instead of blocking the
    /// caller once. Heights whose certificates are not in storage are skipped — a chain we merely
    /// receive from is stored only at its message-bearing blocks, and the destination
    /// preprocesses above such gaps.
    pub(crate) async fn send_missing_blocks(
        &mut self,
        chain_id: ChainId,
        target_next_height: BlockHeight,
        destination_next_height: Option<BlockHeight>,
        max_blocks: u64,
    ) -> Result<BlockHeight, chain_client::Error> {
        let mut next_height = match destination_next_height {
            Some(height) => height,
            None => {
                let query = ChainInfoQuery::new(chain_id);
                self.remote_node
                    .handle_chain_info_query(query)
                    .await?
                    .next_block_height
            }
        };
        // Tried whenever the destination may still be below the checkpoint, rather than only when
        // the window cannot reach the target: an unbounded caller -- a client catching a validator
        // up under a proposal -- would otherwise never offer one, and offering it is cheaper than
        // replaying the prefix even when the window could span it.
        {
            // Best-effort, deliberately: a destination may refuse someone else's execution state
            // and must still be caught up by replaying. Failing the round here would leave such a
            // destination permanently behind, since the replay below would never run.
            match self
                .push_checkpoint_if_useful(chain_id, next_height, target_next_height)
                .await
            {
                Ok(reached) => next_height = reached,
                Err(error) => debug!(
                    %chain_id, %error,
                    "Checkpoint push did not land; falling back to replaying blocks",
                ),
            }
        }
        let last = target_next_height
            .0
            .min(next_height.0.saturating_add(max_blocks));
        let heights = (next_height.0..last).map(BlockHeight).collect::<Vec<_>>();
        let batch = usize::try_from(self.certificate_upload_batch_size).unwrap_or(usize::MAX);
        for chunk in heights.chunks(batch) {
            let certificates = self
                .storage
                .read_certificates_by_heights(chain_id, chunk)
                .await?;
            for certificate in certificates.into_iter().flatten() {
                // The validator's own responses move the cursor, so skip anything it has since
                // reported holding rather than re-sending it.
                if certificate.block().header.height < next_height {
                    continue;
                }
                let info = self.send_confirmed_certificate(&certificate, &[]).await?;
                next_height = info.next_block_height;
            }
        }
        Ok(next_height)
    }

    /// Offers the chain's latest checkpoint when the destination has not reached it, so that it
    /// installs the execution state instead of replaying every block below it, and returns the
    /// height it reports afterwards.
    ///
    /// This is what makes a bounded catch-up window safe on a checkpointed chain. A validator
    /// that bootstrapped from a checkpoint does not hold its own pre-checkpoint history, so a
    /// window sitting entirely below the checkpoint reads heights it cannot supply, skips them,
    /// sends nothing, and retries the same empty round forever. Reaching the checkpoint is the
    /// whole fix: a destination already restores rather than preprocesses a block that starts
    /// with one.
    ///
    /// Where the checkpoint is comes from the certificates themselves --
    /// [`BlockHeader::previous_checkpoint`], and the block's own body when it is a checkpoint --
    /// never from a view of this chain. Opening one would mean a second
    /// [`ChainStateView`](linera_chain::ChainStateView) racing the worker that owns it, which
    /// `Storage::load_chain` documents as causing invalid states and data corruption.
    ///
    /// A missing checkpoint is *not useful* rather than an error. A push the destination
    /// **rejects** does return one, which the caller swallows: replaying is still correct
    /// wherever a checkpoint cannot be had, and a validator is free not to accept one.
    async fn push_checkpoint_if_useful(
        &mut self,
        chain_id: ChainId,
        next_height: BlockHeight,
        target_next_height: BlockHeight,
    ) -> Result<BlockHeight, chain_client::Error> {
        let Some(checkpoint) = self.latest_checkpoint(chain_id, target_next_height).await? else {
            return Ok(next_height);
        };
        // Strictly below means the destination is already past it, and `send_missing_blocks` then
        // has certificates it can actually read, so replaying is both correct and cheaper.
        if checkpoint < next_height {
            return Ok(next_height);
        }
        let Some(certificate) = self
            .storage
            .read_certificates_by_heights(chain_id, &[checkpoint])
            .await?
            .into_iter()
            .flatten()
            .next()
        else {
            return Ok(next_height);
        };
        let info = self.send_confirmed_certificate(&certificate, &[]).await?;
        Ok(info.next_block_height)
    }

    /// Reads the chain's latest checkpoint out of the block we are trying to deliver.
    ///
    /// That block names the checkpoint below it and says whether it is one itself, so a single
    /// certificate answers both halves without loading the chain.
    async fn latest_checkpoint(
        &self,
        chain_id: ChainId,
        target_next_height: BlockHeight,
    ) -> Result<Option<BlockHeight>, chain_client::Error> {
        let Ok(height) = target_next_height.try_sub_one() else {
            return Ok(None);
        };
        let Some(tip) = self
            .storage
            .read_certificates_by_heights(chain_id, &[height])
            .await?
            .into_iter()
            .flatten()
            .next()
        else {
            return Ok(None);
        };
        let block = tip.block();
        if block.body.starts_with_checkpoint() {
            return Ok(Some(block.header.height));
        }
        Ok(block.header.previous_checkpoint)
    }

    /// Sends one confirmed certificate, uploading blobs the validator reports missing.
    ///
    /// A missing *committee* (`EventsNotFound` for the epoch stream) is not recovered here: the
    /// admin chain is a chain like any other, so its own export brings the destination up to
    /// date, and this block succeeds on a later round. Replaying the admin chain from inside
    /// another chain's push is how one export round used to stall on an unbounded foreign
    /// history.
    ///
    /// The pair retries indefinitely — the backoff caps at `max_retry_delay` — so it recovers
    /// whenever the destination learns the epoch, from the admin chain's own export or from the
    /// client, which pushes it on this same error. After a restart the admin chain re-enters
    /// this queue's work-list only once it produces a block, so a quiet admin chain can leave a
    /// pair deferred for a while; `CHAIN_SCOPED_BACKOFFS` is what makes that visible.
    async fn send_confirmed_certificate(
        &mut self,
        certificate: &CacheArc<ConfirmedBlockCertificate>,
        held: &[CacheArc<Blob>],
    ) -> Result<Box<crate::data_types::ChainInfo>, chain_client::Error> {
        let delivery = CrossChainMessageDelivery::NonBlocking;
        let mut result = self
            .remote_node
            .handle_optimized_confirmed_certificate(certificate, delivery)
            .await;
        // The same once-per-cause loop as the client's `RemoteNodeUpdater`: a second
        // `BlobsNotFound` naming new blobs is still recoverable, only repeating a cause is not.
        let mut sent_blobs = false;
        loop {
            match result {
                Err(NodeError::BlobsNotFound(blob_ids)) if !sent_blobs => {
                    self.remote_node
                        .check_blobs_not_found(&**certificate, &blob_ids)?;
                    let blobs = self.resolve_blobs(&blob_ids, held).await?;
                    self.remote_node
                        .node
                        .upload_blobs(blobs.into_iter().map(CacheArc::into_std).collect())
                        .await?;
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

    /// Collects the given blobs, taking each from `held` if present and the rest from storage.
    async fn resolve_blobs(
        &self,
        blob_ids: &[BlobId],
        held: &[CacheArc<Blob>],
    ) -> Result<Vec<CacheArc<Blob>>, chain_client::Error> {
        let mut blobs = Vec::with_capacity(blob_ids.len());
        let mut to_read = Vec::new();
        for blob_id in blob_ids {
            match held.iter().find(|blob| blob.id() == *blob_id) {
                Some(blob) => blobs.push(blob.clone()),
                None => to_read.push(*blob_id),
            }
        }
        if to_read.is_empty() {
            return Ok(blobs);
        }
        let read = self
            .storage
            .read_blobs(&to_read)
            .await?
            .into_iter()
            .collect::<Option<Vec<_>>>();
        blobs.extend(read.ok_or(NodeError::BlobsNotFound(to_read))?);
        Ok(blobs)
    }

    /// Offers a signed proposal to the validator, repairing whatever it reports missing.
    ///
    /// `held` are the blobs the block publishes, which the caller has and the validator may not.
    /// `blob_ids` are all the blobs the proposal needs, published or not; the ones that are not
    /// published are recovered by pushing the chains whose blocks last used them.
    ///
    /// `repairs` carries the one-shot guards. It is the caller's so that a repair stays one-shot
    /// across the calls a caller with a local node makes either side of its own repairs: a
    /// validator that keeps reporting the same cause is not making progress, and looping on it
    /// would not help.
    pub(crate) async fn send_block_proposal(
        &mut self,
        proposal: &BlockProposal,
        held: &[Blob],
        blob_ids: &mut Vec<BlobId>,
        repairs: &mut ProposalRepairs,
        max_catch_up: u64,
        clock_skew_sender: Option<&mpsc::UnboundedSender<ClockSkewReport>>,
    ) -> Result<ProposalOutcome, chain_client::Error> {
        let chain_id = proposal.content.block.chain_id;
        let height = proposal.content.block.height;
        loop {
            let local_time = self.storage.clock().current_time();
            let result = self
                .remote_node
                .handle_block_proposal(Box::new(proposal.clone()))
                .await;
            match result {
                Ok(info) => return Ok(ProposalOutcome::Accepted(info)),
                // Once the gap below the block is closed, a validator still disagreeing about the
                // round is either ahead of us or behind on consensus state we do not hold, and
                // both are the caller's to answer.
                Err(err @ (NodeError::WrongRound(_) | NodeError::UnexpectedBlockHeight { .. }))
                    if repairs.synced_round_and_height =>
                {
                    return Ok(ProposalOutcome::NeedsLocalRepair(err))
                }
                Err(err @ (NodeError::WrongRound(_) | NodeError::UnexpectedBlockHeight { .. })) => {
                    // The validator disagrees with the proposal's round or height. If it is
                    // behind, closing the gap below this block is all it needs; if it is ahead,
                    // or if it is still at the wrong round once level, only a local chain manager
                    // can say more, so the caller is told.
                    repairs.synced_round_and_height = true;
                    debug!(
                        remote_node = self.remote_node.address(),
                        %chain_id, %err,
                        "validator disagrees on round or height; closing any gap below the block",
                    );
                    if !self.is_behind(&err, proposal.content.round) {
                        return Ok(ProposalOutcome::NeedsLocalRepair(err));
                    }
                    self.send_missing_blocks(chain_id, height, None, max_catch_up)
                        .await?;
                }
                // The validator reports *every* missing cross-chain bundle in a single
                // `MissingCrossChainUpdates`, so we push all of the origins at once and retry. If
                // it still reports missing bundles after that, retrying would not make progress,
                // so we surface the error instead of looping.
                Err(NodeError::MissingCrossChainUpdates {
                    chain_id: dependencies_chain_id,
                    bundles,
                }) if dependencies_chain_id == chain_id => {
                    ensure!(
                        !repairs.synced_cross_chain_updates,
                        NodeError::ResponseHandlingError {
                            error: format!(
                                "validator still reports missing cross-chain updates for chain \
                                 {dependencies_chain_id} after they were all synced"
                            ),
                        }
                    );
                    repairs.synced_cross_chain_updates = true;
                    debug!(
                        remote_node = self.remote_node.address(),
                        %chain_id,
                        bundles = bundles.len(),
                        "validator reported missing cross-chain updates; pushing their origins",
                    );
                    // Collapse duplicate origins to the highest height each was reported at.
                    let mut origin_heights: BTreeMap<ChainId, BlockHeight> = BTreeMap::new();
                    for (origin, height) in bundles {
                        let target = height.try_add_one()?;
                        let entry = origin_heights.entry(origin).or_insert(target);
                        *entry = (*entry).max(target);
                    }
                    for (origin, target) in origin_heights {
                        self.send_missing_blocks(origin, target, None, max_catch_up)
                            .await?;
                    }
                }
                Err(NodeError::BlobsNotFound(_) | NodeError::InactiveChain(_))
                    if !blob_ids.is_empty() && !repairs.sent_blobs =>
                {
                    repairs.sent_blobs = true;
                    debug!(
                        remote_node = self.remote_node.address(),
                        %chain_id, "validator is missing blobs; sending the ones we hold",
                    );
                    let published: BTreeSet<_> = proposal
                        .content
                        .block
                        .published_blob_ids()
                        .into_iter()
                        .collect();
                    blob_ids.retain(|blob_id| !published.contains(blob_id));
                    let publishing = held_blobs(&published, held)?;
                    self.remote_node
                        .send_pending_blobs(chain_id, publishing)
                        .await?;
                    // Anything left is a blob the block only reads, which the validator gets by
                    // being brought level on the chains whose blocks last used it.
                    let missing = self
                        .remote_node
                        .node
                        .missing_blob_ids(std::mem::take(blob_ids))
                        .await?;
                    self.send_blocks_for_blobs(&missing).await?;
                }
                // A publisher chain's events can only be located from a local chain manager.
                Err(err @ NodeError::EventsNotFound(_)) => {
                    return Ok(ProposalOutcome::NeedsLocalRepair(err))
                }
                Err(err @ NodeError::ChainError { .. }) => {
                    // The validator rejected the proposal because of its own chain manager state
                    // -- most commonly an incompatible confirmed vote tied to a locking block we
                    // do not have. Only a caller with a local node can absorb what justified it.
                    return Ok(ProposalOutcome::NeedsLocalRepair(err));
                }
                Err(NodeError::InvalidTimestamp {
                    block_timestamp,
                    local_time: validator_local_time,
                    ..
                }) => {
                    // The validator's clock is behind the block's timestamp. Wait for our clock
                    // to reach the timestamp, and for the validator's to catch up with ours.
                    let clock_skew = local_time.delta_since(validator_local_time);
                    debug!(
                        remote_node = self.remote_node.address(),
                        %chain_id, %block_timestamp, ?clock_skew,
                        "validator's clock is behind; waiting and retrying",
                    );
                    // Reported before sleeping, so a caller aggregating skew sees it in time.
                    if let Some(sender) = clock_skew_sender {
                        sender.send((self.remote_node.public_key, clock_skew)).ok();
                    }
                    self.storage
                        .clock()
                        .sleep_until(block_timestamp.saturating_add(clock_skew))
                        .await;
                }
                Err(err) => return Err(err.into()),
            }
        }
    }

    /// Offers a validated certificate to the validator, uploading blobs it reports missing.
    ///
    /// `held` are the blobs of the locking block, which the caller has and the validator may not:
    /// a validated block is not committed, so they need not be in storage.
    pub(crate) async fn send_validated_certificate(
        &mut self,
        certificate: &ValidatedBlockCertificate,
        held: &[Blob],
        delivery: CrossChainMessageDelivery,
        max_catch_up: u64,
    ) -> Result<ProposalOutcome, chain_client::Error> {
        let chain_id = certificate.inner().chain_id();
        let height = certificate.block().header.height;
        let result = self
            .remote_node
            .handle_optimized_validated_certificate(certificate, delivery)
            .await;
        match &result {
            Err(original_err @ NodeError::BlobsNotFound(blob_ids)) => {
                self.remote_node
                    .check_blobs_not_found(certificate, blob_ids)?;
                let blobs = held_blobs(&blob_ids.iter().copied().collect::<BTreeSet<_>>(), held)
                    .map_err(|_| original_err.clone())?;
                self.remote_node.send_pending_blobs(chain_id, blobs).await?;
            }
            Err(err) if self.is_behind(err, certificate.round) => {
                self.send_missing_blocks(chain_id, height, None, max_catch_up)
                    .await?;
            }
            Err(err) => return Ok(ProposalOutcome::NeedsLocalRepair(err.clone())),
            Ok(_) => return Ok(ProposalOutcome::Accepted(result?)),
        }
        Ok(ProposalOutcome::Accepted(
            self.remote_node
                .handle_validated_certificate(certificate.clone())
                .await?,
        ))
    }

    /// Whether the error says the validator is *behind* us, which closing a gap can fix.
    ///
    /// A validator that is ahead is telling us we are the ones out of date, which nothing this
    /// sender holds can repair.
    fn is_behind(&self, error: &NodeError, round: Round) -> bool {
        match error {
            // A lower round is the validator lagging. Equal or higher means it is telling us that
            // we are the ones out of date, which nothing here can repair.
            NodeError::WrongRound(validator_round) => *validator_round < round,
            NodeError::UnexpectedBlockHeight {
                expected_block_height,
                found_block_height,
            } => expected_block_height < found_block_height,
            NodeError::InactiveChain(_) => true,
            _ => false,
        }
    }

    /// Pushes the blocks that published the given blobs, so the validator can read them.
    ///
    /// Only those blocks, not the prefixes below them: a blob's publisher is often a chain the
    /// validator has little of, and closing its whole gap to reach one block would be far more
    /// traffic than the recipient needs.
    async fn send_blocks_for_blobs(
        &mut self,
        blob_ids: &[BlobId],
    ) -> Result<(), chain_client::Error> {
        if blob_ids.is_empty() {
            return Ok(());
        }
        let mut chain_heights: BTreeMap<ChainId, BTreeSet<BlockHeight>> = BTreeMap::new();
        for blob_state in self
            .storage
            .read_blob_states(blob_ids)
            .await?
            .into_iter()
            .flatten()
        {
            match blob_state.origin {
                // Genesis blobs are published by no block; the recipient has them from its own
                // genesis config, so there is nothing to ship.
                BlobOrigin::Genesis => continue,
                BlobOrigin::Published {
                    chain_id,
                    block_height,
                } => {
                    chain_heights
                        .entry(chain_id)
                        .or_default()
                        .insert(block_height);
                }
            }
        }
        for (chain_id, heights) in chain_heights {
            let heights = heights.into_iter().collect::<Vec<_>>();
            let batch = usize::try_from(self.certificate_upload_batch_size).unwrap_or(usize::MAX);
            for chunk in heights.chunks(batch) {
                let certificates = self
                    .storage
                    .read_certificates_by_heights(chain_id, chunk)
                    .await?;
                for certificate in certificates.into_iter().flatten() {
                    self.send_confirmed_certificate(&certificate, &[]).await?;
                }
            }
        }
        Ok(())
    }
}

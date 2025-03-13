// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Operations that persist changes to the chain state when they are successful.

use std::{borrow::Cow, collections::BTreeMap};

use futures::future::Either;
use linera_base::{
    crypto::ValidatorPublicKey,
    data_types::{Blob, BlockHeight, Timestamp},
    ensure,
    identifiers::{ChainId, Owner},
};
use linera_chain::{
    data_types::{
        BlockExecutionOutcome, BlockProposal, ExecutedBlock, MessageBundle, Origin,
        ProposalContent, Target,
    },
    manager,
    types::{ConfirmedBlockCertificate, TimeoutCertificate, ValidatedBlockCertificate},
    ChainExecutionContext, ChainStateView, ExecutionResultExt as _,
};
use linera_execution::committee::{Committee, Epoch};
use linera_storage::{Clock as _, Storage};
use linera_views::{
    context::Context,
    views::{RootView, View},
};
use tokio::sync::oneshot;
use tracing::{debug, instrument, trace, warn};

use super::{check_block_epoch, ChainWorkerConfig, ChainWorkerState};
use crate::{
    data_types::ChainInfoResponse,
    worker::{NetworkActions, Notification, Reason, WorkerError},
};

/// Wrapper type that tracks if the changes to the `chain` state should be rolled back when
/// dropped.
pub struct ChainWorkerStateWithAttemptedChanges<'state, StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
{
    state: &'state mut ChainWorkerState<StorageClient>,
    succeeded: bool,
}

impl<'state, StorageClient> ChainWorkerStateWithAttemptedChanges<'state, StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
{
    /// Creates a new [`ChainWorkerStateWithAttemptedChanges`] instance to change the
    /// `state`.
    pub(super) async fn new(state: &'state mut ChainWorkerState<StorageClient>) -> Self {
        assert!(
            !state.chain.has_pending_changes().await,
            "`ChainStateView` has unexpected leftover changes"
        );

        ChainWorkerStateWithAttemptedChanges {
            state,
            succeeded: false,
        }
    }

    /// Processes a leader timeout issued for this multi-owner chain.
    pub(super) async fn process_timeout(
        &mut self,
        certificate: TimeoutCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        // Check that the chain is active and ready for this timeout.
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        self.state.ensure_is_active()?;
        let (chain_epoch, committee) = self.state.chain.current_committee()?;
        ensure!(
            certificate.inner().epoch == chain_epoch,
            WorkerError::InvalidEpoch {
                chain_id: certificate.inner().chain_id,
                chain_epoch,
                epoch: certificate.inner().epoch
            }
        );
        certificate.check(committee)?;
        let mut actions = NetworkActions::default();
        if self
            .state
            .chain
            .tip_state
            .get()
            .already_validated_block(certificate.inner().height)?
        {
            return Ok((
                ChainInfoResponse::new(&self.state.chain, self.state.config.key_pair()),
                actions,
            ));
        }
        let old_round = self.state.chain.manager.current_round();
        let timeout_chain_id = certificate.inner().chain_id;
        let timeout_height = certificate.inner().height;
        self.state
            .chain
            .manager
            .handle_timeout_certificate(certificate, self.state.storage.clock().current_time());
        let round = self.state.chain.manager.current_round();
        if round > old_round {
            actions.notifications.push(Notification {
                chain_id: timeout_chain_id,
                reason: Reason::NewRound {
                    height: timeout_height,
                    round,
                },
            })
        }
        let info = ChainInfoResponse::new(&self.state.chain, self.state.config.key_pair());
        self.save().await?;
        Ok((info, actions))
    }

    /// Validates a proposal's signatures and blobs.
    pub(super) async fn validate_block(
        &mut self,
        proposal: &BlockProposal,
    ) -> Result<Option<Vec<Blob>>, WorkerError> {
        let BlockProposal {
            content:
                ProposalContent {
                    block,
                    round,
                    outcome: _,
                },
            public_key,
            validated_block_certificate,
            signature: _,
        } = proposal;

        let owner: Owner = public_key.into();
        let chain = &self.state.chain;
        // Check the epoch.
        let (epoch, committee) = chain.current_committee()?;
        check_block_epoch(epoch, block.chain_id, block.epoch)?;
        let policy = committee.policy().clone();
        block.check_proposal_size(policy.maximum_block_proposal_size)?;
        // Check the authentication of the block.
        ensure!(
            chain.manager.verify_owner(proposal),
            WorkerError::InvalidOwner
        );
        if let Some(lite_certificate) = validated_block_certificate {
            // Verify that this block has been validated by a quorum before.
            lite_certificate.check(committee)?;
        } else if let Some(signer) = block.authenticated_signer {
            // Check the authentication of the operations in the new block.
            ensure!(signer == owner, WorkerError::InvalidSigner(signer));
        }
        // Check if the chain is ready for this new block proposal.
        chain.tip_state.get().verify_block_chaining(block)?;
        if chain.manager.check_proposed_block(proposal)? == manager::Outcome::Skip {
            return Ok(None);
        }
        let mut maybe_blobs = self
            .state
            .maybe_get_required_blobs(proposal.required_blob_ids(), None)
            .await?;
        let missing_blob_ids = super::missing_blob_ids(&maybe_blobs);
        if !missing_blob_ids.is_empty() {
            let chain = &mut self.state.chain;
            if chain.ownership().open_multi_leader_rounds {
                // TODO(#3203): Allow multiple pending proposals on permissionless chains.
                chain.pending_proposed_blobs.clear();
            }
            chain
                .pending_proposed_blobs
                .try_load_entry_mut(&owner)
                .await?
                .update(*round, validated_block_certificate.is_some(), maybe_blobs)
                .await?;
            self.save().await?;
            return Err(WorkerError::BlobsNotFound(missing_blob_ids));
        }
        let published_blobs = block
            .published_blob_ids()
            .iter()
            .filter_map(|blob_id| maybe_blobs.remove(blob_id).flatten())
            .collect::<Vec<_>>();
        Ok(Some(published_blobs))
    }

    /// Votes for a block proposal for the next block for this chain.
    pub(super) async fn vote_for_block_proposal(
        &mut self,
        proposal: BlockProposal,
        outcome: BlockExecutionOutcome,
        local_time: Timestamp,
    ) -> Result<(), WorkerError> {
        // Create the vote and store it in the chain state.
        let executed_block = outcome.with(proposal.content.block.clone());
        let created_blobs: BTreeMap<_, _> = executed_block.outcome.iter_created_blobs().collect();
        let blobs = self
            .state
            .get_required_blobs(proposal.expected_blob_ids(), &created_blobs)
            .await?;
        let key_pair = self.state.config.key_pair();
        let manager = &mut self.state.chain.manager;
        match manager.create_vote(proposal, executed_block, key_pair, local_time, blobs)? {
            // Cache the value we voted on, so the client doesn't have to send it again.
            Some(Either::Left(vote)) => {
                self.state
                    .block_values
                    .insert(Cow::Borrowed(vote.value.inner().inner()));
            }
            Some(Either::Right(vote)) => {
                self.state
                    .block_values
                    .insert(Cow::Borrowed(vote.value.inner().inner()));
            }
            None => (),
        }
        self.save().await?;
        Ok(())
    }

    /// Processes a validated block issued for this multi-owner chain.
    pub(super) async fn process_validated_block(
        &mut self,
        certificate: ValidatedBlockCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions, bool), WorkerError> {
        let block = certificate.block();

        let header = &block.header;
        let height = header.height;
        // Check that the chain is active and ready for this validated block.
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        self.state.ensure_is_active()?;
        let (epoch, committee) = self.state.chain.current_committee()?;
        check_block_epoch(epoch, header.chain_id, header.epoch)?;
        certificate.check(committee)?;
        let mut actions = NetworkActions::default();
        let already_committed_block = self
            .state
            .chain
            .tip_state
            .get()
            .already_validated_block(height)?;
        let should_skip_validated_block = || {
            self.state
                .chain
                .manager
                .check_validated_block(&certificate)
                .map(|outcome| outcome == manager::Outcome::Skip)
        };
        if already_committed_block || should_skip_validated_block()? {
            // If we just processed the same pending block, return the chain info unchanged.
            return Ok((
                ChainInfoResponse::new(&self.state.chain, self.state.config.key_pair()),
                actions,
                true,
            ));
        }

        self.state
            .block_values
            .insert(Cow::Borrowed(certificate.inner().inner()));
        let required_blob_ids = block.required_blob_ids();
        let maybe_blobs = self
            .state
            .maybe_get_required_blobs(required_blob_ids, Some(&block.created_blobs()))
            .await?;
        let missing_blob_ids = super::missing_blob_ids(&maybe_blobs);
        if !missing_blob_ids.is_empty() {
            self.state
                .chain
                .pending_validated_blobs
                .update(certificate.round, true, maybe_blobs)
                .await?;
            self.save().await?;
            return Err(WorkerError::BlobsNotFound(missing_blob_ids));
        }
        let blobs = maybe_blobs
            .into_iter()
            .filter_map(|(blob_id, maybe_blob)| Some((blob_id, maybe_blob?)))
            .collect();
        let old_round = self.state.chain.manager.current_round();
        self.state.chain.manager.create_final_vote(
            certificate,
            self.state.config.key_pair(),
            self.state.storage.clock().current_time(),
            blobs,
        )?;
        let info = ChainInfoResponse::new(&self.state.chain, self.state.config.key_pair());
        self.save().await?;
        let round = self.state.chain.manager.current_round();
        if round > old_round {
            actions.notifications.push(Notification {
                chain_id: self.state.chain_id(),
                reason: Reason::NewRound { height, round },
            })
        }
        Ok((info, actions, false))
    }

    /// Processes a confirmed block (aka a commit).
    pub(super) async fn process_confirmed_block(
        &mut self,
        certificate: ConfirmedBlockCertificate,
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        let executed_block: ExecutedBlock = certificate.block().clone().into();
        let block = &executed_block.block;
        let height = block.height;
        let chain_id = block.chain_id;

        // Check that the chain is active and ready for this confirmation.
        let tip = self.state.chain.tip_state.get().clone();
        if tip.next_block_height < height {
            return Err(WorkerError::MissingEarlierBlocks {
                current_block_height: tip.next_block_height,
            });
        }
        if tip.next_block_height > height {
            // Block was already confirmed.
            let info = ChainInfoResponse::new(&self.state.chain, self.state.config.key_pair());
            let actions = self.state.create_network_actions().await?;
            return Ok((info, actions));
        }
        let local_time = self.state.storage.clock().current_time();
        // TODO(#2351): This sets the committee and then checks that committee's signatures.
        if tip.is_first_block() && self.state.chain.is_child() {
            self.state
                .chain
                .execute_init_message_from(block, local_time)
                .await?;
        }
        self.state.ensure_is_active()?;
        // Verify the certificate.
        let (epoch, committee) = self.state.chain.current_committee()?;
        check_block_epoch(epoch, chain_id, block.epoch)?;
        certificate.check(committee)?;
        // This should always be true for valid certificates.
        ensure!(
            tip.block_hash == block.previous_block_hash,
            WorkerError::InvalidBlockChaining
        );

        let required_blob_ids = executed_block.required_blob_ids();
        let created_blobs: BTreeMap<_, _> = executed_block.outcome.iter_created_blobs().collect();
        let blobs_result = self
            .state
            .get_required_blobs(executed_block.required_blob_ids(), &created_blobs)
            .await
            .map(|blobs| blobs.into_values().collect::<Vec<_>>());

        if let Ok(blobs) = &blobs_result {
            self.state
                .storage
                .write_blobs_and_certificate(blobs, &certificate)
                .await?;
            let events = executed_block
                .outcome
                .events
                .iter()
                .flatten()
                .map(|event| (event.id(chain_id), event.value.clone()));
            self.state.storage.write_events(events).await?;
        }

        // Update the blob state with last used certificate hash.
        let blob_state = certificate.value().inner().to_blob_state();
        let overwrite = blobs_result.is_ok(); // Overwrite only if we wrote the certificate.
        let blob_ids = required_blob_ids.into_iter().collect::<Vec<_>>();
        self.state
            .storage
            .maybe_write_blob_states(&blob_ids, blob_state, overwrite)
            .await?;
        let mut blobs = blobs_result?
            .into_iter()
            .map(|blob| (blob.id(), blob))
            .collect::<BTreeMap<_, _>>();
        let published_blobs = block
            .published_blob_ids()
            .iter()
            .filter_map(|blob_id| blobs.remove(blob_id))
            .collect::<Vec<_>>();

        // Execute the block and update inboxes.
        self.state
            .chain
            .remove_bundles_from_inboxes(block.timestamp, &block.incoming_bundles)
            .await?;
        let oracle_responses = Some(executed_block.outcome.oracle_responses.clone());
        let verified_outcome = self
            .state
            .chain
            .execute_block(block, local_time, None, &published_blobs, oracle_responses)
            .await?;
        // We should always agree on the messages and state hash.
        ensure!(
            executed_block.outcome == verified_outcome,
            WorkerError::IncorrectOutcome {
                submitted: Box::new(executed_block.outcome.clone()),
                computed: Box::new(verified_outcome),
            }
        );
        // Advance to next block height.
        let tip = self.state.chain.tip_state.get_mut();
        let hash = certificate.hash();
        tip.block_hash = Some(hash);
        tip.next_block_height.try_add_assign_one()?;
        tip.update_counters(block, &executed_block.outcome)?;
        self.state.chain.confirmed_log.push(hash);
        self.state.track_newly_created_chains(&executed_block);
        let mut actions = self.state.create_network_actions().await?;
        trace!("Processed confirmed block {height} on chain {chain_id:.8}");
        actions.notifications.push(Notification {
            chain_id,
            reason: Reason::NewBlock { height, hash },
        });
        // Persist chain.
        self.save().await?;

        self.state
            .block_values
            .insert(Cow::Owned(certificate.into_inner().into_inner()));

        self.register_delivery_notifier(height, &actions, notify_when_messages_are_delivered)
            .await;
        let info = ChainInfoResponse::new(&self.state.chain, self.state.config.key_pair());

        Ok((info, actions))
    }

    /// Schedules a notification for when cross-chain messages are delivered up to the given
    /// `height`.
    #[instrument(level = "trace", skip(self, notify_when_messages_are_delivered))]
    async fn register_delivery_notifier(
        &mut self,
        height: BlockHeight,
        actions: &NetworkActions,
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) {
        if let Some(notifier) = notify_when_messages_are_delivered {
            if actions
                .cross_chain_requests
                .iter()
                .any(|request| request.has_messages_lower_or_equal_than(height))
            {
                self.state.delivery_notifier.register(height, notifier);
            } else {
                // No need to wait. Also, cross-chain requests may not trigger the
                // notifier later, even if we register it.
                if let Err(()) = notifier.send(()) {
                    warn!("Failed to notify message delivery to caller");
                }
            }
        }
    }

    /// Updates the chain's inboxes, receiving messages from a cross-chain update.
    pub(super) async fn process_cross_chain_update(
        &mut self,
        origin: Origin,
        bundles: Vec<(Epoch, MessageBundle)>,
    ) -> Result<Option<(BlockHeight, NetworkActions)>, WorkerError> {
        // Only process certificates with relevant heights and epochs.
        let next_height_to_receive = self
            .state
            .chain
            .next_block_height_to_receive(&origin)
            .await?;
        let last_anticipated_block_height = self
            .state
            .chain
            .last_anticipated_block_height(&origin)
            .await?;
        let helper = CrossChainUpdateHelper::new(&self.state.config, &self.state.chain);
        let recipient = self.state.chain_id();
        let bundles = helper.select_message_bundles(
            &origin,
            recipient,
            next_height_to_receive,
            last_anticipated_block_height,
            bundles,
        )?;
        let Some(last_updated_height) = bundles.last().map(|bundle| bundle.height) else {
            return Ok(None);
        };
        // Process the received messages in certificates.
        let local_time = self.state.storage.clock().current_time();
        let mut previous_height = None;
        let mut new_outbox_entries = false;
        for bundle in bundles {
            let add_to_received_log = previous_height != Some(bundle.height);
            previous_height = Some(bundle.height);
            // Update the staged chain state with the received block.
            if self
                .state
                .chain
                .receive_message_bundle(&origin, bundle, local_time, add_to_received_log)
                .await?
            {
                new_outbox_entries = true;
            }
        }
        if !self.state.config.allow_inactive_chains && !self.state.chain.is_active() {
            // Refuse to create a chain state if the chain is still inactive by
            // now. Accordingly, do not send a confirmation, so that the
            // cross-chain update is retried later.
            warn!(
                "Refusing to deliver messages to {recipient:?} from {origin:?} \
                at height {last_updated_height} because the recipient is still inactive",
            );
            return Ok(None);
        }
        let actions = if new_outbox_entries {
            self.state.create_network_actions().await?
        } else {
            // Don't create network actions, so that old entries don't cause retry loops.
            NetworkActions::default()
        };
        // Save the chain.
        self.save().await?;
        Ok(Some((last_updated_height, actions)))
    }

    /// Handles the cross-chain request confirming that the recipient was updated.
    pub(super) async fn confirm_updated_recipient(
        &mut self,
        latest_heights: Vec<(Target, BlockHeight)>,
    ) -> Result<(), WorkerError> {
        let mut height_with_fully_delivered_messages = BlockHeight::ZERO;

        for (target, height) in latest_heights {
            let fully_delivered = self
                .state
                .chain
                .mark_messages_as_received(&target, height)
                .await?
                && self
                    .state
                    .all_messages_to_tracked_chains_delivered_up_to(height)
                    .await?;

            if fully_delivered && height > height_with_fully_delivered_messages {
                height_with_fully_delivered_messages = height;
            }
        }

        self.save().await?;

        self.state
            .delivery_notifier
            .notify(height_with_fully_delivered_messages);

        Ok(())
    }

    pub async fn update_received_certificate_trackers(
        &mut self,
        new_trackers: BTreeMap<ValidatorPublicKey, u64>,
    ) -> Result<(), WorkerError> {
        self.state
            .chain
            .update_received_certificate_trackers(new_trackers);
        self.save().await?;
        Ok(())
    }

    /// Attempts to vote for a leader timeout, if possible.
    pub(super) async fn vote_for_leader_timeout(&mut self) -> Result<(), WorkerError> {
        let chain = &mut self.state.chain;
        if let Some(epoch) = chain.execution_state.system.epoch.get() {
            let chain_id = chain.chain_id();
            let height = chain.tip_state.get().next_block_height;
            let key_pair = self.state.config.key_pair();
            let local_time = self.state.storage.clock().current_time();
            if chain
                .manager
                .vote_timeout(chain_id, height, *epoch, key_pair, local_time)
            {
                self.save().await?;
            }
        }
        Ok(())
    }

    /// Votes for falling back to a public chain.
    pub(super) async fn vote_for_fallback(&mut self) -> Result<(), WorkerError> {
        let chain = &mut self.state.chain;
        if let (Some(epoch), Some(entry)) = (
            chain.execution_state.system.epoch.get(),
            chain.unskippable_bundles.front().await?,
        ) {
            let elapsed = self
                .state
                .storage
                .clock()
                .current_time()
                .delta_since(entry.seen);
            if elapsed >= chain.ownership().timeout_config.fallback_duration {
                let chain_id = chain.chain_id();
                let height = chain.tip_state.get().next_block_height;
                let key_pair = self.state.config.key_pair();
                if chain
                    .manager
                    .vote_fallback(chain_id, height, *epoch, key_pair)
                {
                    self.save().await?;
                }
            }
        }
        Ok(())
    }

    pub(super) async fn handle_pending_blob(
        &mut self,
        blob: Blob,
    ) -> Result<ChainInfoResponse, WorkerError> {
        let mut was_expected = self
            .state
            .chain
            .pending_validated_blobs
            .maybe_insert(&blob)
            .await?;
        for (_, mut pending_blobs) in self
            .state
            .chain
            .pending_proposed_blobs
            .try_load_all_entries_mut()
            .await?
        {
            if !pending_blobs.validated.get() {
                let (_, committee) = self.state.chain.current_committee()?;
                let policy = committee.policy();
                policy
                    .check_blob_size(blob.content())
                    .with_execution_context(ChainExecutionContext::Block)?;
                ensure!(
                    u64::try_from(pending_blobs.pending_blobs.count().await?)
                        .is_ok_and(|count| count < policy.maximum_published_blobs),
                    WorkerError::TooManyPublishedBlobs(policy.maximum_published_blobs)
                );
            }
            was_expected = was_expected || pending_blobs.maybe_insert(&blob).await?;
        }
        ensure!(was_expected, WorkerError::UnexpectedBlob);
        self.save().await?;
        Ok(ChainInfoResponse::new(
            &self.state.chain,
            self.state.config.key_pair(),
        ))
    }

    /// Stores the chain state in persistent storage.
    ///
    /// Waits until the [`ChainStateView`] is no longer shared before persisting the changes.
    async fn save(&mut self) -> Result<(), WorkerError> {
        // SAFETY: this is the only place a write-lock is acquired, and read-locks are acquired in
        // the `chain_state_view` method, which has a `&mut self` receiver like this `save` method.
        // That means that when the write-lock is acquired, no readers will be waiting to acquire
        // the lock. This is important because otherwise readers could have a stale view of the
        // chain state.
        let maybe_shared_chain_view = self.state.shared_chain_view.take();
        let _maybe_write_guard = match &maybe_shared_chain_view {
            Some(shared_chain_view) => Some(shared_chain_view.write().await),
            None => None,
        };

        self.state.chain.save().await?;
        self.succeeded = true;
        Ok(())
    }
}

impl<StorageClient> Drop for ChainWorkerStateWithAttemptedChanges<'_, StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if !self.succeeded {
            self.state.chain.rollback();
        }
    }
}

/// Helper type for handling cross-chain updates.
pub(crate) struct CrossChainUpdateHelper<'a> {
    pub allow_messages_from_deprecated_epochs: bool,
    pub current_epoch: Option<Epoch>,
    pub committees: &'a BTreeMap<Epoch, Committee>,
}

impl<'a> CrossChainUpdateHelper<'a> {
    /// Creates a new [`CrossChainUpdateHelper`].
    pub fn new<C>(config: &ChainWorkerConfig, chain: &'a ChainStateView<C>) -> Self
    where
        C: Context + Clone + Send + Sync + 'static,
    {
        CrossChainUpdateHelper {
            allow_messages_from_deprecated_epochs: config.allow_messages_from_deprecated_epochs,
            current_epoch: *chain.execution_state.system.epoch.get(),
            committees: chain.execution_state.system.committees.get(),
        }
    }

    /// Checks basic invariants and deals with repeated heights and deprecated epochs.
    /// * Returns a range of message bundles that are both new to us and not relying on
    ///   an untrusted set of validators.
    /// * In the case of validators, if the epoch(s) of the highest bundles are not
    ///   trusted, we only accept bundles that contain messages that were already
    ///   executed by anticipation (i.e. received in certified blocks).
    /// * Basic invariants are checked for good measure. We still crucially trust
    ///   the worker of the sending chain to have verified and executed the blocks
    ///   correctly.
    pub fn select_message_bundles(
        &self,
        origin: &'a Origin,
        recipient: ChainId,
        next_height_to_receive: BlockHeight,
        last_anticipated_block_height: Option<BlockHeight>,
        mut bundles: Vec<(Epoch, MessageBundle)>,
    ) -> Result<Vec<MessageBundle>, WorkerError> {
        let mut latest_height = None;
        let mut skipped_len = 0;
        let mut trusted_len = 0;
        for (i, (epoch, bundle)) in bundles.iter().enumerate() {
            // Make sure that heights are not decreasing.
            ensure!(
                latest_height <= Some(bundle.height),
                WorkerError::InvalidCrossChainRequest
            );
            latest_height = Some(bundle.height);
            // Check if the block has been received already.
            if bundle.height < next_height_to_receive {
                skipped_len = i + 1;
            }
            // Check if the height is trusted or the epoch is trusted.
            if self.allow_messages_from_deprecated_epochs
                || Some(bundle.height) <= last_anticipated_block_height
                || Some(*epoch) >= self.current_epoch
                || self.committees.contains_key(epoch)
            {
                trusted_len = i + 1;
            }
        }
        if skipped_len > 0 {
            let (_, sample_bundle) = &bundles[skipped_len - 1];
            debug!(
                "Ignoring repeated messages to {recipient:.8} from {origin:} at height {}",
                sample_bundle.height,
            );
        }
        if skipped_len < bundles.len() && trusted_len < bundles.len() {
            let (sample_epoch, sample_bundle) = &bundles[trusted_len];
            warn!(
                "Refusing messages to {recipient:.8} from {origin:} at height {} \
                 because the epoch {} is not trusted any more",
                sample_bundle.height, sample_epoch,
            );
        }
        let bundles = if skipped_len < trusted_len {
            bundles
                .drain(skipped_len..trusted_len)
                .map(|(_, bundle)| bundle)
                .collect()
        } else {
            vec![]
        };
        Ok(bundles)
    }
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Operations that persist changes to the chain state when they are successful.

use std::{borrow::Cow, collections::BTreeMap};

use futures::future::try_join_all;
use linera_base::{
    data_types::{Blob, BlockHeight, Timestamp},
    ensure,
    identifiers::{ChainId, MessageId},
};
use linera_chain::{
    data_types::{
        BlockExecutionOutcome, BlockProposal, Certificate, CertificateValue,
        HashedCertificateValue, MessageBundle, Origin, Target,
    },
    manager, ChainStateView,
};
use linera_execution::{
    committee::{Committee, Epoch},
    BlobState,
};
use linera_storage::Storage;
use linera_views::{
    common::Context,
    views::{RootView, View, ViewError},
};
use tracing::{debug, warn};

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
    ViewError: From<StorageClient::StoreError>,
{
    state: &'state mut ChainWorkerState<StorageClient>,
    succeeded: bool,
}

impl<'state, StorageClient> ChainWorkerStateWithAttemptedChanges<'state, StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
    ViewError: From<StorageClient::StoreError>,
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
        certificate: Certificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        let (chain_id, height, epoch) = match certificate.value() {
            CertificateValue::Timeout {
                chain_id,
                height,
                epoch,
                ..
            } => (*chain_id, *height, *epoch),
            _ => panic!("Expecting a leader timeout certificate"),
        };
        // Check that the chain is active and ready for this timeout.
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        self.state.ensure_is_active()?;
        let (chain_epoch, committee) = self
            .state
            .chain
            .execution_state
            .system
            .current_committee()
            .expect("chain is active");
        ensure!(
            epoch == chain_epoch,
            WorkerError::InvalidEpoch {
                chain_id,
                chain_epoch,
                epoch
            }
        );
        certificate.check(committee)?;
        let mut actions = NetworkActions::default();
        if self
            .state
            .chain
            .tip_state
            .get()
            .already_validated_block(height)?
        {
            return Ok((
                ChainInfoResponse::new(&self.state.chain, self.state.config.key_pair()),
                actions,
            ));
        }
        let old_round = self.state.chain.manager.get().current_round;
        self.state
            .chain
            .manager
            .get_mut()
            .handle_timeout_certificate(
                certificate.clone(),
                self.state.storage.clock().current_time(),
            );
        let round = self.state.chain.manager.get().current_round;
        if round > old_round {
            actions.notifications.push(Notification {
                chain_id,
                reason: Reason::NewRound { height, round },
            })
        }
        let info = ChainInfoResponse::new(&self.state.chain, self.state.config.key_pair());
        self.save().await?;
        Ok((info, actions))
    }

    /// Votes for a block proposal for the next block for this chain.
    pub(super) async fn vote_for_block_proposal(
        &mut self,
        proposal: BlockProposal,
        outcome: BlockExecutionOutcome,
        local_time: Timestamp,
    ) -> Result<(), WorkerError> {
        // Create the vote and store it in the chain state.
        let manager = self.state.chain.manager.get_mut();
        manager.create_vote(proposal, outcome, self.state.config.key_pair(), local_time);
        // Cache the value we voted on, so the client doesn't have to send it again.
        if let Some(vote) = manager.pending() {
            self.state
                .recent_hashed_certificate_values
                .insert(Cow::Borrowed(&vote.value))
                .await;
        }
        self.save().await?;
        Ok(())
    }

    /// Processes a validated block issued for this multi-owner chain.
    pub(super) async fn process_validated_block(
        &mut self,
        certificate: Certificate,
        hashed_certificate_values: &[HashedCertificateValue],
        blobs: &[Blob],
    ) -> Result<(ChainInfoResponse, NetworkActions, bool), WorkerError> {
        let executed_block = match certificate.value() {
            CertificateValue::ValidatedBlock { executed_block } => executed_block,
            _ => panic!("Expecting a validation certificate"),
        };

        let block = &executed_block.block;
        let height = block.height;
        // Check that the chain is active and ready for this validated block.
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        self.state.ensure_is_active()?;
        let (epoch, committee) = self
            .state
            .chain
            .execution_state
            .system
            .current_committee()
            .expect("chain is active");
        check_block_epoch(epoch, block)?;
        certificate.check(committee)?;
        let mut actions = NetworkActions::default();
        let already_validated_block = self
            .state
            .chain
            .tip_state
            .get()
            .already_validated_block(height)?;
        let should_skip_validated_block = || {
            self.state
                .chain
                .manager
                .get()
                .check_validated_block(&certificate)
                .map(|outcome| outcome == manager::Outcome::Skip)
        };
        if already_validated_block || should_skip_validated_block()? {
            // If we just processed the same pending block, return the chain info unchanged.
            return Ok((
                ChainInfoResponse::new(&self.state.chain, self.state.config.key_pair()),
                actions,
                true,
            ));
        }
        self.state
            .recent_hashed_certificate_values
            .insert(Cow::Borrowed(&certificate.value))
            .await;
        // Verify that all required bytecode hashed certificate values and blobs are available, and no
        // unrelated ones provided.
        self.state
            .check_no_missing_blobs(
                block,
                executed_block.required_blob_ids(),
                hashed_certificate_values,
                blobs,
            )
            .await?;
        let old_round = self.state.chain.manager.get().current_round;
        self.state.chain.manager.get_mut().create_final_vote(
            certificate,
            self.state.config.key_pair(),
            self.state.storage.clock().current_time(),
        );
        let info = ChainInfoResponse::new(&self.state.chain, self.state.config.key_pair());
        self.save().await?;
        let round = self.state.chain.manager.get().current_round;
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
        certificate: Certificate,
        hashed_certificate_values: &[HashedCertificateValue],
        blobs: &[Blob],
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        let CertificateValue::ConfirmedBlock { executed_block, .. } = certificate.value() else {
            panic!("Expecting a confirmation certificate");
        };
        let block = &executed_block.block;
        // Check that the chain is active and ready for this confirmation.
        let tip = self.state.chain.tip_state.get().clone();
        if tip.next_block_height < block.height {
            return Err(WorkerError::MissingEarlierBlocks {
                current_block_height: tip.next_block_height,
            });
        }
        if tip.next_block_height > block.height {
            // Block was already confirmed.
            let info = ChainInfoResponse::new(&self.state.chain, self.state.config.key_pair());
            let actions = self.state.create_network_actions().await?;
            return Ok((info, actions));
        }
        // TODO(#2351): This sets the committee and then checks that committee's signatures.
        if tip.is_first_block() && !self.state.chain.is_active() {
            if let Some(incoming_bundle) = block.incoming_bundles.first() {
                if let Some(posted_message) = incoming_bundle.bundle.messages.first() {
                    let message_id = MessageId {
                        chain_id: incoming_bundle.origin.sender,
                        height: incoming_bundle.bundle.height,
                        index: posted_message.index,
                    };
                    let local_time = self.state.storage.clock().current_time();
                    self.state
                        .chain
                        .execute_init_message(
                            message_id,
                            &posted_message.message,
                            incoming_bundle.bundle.timestamp,
                            local_time,
                        )
                        .await?;
                }
            }
        }
        self.state.ensure_is_active()?;
        // Verify the certificate.
        let (epoch, committee) = self
            .state
            .chain
            .execution_state
            .system
            .current_committee()
            .expect("chain is active");
        check_block_epoch(epoch, block)?;
        certificate.check(committee)?;
        // This should always be true for valid certificates.
        ensure!(
            tip.block_hash == block.previous_block_hash,
            WorkerError::InvalidBlockChaining
        );

        let required_blob_ids = executed_block.required_blob_ids();
        // Verify that all required bytecode hashed certificate values and blobs are available, and no
        // unrelated ones provided.
        self.state
            .check_no_missing_blobs(
                block,
                required_blob_ids.clone(),
                hashed_certificate_values,
                blobs,
            )
            .await?;
        // Persist certificate and hashed certificate values.
        self.state
            .recent_hashed_certificate_values
            .insert_all(hashed_certificate_values.iter().map(Cow::Borrowed))
            .await;
        for blob in blobs {
            self.state.cache_recent_blob(Cow::Borrowed(blob)).await;
        }

        let blobs_in_block = self.state.get_blobs(required_blob_ids.clone()).await?;
        let certificate_hash = certificate.hash();

        self.state
            .storage
            .write_hashed_certificate_values_blobs_certificate(
                hashed_certificate_values,
                &blobs_in_block,
                &certificate,
            )
            .await?;

        // Update the blob state with last used certificate hash.
        try_join_all(required_blob_ids.into_iter().map(|blob_id| {
            self.state.storage.maybe_write_blob_state(
                blob_id,
                BlobState {
                    last_used_by: certificate_hash,
                    epoch: certificate.value().epoch(),
                },
            )
        }))
        .await?;

        // Execute the block and update inboxes.
        self.state.chain.remove_bundles_from_inboxes(block).await?;
        let local_time = self.state.storage.clock().current_time();
        let verified_outcome = Box::pin(self.state.chain.execute_block(
            block,
            local_time,
            Some(executed_block.outcome.oracle_responses.clone()),
        ))
        .await?;
        // We should always agree on the messages and state hash.
        ensure!(
            executed_block.outcome == verified_outcome,
            WorkerError::IncorrectOutcome {
                submitted: executed_block.outcome.clone(),
                computed: verified_outcome,
            }
        );
        // Advance to next block height.
        let tip = self.state.chain.tip_state.get_mut();
        tip.block_hash = Some(certificate.hash());
        tip.next_block_height.try_add_assign_one()?;
        tip.num_incoming_bundles += block.incoming_bundles.len() as u32;
        tip.num_operations += block.operations.len() as u32;
        tip.num_outgoing_messages += executed_block.outcome.messages.len() as u32;
        self.state.chain.confirmed_log.push(certificate.hash());
        let info = ChainInfoResponse::new(&self.state.chain, self.state.config.key_pair());
        let mut actions = self.state.create_network_actions().await?;
        actions.notifications.push(Notification {
            chain_id: block.chain_id,
            reason: Reason::NewBlock {
                height: block.height,
                hash: certificate.value.hash(),
            },
        });
        // Persist chain.
        self.save().await?;
        self.state
            .recent_hashed_certificate_values
            .insert(Cow::Owned(certificate.value))
            .await;

        Ok((info, actions))
    }

    /// Updates the chain's inboxes, receiving messages from a cross-chain update.
    pub(super) async fn process_cross_chain_update(
        &mut self,
        origin: Origin,
        bundles: Vec<(Epoch, MessageBundle)>,
    ) -> Result<Option<BlockHeight>, WorkerError> {
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
        for bundle in bundles {
            // Update the staged chain state with the received block.
            self.state
                .chain
                .receive_message_bundle(&origin, bundle, local_time)
                .await?
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
        // Save the chain.
        self.save().await?;
        Ok(Some(last_updated_height))
    }

    /// Handles the cross-chain request confirming that the recipient was updated.
    pub(super) async fn confirm_updated_recipient(
        &mut self,
        latest_heights: Vec<(Target, BlockHeight)>,
    ) -> Result<BlockHeight, WorkerError> {
        let mut height_with_fully_delivered_messages = BlockHeight::ZERO;

        for (target, height) in latest_heights {
            let fully_delivered = self
                .state
                .chain
                .mark_messages_as_received(&target, height)
                .await?
                && self.state.chain.all_messages_delivered_up_to(height);

            if fully_delivered && height > height_with_fully_delivered_messages {
                height_with_fully_delivered_messages = height;
            }
        }

        self.save().await?;

        Ok(height_with_fully_delivered_messages)
    }

    /// Attempts to vote for a leader timeout, if possible.
    pub(super) async fn vote_for_leader_timeout(&mut self) -> Result<(), WorkerError> {
        let chain = &mut self.state.chain;
        if let Some(epoch) = chain.execution_state.system.epoch.get() {
            let chain_id = chain.chain_id();
            let height = chain.tip_state.get().next_block_height;
            let key_pair = self.state.config.key_pair();
            let local_time = self.state.storage.clock().current_time();
            let manager = chain.manager.get_mut();
            if manager.vote_timeout(chain_id, height, *epoch, key_pair, local_time) {
                self.save().await?;
            }
        }
        Ok(())
    }

    /// Votes for falling back to a public chain.
    ///
    pub(super) async fn vote_for_fallback(&mut self) -> Result<(), WorkerError> {
        let chain = &mut self.state.chain;
        if let (Some(epoch), Some(entry)) = (
            chain.execution_state.system.epoch.get(),
            chain.unskippable.front().await?,
        ) {
            let ownership = chain.execution_state.system.ownership.get();
            let elapsed = self
                .state
                .storage
                .clock()
                .current_time()
                .delta_since(entry.seen);
            if elapsed >= ownership.timeout_config.fallback_duration {
                let chain_id = chain.chain_id();
                let height = chain.tip_state.get().next_block_height;
                let key_pair = self.state.config.key_pair();
                let manager = chain.manager.get_mut();
                if manager.vote_fallback(chain_id, height, *epoch, key_pair) {
                    self.save().await?;
                }
            }
        }
        Ok(())
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
    ViewError: From<StorageClient::StoreError>,
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
        ViewError: From<C::Error>,
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
                "Ignoring repeated messages to {recipient:?} from {origin:?} at height {}",
                sample_bundle.height,
            );
        }
        if skipped_len < bundles.len() && trusted_len < bundles.len() {
            let (sample_epoch, sample_bundle) = &bundles[trusted_len];
            warn!(
                "Refusing messages to {recipient:?} from {origin:?} at height {} \
                 because the epoch {:?} is not trusted any more",
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

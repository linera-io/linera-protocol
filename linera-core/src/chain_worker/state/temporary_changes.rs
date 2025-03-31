// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Operations that don't persist any changes to the chain state.

use linera_base::{
    data_types::{ApplicationDescription, ArithmeticError, Blob, Timestamp},
    ensure,
    identifiers::{AccountOwner, ApplicationId},
};
use linera_chain::{
    data_types::{
        BlockExecutionOutcome, BlockProposal, IncomingBundle, MessageAction, ProposalContent,
        ProposedBlock,
    },
    manager,
    types::Block,
};
use linera_execution::{Query, QueryOutcome};
use linera_storage::{Clock as _, Storage};
use linera_views::views::View;
#[cfg(with_testing)]
use {
    linera_base::{crypto::CryptoHash, data_types::BlockHeight},
    linera_chain::{
        data_types::{MessageBundle, Origin},
        types::ConfirmedBlockCertificate,
    },
};

use super::ChainWorkerState;
use crate::{
    data_types::{ChainInfo, ChainInfoQuery, ChainInfoResponse},
    worker::WorkerError,
};

/// Wrapper type that rolls back changes to the `chain` state when dropped.
pub struct ChainWorkerStateWithTemporaryChanges<'state, StorageClient>(
    &'state mut ChainWorkerState<StorageClient>,
)
where
    StorageClient: Storage + Clone + Send + Sync + 'static;

impl<'state, StorageClient> ChainWorkerStateWithTemporaryChanges<'state, StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
{
    /// Creates a new [`ChainWorkerStateWithTemporaryChanges`] instance to temporarily change the
    /// `state`.
    pub(super) async fn new(state: &'state mut ChainWorkerState<StorageClient>) -> Self {
        assert!(
            !state.chain.has_pending_changes().await,
            "`ChainStateView` has unexpected leftover changes"
        );

        ChainWorkerStateWithTemporaryChanges(state)
    }

    /// Returns a stored [`Certificate`] for the chain's block at the requested [`BlockHeight`].
    #[cfg(with_testing)]
    pub(super) async fn read_certificate(
        &mut self,
        height: BlockHeight,
    ) -> Result<Option<ConfirmedBlockCertificate>, WorkerError> {
        self.0.ensure_is_active()?;
        let certificate_hash = match self.0.chain.confirmed_log.get(height.try_into()?).await? {
            Some(hash) => hash,
            None => return Ok(None),
        };
        let certificate = self.0.storage.read_certificate(certificate_hash).await?;
        Ok(Some(certificate))
    }

    /// Searches for a bundle in one of the chain's inboxes.
    #[cfg(with_testing)]
    pub(super) async fn find_bundle_in_inbox(
        &mut self,
        inbox_id: Origin,
        certificate_hash: CryptoHash,
        height: BlockHeight,
        index: u32,
    ) -> Result<Option<MessageBundle>, WorkerError> {
        self.0.ensure_is_active()?;

        let mut inbox = self.0.chain.inboxes.try_load_entry_mut(&inbox_id).await?;
        let mut bundles = inbox.added_bundles.iter_mut().await?;

        Ok(bundles
            .find(|bundle| {
                bundle.certificate_hash == certificate_hash
                    && bundle.height == height
                    && bundle.messages.iter().any(|msg| msg.index == index)
            })
            .cloned())
    }

    /// Queries an application's state on the chain.
    pub(super) async fn query_application(
        &mut self,
        query: Query,
    ) -> Result<QueryOutcome, WorkerError> {
        self.0.ensure_is_active()?;
        let local_time = self.0.storage.clock().current_time();
        let outcome = self
            .0
            .chain
            .query_application(local_time, query, self.0.service_runtime_endpoint.as_mut())
            .await?;
        Ok(outcome)
    }

    /// Returns an application's description.
    pub(super) async fn describe_application(
        &mut self,
        application_id: ApplicationId,
    ) -> Result<ApplicationDescription, WorkerError> {
        self.0.ensure_is_active()?;
        let response = self.0.chain.describe_application(application_id).await?;
        Ok(response)
    }

    /// Executes a block without persisting any changes to the state.
    pub(super) async fn stage_block_execution(
        &mut self,
        block: ProposedBlock,
        round: Option<u32>,
        published_blobs: &[Blob],
    ) -> Result<(Block, ChainInfoResponse), WorkerError> {
        let local_time = self.0.storage.clock().current_time();
        let signer = block.authenticated_signer;
        let (_, committee) = self.0.chain.current_committee()?;
        block.check_proposal_size(committee.policy().maximum_block_proposal_size)?;

        let (outcome, _, _) =
            Box::pin(
                self.0
                    .chain
                    .execute_block(&block, local_time, round, published_blobs, None),
            )
            .await?;

        let mut response = ChainInfoResponse::new(&self.0.chain, None);
        if let Some(signer) = signer {
            response.info.requested_owner_balance = self
                .0
                .chain
                .execution_state
                .system
                .balances
                .get(&signer)
                .await?;
        }

        Ok((outcome.with(block), response))
    }

    /// Validates a proposal's signatures; returns `manager::Outcome::Skip` if we already voted
    /// for it.
    pub(super) async fn check_proposed_block(
        &self,
        proposal: &BlockProposal,
    ) -> Result<manager::Outcome, WorkerError> {
        proposal
            .check_invariants()
            .map_err(|msg| WorkerError::InvalidBlockProposal(msg.to_string()))?;
        proposal.check_signature()?;
        let BlockProposal {
            content,
            public_key,
            validated_block_certificate,
            signature: _,
        } = proposal;
        let block = &content.block;

        let owner = AccountOwner::from(*public_key);
        let chain = &self.0.chain;
        // Check the epoch.
        let (epoch, committee) = chain.current_committee()?;
        super::check_block_epoch(epoch, block.chain_id, block.epoch)?;
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
        Ok(chain.manager.check_proposed_block(proposal)?)
    }

    /// Validates and executes a block proposed to extend this chain.
    pub(super) async fn validate_proposal_content(
        &mut self,
        content: &ProposalContent,
        published_blobs: &[Blob],
    ) -> Result<Option<(BlockExecutionOutcome, Timestamp)>, WorkerError> {
        let ProposalContent {
            block,
            round,
            outcome,
        } = content;

        let local_time = self.0.storage.clock().current_time();
        ensure!(
            block.timestamp.duration_since(local_time) <= self.0.config.grace_period,
            WorkerError::InvalidTimestamp
        );
        self.0.storage.clock().sleep_until(block.timestamp).await;
        let local_time = self.0.storage.clock().current_time();

        let chain = &mut self.0.chain;
        chain
            .remove_bundles_from_inboxes(block.timestamp, &block.incoming_bundles)
            .await?;
        let outcome = if let Some(outcome) = outcome {
            outcome.clone()
        } else {
            Box::pin(chain.execute_block(
                block,
                local_time,
                round.multi_leader(),
                published_blobs,
                None,
            ))
            .await?
            .0
        };

        ensure!(
            !round.is_fast() || !outcome.has_oracle_responses(),
            WorkerError::FastBlockUsingOracles
        );
        // Check if the counters of tip_state would be valid.
        chain.tip_state.get_mut().update_counters(block, &outcome)?;
        // Verify that the resulting chain would have no unconfirmed incoming messages.
        chain.validate_incoming_bundles().await?;
        Ok(Some((outcome, local_time)))
    }

    /// Prepares a [`ChainInfoResponse`] for a [`ChainInfoQuery`].
    pub(super) async fn prepare_chain_info_response(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, WorkerError> {
        let chain = &self.0.chain;
        let mut info = ChainInfo::from(chain);
        if query.request_committees {
            info.requested_committees = Some(chain.execution_state.system.committees.get().clone());
        }
        if query.request_owner_balance == AccountOwner::CHAIN {
            info.requested_owner_balance = Some(*chain.execution_state.system.balance.get());
        } else {
            info.requested_owner_balance = chain
                .execution_state
                .system
                .balances
                .get(&query.request_owner_balance)
                .await?;
        }
        if let Some(next_block_height) = query.test_next_block_height {
            ensure!(
                chain.tip_state.get().next_block_height == next_block_height,
                WorkerError::UnexpectedBlockHeight {
                    expected_block_height: next_block_height,
                    found_block_height: chain.tip_state.get().next_block_height
                }
            );
        }
        if query.request_pending_message_bundles {
            let mut messages = Vec::new();
            let pairs = chain.inboxes.try_load_all_entries().await?;
            let action = if *chain.execution_state.system.closed.get() {
                MessageAction::Reject
            } else {
                MessageAction::Accept
            };
            for (origin, inbox) in pairs {
                for bundle in inbox.added_bundles.elements().await? {
                    messages.push(IncomingBundle {
                        origin: origin.clone(),
                        bundle,
                        action,
                    });
                }
            }

            info.requested_pending_message_bundles = messages;
        }
        if let Some(range) = query.request_sent_certificate_hashes_in_range {
            let start: usize = range.start.try_into()?;
            let end = match range.limit {
                None => chain.confirmed_log.count(),
                Some(limit) => start
                    .checked_add(usize::try_from(limit).map_err(|_| ArithmeticError::Overflow)?)
                    .ok_or(ArithmeticError::Overflow)?
                    .min(chain.confirmed_log.count()),
            };
            let keys = chain.confirmed_log.read(start..end).await?;
            info.requested_sent_certificate_hashes = keys;
        }
        if let Some(start) = query.request_received_log_excluding_first_n {
            let start = usize::try_from(start).map_err(|_| ArithmeticError::Overflow)?;
            info.requested_received_log = chain.received_log.read(start..).await?;
        }
        if query.request_manager_values {
            info.manager.add_values(&chain.manager);
        }
        Ok(ChainInfoResponse::new(info, self.0.config.key_pair()))
    }
}

impl<StorageClient> Drop for ChainWorkerStateWithTemporaryChanges<'_, StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.0.chain.rollback();
    }
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Operations that don't persist any changes to the chain state.

use linera_base::{
    data_types::{ArithmeticError, Timestamp, UserApplicationDescription},
    ensure,
    identifiers::{AccountOwner, ChannelFullName, GenericApplicationId, UserApplicationId},
};
use linera_chain::data_types::{
    BlockExecutionOutcome, ExecutedBlock, IncomingBundle, Medium, MessageAction, ProposalContent,
    ProposedBlock,
};
use linera_execution::{ChannelSubscription, Query, QueryOutcome};
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
        application_id: UserApplicationId,
    ) -> Result<UserApplicationDescription, WorkerError> {
        self.0.ensure_is_active()?;
        let response = self.0.chain.describe_application(application_id).await?;
        Ok(response)
    }

    /// Executes a block without persisting any changes to the state.
    pub(super) async fn stage_block_execution(
        &mut self,
        block: ProposedBlock,
        round: Option<u32>,
    ) -> Result<(ExecutedBlock, ChainInfoResponse), WorkerError> {
        let local_time = self.0.storage.clock().current_time();
        let signer = block.authenticated_signer;

        let executed_block = Box::pin(self.0.chain.execute_block(&block, local_time, round, None))
            .await?
            .with(block);

        let mut response = ChainInfoResponse::new(&self.0.chain, None);
        if let Some(signer) = signer {
            response.info.requested_owner_balance = self
                .0
                .chain
                .execution_state
                .system
                .balances
                .get(&AccountOwner::User(signer))
                .await?;
        }

        Ok((executed_block, response))
    }

    /// Validates a block proposed to extend this chain.
    pub(super) async fn validate_proposal_content(
        &mut self,
        content: &ProposalContent,
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
            Box::pin(chain.execute_block(block, local_time, round.multi_leader(), None)).await?
        };

        let executed_block = outcome.with(block.clone());
        ensure!(
            !round.is_fast() || !executed_block.outcome.has_oracle_responses(),
            WorkerError::FastBlockUsingOracles
        );
        // Check if the counters of tip_state would be valid.
        chain
            .tip_state
            .get_mut()
            .update_counters(block, &executed_block.outcome)?;
        // Verify that the resulting chain would have no unconfirmed incoming messages.
        chain.validate_incoming_bundles().await?;
        Ok(Some((executed_block.outcome, local_time)))
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
        if let Some(owner) = query.request_owner_balance {
            info.requested_owner_balance =
                chain.execution_state.system.balances.get(&owner).await?;
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
            let subscriptions = &chain.execution_state.system.subscriptions;
            for (origin, inbox) in pairs {
                if let Medium::Channel(ChannelFullName {
                    application_id: GenericApplicationId::System,
                    name,
                }) = &origin.medium
                {
                    let subscription = ChannelSubscription {
                        chain_id: origin.sender,
                        name: name.clone(),
                    };
                    if !subscriptions.contains(&subscription).await? {
                        continue; // We are not subscribed to this channel.
                    }
                }
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

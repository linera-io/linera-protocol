// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Operations that don't persist any changes to the chain state.

use linera_base::{
    data_types::{
        ArithmeticError, Blob, BlobContent, CompressedBytecode, Timestamp,
        UserApplicationDescription,
    },
    ensure,
    hashed::Hashed,
    identifiers::{AccountOwner, BlobType, GenericApplicationId, Owner, UserApplicationId},
};
use linera_chain::{
    data_types::{
        BlockExecutionOutcome, BlockProposal, ChannelFullName, ExecutedBlock, IncomingBundle,
        Medium, MessageAction, ProposalContent, ProposedBlock,
    },
    manager,
    types::ValidatedBlock,
};
use linera_execution::{ChannelSubscription, Query, ResourceControlPolicy, Response};
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

use super::{check_block_epoch, ChainWorkerState};
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
    ) -> Result<Response, WorkerError> {
        self.0.ensure_is_active()?;
        let local_time = self.0.storage.clock().current_time();
        let response = self
            .0
            .chain
            .query_application(local_time, query, self.0.service_runtime_endpoint.as_mut())
            .await?;
        Ok(response)
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
        proposal: ProposedBlock,
    ) -> Result<(ExecutedBlock, ChainInfoResponse), WorkerError> {
        let local_time = self.0.storage.clock().current_time();
        let signer = proposal.authenticated_signer;

        let executed_block = Box::pin(self.0.chain.execute_block(&proposal, local_time, None))
            .await?
            .with(proposal);

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
    pub(super) async fn validate_block(
        &mut self,
        proposal: &BlockProposal,
    ) -> Result<Option<(BlockExecutionOutcome, Timestamp)>, WorkerError> {
        let BlockProposal {
            content:
                ProposalContent {
                    block,
                    round,
                    outcome,
                },
            public_key,
            owner,
            blobs,
            validated_block_certificate,
            signature: _,
        } = proposal;
        ensure!(
            validated_block_certificate.is_some() == outcome.is_some(),
            WorkerError::InvalidBlockProposal(
                "Must contain a validation certificate if and only if \
                 it contains the execution outcome from a previous round"
                    .to_string()
            )
        );
        ensure!(
            *owner == Owner::from(public_key),
            WorkerError::InvalidBlockProposal("Public key does not match owner".into())
        );
        self.0.ensure_is_active()?;
        // Check the epoch.
        let (epoch, committee) = self
            .0
            .chain
            .execution_state
            .system
            .current_committee()
            .expect("chain is active");
        check_block_epoch(epoch, block.chain_id, block.epoch)?;
        let policy = committee.policy().clone();
        // Check the authentication of the block.
        ensure!(
            self.0.chain.manager.verify_owner(proposal),
            WorkerError::InvalidOwner
        );
        proposal.check_signature(*public_key)?;
        if let Some(lite_certificate) = validated_block_certificate {
            // Verify that this block has been validated by a quorum before.
            lite_certificate.check(committee)?;
        } else if let Some(signer) = block.authenticated_signer {
            // Check the authentication of the operations in the new block.
            ensure!(signer == *owner, WorkerError::InvalidSigner(signer));
        }
        // Check if the chain is ready for this new block proposal.
        // This should always pass for nodes without voting key.
        self.0.chain.tip_state.get().verify_block_chaining(block)?;
        if self.0.chain.manager.check_proposed_block(proposal)? == manager::Outcome::Skip {
            return Ok(None);
        }
        // Update the inboxes so that we can verify the provided hashed certificate values are
        // legitimately required.
        // Actual execution happens below, after other validity checks.
        self.0
            .chain
            .remove_bundles_from_inboxes(block.timestamp, &block.incoming_bundles)
            .await?;
        // Verify that no unrelated blobs were provided.
        let published_blob_ids = block.published_blob_ids();
        let provided_blob_ids = blobs.iter().map(Blob::id);
        ensure!(
            published_blob_ids.iter().copied().eq(provided_blob_ids),
            WorkerError::WrongBlobsInProposal
        );
        for blob in blobs {
            Self::check_blob_size(blob.content(), &policy)?;
        }

        let local_time = self.0.storage.clock().current_time();
        ensure!(
            block.timestamp.duration_since(local_time) <= self.0.config.grace_period,
            WorkerError::InvalidTimestamp
        );
        self.0.storage.clock().sleep_until(block.timestamp).await;
        let local_time = self.0.storage.clock().current_time();
        let outcome = if let Some(outcome) = outcome {
            outcome.clone()
        } else {
            Box::pin(self.0.chain.execute_block(block, local_time, None)).await?
        };

        let executed_block = outcome.with(block.clone());
        let required_blobs = self
            .0
            .get_required_blobs(executed_block.required_blob_ids(), blobs)
            .await?
            .into_values()
            .collect::<Vec<_>>();
        block.check_proposal_size(policy.maximum_block_proposal_size, &required_blobs)?;
        if let Some(lite_certificate) = &validated_block_certificate {
            let value = Hashed::new(ValidatedBlock::new(executed_block.clone()));
            lite_certificate
                .clone()
                .with_value(value)
                .ok_or_else(|| WorkerError::InvalidLiteCertificate)?;
        }
        ensure!(
            !round.is_fast() || !executed_block.outcome.has_oracle_responses(),
            WorkerError::FastBlockUsingOracles
        );
        // Check if the counters of tip_state would be valid.
        self.0
            .chain
            .tip_state
            .get()
            .verify_counters(block, &executed_block.outcome)?;
        // Verify that the resulting chain would have no unconfirmed incoming messages.
        self.0.chain.validate_incoming_bundles().await?;
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

    fn check_blob_size(
        content: &BlobContent,
        policy: &ResourceControlPolicy,
    ) -> Result<(), WorkerError> {
        ensure!(
            u64::try_from(content.bytes().len())
                .ok()
                .is_some_and(|size| size <= policy.maximum_blob_size),
            WorkerError::BlobTooLarge
        );
        match content.blob_type() {
            BlobType::ContractBytecode | BlobType::ServiceBytecode => {
                ensure!(
                    CompressedBytecode::decompressed_size_at_most(
                        content.bytes(),
                        policy.maximum_bytecode_size
                    )?,
                    WorkerError::BytecodeTooLarge
                );
            }
            BlobType::Data => {}
        }
        Ok(())
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

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The state and functionality of a chain worker.

use std::collections::BTreeMap;

use linera_base::{data_types::BlockHeight, ensure, identifiers::ChainId};
use linera_chain::{
    data_types::{Block, ExecutedBlock, MessageBundle, Origin, Target},
    ChainStateView,
};
use linera_execution::{
    committee::{Committee, Epoch},
    Query, Response, UserApplicationDescription, UserApplicationId,
};
use linera_storage::Storage;
use linera_views::{
    common::Context,
    views::{RootView, View, ViewError},
};
use tracing::{debug, warn};
#[cfg(with_testing)]
use {
    linera_base::identifiers::BytecodeId, linera_chain::data_types::Certificate,
    linera_execution::BytecodeLocation,
};

use super::ChainWorkerConfig;
use crate::{data_types::ChainInfoResponse, worker::WorkerError};

/// The state of the chain worker.
pub struct ChainWorkerState<StorageClient>
where
    StorageClient: Storage + Send + Sync + 'static,
    ViewError: From<StorageClient::ContextError>,
{
    config: ChainWorkerConfig,
    storage: StorageClient,
    chain: ChainStateView<StorageClient::Context>,
    knows_chain_is_active: bool,
}

impl<StorageClient> ChainWorkerState<StorageClient>
where
    StorageClient: Storage + Send + Sync + 'static,
    ViewError: From<StorageClient::ContextError>,
{
    /// Creates a new [`ChainWorkerState`] using the provided `storage` client.
    pub async fn new(
        config: ChainWorkerConfig,
        storage: StorageClient,
        chain_id: ChainId,
    ) -> Result<Self, WorkerError> {
        let chain = storage.load_chain(chain_id).await?;

        Ok(ChainWorkerState {
            config,
            storage,
            chain,
            knows_chain_is_active: false,
        })
    }

    /// Returns the [`ChainId`] of the chain handled by this worker.
    pub fn chain_id(&self) -> ChainId {
        self.chain.chain_id()
    }

    /// Returns a stored [`Certificate`] for the chain's block at the requested [`BlockHeight`].
    #[cfg(with_testing)]
    pub async fn read_certificate(
        &mut self,
        height: BlockHeight,
    ) -> Result<Option<Certificate>, WorkerError> {
        self.ensure_is_active()?;
        let certificate_hash = match self.chain.confirmed_log.get(height.try_into()?).await? {
            Some(hash) => hash,
            None => return Ok(None),
        };
        let certificate = self.storage.read_certificate(certificate_hash).await?;
        Ok(Some(certificate))
    }

    /// Queries an application's state on the chain.
    pub async fn query_application(&mut self, query: Query) -> Result<Response, WorkerError> {
        self.ensure_is_active()?;
        let response = self.chain.query_application(query).await?;
        Ok(response)
    }

    /// Returns the [`BytecodeLocation`] for the requested [`BytecodeId`], if it is known by the
    /// chain.
    #[cfg(with_testing)]
    pub async fn read_bytecode_location(
        &mut self,
        bytecode_id: BytecodeId,
    ) -> Result<Option<BytecodeLocation>, WorkerError> {
        self.ensure_is_active()?;
        let response = self.chain.read_bytecode_location(bytecode_id).await?;
        Ok(response)
    }

    /// Returns an application's description.
    pub async fn describe_application(
        &mut self,
        application_id: UserApplicationId,
    ) -> Result<UserApplicationDescription, WorkerError> {
        self.ensure_is_active()?;
        let response = self.chain.describe_application(application_id).await?;
        Ok(response)
    }

    /// Executes a block without persisting any changes to the state.
    pub async fn stage_block_execution(
        &mut self,
        block: Block,
    ) -> Result<(ExecutedBlock, ChainInfoResponse), WorkerError> {
        self.ensure_is_active()?;

        let local_time = self.storage.clock().current_time();
        let signer = block.authenticated_signer;

        let executed_block = self
            .chain
            .execute_block(&block, local_time, None)
            .await?
            .with(block);

        let mut response = ChainInfoResponse::new(&self.chain, None);
        if let Some(signer) = signer {
            response.info.requested_owner_balance = self
                .chain
                .execution_state
                .system
                .balances
                .get(&signer)
                .await?;
        }

        self.chain.rollback();

        Ok((executed_block, response))
    }

    /// Updates the chain's inboxes, receiving messages from a cross-chain update.
    pub async fn process_cross_chain_update(
        &mut self,
        origin: Origin,
        bundles: Vec<MessageBundle>,
    ) -> Result<Option<BlockHeight>, WorkerError> {
        // Only process certificates with relevant heights and epochs.
        let next_height_to_receive = self.chain.next_block_height_to_receive(&origin).await?;
        let last_anticipated_block_height =
            self.chain.last_anticipated_block_height(&origin).await?;
        let helper = CrossChainUpdateHelper::new(&self.config, &self.chain);
        let recipient = self.chain_id();
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
        let local_time = self.storage.clock().current_time();
        for bundle in bundles {
            // Update the staged chain state with the received block.
            self.chain
                .receive_message_bundle(&origin, bundle, local_time)
                .await?
        }
        if !self.config.allow_inactive_chains && !self.chain.is_active() {
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
        self.chain.save().await?;
        Ok(Some(last_updated_height))
    }

    /// Handles the cross-chain request confirming that the recipient was updated.
    pub async fn confirm_updated_recipient(
        &mut self,
        latest_heights: Vec<(Target, BlockHeight)>,
    ) -> Result<BlockHeight, WorkerError> {
        let mut height_with_fully_delivered_messages = BlockHeight::ZERO;

        for (target, height) in latest_heights {
            let fully_delivered = self
                .chain
                .mark_messages_as_received(&target, height)
                .await?
                && self.chain.all_messages_delivered_up_to(height);

            if fully_delivered && height > height_with_fully_delivered_messages {
                height_with_fully_delivered_messages = height;
            }
        }

        self.chain.save().await?;

        Ok(height_with_fully_delivered_messages)
    }

    /// Ensures that the current chain is active, returning an error otherwise.
    fn ensure_is_active(&mut self) -> Result<(), WorkerError> {
        if !self.knows_chain_is_active {
            self.chain.ensure_is_active()?;
            self.knows_chain_is_active = true;
        }
        Ok(())
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
    /// an untrusted set of validators.
    /// * In the case of validators, if the epoch(s) of the highest bundles are not
    /// trusted, we only accept bundles that contain messages that were already
    /// executed by anticipation (i.e. received in certified blocks).
    /// * Basic invariants are checked for good measure. We still crucially trust
    /// the worker of the sending chain to have verified and executed the blocks
    /// correctly.
    pub fn select_message_bundles(
        &self,
        origin: &'a Origin,
        recipient: ChainId,
        next_height_to_receive: BlockHeight,
        last_anticipated_block_height: Option<BlockHeight>,
        mut bundles: Vec<MessageBundle>,
    ) -> Result<Vec<MessageBundle>, WorkerError> {
        let mut latest_height = None;
        let mut skipped_len = 0;
        let mut trusted_len = 0;
        for (i, bundle) in bundles.iter().enumerate() {
            // Make sure that heights are increasing.
            ensure!(
                latest_height < Some(bundle.height),
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
                || Some(bundle.epoch) >= self.current_epoch
                || self.committees.contains_key(&bundle.epoch)
            {
                trusted_len = i + 1;
            }
        }
        if skipped_len > 0 {
            let sample_bundle = &bundles[skipped_len - 1];
            debug!(
                "Ignoring repeated messages to {recipient:?} from {origin:?} at height {}",
                sample_bundle.height,
            );
        }
        if skipped_len < bundles.len() && trusted_len < bundles.len() {
            let sample_bundle = &bundles[trusted_len];
            warn!(
                "Refusing messages to {recipient:?} from {origin:?} at height {} \
                 because the epoch {:?} is not trusted any more",
                sample_bundle.height, sample_bundle.epoch,
            );
        }
        let certificates = if skipped_len < trusted_len {
            bundles.drain(skipped_len..trusted_len).collect()
        } else {
            vec![]
        };
        Ok(certificates)
    }
}

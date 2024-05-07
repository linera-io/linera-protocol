// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The state and functionality of a chain worker.

use linera_base::identifiers::ChainId;
use linera_chain::{
    data_types::{Block, ExecutedBlock},
    ChainStateView,
};
use linera_execution::{Query, Response, UserApplicationDescription, UserApplicationId};
use linera_storage::Storage;
use linera_views::views::{View, ViewError};

use crate::{data_types::ChainInfoResponse, worker::WorkerError};

/// The state of the chain worker.
pub struct ChainWorkerState<StorageClient>
where
    StorageClient: Storage + Send + Sync + 'static,
    ViewError: From<StorageClient::ContextError>,
{
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
    pub async fn new(storage: StorageClient, chain_id: ChainId) -> Result<Self, WorkerError> {
        let chain = storage.load_chain(chain_id).await?;

        Ok(ChainWorkerState {
            storage,
            chain,
            knows_chain_is_active: false,
        })
    }

    /// Returns the [`ChainId`] of the chain handled by this worker.
    pub fn chain_id(&self) -> ChainId {
        self.chain.chain_id()
    }

    /// Queries an application's state on the chain.
    pub async fn query_application(&mut self, query: Query) -> Result<Response, WorkerError> {
        self.ensure_is_active()?;
        let response = self.chain.query_application(query).await?;
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

    /// Ensures that the current chain is active, returning an error otherwise.
    fn ensure_is_active(&mut self) -> Result<(), WorkerError> {
        if !self.knows_chain_is_active {
            self.chain.ensure_is_active()?;
            self.knows_chain_is_active = true;
        }
        Ok(())
    }
}

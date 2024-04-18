// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The state and functionality of a chain worker.

use linera_base::identifiers::ChainId;
use linera_chain::ChainStateView;
use linera_storage::Storage;
use linera_views::views::ViewError;

use crate::worker::WorkerError;

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

    /// Ensures that the current chain is active, returning an error otherwise.
    fn ensure_is_active(&mut self) -> Result<(), WorkerError> {
        if !self.knows_chain_is_active {
            self.chain.ensure_is_active()?;
            self.knows_chain_is_active = true;
        }
        Ok(())
    }
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An actor that runs a chain worker.

use linera_base::identifiers::ChainId;
use linera_storage::Storage;
use linera_views::views::ViewError;
use tokio::sync::mpsc;
use tracing::{instrument, trace};

use super::{config::ChainWorkerConfig, state::ChainWorkerState};
use crate::worker::WorkerError;

/// A request for the [`ChainWorkerActor`].
pub enum ChainWorkerRequest {}

/// The actor worker type.
pub struct ChainWorkerActor<StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
    ViewError: From<StorageClient::ContextError>,
{
    worker: ChainWorkerState<StorageClient>,
    incoming_requests: mpsc::UnboundedReceiver<ChainWorkerRequest>,
}

impl<StorageClient> ChainWorkerActor<StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
    ViewError: From<StorageClient::ContextError>,
{
    /// Spawns a new task to run the [`ChainWorkerActor`], returning an endpoint for sending
    /// requests to the worker.
    pub async fn spawn(
        config: ChainWorkerConfig,
        storage: StorageClient,
        chain_id: ChainId,
    ) -> Result<mpsc::UnboundedSender<ChainWorkerRequest>, WorkerError> {
        let worker = ChainWorkerState::new(config, storage, chain_id).await?;
        let (sender, receiver) = mpsc::unbounded_channel();

        let actor = ChainWorkerActor {
            worker,
            incoming_requests: receiver,
        };

        tokio::spawn(actor.run());

        Ok(sender)
    }

    /// Runs the worker until there are no more incoming requests.
    #[instrument(skip_all, fields(chain_id = format!("{:.8}", self.worker.chain_id())))]
    async fn run(mut self) {
        trace!("Starting `ChainWorkerActor`");

        while let Some(request) = self.incoming_requests.recv().await {
            match request {}
        }

        trace!("`ChainWorkerActor` finished");
    }
}

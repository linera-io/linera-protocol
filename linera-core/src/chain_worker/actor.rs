// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An actor that runs a chain worker.

use std::sync::Arc;

use linera_base::{
    crypto::CryptoHash,
    data_types::HashedBlob,
    identifiers::{BlobId, ChainId},
};
use linera_chain::data_types::{Block, ExecutedBlock, HashedCertificateValue};
use linera_execution::{Query, Response};
use linera_storage::Storage;
use linera_views::views::ViewError;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tracing::{instrument, trace};

use super::{config::ChainWorkerConfig, state::ChainWorkerState};
use crate::{
    data_types::ChainInfoResponse, value_cache::ValueCache, worker::WorkerError, JoinSetExt as _,
};

/// A request for the [`ChainWorkerActor`].
pub enum ChainWorkerRequest {
    /// Query an application's state.
    QueryApplication {
        query: Query,
        callback: oneshot::Sender<Result<Response, WorkerError>>,
    },

    /// Execute a block but discard any changes to the chain state.
    StageBlockExecution {
        block: Block,
        callback: oneshot::Sender<Result<(ExecutedBlock, ChainInfoResponse), WorkerError>>,
    },
}

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
        certificate_value_cache: Arc<ValueCache<CryptoHash, HashedCertificateValue>>,
        blob_cache: Arc<ValueCache<BlobId, HashedBlob>>,
        chain_id: ChainId,
        join_set: &mut JoinSet<()>,
    ) -> Result<mpsc::UnboundedSender<ChainWorkerRequest>, WorkerError> {
        let worker = ChainWorkerState::load(
            config,
            storage,
            certificate_value_cache,
            blob_cache,
            chain_id,
        )
        .await?;
        let (sender, receiver) = mpsc::unbounded_channel();

        let actor = ChainWorkerActor {
            worker,
            incoming_requests: receiver,
        };

        join_set.spawn_task(actor.run());

        Ok(sender)
    }

    /// Runs the worker until there are no more incoming requests.
    #[instrument(skip_all, fields(chain_id = format!("{:.8}", self.worker.chain_id())))]
    async fn run(mut self) {
        trace!("Starting `ChainWorkerActor`");

        while let Some(request) = self.incoming_requests.recv().await {
            match request {
                ChainWorkerRequest::QueryApplication { query, callback } => {
                    let _ = callback.send(self.worker.query_application(query).await);
                }
                ChainWorkerRequest::StageBlockExecution { block, callback } => {
                    let _ = callback.send(self.worker.stage_block_execution(block).await);
                }
            }
        }

        trace!("`ChainWorkerActor` finished");
    }
}

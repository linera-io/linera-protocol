// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An actor that runs a chain worker.

use std::sync::Arc;

use linera_base::{
    crypto::CryptoHash,
    data_types::{BlockHeight, HashedBlob},
    identifiers::{BlobId, ChainId},
};
use linera_chain::{
    data_types::{
        Block, BlockProposal, Certificate, ExecutedBlock, HashedCertificateValue, MessageBundle,
        Origin, Target,
    },
    ChainStateView,
};
use linera_execution::{Query, Response, UserApplicationDescription, UserApplicationId};
use linera_storage::Storage;
use linera_views::views::ViewError;
use tokio::{
    sync::{mpsc, oneshot, OwnedRwLockReadGuard},
    task::JoinSet,
};
use tracing::{instrument, trace};
#[cfg(with_testing)]
use {
    linera_base::identifiers::BytecodeId, linera_chain::data_types::Event,
    linera_execution::BytecodeLocation,
};

use super::{config::ChainWorkerConfig, state::ChainWorkerState};
use crate::{
    data_types::{ChainInfoQuery, ChainInfoResponse},
    value_cache::ValueCache,
    worker::{NetworkActions, WorkerError},
    JoinSetExt as _,
};

/// A request for the [`ChainWorkerActor`].
pub enum ChainWorkerRequest<Context>
where
    Context: linera_views::common::Context + Clone + Send + Sync + 'static,
    ViewError: From<Context::Error>,
{
    /// Reads the certificate for a requested [`BlockHeight`].
    #[cfg(with_testing)]
    ReadCertificate {
        height: BlockHeight,
        callback: oneshot::Sender<Result<Option<Certificate>, WorkerError>>,
    },

    /// Search for an event in one of the chain's inboxes.
    #[cfg(with_testing)]
    FindEventInInbox {
        inbox_id: Origin,
        certificate_hash: CryptoHash,
        height: BlockHeight,
        index: u32,
        callback: oneshot::Sender<Result<Option<Event>, WorkerError>>,
    },

    /// Request a read-only view of the [`ChainStateView`].
    GetChainStateView {
        callback:
            oneshot::Sender<Result<OwnedRwLockReadGuard<ChainStateView<Context>>, WorkerError>>,
    },

    /// Query an application's state.
    QueryApplication {
        query: Query,
        callback: oneshot::Sender<Result<Response, WorkerError>>,
    },

    /// Read the [`BytecodeLocation`] for a requested [`BytecodeId`].
    #[cfg(with_testing)]
    ReadBytecodeLocation {
        bytecode_id: BytecodeId,
        callback: oneshot::Sender<Result<Option<BytecodeLocation>, WorkerError>>,
    },

    /// Describe an application.
    DescribeApplication {
        application_id: UserApplicationId,
        callback: oneshot::Sender<Result<UserApplicationDescription, WorkerError>>,
    },

    /// Execute a block but discard any changes to the chain state.
    StageBlockExecution {
        block: Block,
        callback: oneshot::Sender<Result<(ExecutedBlock, ChainInfoResponse), WorkerError>>,
    },

    /// Process a leader timeout issued for this multi-owner chain.
    ProcessTimeout {
        certificate: Certificate,
        callback: oneshot::Sender<Result<(ChainInfoResponse, NetworkActions), WorkerError>>,
    },

    /// Handle a proposal for the next block on this chain.
    HandleBlockProposal {
        proposal: BlockProposal,
        callback: oneshot::Sender<Result<(ChainInfoResponse, NetworkActions), WorkerError>>,
    },

    /// Process a validated block issued for this multi-owner chain.
    ProcessValidatedBlock {
        certificate: Certificate,
        callback: oneshot::Sender<Result<(ChainInfoResponse, NetworkActions, bool), WorkerError>>,
    },

    /// Process a confirmed block (a commit).
    ProcessConfirmedBlock {
        certificate: Certificate,
        hashed_certificate_values: Vec<HashedCertificateValue>,
        hashed_blobs: Vec<HashedBlob>,
        callback: oneshot::Sender<Result<(ChainInfoResponse, NetworkActions), WorkerError>>,
    },

    /// Process a cross-chain update.
    ProcessCrossChainUpdate {
        origin: Origin,
        bundles: Vec<MessageBundle>,
        callback: oneshot::Sender<Result<Option<BlockHeight>, WorkerError>>,
    },

    /// Handle cross-chain request to confirm that the recipient was updated.
    ConfirmUpdatedRecipient {
        latest_heights: Vec<(Target, BlockHeight)>,
        callback: oneshot::Sender<Result<BlockHeight, WorkerError>>,
    },

    /// Handle a [`ChainInfoQuery`].
    HandleChainInfoQuery {
        query: ChainInfoQuery,
        callback: oneshot::Sender<Result<(ChainInfoResponse, NetworkActions), WorkerError>>,
    },
}

/// The actor worker type.
pub struct ChainWorkerActor<StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
    ViewError: From<StorageClient::ContextError>,
{
    worker: ChainWorkerState<StorageClient>,
    incoming_requests: mpsc::UnboundedReceiver<ChainWorkerRequest<StorageClient::Context>>,
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
    ) -> Result<mpsc::UnboundedSender<ChainWorkerRequest<StorageClient::Context>>, WorkerError>
    {
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
                #[cfg(with_testing)]
                ChainWorkerRequest::ReadCertificate { height, callback } => {
                    let _ = callback.send(self.worker.read_certificate(height).await);
                }
                #[cfg(with_testing)]
                ChainWorkerRequest::FindEventInInbox {
                    inbox_id,
                    certificate_hash,
                    height,
                    index,
                    callback,
                } => {
                    let _ = callback.send(
                        self.worker
                            .find_event_in_inbox(inbox_id, certificate_hash, height, index)
                            .await,
                    );
                }
                ChainWorkerRequest::GetChainStateView { callback } => {
                    let _ = callback.send(self.worker.chain_state_view().await);
                }
                ChainWorkerRequest::QueryApplication { query, callback } => {
                    let _ = callback.send(self.worker.query_application(query).await);
                }
                #[cfg(with_testing)]
                ChainWorkerRequest::ReadBytecodeLocation {
                    bytecode_id,
                    callback,
                } => {
                    let _ = callback.send(self.worker.read_bytecode_location(bytecode_id).await);
                }
                ChainWorkerRequest::DescribeApplication {
                    application_id,
                    callback,
                } => {
                    let _ = callback.send(self.worker.describe_application(application_id).await);
                }
                ChainWorkerRequest::StageBlockExecution { block, callback } => {
                    let _ = callback.send(self.worker.stage_block_execution(block).await);
                }
                ChainWorkerRequest::ProcessTimeout {
                    certificate,
                    callback,
                } => {
                    let _ = callback.send(self.worker.process_timeout(certificate).await);
                }
                ChainWorkerRequest::HandleBlockProposal { proposal, callback } => {
                    let _ = callback.send(self.worker.handle_block_proposal(proposal).await);
                }
                ChainWorkerRequest::ProcessValidatedBlock {
                    certificate,
                    callback,
                } => {
                    let _ = callback.send(self.worker.process_validated_block(certificate).await);
                }
                ChainWorkerRequest::ProcessConfirmedBlock {
                    certificate,
                    hashed_certificate_values,
                    hashed_blobs,
                    callback,
                } => {
                    let _ = callback.send(
                        self.worker
                            .process_confirmed_block(
                                certificate,
                                &hashed_certificate_values,
                                &hashed_blobs,
                            )
                            .await,
                    );
                }
                ChainWorkerRequest::ProcessCrossChainUpdate {
                    origin,
                    bundles,
                    callback,
                } => {
                    let _ = callback.send(
                        self.worker
                            .process_cross_chain_update(origin, bundles)
                            .await,
                    );
                }
                ChainWorkerRequest::ConfirmUpdatedRecipient {
                    latest_heights,
                    callback,
                } => {
                    let _ =
                        callback.send(self.worker.confirm_updated_recipient(latest_heights).await);
                }
                ChainWorkerRequest::HandleChainInfoQuery { query, callback } => {
                    let _ = callback.send(self.worker.handle_chain_info_query(query).await);
                }
            }
        }

        trace!("`ChainWorkerActor` finished");
    }
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An actor that runs a chain worker.

use std::{
    fmt::{self, Debug, Formatter},
    sync::Arc,
};

use linera_base::{
    crypto::CryptoHash,
    data_types::{BlockHeight, HashedBlob, Timestamp},
    identifiers::{BlobId, ChainId},
};
use linera_chain::{
    data_types::{
        Block, BlockProposal, Certificate, ExecutedBlock, HashedCertificateValue, MessageBundle,
        Origin, Target,
    },
    ChainStateView,
};
use linera_execution::{
    ExecutionRequest, Query, QueryContext, Response, ServiceRuntimeRequest, ServiceSyncRuntime,
    UserApplicationDescription, UserApplicationId,
};
use linera_storage::Storage;
use linera_views::views::ViewError;
use tokio::{
    sync::{mpsc, oneshot, OwnedRwLockReadGuard},
    task::{JoinHandle, JoinSet},
};
use tracing::{instrument, trace, warn};
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
    ViewError: From<StorageClient::StoreError>,
{
    worker: ChainWorkerState<StorageClient>,
    incoming_requests: mpsc::UnboundedReceiver<ChainWorkerRequest<StorageClient::Context>>,
    service_runtime_thread: JoinHandle<()>,
}

impl<StorageClient> ChainWorkerActor<StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
    ViewError: From<StorageClient::StoreError>,
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
        let (service_runtime_thread, execution_state_receiver, runtime_request_sender) =
            Self::spawn_service_runtime_actor(chain_id);

        let worker = ChainWorkerState::load(
            config,
            storage,
            certificate_value_cache,
            blob_cache,
            chain_id,
            execution_state_receiver,
            runtime_request_sender,
        )
        .await?;

        let (sender, receiver) = mpsc::unbounded_channel();
        let actor = ChainWorkerActor {
            worker,
            incoming_requests: receiver,
            service_runtime_thread,
        };

        join_set.spawn_task(actor.run(tracing::Span::current()));

        Ok(sender)
    }

    /// Spawns a blocking task to execute the service runtime actor.
    ///
    /// Returns the task handle and the endpoints to interact with the actor.
    fn spawn_service_runtime_actor(
        chain_id: ChainId,
    ) -> (
        JoinHandle<()>,
        futures::channel::mpsc::UnboundedReceiver<ExecutionRequest>,
        std::sync::mpsc::Sender<ServiceRuntimeRequest>,
    ) {
        let context = QueryContext {
            chain_id,
            next_block_height: BlockHeight(0),
            local_time: Timestamp::from(0),
        };

        let (execution_state_sender, execution_state_receiver) =
            futures::channel::mpsc::unbounded();
        let (runtime_request_sender, runtime_request_receiver) = std::sync::mpsc::channel();

        let service_runtime_thread = tokio::task::spawn_blocking(move || {
            ServiceSyncRuntime::new(execution_state_sender, context).run(runtime_request_receiver)
        });

        (
            service_runtime_thread,
            execution_state_receiver,
            runtime_request_sender,
        )
    }

    /// Runs the worker until there are no more incoming requests.
    #[instrument(
        name = "ChainWorkerActor",
        follows_from = [spawner_span],
        skip_all,
        fields(chain_id = format!("{:.8}", self.worker.chain_id())),
    )]
    async fn run(mut self, spawner_span: tracing::Span) {
        trace!("Starting `ChainWorkerActor`");

        while let Some(request) = self.incoming_requests.recv().await {
            // TODO(#2237): Spawn concurrent tasks for read-only operations
            trace!("Handling `ChainWorkerRequest`: {request:?}");

            let responded = match request {
                #[cfg(with_testing)]
                ChainWorkerRequest::ReadCertificate { height, callback } => callback
                    .send(self.worker.read_certificate(height).await)
                    .is_ok(),
                #[cfg(with_testing)]
                ChainWorkerRequest::FindEventInInbox {
                    inbox_id,
                    certificate_hash,
                    height,
                    index,
                    callback,
                } => callback
                    .send(
                        self.worker
                            .find_event_in_inbox(inbox_id, certificate_hash, height, index)
                            .await,
                    )
                    .is_ok(),
                ChainWorkerRequest::GetChainStateView { callback } => {
                    callback.send(self.worker.chain_state_view().await).is_ok()
                }
                ChainWorkerRequest::QueryApplication { query, callback } => callback
                    .send(self.worker.query_application(query).await)
                    .is_ok(),
                #[cfg(with_testing)]
                ChainWorkerRequest::ReadBytecodeLocation {
                    bytecode_id,
                    callback,
                } => callback
                    .send(self.worker.read_bytecode_location(bytecode_id).await)
                    .is_ok(),
                ChainWorkerRequest::DescribeApplication {
                    application_id,
                    callback,
                } => callback
                    .send(self.worker.describe_application(application_id).await)
                    .is_ok(),
                ChainWorkerRequest::StageBlockExecution { block, callback } => callback
                    .send(self.worker.stage_block_execution(block).await)
                    .is_ok(),
                ChainWorkerRequest::ProcessTimeout {
                    certificate,
                    callback,
                } => callback
                    .send(self.worker.process_timeout(certificate).await)
                    .is_ok(),
                ChainWorkerRequest::HandleBlockProposal { proposal, callback } => callback
                    .send(self.worker.handle_block_proposal(proposal).await)
                    .is_ok(),
                ChainWorkerRequest::ProcessValidatedBlock {
                    certificate,
                    callback,
                } => callback
                    .send(self.worker.process_validated_block(certificate).await)
                    .is_ok(),
                ChainWorkerRequest::ProcessConfirmedBlock {
                    certificate,
                    hashed_certificate_values,
                    hashed_blobs,
                    callback,
                } => callback
                    .send(
                        self.worker
                            .process_confirmed_block(
                                certificate,
                                &hashed_certificate_values,
                                &hashed_blobs,
                            )
                            .await,
                    )
                    .is_ok(),
                ChainWorkerRequest::ProcessCrossChainUpdate {
                    origin,
                    bundles,
                    callback,
                } => callback
                    .send(
                        self.worker
                            .process_cross_chain_update(origin, bundles)
                            .await,
                    )
                    .is_ok(),
                ChainWorkerRequest::ConfirmUpdatedRecipient {
                    latest_heights,
                    callback,
                } => callback
                    .send(self.worker.confirm_updated_recipient(latest_heights).await)
                    .is_ok(),
                ChainWorkerRequest::HandleChainInfoQuery { query, callback } => callback
                    .send(self.worker.handle_chain_info_query(query).await)
                    .is_ok(),
            };

            if !responded {
                warn!("Callback for `ChainWorkerActor` was dropped before a response was sent");
            }
        }

        drop(self.worker);
        self.service_runtime_thread
            .await
            .expect("Service runtime thread should not panic");

        trace!("`ChainWorkerActor` finished");
    }
}

impl<Context> Debug for ChainWorkerRequest<Context>
where
    Context: linera_views::common::Context + Clone + Send + Sync + 'static,
    ViewError: From<Context::Error>,
{
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        match self {
            #[cfg(with_testing)]
            ChainWorkerRequest::ReadCertificate {
                height,
                callback: _callback,
            } => formatter
                .debug_struct("ChainWorkerRequest::ReadCertificate")
                .field("height", &height)
                .finish_non_exhaustive(),
            #[cfg(with_testing)]
            ChainWorkerRequest::FindEventInInbox {
                inbox_id,
                certificate_hash,
                height,
                index,
                callback: _callback,
            } => formatter
                .debug_struct("ChainWorkerRequest::FindEventInInbox")
                .field("inbox_id", &inbox_id)
                .field("certificate_hash", &certificate_hash)
                .field("height", &height)
                .field("index", &index)
                .finish_non_exhaustive(),
            ChainWorkerRequest::GetChainStateView {
                callback: _callback,
            } => formatter
                .debug_struct("ChainWorkerRequest::GetChainStateView")
                .finish_non_exhaustive(),
            ChainWorkerRequest::QueryApplication {
                query,
                callback: _callback,
            } => formatter
                .debug_struct("ChainWorkerRequest::QueryApplication")
                .field("query", &query)
                .finish_non_exhaustive(),
            #[cfg(with_testing)]
            ChainWorkerRequest::ReadBytecodeLocation {
                bytecode_id,
                callback: _callback,
            } => formatter
                .debug_struct("ChainWorkerRequest::ReadBytecodeLocation")
                .field("bytecode_id", &bytecode_id)
                .finish_non_exhaustive(),
            ChainWorkerRequest::DescribeApplication {
                application_id,
                callback: _callback,
            } => formatter
                .debug_struct("ChainWorkerRequest::DescribeApplication")
                .field("application_id", &application_id)
                .finish_non_exhaustive(),
            ChainWorkerRequest::StageBlockExecution {
                block,
                callback: _callback,
            } => formatter
                .debug_struct("ChainWorkerRequest::StageBlockExecution")
                .field("block", &block)
                .finish_non_exhaustive(),
            ChainWorkerRequest::ProcessTimeout {
                certificate,
                callback: _callback,
            } => formatter
                .debug_struct("ChainWorkerRequest::ProcessTimeout")
                .field("certificate", &certificate)
                .finish_non_exhaustive(),
            ChainWorkerRequest::HandleBlockProposal {
                proposal,
                callback: _callback,
            } => formatter
                .debug_struct("ChainWorkerRequest::HandleBlockProposal")
                .field("proposal", &proposal)
                .finish_non_exhaustive(),
            ChainWorkerRequest::ProcessValidatedBlock {
                certificate,
                callback: _callback,
            } => formatter
                .debug_struct("ChainWorkerRequest::ProcessValidatedBlock")
                .field("certificate", &certificate)
                .finish_non_exhaustive(),
            ChainWorkerRequest::ProcessConfirmedBlock {
                certificate,
                hashed_certificate_values,
                hashed_blobs,
                callback: _callback,
            } => formatter
                .debug_struct("ChainWorkerRequest::ProcessConfirmedBlock")
                .field("certificate", &certificate)
                .field("hashed_certificate_values", &hashed_certificate_values)
                .field("hashed_blobs", &hashed_blobs)
                .finish_non_exhaustive(),
            ChainWorkerRequest::ProcessCrossChainUpdate {
                origin,
                bundles,
                callback: _callback,
            } => formatter
                .debug_struct("ChainWorkerRequest::ProcessCrossChainUpdate")
                .field("origin", &origin)
                .field("bundles", &bundles)
                .finish_non_exhaustive(),
            ChainWorkerRequest::ConfirmUpdatedRecipient {
                latest_heights,
                callback: _callback,
            } => formatter
                .debug_struct("ChainWorkerRequest::ConfirmUpdatedRecipient")
                .field("latest_heights", &latest_heights)
                .finish_non_exhaustive(),
            ChainWorkerRequest::HandleChainInfoQuery {
                query,
                callback: _callback,
            } => formatter
                .debug_struct("ChainWorkerRequest::HandleChainInfoQuery")
                .field("query", &query)
                .finish_non_exhaustive(),
        }
    }
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An actor that runs a chain worker.

use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    sync::{Arc, RwLock},
};

use custom_debug_derive::Debug;
use linera_base::{
    crypto::{CryptoHash, ValidatorPublicKey},
    data_types::{Blob, BlockHeight, Timestamp, UserApplicationDescription},
    hashed::Hashed,
    identifiers::{BlobId, ChainId, UserApplicationId},
};
use linera_chain::{
    data_types::{BlockProposal, ExecutedBlock, MessageBundle, Origin, ProposedBlock, Target},
    types::{Block, ConfirmedBlockCertificate, TimeoutCertificate, ValidatedBlockCertificate},
    ChainStateView,
};
use linera_execution::{
    committee::Epoch, Query, QueryContext, QueryOutcome, ServiceRuntimeEndpoint, ServiceSyncRuntime,
};
use linera_storage::Storage;
use tokio::sync::{mpsc, oneshot, OwnedRwLockReadGuard};
use tracing::{debug, instrument, trace, warn, Instrument as _};

use super::{config::ChainWorkerConfig, state::ChainWorkerState, DeliveryNotifier};
use crate::{
    data_types::{ChainInfoQuery, ChainInfoResponse},
    value_cache::ValueCache,
    worker::{NetworkActions, WorkerError},
};

/// A request for the [`ChainWorkerActor`].
#[derive(Debug)]
pub enum ChainWorkerRequest<Context>
where
    Context: linera_views::context::Context + Clone + Send + Sync + 'static,
{
    /// Reads the certificate for a requested [`BlockHeight`].
    #[cfg(with_testing)]
    ReadCertificate {
        height: BlockHeight,
        #[debug(skip)]
        callback: oneshot::Sender<Result<Option<ConfirmedBlockCertificate>, WorkerError>>,
    },

    /// Search for a bundle in one of the chain's inboxes.
    #[cfg(with_testing)]
    FindBundleInInbox {
        inbox_id: Origin,
        certificate_hash: CryptoHash,
        height: BlockHeight,
        index: u32,
        #[debug(skip)]
        callback: oneshot::Sender<Result<Option<MessageBundle>, WorkerError>>,
    },

    /// Request a read-only view of the [`ChainStateView`].
    GetChainStateView {
        #[debug(skip)]
        callback:
            oneshot::Sender<Result<OwnedRwLockReadGuard<ChainStateView<Context>>, WorkerError>>,
    },

    /// Query an application's state.
    QueryApplication {
        query: Query,
        #[debug(skip)]
        callback: oneshot::Sender<Result<QueryOutcome, WorkerError>>,
    },

    /// Describe an application.
    DescribeApplication {
        application_id: UserApplicationId,
        #[debug(skip)]
        callback: oneshot::Sender<Result<UserApplicationDescription, WorkerError>>,
    },

    /// Execute a block but discard any changes to the chain state.
    StageBlockExecution {
        block: ProposedBlock,
        round: Option<u32>,
        published_blobs: Vec<Blob>,
        #[debug(skip)]
        callback: oneshot::Sender<Result<(ExecutedBlock, ChainInfoResponse), WorkerError>>,
    },

    /// Process a leader timeout issued for this multi-owner chain.
    ProcessTimeout {
        certificate: TimeoutCertificate,
        #[debug(skip)]
        callback: oneshot::Sender<Result<(ChainInfoResponse, NetworkActions), WorkerError>>,
    },

    /// Handle a proposal for the next block on this chain.
    HandleBlockProposal {
        proposal: BlockProposal,
        #[debug(skip)]
        callback: oneshot::Sender<Result<(ChainInfoResponse, NetworkActions), WorkerError>>,
    },

    /// Process a validated block issued for this multi-owner chain.
    ProcessValidatedBlock {
        certificate: ValidatedBlockCertificate,
        #[debug(skip)]
        callback: oneshot::Sender<Result<(ChainInfoResponse, NetworkActions, bool), WorkerError>>,
    },

    /// Process a confirmed block (a commit).
    ProcessConfirmedBlock {
        certificate: ConfirmedBlockCertificate,
        #[debug(with = "elide_option")]
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
        #[debug(skip)]
        callback: oneshot::Sender<Result<(ChainInfoResponse, NetworkActions), WorkerError>>,
    },

    /// Process a cross-chain update.
    ProcessCrossChainUpdate {
        origin: Origin,
        bundles: Vec<(Epoch, MessageBundle)>,
        #[debug(skip)]
        callback: oneshot::Sender<Result<Option<(BlockHeight, NetworkActions)>, WorkerError>>,
    },

    /// Handle cross-chain request to confirm that the recipient was updated.
    ConfirmUpdatedRecipient {
        latest_heights: Vec<(Target, BlockHeight)>,
        #[debug(skip)]
        callback: oneshot::Sender<Result<(), WorkerError>>,
    },

    /// Handle a [`ChainInfoQuery`].
    HandleChainInfoQuery {
        query: ChainInfoQuery,
        #[debug(skip)]
        callback: oneshot::Sender<Result<(ChainInfoResponse, NetworkActions), WorkerError>>,
    },

    /// Get a blob if it belongs to the current locking block or pending proposal.
    DownloadPendingBlob {
        blob_id: BlobId,
        #[debug(skip)]
        callback: oneshot::Sender<Result<Blob, WorkerError>>,
    },

    /// Handle a blob that belongs to a pending proposal or validated block certificate.
    HandlePendingBlob {
        blob: Blob,
        #[debug(skip)]
        callback: oneshot::Sender<Result<ChainInfoResponse, WorkerError>>,
    },

    /// Update the received certificate trackers to at least the given values.
    UpdateReceivedCertificateTrackers {
        new_trackers: BTreeMap<ValidatorPublicKey, u64>,
        callback: oneshot::Sender<Result<(), WorkerError>>,
    },
}

/// The actor worker type.
pub struct ChainWorkerActor<StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
{
    worker: ChainWorkerState<StorageClient>,
    service_runtime_thread: Option<linera_base::task::Blocking>,
}

impl<StorageClient> ChainWorkerActor<StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
{
    /// Runs the [`ChainWorkerActor`], first by loading the chain state from `storage` then
    /// handling all `incoming_requests` as they arrive.
    ///
    /// If loading the chain state fails the next request will receive the error reported by the
    /// `storage`, and the actor will then try again to load the state.
    pub async fn run(
        config: ChainWorkerConfig,
        storage: StorageClient,
        executed_block_cache: Arc<ValueCache<CryptoHash, Hashed<Block>>>,
        tracked_chains: Option<Arc<RwLock<HashSet<ChainId>>>>,
        delivery_notifier: DeliveryNotifier,
        chain_id: ChainId,
        mut incoming_requests: mpsc::UnboundedReceiver<(
            ChainWorkerRequest<StorageClient::Context>,
            tracing::Span,
        )>,
    ) {
        let actor = loop {
            let load_result = Self::load(
                config.clone(),
                storage.clone(),
                executed_block_cache.clone(),
                tracked_chains.clone(),
                delivery_notifier.clone(),
                chain_id,
            )
            .await
            .inspect_err(|error| warn!("Failed to load chain state: {error:?}"));

            match load_result {
                Ok(actor) => break actor,
                Err(error) => match incoming_requests.recv().await {
                    Some((request, _span)) => request.send_error(error),
                    None => return,
                },
            }
        };

        actor.handle_requests(incoming_requests).await;
    }

    /// Creates a [`ChainWorkerActor`], loading it with the chain state for the requested
    /// [`ChainId`].
    pub async fn load(
        config: ChainWorkerConfig,
        storage: StorageClient,
        executed_block_cache: Arc<ValueCache<CryptoHash, Hashed<Block>>>,
        tracked_chains: Option<Arc<RwLock<HashSet<ChainId>>>>,
        delivery_notifier: DeliveryNotifier,
        chain_id: ChainId,
    ) -> Result<Self, WorkerError> {
        let (service_runtime_thread, service_runtime_endpoint) = {
            if config.long_lived_services {
                let (thread, endpoint) = Self::spawn_service_runtime_actor(chain_id).await;
                (Some(thread), Some(endpoint))
            } else {
                (None, None)
            }
        };

        let worker = ChainWorkerState::load(
            config,
            storage,
            executed_block_cache,
            tracked_chains,
            delivery_notifier,
            chain_id,
            service_runtime_endpoint,
        )
        .await?;

        Ok(ChainWorkerActor {
            worker,
            service_runtime_thread,
        })
    }

    /// Spawns a blocking task to execute the service runtime actor.
    ///
    /// Returns the task handle and the endpoints to interact with the actor.
    async fn spawn_service_runtime_actor(
        chain_id: ChainId,
    ) -> (linera_base::task::Blocking, ServiceRuntimeEndpoint) {
        let context = QueryContext {
            chain_id,
            next_block_height: BlockHeight(0),
            local_time: Timestamp::from(0),
        };

        let (execution_state_sender, incoming_execution_requests) =
            futures::channel::mpsc::unbounded();
        let (runtime_request_sender, runtime_request_receiver) = std::sync::mpsc::channel();

        let service_runtime_thread = linera_base::task::Blocking::spawn(move |_| async move {
            ServiceSyncRuntime::new(execution_state_sender, context).run(runtime_request_receiver)
        })
        .await;

        let endpoint = ServiceRuntimeEndpoint {
            incoming_execution_requests,
            runtime_request_sender,
        };
        (service_runtime_thread, endpoint)
    }

    /// Runs the worker until there are no more incoming requests.
    #[instrument(
        name = "ChainWorkerActor::handle_requests",
        skip_all,
        fields(chain_id = format!("{:.8}", self.worker.chain_id())),
    )]
    pub async fn handle_requests(
        mut self,
        mut incoming_requests: mpsc::UnboundedReceiver<(
            ChainWorkerRequest<StorageClient::Context>,
            tracing::Span,
        )>,
    ) {
        trace!("Starting `ChainWorkerActor`");

        while let Some((request, span)) = incoming_requests.recv().await {
            Box::pin(self.handle_request(request).instrument(span)).await;
        }

        if let Some(thread) = self.service_runtime_thread {
            drop(self.worker);
            thread.join().await
        }

        trace!("`ChainWorkerActor` finished");
    }

    /// Runs the worker until there are no more incoming requests.
    #[instrument(skip(self))]
    pub async fn handle_request(&mut self, request: ChainWorkerRequest<StorageClient::Context>) {
        // TODO(#2237): Spawn concurrent tasks for read-only operations
        let responded = match request {
            #[cfg(with_testing)]
            ChainWorkerRequest::ReadCertificate { height, callback } => callback
                .send(self.worker.read_certificate(height).await)
                .is_ok(),
            #[cfg(with_testing)]
            ChainWorkerRequest::FindBundleInInbox {
                inbox_id,
                certificate_hash,
                height,
                index,
                callback,
            } => callback
                .send(
                    self.worker
                        .find_bundle_in_inbox(inbox_id, certificate_hash, height, index)
                        .await,
                )
                .is_ok(),
            ChainWorkerRequest::GetChainStateView { callback } => {
                callback.send(self.worker.chain_state_view().await).is_ok()
            }
            ChainWorkerRequest::QueryApplication { query, callback } => callback
                .send(self.worker.query_application(query).await)
                .is_ok(),
            ChainWorkerRequest::DescribeApplication {
                application_id,
                callback,
            } => callback
                .send(self.worker.describe_application(application_id).await)
                .is_ok(),
            ChainWorkerRequest::StageBlockExecution {
                block,
                round,
                published_blobs,
                callback,
            } => callback
                .send(
                    self.worker
                        .stage_block_execution(block, round, &published_blobs)
                        .await,
                )
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
                notify_when_messages_are_delivered,
                callback,
            } => callback
                .send(
                    self.worker
                        .process_confirmed_block(certificate, notify_when_messages_are_delivered)
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
            ChainWorkerRequest::DownloadPendingBlob { blob_id, callback } => callback
                .send(self.worker.download_pending_blob(blob_id).await)
                .is_ok(),
            ChainWorkerRequest::HandlePendingBlob { blob, callback } => callback
                .send(self.worker.handle_pending_blob(blob).await)
                .is_ok(),
            ChainWorkerRequest::UpdateReceivedCertificateTrackers {
                new_trackers,
                callback,
            } => callback
                .send(
                    self.worker
                        .update_received_certificate_trackers(new_trackers)
                        .await,
                )
                .is_ok(),
        };

        if !responded {
            warn!("Callback for `ChainWorkerActor` was dropped before a response was sent");
        }
    }
}

impl<Context> ChainWorkerRequest<Context>
where
    Context: linera_views::context::Context + Clone + Send + Sync + 'static,
{
    /// Responds to this request with an `error`.
    pub fn send_error(self, error: WorkerError) {
        debug!("Immediately sending error to chain worker request {self:?}");

        let responded = match self {
            #[cfg(with_testing)]
            ChainWorkerRequest::ReadCertificate { callback, .. } => {
                callback.send(Err(error)).is_ok()
            }
            #[cfg(with_testing)]
            ChainWorkerRequest::FindBundleInInbox { callback, .. } => {
                callback.send(Err(error)).is_ok()
            }
            ChainWorkerRequest::GetChainStateView { callback } => callback.send(Err(error)).is_ok(),
            ChainWorkerRequest::QueryApplication { callback, .. } => {
                callback.send(Err(error)).is_ok()
            }
            ChainWorkerRequest::DescribeApplication { callback, .. } => {
                callback.send(Err(error)).is_ok()
            }
            ChainWorkerRequest::StageBlockExecution { callback, .. } => {
                callback.send(Err(error)).is_ok()
            }
            ChainWorkerRequest::ProcessTimeout { callback, .. } => {
                callback.send(Err(error)).is_ok()
            }
            ChainWorkerRequest::HandleBlockProposal { callback, .. } => {
                callback.send(Err(error)).is_ok()
            }
            ChainWorkerRequest::ProcessValidatedBlock { callback, .. } => {
                callback.send(Err(error)).is_ok()
            }
            ChainWorkerRequest::ProcessConfirmedBlock { callback, .. } => {
                callback.send(Err(error)).is_ok()
            }
            ChainWorkerRequest::ProcessCrossChainUpdate { callback, .. } => {
                callback.send(Err(error)).is_ok()
            }
            ChainWorkerRequest::ConfirmUpdatedRecipient { callback, .. } => {
                callback.send(Err(error)).is_ok()
            }
            ChainWorkerRequest::HandleChainInfoQuery { callback, .. } => {
                callback.send(Err(error)).is_ok()
            }
            ChainWorkerRequest::DownloadPendingBlob { callback, .. } => {
                callback.send(Err(error)).is_ok()
            }
            ChainWorkerRequest::HandlePendingBlob { callback, .. } => {
                callback.send(Err(error)).is_ok()
            }
            ChainWorkerRequest::UpdateReceivedCertificateTrackers { callback, .. } => {
                callback.send(Err(error)).is_ok()
            }
        };

        if !responded {
            warn!("Callback for `ChainWorkerActor` was dropped before a response was sent");
        }
    }
}

/// Writes an option as `Some(..)` or `None`.
fn elide_option<T>(option: &Option<T>, f: &mut fmt::Formatter) -> fmt::Result {
    match option {
        Some(_) => write!(f, "Some(..)"),
        None => write!(f, "None"),
    }
}

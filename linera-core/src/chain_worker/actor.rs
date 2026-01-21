// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An actor that runs a chain worker.

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    sync::{self, Arc, RwLock},
};

use custom_debug_derive::Debug;
use futures::FutureExt;
use linera_base::{
    crypto::{CryptoHash, ValidatorPublicKey},
    data_types::{ApplicationDescription, Blob, BlockHeight, Epoch, TimeDelta, Timestamp},
    hashed::Hashed,
    identifiers::{ApplicationId, BlobId, ChainId, StreamId},
    task,
    time::Instant,
};
use linera_chain::{
    data_types::{BlockProposal, MessageBundle, ProposedBlock},
    types::{Block, ConfirmedBlockCertificate, TimeoutCertificate, ValidatedBlockCertificate},
    ChainStateView,
};
use linera_execution::{
    system::EventSubscriptions, ExecutionStateView, Query, QueryContext, QueryOutcome,
    ResourceTracker, ServiceRuntimeEndpoint, ServiceSyncRuntime,
};
use linera_storage::{Clock as _, Storage};
use linera_views::context::{Context, InactiveContext};
use tokio::sync::{mpsc, oneshot, OwnedRwLockReadGuard};
use tracing::{instrument, trace, Instrument as _};

use super::{config::ChainWorkerConfig, state::ChainWorkerState, DeliveryNotifier};
use crate::{
    chain_worker::BlockOutcome,
    client::ListeningMode,
    data_types::{ChainInfoQuery, ChainInfoResponse},
    value_cache::ValueCache,
    worker::{NetworkActions, WorkerError},
};

/// Type alias for event subscriptions result.
pub(crate) type EventSubscriptionsResult = Vec<((ChainId, StreamId), EventSubscriptions)>;

/// Type alias for the request channel sender.
pub(crate) type ChainWorkerRequestSender<Ctx> =
    mpsc::UnboundedSender<(ChainWorkerRequest<Ctx>, tracing::Span, Instant)>;

/// Type alias for the request channel receiver.
pub(crate) type ChainWorkerRequestReceiver<Ctx> =
    mpsc::UnboundedReceiver<(ChainWorkerRequest<Ctx>, tracing::Span, Instant)>;

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{
        exponential_bucket_interval, register_histogram, register_int_gauge,
    };
    use prometheus::{Histogram, IntGauge};

    pub static CHAIN_WORKER_REQUEST_QUEUE_WAIT_TIME: LazyLock<Histogram> = LazyLock::new(|| {
        register_histogram(
            "chain_worker_request_queue_wait_time",
            "Time (ms) a chain worker request waits in queue before being processed",
            exponential_bucket_interval(0.1_f64, 10_000.0),
        )
    });

    /// Number of active chain worker actor tasks (outer loop of handle_requests).
    pub static CHAIN_WORKER_ACTORS_ACTIVE: LazyLock<IntGauge> = LazyLock::new(|| {
        register_int_gauge(
            "chain_worker_actors_active",
            "Number of active chain worker actor tasks",
        )
    });

    /// Number of chain workers with chain state loaded in memory (inner loop of handle_requests).
    pub static CHAIN_WORKER_STATES_LOADED: LazyLock<IntGauge> = LazyLock::new(|| {
        register_int_gauge(
            "chain_worker_states_loaded",
            "Number of chain workers with chain state loaded in memory",
        )
    });
}

/// A request for the [`ChainWorkerActor`].
#[derive(Debug)]
pub(crate) enum ChainWorkerRequest<Ctx>
where
    Ctx: Context + Clone + 'static,
{
    /// Reads the certificate for a requested [`BlockHeight`].
    #[cfg(with_testing)]
    ReadCertificate {
        height: BlockHeight,
        #[debug(skip)]
        callback: oneshot::Sender<Result<Option<ConfirmedBlockCertificate>, WorkerError>>,
    },

    /// Request a read-only view of the [`ChainStateView`].
    GetChainStateView {
        #[debug(skip)]
        callback: oneshot::Sender<Result<OwnedRwLockReadGuard<ChainStateView<Ctx>>, WorkerError>>,
    },

    /// Query an application's state.
    QueryApplication {
        query: Query,
        block_hash: Option<CryptoHash>,
        #[debug(skip)]
        callback: oneshot::Sender<Result<QueryOutcome, WorkerError>>,
    },

    /// Describe an application.
    DescribeApplication {
        application_id: ApplicationId,
        #[debug(skip)]
        callback: oneshot::Sender<Result<ApplicationDescription, WorkerError>>,
    },

    /// Execute a block but discard any changes to the chain state.
    StageBlockExecution {
        block: ProposedBlock,
        round: Option<u32>,
        published_blobs: Vec<Blob>,
        #[debug(skip)]
        callback: oneshot::Sender<Result<(Block, ChainInfoResponse, ResourceTracker), WorkerError>>,
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
        callback:
            oneshot::Sender<Result<(ChainInfoResponse, NetworkActions, BlockOutcome), WorkerError>>,
    },

    /// Process a confirmed block (a commit).
    ProcessConfirmedBlock {
        certificate: ConfirmedBlockCertificate,
        #[debug(with = "elide_option")]
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
        #[debug(skip)]
        callback:
            oneshot::Sender<Result<(ChainInfoResponse, NetworkActions, BlockOutcome), WorkerError>>,
    },

    /// Process a cross-chain update.
    ProcessCrossChainUpdate {
        origin: ChainId,
        bundles: Vec<(Epoch, MessageBundle)>,
        #[debug(skip)]
        callback: oneshot::Sender<Result<Option<BlockHeight>, WorkerError>>,
    },

    /// Handle cross-chain request to confirm that the recipient was updated.
    ConfirmUpdatedRecipient {
        recipient: ChainId,
        latest_height: BlockHeight,
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

    /// Get preprocessed block hashes in a given height range.
    GetPreprocessedBlockHashes {
        start: BlockHeight,
        end: BlockHeight,
        #[debug(skip)]
        callback: oneshot::Sender<Result<Vec<CryptoHash>, WorkerError>>,
    },

    /// Get the next block height to receive from an inbox.
    GetInboxNextHeight {
        origin: ChainId,
        #[debug(skip)]
        callback: oneshot::Sender<Result<BlockHeight, WorkerError>>,
    },

    /// Get locking blobs for specific blob IDs.
    GetLockingBlobs {
        blob_ids: Vec<BlobId>,
        #[debug(skip)]
        callback: oneshot::Sender<Result<Option<Vec<Blob>>, WorkerError>>,
    },

    /// Get block hashes for specified heights.
    GetBlockHashes {
        heights: Vec<BlockHeight>,
        #[debug(skip)]
        callback: oneshot::Sender<Result<Vec<CryptoHash>, WorkerError>>,
    },

    /// Get proposed blobs from the manager for specified blob IDs.
    GetProposedBlobs {
        blob_ids: Vec<BlobId>,
        #[debug(skip)]
        callback: oneshot::Sender<Result<Vec<Blob>, WorkerError>>,
    },

    /// Get event subscriptions as a list of ((ChainId, StreamId), EventSubscriptions).
    GetEventSubscriptions {
        #[debug(skip)]
        callback: oneshot::Sender<Result<EventSubscriptionsResult, WorkerError>>,
    },

    /// Get the next expected event index for a stream.
    GetNextExpectedEvent {
        stream_id: StreamId,
        #[debug(skip)]
        callback: oneshot::Sender<Result<Option<u32>, WorkerError>>,
    },

    /// Get received certificate trackers.
    GetReceivedCertificateTrackers {
        #[debug(skip)]
        callback: oneshot::Sender<Result<HashMap<ValidatorPublicKey, u64>, WorkerError>>,
    },

    /// Get tip state info for next_outbox_heights calculation.
    GetTipStateAndOutboxInfo {
        receiver_id: ChainId,
        #[debug(skip)]
        callback: oneshot::Sender<Result<(BlockHeight, Option<BlockHeight>), WorkerError>>,
    },

    /// Get the next height to preprocess.
    GetNextHeightToPreprocess {
        #[debug(skip)]
        callback: oneshot::Sender<Result<BlockHeight, WorkerError>>,
    },
}

/// The actor worker type.
pub(crate) struct ChainWorkerActor<StorageClient>
where
    StorageClient: Storage + Clone + 'static,
{
    chain_id: ChainId,
    config: ChainWorkerConfig,
    storage: StorageClient,
    block_values: Arc<ValueCache<CryptoHash, Hashed<Block>>>,
    execution_state_cache: Arc<ValueCache<CryptoHash, ExecutionStateView<InactiveContext>>>,
    chain_modes: Option<Arc<sync::RwLock<BTreeMap<ChainId, ListeningMode>>>>,
    delivery_notifier: DeliveryNotifier,
    is_tracked: bool,
}

struct ServiceRuntimeActor {
    task: web_thread_pool::Task<()>,
    endpoint: ServiceRuntimeEndpoint,
}

impl ServiceRuntimeActor {
    /// Spawns a blocking task to execute the service runtime actor.
    ///
    /// Returns the task handle and the endpoints to interact with the actor.
    async fn spawn(chain_id: ChainId, thread_pool: &linera_execution::ThreadPool) -> Self {
        let (execution_state_sender, incoming_execution_requests) =
            futures::channel::mpsc::unbounded();
        let (runtime_request_sender, runtime_request_receiver) = std::sync::mpsc::channel();

        Self {
            endpoint: ServiceRuntimeEndpoint {
                incoming_execution_requests,
                runtime_request_sender,
            },
            task: thread_pool
                .run((), move |()| async move {
                    ServiceSyncRuntime::new(
                        execution_state_sender,
                        QueryContext {
                            chain_id,
                            next_block_height: BlockHeight(0),
                            local_time: Timestamp::from(0),
                        },
                    )
                    .run(runtime_request_receiver)
                })
                .await,
        }
    }
}

impl<StorageClient> ChainWorkerActor<StorageClient>
where
    StorageClient: Storage + Clone + 'static,
{
    /// Runs the [`ChainWorkerActor`]. The chain state is loaded when the first request
    /// arrives.
    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn run(
        config: ChainWorkerConfig,
        storage: StorageClient,
        block_values: Arc<ValueCache<CryptoHash, Hashed<Block>>>,
        execution_state_cache: Arc<ValueCache<CryptoHash, ExecutionStateView<InactiveContext>>>,
        chain_modes: Option<Arc<RwLock<BTreeMap<ChainId, ListeningMode>>>>,
        delivery_notifier: DeliveryNotifier,
        chain_id: ChainId,
        request_sender: ChainWorkerRequestSender<StorageClient::Context>,
        request_receiver: ChainWorkerRequestReceiver<StorageClient::Context>,
        is_tracked: bool,
    ) {
        #[cfg(with_metrics)]
        metrics::CHAIN_WORKER_ACTORS_ACTIVE.inc();
        let actor = ChainWorkerActor {
            config,
            storage,
            block_values,
            execution_state_cache,
            chain_modes,
            delivery_notifier,
            chain_id,
            is_tracked,
        };
        if let Err(err) = actor
            .handle_requests(request_sender, request_receiver)
            .await
        {
            tracing::error!("Chain actor error: {err}");
        }
        #[cfg(with_metrics)]
        metrics::CHAIN_WORKER_ACTORS_ACTIVE.dec();
    }

    /// Returns the TTL timeout timestamp.
    fn ttl_timeout(&self) -> Timestamp {
        let now = self.storage.clock().current_time();
        let timeout = if self.is_tracked {
            self.config.sender_chain_ttl
        } else {
            self.config.ttl
        };
        let ttl = TimeDelta::from_micros(u64::try_from(timeout.as_micros()).unwrap_or(u64::MAX));
        now.saturating_add(ttl)
    }

    /// Checks if a block proposal should be delayed because its timestamp is in the future.
    ///
    /// Returns `Some(timestamp)` if the proposal should be delayed until that timestamp,
    /// or `None` if it should be processed immediately (either because the timestamp is
    /// not in the future, or because it's beyond the grace period and should error).
    fn delay_until(&self, proposal: &BlockProposal) -> Option<Timestamp> {
        let block_timestamp = proposal.content.block.timestamp;
        let now = self.storage.clock().current_time();
        let delta = block_timestamp.delta_since(now);

        // Only delay if the timestamp is in the future but within the grace period.
        // If it's beyond the grace period, process immediately to return an error.
        // This prevents malicious clients from filling our delay queue with far-future proposals.
        let grace_period = TimeDelta::from_micros(
            u64::try_from(self.config.block_time_grace_period.as_micros()).unwrap_or(u64::MAX),
        );
        (delta > TimeDelta::ZERO && delta <= grace_period).then_some(block_timestamp)
    }

    /// If the request is a block proposal that should be delayed, spawns a task to
    /// re-queue it and returns `None`. Otherwise, records queue wait metrics and
    /// returns the request for immediate processing.
    fn preprocess_request(
        &self,
        request: ChainWorkerRequest<StorageClient::Context>,
        span: tracing::Span,
        queued_at: Instant,
        request_sender: &ChainWorkerRequestSender<StorageClient::Context>,
    ) -> Option<(
        ChainWorkerRequest<StorageClient::Context>,
        tracing::Span,
        Instant,
    )> {
        // Check if this request should be delayed.
        if let ChainWorkerRequest::HandleBlockProposal { ref proposal, .. } = request {
            if let Some(delay_until) = self.delay_until(proposal) {
                tracing::debug!(%delay_until, "delaying block proposal");
                let sender = request_sender.clone();
                let clock = self.storage.clock().clone();
                task::spawn(async move {
                    clock.sleep_until(delay_until).await;
                    // Re-insert the request into the queue. If the channel is closed,
                    // the actor is shutting down, so we can ignore the error.
                    sender.send((request, span, queued_at)).ok();
                })
                .forget();
                return None;
            }
        }

        // Record how long the request waited in queue (in milliseconds).
        #[cfg(with_metrics)]
        {
            let queue_wait_time_ms = queued_at.elapsed().as_secs_f64() * 1000.0;
            metrics::CHAIN_WORKER_REQUEST_QUEUE_WAIT_TIME.observe(queue_wait_time_ms);
        }

        Some((request, span, queued_at))
    }

    /// Runs the worker until there are no more incoming requests.
    #[instrument(
        skip_all,
        fields(chain_id = format!("{:.8}", self.chain_id), long_lived_services = %self.config.long_lived_services),
    )]
    async fn handle_requests(
        self,
        request_sender: ChainWorkerRequestSender<StorageClient::Context>,
        mut incoming_requests: ChainWorkerRequestReceiver<StorageClient::Context>,
    ) -> Result<(), WorkerError> {
        trace!("Starting `ChainWorkerActor`");

        while let Some((request, span, queued_at)) = incoming_requests.recv().await {
            let Some((request, span, _)) =
                self.preprocess_request(request, span, queued_at, &request_sender)
            else {
                continue;
            };

            let (service_runtime_task, service_runtime_endpoint) =
                if self.config.long_lived_services {
                    let actor =
                        ServiceRuntimeActor::spawn(self.chain_id, self.storage.thread_pool()).await;
                    (Some(actor.task), Some(actor.endpoint))
                } else {
                    (None, None)
                };

            trace!("Loading chain state of {}", self.chain_id);
            let mut worker = ChainWorkerState::load(
                self.config.clone(),
                self.storage.clone(),
                self.block_values.clone(),
                self.execution_state_cache.clone(),
                self.chain_modes.clone(),
                self.delivery_notifier.clone(),
                self.chain_id,
                service_runtime_endpoint,
            )
            .instrument(span.clone())
            .await?;
            #[cfg(with_metrics)]
            metrics::CHAIN_WORKER_STATES_LOADED.inc();

            Box::pin(worker.handle_request(request))
                .instrument(span)
                .await;

            loop {
                let ttl_timeout = self.ttl_timeout();

                futures::select! {
                    () = self.storage.clock().sleep_until(ttl_timeout).fuse() => {
                        break;
                    }
                    maybe_request = incoming_requests.recv().fuse() => {
                        let Some((request, span, queued_at)) = maybe_request else {
                            break; // Request sender was dropped.
                        };
                        let Some((request, span, _)) =
                            self.preprocess_request(request, span, queued_at, &request_sender)
                        else {
                            continue;
                        };

                        Box::pin(worker.handle_request(request)).instrument(span).await;
                    }
                }
            }

            trace!("Unloading chain state of {} ...", self.chain_id);
            worker.clear_shared_chain_view().await;
            drop(worker);
            #[cfg(with_metrics)]
            metrics::CHAIN_WORKER_STATES_LOADED.dec();
            if let Some(task) = service_runtime_task {
                task.await?;
            }
            trace!("Done unloading chain state of {}", self.chain_id);
        }

        trace!("`ChainWorkerActor` finished");
        Ok(())
    }
}

/// Writes an option as `Some(..)` or `None`.
fn elide_option<T>(option: &Option<T>, f: &mut fmt::Formatter) -> fmt::Result {
    match option {
        Some(_) => write!(f, "Some(..)"),
        None => write!(f, "None"),
    }
}

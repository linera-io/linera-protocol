// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An actor that runs a chain worker.

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    sync::{self, Arc, RwLock},
};

use custom_debug_derive::Debug;
use futures::{
    stream::{FuturesUnordered, StreamExt as _},
    FutureExt,
};
use linera_base::{
    crypto::{CryptoHash, ValidatorPublicKey},
    data_types::{ApplicationDescription, Blob, BlockHeight, Epoch, TimeDelta, Timestamp},
    hashed::Hashed,
    identifiers::{ApplicationId, BlobId, ChainId, StreamId},
    task,
    time::Instant,
};
use linera_chain::{
    data_types::{BlockProposal, BundleExecutionPolicy, MessageBundle, ProposedBlock},
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

use super::{config::ChainWorkerConfig, state::ChainWorkerState, BoxedFuture, DeliveryNotifier};
use crate::{
    chain_worker::BlockOutcome,
    client::ListeningMode,
    data_types::{ChainInfoQuery, ChainInfoResponse},
    value_cache::ValueCache,
    worker::{NetworkActions, WorkerError},
};

/// Type alias for event subscriptions result.
pub(crate) type EventSubscriptionsResult = Vec<((ChainId, StreamId), EventSubscriptions)>;

/// A request item sent through a channel.
type RequestItem<Ctx> = (ChainWorkerRequest<Ctx>, tracing::Span, Instant);

/// The endpoint for sending requests to a [`ChainWorkerActor`].
///
/// Read-only requests and write requests use separate channels so that
/// the actor can schedule them with a fairness policy.
#[derive(Clone)]
pub(crate) struct ChainActorEndpoint<Ctx: Context + Clone + 'static> {
    read_sender: mpsc::UnboundedSender<RequestItem<Ctx>>,
    write_sender: mpsc::UnboundedSender<RequestItem<Ctx>>,
}

impl<Ctx: Context + Clone + 'static> ChainActorEndpoint<Ctx> {
    /// Creates a new endpoint from the read and write senders.
    pub(crate) fn new(
        read_sender: mpsc::UnboundedSender<RequestItem<Ctx>>,
        write_sender: mpsc::UnboundedSender<RequestItem<Ctx>>,
    ) -> Self {
        Self {
            read_sender,
            write_sender,
        }
    }

    /// Sends a request to the appropriate channel based on whether it's read-only.
    pub(crate) fn send(
        &self,
        item: RequestItem<Ctx>,
    ) -> Result<(), Box<mpsc::error::SendError<RequestItem<Ctx>>>> {
        let result = if item.0.is_read_only() {
            self.read_sender.send(item)
        } else {
            self.write_sender.send(item)
        };
        result.map_err(Box::new)
    }
}

/// The receiver side of the dual-channel endpoint.
pub(crate) struct ChainActorReceivers<Ctx: Context + Clone + 'static> {
    read_receiver: mpsc::UnboundedReceiver<RequestItem<Ctx>>,
    write_receiver: mpsc::UnboundedReceiver<RequestItem<Ctx>>,
}

impl<Ctx: Context + Clone + 'static> ChainActorReceivers<Ctx> {
    /// Creates new receivers from the read and write receivers.
    pub(crate) fn new(
        read_receiver: mpsc::UnboundedReceiver<RequestItem<Ctx>>,
        write_receiver: mpsc::UnboundedReceiver<RequestItem<Ctx>>,
    ) -> Self {
        Self {
            read_receiver,
            write_receiver,
        }
    }
}

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

    /// Execute a block with a policy for handling bundle failures.
    /// The block may be modified (bundles rejected or removed) based on the policy.
    StageBlockExecutionWithPolicy {
        block: ProposedBlock,
        round: Option<u32>,
        published_blobs: Vec<Blob>,
        policy: BundleExecutionPolicy,
        #[debug(skip)]
        callback: oneshot::Sender<
            Result<(ProposedBlock, Block, ChainInfoResponse, ResourceTracker), WorkerError>,
        >,
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

impl<Ctx: Context + Clone + 'static> ChainWorkerRequest<Ctx> {
    /// Whether this request is a concurrent-safe read that can run concurrently
    /// via `FuturesUnordered`. Read-only requests are routed to the read channel
    /// for concurrent execution.
    fn is_read_only(&self) -> bool {
        match self {
            // Simple getters: take `&self`, only read from ChainStateView.
            ChainWorkerRequest::DownloadPendingBlob { .. }
            | ChainWorkerRequest::GetPreprocessedBlockHashes { .. }
            | ChainWorkerRequest::GetInboxNextHeight { .. }
            | ChainWorkerRequest::GetLockingBlobs { .. }
            | ChainWorkerRequest::GetBlockHashes { .. }
            | ChainWorkerRequest::GetProposedBlobs { .. }
            | ChainWorkerRequest::GetEventSubscriptions { .. }
            | ChainWorkerRequest::GetNextExpectedEvent { .. }
            | ChainWorkerRequest::GetReceivedCertificateTrackers { .. }
            | ChainWorkerRequest::GetTipStateAndOutboxInfo { .. }
            | ChainWorkerRequest::GetNextHeightToPreprocess { .. } => true,
            // ReadCertificate: chain is eagerly initialized, so only needs &self.
            #[cfg(with_testing)]
            ChainWorkerRequest::ReadCertificate { .. } => true,
            // GetChainStateView: shared_chain_view is pre-initialized before
            // each read batch.
            ChainWorkerRequest::GetChainStateView { .. } => true,
            // DescribeApplication: reads the blob directly from storage,
            // skipping blob tracking (which is always rolled back for reads).
            ChainWorkerRequest::DescribeApplication { .. } => true,
            // QueryApplication: clones chain state for concurrent execution.
            // Requires preparation (&mut self for clone_unchecked), but the
            // resulting owned future runs concurrently.
            ChainWorkerRequest::QueryApplication { .. } => true,
            // All remaining variants mutate chain state.
            ChainWorkerRequest::StageBlockExecution { .. }
            | ChainWorkerRequest::StageBlockExecutionWithPolicy { .. }
            | ChainWorkerRequest::ProcessTimeout { .. }
            | ChainWorkerRequest::HandleBlockProposal { .. }
            | ChainWorkerRequest::ProcessValidatedBlock { .. }
            | ChainWorkerRequest::ProcessConfirmedBlock { .. }
            | ChainWorkerRequest::ProcessCrossChainUpdate { .. }
            | ChainWorkerRequest::ConfirmUpdatedRecipient { .. }
            | ChainWorkerRequest::HandleChainInfoQuery { .. }
            | ChainWorkerRequest::UpdateReceivedCertificateTrackers { .. }
            | ChainWorkerRequest::HandlePendingBlob { .. } => false,
        }
    }

    /// Whether this read-only request requires a preparation step with `&mut self`
    /// before it can run concurrently. Currently only `QueryApplication` needs this
    /// (to clone the chain state via `clone_unchecked`).
    fn needs_preparation(&self) -> bool {
        matches!(self, ChainWorkerRequest::QueryApplication { .. })
    }
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
        endpoint: ChainActorEndpoint<StorageClient::Context>,
        receivers: ChainActorReceivers<StorageClient::Context>,
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
        if let Err(err) = actor.handle_requests(endpoint, receivers).await {
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
        endpoint: &ChainActorEndpoint<StorageClient::Context>,
    ) -> Option<(
        ChainWorkerRequest<StorageClient::Context>,
        tracing::Span,
        Instant,
    )> {
        // Check if this request should be delayed.
        if let ChainWorkerRequest::HandleBlockProposal { ref proposal, .. } = request {
            if let Some(delay_until) = self.delay_until(proposal) {
                tracing::debug!(%delay_until, "delaying block proposal");
                let endpoint = endpoint.clone();
                let clock = self.storage.clock().clone();
                task::spawn(async move {
                    clock.sleep_until(delay_until).await;
                    // Re-insert the request into the queue. If the channel is closed,
                    // the actor is shutting down, so we can ignore the error.
                    endpoint.send((request, span, queued_at)).ok();
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
    ///
    /// Prioritizes write requests (block processing, cross-chain updates, etc.) over
    /// concurrent-safe reads (simple getters on chain state). After draining all pending
    /// writes, all pending reads are collected and executed concurrently via
    /// `FuturesUnordered`, overlapping their storage I/O. When both channels have
    /// requests ready, writes are selected first.
    #[instrument(
        skip_all,
        fields(chain_id = format!("{:.8}", self.chain_id), long_lived_services = %self.config.long_lived_services),
    )]
    async fn handle_requests(
        self,
        endpoint: ChainActorEndpoint<StorageClient::Context>,
        mut receivers: ChainActorReceivers<StorageClient::Context>,
    ) -> Result<(), WorkerError> {
        trace!("Starting `ChainWorkerActor`");

        // Outer loop: wait for first request, load state, process until TTL.
        loop {
            // Wait for the first request from either channel.
            let first_request = futures::select! {
                req = receivers.write_receiver.recv().fuse() => req,
                req = receivers.read_receiver.recv().fuse() => req,
            };
            let Some((request, span, queued_at)) = first_request else {
                break; // Both channels closed.
            };
            let Some((request, span, _)) =
                self.preprocess_request(request, span, queued_at, &endpoint)
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

            // Eagerly initialize the chain so that read-only methods
            // (ReadCertificate, QueryApplication, etc.) don't need &mut self
            // for initialization. For inactive chains (not yet opened via
            // cross-chain messages), this may fail, which is fine — the chain
            // will be initialized when the activating request arrives.
            if let Err(err) = worker.initialize_and_save_if_needed().await {
                trace!("Eager initialization skipped for {}: {err}", self.chain_id);
            }

            // Process the first request.
            Box::pin(worker.handle_request(request))
                .instrument(span)
                .await;

            // Inner loop: write-priority scheduling.
            loop {
                // Phase 1: Drain all pending writes.
                while let Ok((request, span, queued_at)) = receivers.write_receiver.try_recv() {
                    if let Some((request, span, _)) =
                        self.preprocess_request(request, span, queued_at, &endpoint)
                    {
                        Box::pin(worker.handle_request(request))
                            .instrument(span)
                            .await;
                    }
                }

                // Phase 2: Collect and process all pending reads concurrently.
                // Two-step approach: first prepare (needs &mut worker for
                // QueryApplication clone and shared_chain_view init), then
                // execute all futures concurrently.
                {
                    let mut pending_reads = Vec::new();
                    while let Ok(item) = receivers.read_receiver.try_recv() {
                        pending_reads.push(item);
                    }
                    if !pending_reads.is_empty() {
                        // Step A: Prepare (needs &mut worker).
                        worker.ensure_shared_chain_view_initialized()?;

                        let mut owned_futures = Vec::new();
                        let mut simple_items = Vec::new();
                        for (request, span, queued_at) in pending_reads {
                            if let Some((request, span, _)) =
                                self.preprocess_request(request, span, queued_at, &endpoint)
                            {
                                if request.needs_preparation() {
                                    if let ChainWorkerRequest::QueryApplication {
                                        query,
                                        block_hash,
                                        callback,
                                    } = request
                                    {
                                        owned_futures.push((
                                            worker.prepare_query_application(
                                                query, block_hash, callback,
                                            ),
                                            span,
                                        ));
                                    }
                                } else {
                                    simple_items.push((request, span));
                                }
                            }
                        }
                        // &mut worker borrow ends here.

                        // Step B: Execute all concurrently.
                        let mut read_futures: FuturesUnordered<BoxedFuture<'_>> =
                            FuturesUnordered::new();
                        for (request, span) in simple_items {
                            read_futures.push(Box::pin(
                                worker.handle_read_request(request).instrument(span),
                            ));
                        }
                        for (fut, span) in owned_futures {
                            read_futures.push(Box::pin(fut.instrument(span)));
                        }
                        while read_futures.next().await.is_some() {}
                        continue; // Back to phase 1 to check for writes.
                    }
                }

                // Phase 3: Nothing available — block on either channel or TTL.
                let ttl_timeout = self.ttl_timeout();

                futures::select! {
                    () = self.storage.clock().sleep_until(ttl_timeout).fuse() => {
                        break; // TTL expired, unload state.
                    }
                    maybe_request = receivers.write_receiver.recv().fuse() => {
                        let Some((request, span, queued_at)) = maybe_request else {
                            break;
                        };
                        if let Some((request, span, _)) =
                            self.preprocess_request(request, span, queued_at, &endpoint)
                        {
                            Box::pin(worker.handle_request(request))
                                .instrument(span)
                                .await;
                        }
                    }
                    maybe_request = receivers.read_receiver.recv().fuse() => {
                        let Some((request, span, queued_at)) = maybe_request else {
                            break;
                        };
                        if let Some((request, span, _)) =
                            self.preprocess_request(request, span, queued_at, &endpoint)
                        {
                            if request.needs_preparation() {
                                if let ChainWorkerRequest::QueryApplication {
                                    query,
                                    block_hash,
                                    callback,
                                } = request
                                {
                                    let fut = worker.prepare_query_application(
                                        query, block_hash, callback,
                                    );
                                    fut.instrument(span).await;
                                }
                            } else {
                                if matches!(
                                    request,
                                    ChainWorkerRequest::GetChainStateView { .. }
                                ) {
                                    worker.ensure_shared_chain_view_initialized()?;
                                }
                                worker
                                    .handle_read_request(request)
                                    .instrument(span)
                                    .await;
                            }
                        }
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

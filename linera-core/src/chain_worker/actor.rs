// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An actor that runs a chain worker.

use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    pin::Pin,
    sync::{self, Arc, RwLock},
};

use custom_debug_derive::Debug;
use futures::{stream::Peekable, FutureExt as _, StreamExt as _};
use linera_base::{
    crypto::{CryptoHash, ValidatorPublicKey},
    data_types::{ApplicationDescription, Blob, BlockHeight, Epoch, TimeDelta, Timestamp},
    hashed::Hashed,
    identifiers::{ApplicationId, BlobId, ChainId},
    time::Instant,
};
use linera_chain::{
    data_types::{BlockProposal, MessageBundle, ProposedBlock},
    types::{Block, ConfirmedBlockCertificate, TimeoutCertificate, ValidatedBlockCertificate},
    ChainError, ChainStateView,
};
use linera_execution::{
    ExecutionStateView, Query, QueryContext, QueryOutcome, ServiceRuntimeEndpoint,
    ServiceSyncRuntime,
};
use linera_storage::{Clock as _, Storage};
use linera_views::context::InactiveContext;
use tokio::sync::{mpsc, oneshot, OwnedRwLockReadGuard};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, instrument, trace, Instrument as _};

use super::{config::ChainWorkerConfig, state::ChainWorkerState, DeliveryNotifier};
use crate::{
    chain_worker::BlockOutcome,
    data_types::{ChainInfoQuery, ChainInfoResponse},
    value_cache::ValueCache,
    worker::{NetworkActions, WorkerError},
};

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{exponential_bucket_interval, register_histogram};
    use prometheus::Histogram;

    pub static CHAIN_WORKER_REQUEST_QUEUE_WAIT_TIME: LazyLock<Histogram> = LazyLock::new(|| {
        register_histogram(
            "chain_worker_request_queue_wait_time",
            "Time (ms) a chain worker request waits in queue before being processed",
            exponential_bucket_interval(0.1_f64, 10_000.0),
        )
    });
}

/// A request for the [`ChainWorkerActor`].
#[derive(Debug)]
pub(crate) enum ChainWorkerRequest<Context>
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

    /// Request a read-only view of the [`ChainStateView`].
    GetChainStateView {
        #[debug(skip)]
        callback:
            oneshot::Sender<Result<OwnedRwLockReadGuard<ChainStateView<Context>>, WorkerError>>,
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
        callback: oneshot::Sender<Result<(Block, ChainInfoResponse), WorkerError>>,
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

    /// Read a range from the confirmed log.
    ReadConfirmedLog {
        start: BlockHeight,
        end: BlockHeight,
        #[debug(skip)]
        callback: oneshot::Sender<Result<Vec<CryptoHash>, WorkerError>>,
    },
}

/// A request to process a cross-chain update.
#[derive(Debug)]
pub(crate) struct CrossChainUpdateRequest {
    pub origin: ChainId,
    pub bundles: Vec<(Epoch, MessageBundle)>,
    #[debug(skip)]
    pub callback: oneshot::Sender<Result<Option<BlockHeight>, WorkerError>>,
}

/// A request to confirm that a recipient was updated.
#[derive(Debug)]
pub(crate) struct ConfirmUpdatedRecipientRequest {
    pub recipient: ChainId,
    pub latest_height: BlockHeight,
    #[debug(skip)]
    pub callback: oneshot::Sender<Result<(), WorkerError>>,
}

/// A callback sender for cross-chain update results, mapped by origin chain ID.
type CrossChainUpdateCallbacks =
    BTreeMap<ChainId, Vec<oneshot::Sender<Result<Option<BlockHeight>, WorkerError>>>>;

/// The type of request to process next in the actor loop.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RequestType {
    /// A cross-chain update request.
    CrossChainUpdate,
    /// A confirmation that a recipient was updated.
    Confirmation,
    /// A regular chain worker request.
    Regular,
}

/// The receiver endpoints for a [`ChainWorkerActor`].
pub(crate) struct ChainActorReceivers<Context>
where
    Context: linera_views::context::Context + Clone + Send + Sync + 'static,
{
    /// Receiver for regular chain worker requests.
    pub requests: mpsc::UnboundedReceiver<(ChainWorkerRequest<Context>, tracing::Span, Instant)>,
    /// Receiver for cross-chain update requests.
    pub cross_chain_updates:
        mpsc::UnboundedReceiver<(CrossChainUpdateRequest, tracing::Span, Instant)>,
    /// Receiver for confirmation requests.
    pub confirmations:
        mpsc::UnboundedReceiver<(ConfirmUpdatedRecipientRequest, tracing::Span, Instant)>,
}

/// The actor worker type.
pub(crate) struct ChainWorkerActor<StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
{
    chain_id: ChainId,
    config: ChainWorkerConfig,
    storage: StorageClient,
    block_values: Arc<ValueCache<CryptoHash, Hashed<Block>>>,
    execution_state_cache: Arc<ValueCache<CryptoHash, ExecutionStateView<InactiveContext>>>,
    tracked_chains: Option<Arc<sync::RwLock<HashSet<ChainId>>>>,
    delivery_notifier: DeliveryNotifier,
    is_tracked: bool,
}

struct ServiceRuntimeActor {
    thread: web_thread::Thread,
    task: web_thread::Task<()>,
    endpoint: ServiceRuntimeEndpoint,
}

impl ServiceRuntimeActor {
    /// Spawns a blocking task to execute the service runtime actor.
    ///
    /// Returns the task handle and the endpoints to interact with the actor.
    async fn spawn(chain_id: ChainId) -> Self {
        let (execution_state_sender, incoming_execution_requests) =
            futures::channel::mpsc::unbounded();
        let (runtime_request_sender, runtime_request_receiver) = std::sync::mpsc::channel();

        let thread = web_thread::Thread::new();

        Self {
            endpoint: ServiceRuntimeEndpoint {
                incoming_execution_requests,
                runtime_request_sender,
            },
            task: thread.run((), move |()| async move {
                ServiceSyncRuntime::new(
                    execution_state_sender,
                    QueryContext {
                        chain_id,
                        next_block_height: BlockHeight(0),
                        local_time: Timestamp::from(0),
                    },
                )
                .run(runtime_request_receiver)
            }),
            thread,
        }
    }
}

impl<StorageClient> ChainWorkerActor<StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
{
    /// Runs the [`ChainWorkerActor`]. The chain state is loaded when the first request
    /// arrives.
    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn run(
        config: ChainWorkerConfig,
        storage: StorageClient,
        block_values: Arc<ValueCache<CryptoHash, Hashed<Block>>>,
        execution_state_cache: Arc<ValueCache<CryptoHash, ExecutionStateView<InactiveContext>>>,
        tracked_chains: Option<Arc<RwLock<HashSet<ChainId>>>>,
        delivery_notifier: DeliveryNotifier,
        chain_id: ChainId,
        receivers: ChainActorReceivers<StorageClient::Context>,
        is_tracked: bool,
    ) {
        let actor = ChainWorkerActor {
            config,
            storage,
            block_values,
            execution_state_cache,
            tracked_chains,
            delivery_notifier,
            chain_id,
            is_tracked,
        };
        if let Err(err) = actor.handle_requests(receivers).await {
            tracing::error!("Chain actor error: {err}");
        }
    }

    /// Sleeps for the configured TTL.
    pub(super) async fn sleep_until_timeout(&self) {
        let now = self.storage.clock().current_time();
        let timeout = if self.is_tracked {
            self.config.sender_chain_ttl
        } else {
            self.config.ttl
        };
        let ttl = TimeDelta::from_micros(u64::try_from(timeout.as_micros()).unwrap_or(u64::MAX));
        let timeout = now.saturating_add(ttl);
        self.storage.clock().sleep_until(timeout).await
    }

    /// Runs the worker until there are no more incoming requests.
    #[instrument(
        skip_all,
        fields(chain_id = format!("{:.8}", self.chain_id), long_lived_services = %self.config.long_lived_services),
    )]
    async fn handle_requests(
        self,
        receivers: ChainActorReceivers<StorageClient::Context>,
    ) -> Result<(), WorkerError> {
        trace!("Starting `ChainWorkerActor`");

        let regular_batch_size = self.config.regular_request_batch_size;
        let cross_chain_batch_size = self.config.cross_chain_update_batch_size;

        // The first iteration waits indefinitely; subsequent iterations have a timeout.
        let mut first_iteration = true;

        // The chain worker state, loaded lazily.
        let mut worker: Option<ChainWorkerState<StorageClient>> = None;
        let mut service_runtime_task = None;
        #[allow(unused)]
        let mut service_runtime_thread = None;

        // Convert receivers to peekable streams so we can wait without consuming.
        let mut requests = UnboundedReceiverStream::new(receivers.requests).peekable();
        let mut cross_chain_updates =
            UnboundedReceiverStream::new(receivers.cross_chain_updates).peekable();
        let mut confirmations = UnboundedReceiverStream::new(receivers.confirmations).peekable();

        // Rotate through request types: Regular -> CrossChainUpdate -> Confirmation -> ...
        let mut next_type_idx = 0;

        fn is_ready<T>(stream: &mut Peekable<UnboundedReceiverStream<T>>) -> bool {
            Pin::new(stream).peek().now_or_never().is_some()
        }

        loop {
            // Check which streams have data ready.
            let types = [
                (RequestType::Confirmation, is_ready(&mut confirmations)),
                (
                    RequestType::CrossChainUpdate,
                    is_ready(&mut cross_chain_updates),
                ),
                (RequestType::Regular, is_ready(&mut requests)),
            ];

            // Find the next ready queue in rotation order.
            let Some((type_idx, request_type)) = types
                .iter()
                .enumerate()
                .cycle()
                .skip(next_type_idx)
                .take(types.len())
                .find_map(|(idx, (request_type, is_ready))| {
                    is_ready.then_some((idx, request_type))
                })
            else {
                // No requests available, wait on all queues.
                // On the first iteration, wait indefinitely. Otherwise, use a timeout.
                let timeout_future = if first_iteration {
                    futures::future::pending().left_future()
                } else {
                    self.sleep_until_timeout().right_future()
                };

                // Wait for any stream to have data or timeout.
                let dropped = futures::select! {
                    () = timeout_future.fuse() => {
                        // Timeout: unload chain state.
                        if let Some(mut w) = worker.take() {
                            trace!("Unloading chain state of {} ...", self.chain_id);
                            w.clear_shared_chain_view().await;
                            drop(w);
                            if let Some(task) = service_runtime_task.take() {
                                task.await?;
                            }
                            service_runtime_thread = None;
                            trace!("Done unloading chain state of {}", self.chain_id);
                        }
                        first_iteration = true;
                        continue;
                    },
                    result = Pin::new(&mut cross_chain_updates).peek().fuse() => result.is_none(),
                    result = Pin::new(&mut confirmations).peek().fuse() => result.is_none(),
                    result = Pin::new(&mut requests).peek().fuse() => result.is_none(),
                };

                if dropped {
                    break;
                }
                // After waking, re-evaluate at top of loop.
                continue;
            };

            first_iteration = false;

            // Advance rotation to next type.
            next_type_idx = (type_idx + 1) % types.len();

            // Load chain state if not already loaded.
            let worker = match &mut worker {
                Some(w) => w,
                None => {
                    let service_runtime_endpoint = if self.config.long_lived_services {
                        let actor = ServiceRuntimeActor::spawn(self.chain_id).await;
                        service_runtime_thread = Some(actor.thread);
                        service_runtime_task = Some(actor.task);
                        Some(actor.endpoint)
                    } else {
                        None
                    };

                    trace!("Loading chain state of {}", self.chain_id);
                    worker = Some(
                        ChainWorkerState::load(
                            self.config.clone(),
                            self.storage.clone(),
                            self.block_values.clone(),
                            self.execution_state_cache.clone(),
                            self.tracked_chains.clone(),
                            self.delivery_notifier.clone(),
                            self.chain_id,
                            service_runtime_endpoint,
                        )
                        .await?,
                    );
                    worker.as_mut().unwrap()
                }
            };

            // Process the request based on type.
            match request_type {
                RequestType::CrossChainUpdate => {
                    // Drain requests and group by origin, merging bundles.
                    let mut updates: BTreeMap<ChainId, Vec<(Epoch, MessageBundle)>> =
                        BTreeMap::new();
                    let mut callbacks_by_origin: CrossChainUpdateCallbacks = BTreeMap::new();
                    let mut count = 0;

                    while count < cross_chain_batch_size {
                        match Pin::new(&mut cross_chain_updates).next().now_or_never() {
                            Some(Some((req, _span, _queued_at))) => {
                                #[cfg(with_metrics)]
                                {
                                    let wait_ms = _queued_at.elapsed().as_secs_f64() * 1000.0;
                                    metrics::CHAIN_WORKER_REQUEST_QUEUE_WAIT_TIME.observe(wait_ms);
                                }
                                updates.entry(req.origin).or_default().extend(req.bundles);
                                callbacks_by_origin
                                    .entry(req.origin)
                                    .or_default()
                                    .push(req.callback);
                                count += 1;
                            }
                            None | Some(None) => break,
                        }
                    }

                    if updates.is_empty() {
                        tracing::error!("cross-chain update queue empty; this is a bug!");
                        continue; // Queue was empty after all.
                    }
                    trace!("batching {count} cross-chain updates");

                    // Sort and deduplicate bundles for each origin.
                    for bundles in updates.values_mut() {
                        // Sort by height, then transaction_index, then epoch (descending).
                        bundles.sort_by(|(epoch_a, a), (epoch_b, b)| {
                            a.height
                                .cmp(&b.height)
                                .then_with(|| a.transaction_index.cmp(&b.transaction_index))
                                .then_with(|| epoch_b.cmp(epoch_a))
                        });
                        // Deduplicate by (height, transaction_index), keeping latest epoch.
                        bundles.dedup_by(|(_, a), (_, b)| {
                            a.height == b.height && a.transaction_index == b.transaction_index
                        });
                    }

                    match Box::pin(worker.process_cross_chain_update(updates)).await {
                        Ok(heights_by_origin) => {
                            for (origin, height) in heights_by_origin {
                                if let Some(callbacks) = callbacks_by_origin.remove(&origin) {
                                    for callback in callbacks {
                                        let _ = callback.send(Ok(height));
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            let all_callbacks: Vec<_> =
                                callbacks_by_origin.into_values().flatten().collect();
                            send_batch_error(all_callbacks, err);
                        }
                    }
                }
                RequestType::Confirmation => {
                    // Drain requests and group by recipient, keeping max height.
                    let mut confirmations_map: BTreeMap<ChainId, BlockHeight> = BTreeMap::new();
                    let mut callbacks = Vec::new();
                    let mut count = 0;

                    while count < cross_chain_batch_size {
                        match Pin::new(&mut confirmations).next().now_or_never() {
                            Some(Some((req, _span, _queued_at))) => {
                                #[cfg(with_metrics)]
                                {
                                    let wait_ms = _queued_at.elapsed().as_secs_f64() * 1000.0;
                                    metrics::CHAIN_WORKER_REQUEST_QUEUE_WAIT_TIME.observe(wait_ms);
                                }
                                confirmations_map
                                    .entry(req.recipient)
                                    .and_modify(|h| *h = (*h).max(req.latest_height))
                                    .or_insert(req.latest_height);
                                callbacks.push(req.callback);
                                count += 1;
                            }
                            None | Some(None) => break,
                        }
                    }

                    if confirmations_map.is_empty() {
                        tracing::error!("cross-chain confirmation queue empty; this is a bug!");
                        continue; // Queue was empty after all.
                    }
                    trace!("batching {count} confirmations");

                    match Box::pin(worker.confirm_updated_recipient(confirmations_map)).await {
                        Ok(()) => {
                            for callback in callbacks {
                                let _ = callback.send(Ok(()));
                            }
                        }
                        Err(err) => send_batch_error(callbacks, err),
                    }
                }
                RequestType::Regular => {
                    for _ in 0..regular_batch_size {
                        let Some(Some((request, span, _queued_at))) =
                            Pin::new(&mut requests).next().now_or_never()
                        else {
                            break;
                        };
                        #[cfg(with_metrics)]
                        {
                            let wait_ms = _queued_at.elapsed().as_secs_f64() * 1000.0;
                            metrics::CHAIN_WORKER_REQUEST_QUEUE_WAIT_TIME.observe(wait_ms);
                        }
                        Box::pin(worker.handle_request(request))
                            .instrument(span)
                            .await;
                    }
                }
            }
        }

        // Clean up on exit.
        if let Some(mut w) = worker.take() {
            trace!("Unloading chain state of {} ...", self.chain_id);
            w.clear_shared_chain_view().await;
            drop(w);
            if let Some(task) = service_runtime_task.take() {
                task.await?;
            }
            drop(service_runtime_thread);
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

/// Sends an error to the first callback and a generic batch failure to the rest.
fn send_batch_error<T>(callbacks: Vec<oneshot::Sender<Result<T, WorkerError>>>, err: WorkerError) {
    let mut iter = callbacks.into_iter();
    if let Some(first) = iter.next() {
        let _ = first.send(Err(err));
    }
    for callback in iter {
        let _ = callback.send(Err(WorkerError::ChainError(Box::new(
            ChainError::InternalError("Batch processing failed".to_string()),
        ))));
    }
}

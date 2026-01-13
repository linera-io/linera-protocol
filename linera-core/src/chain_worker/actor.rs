// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An actor that runs a chain worker.

use std::{
    cmp::{Ordering, Reverse},
    collections::{BTreeMap, BinaryHeap, HashMap},
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
    time::Instant,
};
use linera_chain::{
    data_types::{BlockProposal, MessageBundle, ProposedBlock},
    types::{Block, ConfirmedBlockCertificate, TimeoutCertificate, ValidatedBlockCertificate},
    ChainStateView,
};
use linera_execution::{
    system::EventSubscriptions, ExecutionStateView, Query, QueryContext, QueryOutcome,
    ServiceRuntimeEndpoint, ServiceSyncRuntime,
};
use linera_storage::{Clock as _, Storage};
use linera_views::context::{Context, InactiveContext};
use tokio::sync::{mpsc, oneshot, OwnedRwLockReadGuard};
use tracing::{debug, instrument, trace, Instrument as _};

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

/// A block proposal request that is delayed until its timestamp is reached.
///
/// Block proposals with future timestamps (within the grace period) are queued
/// and processed when their timestamp arrives, rather than blocking the actor.
struct DelayedProposal<Ctx>
where
    Ctx: Context + Clone + 'static,
{
    /// The timestamp when this proposal should be processed.
    timestamp: Timestamp,
    /// Sequence number to maintain FIFO order among proposals with the same timestamp.
    sequence: u64,
    /// The block proposal request.
    request: ChainWorkerRequest<Ctx>,
    /// The tracing span for this request.
    span: tracing::Span,
    /// When the request was originally queued (for metrics).
    _queued_at: Instant,
}

impl<Ctx> PartialEq for DelayedProposal<Ctx>
where
    Ctx: Context + Clone + 'static,
{
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp && self.sequence == other.sequence
    }
}

impl<Ctx> Eq for DelayedProposal<Ctx> where Ctx: Context + Clone + 'static {}

impl<Ctx> PartialOrd for DelayedProposal<Ctx>
where
    Ctx: Context + Clone + 'static,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<Ctx> Ord for DelayedProposal<Ctx>
where
    Ctx: Context + Clone + 'static,
{
    fn cmp(&self, other: &Self) -> Ordering {
        // Order by timestamp first, then by sequence number.
        // Earlier timestamps and lower sequence numbers come first.
        self.timestamp
            .cmp(&other.timestamp)
            .then_with(|| self.sequence.cmp(&other.sequence))
    }
}

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
        incoming_requests: mpsc::UnboundedReceiver<(
            ChainWorkerRequest<StorageClient::Context>,
            tracing::Span,
            Instant,
        )>,
        is_tracked: bool,
    ) {
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
        if let Err(err) = actor.handle_requests(incoming_requests).await {
            tracing::error!("Chain actor error: {err}");
        }
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
        // Only validators need to wait for the timestamp.
        self.config.key_pair.as_ref()?;

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

    /// Computes the next wakeup time considering both TTL and delayed proposals.
    fn next_wakeup_time(
        &self,
        delayed_proposals: &BinaryHeap<Reverse<DelayedProposal<StorageClient::Context>>>,
    ) -> Timestamp {
        let ttl_timeout = self.ttl_timeout();
        match delayed_proposals.peek() {
            Some(Reverse(proposal)) => ttl_timeout.min(proposal.timestamp),
            None => ttl_timeout,
        }
    }

    /// Pops all delayed proposals that are ready to be processed.
    fn pop_ready_delayed(
        &self,
        delayed_proposals: &mut BinaryHeap<Reverse<DelayedProposal<StorageClient::Context>>>,
    ) -> Vec<DelayedProposal<StorageClient::Context>> {
        let now = self.storage.clock().current_time();
        let mut ready = Vec::new();
        while let Some(Reverse(proposal)) = delayed_proposals.peek() {
            if proposal.timestamp <= now {
                if let Some(Reverse(proposal)) = delayed_proposals.pop() {
                    ready.push(proposal);
                }
            } else {
                break;
            }
        }
        ready
    }

    /// Runs the worker until there are no more incoming requests.
    #[instrument(
        skip_all,
        fields(chain_id = format!("{:.8}", self.chain_id), long_lived_services = %self.config.long_lived_services),
    )]
    async fn handle_requests(
        self,
        mut incoming_requests: mpsc::UnboundedReceiver<(
            ChainWorkerRequest<StorageClient::Context>,
            tracing::Span,
            Instant,
        )>,
    ) -> Result<(), WorkerError> {
        trace!("Starting `ChainWorkerActor`");

        // Queue for block proposals with future timestamps.
        let mut delayed_proposals: BinaryHeap<Reverse<DelayedProposal<StorageClient::Context>>> =
            BinaryHeap::new();
        let mut next_sequence: u64 = 0;

        while let Some((request, span, _queued_at)) = incoming_requests.recv().await {
            // Record how long the request waited in queue (in milliseconds)
            #[cfg(with_metrics)]
            {
                let queue_wait_time_ms = _queued_at.elapsed().as_secs_f64() * 1000.0;
                metrics::CHAIN_WORKER_REQUEST_QUEUE_WAIT_TIME.observe(queue_wait_time_ms);
            }

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

            // Check if the first request should be delayed.
            if let ChainWorkerRequest::HandleBlockProposal { ref proposal, .. } = request {
                if let Some(timestamp) = self.delay_until(proposal) {
                    debug!(delay_until = %timestamp, "delaying block proposal");
                    delayed_proposals.push(Reverse(DelayedProposal {
                        timestamp,
                        sequence: next_sequence,
                        request,
                        span,
                        _queued_at,
                    }));
                    next_sequence += 1;
                } else {
                    Box::pin(worker.handle_request(request))
                        .instrument(span)
                        .await;
                }
            } else {
                Box::pin(worker.handle_request(request))
                    .instrument(span)
                    .await;
            }

            loop {
                // Process any delayed proposals that are now ready.
                for ready in self.pop_ready_delayed(&mut delayed_proposals) {
                    #[cfg(with_metrics)]
                    {
                        let queue_wait_time_ms = ready._queued_at.elapsed().as_secs_f64() * 1000.0;
                        metrics::CHAIN_WORKER_REQUEST_QUEUE_WAIT_TIME.observe(queue_wait_time_ms);
                    }
                    Box::pin(worker.handle_request(ready.request))
                        .instrument(ready.span)
                        .await;
                }

                // Compute the next wakeup time (minimum of TTL and earliest delayed proposal).
                let wakeup_time = self.next_wakeup_time(&delayed_proposals);
                let ttl_timeout = self.ttl_timeout();

                futures::select! {
                    () = self.storage.clock().sleep_until(wakeup_time).fuse() => {
                        // Check if any delayed proposals are now ready.
                        let ready_proposals = self.pop_ready_delayed(&mut delayed_proposals);
                        if !ready_proposals.is_empty() {
                            for ready in ready_proposals {
                                #[cfg(with_metrics)]
                                {
                                    let queue_wait_time_ms =
                                        ready._queued_at.elapsed().as_secs_f64() * 1000.0;
                                    metrics::CHAIN_WORKER_REQUEST_QUEUE_WAIT_TIME
                                        .observe(queue_wait_time_ms);
                                }
                                Box::pin(worker.handle_request(ready.request))
                                    .instrument(ready.span)
                                    .await;
                            }
                        } else if wakeup_time >= ttl_timeout && delayed_proposals.is_empty() {
                            // TTL expired and no delayed proposals waiting.
                            break;
                        }
                        // Otherwise, there are still delayed proposals waiting; continue the loop.
                    }
                    maybe_request = incoming_requests.recv().fuse() => {
                        let Some((request, span, _queued_at)) = maybe_request else {
                            break; // Request sender was dropped.
                        };

                        // Record how long the request waited in queue (in milliseconds)
                        #[cfg(with_metrics)]
                        {
                            let queue_wait_time_ms = _queued_at.elapsed().as_secs_f64() * 1000.0;
                            metrics::CHAIN_WORKER_REQUEST_QUEUE_WAIT_TIME.observe(queue_wait_time_ms);
                        }

                        // Check if this request should be delayed.
                        if let ChainWorkerRequest::HandleBlockProposal { ref proposal, .. } =
                            request
                        {
                            if let Some(timestamp) = self.delay_until(proposal) {
                                debug!(
                                    "Delaying block proposal until timestamp {}",
                                    timestamp
                                );
                                delayed_proposals.push(Reverse(DelayedProposal {
                                    timestamp,
                                    sequence: next_sequence,
                                    request,
                                    span,
                                    _queued_at,
                                }));
                                next_sequence += 1;
                                continue;
                            }
                        }

                        Box::pin(worker.handle_request(request)).instrument(span).await;
                    }
                }
            }

            trace!("Unloading chain state of {} ...", self.chain_id);
            worker.clear_shared_chain_view().await;
            drop(worker);
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

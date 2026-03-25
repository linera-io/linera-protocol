// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use futures::{
    future::{Either, Shared},
    FutureExt as _,
};
use linera_base::{
    crypto::{CryptoError, CryptoHash, ValidatorPublicKey},
    data_types::{
        ApplicationDescription, ArithmeticError, Blob, BlockHeight, Epoch, Round, TimeDelta,
        Timestamp,
    },
    doc_scalar,
    identifiers::{AccountOwner, ApplicationId, BlobId, ChainId, EventId, StreamId},
};
#[cfg(with_testing)]
use linera_chain::ChainExecutionContext;
use linera_chain::{
    data_types::{
        BlockExecutionOutcome, BlockProposal, BundleExecutionPolicy, MessageBundle, ProposedBlock,
    },
    types::{
        Block, CertificateValue, ConfirmedBlock, ConfirmedBlockCertificate, GenericCertificate,
        LiteCertificate, Timeout, TimeoutCertificate, ValidatedBlock, ValidatedBlockCertificate,
    },
    ChainError, ChainStateView,
};
use linera_execution::{ExecutionError, ExecutionStateView, Query, QueryOutcome, ResourceTracker};
use linera_storage::{Clock as _, Storage};
use linera_views::{context::InactiveContext, ViewError};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{oneshot, OwnedRwLockReadGuard};
use tracing::{instrument, trace, warn};

/// A read guard providing access to a chain's [`ChainStateView`].
///
/// Holds a read lock on the chain worker state, preventing writes for its
/// lifetime. The `OwnedRwLockReadGuard` internally holds a strong `Arc`
/// reference to the `RwLock<ChainWorkerState>`, keeping the state alive.
/// Dereferences to `ChainStateView`.
pub struct ChainStateViewReadGuard<S: Storage>(
    OwnedRwLockReadGuard<ChainWorkerState<S>, ChainStateView<S::Context>>,
);

impl<S: Storage> std::ops::Deref for ChainStateViewReadGuard<S> {
    type Target = ChainStateView<S::Context>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

use linera_cache::{UniqueValueCache, ValueCache};

/// Re-export of [`EventSubscriptionsResult`] for use by other crate modules.
pub(crate) use crate::chain_worker::EventSubscriptionsResult;
use crate::{
    chain_worker::{
        handle, state::ChainWorkerState, BlockOutcome, ChainWorkerConfig, DeliveryNotifier,
    },
    client::ListeningMode,
    data_types::{ChainInfoQuery, ChainInfoResponse, CrossChainRequest},
    notifier::Notifier,
};

#[cfg(test)]
#[path = "unit_tests/worker_tests.rs"]
mod worker_tests;

/// Wraps a future in `SyncFuture` on non-web targets so that it satisfies `Sync` bounds.
/// On web targets the future is returned as-is.
#[cfg(not(web))]
pub(crate) fn wrap_future<F: std::future::Future>(f: F) -> sync_wrapper::SyncFuture<F> {
    sync_wrapper::SyncFuture::new(f)
}

/// Wraps a future in `SyncFuture` on non-web targets so that it satisfies `Sync` bounds.
/// On web targets the future is returned as-is.
#[cfg(web)]
pub(crate) fn wrap_future<F: std::future::Future>(f: F) -> F {
    f
}

pub const DEFAULT_BLOCK_CACHE_SIZE: usize = 5_000;
pub const DEFAULT_EXECUTION_STATE_CACHE_SIZE: usize = 10_000;

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{
        exponential_bucket_interval, register_histogram, register_histogram_vec,
        register_int_counter, register_int_counter_vec,
    };
    use linera_chain::types::ConfirmedBlockCertificate;
    use prometheus::{Histogram, HistogramVec, IntCounter, IntCounterVec};

    pub static NUM_ROUNDS_IN_CERTIFICATE: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "num_rounds_in_certificate",
            "Number of rounds in certificate",
            &["certificate_value", "round_type"],
            exponential_bucket_interval(0.1, 50.0),
        )
    });

    pub static NUM_ROUNDS_IN_BLOCK_PROPOSAL: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "num_rounds_in_block_proposal",
            "Number of rounds in block proposal",
            &["round_type"],
            exponential_bucket_interval(0.1, 50.0),
        )
    });

    pub static TRANSACTION_COUNT: LazyLock<IntCounterVec> =
        LazyLock::new(|| register_int_counter_vec("transaction_count", "Transaction count", &[]));

    pub static INCOMING_BUNDLE_COUNT: LazyLock<IntCounter> =
        LazyLock::new(|| register_int_counter("incoming_bundle_count", "Incoming bundle count"));

    pub static INCOMING_MESSAGE_COUNT: LazyLock<IntCounter> =
        LazyLock::new(|| register_int_counter("incoming_message_count", "Incoming message count"));

    pub static OPERATION_COUNT: LazyLock<IntCounter> =
        LazyLock::new(|| register_int_counter("operation_count", "Operation count"));

    pub static OPERATIONS_PER_BLOCK: LazyLock<Histogram> = LazyLock::new(|| {
        register_histogram(
            "operations_per_block",
            "Number of operations per block",
            exponential_bucket_interval(1.0, 10000.0),
        )
    });

    pub static INCOMING_BUNDLES_PER_BLOCK: LazyLock<Histogram> = LazyLock::new(|| {
        register_histogram(
            "incoming_bundles_per_block",
            "Number of incoming bundles per block",
            exponential_bucket_interval(1.0, 10000.0),
        )
    });

    pub static TRANSACTIONS_PER_BLOCK: LazyLock<Histogram> = LazyLock::new(|| {
        register_histogram(
            "transactions_per_block",
            "Number of transactions per block",
            exponential_bucket_interval(1.0, 10000.0),
        )
    });

    pub static NUM_BLOCKS: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec("num_blocks", "Number of blocks added to chains", &[])
    });

    pub static CERTIFICATES_SIGNED: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "certificates_signed",
            "Number of confirmed block certificates signed by each validator",
            &["validator_name"],
        )
    });

    pub static CHAIN_INFO_QUERIES: LazyLock<IntCounter> = LazyLock::new(|| {
        register_int_counter(
            "chain_info_queries",
            "Number of chain info queries processed",
        )
    });

    /// Holds metrics data extracted from a confirmed block certificate.
    pub struct MetricsData {
        certificate_log_str: &'static str,
        round_type: &'static str,
        round_number: u32,
        confirmed_transactions: u64,
        confirmed_incoming_bundles: u64,
        confirmed_incoming_messages: u64,
        confirmed_operations: u64,
        validators_with_signatures: Vec<String>,
    }

    impl MetricsData {
        /// Creates a new `MetricsData` by extracting data from the certificate.
        pub fn new(certificate: &ConfirmedBlockCertificate) -> Self {
            Self {
                certificate_log_str: certificate.inner().to_log_str(),
                round_type: certificate.round.type_name(),
                round_number: certificate.round.number(),
                confirmed_transactions: certificate.block().body.transactions.len() as u64,
                confirmed_incoming_bundles: certificate.block().body.incoming_bundles().count()
                    as u64,
                confirmed_incoming_messages: certificate
                    .block()
                    .body
                    .incoming_bundles()
                    .map(|b| b.messages().count())
                    .sum::<usize>() as u64,
                confirmed_operations: certificate.block().body.operations().count() as u64,
                validators_with_signatures: certificate
                    .signatures()
                    .iter()
                    .map(|(validator_name, _)| validator_name.to_string())
                    .collect(),
            }
        }

        /// Records the metrics for a processed block.
        pub fn record(self) {
            NUM_BLOCKS.with_label_values(&[]).inc();
            NUM_ROUNDS_IN_CERTIFICATE
                .with_label_values(&[self.certificate_log_str, self.round_type])
                .observe(self.round_number as f64);
            TRANSACTIONS_PER_BLOCK.observe(self.confirmed_transactions as f64);
            INCOMING_BUNDLES_PER_BLOCK.observe(self.confirmed_incoming_bundles as f64);
            OPERATIONS_PER_BLOCK.observe(self.confirmed_operations as f64);
            if self.confirmed_transactions > 0 {
                TRANSACTION_COUNT
                    .with_label_values(&[])
                    .inc_by(self.confirmed_transactions);
                if self.confirmed_incoming_bundles > 0 {
                    INCOMING_BUNDLE_COUNT.inc_by(self.confirmed_incoming_bundles);
                }
                if self.confirmed_incoming_messages > 0 {
                    INCOMING_MESSAGE_COUNT.inc_by(self.confirmed_incoming_messages);
                }
                if self.confirmed_operations > 0 {
                    OPERATION_COUNT.inc_by(self.confirmed_operations);
                }
            }

            for validator_name in self.validators_with_signatures {
                CERTIFICATES_SIGNED
                    .with_label_values(&[&validator_name])
                    .inc();
            }
        }
    }
}

/// Instruct the networking layer to send cross-chain requests and/or push notifications.
#[derive(Default, Debug)]
pub struct NetworkActions {
    /// The cross-chain requests
    pub cross_chain_requests: Vec<CrossChainRequest>,
    /// The push notifications.
    pub notifications: Vec<Notification>,
}

impl NetworkActions {
    pub fn extend(&mut self, other: NetworkActions) {
        self.cross_chain_requests.extend(other.cross_chain_requests);
        self.notifications.extend(other.notifications);
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Notification that a chain has a new certified block or a new message.
pub struct Notification {
    pub chain_id: ChainId,
    pub reason: Reason,
}

doc_scalar!(
    Notification,
    "Notify that a chain has a new certified block or a new message"
);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Reason for the notification.
pub enum Reason {
    NewBlock {
        height: BlockHeight,
        hash: CryptoHash,
        event_streams: BTreeSet<StreamId>,
    },
    NewIncomingBundle {
        origin: ChainId,
        height: BlockHeight,
    },
    NewRound {
        height: BlockHeight,
        round: Round,
    },
    BlockExecuted {
        height: BlockHeight,
        hash: CryptoHash,
    },
    // NOTE: Keep this at the end for backward compatibility with old validators
    // that use bincode integer-based variant indices. Old validators don't emit
    // NewEvents, and inserting it before existing variants would shift their indices.
    NewEvents {
        height: BlockHeight,
        hash: CryptoHash,
        event_streams: BTreeSet<StreamId>,
    },
}

/// Error type for worker operations.
#[derive(Debug, Error)]
pub enum WorkerError {
    #[error(transparent)]
    CryptoError(#[from] CryptoError),

    #[error(transparent)]
    ArithmeticError(#[from] ArithmeticError),

    #[error(transparent)]
    ViewError(#[from] ViewError),

    #[error("Certificates are in confirmed_log but not in storage: {0:?}")]
    ReadCertificatesError(Vec<CryptoHash>),

    #[error(transparent)]
    ChainError(#[from] Box<ChainError>),

    #[error(transparent)]
    BcsError(#[from] bcs::Error),

    // Chain access control
    #[error("Block was not signed by an authorized owner")]
    InvalidOwner,

    #[error("Operations in the block are not authenticated by the proper owner: {0}")]
    InvalidSigner(AccountOwner),

    // Chaining
    #[error(
        "Chain is expecting a next block at height {expected_block_height} but the given block \
        is at height {found_block_height} instead"
    )]
    UnexpectedBlockHeight {
        expected_block_height: BlockHeight,
        found_block_height: BlockHeight,
    },
    #[error("Unexpected epoch {epoch}: chain {chain_id} is at {chain_epoch}")]
    InvalidEpoch {
        chain_id: ChainId,
        chain_epoch: Epoch,
        epoch: Epoch,
    },

    #[error("Events not found: {0:?}")]
    EventsNotFound(Vec<EventId>),

    // Other server-side errors
    #[error("Invalid cross-chain request")]
    InvalidCrossChainRequest,
    #[error("The block does not contain the hash that we expected for the previous block")]
    InvalidBlockChaining,
    #[error(
        "The given outcome is not what we computed after executing the block.\n\
        Computed: {computed:#?}\n\
        Submitted: {submitted:#?}"
    )]
    IncorrectOutcome {
        computed: Box<BlockExecutionOutcome>,
        submitted: Box<BlockExecutionOutcome>,
    },
    #[error(
        "Block timestamp ({block_timestamp}) is further in the future from local time \
        ({local_time}) than block time grace period ({block_time_grace_period:?}) \
        [us:{block_timestamp_us}:{local_time_us}]",
        block_timestamp_us = block_timestamp.micros(),
        local_time_us = local_time.micros(),
    )]
    InvalidTimestamp {
        block_timestamp: Timestamp,
        local_time: Timestamp,
        block_time_grace_period: Duration,
    },
    #[error("We don't have the value for the certificate.")]
    MissingCertificateValue,
    #[error("The hash certificate doesn't match its value.")]
    InvalidLiteCertificate,
    #[error("Fast blocks cannot query oracles")]
    FastBlockUsingOracles,
    #[error("Blobs not found: {0:?}")]
    BlobsNotFound(Vec<BlobId>),
    #[error("confirmed_log entry at height {height} for chain {chain_id:8} not found")]
    ConfirmedLogEntryNotFound {
        height: BlockHeight,
        chain_id: ChainId,
    },
    #[error("preprocessed_blocks entry at height {height} for chain {chain_id:8} not found")]
    PreprocessedBlocksEntryNotFound {
        height: BlockHeight,
        chain_id: ChainId,
    },
    #[error("The block proposal is invalid: {0}")]
    InvalidBlockProposal(String),
    #[error("Blob was not required by any pending block")]
    UnexpectedBlob,
    #[error("Number of published blobs per block must not exceed {0}")]
    TooManyPublishedBlobs(u64),
    #[error("Missing network description")]
    MissingNetworkDescription,
    #[error("thread error: {0}")]
    Thread(#[from] web_thread_pool::Error),

    #[error("Fallback mode is not available on this network")]
    NoFallbackMode,
}

impl WorkerError {
    /// Returns whether this error is caused by an issue in the local node.
    ///
    /// Returns `false` whenever the error could be caused by a bad message from a peer.
    pub fn is_local(&self) -> bool {
        match self {
            WorkerError::CryptoError(_)
            | WorkerError::ArithmeticError(_)
            | WorkerError::InvalidOwner
            | WorkerError::InvalidSigner(_)
            | WorkerError::UnexpectedBlockHeight { .. }
            | WorkerError::InvalidEpoch { .. }
            | WorkerError::EventsNotFound(_)
            | WorkerError::InvalidBlockChaining
            | WorkerError::IncorrectOutcome { .. }
            | WorkerError::InvalidTimestamp { .. }
            | WorkerError::MissingCertificateValue
            | WorkerError::InvalidLiteCertificate
            | WorkerError::FastBlockUsingOracles
            | WorkerError::BlobsNotFound(_)
            | WorkerError::InvalidBlockProposal(_)
            | WorkerError::UnexpectedBlob
            | WorkerError::TooManyPublishedBlobs(_)
            | WorkerError::NoFallbackMode
            | WorkerError::ViewError(ViewError::NotFound(_)) => false,
            WorkerError::BcsError(_)
            | WorkerError::InvalidCrossChainRequest
            | WorkerError::ViewError(_)
            | WorkerError::ConfirmedLogEntryNotFound { .. }
            | WorkerError::PreprocessedBlocksEntryNotFound { .. }
            | WorkerError::MissingNetworkDescription
            | WorkerError::Thread(_)
            | WorkerError::ReadCertificatesError(_) => true,
            WorkerError::ChainError(chain_error) => chain_error.is_local(),
        }
    }
}

impl From<ChainError> for WorkerError {
    #[instrument(level = "trace", skip(chain_error))]
    fn from(chain_error: ChainError) -> Self {
        match chain_error {
            ChainError::ExecutionError(execution_error, context) => match *execution_error {
                ExecutionError::BlobsNotFound(blob_ids) => Self::BlobsNotFound(blob_ids),
                ExecutionError::EventsNotFound(event_ids) => Self::EventsNotFound(event_ids),
                _ => Self::ChainError(Box::new(ChainError::ExecutionError(
                    execution_error,
                    context,
                ))),
            },
            error => Self::ChainError(Box::new(error)),
        }
    }
}

#[cfg(with_testing)]
impl WorkerError {
    /// Returns the inner [`ExecutionError`] in this error.
    ///
    /// # Panics
    ///
    /// If this is not caused by an [`ExecutionError`].
    pub fn expect_execution_error(self, expected_context: ChainExecutionContext) -> ExecutionError {
        let WorkerError::ChainError(chain_error) = self else {
            panic!("Expected an `ExecutionError`. Got: {self:#?}");
        };

        let ChainError::ExecutionError(execution_error, context) = *chain_error else {
            panic!("Expected an `ExecutionError`. Got: {chain_error:#?}");
        };

        assert_eq!(context, expected_context);

        *execution_error
    }
}

type ChainWorkerArc<S> = Arc<tokio::sync::RwLock<ChainWorkerState<S>>>;
type ChainWorkerWeak<S> = std::sync::Weak<tokio::sync::RwLock<ChainWorkerState<S>>>;
type ChainWorkerFuture<S> = Shared<oneshot::Receiver<ChainWorkerWeak<S>>>;

/// Each map entry is a `Shared<oneshot::Receiver<Weak<...>>>`:
///
/// - `peek()` returns `None` while a task is loading the worker from storage.
/// - `peek()` returns `Some(Ok(weak))` once the worker is loaded.
/// - `peek()` returns `Some(Err(_))` if loading failed (sender dropped).
///
/// Callers that find a pending entry clone the `Shared` future and await it.
type ChainWorkerMap<S> = Arc<papaya::HashMap<ChainId, ChainWorkerFuture<S>>>;

/// Starts a background task that periodically removes dead weak references
/// from the chain handle map. The actual lifetime management is handled by
/// each handle's keep-alive task.
fn start_sweep<S: Storage + Clone + 'static>(
    chain_workers: &ChainWorkerMap<S>,
    config: &ChainWorkerConfig,
) {
    // Sweep at the smaller of the two TTLs. If both are None, workers
    // live forever so there's nothing to sweep.
    let interval = match (config.ttl, config.sender_chain_ttl) {
        (None, None) => return,
        (Some(d), None) | (None, Some(d)) => d,
        (Some(a), Some(b)) => a.min(b),
    };
    let weak_map = Arc::downgrade(chain_workers);
    linera_base::Task::spawn(async move {
        loop {
            linera_base::time::timer::sleep(interval).await;
            let Some(map) = weak_map.upgrade() else {
                break;
            };
            map.pin_owned().retain(|_, shared| match shared.peek() {
                Some(Ok(weak)) => weak.strong_count() > 0,
                Some(Err(_)) => false, // Loading failed; clean up.
                None => true,          // Still loading; keep.
            });
        }
    })
    .forget();
}

/// State of a worker in a validator or a local node.
pub struct WorkerState<StorageClient: Storage> {
    /// Access to local persistent storage.
    storage: StorageClient,
    /// Configuration options for chain workers.
    chain_worker_config: ChainWorkerConfig,
    block_cache: Arc<ValueCache<CryptoHash, Block>>,
    execution_state_cache: Arc<UniqueValueCache<CryptoHash, ExecutionStateView<InactiveContext>>>,
    /// Chains tracked by a worker, along with their listening modes.
    chain_modes: Option<Arc<RwLock<BTreeMap<ChainId, ListeningMode>>>>,
    /// One-shot channels to notify callers when messages of a particular chain have been
    /// delivered.
    delivery_notifiers: Arc<Mutex<DeliveryNotifiers>>,
    /// The cache of loaded chain workers. Stores weak references; each worker
    /// manages its own lifetime via a keep-alive task. A background sweep
    /// periodically removes dead entries.
    chain_workers: ChainWorkerMap<StorageClient>,
}

impl<StorageClient> Clone for WorkerState<StorageClient>
where
    StorageClient: Storage + Clone,
{
    fn clone(&self) -> Self {
        WorkerState {
            storage: self.storage.clone(),
            chain_worker_config: self.chain_worker_config.clone(),
            block_cache: self.block_cache.clone(),
            execution_state_cache: self.execution_state_cache.clone(),
            chain_modes: self.chain_modes.clone(),
            delivery_notifiers: self.delivery_notifiers.clone(),
            chain_workers: self.chain_workers.clone(),
        }
    }
}

pub(crate) type DeliveryNotifiers = HashMap<ChainId, DeliveryNotifier>;

impl<StorageClient> WorkerState<StorageClient>
where
    StorageClient: Storage,
{
    #[instrument(level = "trace", skip(self))]
    pub fn nickname(&self) -> &str {
        &self.chain_worker_config.nickname
    }

    /// Sets the priority bundle origins.
    pub fn with_priority_bundle_origins(
        mut self,
        origins: std::collections::HashSet<ChainId>,
    ) -> Self {
        self.chain_worker_config.priority_bundle_origins = origins;
        self
    }

    /// Returns the storage client so that it can be manipulated or queried.
    #[instrument(level = "trace", skip(self))]
    #[cfg(not(feature = "test"))]
    pub(crate) fn storage_client(&self) -> &StorageClient {
        &self.storage
    }

    /// Returns the storage client so that it can be manipulated or queried by tests in other
    /// crates.
    #[instrument(level = "trace", skip(self))]
    #[cfg(feature = "test")]
    pub fn storage_client(&self) -> &StorageClient {
        &self.storage
    }

    #[instrument(level = "trace", skip(self, certificate))]
    pub(crate) async fn full_certificate(
        &self,
        certificate: LiteCertificate<'_>,
    ) -> Result<Either<ConfirmedBlockCertificate, ValidatedBlockCertificate>, WorkerError> {
        let block = self
            .block_cache
            .get_hashed(&certificate.value.value_hash)
            .ok_or(WorkerError::MissingCertificateValue)?;

        match certificate.value.kind {
            linera_chain::types::CertificateKind::Confirmed => {
                let value = ConfirmedBlock::from_hashed(block);
                Ok(Either::Left(
                    certificate
                        .with_value(value)
                        .ok_or(WorkerError::InvalidLiteCertificate)?,
                ))
            }
            linera_chain::types::CertificateKind::Validated => {
                let value = ValidatedBlock::from_hashed(block);
                Ok(Either::Right(
                    certificate
                        .with_value(value)
                        .ok_or(WorkerError::InvalidLiteCertificate)?,
                ))
            }
            _ => Err(WorkerError::InvalidLiteCertificate),
        }
    }
}

#[allow(async_fn_in_trait)]
#[cfg_attr(not(web), trait_variant::make(Send))]
pub trait ProcessableCertificate: CertificateValue + Sized + 'static {
    async fn process_certificate<S: Storage + Clone + 'static>(
        worker: &WorkerState<S>,
        certificate: GenericCertificate<Self>,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError>;
}

impl ProcessableCertificate for ConfirmedBlock {
    async fn process_certificate<S: Storage + Clone + 'static>(
        worker: &WorkerState<S>,
        certificate: ConfirmedBlockCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        Box::pin(worker.handle_confirmed_certificate(certificate, None)).await
    }
}

impl ProcessableCertificate for ValidatedBlock {
    async fn process_certificate<S: Storage + Clone + 'static>(
        worker: &WorkerState<S>,
        certificate: ValidatedBlockCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        Box::pin(worker.handle_validated_certificate(certificate)).await
    }
}

impl ProcessableCertificate for Timeout {
    async fn process_certificate<S: Storage + Clone + 'static>(
        worker: &WorkerState<S>,
        certificate: TimeoutCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        worker.handle_timeout_certificate(certificate).await
    }
}

impl<StorageClient> WorkerState<StorageClient>
where
    StorageClient: Storage + Clone + 'static,
{
    /// Creates a new `WorkerState`.
    ///
    /// The `chain_worker_config` must be fully configured before calling this, because the
    /// TTL sweep task is started immediately based on the config's TTL values.
    #[instrument(level = "trace", skip(storage, chain_worker_config))]
    pub fn new(
        storage: StorageClient,
        chain_worker_config: ChainWorkerConfig,
        chain_modes: Option<Arc<RwLock<BTreeMap<ChainId, ListeningMode>>>>,
    ) -> Self {
        let chain_workers = Arc::new(papaya::HashMap::new());
        start_sweep(&chain_workers, &chain_worker_config);
        let block_cache_size = chain_worker_config.block_cache_size;
        let execution_state_cache_size = chain_worker_config.execution_state_cache_size;
        WorkerState {
            storage,
            chain_worker_config,
            block_cache: Arc::new(ValueCache::new(block_cache_size)),
            execution_state_cache: Arc::new(UniqueValueCache::new(execution_state_cache_size)),
            chain_modes,
            delivery_notifiers: Arc::default(),
            chain_workers,
        }
    }

    #[instrument(level = "trace", skip(self, certificate, notifier))]
    #[inline]
    pub async fn fully_handle_certificate_with_notifications<T>(
        &self,
        certificate: GenericCertificate<T>,
        notifier: &impl Notifier,
    ) -> Result<ChainInfoResponse, WorkerError>
    where
        T: ProcessableCertificate,
    {
        let notifications = (*notifier).clone();
        let this = self.clone();
        linera_base::Task::spawn(async move {
            let (response, actions) =
                ProcessableCertificate::process_certificate(&this, certificate).await?;
            notifications.notify(&actions.notifications);
            let mut requests = VecDeque::from(actions.cross_chain_requests);
            while let Some(request) = requests.pop_front() {
                let actions = this.handle_cross_chain_request(request).await?;
                requests.extend(actions.cross_chain_requests);
                notifications.notify(&actions.notifications);
            }
            Ok(response)
        })
        .await
    }

    /// Acquires a read lock on the chain worker and executes the given closure.
    ///
    /// The future is boxed to keep deeply nested types off the stack. On non-web
    /// targets it is also wrapped in `SyncFuture` to satisfy `Sync` bounds.
    async fn chain_read<R, F, Fut>(&self, chain_id: ChainId, f: F) -> Result<R, WorkerError>
    where
        F: FnOnce(OwnedRwLockReadGuard<ChainWorkerState<StorageClient>>) -> Fut,
        Fut: std::future::Future<Output = Result<R, WorkerError>>,
    {
        let state = self.get_or_create_chain_worker(chain_id).await?;
        Box::pin(wrap_future(async move {
            let guard = handle::read_lock(&state).await;
            f(guard).await
        }))
        .await
    }

    /// Acquires a write lock on the chain worker and executes the given closure.
    ///
    /// The [`RollbackGuard`] automatically rolls back uncommitted chain state changes
    /// when dropped, ensuring cancellation safety. The future is boxed to keep deeply
    /// nested types off the stack.
    async fn chain_write<R, F, Fut>(&self, chain_id: ChainId, f: F) -> Result<R, WorkerError>
    where
        F: FnOnce(handle::RollbackGuard<StorageClient>) -> Fut,
        Fut: std::future::Future<Output = Result<R, WorkerError>>,
    {
        let state = self.get_or_create_chain_worker(chain_id).await?;
        Box::pin(wrap_future(async move {
            let guard = handle::write_lock(&state).await;
            f(guard).await
        }))
        .await
    }

    /// Gets or creates a chain worker for the given chain.
    ///
    /// The oneshot channel is created outside the `compute` closure to keep
    /// the closure pure (papaya may call it more than once on CAS retry and
    /// may memoize the output). If the fast path hits, the unused channel is
    /// dropped harmlessly.
    ///
    /// Returns a type-erased future to keep `!Sync` intermediate types (e.g.
    /// `std::sync::mpsc::Receiver` from `handle::ServiceRuntimeActor::spawn`) out of
    /// the caller's future type.
    fn get_or_create_chain_worker(
        &self,
        chain_id: ChainId,
    ) -> std::pin::Pin<
        Box<
            impl std::future::Future<Output = Result<ChainWorkerArc<StorageClient>, WorkerError>> + '_,
        >,
    > {
        Box::pin(wrap_future(async move {
            loop {
                // Create the channel outside the closure so that the tx/rx
                // always match regardless of CAS retries.
                let (tx, rx) = oneshot::channel();
                let shared_rx = rx.shared();

                // The papaya guard is !Send, so it must be dropped before
                // any .await point.
                let wait_or_tx = {
                    let pin = self.chain_workers.pin();
                    match pin.compute(chain_id, |existing| match existing {
                        Some((_, entry)) => match entry.peek() {
                            Some(Ok(weak)) => match weak.upgrade() {
                                Some(arc) => papaya::Operation::Abort(Ok(arc)),
                                None => papaya::Operation::Insert(shared_rx.clone()),
                            },
                            Some(Err(_)) => papaya::Operation::Insert(shared_rx.clone()),
                            None => papaya::Operation::Abort(Err(entry.clone())),
                        },
                        None => papaya::Operation::Insert(shared_rx.clone()),
                    }) {
                        papaya::Compute::Aborted(Ok(arc), ..) => return Ok(arc),
                        papaya::Compute::Aborted(Err(wait), ..) => Either::Left(wait),
                        papaya::Compute::Inserted { .. } | papaya::Compute::Updated { .. } => {
                            Either::Right(tx)
                        }
                        papaya::Compute::Removed { .. } => unreachable!(),
                    }
                };

                match wait_or_tx {
                    Either::Left(wait) => {
                        // Another task is loading. Await the shared future.
                        if let Ok(weak) = wait.await {
                            if let Some(arc) = weak.upgrade() {
                                return Ok(arc);
                            }
                        }
                        // Loading failed or worker already dead; retry.
                    }
                    Either::Right(tx) => {
                        // We claimed the loading slot. Load from storage.
                        // On success, send the Weak through the channel.
                        // On error, dropping tx wakes waiters so they can retry.
                        let worker = self.load_chain_worker(chain_id).await?;
                        if tx.send(Arc::downgrade(&worker)).is_err() {
                            tracing::error!(%chain_id, "Receiver dropped while loading worker state.");
                            continue;
                        }
                        return Ok(worker);
                    }
                }
            }
        }))
    }

    /// Loads a chain worker state from storage and wraps it in an Arc.
    async fn load_chain_worker(
        &self,
        chain_id: ChainId,
    ) -> Result<ChainWorkerArc<StorageClient>, WorkerError> {
        let delivery_notifier = self
            .delivery_notifiers
            .lock()
            .unwrap()
            .entry(chain_id)
            .or_default()
            .clone();

        let is_tracked = self.chain_modes.as_ref().is_some_and(|chain_modes| {
            chain_modes
                .read()
                .unwrap()
                .get(&chain_id)
                .is_some_and(ListeningMode::is_full)
        });

        let (service_runtime_endpoint, service_runtime_task) =
            if self.chain_worker_config.long_lived_services {
                let actor =
                    handle::ServiceRuntimeActor::spawn(chain_id, self.storage.thread_pool()).await;
                (Some(actor.endpoint), Some(actor.task))
            } else {
                (None, None)
            };

        let state = crate::chain_worker::state::ChainWorkerState::load(
            self.chain_worker_config.clone(),
            self.storage.clone(),
            self.block_cache.clone(),
            self.execution_state_cache.clone(),
            self.chain_modes.clone(),
            delivery_notifier,
            chain_id,
            service_runtime_endpoint,
            service_runtime_task,
        )
        .await?;

        Ok(handle::create_chain_worker(
            state,
            is_tracked,
            &self.chain_worker_config,
        ))
    }

    /// Tries to execute a block proposal without any verification other than block execution.
    #[instrument(level = "trace", skip(self, block))]
    pub async fn stage_block_execution(
        &self,
        block: ProposedBlock,
        round: Option<u32>,
        published_blobs: Vec<Blob>,
        policy: BundleExecutionPolicy,
    ) -> Result<(ProposedBlock, Block, ChainInfoResponse, ResourceTracker), WorkerError> {
        let chain_id = block.chain_id;
        self.chain_write(chain_id, |mut guard| async move {
            guard
                .stage_block_execution(block, round, &published_blobs, policy)
                .await
        })
        .await
    }

    /// Executes a [`Query`] for an application's state on a specific chain.
    ///
    /// If `block_hash` is specified, system will query the application's state
    /// at that block. If it doesn't exist, it uses latest state.
    #[instrument(level = "trace", skip(self, chain_id, query))]
    pub async fn query_application(
        &self,
        chain_id: ChainId,
        query: Query,
        block_hash: Option<CryptoHash>,
    ) -> Result<(QueryOutcome, BlockHeight), WorkerError> {
        self.chain_write(chain_id, |mut guard| async move {
            guard.query_application(query, block_hash).await
        })
        .await
    }

    #[instrument(level = "trace", skip(self, chain_id, application_id), fields(
        nickname = %self.nickname(),
        chain_id = %chain_id,
        application_id = %application_id
    ))]
    pub async fn describe_application(
        &self,
        chain_id: ChainId,
        application_id: ApplicationId,
    ) -> Result<ApplicationDescription, WorkerError> {
        let state = self.get_or_create_chain_worker(chain_id).await?;
        let guard = handle::read_lock_initialized(&state).await?;
        guard.describe_application_readonly(application_id).await
    }

    /// Processes a confirmed block (aka a commit).
    #[instrument(
        level = "trace",
        skip(self, certificate, notify_when_messages_are_delivered),
        fields(
            nickname = %self.nickname(),
            chain_id = %certificate.block().header.chain_id,
            block_height = %certificate.block().header.height
        )
    )]
    async fn process_confirmed_block(
        &self,
        certificate: ConfirmedBlockCertificate,
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions, BlockOutcome), WorkerError> {
        let chain_id = certificate.block().header.chain_id;
        self.chain_write(chain_id, |mut guard| async move {
            guard
                .process_confirmed_block(certificate, notify_when_messages_are_delivered)
                .await
        })
        .await
    }

    /// Processes a validated block issued from a multi-owner chain.
    #[instrument(level = "trace", skip(self, certificate), fields(
        nickname = %self.nickname(),
        chain_id = %certificate.block().header.chain_id,
        block_height = %certificate.block().header.height
    ))]
    async fn process_validated_block(
        &self,
        certificate: ValidatedBlockCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions, BlockOutcome), WorkerError> {
        let chain_id = certificate.block().header.chain_id;
        self.chain_write(chain_id, |mut guard| async move {
            guard.process_validated_block(certificate).await
        })
        .await
    }

    /// Processes a leader timeout issued from a multi-owner chain.
    #[instrument(level = "trace", skip(self, certificate), fields(
        nickname = %self.nickname(),
        chain_id = %certificate.value().chain_id(),
        height = %certificate.value().height()
    ))]
    async fn process_timeout(
        &self,
        certificate: TimeoutCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        let chain_id = certificate.value().chain_id();
        self.chain_write(chain_id, |mut guard| async move {
            guard.process_timeout(certificate).await
        })
        .await
    }

    #[instrument(level = "trace", skip(self, origin, recipient, bundles), fields(
        nickname = %self.nickname(),
        origin = %origin,
        recipient = %recipient,
        num_bundles = %bundles.len()
    ))]
    async fn process_cross_chain_update(
        &self,
        origin: ChainId,
        recipient: ChainId,
        bundles: Vec<(Epoch, MessageBundle)>,
    ) -> Result<Option<BlockHeight>, WorkerError> {
        self.chain_write(recipient, |mut guard| async move {
            guard.process_cross_chain_update(origin, bundles).await
        })
        .await
    }

    /// Returns a stored [`ConfirmedBlockCertificate`] for a chain's block.
    #[instrument(level = "trace", skip(self, chain_id, height), fields(
        nickname = %self.nickname(),
        chain_id = %chain_id,
        height = %height
    ))]
    #[cfg(with_testing)]
    pub async fn read_certificate(
        &self,
        chain_id: ChainId,
        height: BlockHeight,
    ) -> Result<Option<ConfirmedBlockCertificate>, WorkerError> {
        let state = self.get_or_create_chain_worker(chain_id).await?;
        let guard = handle::read_lock_initialized(&state).await?;
        guard.read_certificate(height).await
    }

    /// Returns a read-only view of the [`ChainStateView`] of a chain referenced by its
    /// [`ChainId`].
    ///
    /// The returned guard holds a read lock on the chain state, preventing writes for
    /// its lifetime. Multiple concurrent readers are allowed.
    #[instrument(level = "trace", skip(self), fields(
        nickname = %self.nickname(),
        chain_id = %chain_id
    ))]
    pub async fn chain_state_view(
        &self,
        chain_id: ChainId,
    ) -> Result<ChainStateViewReadGuard<StorageClient>, WorkerError> {
        let state = self.get_or_create_chain_worker(chain_id).await?;
        let guard = handle::read_lock(&state).await;
        Ok(ChainStateViewReadGuard(OwnedRwLockReadGuard::map(
            guard,
            |s| s.chain(),
        )))
    }

    #[instrument(skip_all, fields(
        nick = self.nickname(),
        chain_id = format!("{:.8}", proposal.content.block.chain_id),
        height = %proposal.content.block.height,
    ))]
    pub async fn handle_block_proposal(
        &self,
        proposal: BlockProposal,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        trace!("{} <-- {:?}", self.nickname(), proposal);
        #[cfg(with_metrics)]
        let round = proposal.content.round;

        let chain_id = proposal.content.block.chain_id;
        // Delay if block timestamp is in the future but within grace period.
        let now = self.storage.clock().current_time();
        let block_timestamp = proposal.content.block.timestamp;
        let delta = block_timestamp.delta_since(now);
        let grace_period = TimeDelta::from_micros(
            u64::try_from(self.chain_worker_config.block_time_grace_period.as_micros())
                .unwrap_or(u64::MAX),
        );
        if delta > TimeDelta::ZERO && delta <= grace_period {
            self.storage.clock().sleep_until(block_timestamp).await;
        }

        let response = self
            .chain_write(chain_id, |mut guard| async move {
                guard.handle_block_proposal(proposal).await
            })
            .await?;
        #[cfg(with_metrics)]
        metrics::NUM_ROUNDS_IN_BLOCK_PROPOSAL
            .with_label_values(&[round.type_name()])
            .observe(round.number() as f64);
        Ok(response)
    }

    /// Processes a certificate, e.g. to extend a chain with a confirmed block.
    // Other fields will be included in the caller's span.
    #[instrument(skip_all, fields(hash = %certificate.value.value_hash))]
    pub async fn handle_lite_certificate(
        &self,
        certificate: LiteCertificate<'_>,
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        match self.full_certificate(certificate).await? {
            Either::Left(confirmed) => {
                Box::pin(
                    self.handle_confirmed_certificate(
                        confirmed,
                        notify_when_messages_are_delivered,
                    ),
                )
                .await
            }
            Either::Right(validated) => {
                if let Some(notifier) = notify_when_messages_are_delivered {
                    // Nothing to wait for.
                    if let Err(()) = notifier.send(()) {
                        warn!("Failed to notify message delivery to caller");
                    }
                }
                Box::pin(self.handle_validated_certificate(validated)).await
            }
        }
    }

    /// Processes a confirmed block certificate.
    #[instrument(skip_all, fields(
        nick = self.nickname(),
        chain_id = format!("{:.8}", certificate.block().header.chain_id),
        height = %certificate.block().header.height,
    ))]
    pub async fn handle_confirmed_certificate(
        &self,
        certificate: ConfirmedBlockCertificate,
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        trace!("{} <-- {:?}", self.nickname(), certificate);
        #[cfg(with_metrics)]
        let metrics_data = metrics::MetricsData::new(&certificate);

        #[allow(unused_variables)]
        let (info, actions, outcome) =
            Box::pin(self.process_confirmed_block(certificate, notify_when_messages_are_delivered))
                .await?;

        #[cfg(with_metrics)]
        if matches!(outcome, BlockOutcome::Processed) {
            metrics_data.record();
        }
        Ok((info, actions))
    }

    /// Processes a validated block certificate.
    #[instrument(skip_all, fields(
        nick = self.nickname(),
        chain_id = format!("{:.8}", certificate.block().header.chain_id),
        height = %certificate.block().header.height,
    ))]
    pub async fn handle_validated_certificate(
        &self,
        certificate: ValidatedBlockCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        trace!("{} <-- {:?}", self.nickname(), certificate);

        #[cfg(with_metrics)]
        let round = certificate.round;
        #[cfg(with_metrics)]
        let cert_str = certificate.inner().to_log_str();

        #[allow(unused_variables)]
        let (info, actions, outcome) = Box::pin(self.process_validated_block(certificate)).await?;
        #[cfg(with_metrics)]
        {
            if matches!(outcome, BlockOutcome::Processed) {
                metrics::NUM_ROUNDS_IN_CERTIFICATE
                    .with_label_values(&[cert_str, round.type_name()])
                    .observe(round.number() as f64);
            }
        }
        Ok((info, actions))
    }

    /// Processes a timeout certificate
    #[instrument(skip_all, fields(
        nick = self.nickname(),
        chain_id = format!("{:.8}", certificate.inner().chain_id()),
        height = %certificate.inner().height(),
    ))]
    pub async fn handle_timeout_certificate(
        &self,
        certificate: TimeoutCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        trace!("{} <-- {:?}", self.nickname(), certificate);
        self.process_timeout(certificate).await
    }

    #[instrument(skip_all, fields(
        nick = self.nickname(),
        chain_id = format!("{:.8}", query.chain_id)
    ))]
    pub async fn handle_chain_info_query(
        &self,
        query: ChainInfoQuery,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        trace!("{} <-- {:?}", self.nickname(), query);
        #[cfg(with_metrics)]
        metrics::CHAIN_INFO_QUERIES.inc();
        let chain_id = query.chain_id;
        let result = self
            .chain_write(chain_id, |mut guard| async move {
                guard.handle_chain_info_query(query).await
            })
            .await;
        trace!("{} --> {:?}", self.nickname(), result);
        result
    }

    #[instrument(skip_all, fields(
        nick = self.nickname(),
        chain_id = format!("{:.8}", chain_id)
    ))]
    pub async fn download_pending_blob(
        &self,
        chain_id: ChainId,
        blob_id: BlobId,
    ) -> Result<Blob, WorkerError> {
        trace!(
            "{} <-- download_pending_blob({chain_id:8}, {blob_id:8})",
            self.nickname()
        );
        let result = self
            .chain_read(chain_id, |guard| async move {
                guard.download_pending_blob(blob_id).await
            })
            .await;
        trace!(
            "{} --> {:?}",
            self.nickname(),
            result.as_ref().map(|_| blob_id)
        );
        result
    }

    #[instrument(skip_all, fields(
        nick = self.nickname(),
        chain_id = format!("{:.8}", chain_id)
    ))]
    pub async fn handle_pending_blob(
        &self,
        chain_id: ChainId,
        blob: Blob,
    ) -> Result<ChainInfoResponse, WorkerError> {
        let blob_id = blob.id();
        trace!(
            "{} <-- handle_pending_blob({chain_id:8}, {blob_id:8})",
            self.nickname()
        );
        let result = self
            .chain_write(chain_id, |mut guard| async move {
                guard.handle_pending_blob(blob).await
            })
            .await;
        trace!(
            "{} --> {:?}",
            self.nickname(),
            result.as_ref().map(|_| blob_id)
        );
        result
    }

    #[instrument(skip_all, fields(
        nick = self.nickname(),
        chain_id = format!("{:.8}", request.target_chain_id())
    ))]
    pub async fn handle_cross_chain_request(
        &self,
        request: CrossChainRequest,
    ) -> Result<NetworkActions, WorkerError> {
        trace!("{} <-- {:?}", self.nickname(), request);
        match request {
            CrossChainRequest::UpdateRecipient {
                sender,
                recipient,
                bundles,
            } => {
                let mut actions = NetworkActions::default();
                let origin = sender;
                let Some(height) = self
                    .process_cross_chain_update(origin, recipient, bundles)
                    .await?
                else {
                    return Ok(actions);
                };
                actions.notifications.push(Notification {
                    chain_id: recipient,
                    reason: Reason::NewIncomingBundle { origin, height },
                });
                actions
                    .cross_chain_requests
                    .push(CrossChainRequest::ConfirmUpdatedRecipient {
                        sender,
                        recipient,
                        latest_height: height,
                    });
                Ok(actions)
            }
            CrossChainRequest::ConfirmUpdatedRecipient {
                sender,
                recipient,
                latest_height,
            } => {
                self.chain_write(sender, |mut guard| async move {
                    guard
                        .confirm_updated_recipient(recipient, latest_height)
                        .await
                })
                .await?;
                Ok(NetworkActions::default())
            }
        }
    }

    /// Updates the received certificate trackers to at least the given values.
    #[instrument(skip_all, fields(
        nickname = %self.nickname(),
        chain_id = %chain_id,
        num_trackers = %new_trackers.len()
    ))]
    pub async fn update_received_certificate_trackers(
        &self,
        chain_id: ChainId,
        new_trackers: BTreeMap<ValidatorPublicKey, u64>,
    ) -> Result<(), WorkerError> {
        self.chain_write(chain_id, |mut guard| async move {
            guard
                .update_received_certificate_trackers(new_trackers)
                .await
        })
        .await
    }

    /// Gets preprocessed block hashes in a given height range.
    #[instrument(skip_all, fields(
        nickname = %self.nickname(),
        chain_id = %chain_id,
        start = %start,
        end = %end
    ))]
    pub async fn get_preprocessed_block_hashes(
        &self,
        chain_id: ChainId,
        start: BlockHeight,
        end: BlockHeight,
    ) -> Result<Vec<CryptoHash>, WorkerError> {
        self.chain_read(chain_id, |guard| async move {
            guard.get_preprocessed_block_hashes(start, end).await
        })
        .await
    }

    /// Gets the next block height to receive from an inbox.
    #[instrument(skip_all, fields(
        nickname = %self.nickname(),
        chain_id = %chain_id,
        origin = %origin
    ))]
    pub async fn get_inbox_next_height(
        &self,
        chain_id: ChainId,
        origin: ChainId,
    ) -> Result<BlockHeight, WorkerError> {
        self.chain_read(chain_id, |guard| async move {
            guard.get_inbox_next_height(origin).await
        })
        .await
    }

    /// Gets locking blobs for specific blob IDs.
    /// Returns `Ok(None)` if any of the blobs is not found.
    #[instrument(skip_all, fields(
        nickname = %self.nickname(),
        chain_id = %chain_id,
        num_blob_ids = %blob_ids.len()
    ))]
    pub async fn get_locking_blobs(
        &self,
        chain_id: ChainId,
        blob_ids: Vec<BlobId>,
    ) -> Result<Option<Vec<Blob>>, WorkerError> {
        self.chain_read(chain_id, |guard| async move {
            guard.get_locking_blobs(blob_ids).await
        })
        .await
    }

    /// Gets block hashes for the given heights.
    pub async fn get_block_hashes(
        &self,
        chain_id: ChainId,
        heights: Vec<BlockHeight>,
    ) -> Result<Vec<CryptoHash>, WorkerError> {
        self.chain_read(chain_id, |guard| async move {
            guard.get_block_hashes(heights).await
        })
        .await
    }

    /// Gets proposed blobs from the manager for specified blob IDs.
    pub async fn get_proposed_blobs(
        &self,
        chain_id: ChainId,
        blob_ids: Vec<BlobId>,
    ) -> Result<Vec<Blob>, WorkerError> {
        self.chain_read(chain_id, |guard| async move {
            guard.get_proposed_blobs(blob_ids).await
        })
        .await
    }

    /// Gets event subscriptions from the chain.
    pub async fn get_event_subscriptions(
        &self,
        chain_id: ChainId,
    ) -> Result<EventSubscriptionsResult, WorkerError> {
        self.chain_read(chain_id, |guard| async move {
            guard.get_event_subscriptions().await
        })
        .await
    }

    /// Gets the next expected event index for a stream.
    pub async fn get_next_expected_event(
        &self,
        chain_id: ChainId,
        stream_id: StreamId,
    ) -> Result<Option<u32>, WorkerError> {
        self.chain_read(chain_id, |guard| async move {
            guard.get_next_expected_event(stream_id).await
        })
        .await
    }

    /// Gets received certificate trackers.
    pub async fn get_received_certificate_trackers(
        &self,
        chain_id: ChainId,
    ) -> Result<HashMap<ValidatorPublicKey, u64>, WorkerError> {
        self.chain_read(chain_id, |guard| async move {
            guard.get_received_certificate_trackers().await
        })
        .await
    }

    /// Gets tip state and outbox info for next_outbox_heights calculation.
    pub async fn get_tip_state_and_outbox_info(
        &self,
        chain_id: ChainId,
        receiver_id: ChainId,
    ) -> Result<(BlockHeight, Option<BlockHeight>), WorkerError> {
        self.chain_read(chain_id, |guard| async move {
            guard.get_tip_state_and_outbox_info(receiver_id).await
        })
        .await
    }

    /// Gets the next height to preprocess.
    pub async fn get_next_height_to_preprocess(
        &self,
        chain_id: ChainId,
    ) -> Result<BlockHeight, WorkerError> {
        self.chain_read(chain_id, |guard| async move {
            guard.get_next_height_to_preprocess().await
        })
        .await
    }

    /// Gets the chain manager's seed for leader election.
    pub async fn get_manager_seed(&self, chain_id: ChainId) -> Result<u64, WorkerError> {
        self.chain_read(
            chain_id,
            |guard| async move { guard.get_manager_seed().await },
        )
        .await
    }

    /// Gets the stream event count for a stream.
    pub async fn get_stream_event_count(
        &self,
        chain_id: ChainId,
        stream_id: StreamId,
    ) -> Result<Option<u32>, WorkerError> {
        self.chain_read(chain_id, |guard| async move {
            guard.get_stream_event_count(stream_id).await
        })
        .await
    }

    /// Gets the previous event blocks for specific streams.
    pub async fn previous_event_blocks(
        &self,
        chain_id: ChainId,
        stream_ids: Vec<StreamId>,
    ) -> Result<BTreeMap<StreamId, (BlockHeight, CryptoHash)>, WorkerError> {
        self.chain_read(chain_id, |guard| async move {
            guard.get_previous_event_blocks(stream_ids).await
        })
        .await
    }
}

#[cfg(with_testing)]
impl<StorageClient> WorkerState<StorageClient>
where
    StorageClient: Storage + Clone + 'static,
{
    /// Gets a reference to the validator's [`ValidatorPublicKey`].
    ///
    /// # Panics
    ///
    /// If the validator doesn't have a key pair assigned to it.
    #[instrument(level = "trace", skip(self))]
    pub fn public_key(&self) -> ValidatorPublicKey {
        self.chain_worker_config
            .key_pair()
            .expect(
                "Test validator should have a key pair assigned to it \
                in order to obtain its public key",
            )
            .public()
    }
}

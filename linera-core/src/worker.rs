// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use futures::future::Either;
use linera_base::{
    crypto::{CryptoError, CryptoHash, ValidatorPublicKey, ValidatorSecretKey},
    data_types::{ApplicationDescription, ArithmeticError, Blob, BlockHeight, Epoch, Round},
    doc_scalar,
    hashed::Hashed,
    identifiers::{AccountOwner, ApplicationId, BlobId, ChainId, EventId, StreamId},
    time::timer::{sleep, timeout},
};
#[cfg(with_testing)]
use linera_chain::ChainExecutionContext;
use linera_chain::{
    data_types::{BlockExecutionOutcome, BlockProposal, MessageBundle, ProposedBlock},
    types::{
        Block, CertificateValue, ConfirmedBlock, ConfirmedBlockCertificate, GenericCertificate,
        LiteCertificate, Timeout, TimeoutCertificate, ValidatedBlock, ValidatedBlockCertificate,
    },
    ChainError, ChainStateView,
};
use linera_execution::{ExecutionError, ExecutionStateView, Query, QueryOutcome};
use linera_storage::Storage;
use linera_views::{context::InactiveContext, ViewError};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, OwnedRwLockReadGuard};
use tracing::{error, instrument, trace, warn};

use crate::{
    chain_worker::{ChainWorkerActor, ChainWorkerConfig, ChainWorkerRequest, DeliveryNotifier},
    data_types::{ChainInfoQuery, ChainInfoResponse, CrossChainRequest},
    join_set_ext::{JoinSet, JoinSetExt},
    notifier::Notifier,
    value_cache::ValueCache,
};

#[cfg(test)]
#[path = "unit_tests/worker_tests.rs"]
mod worker_tests;

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{
        exponential_bucket_interval, register_histogram_vec, register_int_counter,
        register_int_counter_vec,
    };
    use prometheus::{HistogramVec, IntCounter, IntCounterVec};

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

    // Chain access control
    #[error("Block was not signed by an authorized owner")]
    InvalidOwner,

    #[error("Operations in the block are not authenticated by the proper signer: {0}")]
    InvalidSigner(AccountOwner),

    // Chaining
    #[error(
        "Was expecting block height {expected_block_height} but found {found_block_height} instead"
    )]
    UnexpectedBlockHeight {
        expected_block_height: BlockHeight,
        found_block_height: BlockHeight,
    },
    #[error("Unexpected epoch {epoch:}: chain {chain_id:} is at {chain_epoch:}")]
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
    #[error("The block timestamp is in the future.")]
    InvalidTimestamp,
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
    #[error("The worker is too busy to handle new chains")]
    FullChainWorkerCache,
    #[error("Failed to join spawned worker task")]
    JoinError,
    #[error("Blob was not required by any pending block")]
    UnexpectedBlob,
    #[error("Number of published blobs per block must not exceed {0}")]
    TooManyPublishedBlobs(u64),
    #[error("Missing network description")]
    MissingNetworkDescription,
}

impl From<ChainError> for WorkerError {
    #[instrument(level = "trace", skip(chain_error))]
    fn from(chain_error: ChainError) -> Self {
        match chain_error {
            ChainError::ExecutionError(execution_error, context) => {
                if let ExecutionError::BlobsNotFound(blob_ids) = *execution_error {
                    Self::BlobsNotFound(blob_ids)
                } else {
                    Self::ChainError(Box::new(ChainError::ExecutionError(
                        execution_error,
                        context,
                    )))
                }
            }
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

/// State of a worker in a validator or a local node.
pub struct WorkerState<StorageClient>
where
    StorageClient: Storage,
{
    /// A name used for logging
    nickname: String,
    /// Access to local persistent storage.
    storage: StorageClient,
    /// Configuration options for the [`ChainWorker`]s.
    chain_worker_config: ChainWorkerConfig,
    block_cache: Arc<ValueCache<CryptoHash, Hashed<Block>>>,
    execution_state_cache: Arc<ValueCache<CryptoHash, ExecutionStateView<InactiveContext>>>,
    /// Chain IDs that should be tracked by a worker.
    tracked_chains: Option<Arc<RwLock<HashSet<ChainId>>>>,
    /// One-shot channels to notify callers when messages of a particular chain have been
    /// delivered.
    delivery_notifiers: Arc<Mutex<DeliveryNotifiers>>,
    /// The set of spawned [`ChainWorkerActor`] tasks.
    chain_worker_tasks: Arc<Mutex<JoinSet>>,
    /// The cache of running [`ChainWorkerActor`]s.
    chain_workers: Arc<Mutex<BTreeMap<ChainId, ChainActorEndpoint<StorageClient>>>>,
}

impl<StorageClient> Clone for WorkerState<StorageClient>
where
    StorageClient: Storage + Clone,
{
    fn clone(&self) -> Self {
        WorkerState {
            nickname: self.nickname.clone(),
            storage: self.storage.clone(),
            chain_worker_config: self.chain_worker_config.clone(),
            block_cache: self.block_cache.clone(),
            execution_state_cache: self.execution_state_cache.clone(),
            tracked_chains: self.tracked_chains.clone(),
            delivery_notifiers: self.delivery_notifiers.clone(),
            chain_worker_tasks: self.chain_worker_tasks.clone(),
            chain_workers: self.chain_workers.clone(),
        }
    }
}

/// The sender endpoint for [`ChainWorkerRequest`]s.
type ChainActorEndpoint<StorageClient> = mpsc::UnboundedSender<(
    ChainWorkerRequest<<StorageClient as Storage>::Context>,
    tracing::Span,
)>;

pub(crate) type DeliveryNotifiers = HashMap<ChainId, DeliveryNotifier>;

impl<StorageClient> WorkerState<StorageClient>
where
    StorageClient: Storage,
{
    #[instrument(level = "trace", skip(nickname, key_pair, storage))]
    pub fn new(
        nickname: String,
        key_pair: Option<ValidatorSecretKey>,
        storage: StorageClient,
    ) -> Self {
        WorkerState {
            nickname,
            storage,
            chain_worker_config: ChainWorkerConfig::default().with_key_pair(key_pair),
            block_cache: Arc::new(ValueCache::default()),
            execution_state_cache: Arc::new(ValueCache::default()),
            tracked_chains: None,
            delivery_notifiers: Arc::default(),
            chain_worker_tasks: Arc::default(),
            chain_workers: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    #[instrument(level = "trace", skip(nickname, storage))]
    pub fn new_for_client(
        nickname: String,
        storage: StorageClient,
        tracked_chains: Arc<RwLock<HashSet<ChainId>>>,
    ) -> Self {
        WorkerState {
            nickname,
            storage,
            chain_worker_config: ChainWorkerConfig::default(),
            block_cache: Arc::new(ValueCache::default()),
            execution_state_cache: Arc::new(ValueCache::default()),
            tracked_chains: Some(tracked_chains),
            delivery_notifiers: Arc::default(),
            chain_worker_tasks: Arc::default(),
            chain_workers: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    #[instrument(level = "trace", skip(self, value))]
    pub fn with_allow_inactive_chains(mut self, value: bool) -> Self {
        self.chain_worker_config.allow_inactive_chains = value;
        self
    }

    #[instrument(level = "trace", skip(self, value))]
    pub fn with_allow_messages_from_deprecated_epochs(mut self, value: bool) -> Self {
        self.chain_worker_config
            .allow_messages_from_deprecated_epochs = value;
        self
    }

    #[instrument(level = "trace", skip(self, value))]
    pub fn with_long_lived_services(mut self, value: bool) -> Self {
        self.chain_worker_config.long_lived_services = value;
        self
    }

    /// Returns an instance with the specified grace period.
    ///
    /// Blocks with a timestamp this far in the future will still be accepted, but the validator
    /// will wait until that timestamp before voting.
    #[instrument(level = "trace", skip(self))]
    pub fn with_grace_period(mut self, grace_period: Duration) -> Self {
        self.chain_worker_config.grace_period = grace_period;
        self
    }

    /// Returns an instance with the specified chain worker TTL.
    ///
    /// Idle chain workers free their memory after that duration without requests.
    #[instrument(level = "trace", skip(self))]
    pub fn with_chain_worker_ttl(mut self, chain_worker_ttl: Duration) -> Self {
        self.chain_worker_config.ttl = chain_worker_ttl;
        self
    }

    #[instrument(level = "trace", skip(self))]
    pub fn nickname(&self) -> &str {
        &self.nickname
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
            .get(&certificate.value.value_hash)
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
            _ => return Err(WorkerError::InvalidLiteCertificate),
        }
    }
}

#[allow(async_fn_in_trait)]
#[cfg_attr(not(web), trait_variant::make(Send))]
pub trait ProcessableCertificate: CertificateValue + Sized + 'static {
    async fn process_certificate<S: Storage + Clone + Send + Sync + 'static>(
        worker: &WorkerState<S>,
        certificate: GenericCertificate<Self>,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError>;
}

impl ProcessableCertificate for ConfirmedBlock {
    async fn process_certificate<S: Storage + Clone + Send + Sync + 'static>(
        worker: &WorkerState<S>,
        certificate: ConfirmedBlockCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        worker.handle_confirmed_certificate(certificate, None).await
    }
}

impl ProcessableCertificate for ValidatedBlock {
    async fn process_certificate<S: Storage + Clone + Send + Sync + 'static>(
        worker: &WorkerState<S>,
        certificate: ValidatedBlockCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        worker.handle_validated_certificate(certificate).await
    }
}

impl ProcessableCertificate for Timeout {
    async fn process_certificate<S: Storage + Clone + Send + Sync + 'static>(
        worker: &WorkerState<S>,
        certificate: TimeoutCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        worker.handle_timeout_certificate(certificate).await
    }
}

impl<StorageClient> WorkerState<StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
{
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
        linera_base::task::spawn(async move {
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
        .unwrap_or_else(|_| Err(WorkerError::JoinError))
    }

    /// Tries to execute a block proposal without any verification other than block execution.
    #[instrument(level = "trace", skip(self, block))]
    pub async fn stage_block_execution(
        &self,
        block: ProposedBlock,
        round: Option<u32>,
        published_blobs: Vec<Blob>,
    ) -> Result<(Block, ChainInfoResponse), WorkerError> {
        self.query_chain_worker(block.chain_id, move |callback| {
            ChainWorkerRequest::StageBlockExecution {
                block,
                round,
                published_blobs,
                callback,
            }
        })
        .await
    }

    /// Executes a [`Query`] for an application's state on a specific chain.
    #[instrument(level = "trace", skip(self, chain_id, query))]
    pub async fn query_application(
        &self,
        chain_id: ChainId,
        query: Query,
    ) -> Result<QueryOutcome, WorkerError> {
        self.query_chain_worker(chain_id, move |callback| {
            ChainWorkerRequest::QueryApplication { query, callback }
        })
        .await
    }

    #[instrument(level = "trace", skip(self, chain_id, application_id))]
    pub async fn describe_application(
        &self,
        chain_id: ChainId,
        application_id: ApplicationId,
    ) -> Result<ApplicationDescription, WorkerError> {
        self.query_chain_worker(chain_id, move |callback| {
            ChainWorkerRequest::DescribeApplication {
                application_id,
                callback,
            }
        })
        .await
    }

    /// Processes a confirmed block (aka a commit).
    #[instrument(
        level = "trace",
        skip(self, certificate, notify_when_messages_are_delivered)
    )]
    async fn process_confirmed_block(
        &self,
        certificate: ConfirmedBlockCertificate,
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        let chain_id = certificate.block().header.chain_id;
        self.query_chain_worker(chain_id, move |callback| {
            ChainWorkerRequest::ProcessConfirmedBlock {
                certificate,
                notify_when_messages_are_delivered,
                callback,
            }
        })
        .await
    }

    /// Processes a validated block issued from a multi-owner chain.
    #[instrument(level = "trace", skip(self, certificate))]
    async fn process_validated_block(
        &self,
        certificate: ValidatedBlockCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions, bool), WorkerError> {
        let chain_id = certificate.block().header.chain_id;
        self.query_chain_worker(chain_id, move |callback| {
            ChainWorkerRequest::ProcessValidatedBlock {
                certificate,
                callback,
            }
        })
        .await
    }

    /// Processes a leader timeout issued from a multi-owner chain.
    #[instrument(level = "trace", skip(self, certificate))]
    async fn process_timeout(
        &self,
        certificate: TimeoutCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        let chain_id = certificate.value().chain_id();
        self.query_chain_worker(chain_id, move |callback| {
            ChainWorkerRequest::ProcessTimeout {
                certificate,
                callback,
            }
        })
        .await
    }

    #[instrument(level = "trace", skip(self, origin, recipient, bundles))]
    async fn process_cross_chain_update(
        &self,
        origin: ChainId,
        recipient: ChainId,
        bundles: Vec<(Epoch, MessageBundle)>,
    ) -> Result<Option<BlockHeight>, WorkerError> {
        self.query_chain_worker(recipient, move |callback| {
            ChainWorkerRequest::ProcessCrossChainUpdate {
                origin,
                bundles,
                callback,
            }
        })
        .await
    }

    /// Returns a stored [`ConfirmedBlockCertificate`] for a chain's block.
    #[instrument(level = "trace", skip(self, chain_id, height))]
    #[cfg(with_testing)]
    pub async fn read_certificate(
        &self,
        chain_id: ChainId,
        height: BlockHeight,
    ) -> Result<Option<ConfirmedBlockCertificate>, WorkerError> {
        self.query_chain_worker(chain_id, move |callback| {
            ChainWorkerRequest::ReadCertificate { height, callback }
        })
        .await
    }

    /// Returns a read-only view of the [`ChainStateView`] of a chain referenced by its
    /// [`ChainId`].
    ///
    /// The returned view holds a lock on the chain state, which prevents the worker from changing
    /// the state of that chain.
    #[instrument(level = "trace", skip(self))]
    pub async fn chain_state_view(
        &self,
        chain_id: ChainId,
    ) -> Result<OwnedRwLockReadGuard<ChainStateView<StorageClient::Context>>, WorkerError> {
        self.query_chain_worker(chain_id, |callback| ChainWorkerRequest::GetChainStateView {
            callback,
        })
        .await
    }

    #[instrument(level = "trace", skip(self, request_builder))]
    /// Sends a request to the [`ChainWorker`] for a [`ChainId`] and waits for the `Response`.
    async fn query_chain_worker<Response>(
        &self,
        chain_id: ChainId,
        request_builder: impl FnOnce(
            oneshot::Sender<Result<Response, WorkerError>>,
        ) -> ChainWorkerRequest<StorageClient::Context>,
    ) -> Result<Response, WorkerError> {
        let chain_actor = self.get_chain_worker_endpoint(chain_id).await?;
        let (callback, response) = oneshot::channel();

        chain_actor
            .send((request_builder(callback), tracing::Span::current()))
            .expect("`ChainWorkerActor` stopped executing unexpectedly");

        response
            .await
            .expect("`ChainWorkerActor` stopped executing without responding")
    }

    /// Retrieves an endpoint to a [`ChainWorkerActor`] from the cache, creating one and adding it
    /// to the cache if needed.
    #[instrument(level = "trace", skip(self))]
    async fn get_chain_worker_endpoint(
        &self,
        chain_id: ChainId,
    ) -> Result<ChainActorEndpoint<StorageClient>, WorkerError> {
        let (sender, new_receiver) = timeout(Duration::from_secs(3), async move {
            loop {
                match self.try_get_chain_worker_endpoint(chain_id) {
                    Some(endpoint) => break endpoint,
                    None => sleep(Duration::from_millis(250)).await,
                }
            }
        })
        .await
        .map_err(|_| WorkerError::FullChainWorkerCache)?;

        if let Some(receiver) = new_receiver {
            let delivery_notifier = self
                .delivery_notifiers
                .lock()
                .unwrap()
                .entry(chain_id)
                .or_default()
                .clone();

            let actor_task = ChainWorkerActor::run(
                self.chain_worker_config.clone(),
                self.storage.clone(),
                self.block_cache.clone(),
                self.execution_state_cache.clone(),
                self.tracked_chains.clone(),
                delivery_notifier,
                chain_id,
                receiver,
            );

            self.chain_worker_tasks
                .lock()
                .unwrap()
                .spawn_task(actor_task);
        }

        Ok(sender)
    }

    /// Retrieves an endpoint to a [`ChainWorkerActor`] from the cache, attempting to create one
    /// and add it to the cache if needed.
    ///
    /// Returns [`None`] if the cache is full and no candidate for eviction was found.
    #[instrument(level = "trace", skip(self))]
    #[expect(clippy::type_complexity)]
    fn try_get_chain_worker_endpoint(
        &self,
        chain_id: ChainId,
    ) -> Option<(
        ChainActorEndpoint<StorageClient>,
        Option<
            mpsc::UnboundedReceiver<(ChainWorkerRequest<StorageClient::Context>, tracing::Span)>,
        >,
    )> {
        let mut chain_workers = self.chain_workers.lock().unwrap();

        if let Some(endpoint) = chain_workers.get(&chain_id) {
            Some((endpoint.clone(), None))
        } else {
            let (sender, receiver) = mpsc::unbounded_channel();
            chain_workers.insert(chain_id, sender.clone());
            Some((sender, Some(receiver)))
        }
    }

    #[instrument(skip_all, fields(
        nick = self.nickname,
        chain_id = format!("{:.8}", proposal.content.block.chain_id),
        height = %proposal.content.block.height,
    ))]
    pub async fn handle_block_proposal(
        &self,
        proposal: BlockProposal,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        trace!("{} <-- {:?}", self.nickname, proposal);
        #[cfg(with_metrics)]
        let round = proposal.content.round;
        let response = self
            .query_chain_worker(proposal.content.block.chain_id, move |callback| {
                ChainWorkerRequest::HandleBlockProposal { proposal, callback }
            })
            .await?;
        #[cfg(with_metrics)]
        metrics::NUM_ROUNDS_IN_BLOCK_PROPOSAL
            .with_label_values(&[round.type_name()])
            .observe(round.number() as f64);
        Ok(response)
    }

    /// Processes a certificate, e.g. to extend a chain with a confirmed block.
    // Other fields will be included in handle_certificate's span.
    #[instrument(skip_all, fields(hash = %certificate.value.value_hash))]
    pub async fn handle_lite_certificate(
        &self,
        certificate: LiteCertificate<'_>,
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        match self.full_certificate(certificate).await? {
            Either::Left(confirmed) => {
                self.handle_confirmed_certificate(confirmed, notify_when_messages_are_delivered)
                    .await
            }
            Either::Right(validated) => {
                if let Some(notifier) = notify_when_messages_are_delivered {
                    // Nothing to wait for.
                    if let Err(()) = notifier.send(()) {
                        warn!("Failed to notify message delivery to caller");
                    }
                }
                self.handle_validated_certificate(validated).await
            }
        }
    }

    /// Processes a confirmed block certificate.
    #[instrument(skip_all, fields(
        nick = self.nickname,
        chain_id = format!("{:.8}", certificate.block().header.chain_id),
        height = %certificate.block().header.height,
    ))]
    pub async fn handle_confirmed_certificate(
        &self,
        certificate: ConfirmedBlockCertificate,
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        trace!("{} <-- {:?}", self.nickname, certificate);
        #[cfg(with_metrics)]
        let metrics_data = if self
            .chain_state_view(certificate.block().header.chain_id)
            .await?
            .tip_state
            .get()
            .next_block_height
            > certificate.block().header.height
        {
            // Block already processed, no metrics to report.
            None
        } else {
            Some((
                certificate.inner().to_log_str(),
                certificate.round.type_name(),
                certificate.round.number(),
                certificate.block().body.transactions.len() as u64,
                certificate
                    .signatures()
                    .iter()
                    .map(|(validator_name, _)| validator_name.to_string())
                    .collect::<Vec<_>>(),
            ))
        };

        let result = self
            .process_confirmed_block(certificate, notify_when_messages_are_delivered)
            .await?;

        #[cfg(with_metrics)]
        {
            if let Some(metrics_data) = metrics_data {
                let (
                    certificate_log_str,
                    round_type,
                    round_number,
                    confirmed_transactions,
                    validators_with_signatures,
                ) = metrics_data;
                metrics::NUM_BLOCKS.with_label_values(&[]).inc();
                metrics::NUM_ROUNDS_IN_CERTIFICATE
                    .with_label_values(&[certificate_log_str, round_type])
                    .observe(round_number as f64);
                if confirmed_transactions > 0 {
                    metrics::TRANSACTION_COUNT
                        .with_label_values(&[])
                        .inc_by(confirmed_transactions);
                }

                for validator_name in validators_with_signatures {
                    metrics::CERTIFICATES_SIGNED
                        .with_label_values(&[&validator_name])
                        .inc();
                }
            }
        }
        Ok(result)
    }

    /// Processes a validated block certificate.
    #[instrument(skip_all, fields(
        nick = self.nickname,
        chain_id = format!("{:.8}", certificate.block().header.chain_id),
        height = %certificate.block().header.height,
    ))]
    pub async fn handle_validated_certificate(
        &self,
        certificate: ValidatedBlockCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        trace!("{} <-- {:?}", self.nickname, certificate);

        #[cfg(with_metrics)]
        let round = certificate.round;
        #[cfg(with_metrics)]
        let cert_str = certificate.inner().to_log_str();

        let (info, actions, _duplicated) = self.process_validated_block(certificate).await?;
        #[cfg(with_metrics)]
        {
            if !_duplicated {
                metrics::NUM_ROUNDS_IN_CERTIFICATE
                    .with_label_values(&[cert_str, round.type_name()])
                    .observe(round.number() as f64);
            }
        }
        Ok((info, actions))
    }

    /// Processes a timeout certificate
    #[instrument(skip_all, fields(
        nick = self.nickname,
        chain_id = format!("{:.8}", certificate.inner().chain_id()),
        height = %certificate.inner().height(),
    ))]
    pub async fn handle_timeout_certificate(
        &self,
        certificate: TimeoutCertificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        trace!("{} <-- {:?}", self.nickname, certificate);
        self.process_timeout(certificate).await
    }

    #[instrument(skip_all, fields(
        nick = self.nickname,
        chain_id = format!("{:.8}", query.chain_id)
    ))]
    pub async fn handle_chain_info_query(
        &self,
        query: ChainInfoQuery,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        trace!("{} <-- {:?}", self.nickname, query);
        #[cfg(with_metrics)]
        metrics::CHAIN_INFO_QUERIES.inc();
        let result = self
            .query_chain_worker(query.chain_id, move |callback| {
                ChainWorkerRequest::HandleChainInfoQuery { query, callback }
            })
            .await;
        trace!("{} --> {:?}", self.nickname, result);
        result
    }

    #[instrument(skip_all, fields(
        nick = self.nickname,
        chain_id = format!("{:.8}", chain_id)
    ))]
    pub async fn download_pending_blob(
        &self,
        chain_id: ChainId,
        blob_id: BlobId,
    ) -> Result<Blob, WorkerError> {
        trace!(
            "{} <-- download_pending_blob({chain_id:8}, {blob_id:8})",
            self.nickname
        );
        let result = self
            .query_chain_worker(chain_id, move |callback| {
                ChainWorkerRequest::DownloadPendingBlob { blob_id, callback }
            })
            .await;
        trace!(
            "{} --> {:?}",
            self.nickname,
            result.as_ref().map(|_| blob_id)
        );
        result
    }

    #[instrument(skip_all, fields(
        nick = self.nickname,
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
            self.nickname
        );
        let result = self
            .query_chain_worker(chain_id, move |callback| {
                ChainWorkerRequest::HandlePendingBlob { blob, callback }
            })
            .await;
        trace!(
            "{} --> {:?}",
            self.nickname,
            result.as_ref().map(|_| blob_id)
        );
        result
    }

    #[instrument(skip_all, fields(
        nick = self.nickname,
        chain_id = format!("{:.8}", request.target_chain_id())
    ))]
    pub async fn handle_cross_chain_request(
        &self,
        request: CrossChainRequest,
    ) -> Result<NetworkActions, WorkerError> {
        trace!("{} <-- {:?}", self.nickname, request);
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
                self.query_chain_worker(sender, move |callback| {
                    ChainWorkerRequest::ConfirmUpdatedRecipient {
                        recipient,
                        latest_height,
                        callback,
                    }
                })
                .await?;
                Ok(NetworkActions::default())
            }
        }
    }

    /// Updates the received certificate trackers to at least the given values.
    pub async fn update_received_certificate_trackers(
        &self,
        chain_id: ChainId,
        new_trackers: BTreeMap<ValidatorPublicKey, u64>,
    ) -> Result<(), WorkerError> {
        self.query_chain_worker(chain_id, move |callback| {
            ChainWorkerRequest::UpdateReceivedCertificateTrackers {
                new_trackers,
                callback,
            }
        })
        .await
    }
}

#[cfg(with_testing)]
impl<StorageClient> WorkerState<StorageClient>
where
    StorageClient: Storage,
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
                in order to obtain it's public key",
            )
            .public()
    }
}

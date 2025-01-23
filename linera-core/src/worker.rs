// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    num::NonZeroUsize,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use futures::future::Either;
#[cfg(with_testing)]
use linera_base::crypto::PublicKey;
use linera_base::{
    crypto::{CryptoError, CryptoHash, KeyPair},
    data_types::{
        ArithmeticError, Blob, BlockHeight, DecompressionError, Round, UserApplicationDescription,
    },
    doc_scalar,
    hashed::Hashed,
    identifiers::{BlobId, ChainId, Owner, UserApplicationId},
    time::timer::{sleep, timeout},
};
use linera_chain::{
    data_types::{
        BlockExecutionOutcome, BlockProposal, ExecutedBlock, MessageBundle, Origin, ProposedBlock,
        Target,
    },
    types::{
        Block, CertificateValue, ConfirmedBlock, ConfirmedBlockCertificate, GenericCertificate,
        LiteCertificate, Timeout, TimeoutCertificate, ValidatedBlock, ValidatedBlockCertificate,
    },
    ChainError, ChainStateView,
};
use linera_execution::{
    committee::{Epoch, ValidatorName},
    ExecutionError, Query, Response,
};
use linera_storage::Storage;
use linera_views::views::ViewError;
use lru::LruCache;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, OwnedRwLockReadGuard};
use tracing::{error, instrument, trace, warn, Instrument as _};
#[cfg(with_metrics)]
use {
    linera_base::prometheus_util::{
        bucket_interval, register_histogram_vec, register_int_counter_vec,
    },
    prometheus::{HistogramVec, IntCounterVec},
    std::sync::LazyLock,
};

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
static NUM_ROUNDS_IN_CERTIFICATE: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "num_rounds_in_certificate",
        "Number of rounds in certificate",
        &["certificate_value", "round_type"],
        bucket_interval(0.1, 50.0),
    )
});

#[cfg(with_metrics)]
static NUM_ROUNDS_IN_BLOCK_PROPOSAL: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "num_rounds_in_block_proposal",
        "Number of rounds in block proposal",
        &["round_type"],
        bucket_interval(0.1, 50.0),
    )
});

#[cfg(with_metrics)]
static TRANSACTION_COUNT: LazyLock<IntCounterVec> =
    LazyLock::new(|| register_int_counter_vec("transaction_count", "Transaction count", &[]));

#[cfg(with_metrics)]
static NUM_BLOCKS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec("num_blocks", "Number of blocks added to chains", &[])
});

#[cfg(with_metrics)]
static CERTIFICATES_SIGNED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "certificates_signed",
        "Number of confirmed block certificates signed by each validator",
        &["validator_name"],
    )
});

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
    },
    NewIncomingBundle {
        origin: Origin,
        height: BlockHeight,
    },
    NewRound {
        height: BlockHeight,
        round: Round,
    },
}

/// Error type for worker operations..
#[derive(Debug, Error)]
pub enum WorkerError {
    #[error(transparent)]
    CryptoError(#[from] CryptoError),

    #[error(transparent)]
    ArithmeticError(#[from] ArithmeticError),

    #[error(transparent)]
    ViewError(ViewError),

    #[error(transparent)]
    ChainError(#[from] Box<ChainError>),

    // Chain access control
    #[error("Block was not signed by an authorized owner")]
    InvalidOwner,

    #[error("Operations in the block are not authenticated by the proper signer: {0}")]
    InvalidSigner(Owner),

    // Chaining
    #[error(
        "Was expecting block height {expected_block_height} but found {found_block_height} instead"
    )]
    UnexpectedBlockHeight {
        expected_block_height: BlockHeight,
        found_block_height: BlockHeight,
    },
    #[error("Cannot confirm a block before its predecessors: {current_block_height:?}")]
    MissingEarlierBlocks { current_block_height: BlockHeight },
    #[error("Unexpected epoch {epoch:}: chain {chain_id:} is at {chain_epoch:}")]
    InvalidEpoch {
        chain_id: ChainId,
        chain_epoch: Epoch,
        epoch: Epoch,
    },

    // Other server-side errors
    #[error("Invalid cross-chain request")]
    InvalidCrossChainRequest,
    #[error("The block does contain the hash that we expected for the previous block")]
    InvalidBlockChaining,
    #[error(
        "
        The given outcome is not what we computed after executing the block.\n\
        Computed: {computed:#?}\n\
        Submitted: {submitted:#?}\n
    "
    )]
    IncorrectOutcome {
        computed: Box<BlockExecutionOutcome>,
        submitted: Box<BlockExecutionOutcome>,
    },
    #[error("The timestamp of a Tick operation is in the future.")]
    InvalidTimestamp,
    #[error("We don't have the value for the certificate.")]
    MissingCertificateValue,
    #[error("The hash certificate doesn't match its value.")]
    InvalidLiteCertificate,
    #[error("An additional blob was provided that is not required: {blob_id}.")]
    UnneededBlob { blob_id: BlobId },
    #[error("The blobs provided in the proposal were not the published ones, in order.")]
    WrongBlobsInProposal,
    #[error("Fast blocks cannot query oracles")]
    FastBlockUsingOracles,
    #[error("Blobs not found: {0:?}")]
    BlobsNotFound(Vec<BlobId>),
    #[error("The block proposal is invalid: {0}")]
    InvalidBlockProposal(String),
    #[error("The worker is too busy to handle new chains")]
    FullChainWorkerCache,
    #[error("Failed to join spawned worker task")]
    JoinError,
    #[error("Blob exceeds size limit")]
    BlobTooLarge,
    #[error("Bytecode exceeds size limit")]
    BytecodeTooLarge,
    #[error(transparent)]
    Decompression(#[from] DecompressionError),
}

impl From<ChainError> for WorkerError {
    #[instrument(level = "trace", skip(chain_error))]
    fn from(chain_error: ChainError) -> Self {
        match chain_error {
            ChainError::BlobsNotFound(blob_ids) => Self::BlobsNotFound(blob_ids),
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

impl From<ViewError> for WorkerError {
    fn from(view_error: ViewError) -> Self {
        match view_error {
            ViewError::BlobsNotFound(blob_ids) => Self::BlobsNotFound(blob_ids),
            error => Self::ViewError(error),
        }
    }
}

/// State of a worker in a validator or a local node.
#[derive(Clone)]
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
    executed_block_cache: Arc<ValueCache<CryptoHash, Hashed<Block>>>,
    /// Chain IDs that should be tracked by a worker.
    tracked_chains: Option<Arc<RwLock<HashSet<ChainId>>>>,
    /// One-shot channels to notify callers when messages of a particular chain have been
    /// delivered.
    delivery_notifiers: Arc<Mutex<DeliveryNotifiers>>,
    /// The set of spawned [`ChainWorkerActor`] tasks.
    chain_worker_tasks: Arc<Mutex<JoinSet>>,
    /// The cache of running [`ChainWorkerActor`]s.
    chain_workers: Arc<Mutex<LruCache<ChainId, ChainActorEndpoint<StorageClient>>>>,
}

/// The sender endpoint for [`ChainWorkerRequest`]s.
type ChainActorEndpoint<StorageClient> =
    mpsc::UnboundedSender<ChainWorkerRequest<<StorageClient as Storage>::Context>>;

pub(crate) type DeliveryNotifiers = HashMap<ChainId, DeliveryNotifier>;

impl<StorageClient> WorkerState<StorageClient>
where
    StorageClient: Storage,
{
    #[instrument(level = "trace", skip(nickname, key_pair, storage))]
    pub fn new(
        nickname: String,
        key_pair: Option<KeyPair>,
        storage: StorageClient,
        chain_worker_limit: NonZeroUsize,
    ) -> Self {
        WorkerState {
            nickname,
            storage,
            chain_worker_config: ChainWorkerConfig::default().with_key_pair(key_pair),
            executed_block_cache: Arc::new(ValueCache::default()),
            tracked_chains: None,
            delivery_notifiers: Arc::default(),
            chain_worker_tasks: Arc::default(),
            chain_workers: Arc::new(Mutex::new(LruCache::new(chain_worker_limit))),
        }
    }

    #[instrument(level = "trace", skip(nickname, storage))]
    pub fn new_for_client(
        nickname: String,
        storage: StorageClient,
        tracked_chains: Arc<RwLock<HashSet<ChainId>>>,
        chain_worker_limit: NonZeroUsize,
    ) -> Self {
        WorkerState {
            nickname,
            storage,
            chain_worker_config: ChainWorkerConfig::default(),
            executed_block_cache: Arc::new(ValueCache::default()),
            tracked_chains: Some(tracked_chains),
            delivery_notifiers: Arc::default(),
            chain_worker_tasks: Arc::default(),
            chain_workers: Arc::new(Mutex::new(LruCache::new(chain_worker_limit))),
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

    #[instrument(level = "trace", skip(self, tracked_chains))]
    /// Configures the subset of chains that this worker is tracking.
    pub fn with_tracked_chains(
        mut self,
        tracked_chains: impl IntoIterator<Item = ChainId>,
    ) -> Self {
        self.tracked_chains = Some(Arc::new(RwLock::new(tracked_chains.into_iter().collect())));
        self
    }

    /// Returns an instance with the specified grace period, in microseconds.
    ///
    /// Blocks with a timestamp this far in the future will still be accepted, but the validator
    /// will wait until that timestamp before voting.
    #[instrument(level = "trace", skip(self, grace_period))]
    pub fn with_grace_period(mut self, grace_period: Duration) -> Self {
        self.chain_worker_config.grace_period = grace_period;
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

    #[instrument(level = "trace", skip(self, key_pair))]
    #[cfg(test)]
    pub(crate) async fn with_key_pair(mut self, key_pair: Option<Arc<KeyPair>>) -> Self {
        self.chain_worker_config.key_pair = key_pair;
        self.chain_workers.lock().unwrap().clear();
        self
    }

    #[instrument(level = "trace", skip(self, certificate))]
    pub(crate) async fn full_certificate(
        &self,
        certificate: LiteCertificate<'_>,
    ) -> Result<Either<ConfirmedBlockCertificate, ValidatedBlockCertificate>, WorkerError> {
        let executed_block = self
            .executed_block_cache
            .get(&certificate.value.value_hash)
            .await
            .ok_or(WorkerError::MissingCertificateValue)?;

        match certificate.value.kind {
            linera_chain::types::CertificateKind::Confirmed => {
                let value = ConfirmedBlock::from_hashed(executed_block);
                Ok(Either::Left(
                    certificate
                        .with_value(Hashed::new(value))
                        .ok_or(WorkerError::InvalidLiteCertificate)?,
                ))
            }
            linera_chain::types::CertificateKind::Validated => {
                let value = ValidatedBlock::from_hashed(executed_block);
                Ok(Either::Right(
                    certificate
                        .with_value(Hashed::new(value))
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
    ) -> Result<(ExecutedBlock, ChainInfoResponse), WorkerError> {
        self.query_chain_worker(block.chain_id, move |callback| {
            ChainWorkerRequest::StageBlockExecution { block, callback }
        })
        .await
    }

    /// Executes a [`Query`] for an application's state on a specific chain.
    #[instrument(level = "trace", skip(self, chain_id, query))]
    pub async fn query_application(
        &self,
        chain_id: ChainId,
        query: Query,
    ) -> Result<Response, WorkerError> {
        self.query_chain_worker(chain_id, move |callback| {
            ChainWorkerRequest::QueryApplication { query, callback }
        })
        .await
    }

    #[instrument(level = "trace", skip(self, chain_id, application_id))]
    pub async fn describe_application(
        &self,
        chain_id: ChainId,
        application_id: UserApplicationId,
    ) -> Result<UserApplicationDescription, WorkerError> {
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

        let (response, actions) = self
            .query_chain_worker(chain_id, move |callback| {
                ChainWorkerRequest::ProcessConfirmedBlock {
                    certificate,
                    notify_when_messages_are_delivered,
                    callback,
                }
            })
            .await?;

        #[cfg(with_metrics)]
        NUM_BLOCKS.with_label_values(&[]).inc();

        Ok((response, actions))
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
        let chain_id = certificate.inner().chain_id;
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
        origin: Origin,
        recipient: ChainId,
        bundles: Vec<(Epoch, MessageBundle)>,
    ) -> Result<Option<(BlockHeight, NetworkActions)>, WorkerError> {
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
            .send(request_builder(callback))
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
                warn!("No chain worker candidates found for eviction, retrying...");
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

            let actor = ChainWorkerActor::load(
                self.chain_worker_config.clone(),
                self.storage.clone(),
                self.executed_block_cache.clone(),
                self.tracked_chains.clone(),
                delivery_notifier,
                chain_id,
            )
            .await?;

            self.chain_worker_tasks
                .lock()
                .unwrap()
                .spawn_task(actor.run(receiver).in_current_span());
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
        Option<mpsc::UnboundedReceiver<ChainWorkerRequest<StorageClient::Context>>>,
    )> {
        let mut chain_workers = self.chain_workers.lock().unwrap();

        if let Some(endpoint) = chain_workers.get(&chain_id) {
            Some((endpoint.clone(), None))
        } else {
            if chain_workers.len() >= usize::from(chain_workers.cap()) {
                let (chain_to_evict, _) = chain_workers
                    .iter()
                    .rev()
                    .find(|(_, candidate_endpoint)| candidate_endpoint.strong_count() <= 1)?;
                let chain_to_evict = *chain_to_evict;

                chain_workers.pop(&chain_to_evict);
                self.clean_up_finished_chain_workers(&chain_workers);
            }

            let (sender, receiver) = mpsc::unbounded_channel();
            chain_workers.push(chain_id, sender.clone());

            Some((sender, Some(receiver)))
        }
    }

    /// Cleans up any finished chain workers and their delivery notifiers.
    fn clean_up_finished_chain_workers(
        &self,
        active_chain_workers: &LruCache<ChainId, ChainActorEndpoint<StorageClient>>,
    ) {
        self.chain_worker_tasks
            .lock()
            .unwrap()
            .reap_finished_tasks();

        self.delivery_notifiers
            .lock()
            .unwrap()
            .retain(|chain_id, notifier| {
                !notifier.is_empty() || active_chain_workers.contains(chain_id)
            });
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
        NUM_ROUNDS_IN_BLOCK_PROPOSAL
            .with_label_values(&[round.type_name()])
            .observe(round.number() as f64);
        Ok(response)
    }

    /// Processes a certificate, e.g. to extend a chain with a confirmed block.
    // Other fields will be included in handle_certificate's span.
    #[instrument(skip_all, fields(hash = %certificate.value.value_hash))]
    pub async fn handle_lite_certificate<'a>(
        &self,
        certificate: LiteCertificate<'a>,
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        match self.full_certificate(certificate).await? {
            Either::Left(confirmed) => {
                self.handle_confirmed_certificate(confirmed, notify_when_messages_are_delivered)
                    .await
            }
            Either::Right(validated) => self.handle_validated_certificate(validated).await,
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
        {
            let confirmed_transactions = (certificate.block().body.incoming_bundles.len()
                + certificate.block().body.operations.len())
                as u64;

            NUM_ROUNDS_IN_CERTIFICATE
                .with_label_values(&[
                    certificate.inner().to_log_str(),
                    certificate.round.type_name(),
                ])
                .observe(certificate.round.number() as f64);
            if confirmed_transactions > 0 {
                TRANSACTION_COUNT
                    .with_label_values(&[])
                    .inc_by(confirmed_transactions);
            }

            for (validator_name, _) in certificate.signatures() {
                CERTIFICATES_SIGNED
                    .with_label_values(&[&validator_name.to_string()])
                    .inc();
            }
        }

        self.process_confirmed_block(certificate, notify_when_messages_are_delivered)
            .await
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
                NUM_ROUNDS_IN_CERTIFICATE
                    .with_label_values(&[cert_str, round.type_name()])
                    .observe(round.number() as f64);
            }
        }
        Ok((info, actions))
    }

    /// Processes a timeout certificate
    #[instrument(skip_all, fields(
        nick = self.nickname,
        chain_id = format!("{:.8}", certificate.inner().chain_id),
        height = %certificate.inner().height,
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
                bundle_vecs,
            } => {
                let mut height_by_origin = Vec::new();
                let mut actions = NetworkActions::default();
                for (medium, bundles) in bundle_vecs {
                    let origin = Origin { sender, medium };
                    if let Some((height, new_actions)) = self
                        .process_cross_chain_update(origin.clone(), recipient, bundles)
                        .await?
                    {
                        actions.extend(new_actions);
                        height_by_origin.push((origin, height));
                    }
                }
                if height_by_origin.is_empty() {
                    return Ok(NetworkActions::default());
                }
                let mut latest_heights = Vec::new();
                for (origin, height) in height_by_origin {
                    latest_heights.push((origin.medium.clone(), height));
                    actions.notifications.push(Notification {
                        chain_id: recipient,
                        reason: Reason::NewIncomingBundle { origin, height },
                    });
                }
                actions
                    .cross_chain_requests
                    .push(CrossChainRequest::ConfirmUpdatedRecipient {
                        sender,
                        recipient,
                        latest_heights,
                    });
                Ok(actions)
            }
            CrossChainRequest::ConfirmUpdatedRecipient {
                sender,
                recipient,
                latest_heights,
            } => {
                let latest_heights = latest_heights
                    .into_iter()
                    .map(|(medium, height)| (Target { recipient, medium }, height))
                    .collect();
                self.query_chain_worker(sender, move |callback| {
                    ChainWorkerRequest::ConfirmUpdatedRecipient {
                        latest_heights,
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
        new_trackers: BTreeMap<ValidatorName, u64>,
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
    /// Gets a reference to the validator's [`PublicKey`].
    ///
    /// # Panics
    ///
    /// If the validator doesn't have a key pair assigned to it.
    #[instrument(level = "trace", skip(self))]
    pub fn public_key(&self) -> PublicKey {
        self.chain_worker_config
            .key_pair()
            .expect(
                "Test validator should have a key pair assigned to it \
                in order to obtain it's public key",
            )
            .public()
    }
}

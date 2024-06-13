// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    borrow::Cow,
    collections::{hash_map, BTreeMap, HashMap, VecDeque},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use linera_base::{
    crypto::{CryptoHash, KeyPair},
    data_types::{ArithmeticError, BlockHeight, HashedBlob, Round},
    doc_scalar, ensure,
    identifiers::{BlobId, ChainId, Owner},
    sync::Lazy,
};
use linera_chain::{
    data_types::{
        Block, BlockProposal, Certificate, CertificateValue, ExecutedBlock, HashedCertificateValue,
        LiteCertificate, MessageBundle, Origin, OutgoingMessage, Target,
    },
    ChainStateView,
};
use linera_execution::{
    committee::Epoch, BytecodeLocation, Query, Response, UserApplicationDescription,
    UserApplicationId,
};
use linera_storage::Storage;
use linera_views::views::ViewError;
use lru::LruCache;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot, Mutex, OwnedRwLockReadGuard},
    task::JoinSet,
};
use tracing::{error, instrument, trace, warn};
#[cfg(with_metrics)]
use {
    linera_base::prometheus_util,
    prometheus::{HistogramVec, IntCounterVec},
};
#[cfg(with_testing)]
use {
    linera_base::{
        crypto::PublicKey,
        identifiers::{BytecodeId, Destination, MessageId},
    },
    linera_chain::data_types::{ChannelFullName, IncomingMessage, Medium, MessageAction},
};

use crate::{
    chain_worker::{ChainWorkerActor, ChainWorkerConfig, ChainWorkerRequest},
    data_types::{ChainInfoQuery, ChainInfoResponse, CrossChainRequest},
    value_cache::ValueCache,
};

#[cfg(test)]
#[path = "unit_tests/worker_tests.rs"]
mod worker_tests;

/// The maximum number of [`ChainWorkerActor`]s to keep running.
static CHAIN_WORKER_LIMIT: Lazy<NonZeroUsize> =
    Lazy::new(|| NonZeroUsize::new(1_000).expect("`CHAIN_WORKER_LIMIT` should not be zero"));

#[cfg(with_metrics)]
static NUM_ROUNDS_IN_CERTIFICATE: Lazy<HistogramVec> = Lazy::new(|| {
    prometheus_util::register_histogram_vec(
        "num_rounds_in_certificate",
        "Number of rounds in certificate",
        &["certificate_value", "round_type"],
        Some(vec![
            0.5, 1.0, 2.0, 3.0, 4.0, 6.0, 8.0, 10.0, 15.0, 25.0, 50.0,
        ]),
    )
    .expect("Counter creation should not fail")
});

#[cfg(with_metrics)]
static NUM_ROUNDS_IN_BLOCK_PROPOSAL: Lazy<HistogramVec> = Lazy::new(|| {
    prometheus_util::register_histogram_vec(
        "num_rounds_in_block_proposal",
        "Number of rounds in block proposal",
        &["round_type"],
        Some(vec![
            0.5, 1.0, 2.0, 3.0, 4.0, 6.0, 8.0, 10.0, 15.0, 25.0, 50.0,
        ]),
    )
    .expect("Counter creation should not fail")
});

#[cfg(with_metrics)]
static TRANSACTION_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    prometheus_util::register_int_counter_vec("transaction_count", "Transaction count", &[])
        .expect("Counter creation should not fail")
});

#[cfg(with_metrics)]
static NUM_BLOCKS: Lazy<IntCounterVec> = Lazy::new(|| {
    prometheus_util::register_int_counter_vec("num_blocks", "Number of blocks added to chains", &[])
        .expect("Counter creation should not fail")
});

/// Interface provided by each physical shard (aka "worker") of a validator or a local node.
/// * All commands return either the current chain info or an error.
/// * Repeating commands produces no changes and returns no error.
/// * Some handlers may return cross-chain requests, that is, messages
///   to be communicated to other workers of the same validator.
#[cfg_attr(not(web), async_trait)]
#[cfg_attr(web, async_trait(?Send))]
pub trait ValidatorWorker {
    /// Proposes a new block. In case of success, the chain info contains a vote on the new
    /// block.
    async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError>;

    /// Processes a certificate, e.g. to extend a chain with a confirmed block.
    async fn handle_lite_certificate<'a>(
        &mut self,
        certificate: LiteCertificate<'a>,
        notify_message_delivery: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError>;

    /// Processes a certificate, e.g. to extend a chain with a confirmed block.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
        hashed_certificate_values: Vec<HashedCertificateValue>,
        hashed_blobs: Vec<HashedBlob>,
        notify_message_delivery: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError>;

    /// Handles information queries on chains.
    async fn handle_chain_info_query(
        &self,
        query: ChainInfoQuery,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError>;

    /// Handles a (trusted!) cross-chain request.
    async fn handle_cross_chain_request(
        &mut self,
        request: CrossChainRequest,
    ) -> Result<NetworkActions, WorkerError>;
}

/// Instruct the networking layer to send cross-chain requests and/or push notifications.
#[derive(Default, Debug)]
pub struct NetworkActions {
    /// The cross-chain requests
    pub cross_chain_requests: Vec<CrossChainRequest>,
    /// The push notifications.
    pub notifications: Vec<Notification>,
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
#[allow(clippy::large_enum_variant)]
/// Reason for the notification.
pub enum Reason {
    NewBlock {
        height: BlockHeight,
        hash: CryptoHash,
    },
    NewIncomingMessage {
        origin: Origin,
        height: BlockHeight,
    },
    NewRound {
        height: BlockHeight,
        round: Round,
    },
}

/// Error type for [`ValidatorWorker`].
#[derive(Debug, Error)]
pub enum WorkerError {
    #[error(transparent)]
    CryptoError(#[from] linera_base::crypto::CryptoError),

    #[error(transparent)]
    ArithmeticError(#[from] ArithmeticError),

    #[error(transparent)]
    ViewError(#[from] linera_views::views::ViewError),

    #[error(transparent)]
    ChainError(#[from] Box<linera_chain::ChainError>),

    // Chain access control
    #[error("Block was not signed by an authorized owner")]
    InvalidOwner,

    #[error("Operations in the block are not authenticated by the proper signer")]
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
    #[error("The given state hash is not what we computed after executing the block")]
    IncorrectStateHash,
    #[error(
        "
        The given messages are not what we computed after executing the block.\n\
        Computed: {computed:#?}\n\
        Submitted: {submitted:#?}\n
    "
    )]
    IncorrectMessages {
        computed: Vec<Vec<OutgoingMessage>>,
        submitted: Vec<Vec<OutgoingMessage>>,
    },
    #[error("The timestamp of a Tick operation is in the future.")]
    InvalidTimestamp,
    #[error("We don't have the value for the certificate.")]
    MissingCertificateValue,
    #[error("The hash certificate doesn't match its value.")]
    InvalidLiteCertificate,
    #[error("An additional value was provided that is not required: {value_hash}.")]
    UnneededValue { value_hash: CryptoHash },
    #[error("An additional blob was provided that is not required: {blob_id}.")]
    UnneededBlob { blob_id: BlobId },
    #[error("The certificate in the block proposal is not a ValidatedBlock")]
    MissingExecutedBlockInProposal,
    #[error("Fast blocks cannot query oracles")]
    FastBlockUsingOracles,
    #[error("The following values containing application bytecode are missing: {0:?} and the following blobs are missing: {1:?}.")]
    ApplicationBytecodesOrBlobsNotFound(Vec<BytecodeLocation>, Vec<BlobId>),
    #[error("The block proposal is invalid: {0}")]
    InvalidBlockProposal(String),
}

impl From<linera_chain::ChainError> for WorkerError {
    fn from(chain_error: linera_chain::ChainError) -> Self {
        WorkerError::ChainError(Box::new(chain_error))
    }
}

/// State of a worker in a validator or a local node.
#[derive(Clone)]
pub struct WorkerState<StorageClient>
where
    StorageClient: Storage,
    ViewError: From<StorageClient::ContextError>,
{
    /// A name used for logging
    nickname: String,
    /// Access to local persistent storage.
    storage: StorageClient,
    /// Configuration options for the [`ChainWorker`]s.
    chain_worker_config: ChainWorkerConfig,
    /// Cached hashed certificate values by hash.
    recent_hashed_certificate_values: Arc<ValueCache<CryptoHash, HashedCertificateValue>>,
    /// Cached hashed blobs by `BlobId`.
    recent_hashed_blobs: Arc<ValueCache<BlobId, HashedBlob>>,
    /// One-shot channels to notify callers when messages of a particular chain have been
    /// delivered.
    delivery_notifiers: Arc<Mutex<DeliveryNotifiers>>,
    /// The set of spawned [`ChainWorkerActor`] tasks.
    chain_worker_tasks: Arc<Mutex<JoinSet<()>>>,
    /// The cache of running [`ChainWorkerActor`]s.
    chain_workers: Arc<Mutex<LruCache<ChainId, ChainActorEndpoint<StorageClient>>>>,
}

/// The sender endpoint for [`ChainWorkerRequest`]s.
type ChainActorEndpoint<StorageClient> =
    mpsc::UnboundedSender<ChainWorkerRequest<<StorageClient as Storage>::Context>>;

pub(crate) type DeliveryNotifiers =
    HashMap<ChainId, BTreeMap<BlockHeight, Vec<oneshot::Sender<()>>>>;

impl<StorageClient> WorkerState<StorageClient>
where
    StorageClient: Storage,
    ViewError: From<StorageClient::ContextError>,
{
    pub fn new(nickname: String, key_pair: Option<KeyPair>, storage: StorageClient) -> Self {
        WorkerState {
            nickname,
            storage,
            chain_worker_config: ChainWorkerConfig::default().with_key_pair(key_pair),
            recent_hashed_certificate_values: Arc::new(ValueCache::default()),
            recent_hashed_blobs: Arc::new(ValueCache::default()),
            delivery_notifiers: Arc::default(),
            chain_worker_tasks: Arc::default(),
            chain_workers: Arc::new(Mutex::new(LruCache::new(*CHAIN_WORKER_LIMIT))),
        }
    }

    pub fn new_for_client(
        nickname: String,
        storage: StorageClient,
        recent_hashed_certificate_values: Arc<ValueCache<CryptoHash, HashedCertificateValue>>,
        recent_hashed_blobs: Arc<ValueCache<BlobId, HashedBlob>>,
        delivery_notifiers: Arc<Mutex<DeliveryNotifiers>>,
    ) -> Self {
        WorkerState {
            nickname,
            storage,
            chain_worker_config: ChainWorkerConfig::default(),
            recent_hashed_certificate_values,
            recent_hashed_blobs,
            delivery_notifiers,
            chain_worker_tasks: Arc::default(),
            chain_workers: Arc::new(Mutex::new(LruCache::new(*CHAIN_WORKER_LIMIT))),
        }
    }

    pub fn with_allow_inactive_chains(mut self, value: bool) -> Self {
        self.chain_worker_config.allow_inactive_chains = value;
        self
    }

    pub fn with_allow_messages_from_deprecated_epochs(mut self, value: bool) -> Self {
        self.chain_worker_config
            .allow_messages_from_deprecated_epochs = value;
        self
    }

    /// Returns an instance with the specified grace period, in microseconds.
    ///
    /// Blocks with a timestamp this far in the future will still be accepted, but the validator
    /// will wait until that timestamp before voting.
    pub fn with_grace_period(mut self, grace_period: Duration) -> Self {
        self.chain_worker_config.grace_period = grace_period;
        self
    }

    pub fn nickname(&self) -> &str {
        &self.nickname
    }

    pub fn recent_hashed_blobs(&self) -> Arc<ValueCache<BlobId, HashedBlob>> {
        self.recent_hashed_blobs.clone()
    }

    /// Returns the storage client so that it can be manipulated or queried.
    #[cfg(not(feature = "test"))]
    pub(crate) fn storage_client(&self) -> &StorageClient {
        &self.storage
    }

    /// Returns the storage client so that it can be manipulated or queried by tests in other
    /// crates.
    #[cfg(feature = "test")]
    pub fn storage_client(&self) -> &StorageClient {
        &self.storage
    }

    #[cfg(test)]
    pub(crate) fn with_key_pair(mut self, key_pair: Option<Arc<KeyPair>>) -> Self {
        self.chain_worker_config.key_pair = key_pair;
        self
    }

    pub(crate) async fn full_certificate(
        &mut self,
        certificate: LiteCertificate<'_>,
    ) -> Result<Certificate, WorkerError> {
        self.recent_hashed_certificate_values
            .full_certificate(certificate)
            .await
    }

    pub(crate) async fn recent_hashed_certificate_value(
        &mut self,
        hash: &CryptoHash,
    ) -> Option<HashedCertificateValue> {
        self.recent_hashed_certificate_values.get(hash).await
    }

    pub(crate) async fn recent_blob(&mut self, blob_id: &BlobId) -> Option<HashedBlob> {
        self.recent_hashed_blobs.get(blob_id).await
    }
}

impl<StorageClient> WorkerState<StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
    ViewError: From<StorageClient::ContextError>,
{
    // NOTE: This only works for non-sharded workers!
    #[cfg(with_testing)]
    pub async fn fully_handle_certificate(
        &mut self,
        certificate: Certificate,
        hashed_certificate_values: Vec<HashedCertificateValue>,
        hashed_blobs: Vec<HashedBlob>,
    ) -> Result<ChainInfoResponse, WorkerError> {
        self.fully_handle_certificate_with_notifications(
            certificate,
            hashed_certificate_values,
            hashed_blobs,
            None::<&mut Vec<Notification>>,
        )
        .await
    }

    #[inline]
    pub(crate) async fn fully_handle_certificate_with_notifications(
        &mut self,
        certificate: Certificate,
        hashed_certificate_values: Vec<HashedCertificateValue>,
        hashed_blobs: Vec<HashedBlob>,
        mut notifications: Option<&mut impl Extend<Notification>>,
    ) -> Result<ChainInfoResponse, WorkerError> {
        let (response, actions) = self
            .handle_certificate(certificate, hashed_certificate_values, hashed_blobs, None)
            .await?;
        if let Some(ref mut notifications) = notifications {
            notifications.extend(actions.notifications);
        }
        let mut requests = VecDeque::from(actions.cross_chain_requests);
        while let Some(request) = requests.pop_front() {
            let actions = self.handle_cross_chain_request(request).await?;
            requests.extend(actions.cross_chain_requests);
            if let Some(ref mut notifications) = notifications {
                notifications.extend(actions.notifications);
            }
        }
        Ok(response)
    }

    /// Tries to execute a block proposal without any verification other than block execution.
    pub async fn stage_block_execution(
        &mut self,
        block: Block,
    ) -> Result<(ExecutedBlock, ChainInfoResponse), WorkerError> {
        self.query_chain_worker(block.chain_id, move |callback| {
            ChainWorkerRequest::StageBlockExecution { block, callback }
        })
        .await
    }

    // Schedule a notification when cross-chain messages are delivered up to the given height.
    async fn register_delivery_notifier(
        &mut self,
        chain_id: ChainId,
        height: BlockHeight,
        actions: &NetworkActions,
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) {
        if let Some(notifier) = notify_when_messages_are_delivered {
            if actions
                .cross_chain_requests
                .iter()
                .any(|request| request.has_messages_lower_or_equal_than(height))
            {
                self.delivery_notifiers
                    .lock()
                    .await
                    .entry(chain_id)
                    .or_default()
                    .entry(height)
                    .or_default()
                    .push(notifier);
            } else {
                // No need to wait. Also, cross-chain requests may not trigger the
                // notifier later, even if we register it.
                if let Err(()) = notifier.send(()) {
                    warn!("Failed to notify message delivery to caller");
                }
            }
        }
    }

    /// Executes a [`Query`] for an application's state on a specific chain.
    pub async fn query_application(
        &mut self,
        chain_id: ChainId,
        query: Query,
    ) -> Result<Response, WorkerError> {
        self.query_chain_worker(chain_id, move |callback| {
            ChainWorkerRequest::QueryApplication { query, callback }
        })
        .await
    }

    #[cfg(with_testing)]
    pub async fn read_bytecode_location(
        &mut self,
        chain_id: ChainId,
        bytecode_id: BytecodeId,
    ) -> Result<Option<BytecodeLocation>, WorkerError> {
        self.query_chain_worker(chain_id, move |callback| {
            ChainWorkerRequest::ReadBytecodeLocation {
                bytecode_id,
                callback,
            }
        })
        .await
    }

    pub async fn describe_application(
        &mut self,
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
    async fn process_confirmed_block(
        &mut self,
        certificate: Certificate,
        hashed_certificate_values: &[HashedCertificateValue],
        hashed_blobs: &[HashedBlob],
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        let CertificateValue::ConfirmedBlock { executed_block, .. } = certificate.value() else {
            panic!("Expecting a confirmation certificate");
        };
        let chain_id = executed_block.block.chain_id;
        let height = executed_block.block.height;

        let (response, actions) = self
            .query_chain_worker(chain_id, move |callback| {
                ChainWorkerRequest::ProcessConfirmedBlock {
                    certificate,
                    hashed_certificate_values: hashed_certificate_values.to_owned(),
                    hashed_blobs: hashed_blobs.to_owned(),
                    callback,
                }
            })
            .await?;

        self.register_delivery_notifier(
            chain_id,
            height,
            &actions,
            notify_when_messages_are_delivered,
        )
        .await;

        #[cfg(with_metrics)]
        NUM_BLOCKS.with_label_values(&[]).inc();

        Ok((response, actions))
    }

    /// Processes a validated block issued from a multi-owner chain.
    async fn process_validated_block(
        &mut self,
        certificate: Certificate,
    ) -> Result<(ChainInfoResponse, NetworkActions, bool), WorkerError> {
        let CertificateValue::ValidatedBlock {
            executed_block: ExecutedBlock { block, .. },
        } = certificate.value()
        else {
            panic!("Expecting a validation certificate");
        };
        self.query_chain_worker(block.chain_id, move |callback| {
            ChainWorkerRequest::ProcessValidatedBlock {
                certificate,
                callback,
            }
        })
        .await
    }

    /// Processes a leader timeout issued from a multi-owner chain.
    async fn process_timeout(
        &mut self,
        certificate: Certificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        let CertificateValue::Timeout { chain_id, .. } = certificate.value() else {
            panic!("Expecting a leader timeout certificate");
        };
        self.query_chain_worker(*chain_id, move |callback| {
            ChainWorkerRequest::ProcessTimeout {
                certificate,
                callback,
            }
        })
        .await
    }

    async fn process_cross_chain_update(
        &mut self,
        origin: Origin,
        recipient: ChainId,
        bundles: Vec<MessageBundle>,
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

    /// Inserts a [`HashedCertificateValue`] into the worker's cache.
    pub(crate) async fn cache_recent_hashed_certificate_value<'a>(
        &mut self,
        value: Cow<'a, HashedCertificateValue>,
    ) -> bool {
        self.recent_hashed_certificate_values.insert(value).await
    }

    /// Inserts a [`HashedBlob`] into the worker's cache.
    pub async fn cache_recent_blob<'a>(&mut self, hashed_blob: Cow<'a, HashedBlob>) -> bool {
        self.recent_hashed_blobs.insert(hashed_blob).await
    }

    /// Returns a stored [`Certificate`] for a chain's block.
    #[cfg(with_testing)]
    pub async fn read_certificate(
        &self,
        chain_id: ChainId,
        height: BlockHeight,
    ) -> Result<Option<Certificate>, WorkerError> {
        self.query_chain_worker(chain_id, move |callback| {
            ChainWorkerRequest::ReadCertificate { height, callback }
        })
        .await
    }

    /// Returns an [`IncomingMessage`] that's awaiting to be received by the chain specified by
    /// `chain_id`.
    #[cfg(with_testing)]
    pub async fn find_incoming_message(
        &self,
        chain_id: ChainId,
        message_id: MessageId,
    ) -> Result<Option<IncomingMessage>, WorkerError> {
        let sender = message_id.chain_id;
        let Some(certificate) = self.read_certificate(sender, message_id.height).await? else {
            return Ok(None);
        };
        let Some(executed_block) = certificate.value().executed_block() else {
            return Ok(None);
        };
        let Some(outgoing_message) = executed_block.message_by_id(&message_id).cloned() else {
            return Ok(None);
        };

        let medium = match outgoing_message.destination {
            Destination::Recipient(_) => Medium::Direct,
            Destination::Subscribers(name) => {
                let application_id = outgoing_message.message.application_id();
                Medium::Channel(ChannelFullName {
                    application_id,
                    name,
                })
            }
        };
        let origin = Origin { sender, medium };

        let Some(event) = self
            .query_chain_worker(chain_id, {
                let origin = origin.clone();
                move |callback| ChainWorkerRequest::FindEventInInbox {
                    inbox_id: origin,
                    certificate_hash: certificate.hash(),
                    height: message_id.height,
                    index: message_id.index,
                    callback,
                }
            })
            .await?
        else {
            return Ok(None);
        };

        assert_eq!(event.message, outgoing_message.message);

        Ok(Some(IncomingMessage {
            origin,
            event,
            action: MessageAction::Accept,
        }))
    }

    /// Returns a read-only view of the [`ChainStateView`] of a chain referenced by its
    /// [`ChainId`].
    ///
    /// The returned view holds a lock on the chain state, which prevents the worker from changing
    /// the state of that chain.
    pub async fn chain_state_view(
        &self,
        chain_id: ChainId,
    ) -> Result<OwnedRwLockReadGuard<ChainStateView<StorageClient::Context>>, WorkerError> {
        self.query_chain_worker(chain_id, |callback| ChainWorkerRequest::GetChainStateView {
            callback,
        })
        .await
    }

    /// Sends a request to the [`ChainWorker`] for a [`ChainId`] and waits for the `Response`.
    async fn query_chain_worker<Response>(
        &self,
        chain_id: ChainId,
        request_builder: impl FnOnce(
            oneshot::Sender<Result<Response, WorkerError>>,
        ) -> ChainWorkerRequest<StorageClient::Context>,
    ) -> Result<Response, WorkerError> {
        let chain_actor = ChainWorkerActor::spawn(
            self.chain_worker_config.clone(),
            self.storage.clone(),
            self.recent_hashed_certificate_values.clone(),
            self.recent_hashed_blobs.clone(),
            chain_id,
            &mut *self.chain_worker_tasks.lock().await,
        )
        .await?;
        let (callback, response) = oneshot::channel();

        chain_actor
            .send(request_builder(callback))
            .expect("`ChainWorkerActor` stopped executing unexpectedly");

        response
            .await
            .expect("`ChainWorkerActor` stopped executing without responding")
    }
}

#[cfg(with_testing)]
impl<StorageClient> WorkerState<StorageClient>
where
    StorageClient: Storage,
    ViewError: From<StorageClient::ContextError>,
{
    /// Gets a reference to the validator's [`PublicKey`].
    ///
    /// # Panics
    ///
    /// If the validator doesn't have a key pair assigned to it.
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

#[cfg_attr(not(web), async_trait)]
#[cfg_attr(web, async_trait(?Send))]
impl<StorageClient> ValidatorWorker for WorkerState<StorageClient>
where
    StorageClient: Storage + Clone + Send + Sync + 'static,
    ViewError: From<StorageClient::ContextError>,
{
    #[instrument(skip_all, fields(
        nick = self.nickname,
        chain_id = format!("{:.8}", proposal.content.block.chain_id),
        height = %proposal.content.block.height,
    ))]
    async fn handle_block_proposal(
        &mut self,
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

    // Other fields will be included in handle_certificate's span.
    #[instrument(skip_all, fields(hash = %certificate.value.value_hash))]
    /// Processes a certificate, e.g. to extend a chain with a confirmed block.
    async fn handle_lite_certificate<'a>(
        &mut self,
        certificate: LiteCertificate<'a>,
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        let full_cert = self.full_certificate(certificate).await?;
        self.handle_certificate(
            full_cert,
            vec![],
            vec![],
            notify_when_messages_are_delivered,
        )
        .await
    }

    /// Processes a certificate.
    #[instrument(skip_all, fields(
        nick = self.nickname,
        chain_id = format!("{:.8}", certificate.value().chain_id()),
        height = %certificate.value().height(),
    ))]
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
        hashed_certificate_values: Vec<HashedCertificateValue>,
        hashed_blobs: Vec<HashedBlob>,
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        trace!("{} <-- {:?}", self.nickname, certificate);
        ensure!(
            certificate.value().is_confirmed() || hashed_certificate_values.is_empty(),
            WorkerError::UnneededValue {
                value_hash: hashed_certificate_values[0].hash(),
            }
        );

        #[cfg(with_metrics)]
        let (round, log_str, mut confirmed_transactions, mut duplicated) = (
            certificate.round,
            certificate.value().to_log_str(),
            0u64,
            false,
        );

        let (info, actions) = match certificate.value() {
            CertificateValue::ValidatedBlock { .. } => {
                // Confirm the validated block.
                let validation_outcomes = self.process_validated_block(certificate).await?;
                #[cfg(with_metrics)]
                {
                    duplicated = validation_outcomes.2;
                }
                let (info, actions, _) = validation_outcomes;
                (info, actions)
            }
            CertificateValue::ConfirmedBlock {
                executed_block: _executed_block,
            } => {
                #[cfg(with_metrics)]
                {
                    confirmed_transactions = (_executed_block.block.incoming_messages.len()
                        + _executed_block.block.operations.len())
                        as u64;
                }
                // Execute the confirmed block.
                self.process_confirmed_block(
                    certificate,
                    &hashed_certificate_values,
                    &hashed_blobs,
                    notify_when_messages_are_delivered,
                )
                .await?
            }
            CertificateValue::Timeout { .. } => {
                // Handle the leader timeout.
                self.process_timeout(certificate).await?
            }
        };

        #[cfg(with_metrics)]
        if !duplicated {
            NUM_ROUNDS_IN_CERTIFICATE
                .with_label_values(&[log_str, round.type_name()])
                .observe(round.number() as f64);
            if confirmed_transactions > 0 {
                TRANSACTION_COUNT
                    .with_label_values(&[])
                    .inc_by(confirmed_transactions);
            }
        }
        Ok((info, actions))
    }

    #[instrument(skip_all, fields(
        nick = self.nickname,
        chain_id = format!("{:.8}", query.chain_id)
    ))]
    async fn handle_chain_info_query(
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
        chain_id = format!("{:.8}", request.target_chain_id())
    ))]
    async fn handle_cross_chain_request(
        &mut self,
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
                for (medium, bundles) in bundle_vecs {
                    let origin = Origin { sender, medium };
                    if let Some(height) = self
                        .process_cross_chain_update(origin.clone(), recipient, bundles)
                        .await?
                    {
                        height_by_origin.push((origin, height));
                    }
                }
                if height_by_origin.is_empty() {
                    return Ok(NetworkActions::default());
                }
                let mut notifications = Vec::new();
                let mut latest_heights = Vec::new();
                for (origin, height) in height_by_origin {
                    latest_heights.push((origin.medium.clone(), height));
                    notifications.push(Notification {
                        chain_id: recipient,
                        reason: Reason::NewIncomingMessage { origin, height },
                    });
                }
                let cross_chain_requests = vec![CrossChainRequest::ConfirmUpdatedRecipient {
                    sender,
                    recipient,
                    latest_heights,
                }];
                Ok(NetworkActions {
                    cross_chain_requests,
                    notifications,
                })
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
                let height_with_fully_delivered_messages = self
                    .query_chain_worker(sender, move |callback| {
                        ChainWorkerRequest::ConfirmUpdatedRecipient {
                            latest_heights,
                            callback,
                        }
                    })
                    .await?;
                // Handle delivery notifiers for this chain, if any.
                if let hash_map::Entry::Occupied(mut map) =
                    self.delivery_notifiers.lock().await.entry(sender)
                {
                    while let Some(entry) = map.get_mut().first_entry() {
                        if entry.key() > &height_with_fully_delivered_messages {
                            break;
                        }
                        let notifiers = entry.remove();
                        trace!("Notifying {} callers", notifiers.len());
                        for notifier in notifiers {
                            if let Err(()) = notifier.send(()) {
                                warn!("Failed to notify message delivery to caller");
                            }
                        }
                    }
                    if map.get().is_empty() {
                        map.remove();
                    }
                }
                Ok(NetworkActions::default())
            }
        }
    }
}

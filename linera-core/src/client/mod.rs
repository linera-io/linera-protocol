// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{hash_map, BTreeMap, HashMap, HashSet},
    convert::Infallible,
    iter,
    num::NonZeroUsize,
    ops::{Deref, DerefMut},
    sync::{Arc, RwLock},
};

use chain_state::ChainState;
use dashmap::{
    mapref::one::{MappedRef as DashMapMappedRef, Ref as DashMapRef, RefMut as DashMapRefMut},
    DashMap,
};
use futures::{
    future::{self, try_join_all, FusedFuture, Future},
    stream::{self, AbortHandle, FusedStream, FuturesUnordered, StreamExt},
};
#[cfg(not(target_arch = "wasm32"))]
use linera_base::data_types::Bytecode;
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use linera_base::{
    abi::Abi,
    crypto::{CryptoHash, KeyPair, PublicKey},
    data_types::{
        Amount, ApplicationPermissions, ArithmeticError, Blob, BlockHeight, Round, Timestamp,
    },
    ensure,
    identifiers::{
        Account, ApplicationId, BlobId, BlobType, BytecodeId, ChainId, MessageId, Owner,
        UserApplicationId,
    },
    ownership::{ChainOwnership, TimeoutConfig},
};
use linera_chain::{
    data_types::{
        Block, BlockProposal, Certificate, CertificateValue, ExecutedBlock, HashedCertificateValue,
        IncomingBundle, LiteCertificate, LiteVote, MessageAction, PostedMessage,
    },
    manager::ChainManagerInfo,
    ChainError, ChainExecutionContext, ChainStateView,
};
use linera_execution::{
    committee::{Committee, Epoch, ValidatorName},
    system::{
        AdminOperation, OpenChainConfig, Recipient, SystemChannel, SystemOperation,
        CREATE_APPLICATION_MESSAGE_INDEX, OPEN_CHAIN_MESSAGE_INDEX,
    },
    ExecutionError, Message, Operation, Query, Response, SystemExecutionError, SystemMessage,
    SystemQuery, SystemResponse,
};
use linera_storage::{Clock as _, Storage};
use linera_views::views::ViewError;
use serde::Serialize;
use thiserror::Error;
use tokio::sync::OwnedRwLockReadGuard;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, error, info, warn, Instrument as _};

use crate::{
    data_types::{
        BlockHeightRange, ChainInfo, ChainInfoQuery, ChainInfoResponse, ClientOutcome, RoundTimeout,
    },
    local_node::{LocalNodeClient, LocalNodeError},
    node::{
        CrossChainMessageDelivery, NodeError, NotificationStream, ValidatorNode,
        ValidatorNodeProvider,
    },
    notifier::Notifier,
    remote_node::RemoteNode,
    updater::{communicate_with_quorum, CommunicateAction, CommunicationError, ValidatorUpdater},
    worker::{Notification, Reason, WorkerError, WorkerState},
};

mod chain_state;
#[cfg(test)]
#[path = "../unit_tests/client_tests.rs"]
mod client_tests;

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util;
    use prometheus::HistogramVec;

    pub static PROCESS_INBOX_WITHOUT_PREPARE_LATENCY: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            prometheus_util::register_histogram_vec(
                "process_inbox_latency",
                "process_inbox latency",
                &[],
                Some(vec![
                    0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                    25.0, 50.0, 100.0, 250.0, 500.0,
                ]),
            )
            .expect("Histogram creation should not fail")
        });

    pub static PREPARE_CHAIN_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        prometheus_util::register_histogram_vec(
            "prepare_chain_latency",
            "prepare_chain latency",
            &[],
            Some(vec![
                0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                25.0, 50.0, 100.0, 250.0, 500.0,
            ]),
        )
        .expect("Histogram creation should not fail")
    });

    pub static SYNCHRONIZE_CHAIN_STATE_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        prometheus_util::register_histogram_vec(
            "synchronize_chain_state_latency",
            "synchronize_chain_state latency",
            &[],
            Some(vec![
                0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                25.0, 50.0, 100.0, 250.0, 500.0,
            ]),
        )
        .expect("Histogram creation should not fail")
    });

    pub static EXECUTE_BLOCK_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        prometheus_util::register_histogram_vec(
            "execute_block_latency",
            "execute_block latency",
            &[],
            Some(vec![
                0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                25.0, 50.0, 100.0, 250.0, 500.0,
            ]),
        )
        .expect("Histogram creation should not fail")
    });

    pub static FIND_RECEIVED_CERTIFICATES_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        prometheus_util::register_histogram_vec(
            "find_received_certificates_latency",
            "find_received_certificates latency",
            &[],
            Some(vec![
                0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                25.0, 50.0, 100.0, 250.0, 500.0,
            ]),
        )
        .expect("Histogram creation should not fail")
    });
}

/// A builder that creates [`ChainClient`]s which share the cache and notifiers.
pub struct Client<ValidatorNodeProvider, Storage>
where
    Storage: linera_storage::Storage,
{
    /// How to talk to the validators.
    validator_node_provider: ValidatorNodeProvider,
    /// Local node to manage the execution state and the local storage of the chains that we are
    /// tracking.
    local_node: LocalNodeClient<Storage>,
    /// Maximum number of pending message bundles processed at a time in a block.
    max_pending_message_bundles: usize,
    /// The policy for automatically handling incoming messages.
    message_policy: MessagePolicy,
    /// Whether to block on cross-chain message delivery.
    cross_chain_message_delivery: CrossChainMessageDelivery,
    /// Chains that should be tracked by the client.
    // TODO(#2412): Merge with set of chains the client is receiving notifications from validators
    tracked_chains: Arc<RwLock<HashSet<ChainId>>>,
    /// References to clients waiting for chain notifications.
    notifier: Arc<Notifier<Notification>>,
    /// A copy of the storage client so that we don't have to lock the local node client
    /// to retrieve it.
    storage: Storage,
    /// Chain state for the managed chains.
    chains: DashMap<ChainId, ChainState>,
}

impl<P, S: Storage + Clone> Client<P, S> {
    /// Creates a new `Client` with a new cache and notifiers.
    #[tracing::instrument(level = "trace", skip_all)]
    pub fn new(
        validator_node_provider: P,
        storage: S,
        max_pending_message_bundles: usize,
        cross_chain_message_delivery: CrossChainMessageDelivery,
        long_lived_services: bool,
        tracked_chains: impl IntoIterator<Item = ChainId>,
        name: impl Into<String>,
    ) -> Self {
        let tracked_chains = Arc::new(RwLock::new(tracked_chains.into_iter().collect()));
        let state = WorkerState::new_for_client(
            name.into(),
            storage.clone(),
            tracked_chains.clone(),
            NonZeroUsize::new(20).expect("Chain worker limit should not be zero"),
        )
        .with_long_lived_services(long_lived_services)
        .with_allow_inactive_chains(true)
        .with_allow_messages_from_deprecated_epochs(true);
        let local_node = LocalNodeClient::new(state);

        Self {
            validator_node_provider,
            local_node,
            chains: DashMap::new(),
            max_pending_message_bundles,
            message_policy: MessagePolicy::new(BlanketMessagePolicy::Accept, None),
            cross_chain_message_delivery,
            tracked_chains,
            notifier: Arc::new(Notifier::default()),
            storage,
        }
    }

    /// Returns the storage client used by this client's local node.
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn storage_client(&self) -> &S {
        &self.storage
    }

    /// Returns a reference to the [`LocalNodeClient`] of the client.
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn local_node(&self) -> &LocalNodeClient<S> {
        &self.local_node
    }

    /// Adds a chain to the set of chains tracked by the local node.
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn track_chain(&self, chain_id: ChainId) {
        self.tracked_chains
            .write()
            .expect("Panics should not happen while holding a lock to `tracked_chains`")
            .insert(chain_id);
    }

    /// Creates a new `ChainClient`.
    #[tracing::instrument(level = "trace", skip_all, fields(chain_id, next_block_height))]
    #[expect(clippy::too_many_arguments)]
    pub fn create_chain_client(
        self: &Arc<Self>,
        chain_id: ChainId,
        known_key_pairs: Vec<KeyPair>,
        admin_id: ChainId,
        block_hash: Option<CryptoHash>,
        timestamp: Timestamp,
        next_block_height: BlockHeight,
        pending_block: Option<Block>,
        pending_blobs: BTreeMap<BlobId, Blob>,
    ) -> ChainClient<P, S> {
        // If the entry already exists we assume that the entry is more up to date than
        // the arguments: If they were read from the wallet file, they might be stale.
        if let dashmap::mapref::entry::Entry::Vacant(e) = self.chains.entry(chain_id) {
            e.insert(ChainState::new(
                known_key_pairs,
                admin_id,
                block_hash,
                timestamp,
                next_block_height,
                pending_block,
                pending_blobs,
            ));
        }

        ChainClient {
            client: self.clone(),
            chain_id,
            options: ChainClientOptions {
                max_pending_message_bundles: self.max_pending_message_bundles,
                message_policy: self.message_policy.clone(),
                cross_chain_message_delivery: self.cross_chain_message_delivery,
            },
        }
    }
}

impl<P, S> Client<P, S>
where
    P: ValidatorNodeProvider + Sync + 'static,
    S: Storage + Sync + Send + Clone + 'static,
{
    async fn download_certificates(
        &self,
        nodes: &[RemoteNode<P::Node>],
        chain_id: ChainId,
        height: BlockHeight,
    ) -> Result<Box<ChainInfo>, LocalNodeError> {
        let mut notifications = Vec::<Notification>::new();
        let info = self
            .local_node
            .download_certificates(nodes, chain_id, height, &mut notifications)
            .await?;
        self.notifier.handle_notifications(&notifications);
        Ok(info)
    }

    async fn try_process_certificates(
        &self,
        remote_node: &RemoteNode<P::Node>,
        chain_id: ChainId,
        certificates: Vec<Certificate>,
    ) -> Option<Box<ChainInfo>> {
        let mut notifications = Vec::<Notification>::new();
        let result = self
            .local_node
            .try_process_certificates(remote_node, chain_id, certificates, &mut notifications)
            .await;
        self.notifier.handle_notifications(&notifications);
        result
    }

    async fn handle_certificate(
        &self,
        certificate: Certificate,
        blobs: Vec<Blob>,
    ) -> Result<ChainInfoResponse, LocalNodeError> {
        let mut notifications = Vec::<Notification>::new();
        let result = self
            .local_node
            .handle_certificate(certificate, blobs, &mut notifications)
            .await;
        self.notifier.handle_notifications(&notifications);
        result
    }
}

/// Policies for automatically handling incoming messages.
#[derive(Clone, Debug)]
pub struct MessagePolicy {
    /// The blanket policy applied to all messages.
    blanket: BlanketMessagePolicy,
    /// A collection of chains which restrict the origin of messages to be
    /// accepted. `Option::None` means that messages from all chains are accepted. An empty
    /// `HashSet` denotes that messages from no chains are accepted.
    restrict_chain_ids_to: Option<HashSet<ChainId>>,
}

#[derive(Copy, Clone, Debug, clap::ValueEnum)]
pub enum BlanketMessagePolicy {
    /// Automatically accept all incoming messages. Reject them only if execution fails.
    Accept,
    /// Automatically reject tracked messages, ignore or skip untracked messages, but accept
    /// protected ones.
    Reject,
    /// Don't include any messages in blocks, and don't make any decision whether to accept or
    /// reject.
    Ignore,
}

impl MessagePolicy {
    pub fn new(
        blanket: BlanketMessagePolicy,
        restrict_chain_ids_to: Option<HashSet<ChainId>>,
    ) -> Self {
        Self {
            blanket,
            restrict_chain_ids_to,
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn handle(&self, bundle: &mut IncomingBundle) -> bool {
        if self.is_reject() {
            if bundle.bundle.is_skippable() {
                return false;
            } else if bundle.bundle.is_tracked() {
                bundle.action = MessageAction::Reject;
            }
        }
        let sender = bundle.origin.sender;
        match &self.restrict_chain_ids_to {
            None => true,
            Some(chains) => chains.contains(&sender),
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn is_ignore(&self) -> bool {
        matches!(self.blanket, BlanketMessagePolicy::Ignore)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn is_reject(&self) -> bool {
        matches!(self.blanket, BlanketMessagePolicy::Reject)
    }
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ChainClientOptions {
    /// Maximum number of pending message bundles processed at a time in a block.
    pub max_pending_message_bundles: usize,
    /// The policy for automatically handling incoming messages.
    pub message_policy: MessagePolicy,
    /// Whether to block on cross-chain message delivery.
    pub cross_chain_message_delivery: CrossChainMessageDelivery,
}

/// Client to operate a chain by interacting with validators and the given local storage
/// implementation.
/// * The chain being operated is called the "local chain" or just the "chain".
/// * As a rule, operations are considered successful (and communication may stop) when
///   they succeeded in gathering a quorum of responses.
pub struct ChainClient<ValidatorNodeProvider, Storage>
where
    Storage: linera_storage::Storage,
{
    /// The Linera [`Client`] that manages operations for this chain client.
    client: Arc<Client<ValidatorNodeProvider, Storage>>,
    /// The off-chain chain ID.
    chain_id: ChainId,
    /// The client options.
    options: ChainClientOptions,
}

impl<P, S> Clone for ChainClient<P, S>
where
    S: linera_storage::Storage,
{
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            chain_id: self.chain_id,
            options: self.options.clone(),
        }
    }
}

impl<P, S> std::fmt::Debug for ChainClient<P, S>
where
    S: linera_storage::Storage,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter
            .debug_struct("ChainClient")
            .field("chain_id", &format!("{:.8}", self.chain_id))
            .finish_non_exhaustive()
    }
}

/// Error type for [`ChainClient`].
#[derive(Debug, Error)]
pub enum ChainClientError {
    #[error("Local node operation failed: {0}")]
    LocalNodeError(#[from] LocalNodeError),

    #[error("Remote node operation failed: {0}")]
    RemoteNodeError(#[from] NodeError),

    #[error(transparent)]
    ArithmeticError(#[from] ArithmeticError),

    #[error("JSON (de)serialization error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Chain operation failed: {0}")]
    ChainError(#[from] ChainError),

    #[error(transparent)]
    CommunicationError(#[from] CommunicationError<NodeError>),

    #[error("Internal error within chain client: {0}")]
    InternalError(&'static str),

    #[error(
        "Cannot accept a certificate from an unknown committee in the future. \
         Please synchronize the local view of the admin chain"
    )]
    CommitteeSynchronizationError,

    #[error("The local node is behind the trusted state in wallet and needs synchronization with validators")]
    WalletSynchronizationError,

    #[error("The state of the client is incompatible with the proposed block: {0}")]
    BlockProposalError(&'static str),

    #[error(
        "Cannot accept a certificate from a committee that was retired. \
         Try a newer certificate from the same origin"
    )]
    CommitteeDeprecationError,

    #[error("Protocol error within chain client: {0}")]
    ProtocolError(&'static str),

    #[error("No key available to interact with chain {0}")]
    CannotFindKeyForChain(ChainId),

    #[error("Found several possible identities to interact with chain {0}")]
    FoundMultipleKeysForChain(ChainId),

    #[error(transparent)]
    ViewError(#[from] ViewError),

    #[error("Blobs not found: {0:?}")]
    BlobsNotFound(Vec<BlobId>),
}

impl From<Infallible> for ChainClientError {
    fn from(infallible: Infallible) -> Self {
        match infallible {}
    }
}

// We never want to pass the DashMap references over an `await` point, for fear of
// deadlocks. The following construct will cause a (relatively) helpful error if we do.

pub struct Unsend<T> {
    inner: T,
    _phantom: std::marker::PhantomData<*mut u8>,
}

impl<T> Unsend<T> {
    fn new(inner: T) -> Self {
        Self {
            inner,
            _phantom: Default::default(),
        }
    }
}

impl<T: Deref> Deref for Unsend<T> {
    type Target = T::Target;
    fn deref(&self) -> &T::Target {
        self.inner.deref()
    }
}

impl<T: DerefMut> DerefMut for Unsend<T> {
    fn deref_mut(&mut self) -> &mut T::Target {
        self.inner.deref_mut()
    }
}

pub type ChainGuard<'a, T> = Unsend<DashMapRef<'a, ChainId, T>>;
pub type ChainGuardMut<'a, T> = Unsend<DashMapRefMut<'a, ChainId, T>>;
pub type ChainGuardMapped<'a, T> = Unsend<DashMapMappedRef<'a, ChainId, ChainState, T>>;

impl<P: 'static, S: Storage> ChainClient<P, S> {
    /// Gets a shared reference to the chain's state.
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn state(&self) -> ChainGuard<ChainState> {
        Unsend::new(
            self.client
                .chains
                .get(&self.chain_id)
                .expect("Chain client constructed for invalid chain"),
        )
    }

    /// Gets a mutable reference to the state.
    /// Beware: this will block any other reference to any chain's state!
    #[tracing::instrument(level = "trace", skip(self))]
    fn state_mut(&self) -> ChainGuardMut<ChainState> {
        Unsend::new(
            self.client
                .chains
                .get_mut(&self.chain_id)
                .expect("Chain client constructed for invalid chain"),
        )
    }

    /// Gets the per-`ChainClient` options.
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn options_mut(&mut self) -> &mut ChainClientOptions {
        &mut self.options
    }

    /// Gets the ID of the associated chain.
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    /// Gets the hash of the latest known block.
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn block_hash(&self) -> Option<CryptoHash> {
        self.state().block_hash()
    }

    /// Gets the earliest possible timestamp for the next block.
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn timestamp(&self) -> Timestamp {
        self.state().timestamp()
    }

    /// Gets the next block height.
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn next_block_height(&self) -> BlockHeight {
        self.state().next_block_height()
    }

    /// Gets a guarded reference to the next pending block.
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn pending_block(&self) -> ChainGuardMapped<Option<Block>> {
        Unsend::new(self.state().inner.map(|state| state.pending_block()))
    }

    /// Gets a guarded reference to the set of pending blobs.
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn pending_blobs(&self) -> ChainGuardMapped<BTreeMap<BlobId, Blob>> {
        Unsend::new(self.state().inner.map(|state| state.pending_blobs()))
    }
}

enum ReceiveCertificateMode {
    NeedsCheck,
    AlreadyChecked,
}

enum HandleCertificateResult {
    OldEpoch,
    New,
    FutureEpoch,
}

impl<P, S> ChainClient<P, S>
where
    P: ValidatorNodeProvider + Sync + 'static,
    S: Storage + Clone + Send + Sync + 'static,
{
    /// Obtains a `ChainStateView` for a given `ChainId`.
    #[tracing::instrument(level = "trace")]
    pub async fn chain_state_view(
        &self,
    ) -> Result<OwnedRwLockReadGuard<ChainStateView<S::Context>>, LocalNodeError> {
        Ok(self
            .client
            .local_node
            .chain_state_view(self.chain_id)
            .await?)
    }

    /// Subscribes to notifications from this client's chain.
    #[tracing::instrument(level = "trace")]
    pub async fn subscribe(&self) -> Result<NotificationStream, LocalNodeError> {
        Ok(Box::pin(UnboundedReceiverStream::new(
            self.client.notifier.subscribe(vec![self.chain_id]),
        )))
    }

    /// Returns the storage client used by this client's local node.
    #[tracing::instrument(level = "trace")]
    pub fn storage_client(&self) -> S {
        self.client.storage_client().clone()
    }

    /// Obtains the basic `ChainInfo` data for the local chain.
    #[tracing::instrument(level = "trace")]
    pub async fn chain_info(&self) -> Result<Box<ChainInfo>, LocalNodeError> {
        let query = ChainInfoQuery::new(self.chain_id);
        let response = self
            .client
            .local_node
            .handle_chain_info_query(query)
            .await?;
        Ok(response.info)
    }

    /// Obtains the basic `ChainInfo` data for the local chain, with chain manager values.
    #[tracing::instrument(level = "trace")]
    pub async fn chain_info_with_manager_values(&self) -> Result<Box<ChainInfo>, LocalNodeError> {
        let query = ChainInfoQuery::new(self.chain_id).with_manager_values();
        let response = self
            .client
            .local_node
            .handle_chain_info_query(query)
            .await?;
        Ok(response.info)
    }

    /// Obtains up to `self.options.max_pending_message_bundles` pending message bundles for the
    /// local chain.
    #[tracing::instrument(level = "trace")]
    async fn pending_message_bundles(&self) -> Result<Vec<IncomingBundle>, ChainClientError> {
        if self.next_block_height() != BlockHeight::ZERO && self.options.message_policy.is_ignore()
        {
            return Ok(Vec::new()); // OpenChain is already received, other are ignored.
        }
        let query = ChainInfoQuery::new(self.chain_id).with_pending_message_bundles();
        let info = self
            .client
            .local_node
            .handle_chain_info_query(query)
            .await?
            .info;
        ensure!(
            info.next_block_height == self.next_block_height(),
            ChainClientError::WalletSynchronizationError
        );
        let mut requested_pending_message_bundles = info.requested_pending_message_bundles;
        let mut pending_message_bundles = vec![];
        // The first incoming message of any child chain must be `OpenChain`. We must have it in
        // our inbox, and include it before all other messages.
        if info.next_block_height == BlockHeight::ZERO
            && info
                .description
                .ok_or_else(|| LocalNodeError::InactiveChain(self.chain_id))?
                .is_child()
        {
            let Some(index) = requested_pending_message_bundles
                .iter()
                .position(|message| {
                    matches!(
                        message.bundle.messages.first(),
                        Some(PostedMessage {
                            message: Message::System(SystemMessage::OpenChain(_)),
                            ..
                        })
                    )
                })
            else {
                return Err(LocalNodeError::InactiveChain(self.chain_id).into());
            };
            let open_chain_bundle = requested_pending_message_bundles.remove(index);
            pending_message_bundles.push(open_chain_bundle);
        }
        if self.options.message_policy.is_ignore() {
            return Ok(pending_message_bundles); // Ignore messages other than OpenChain.
        }
        for mut bundle in requested_pending_message_bundles {
            if pending_message_bundles.len() >= self.options.max_pending_message_bundles {
                warn!(
                    "Limiting block to {} incoming message bundles",
                    self.options.max_pending_message_bundles
                );
                break;
            }
            if !self.options.message_policy.handle(&mut bundle) {
                continue;
            }
            pending_message_bundles.push(bundle);
        }
        Ok(pending_message_bundles)
    }

    /// Obtains the current epoch of the given chain as well as its set of trusted committees.
    #[tracing::instrument(level = "trace")]
    pub async fn epoch_and_committees(
        &self,
        chain_id: ChainId,
    ) -> Result<(Option<Epoch>, BTreeMap<Epoch, Committee>), LocalNodeError> {
        let query = ChainInfoQuery::new(chain_id).with_committees();
        let info = self
            .client
            .local_node
            .handle_chain_info_query(query)
            .await?
            .info;
        let epoch = info.epoch;
        let committees = info
            .requested_committees
            .ok_or(LocalNodeError::InvalidChainInfoResponse)?;
        Ok((epoch, committees))
    }

    /// Obtains the epochs of the committees trusted by the local chain.
    #[tracing::instrument(level = "trace")]
    pub async fn epochs(&self) -> Result<Vec<Epoch>, LocalNodeError> {
        let (_epoch, committees) = self.epoch_and_committees(self.chain_id).await?;
        Ok(committees.into_keys().collect())
    }

    /// Obtains the committee for the current epoch of the local chain.
    #[tracing::instrument(level = "trace")]
    pub async fn local_committee(&self) -> Result<Committee, LocalNodeError> {
        let (epoch, mut committees) = self.epoch_and_committees(self.chain_id).await?;
        committees
            .remove(
                epoch
                    .as_ref()
                    .ok_or(LocalNodeError::InactiveChain(self.chain_id))?,
            )
            .ok_or(LocalNodeError::InactiveChain(self.chain_id))
    }

    /// Obtains all the committees trusted by either the local chain or its admin chain. Also
    /// return the latest trusted epoch.
    #[tracing::instrument(level = "trace")]
    async fn known_committees(
        &self,
    ) -> Result<(BTreeMap<Epoch, Committee>, Epoch), LocalNodeError> {
        let (epoch, mut committees) = self.epoch_and_committees(self.chain_id).await?;
        let admin_id = self.state().admin_id();
        let (admin_epoch, admin_committees) = self.epoch_and_committees(admin_id).await?;
        committees.extend(admin_committees);
        let epoch = std::cmp::max(epoch.unwrap_or_default(), admin_epoch.unwrap_or_default());
        Ok((committees, epoch))
    }

    #[tracing::instrument(level = "trace")]
    fn make_nodes(&self, committee: &Committee) -> Result<Vec<RemoteNode<P::Node>>, NodeError> {
        Ok(self
            .client
            .validator_node_provider
            .make_nodes(committee)?
            .map(|(name, node)| RemoteNode { name, node })
            .collect())
    }

    /// Obtains the validators trusted by the local chain.
    #[tracing::instrument(level = "trace")]
    async fn validator_nodes(&self) -> Result<Vec<RemoteNode<P::Node>>, ChainClientError> {
        match self.local_committee().await {
            Ok(committee) => Ok(self.make_nodes(&committee)?),
            Err(LocalNodeError::InactiveChain(_)) => Ok(Vec::new()),
            Err(LocalNodeError::WorkerError(WorkerError::ChainError(error)))
                if matches!(*error, ChainError::InactiveChain(_)) =>
            {
                Ok(Vec::new())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Obtains the current epoch of the local chain.
    #[tracing::instrument(level = "trace")]
    async fn epoch(&self) -> Result<Epoch, LocalNodeError> {
        self.chain_info()
            .await?
            .epoch
            .ok_or(LocalNodeError::InactiveChain(self.chain_id))
    }

    /// Obtains the identity of the current owner of the chain. Returns an error if we have the
    /// private key for more than one identity.
    #[tracing::instrument(level = "trace")]
    pub async fn identity(&self) -> Result<Owner, ChainClientError> {
        let manager = self.chain_info().await?.manager;
        ensure!(
            manager.ownership.is_active(),
            LocalNodeError::InactiveChain(self.chain_id)
        );
        let mut identities = manager
            .ownership
            .all_owners()
            .chain(&manager.leader)
            .filter(|owner| self.state().known_key_pairs().contains_key(owner));
        let Some(identity) = identities.next() else {
            return Err(ChainClientError::CannotFindKeyForChain(self.chain_id));
        };
        ensure!(
            identities.all(|id| id == identity),
            ChainClientError::FoundMultipleKeysForChain(self.chain_id)
        );
        Ok(*identity)
    }

    /// Obtains the key pair associated to the current identity.
    #[tracing::instrument(level = "trace")]
    pub async fn key_pair(&self) -> Result<KeyPair, ChainClientError> {
        let id = self.identity().await?;
        Ok(self
            .state()
            .known_key_pairs()
            .get(&id)
            .expect("key should be known at this point")
            .copy())
    }

    /// Obtains the public key associated to the current identity.
    #[tracing::instrument(level = "trace")]
    pub async fn public_key(&self) -> Result<PublicKey, ChainClientError> {
        Ok(self.key_pair().await?.public())
    }

    /// Prepares the chain for the next operation, i.e. makes sure we have synchronized it up to
    /// its current height.
    #[tracing::instrument(level = "trace")]
    async fn prepare_chain(&self) -> Result<Box<ChainInfo>, ChainClientError> {
        #[cfg(with_metrics)]
        let _latency = metrics::PREPARE_CHAIN_LATENCY.measure_latency();

        // Verify that our local storage contains enough history compared to the
        // expected block height. Otherwise, download the missing history from the
        // network.
        let next_block_height = self.next_block_height();
        let nodes = self.validator_nodes().await?;
        let mut info = self
            .client
            .download_certificates(&nodes, self.chain_id, next_block_height)
            .await?;
        if info.next_block_height == next_block_height {
            // Check that our local node has the expected block hash.
            ensure!(
                self.block_hash() == info.block_hash,
                ChainClientError::InternalError("Invalid chain of blocks in local node")
            );
        }
        let ownership = &info.manager.ownership;
        let keys: HashSet<_> = self.state().known_key_pairs().keys().cloned().collect();
        if ownership.all_owners().any(|owner| !keys.contains(owner)) {
            let mutex = self.state().client_mutex();
            let _guard = mutex.lock_owned().await;

            // For chains with any owner other than ourselves, we could be missing recent
            // certificates created by other owners. Further synchronize blocks from the network.
            // This is a best-effort that depends on network conditions.
            let nodes = self.validator_nodes().await?;
            info = self.synchronize_chain_state(&nodes, self.chain_id).await?;
        }
        self.update_from_info(&info);
        Ok(info)
    }

    /// Submits a validated block for finalization and returns the confirmed block certificate.
    #[tracing::instrument(level = "trace", skip(committee, certificate))]
    async fn finalize_block(
        &self,
        committee: &Committee,
        certificate: Certificate,
    ) -> Result<Certificate, ChainClientError> {
        let value = certificate.value.validated_to_confirmed().ok_or_else(|| {
            ChainClientError::InternalError(
                "Certificate for finalization must be a validated block",
            )
        })?;
        let finalize_action = CommunicateAction::FinalizeBlock {
            certificate,
            delivery: self.options.cross_chain_message_delivery,
        };
        let certificate = self
            .communicate_chain_action(committee, finalize_action, value)
            .await?;
        self.receive_certificate_and_update_validators_internal(
            certificate.clone(),
            ReceiveCertificateMode::AlreadyChecked,
        )
        .await?;
        Ok(certificate)
    }

    /// Submits a block proposal to the validators. If it is a slow round, also submits the
    /// validated block for finalization. Returns the confirmed block certificate.
    #[tracing::instrument(level = "trace", skip(committee, proposal, value))]
    async fn submit_block_proposal(
        &self,
        committee: &Committee,
        proposal: Box<BlockProposal>,
        value: HashedCertificateValue,
    ) -> Result<Certificate, ChainClientError> {
        let blob_ids = value
            .inner()
            .executed_block()
            .expect("The result of executing a proposal is always an executed block")
            .outcome
            .required_blob_ids();
        let submit_action = CommunicateAction::SubmitBlock { proposal, blob_ids };
        let certificate = self
            .communicate_chain_action(committee, submit_action, value)
            .await?;
        self.process_certificate(certificate.clone(), vec![])
            .await?;
        if certificate.value().is_confirmed() {
            Ok(certificate)
        } else {
            self.finalize_block(committee, certificate).await
        }
    }

    /// Broadcasts certified blocks to validators.
    #[tracing::instrument(level = "trace", skip(committee, delivery))]
    async fn communicate_chain_updates(
        &self,
        committee: &Committee,
        chain_id: ChainId,
        height: BlockHeight,
        delivery: CrossChainMessageDelivery,
    ) -> Result<(), ChainClientError> {
        let local_node = self.client.local_node.clone();
        let nodes = self.make_nodes(committee)?;
        communicate_with_quorum(
            &nodes,
            committee,
            |_: &()| (),
            |remote_node| {
                let mut updater = ValidatorUpdater {
                    remote_node,
                    local_node: local_node.clone(),
                };
                Box::pin(async move {
                    updater
                        .send_chain_information(chain_id, height, delivery)
                        .await
                })
            },
        )
        .await?;
        Ok(())
    }

    /// Broadcasts certified blocks and optionally a block proposal, certificate or
    /// leader timeout request.
    ///
    /// In that case, it verifies that the validator votes are for the provided value,
    /// and returns a certificate.
    #[tracing::instrument(level = "trace", skip(committee, action, value))]
    async fn communicate_chain_action(
        &self,
        committee: &Committee,
        action: CommunicateAction,
        value: HashedCertificateValue,
    ) -> Result<Certificate, ChainClientError> {
        let local_node = self.client.local_node.clone();
        let nodes = self.make_nodes(committee)?;
        let ((votes_hash, votes_round), votes) = communicate_with_quorum(
            &nodes,
            committee,
            |vote: &LiteVote| (vote.value.value_hash, vote.round),
            |remote_node| {
                let mut updater = ValidatorUpdater {
                    remote_node,
                    local_node: local_node.clone(),
                };
                let action = action.clone();
                Box::pin(async move { updater.send_chain_update(action).await })
            },
        )
        .await?;
        ensure!(
            (votes_hash, votes_round) == (value.hash(), action.round()),
            ChainClientError::ProtocolError("Unexpected response from validators")
        );
        // Certificate is valid because
        // * `communicate_with_quorum` ensured a sufficient "weight" of
        // (non-error) answers were returned by validators.
        // * each answer is a vote signed by the expected validator.
        let certificate = LiteCertificate::try_from_votes(votes)
            .ok_or_else(|| {
                ChainClientError::InternalError("Vote values or rounds don't match; this is a bug")
            })?
            .with_value(value)
            .ok_or_else(|| {
                ChainClientError::ProtocolError("A quorum voted for an unexpected value")
            })?;
        Ok(certificate)
    }

    /// Processes the confirmed block certificate and its ancestors in the local node, then
    /// updates the validators up to that certificate.
    #[tracing::instrument(level = "trace", skip(certificate, mode))]
    async fn receive_certificate_and_update_validators_internal(
        &self,
        certificate: Certificate,
        mode: ReceiveCertificateMode,
    ) -> Result<(), ChainClientError> {
        let block_chain_id = certificate.value().chain_id();
        let block_height = certificate.value().height();

        self.receive_certificate_internal(certificate, mode).await?;

        // Make sure a quorum of validators (according to our new local committee) are up-to-date
        // for data availability.
        let local_committee = self.local_committee().await?;
        self.communicate_chain_updates(
            &local_committee,
            block_chain_id,
            block_height.try_add_one()?,
            CrossChainMessageDelivery::Blocking,
        )
        .await?;
        Ok(())
    }

    /// Processes the confirmed block certificate in the local node. Also downloads and processes
    /// all ancestors that are still missing.
    #[tracing::instrument(level = "trace", skip(certificate, mode))]
    async fn receive_certificate_internal(
        &self,
        certificate: Certificate,
        mode: ReceiveCertificateMode,
    ) -> Result<(), ChainClientError> {
        let CertificateValue::ConfirmedBlock { executed_block, .. } = certificate.value() else {
            return Err(ChainClientError::InternalError(
                "Was expecting a confirmed chain operation",
            ));
        };
        let block = &executed_block.block;

        // Verify the certificate before doing any expensive networking.
        let (committees, max_epoch) = self.known_committees().await?;
        ensure!(
            block.epoch <= max_epoch,
            ChainClientError::CommitteeSynchronizationError
        );
        let remote_committee = committees
            .get(&block.epoch)
            .ok_or_else(|| ChainClientError::CommitteeDeprecationError)?;
        if let ReceiveCertificateMode::NeedsCheck = mode {
            certificate.check(remote_committee)?;
        }
        // Recover history from the network. We assume that the committee that signed the
        // certificate is still active.
        let nodes = self.make_nodes(remote_committee)?;
        self.client
            .download_certificates(&nodes, block.chain_id, block.height)
            .await?;
        // Process the received operations. Download required hashed certificate values if necessary.
        if let Err(err) = self.process_certificate(certificate.clone(), vec![]).await {
            match &err {
                LocalNodeError::WorkerError(WorkerError::BlobsNotFound(blob_ids)) => {
                    let blobs = LocalNodeClient::<S>::download_blobs(blob_ids, &nodes).await;

                    ensure!(blobs.len() == blob_ids.len(), err);
                    self.process_certificate(certificate.clone(), blobs).await?;
                }
                _ => {
                    // The certificate is not as expected. Give up.
                    warn!("Failed to process network hashed certificate value");
                    return Err(err.into());
                }
            }
        }

        Ok(())
    }

    /// Downloads and processes all confirmed block certificates that sent any message to this
    /// chain, including their ancestors.
    #[tracing::instrument(level = "trace")]
    async fn synchronize_received_certificates_from_validator(
        &self,
        chain_id: ChainId,
        remote_node: &RemoteNode<P::Node>,
    ) -> Result<(ValidatorName, u64, Vec<Certificate>), NodeError> {
        let tracker = self
            .state()
            .received_certificate_trackers()
            .get(&remote_node.name)
            .copied()
            .unwrap_or(0);
        let (committees, max_epoch) = self
            .known_committees()
            .await
            .map_err(|_| NodeError::InvalidChainInfoResponse)?;
        // Retrieve newly received certificates from this validator.
        let query = ChainInfoQuery::new(chain_id).with_received_log_excluding_first_n(tracker);
        let info = remote_node.handle_chain_info_query(query).await?;
        let mut certificates: Vec<Certificate> = Vec::new();
        let mut new_tracker = tracker;

        for entry in info.requested_received_log {
            let query = ChainInfoQuery::new(entry.chain_id)
                .with_sent_certificate_hashes_in_range(BlockHeightRange::single(entry.height));

            let local_response = self
                .client
                .local_node
                .handle_chain_info_query(query.clone())
                .await
                .map_err(|error| NodeError::LocalNodeQuery {
                    error: error.to_string(),
                })?
                .info;
            if !local_response.requested_sent_certificate_hashes.is_empty() {
                // We've already processed incoming messages for this certificate.
                new_tracker += 1;
                continue;
            }

            let remote_response = remote_node.handle_chain_info_query(query).await?;
            let certificate_hash =
                match remote_response.requested_sent_certificate_hashes.as_slice() {
                    &[hash] => hash,
                    [] => {
                        warn!("Validator didn't have certificate he claimed to have.");
                        break;
                    }
                    _ => {
                        error!("Validator sent more than one certificate hash for a single block.");
                        break;
                    }
                };

            let certificate = remote_node
                .node
                .download_certificate(certificate_hash)
                .await?;

            match self
                .check_certificate(max_epoch, &committees, &certificate)
                .await?
            {
                HandleCertificateResult::FutureEpoch => {
                    warn!("Postponing received certificate from {:.8} at height {} from future epoch {}",
                          entry.chain_id, entry.height, certificate.value().epoch());
                    // Stop the synchronization here. Do not increment the tracker further so
                    // that this certificate can still be downloaded later, once our committee
                    // is updated.
                    break;
                }
                HandleCertificateResult::OldEpoch => {
                    // This epoch is not recognized any more. Let's skip the certificate.
                    // If a higher block with a recognized epoch comes up later from the
                    // same chain, the call to `receive_certificate` below will download
                    // the skipped certificate again.
                    warn!(
                        "Skipping received certificate from past epoch {:?}",
                        certificate.value().epoch()
                    );
                    new_tracker += 1;
                }
                HandleCertificateResult::New => {
                    certificates.push(certificate);
                    new_tracker += 1;
                }
            }
        }
        Ok((remote_node.name, new_tracker, certificates))
    }

    async fn check_certificate(
        &self,
        highest_known_epoch: Epoch,
        committees: &BTreeMap<Epoch, Committee>,
        incoming_certificate: &Certificate,
    ) -> Result<HandleCertificateResult, NodeError> {
        let CertificateValue::ConfirmedBlock { executed_block, .. } = incoming_certificate.value()
        else {
            return Err(NodeError::InvalidChainInfoResponse);
        };
        let block = &executed_block.block;
        // Check that certificates are valid w.r.t one of our trusted committees.
        if block.epoch > highest_known_epoch {
            return Ok(HandleCertificateResult::FutureEpoch);
        }
        if let Some(known_committee) = committees.get(&block.epoch) {
            // This epoch is recognized by our chain. Let's verify the
            // certificate.
            let _ = incoming_certificate.check(known_committee)?;
            Ok(HandleCertificateResult::New)
        } else {
            // We don't accept a certificate from a committee that was retired.
            return Ok(HandleCertificateResult::OldEpoch);
        }
    }

    /// Processes the result of [`synchronize_received_certificates_from_validator`] and updates
    /// the tracker for this validator.
    #[tracing::instrument(level = "trace", skip(tracker, certificates))]
    async fn receive_certificates_from_validator(
        &self,
        name: ValidatorName,
        tracker: u64,
        certificates: Vec<Certificate>,
    ) {
        for certificate in certificates {
            let hash = certificate.hash();
            if let Err(e) = self
                .receive_certificate_internal(certificate, ReceiveCertificateMode::AlreadyChecked)
                .await
            {
                warn!("Received invalid certificate {hash} from {name}: {e}");
                // Do not update the validator's tracker in case of error.
                // Move on to the next validator.
                return;
            }
        }
        // Update tracker.
        self.state_mut()
            .update_received_certificate_tracker(name, tracker);
    }

    /// Attempts to download new received certificates.
    ///
    /// This is a best effort: it will only find certificates that have been confirmed
    /// amongst sufficiently many validators of the current committee of the target
    /// chain.
    ///
    /// However, this should be the case whenever a sender's chain is still in use and
    /// is regularly upgraded to new committees.
    #[tracing::instrument(level = "trace")]
    async fn find_received_certificates(&self) -> Result<(), ChainClientError> {
        #[cfg(with_metrics)]
        let _latency = metrics::FIND_RECEIVED_CERTIFICATES_LATENCY.measure_latency();

        // Use network information from the local chain.
        let chain_id = self.chain_id;
        let local_committee = self.local_committee().await?;
        let nodes = self.make_nodes(&local_committee)?;
        // Synchronize the state of the admin chain from the network.
        let admin_id = self.state().admin_id();
        self.synchronize_chain_state(&nodes, admin_id).await?;
        let client = self.clone();
        // Proceed to downloading received certificates.
        let result = communicate_with_quorum(
            &nodes,
            &local_committee,
            |_| (),
            |remote_node| {
                let client = client.clone();
                Box::pin(async move {
                    client
                        .synchronize_received_certificates_from_validator(chain_id, &remote_node)
                        .await
                })
            },
        )
        .await;
        let responses = match result {
            Ok(((), responses)) => responses,
            Err(CommunicationError::Trusted(NodeError::InactiveChain(id))) if id == chain_id => {
                // The chain is visibly not active (yet or any more) so there is no need
                // to synchronize received certificates.
                return Ok(());
            }
            Err(error) => {
                return Err(error.into());
            }
        };
        for (name, tracker, certificates) in responses {
            // Process received certificates.
            self.receive_certificates_from_validator(name, tracker, certificates)
                .await;
        }
        Ok(())
    }

    /// Sends money.
    #[tracing::instrument(level = "trace")]
    pub async fn transfer(
        &self,
        owner: Option<Owner>,
        amount: Amount,
        recipient: Recipient,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        // TODO(#467): check the balance of `owner` before signing any block proposal.
        self.execute_operation(Operation::System(SystemOperation::Transfer {
            owner,
            recipient,
            amount,
        }))
        .await
    }

    /// Verify if a data blob is readable from storage.
    // TODO(#2490): Consider removing or renaming this.
    #[tracing::instrument(level = "trace", skip(hash))]
    pub async fn read_data_blob(
        &self,
        hash: CryptoHash,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        let blob_id = BlobId {
            hash,
            blob_type: BlobType::Data,
        };
        self.execute_operation(Operation::System(SystemOperation::ReadBlob { blob_id }))
            .await
    }

    /// Claims money in a remote chain.
    #[tracing::instrument(level = "trace")]
    pub async fn claim(
        &self,
        owner: Owner,
        target_id: ChainId,
        recipient: Recipient,
        amount: Amount,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::Claim {
            owner,
            target_id,
            recipient,
            amount,
        }))
        .await
    }

    /// Handles the certificate in the local node and the resulting notifications.
    #[tracing::instrument(level = "trace", skip(certificate))]
    async fn process_certificate(
        &self,
        certificate: Certificate,
        blobs: Vec<Blob>,
    ) -> Result<(), LocalNodeError> {
        let info = self
            .client
            .handle_certificate(certificate, blobs)
            .await?
            .info;
        self.update_from_info(&info);
        Ok(())
    }

    /// Updates the latest block and next block height and round information from the chain info.
    #[tracing::instrument(level = "trace", skip(info))]
    fn update_from_info(&self, info: &ChainInfo) {
        if info.chain_id == self.chain_id {
            self.state_mut().update_from_info(info);
        }
    }

    /// Requests a leader timeout vote from all validators. If a quorum signs it, creates a
    /// certificate and sends it to all validators, to make them enter the next round.
    #[tracing::instrument(level = "trace")]
    pub async fn request_leader_timeout(&self) -> Result<Certificate, ChainClientError> {
        let chain_id = self.chain_id;
        let query = ChainInfoQuery::new(chain_id).with_committees();
        let info = self
            .client
            .local_node
            .handle_chain_info_query(query)
            .await?
            .info;
        let epoch = info.epoch.ok_or(LocalNodeError::InactiveChain(chain_id))?;
        let committee = info
            .requested_committees
            .ok_or(LocalNodeError::InvalidChainInfoResponse)?
            .remove(&epoch)
            .ok_or(LocalNodeError::InactiveChain(chain_id))?;
        let height = info.next_block_height;
        let round = info.manager.current_round;
        let action = CommunicateAction::RequestTimeout {
            height,
            round,
            chain_id,
        };
        let value = HashedCertificateValue::new_timeout(chain_id, height, epoch);
        let certificate = self
            .communicate_chain_action(&committee, action, value)
            .await?;
        self.process_certificate(certificate.clone(), vec![])
            .await?;
        // The block height didn't increase, but this will communicate the timeout as well.
        self.communicate_chain_updates(
            &committee,
            chain_id,
            height,
            CrossChainMessageDelivery::NonBlocking,
        )
        .await?;
        Ok(certificate)
    }

    /// Downloads and processes any certificates we are missing for the given chain.
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn synchronize_chain_state(
        &self,
        validators: &[RemoteNode<P::Node>],
        chain_id: ChainId,
    ) -> Result<Box<ChainInfo>, ChainClientError> {
        #[cfg(with_metrics)]
        let _latency = metrics::SYNCHRONIZE_CHAIN_STATE_LATENCY.measure_latency();

        let mut futures = vec![];

        for remote_node in validators {
            let client = self.clone();
            futures.push(async move {
                client
                    .try_synchronize_chain_state_from(remote_node, chain_id)
                    .await
            });
        }

        for result in future::join_all(futures).await {
            if let Err(e) = result {
                error!(?e, "Error synchronizing chain state");
            }
        }

        self.client
            .local_node
            .local_chain_info(chain_id)
            .await
            .map_err(Into::into)
    }

    /// Downloads any certificates from the specified validator that we are missing for the given
    /// chain, and processes them.
    #[tracing::instrument(level = "trace", skip(self, remote_node, chain_id))]
    async fn try_synchronize_chain_state_from(
        &self,
        remote_node: &RemoteNode<P::Node>,
        chain_id: ChainId,
    ) -> Result<(), ChainClientError> {
        let local_info = self.client.local_node.local_chain_info(chain_id).await?;
        let range = BlockHeightRange {
            start: local_info.next_block_height,
            limit: None,
        };
        let query = ChainInfoQuery::new(chain_id)
            .with_sent_certificate_hashes_in_range(range)
            .with_manager_values();
        let info = remote_node.handle_chain_info_query(query).await?;

        let certificates = future::try_join_all(
            info.requested_sent_certificate_hashes
                .into_iter()
                .map(move |hash| async move { remote_node.node.download_certificate(hash).await }),
        )
        .await?;

        if !certificates.is_empty()
            && self
                .client
                .try_process_certificates(remote_node, chain_id, certificates)
                .await
                .is_none()
        {
            return Ok(());
        };
        if let Some(proposal) = info.manager.requested_proposed {
            let owner = proposal.owner;
            while let Err(original_err) = self
                .client
                .local_node
                .handle_block_proposal(*proposal.clone())
                .await
            {
                if let Some(blob_ids) = original_err.get_blobs_not_found() {
                    self.update_local_node_with_blobs_from(blob_ids, remote_node)
                        .await?;
                    continue; // We found the missing blobs: retry.
                }

                warn!(
                    "Skipping proposal from {} and validator {}: {}",
                    owner, remote_node.name, original_err
                );
                break;
            }
        }
        if let Some(cert) = info.manager.requested_locked {
            let hash = cert.hash();
            let mut blobs = vec![];
            while let Err(original_err) = self.client.handle_certificate(*cert.clone(), blobs).await
            {
                if let LocalNodeError::WorkerError(WorkerError::BlobsNotFound(blob_ids)) =
                    &original_err
                {
                    blobs = remote_node
                        .find_missing_blobs(blob_ids.clone(), chain_id)
                        .await?;

                    if blobs.len() == blob_ids.len() {
                        continue; // We found the missing blobs: retry.
                    }
                }
                warn!(
                    "Skipping certificate {} from validator {}: {}",
                    hash, remote_node.name, original_err
                );
                break;
            }
        }
        Ok(())
    }

    /// Downloads and processes from the specified validator a confirmed block certificate that
    /// uses the given blob. If this succeeds, the blob will be in our storage.
    async fn update_local_node_with_blobs_from(
        &self,
        blob_ids: Vec<BlobId>,
        remote_node: &RemoteNode<P::Node>,
    ) -> Result<(), ChainClientError> {
        try_join_all(blob_ids.into_iter().map(|blob_id| async move {
            let certificate = remote_node.download_certificate_for_blob(blob_id).await?;
            // This will download all ancestors of the certificate and process all of them locally.
            self.receive_certificate(certificate).await
        }))
        .await?;

        Ok(())
    }

    /// Downloads and processes a confirmed block certificate that uses the given blob.
    /// If this succeeds, the blob will be in our storage.
    pub async fn receive_certificate_for_blob(
        &self,
        blob_id: BlobId,
    ) -> Result<(), ChainClientError> {
        self.receive_certificates_for_blobs(vec![blob_id]).await
    }

    /// Downloads and processes confirmed block certificates that use the given blobs.
    /// If this succeeds, the blobs will be in our storage.
    pub async fn receive_certificates_for_blobs(
        &self,
        blob_ids: Vec<BlobId>,
    ) -> Result<(), ChainClientError> {
        let validators = self.validator_nodes().await?;
        let mut tasks = BTreeMap::new();

        for blob_id in blob_ids {
            if tasks.contains_key(&blob_id) {
                continue;
            }

            tasks.insert(
                blob_id,
                LocalNodeClient::<S>::download_certificate_for_blob_from_validators_futures(
                    &validators,
                    blob_id,
                )
                .await,
            );
        }

        let mut missing_blobs = Vec::new();
        for (blob_id, mut blob_id_tasks) in tasks {
            let mut found_blob = false;
            while let Some(result) = blob_id_tasks.next().await {
                if let Some(cert) = result {
                    if self.receive_certificate(cert).await.is_ok() {
                        found_blob = true;
                        break;
                    }
                }
            }

            if !found_blob {
                missing_blobs.push(blob_id);
            }
        }

        if missing_blobs.is_empty() {
            Ok(())
        } else {
            Err(ChainClientError::BlobsNotFound(missing_blobs))
        }
    }

    /// Attempts to execute the block locally. If any incoming message execution fails, that
    /// message is rejected and execution is retried, until the block accepts only messages
    /// that succeed.
    #[tracing::instrument(level = "trace", skip(block))]
    async fn stage_block_execution_and_discard_failing_messages(
        &self,
        mut block: Block,
    ) -> Result<(ExecutedBlock, ChainInfoResponse), ChainClientError> {
        loop {
            let result = self.stage_block_execution(block.clone()).await;
            if let Err(ChainClientError::LocalNodeError(LocalNodeError::WorkerError(
                WorkerError::ChainError(chain_error),
            ))) = &result
            {
                if let ChainError::ExecutionError(
                    error,
                    ChainExecutionContext::IncomingBundle(index),
                ) = &**chain_error
                {
                    let message = block
                        .incoming_bundles
                        .get_mut(*index as usize)
                        .expect("Message at given index should exist");
                    if message.bundle.is_protected() {
                        error!("Protected incoming message failed to execute locally: {message:?}");
                    } else {
                        // Reject the faulty message from the block and continue.
                        // TODO(#1420): This is potentially a bit heavy-handed for
                        // retryable errors.
                        info!(
                            %error, origin = ?message.origin,
                            "Message failed to execute locally and will be rejected."
                        );
                        message.action = MessageAction::Reject;
                        continue;
                    }
                }
            }
            return result;
        }
    }

    /// Attempts to execute the block locally. If any attempt to read a blob fails, the blob is
    /// downloaded and execution is retried.
    #[tracing::instrument(level = "trace", skip(block))]
    async fn stage_block_execution(
        &self,
        block: Block,
    ) -> Result<(ExecutedBlock, ChainInfoResponse), ChainClientError> {
        loop {
            let result = self
                .client
                .local_node
                .stage_block_execution(block.clone())
                .await;
            if let Err(err) = &result {
                if let Some(blob_ids) = err.get_blobs_not_found() {
                    self.receive_certificates_for_blobs(blob_ids).await?;
                    continue; // We found the missing blob: retry.
                }
            }
            return Ok(result?);
        }
    }

    /// Tries to read blobs from either the pending blobs or the local node's cache, or
    /// storage
    #[tracing::instrument(level = "trace", skip(blob_ids))]
    async fn read_local_blobs(
        &self,
        blob_ids: impl IntoIterator<Item = BlobId>,
    ) -> Result<Vec<Blob>, LocalNodeError> {
        let mut blobs = Vec::new();
        for blob_id in blob_ids {
            if let Some(blob) = self.client.local_node.recent_blob(&blob_id).await {
                blobs.push(blob);
                continue;
            }

            let maybe_blob = self.pending_blobs().get(&blob_id).cloned();
            if let Some(blob) = maybe_blob {
                self.client.local_node.cache_recent_blob(&blob).await;
                blobs.push(blob);
                continue;
            }

            blobs.push(
                self.storage_client()
                    .read_blob(blob_id)
                    .await
                    .map_err(|_| LocalNodeError::CannotReadLocalBlob {
                        chain_id: self.chain_id,
                        blob_id,
                    })?,
            );
        }
        Ok(blobs)
    }

    /// Executes (or retries) a regular block proposal. Updates local balance.
    #[tracing::instrument(level = "trace", skip(block, round, manager))]
    async fn propose_block(
        &self,
        block: Block,
        round: Round,
        manager: ChainManagerInfo,
    ) -> Result<Certificate, ChainClientError> {
        let next_block_height;
        let block_hash;
        {
            let state = self.state();
            next_block_height = state.next_block_height();
            block_hash = state.block_hash();
            // In the fast round, we must never make any conflicting proposals.
            if round.is_fast() {
                if let Some(pending) = &state.pending_block() {
                    ensure!(
                        pending == &block,
                        ChainClientError::BlockProposalError(
                            "Client state has a different pending block; \
                             use the `linera retry-pending-block` command to commit that first"
                        )
                    );
                }
            }
        }

        ensure!(
            block.height == next_block_height,
            ChainClientError::BlockProposalError("Unexpected block height")
        );
        ensure!(
            block.previous_block_hash == block_hash,
            ChainClientError::BlockProposalError("Unexpected previous block hash")
        );
        // Make sure that we follow the steps in the multi-round protocol.
        let executed_block = if let Some(validated_block_certificate) = &manager.requested_locked {
            ensure!(
                validated_block_certificate.value().block() == Some(&block),
                ChainClientError::BlockProposalError(
                    "A different block has already been validated at this height"
                )
            );
            validated_block_certificate
                .value()
                .executed_block()
                .unwrap()
                .clone()
        } else {
            self.stage_block_execution_and_discard_failing_messages(block)
                .await?
                .0
        };
        let block = executed_block.block.clone();
        if let Some(proposal) = manager.requested_proposed {
            if proposal.content.round.is_fast() {
                ensure!(
                    proposal.content.block == block,
                    ChainClientError::BlockProposalError(
                        "Chain manager has a different pending block in the fast round"
                    )
                );
            }
        }
        let hashed_value = if round.is_fast() {
            HashedCertificateValue::new_confirmed(executed_block)
        } else {
            HashedCertificateValue::new_validated(executed_block)
        };
        // Collect the hashed certificate values required for execution.
        let committee = self.local_committee().await?;
        let blobs = self.read_local_blobs(block.published_blob_ids()).await?;
        // Create the final block proposal.
        let key_pair = self.key_pair().await?;
        let proposal = if let Some(cert) = manager.requested_locked {
            Box::new(BlockProposal::new_retry(round, *cert, &key_pair, blobs))
        } else {
            Box::new(BlockProposal::new_initial(
                round,
                block.clone(),
                &key_pair,
                blobs,
            ))
        };
        // Check the final block proposal. This will be cheaper after #1401.
        self.client
            .local_node
            .handle_block_proposal(*proposal.clone())
            .await?;
        // Remember what we are trying to do before sending the proposal to the validators.
        self.state_mut().set_pending_block(block);
        // Send the query to validators.
        let certificate = self
            .submit_block_proposal(&committee, proposal, hashed_value)
            .await?;
        // Communicate the new certificate now.
        let next_block_height = self.next_block_height();
        self.communicate_chain_updates(
            &committee,
            self.chain_id,
            next_block_height,
            self.options.cross_chain_message_delivery,
        )
        .await?;
        if let Ok(new_committee) = self.local_committee().await {
            if new_committee != committee {
                // If the configuration just changed, communicate to the new committee as well.
                // (This is actually more important that updating the previous committee.)
                let next_block_height = self.next_block_height();
                self.communicate_chain_updates(
                    &new_committee,
                    self.chain_id,
                    next_block_height,
                    self.options.cross_chain_message_delivery,
                )
                .await?;
            }
        }
        Ok(certificate)
    }

    /// Executes a list of operations.
    #[tracing::instrument(level = "trace", skip(operations))]
    pub async fn execute_operations(
        &self,
        operations: Vec<Operation>,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.prepare_chain().await?;
        self.execute_without_prepare(operations).await
    }

    /// Executes a list of operations, without calling `prepare_chain`.
    #[tracing::instrument(level = "trace", skip(operations))]
    pub async fn execute_without_prepare(
        &self,
        operations: Vec<Operation>,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        loop {
            // TODO(#2066): Remove boxing once the call-stack is shallower
            match Box::pin(self.execute_block(operations.clone())).await? {
                ExecuteBlockOutcome::Executed(certificate) => {
                    return Ok(ClientOutcome::Committed(certificate));
                }
                ExecuteBlockOutcome::Conflict(certificate) => {
                    info!(
                        height = %certificate.value().height(),
                        "Another block was committed; retrying."
                    );
                }
                ExecuteBlockOutcome::WaitForTimeout(timeout) => {
                    return Ok(ClientOutcome::WaitForTimeout(timeout));
                }
            };
        }
    }

    /// Executes an operation.
    #[tracing::instrument(level = "trace", skip(operation))]
    pub async fn execute_operation(
        &self,
        operation: Operation,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.execute_operations(vec![operation]).await
    }

    /// Executes a new block.
    ///
    /// This must be preceded by a call to `prepare_chain()`.
    #[tracing::instrument(level = "trace", skip(operations))]
    async fn execute_block(
        &self,
        operations: Vec<Operation>,
    ) -> Result<ExecuteBlockOutcome, ChainClientError> {
        #[cfg(with_metrics)]
        let _latency = metrics::EXECUTE_BLOCK_LATENCY.measure_latency();

        let mutex = self.state().client_mutex();
        let _guard = mutex.lock_owned().await;
        match self.process_pending_block_without_prepare().await? {
            ClientOutcome::Committed(Some(certificate)) => {
                return Ok(ExecuteBlockOutcome::Conflict(certificate))
            }
            ClientOutcome::Committed(None) => {}
            ClientOutcome::WaitForTimeout(timeout) => {
                return Ok(ExecuteBlockOutcome::WaitForTimeout(timeout))
            }
        }
        let incoming_bundles = self.pending_message_bundles().await?;
        let confirmed_value = self.set_pending_block(incoming_bundles, operations).await?;
        match self.process_pending_block_without_prepare().await? {
            ClientOutcome::Committed(Some(certificate))
                if certificate.value().block() == confirmed_value.inner().block() =>
            {
                Ok(ExecuteBlockOutcome::Executed(certificate))
            }
            ClientOutcome::Committed(Some(certificate)) => {
                Ok(ExecuteBlockOutcome::Conflict(certificate))
            }
            // Should be unreachable: We did set a pending block.
            ClientOutcome::Committed(None) => Err(ChainClientError::BlockProposalError(
                "Unexpected block proposal error",
            )),
            ClientOutcome::WaitForTimeout(timeout) => {
                Ok(ExecuteBlockOutcome::WaitForTimeout(timeout))
            }
        }
    }

    /// Sets the pending block, so that next time `process_pending_block_without_prepare` is
    /// called, it will be proposed to the validators.
    #[tracing::instrument(level = "trace", skip(incoming_bundles, operations))]
    async fn set_pending_block(
        &self,
        incoming_bundles: Vec<IncomingBundle>,
        operations: Vec<Operation>,
    ) -> Result<HashedCertificateValue, ChainClientError> {
        let timestamp = self.next_timestamp(&incoming_bundles).await;
        let identity = self.identity().await?;
        let previous_block_hash;
        let height;
        {
            let state = self.state();
            previous_block_hash = state.block_hash();
            height = state.next_block_height();
        }
        let block = Block {
            epoch: self.epoch().await?,
            chain_id: self.chain_id,
            incoming_bundles,
            operations,
            previous_block_hash,
            height,
            authenticated_signer: Some(identity),
            timestamp,
        };
        // Make sure every incoming message succeeds and otherwise remove them.
        // Also, compute the final certified hash while we're at it.
        let (executed_block, _) = self
            .stage_block_execution_and_discard_failing_messages(block)
            .await?;
        self.state_mut()
            .set_pending_block(executed_block.block.clone());
        Ok(HashedCertificateValue::new_confirmed(executed_block))
    }

    /// Returns a suitable timestamp for the next block.
    ///
    /// This will usually be the current time according to the local clock, but may be slightly
    /// ahead to make sure it's not earlier than the incoming messages or the previous block.
    #[tracing::instrument(level = "trace", skip(incoming_bundles))]
    async fn next_timestamp(&self, incoming_bundles: &[IncomingBundle]) -> Timestamp {
        let local_time = self.storage_client().clock().current_time();
        incoming_bundles
            .iter()
            .map(|msg| msg.bundle.timestamp)
            .max()
            .map_or(local_time, |timestamp| timestamp.max(local_time))
            .max(self.timestamp())
    }

    /// Queries an application.
    #[tracing::instrument(level = "trace", skip(query))]
    pub async fn query_application(&self, query: Query) -> Result<Response, ChainClientError> {
        let response = self
            .client
            .local_node
            .query_application(self.chain_id, query)
            .await?;
        Ok(response)
    }

    /// Queries a system application.
    #[tracing::instrument(level = "trace", skip(query))]
    pub async fn query_system_application(
        &self,
        query: SystemQuery,
    ) -> Result<SystemResponse, ChainClientError> {
        let response = self
            .client
            .local_node
            .query_application(self.chain_id, Query::System(query))
            .await?;
        match response {
            Response::System(response) => Ok(response),
            _ => Err(ChainClientError::InternalError(
                "Unexpected response for system query",
            )),
        }
    }

    /// Queries a user application.
    #[tracing::instrument(level = "trace", skip(application_id, query))]
    pub async fn query_user_application<A: Abi>(
        &self,
        application_id: UserApplicationId<A>,
        query: &A::Query,
    ) -> Result<A::QueryResponse, ChainClientError> {
        let query = Query::user(application_id, query)?;
        let response = self
            .client
            .local_node
            .query_application(self.chain_id, query)
            .await?;
        match response {
            Response::User(response) => Ok(serde_json::from_slice(&response)?),
            _ => Err(ChainClientError::InternalError(
                "Unexpected response for user query",
            )),
        }
    }

    /// Obtains the local balance of the chain account after staging the execution of
    /// incoming messages in a new block.
    ///
    /// Does not attempt to synchronize with validators. The result will reflect up to
    /// `max_pending_message_bundles` incoming message bundles and the execution fees for a single
    /// block.
    #[tracing::instrument(level = "trace")]
    pub async fn query_balance(&self) -> Result<Amount, ChainClientError> {
        let (balance, _) = self.query_balances_with_owner(None).await?;
        Ok(balance)
    }

    /// Obtains the local balance of a user account after staging the execution of
    /// incoming messages in a new block.
    ///
    /// Does not attempt to synchronize with validators. The result will reflect up to
    /// `max_pending_message_bundles` incoming message bundles and the execution fees for a single
    /// block.
    #[tracing::instrument(level = "trace", skip(owner))]
    pub async fn query_owner_balance(&self, owner: Owner) -> Result<Amount, ChainClientError> {
        Ok(self
            .query_balances_with_owner(Some(owner))
            .await?
            .1
            .unwrap_or(Amount::ZERO))
    }

    /// Obtains the local balance of the chain account and optionally another user after
    /// staging the execution of incoming messages in a new block.
    ///
    /// Does not attempt to synchronize with validators. The result will reflect up to
    /// `max_pending_message_bundles` incoming message bundles and the execution fees for a single
    /// block.
    #[tracing::instrument(level = "trace", skip(owner))]
    async fn query_balances_with_owner(
        &self,
        owner: Option<Owner>,
    ) -> Result<(Amount, Option<Amount>), ChainClientError> {
        let incoming_bundles = self.pending_message_bundles().await?;
        let timestamp = self.next_timestamp(&incoming_bundles).await;
        let block = Block {
            epoch: self.epoch().await?,
            chain_id: self.chain_id,
            incoming_bundles,
            operations: Vec::new(),
            previous_block_hash: self.block_hash(),
            height: self.next_block_height(),
            authenticated_signer: owner,
            timestamp,
        };
        match self
            .stage_block_execution_and_discard_failing_messages(block)
            .await
        {
            Ok((_, response)) => Ok((
                response.info.chain_balance,
                response.info.requested_owner_balance,
            )),
            Err(ChainClientError::LocalNodeError(LocalNodeError::WorkerError(
                WorkerError::ChainError(error),
            ))) if matches!(
                *error,
                ChainError::ExecutionError(
                    ExecutionError::SystemError(
                        SystemExecutionError::InsufficientFundingForFees { .. }
                    ),
                    ChainExecutionContext::Block
                )
            ) =>
            {
                // We can't even pay for the execution of one empty block. Let's return zero.
                Ok((Amount::ZERO, Some(Amount::ZERO)))
            }
            Err(error) => Err(error),
        }
    }

    /// Reads the local balance of the chain account.
    ///
    /// Does not process the inbox or attempt to synchronize with validators.
    #[tracing::instrument(level = "trace")]
    pub async fn local_balance(&self) -> Result<Amount, ChainClientError> {
        let (balance, _) = self.local_balances_with_owner(None).await?;
        Ok(balance)
    }

    /// Reads the local balance of a user account.
    ///
    /// Does not process the inbox or attempt to synchronize with validators.
    #[tracing::instrument(level = "trace", skip(owner))]
    pub async fn local_owner_balance(&self, owner: Owner) -> Result<Amount, ChainClientError> {
        Ok(self
            .local_balances_with_owner(Some(owner))
            .await?
            .1
            .unwrap_or(Amount::ZERO))
    }

    /// Reads the local balance of the chain account and optionally another user.
    ///
    /// Does not process the inbox or attempt to synchronize with validators.
    #[tracing::instrument(level = "trace", skip(owner))]
    async fn local_balances_with_owner(
        &self,
        owner: Option<Owner>,
    ) -> Result<(Amount, Option<Amount>), ChainClientError> {
        let next_block_height = self.next_block_height();
        ensure!(
            self.chain_info().await?.next_block_height == next_block_height,
            ChainClientError::WalletSynchronizationError
        );
        let mut query = ChainInfoQuery::new(self.chain_id);
        query.request_owner_balance = owner;
        let response = self
            .client
            .local_node
            .handle_chain_info_query(query)
            .await?;
        Ok((
            response.info.chain_balance,
            response.info.requested_owner_balance,
        ))
    }

    /// Attempts to update all validators about the local chain.
    #[tracing::instrument(level = "trace")]
    pub async fn update_validators(&self) -> Result<(), ChainClientError> {
        let committee = self.local_committee().await?;
        let next_block_height = self.next_block_height();
        self.communicate_chain_updates(
            &committee,
            self.chain_id,
            next_block_height,
            CrossChainMessageDelivery::NonBlocking,
        )
        .await?;
        Ok(())
    }

    /// Requests a `RegisterApplications` message from another chain so the application can be used
    /// on this one.
    #[tracing::instrument(level = "trace")]
    pub async fn request_application(
        &self,
        application_id: UserApplicationId,
        chain_id: Option<ChainId>,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        let chain_id = chain_id.unwrap_or(application_id.creation.chain_id);
        self.execute_operation(Operation::System(SystemOperation::RequestApplication {
            application_id,
            chain_id,
        }))
        .await
    }

    /// Sends tokens to a chain.
    #[tracing::instrument(level = "trace")]
    pub async fn transfer_to_account(
        &self,
        owner: Option<Owner>,
        amount: Amount,
        account: Account,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.transfer(owner, amount, Recipient::Account(account))
            .await
    }

    /// Burns tokens.
    #[tracing::instrument(level = "trace")]
    pub async fn burn(
        &self,
        owner: Option<Owner>,
        amount: Amount,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.transfer(owner, amount, Recipient::Burn).await
    }

    /// Attempts to synchronize chains that have sent us messages and populate our local
    /// inbox.
    ///
    /// To create a block that actually executes the messages in the inbox,
    /// `process_inbox` must be called separately.
    #[tracing::instrument(level = "trace")]
    pub async fn synchronize_from_validators(&self) -> Result<Box<ChainInfo>, ChainClientError> {
        self.find_received_certificates().await?;
        self.prepare_chain().await
    }

    /// Processes the last pending block
    #[tracing::instrument(level = "trace")]
    pub async fn process_pending_block(
        &self,
    ) -> Result<ClientOutcome<Option<Certificate>>, ChainClientError> {
        self.synchronize_from_validators().await?;
        self.process_pending_block_without_prepare().await
    }

    /// Processes the last pending block. Assumes that the local chain is up to date.
    #[tracing::instrument(level = "trace")]
    async fn process_pending_block_without_prepare(
        &self,
    ) -> Result<ClientOutcome<Option<Certificate>>, ChainClientError> {
        let identity = self.identity().await?;
        let mut info = self.chain_info_with_manager_values().await?;
        // If the current round has timed out, we request a timeout certificate and retry in
        // the next round.
        if let Some(round_timeout) = info.manager.round_timeout {
            if round_timeout <= self.storage_client().clock().current_time() {
                self.request_leader_timeout().await?;
                info = self.chain_info_with_manager_values().await?;
            }
        }
        self.state_mut().update_from_info(&info);
        let manager = *info.manager;

        // If there is a validated block in the current round, finalize it.
        if let Some(certificate) = &manager.requested_locked {
            if certificate.round == manager.current_round {
                let committee = self.local_committee().await?;
                match self.finalize_block(&committee, *certificate.clone()).await {
                    Ok(certificate) => return Ok(ClientOutcome::Committed(Some(certificate))),
                    Err(ChainClientError::CommunicationError(_)) => {
                        // Communication errors in this case often mean that someone else already
                        // finalized the block.
                        let timestamp = manager.round_timeout.ok_or_else(|| {
                            ChainClientError::BlockProposalError(
                                "Cannot propose in the current round.",
                            )
                        })?;
                        return Ok(ClientOutcome::WaitForTimeout(RoundTimeout {
                            timestamp,
                            current_round: manager.current_round,
                            next_block_height: info.next_block_height,
                        }));
                    }
                    Err(error) => return Err(error),
                }
            }
        }

        // The block we want to propose is either the highest validated, or our pending one.
        let Some(block) = manager
            .highest_validated_block()
            .cloned()
            .or_else(|| self.state().pending_block().clone())
        else {
            return Ok(ClientOutcome::Committed(None)); // Nothing to propose.
        };

        // If there is a conflicting proposal in the current round, we can only propose if the
        // next round can be started without a timeout, i.e. if we are in a multi-leader round.
        let conflicting_proposal = manager.requested_proposed.as_ref().is_some_and(|proposal| {
            proposal.content.round == manager.current_round && proposal.content.block != block
        });
        let round = if !conflicting_proposal {
            manager.current_round
        } else if let Some(round) = manager
            .ownership
            .next_round(manager.current_round)
            .filter(|_| manager.current_round.is_multi_leader())
        {
            round
        } else if let Some(timestamp) = manager.round_timeout {
            return Ok(ClientOutcome::WaitForTimeout(RoundTimeout {
                timestamp,
                current_round: manager.current_round,
                next_block_height: info.next_block_height,
            }));
        } else {
            return Err(ChainClientError::BlockProposalError(
                "Conflicting proposal in the current round.",
            ));
        };
        let can_propose = match round {
            Round::Fast => manager.ownership.super_owners.contains_key(&identity),
            Round::MultiLeader(_) => true,
            Round::SingleLeader(_) | Round::Validator(_) => manager.leader == Some(identity),
        };
        if can_propose {
            let certificate = self.propose_block(block.clone(), round, manager).await?;
            Ok(ClientOutcome::Committed(Some(certificate)))
        } else {
            // TODO(#1424): Local timeout might not match validators' exactly.
            let timestamp = manager.round_timeout.ok_or_else(|| {
                ChainClientError::BlockProposalError("Cannot propose in the current round.")
            })?;
            Ok(ClientOutcome::WaitForTimeout(RoundTimeout {
                timestamp,
                current_round: manager.current_round,
                next_block_height: info.next_block_height,
            }))
        }
    }

    /// Clears the information on any operation that previously failed.
    #[tracing::instrument(level = "trace")]
    pub fn clear_pending_block(&self) {
        self.state_mut().clear_pending_block();
    }

    /// Processes a confirmed block for which this chain is a recipient and updates validators.
    #[tracing::instrument(
        level = "trace",
        skip(certificate),
        fields(certificate_hash = ?certificate.hash()),
    )]
    pub async fn receive_certificate_and_update_validators(
        &self,
        certificate: Certificate,
    ) -> Result<(), ChainClientError> {
        self.receive_certificate_and_update_validators_internal(
            certificate,
            ReceiveCertificateMode::NeedsCheck,
        )
        .await
    }

    /// Processes confirmed operation for which this chain is a recipient.
    #[tracing::instrument(
        level = "trace",
        skip(certificate),
        fields(certificate_hash = ?certificate.hash()),
    )]
    pub async fn receive_certificate(
        &self,
        certificate: Certificate,
    ) -> Result<(), ChainClientError> {
        self.receive_certificate_internal(certificate, ReceiveCertificateMode::NeedsCheck)
            .await
    }

    /// Rotates the key of the chain.
    #[tracing::instrument(level = "trace", skip(key_pair))]
    pub async fn rotate_key_pair(
        &self,
        key_pair: KeyPair,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        let new_public_key = self.state_mut().insert_known_key_pair(key_pair);
        self.transfer_ownership(new_public_key).await
    }

    /// Transfers ownership of the chain to a single super owner.
    #[tracing::instrument(level = "trace")]
    pub async fn transfer_ownership(
        &self,
        new_public_key: PublicKey,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::ChangeOwnership {
            super_owners: vec![new_public_key],
            owners: Vec::new(),
            multi_leader_rounds: 2,
            timeout_config: TimeoutConfig::default(),
        }))
        .await
    }

    /// Adds another owner to the chain, and turns existing super owners into regular owners.
    #[tracing::instrument(level = "trace")]
    pub async fn share_ownership(
        &self,
        new_public_key: PublicKey,
        new_weight: u64,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        loop {
            let ownership = self.prepare_chain().await?.manager.ownership;
            ensure!(
                ownership.is_active(),
                ChainError::InactiveChain(self.chain_id)
            );
            let mut owners = ownership.owners.values().copied().collect::<Vec<_>>();
            owners.extend(
                ownership
                    .super_owners
                    .values()
                    .copied()
                    .zip(iter::repeat(100)),
            );
            owners.push((new_public_key, new_weight));
            let operations = vec![Operation::System(SystemOperation::ChangeOwnership {
                super_owners: Vec::new(),
                owners,
                multi_leader_rounds: ownership.multi_leader_rounds,
                timeout_config: ownership.timeout_config,
            })];
            match self.execute_block(operations).await? {
                ExecuteBlockOutcome::Executed(certificate) => {
                    return Ok(ClientOutcome::Committed(certificate));
                }
                ExecuteBlockOutcome::Conflict(certificate) => {
                    info!(
                        height = %certificate.value().height(),
                        "Another block was committed; retrying."
                    );
                }
                ExecuteBlockOutcome::WaitForTimeout(timeout) => {
                    return Ok(ClientOutcome::WaitForTimeout(timeout));
                }
            };
        }
    }

    /// Changes the ownership of this chain. Fails if it would remove existing owners, unless
    /// `remove_owners` is `true`.
    #[tracing::instrument(level = "trace")]
    pub async fn change_ownership(
        &self,
        ownership: ChainOwnership,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::ChangeOwnership {
            super_owners: ownership.super_owners.values().cloned().collect(),
            owners: ownership.owners.values().cloned().collect(),
            multi_leader_rounds: ownership.multi_leader_rounds,
            timeout_config: ownership.timeout_config.clone(),
        }))
        .await
    }

    /// Changes the application permissions configuration on this chain.
    #[tracing::instrument(level = "trace", skip(application_permissions))]
    pub async fn change_application_permissions(
        &self,
        application_permissions: ApplicationPermissions,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        let operation = SystemOperation::ChangeApplicationPermissions(application_permissions);
        self.execute_operation(operation.into()).await
    }

    /// Opens a new chain with a derived UID.
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn open_chain(
        &self,
        ownership: ChainOwnership,
        application_permissions: ApplicationPermissions,
        balance: Amount,
    ) -> Result<ClientOutcome<(MessageId, Certificate)>, ChainClientError> {
        self.prepare_chain().await?;
        loop {
            let (epoch, committees) = self.epoch_and_committees(self.chain_id).await?;
            let epoch = epoch.ok_or(LocalNodeError::InactiveChain(self.chain_id))?;
            let config = OpenChainConfig {
                ownership: ownership.clone(),
                committees,
                admin_id: self.state().admin_id(),
                epoch,
                balance,
                application_permissions: application_permissions.clone(),
            };
            let operation = Operation::System(SystemOperation::OpenChain(config));
            let certificate = match self.execute_block(vec![operation]).await? {
                ExecuteBlockOutcome::Executed(certificate) => certificate,
                ExecuteBlockOutcome::Conflict(_) => continue,
                ExecuteBlockOutcome::WaitForTimeout(timeout) => {
                    return Ok(ClientOutcome::WaitForTimeout(timeout));
                }
            };
            // The first message of the only operation created the new chain.
            let message_id = certificate
                .value()
                .executed_block()
                .and_then(|executed_block| {
                    executed_block.message_id_for_operation(0, OPEN_CHAIN_MESSAGE_INDEX)
                })
                .ok_or_else(|| ChainClientError::InternalError("Failed to create new chain"))?;
            // Add the new chain to the list of tracked chains
            self.client.track_chain(ChainId::child(message_id));
            self.client
                .local_node
                .retry_pending_cross_chain_requests(self.chain_id)
                .await?;
            return Ok(ClientOutcome::Committed((message_id, certificate)));
        }
    }

    /// Closes the chain (and loses everything in it!!).
    #[tracing::instrument(level = "trace")]
    pub async fn close_chain(&self) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::CloseChain))
            .await
    }

    /// Publishes some bytecode.
    #[cfg(not(target_arch = "wasm32"))]
    #[tracing::instrument(level = "trace", skip(contract, service))]
    pub async fn publish_bytecode(
        &self,
        contract: Bytecode,
        service: Bytecode,
    ) -> Result<ClientOutcome<(BytecodeId, Certificate)>, ChainClientError> {
        let (compressed_contract, compressed_service) =
            tokio::task::spawn_blocking(move || (contract.compress(), service.compress()))
                .await
                .expect("Compression should not panic");
        let contract_blob = Blob::new_contract_bytecode(compressed_contract);
        let service_blob = Blob::new_service_bytecode(compressed_service);

        let bytecode_id = BytecodeId::new(contract_blob.id().hash, service_blob.id().hash);
        self.add_pending_blobs([contract_blob, service_blob]).await;
        self.execute_operation(Operation::System(SystemOperation::PublishBytecode {
            bytecode_id,
        }))
        .await?
        .try_map(|certificate| Ok((bytecode_id, certificate)))
    }

    /// Publishes some data blobs.
    #[tracing::instrument(level = "trace", skip(bytes))]
    pub async fn publish_data_blobs(
        &self,
        bytes: Vec<Vec<u8>>,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        let blobs = bytes.into_iter().map(Blob::new_data);
        let publish_blob_operations = blobs
            .clone()
            .map(|blob| {
                Operation::System(SystemOperation::PublishDataBlob {
                    blob_hash: blob.id().hash,
                })
            })
            .collect();
        self.add_pending_blobs(blobs).await;
        self.execute_operations(publish_blob_operations).await
    }

    /// Publishes some data blob.
    #[tracing::instrument(level = "trace", skip(bytes))]
    pub async fn publish_data_blob(
        &self,
        bytes: Vec<u8>,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.publish_data_blobs(vec![bytes]).await
    }

    /// Adds pending blobs
    pub async fn add_pending_blobs(&self, pending_blobs: impl IntoIterator<Item = Blob>) {
        for blob in pending_blobs {
            self.client.local_node.cache_recent_blob(&blob).await;
            self.state_mut().insert_pending_blob(blob);
        }
    }

    /// Creates an application by instantiating some bytecode.
    #[tracing::instrument(
        level = "trace",
        skip(self, parameters, instantiation_argument, required_application_ids)
    )]
    pub async fn create_application<
        A: Abi,
        Parameters: Serialize,
        InstantiationArgument: Serialize,
    >(
        &self,
        bytecode_id: BytecodeId<A, Parameters, InstantiationArgument>,
        parameters: &Parameters,
        instantiation_argument: &InstantiationArgument,
        required_application_ids: Vec<UserApplicationId>,
    ) -> Result<ClientOutcome<(UserApplicationId<A>, Certificate)>, ChainClientError> {
        let instantiation_argument = serde_json::to_vec(instantiation_argument)?;
        let parameters = serde_json::to_vec(parameters)?;
        Ok(self
            .create_application_untyped(
                bytecode_id.forget_abi(),
                parameters,
                instantiation_argument,
                required_application_ids,
            )
            .await?
            .map(|(app_id, cert)| (app_id.with_abi(), cert)))
    }

    /// Creates an application by instantiating some bytecode.
    #[tracing::instrument(
        level = "trace",
        skip(
            self,
            bytecode_id,
            parameters,
            instantiation_argument,
            required_application_ids
        )
    )]
    pub async fn create_application_untyped(
        &self,
        bytecode_id: BytecodeId,
        parameters: Vec<u8>,
        instantiation_argument: Vec<u8>,
        required_application_ids: Vec<UserApplicationId>,
    ) -> Result<ClientOutcome<(UserApplicationId, Certificate)>, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::CreateApplication {
            bytecode_id,
            parameters,
            instantiation_argument,
            required_application_ids,
        }))
        .await?
        .try_map(|certificate| {
            // The first message of the only operation created the application.
            let creation = certificate
                .value()
                .executed_block()
                .and_then(|executed_block| {
                    executed_block.message_id_for_operation(0, CREATE_APPLICATION_MESSAGE_INDEX)
                })
                .ok_or_else(|| ChainClientError::InternalError("Failed to create application"))?;
            let id = ApplicationId {
                creation,
                bytecode_id,
            };
            Ok((id, certificate))
        })
    }

    /// Creates a new committee and starts using it (admin chains only).
    #[tracing::instrument(level = "trace", skip(committee))]
    pub async fn stage_new_committee(
        &self,
        committee: Committee,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        loop {
            self.prepare_chain().await?;
            let epoch = self.epoch().await?;
            match self
                .execute_block(vec![Operation::System(SystemOperation::Admin(
                    AdminOperation::CreateCommittee {
                        epoch: epoch.try_add_one()?,
                        committee: committee.clone(),
                    },
                ))])
                .await?
            {
                ExecuteBlockOutcome::Executed(certificate) => {
                    return Ok(ClientOutcome::Committed(certificate))
                }
                ExecuteBlockOutcome::Conflict(_) => continue,
                ExecuteBlockOutcome::WaitForTimeout(timeout) => {
                    return Ok(ClientOutcome::WaitForTimeout(timeout));
                }
            };
        }
    }

    /// Synchronizes the chain with the validators and creates blocks without any operations to
    /// process all incoming messages. This may require several blocks.
    ///
    /// If not all certificates could be processed due to a timeout, the timestamp for when to retry
    /// is returned, too.
    #[tracing::instrument(level = "trace")]
    pub async fn process_inbox(
        &self,
    ) -> Result<(Vec<Certificate>, Option<RoundTimeout>), ChainClientError> {
        self.prepare_chain().await?;
        self.process_inbox_without_prepare().await
    }

    /// Creates blocks without any operations to process all incoming messages. This may require
    /// several blocks.
    ///
    /// If not all certificates could be processed due to a timeout, the timestamp for when to retry
    /// is returned, too.
    #[tracing::instrument(level = "trace")]
    pub async fn process_inbox_without_prepare(
        &self,
    ) -> Result<(Vec<Certificate>, Option<RoundTimeout>), ChainClientError> {
        #[cfg(with_metrics)]
        let _latency = metrics::PROCESS_INBOX_WITHOUT_PREPARE_LATENCY.measure_latency();

        let mut certificates = Vec::new();
        loop {
            let incoming_bundles = self.pending_message_bundles().await?;
            if incoming_bundles.is_empty() {
                return Ok((certificates, None));
            }
            match self.execute_block(vec![]).await {
                Ok(ExecuteBlockOutcome::Executed(certificate))
                | Ok(ExecuteBlockOutcome::Conflict(certificate)) => certificates.push(certificate),
                Ok(ExecuteBlockOutcome::WaitForTimeout(timeout)) => {
                    return Ok((certificates, Some(timeout)));
                }
                Err(error) => return Err(error),
            };
        }
    }

    /// Starts listening to the admin chain for new committees. (This is only useful for
    /// other genesis chains or for testing.)
    #[tracing::instrument(level = "trace")]
    pub async fn subscribe_to_new_committees(
        &self,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        let operation = SystemOperation::Subscribe {
            chain_id: self.state().admin_id(),
            channel: SystemChannel::Admin,
        };
        self.execute_operation(Operation::System(operation)).await
    }

    /// Stops listening to the admin chain for new committees. (This is only useful for
    /// testing.)
    #[tracing::instrument(level = "trace")]
    pub async fn unsubscribe_from_new_committees(
        &self,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        let operation = SystemOperation::Unsubscribe {
            chain_id: self.state().admin_id(),
            channel: SystemChannel::Admin,
        };
        self.execute_operation(Operation::System(operation)).await
    }

    /// Deprecates all the configurations of voting rights but the last one (admin chains
    /// only). Currently, each individual chain is still entitled to wait before accepting
    /// this command. However, it is expected that deprecated validators stop functioning
    /// shortly after such command is issued.
    #[tracing::instrument(level = "trace")]
    pub async fn finalize_committee(&self) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.prepare_chain().await?;
        let (current_epoch, committees) = self.epoch_and_committees(self.chain_id).await?;
        let current_epoch = current_epoch.ok_or(LocalNodeError::InactiveChain(self.chain_id))?;
        let operations = committees
            .keys()
            .filter_map(|epoch| {
                if *epoch != current_epoch {
                    Some(Operation::System(SystemOperation::Admin(
                        AdminOperation::RemoveCommittee { epoch: *epoch },
                    )))
                } else {
                    None
                }
            })
            .collect();
        self.execute_without_prepare(operations).await
    }

    /// Sends money to a chain.
    /// Do not check balance. (This may block the client)
    /// Do not confirm the transaction.
    #[tracing::instrument(level = "trace")]
    pub async fn transfer_to_account_unsafe_unconfirmed(
        &self,
        owner: Option<Owner>,
        amount: Amount,
        account: Account,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::Transfer {
            owner,
            recipient: Recipient::Account(account),
            amount,
        }))
        .await
    }

    #[tracing::instrument(level = "trace", skip(hash))]
    pub async fn read_hashed_certificate_value(
        &self,
        hash: CryptoHash,
    ) -> Result<HashedCertificateValue, ViewError> {
        self.client
            .storage_client()
            .read_hashed_certificate_value(hash)
            .await
    }

    /// Handles any cross-chain requests for any pending outgoing messages.
    #[tracing::instrument(level = "trace")]
    pub async fn retry_pending_outgoing_messages(&self) -> Result<(), ChainClientError> {
        self.client
            .local_node
            .retry_pending_cross_chain_requests(self.chain_id)
            .await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(from, limit))]
    pub async fn read_hashed_certificate_values_downward(
        &self,
        from: CryptoHash,
        limit: u32,
    ) -> Result<Vec<HashedCertificateValue>, ViewError> {
        self.client
            .storage_client()
            .read_hashed_certificate_values_downward(from, limit)
            .await
    }

    #[tracing::instrument(level = "trace", skip(local_node))]
    async fn local_chain_info(
        &self,
        chain_id: ChainId,
        local_node: &mut LocalNodeClient<S>,
    ) -> Option<Box<ChainInfo>> {
        let Ok(info) = local_node.local_chain_info(chain_id).await else {
            error!("Fail to read local chain info for {chain_id}");
            return None;
        };
        // Useful in case `chain_id` is the same as the local chain.
        self.update_from_info(&info);
        Some(info)
    }

    #[tracing::instrument(level = "trace", skip(chain_id, local_node))]
    async fn local_next_block_height(
        &self,
        chain_id: ChainId,
        local_node: &mut LocalNodeClient<S>,
    ) -> Option<BlockHeight> {
        let info = self.local_chain_info(chain_id, local_node).await?;
        Some(info.next_block_height)
    }

    #[tracing::instrument(level = "trace", skip(remote_node, local_node, notification))]
    async fn process_notification(
        &self,
        remote_node: RemoteNode<P::Node>,
        mut local_node: LocalNodeClient<S>,
        notification: Notification,
    ) {
        match notification.reason {
            Reason::NewIncomingBundle { origin, height } => {
                if self
                    .local_next_block_height(origin.sender, &mut local_node)
                    .await
                    > Some(height)
                {
                    debug!("Accepting redundant notification for new message");
                    return;
                }
                if let Err(error) = self
                    .find_received_certificates_from_validator(remote_node)
                    .await
                {
                    error!("Fail to process notification: {error}");
                    return;
                }
                if self
                    .local_next_block_height(origin.sender, &mut local_node)
                    .await
                    <= Some(height)
                {
                    error!("Fail to synchronize new message after notification");
                }
            }
            Reason::NewBlock { height, .. } => {
                let chain_id = notification.chain_id;
                if self
                    .local_next_block_height(chain_id, &mut local_node)
                    .await
                    > Some(height)
                {
                    debug!("Accepting redundant notification for new block");
                    return;
                }
                if let Err(error) = self
                    .try_synchronize_chain_state_from(&remote_node, chain_id)
                    .await
                {
                    error!("Fail to process notification: {error}");
                    return;
                }
                let local_height = self
                    .local_next_block_height(chain_id, &mut local_node)
                    .await;
                if local_height <= Some(height) {
                    error!("Fail to synchronize new block after notification");
                }
            }
            Reason::NewRound { height, round } => {
                let chain_id = notification.chain_id;
                if let Some(info) = self.local_chain_info(chain_id, &mut local_node).await {
                    if (info.next_block_height, info.manager.current_round) >= (height, round) {
                        debug!("Accepting redundant notification for new round");
                        return;
                    }
                }
                if let Err(error) = self
                    .try_synchronize_chain_state_from(&remote_node, chain_id)
                    .await
                {
                    error!("Fail to process notification: {error}");
                    return;
                }
                let Some(info) = self.local_chain_info(chain_id, &mut local_node).await else {
                    error!("Fail to read local chain info for {chain_id}");
                    return;
                };
                if (info.next_block_height, info.manager.current_round) < (height, round) {
                    error!("Fail to synchronize new block after notification");
                }
            }
        }
    }

    /// Spawns a task that listens to notifications about the current chain from all validators,
    /// and synchronizes the local state accordingly.
    #[tracing::instrument(level = "trace", fields(chain_id = ?self.chain_id))]
    pub async fn listen(
        &self,
    ) -> Result<(impl Future<Output = ()>, AbortOnDrop, NotificationStream), ChainClientError> {
        use future::FutureExt as _;

        async fn await_while_polling<F: FusedFuture>(
            future: F,
            background_work: impl FusedStream<Item = ()>,
        ) -> F::Output {
            tokio::pin!(future);
            tokio::pin!(background_work);
            loop {
                futures::select! {
                    _ = background_work.next() => (),
                    result = future => return result,
                }
            }
        }

        let mut senders = HashMap::new(); // Senders to cancel notification streams.
        let notifications = self.subscribe().await?;
        let (abortable_notifications, abort) = stream::abortable(self.subscribe().await?);
        if let Err(error) = self.synchronize_from_validators().await {
            error!("Failed to synchronize from validators: {}", error);
        }

        // Beware: if this future ceases to make progress, notification processing will
        // deadlock, because of the issue described in
        // https://github.com/linera-io/linera-protocol/pull/1173.

        // TODO(#2013): replace this lock with an asychronous communication channel

        let mut process_notifications = FuturesUnordered::new();

        match self.update_streams(&mut senders).await {
            Ok(handler) => process_notifications.push(handler),
            Err(error) => error!("Failed to update committee: {error}"),
        };

        let this = self.clone();
        let update_streams = async move {
            let mut abortable_notifications = abortable_notifications.fuse();

            while let Some(notification) =
                await_while_polling(abortable_notifications.next(), &mut process_notifications)
                    .await
            {
                if let Reason::NewBlock { .. } = notification.reason {
                    match await_while_polling(
                        this.update_streams(&mut senders).fuse(),
                        &mut process_notifications,
                    )
                    .await
                    {
                        Ok(handler) => process_notifications.push(handler),
                        Err(error) => error!("Failed to update comittee: {error}"),
                    }
                }
            }

            for abort in senders.into_values() {
                abort.abort();
            }

            let () = process_notifications.collect().await;
        }
        .in_current_span();

        Ok((update_streams, AbortOnDrop(abort), notifications))
    }

    #[tracing::instrument(level = "trace", skip(senders))]
    async fn update_streams(
        &self,
        senders: &mut HashMap<ValidatorName, AbortHandle>,
    ) -> Result<impl Future<Output = ()>, ChainClientError> {
        let (chain_id, nodes, local_node) = {
            let committee = self.local_committee().await?;
            let nodes: HashMap<_, _> = self
                .client
                .validator_node_provider
                .make_nodes(&committee)?
                .collect();
            (self.chain_id, nodes, self.client.local_node.clone())
        };
        // Drop removed validators.
        senders.retain(|name, abort| {
            if !nodes.contains_key(name) {
                abort.abort();
            }
            !abort.is_aborted()
        });
        // Add tasks for new validators.
        let validator_tasks = FuturesUnordered::new();
        for (name, node) in nodes {
            let hash_map::Entry::Vacant(entry) = senders.entry(name) else {
                continue;
            };
            let (mut stream, abort) = match node.subscribe(vec![chain_id]).await {
                Err(error) => {
                    info!(?error, "Could not connect to validator {name}");
                    continue;
                }
                Ok(stream) => stream::abortable(stream),
            };
            let this = self.clone();
            let local_node = local_node.clone();
            let remote_node = RemoteNode { name, node };
            validator_tasks.push(async move {
                while let Some(notification) = stream.next().await {
                    this.process_notification(
                        remote_node.clone(),
                        local_node.clone(),
                        notification,
                    )
                    .await;
                }
            });
            entry.insert(abort);
        }
        Ok(validator_tasks.collect())
    }

    /// Attempts to download new received certificates from a particular validator.
    ///
    /// This is similar to `find_received_certificates` but for only one validator.
    /// We also don't try to synchronize the admin chain.
    #[tracing::instrument(level = "trace")]
    async fn find_received_certificates_from_validator(
        &self,
        remote_node: RemoteNode<P::Node>,
    ) -> Result<(), ChainClientError> {
        let mutex = self.state().client_mutex();
        let _guard = mutex.lock_owned().await;

        let chain_id = self.chain_id;
        // Proceed to downloading received certificates.
        let (name, tracker, certificates) = self
            .synchronize_received_certificates_from_validator(chain_id, &remote_node)
            .await?;

        drop(_guard);
        // Process received certificates. If the client state has changed during the
        // network calls, we should still be fine.
        self.receive_certificates_from_validator(name, tracker, certificates)
            .await;
        Ok(())
    }
}

/// The outcome of trying to commit a list of incoming messages and operations to the chain.
#[derive(Debug)]
enum ExecuteBlockOutcome {
    /// A block with the messages and operations was committed.
    Executed(Certificate),
    /// A different block was already proposed and got committed. Check whether the messages and
    /// operations are still suitable, and try again at the next block height.
    Conflict(Certificate),
    /// We are not the round leader and cannot do anything. Try again at the specified time or
    /// or whenever the round or block height changes.
    WaitForTimeout(RoundTimeout),
}

/// Wrapper for `AbortHandle` that aborts when its dropped.
#[must_use]
pub struct AbortOnDrop(AbortHandle);

impl Drop for AbortOnDrop {
    #[tracing::instrument(level = "trace", skip(self))]
    fn drop(&mut self) {
        self.0.abort();
    }
}

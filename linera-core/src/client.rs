// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{hash_map, BTreeMap, HashMap},
    convert::Infallible,
    iter,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use dashmap::{
    mapref::one::{MappedRef as DashMapMappedRef, Ref as DashMapRef, RefMut as DashMapRefMut},
    DashMap,
};
use futures::{
    future::{self, FusedFuture, Future},
    stream::{self, AbortHandle, FusedStream, FuturesUnordered, StreamExt},
};
use linera_base::{
    abi::Abi,
    crypto::{CryptoHash, KeyPair, PublicKey},
    data_types::{
        Amount, ApplicationPermissions, ArithmeticError, BlockHeight, HashedBlob, Round, Timestamp,
    },
    ensure,
    identifiers::{Account, ApplicationId, BlobId, BytecodeId, ChainId, MessageId, Owner},
    ownership::{ChainOwnership, TimeoutConfig},
};
use linera_chain::{
    data_types::{
        Block, BlockProposal, Certificate, CertificateValue, ExecutedBlock, HashedCertificateValue,
        IncomingMessage, LiteCertificate, LiteVote, MessageAction,
    },
    manager::ChainManagerInfo,
    ChainError, ChainExecutionContext, ChainStateView,
};
use linera_execution::{
    committee::{Committee, Epoch, ValidatorName},
    system::{
        AdminOperation, OpenChainConfig, Recipient, SystemChannel, SystemOperation, UserData,
        CREATE_APPLICATION_MESSAGE_INDEX, OPEN_CHAIN_MESSAGE_INDEX, PUBLISH_BYTECODE_MESSAGE_INDEX,
    },
    Bytecode, ExecutionError, Message, Operation, Query, Response, SystemExecutionError,
    SystemMessage, SystemQuery, SystemResponse, UserApplicationId,
};
use linera_storage::Storage;
use linera_views::views::ViewError;
use serde::Serialize;
use thiserror::Error;
use tokio::sync::{Mutex, OwnedRwLockReadGuard};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, error, info, warn, Instrument as _};

use crate::{
    data_types::{
        BlockHeightRange, ChainInfo, ChainInfoQuery, ChainInfoResponse, ClientOutcome, RoundTimeout,
    },
    local_node::{LocalNodeClient, LocalNodeError},
    node::{
        CrossChainMessageDelivery, LocalValidatorNode, LocalValidatorNodeProvider, NodeError,
        NotificationStream,
    },
    notifier::Notifier,
    updater::{communicate_with_quorum, CommunicateAction, CommunicationError, ValidatorUpdater},
    worker::{Notification, Reason, WorkerError, WorkerState},
};

#[cfg(test)]
#[path = "unit_tests/client_tests.rs"]
mod client_tests;

/// A builder that creates `ChainClients` which share the cache and notifiers.
pub struct Client<ValidatorNodeProvider, Storage>
where
    Storage: linera_storage::Storage,
    ViewError: From<Storage::StoreError>,
{
    /// How to talk to the validators.
    validator_node_provider: ValidatorNodeProvider,
    /// Local node to manage the execution state and the local storage of the chains that we are
    /// tracking.
    local_node: LocalNodeClient<Storage>,
    /// Maximum number of pending messages processed at a time in a block.
    max_pending_messages: usize,
    /// The policy for automatically handling incoming messages.
    message_policy: MessagePolicy,
    /// Whether to block on cross-chain message delivery.
    cross_chain_message_delivery: CrossChainMessageDelivery,
    /// References to clients waiting for chain notifications.
    notifier: Arc<Notifier<Notification>>,
    /// A copy of the storage client so that we don't have to lock the local node client
    /// to retrieve it.
    storage: Storage,
    /// Chain state for the managed chains.
    chains: DashMap<ChainId, ChainState>,
}

impl<P, S: Storage + Clone> Client<P, S>
where
    ViewError: From<S::StoreError>,
{
    #[tracing::instrument(level = "trace", skip_all)]
    /// Creates a new `Client` with a new cache and notifiers.
    pub fn new(
        validator_node_provider: P,
        storage: S,
        max_pending_messages: usize,
        cross_chain_message_delivery: CrossChainMessageDelivery,
    ) -> Self {
        let state = WorkerState::new_for_client("Client node".to_string(), storage.clone())
            .with_allow_inactive_chains(true)
            .with_allow_messages_from_deprecated_epochs(true);
        let local_node = LocalNodeClient::new(state);

        Self {
            validator_node_provider,
            local_node,
            chains: DashMap::new(),
            max_pending_messages,
            message_policy: MessagePolicy::Accept,
            cross_chain_message_delivery,
            notifier: Arc::new(Notifier::default()),
            storage,
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    /// Returns the storage client used by this client's local node.
    pub fn storage_client(&self) -> &S {
        &self.storage
    }

    #[tracing::instrument(level = "trace", skip(self))]
    /// Returns a reference to the [`LocalNodeClient`] of the client.
    pub fn local_node(&self) -> &LocalNodeClient<S> {
        &self.local_node
    }

    #[tracing::instrument(level = "trace", skip_all, fields(chain_id, next_block_height))]
    /// Creates a new `ChainClient`.
    #[allow(clippy::too_many_arguments)]
    pub fn create_chain_client(
        self: &Arc<Self>,
        chain_id: ChainId,
        known_key_pairs: Vec<KeyPair>,
        admin_id: ChainId,
        block_hash: Option<CryptoHash>,
        timestamp: Timestamp,
        next_block_height: BlockHeight,
        pending_block: Option<Block>,
        pending_blobs: BTreeMap<BlobId, HashedBlob>,
    ) -> ChainClient<P, S>
    where
        ViewError: From<S::StoreError>,
    {
        let known_key_pairs = known_key_pairs
            .into_iter()
            .map(|kp| (Owner::from(kp.public()), kp))
            .collect();

        let dashmap::mapref::entry::Entry::Vacant(e) = self.chains.entry(chain_id) else {
            panic!("Inserting already-existing chain {chain_id}");
        };
        e.insert(ChainState {
            known_key_pairs,
            admin_id,
            block_hash,
            timestamp,
            next_block_height,
            pending_block,
            pending_blobs,
            received_certificate_trackers: HashMap::new(),
            preparing_block: Arc::default(),
        });

        ChainClient {
            client: self.clone(),
            chain_id,
            options: ChainClientOptions {
                max_pending_messages: self.max_pending_messages,
                message_policy: self.message_policy,
                cross_chain_message_delivery: self.cross_chain_message_delivery,
            },
        }
    }
}

/// Policies for automatically handling incoming messages.
///
/// These apply to all messages except for the initial `OpenChain`, which is always accepted.
#[derive(Copy, Clone, Debug, clap::ValueEnum)]
pub enum MessagePolicy {
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
    #[tracing::instrument(level = "trace", skip(self))]
    fn is_ignore(&self) -> bool {
        matches!(self, Self::Ignore)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn is_reject(&self) -> bool {
        matches!(self, Self::Reject)
    }
}

pub struct ChainState {
    /// Latest block hash, if any.
    pub block_hash: Option<CryptoHash>,
    /// The earliest possible timestamp for the next block.
    pub timestamp: Timestamp,
    /// Sequence number that we plan to use for the next block.
    /// We track this value outside local storage mainly for security reasons.
    pub next_block_height: BlockHeight,
    /// Pending block.
    pub pending_block: Option<Block>,
    /// Known key pairs from present and past identities.
    pub known_key_pairs: BTreeMap<Owner, KeyPair>,
    /// The ID of the admin chain.
    pub admin_id: ChainId,

    /// Support synchronization of received certificates.
    pub received_certificate_trackers: HashMap<ValidatorName, u64>,
    /// This contains blobs belonging to our `pending_block` that may not even have
    /// been processed by (i.e. been proposed to) our own local chain manager yet.
    pub pending_blobs: BTreeMap<BlobId, HashedBlob>,

    /// A mutex that is held whilst we are preparing the next block, to ensure that no
    /// other client can begin preparing a block.
    preparing_block: Arc<Mutex<()>>,
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ChainClientOptions {
    /// Maximum number of pending messages processed at a time in a block.
    pub max_pending_messages: usize,
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
    ViewError: From<<Storage as linera_storage::Storage>::StoreError>,
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
    ViewError: From<<S as linera_storage::Storage>::StoreError>,
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
    ViewError: From<<S as linera_storage::Storage>::StoreError>,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter
            .debug_struct("ChainClient")
            .field("chain_id", &self.chain_id)
            .field("options", &self.options)
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

    #[error("Blob not found: {0}")]
    BlobNotFound(BlobId),

    #[error("Got invalid last used by certificate for blob {0} from validator {1}")]
    InvalidLastUsedByCertificate(BlobId, ValidatorName),
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

impl<P, S> ChainClient<P, S>
where
    S: Storage,
    ViewError: From<S::StoreError>,
{
    #[tracing::instrument(level = "trace", skip(self))]
    /// Gets a shared reference to the chain's state.
    pub fn state(&self) -> ChainGuard<ChainState> {
        Unsend::new(
            self.client
                .chains
                .get(&self.chain_id)
                .expect("Chain client constructed for invalid chain"),
        )
    }

    #[tracing::instrument(level = "trace", skip(self))]
    /// Gets a mutable reference to the state.
    /// Beware: this will block any other reference to any chain's state!
    fn state_mut(&self) -> ChainGuardMut<ChainState> {
        Unsend::new(
            self.client
                .chains
                .get_mut(&self.chain_id)
                .expect("Chain client constructed for invalid chain"),
        )
    }

    #[tracing::instrument(level = "trace", skip(self))]
    /// Gets the per-`ChainClient` options.
    pub fn options_mut(&mut self) -> &mut ChainClientOptions {
        &mut self.options
    }

    #[tracing::instrument(level = "trace", skip(self))]
    /// Gets the ID of the associated chain.
    pub fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    #[tracing::instrument(level = "trace", skip(self))]
    /// Gets the hash of the latest known block.
    pub fn block_hash(&self) -> Option<CryptoHash> {
        self.state().block_hash
    }

    #[tracing::instrument(level = "trace", skip(self))]
    /// Gets the earliest possible timestamp for the next block.
    pub fn timestamp(&self) -> Timestamp {
        self.state().timestamp
    }

    #[tracing::instrument(level = "trace", skip(self))]
    /// Gets the next block height.
    pub fn next_block_height(&self) -> BlockHeight {
        self.state().next_block_height
    }

    #[tracing::instrument(level = "trace", skip(self))]
    /// Gets a guarded reference to the next pending block.
    pub fn pending_block(&self) -> ChainGuardMapped<Option<Block>> {
        Unsend::new(self.state().inner.map(|state| &state.pending_block))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    /// Gets a guarded reference to the set of pending blobs.
    pub fn pending_blobs(&self) -> ChainGuardMapped<BTreeMap<BlobId, HashedBlob>> {
        Unsend::new(self.state().inner.map(|state| &state.pending_blobs))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    /// Gets the ID of the admin chain.
    pub fn admin_id(&self) -> ChainId {
        self.state().admin_id
    }
}

enum ReceiveCertificateMode {
    NeedsCheck,
    AlreadyChecked,
}

impl<P, S> ChainClient<P, S>
where
    P: LocalValidatorNodeProvider + Sync,
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::StoreError>,
{
    #[tracing::instrument(level = "trace")]
    /// Obtains a `ChainStateView` for a given `ChainId`.
    pub async fn chain_state_view(
        &self,
    ) -> Result<OwnedRwLockReadGuard<ChainStateView<S::Context>>, LocalNodeError> {
        Ok(self
            .client
            .local_node
            .chain_state_view(self.chain_id)
            .await?)
    }

    #[tracing::instrument(level = "trace")]
    /// Subscribes to notifications from this client's chain.
    pub async fn subscribe(&self) -> Result<NotificationStream, LocalNodeError> {
        Ok(Box::pin(UnboundedReceiverStream::new(
            self.client.notifier.subscribe(vec![self.chain_id]),
        )))
    }

    #[tracing::instrument(level = "trace")]
    /// Returns the storage client used by this client's local node.
    pub fn storage_client(&self) -> S {
        self.client.storage_client().clone()
    }

    #[tracing::instrument(level = "trace")]
    /// Obtains the basic `ChainInfo` data for the local chain.
    pub async fn chain_info(&self) -> Result<Box<ChainInfo>, LocalNodeError> {
        let query = ChainInfoQuery::new(self.chain_id);
        let response = self
            .client
            .local_node
            .handle_chain_info_query(query)
            .await?;
        Ok(response.info)
    }

    #[tracing::instrument(level = "trace")]
    /// Obtains the basic `ChainInfo` data for the local chain, with chain manager values.
    pub async fn chain_info_with_manager_values(&self) -> Result<Box<ChainInfo>, LocalNodeError> {
        let query = ChainInfoQuery::new(self.chain_id).with_manager_values();
        let response = self
            .client
            .local_node
            .handle_chain_info_query(query)
            .await?;
        Ok(response.info)
    }

    #[tracing::instrument(level = "trace")]
    /// Obtains up to `self.options.max_pending_messages` pending messages for the local chain.
    ///
    /// Messages known to be redundant are filtered out: A `RegisterApplications` message whose
    /// entries are already known never needs to be included in a block.
    async fn pending_messages(&self) -> Result<Vec<IncomingMessage>, ChainClientError> {
        if self.state().next_block_height != BlockHeight::ZERO
            && self.options.message_policy.is_ignore()
        {
            return Ok(Vec::new()); // OpenChain is already received, other are ignored.
        }
        let query = ChainInfoQuery::new(self.chain_id).with_pending_messages();
        let info = self
            .client
            .local_node
            .handle_chain_info_query(query)
            .await?
            .info;
        ensure!(
            info.next_block_height == self.state().next_block_height,
            ChainClientError::WalletSynchronizationError
        );
        let mut requested_pending_messages = info.requested_pending_messages;
        let mut pending_messages = vec![];
        // The first incoming message of any child chain must be `OpenChain`. We must have it in
        // our inbox, and include it before all other messages.
        if info.next_block_height == BlockHeight::ZERO
            && info
                .description
                .ok_or_else(|| LocalNodeError::InactiveChain(self.chain_id))?
                .is_child()
        {
            let Some(index) = requested_pending_messages.iter().position(|message| {
                matches!(
                    message.event.message,
                    Message::System(SystemMessage::OpenChain(_))
                )
            }) else {
                return Err(LocalNodeError::InactiveChain(self.chain_id).into());
            };
            let open_chain_message = requested_pending_messages.remove(index);
            pending_messages.push(open_chain_message);
        }
        if self.options.message_policy.is_ignore() {
            return Ok(pending_messages); // Ignore messages other than OpenChain.
        }
        for mut message in requested_pending_messages {
            if pending_messages.len() >= self.options.max_pending_messages {
                warn!(
                    "Limiting block to {} incoming messages",
                    self.options.max_pending_messages
                );
                break;
            }
            if self.options.message_policy.is_reject() {
                if message.event.is_skippable() {
                    continue;
                } else if message.event.is_tracked() {
                    message.action = MessageAction::Reject;
                }
            }
            if let Message::System(SystemMessage::RegisterApplications { applications }) =
                &message.event.message
            {
                let chain_id = self.chain_id;
                if applications
                    .iter()
                    .map(|application| {
                        self.client
                            .local_node
                            .describe_application(chain_id, application.into())
                    })
                    .collect::<FuturesUnordered<_>>()
                    .all(|result| async move { result.is_ok() })
                    .await
                {
                    continue; // These applications are already registered; skip register message.
                }
            }
            pending_messages.push(message);
        }
        Ok(pending_messages)
    }

    #[tracing::instrument(level = "trace")]
    /// Obtains the set of committees trusted by the local chain.
    async fn committees(&self) -> Result<BTreeMap<Epoch, Committee>, LocalNodeError> {
        let (_epoch, committees) = self.epoch_and_committees(self.chain_id).await?;
        Ok(committees)
    }

    #[tracing::instrument(level = "trace")]
    /// Obtains the current epoch of the given chain as well as its set of trusted committees.
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

    #[tracing::instrument(level = "trace")]
    /// Obtains the epochs of the committees trusted by the local chain.
    pub async fn epochs(&self) -> Result<Vec<Epoch>, LocalNodeError> {
        let committees = self.committees().await?;
        Ok(committees.into_keys().collect())
    }

    #[tracing::instrument(level = "trace")]
    /// Obtains the committee for the current epoch of the local chain.
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

    #[tracing::instrument(level = "trace")]
    /// Obtains all the committees trusted by either the local chain or its admin chain. Also
    /// return the latest trusted epoch.
    async fn known_committees(
        &self,
    ) -> Result<(BTreeMap<Epoch, Committee>, Epoch), LocalNodeError> {
        let (epoch, mut committees) = self.epoch_and_committees(self.chain_id).await?;
        let (admin_epoch, admin_committees) = self.epoch_and_committees(self.admin_id()).await?;
        committees.extend(admin_committees);
        let epoch = std::cmp::max(epoch.unwrap_or_default(), admin_epoch.unwrap_or_default());
        Ok((committees, epoch))
    }

    #[tracing::instrument(level = "trace")]
    /// Obtains the validators trusted by the local chain.
    async fn validator_nodes(&self) -> Result<Vec<(ValidatorName, P::Node)>, ChainClientError> {
        match self.local_committee().await {
            Ok(committee) => Ok(self
                .client
                .validator_node_provider
                .make_nodes(&committee)?
                .collect()),
            Err(LocalNodeError::InactiveChain(_)) => Ok(Vec::new()),
            Err(LocalNodeError::WorkerError(WorkerError::ChainError(error)))
                if matches!(*error, ChainError::InactiveChain(_)) =>
            {
                Ok(Vec::new())
            }
            Err(e) => Err(e.into()),
        }
    }

    #[tracing::instrument(level = "trace")]
    /// Obtains the current epoch of the local chain.
    async fn epoch(&self) -> Result<Epoch, LocalNodeError> {
        self.chain_info()
            .await?
            .epoch
            .ok_or(LocalNodeError::InactiveChain(self.chain_id))
    }

    #[tracing::instrument(level = "trace")]
    /// Obtains the identity of the current owner of the chain. Returns an error if we have the
    /// private key for more than one identity.
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
            .filter(|owner| self.state().known_key_pairs.contains_key(owner));
        let Some(identity) = identities.next() else {
            return Err(ChainClientError::CannotFindKeyForChain(self.chain_id));
        };
        ensure!(
            identities.all(|id| id == identity),
            ChainClientError::FoundMultipleKeysForChain(self.chain_id)
        );
        Ok(*identity)
    }

    #[tracing::instrument(level = "trace")]
    /// Obtains the key pair associated to the current identity.
    pub async fn key_pair(&self) -> Result<KeyPair, ChainClientError> {
        let id = self.identity().await?;
        Ok(self
            .state()
            .known_key_pairs
            .get(&id)
            .expect("key should be known at this point")
            .copy())
    }

    #[tracing::instrument(level = "trace", skip(notifications))]
    /// Notifies subscribers and clears the `notifications`.
    fn handle_notifications(&self, notifications: &mut Vec<Notification>) {
        self.client.notifier.handle_notifications(notifications);
        notifications.clear();
    }

    #[tracing::instrument(level = "trace")]
    /// Obtains the public key associated to the current identity.
    pub async fn public_key(&self) -> Result<PublicKey, ChainClientError> {
        Ok(self.key_pair().await?.public())
    }

    #[tracing::instrument(level = "trace")]
    /// Prepares the chain for the next operation.
    async fn prepare_chain(&self) -> Result<Box<ChainInfo>, ChainClientError> {
        // Verify that our local storage contains enough history compared to the
        // expected block height. Otherwise, download the missing history from the
        // network.
        let next_block_height = self.state().next_block_height;
        let nodes = self.validator_nodes().await?;
        let mut notifications = vec![];
        let mut info = self
            .client
            .local_node
            .download_certificates(&nodes, self.chain_id, next_block_height, &mut notifications)
            .await?;
        self.handle_notifications(&mut notifications);
        if info.next_block_height == next_block_height {
            // Check that our local node has the expected block hash.
            ensure!(
                self.state().block_hash == info.block_hash,
                ChainClientError::InternalError("Invalid chain of blocks in local node")
            );
        }
        let ownership = &info.manager.ownership;
        let keys: std::collections::HashSet<_> =
            self.state().known_key_pairs.keys().cloned().collect();
        if ownership.all_owners().any(|owner| !keys.contains(owner)) {
            // For chains with any owner other than ourselves, we could be missing recent
            // certificates created by other owners. Further synchronize blocks from the network.
            // This is a best-effort that depends on network conditions.
            let nodes = self.validator_nodes().await?;
            info = self
                .synchronize_chain_state(&nodes, self.chain_id, &mut notifications)
                .await?;
            self.handle_notifications(&mut notifications);
        }
        self.update_from_info(&info);
        Ok(info)
    }

    #[tracing::instrument(level = "trace", skip(committee, certificate))]
    /// Submits a validated block for finalization and returns the confirmed block certificate.
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

    #[tracing::instrument(level = "trace", skip(committee, proposal, value))]
    /// Submits a block proposal to the validators. If it is a slow round, also submits the
    /// validated block for finalization. Returns the confirmed block certificate.
    async fn submit_block_proposal(
        &self,
        committee: &Committee,
        proposal: BlockProposal,
        value: HashedCertificateValue,
    ) -> Result<Certificate, ChainClientError> {
        let submit_action = CommunicateAction::SubmitBlock { proposal };
        let certificate = self
            .communicate_chain_action(committee, submit_action, value)
            .await?;
        self.process_certificate(certificate.clone(), vec![], vec![])
            .await?;
        if certificate.value().is_confirmed() {
            Ok(certificate)
        } else {
            self.finalize_block(committee, certificate).await
        }
    }

    #[tracing::instrument(level = "trace")]
    /// Returns the pending blobs from the local node's chain manager.
    async fn chain_managers_pending_blobs(
        &self,
    ) -> Result<BTreeMap<BlobId, HashedBlob>, LocalNodeError> {
        let chain = self.chain_state_view().await?;
        Ok(chain.manager.get().pending_blobs.clone())
    }

    #[tracing::instrument(level = "trace", skip(committee, delivery))]
    /// Broadcasts certified blocks to validators.
    async fn communicate_chain_updates(
        &self,
        committee: &Committee,
        chain_id: ChainId,
        height: BlockHeight,
        delivery: CrossChainMessageDelivery,
    ) -> Result<(), ChainClientError> {
        let local_node = self.client.local_node.clone();
        let nodes: Vec<_> = self
            .client
            .validator_node_provider
            .make_nodes(committee)?
            .collect();
        communicate_with_quorum(
            &nodes,
            committee,
            |_: &()| (),
            |name, node| {
                let mut updater = ValidatorUpdater {
                    name,
                    node,
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

    #[tracing::instrument(level = "trace", skip(committee, action, value))]
    /// Broadcasts certified blocks and optionally a block proposal, certificate or
    /// leader timeout request.
    ///
    /// In that case, it verifies that the validator votes are for the provided value,
    /// and returns a certificate.
    async fn communicate_chain_action(
        &self,
        committee: &Committee,
        action: CommunicateAction,
        value: HashedCertificateValue,
    ) -> Result<Certificate, ChainClientError> {
        let local_node = self.client.local_node.clone();
        let nodes: Vec<_> = self
            .client
            .validator_node_provider
            .make_nodes(committee)?
            .collect();
        let ((votes_hash, votes_round), votes) = communicate_with_quorum(
            &nodes,
            committee,
            |vote: &LiteVote| (vote.value.value_hash, vote.round),
            |name, node| {
                let mut updater = ValidatorUpdater {
                    name,
                    node,
                    local_node: local_node.clone(),
                };
                let action = action.clone();
                Box::pin(async move { updater.send_chain_update(action).await })
            },
        )
        .await?;
        let round = match action {
            CommunicateAction::SubmitBlock { proposal } => proposal.content.round,
            CommunicateAction::FinalizeBlock { certificate, .. } => certificate.round,
            CommunicateAction::RequestTimeout { round, .. } => round,
        };
        ensure!(
            (votes_hash, votes_round) == (value.hash(), round),
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

    #[tracing::instrument(level = "trace", skip(certificate, mode))]
    /// Processes the confirmed block certificate and its ancestors in the local node, then
    /// updates the validators up to that certificate.
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

    #[tracing::instrument(level = "trace", skip(certificate, mode))]
    /// Processes the confirmed block certificate in the local node. Also downloads and processes
    /// all ancestors that are still missing.
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
        let nodes: Vec<_> = self
            .client
            .validator_node_provider
            .make_nodes(remote_committee)?
            .collect();
        let mut notifications = vec![];
        self.client
            .local_node
            .download_certificates(&nodes, block.chain_id, block.height, &mut notifications)
            .await?;
        self.handle_notifications(&mut notifications);
        // Process the received operations. Download required hashed certificate values if necessary.
        if let Err(err) = self
            .process_certificate(certificate.clone(), vec![], vec![])
            .await
        {
            match &err {
                LocalNodeError::WorkerError(WorkerError::ApplicationBytecodesOrBlobsNotFound(
                    locations,
                    blob_ids,
                )) => {
                    let values =
                        LocalNodeClient::<S>::download_hashed_certificate_values(locations, &nodes)
                            .await;
                    let blobs = LocalNodeClient::<S>::download_blobs(blob_ids, &nodes).await;

                    ensure!(
                        blobs.len() == blob_ids.len() && values.len() == locations.len(),
                        err
                    );
                    self.process_certificate(certificate.clone(), values, blobs)
                        .await?;
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

    #[tracing::instrument(
        level = "trace",
        skip(tracker, committees, max_epoch, node, node_client)
    )]
    /// Downloads and processes all confirmed block certificates that sent any message to this
    /// chain, including their ancestors.
    async fn synchronize_received_certificates_from_validator(
        chain_id: ChainId,
        name: ValidatorName,
        tracker: u64,
        committees: BTreeMap<Epoch, Committee>,
        max_epoch: Epoch,
        node: impl LocalValidatorNode,
        node_client: LocalNodeClient<S>,
    ) -> Result<(ValidatorName, u64, Vec<Certificate>), NodeError> {
        // Retrieve newly received certificates from this validator.
        let query = ChainInfoQuery::new(chain_id).with_received_log_excluding_first_nth(tracker);
        let response = node.handle_chain_info_query(query).await?;
        // Responses are authenticated for accountability.
        response.check(&name)?;
        let mut certificates = Vec::new();
        let mut new_tracker = tracker;
        for entry in response.info.requested_received_log {
            let query = ChainInfoQuery::new(entry.chain_id)
                .with_sent_certificate_hashes_in_range(BlockHeightRange::single(entry.height));
            let local_response = node_client
                .handle_chain_info_query(query.clone())
                .await
                .map_err(|error| NodeError::LocalNodeQuery {
                    error: error.to_string(),
                })?;
            if !local_response
                .info
                .requested_sent_certificate_hashes
                .is_empty()
            {
                new_tracker += 1;
                continue;
            }

            let mut response = node.handle_chain_info_query(query).await?;
            let Some(certificate_hash) = response.info.requested_sent_certificate_hashes.pop()
            else {
                break;
            };

            let certificate = node.download_certificate(certificate_hash).await?;
            let CertificateValue::ConfirmedBlock { executed_block, .. } = certificate.value()
            else {
                return Err(NodeError::InvalidChainInfoResponse);
            };
            let block = &executed_block.block;
            // Check that certificates are valid w.r.t one of our trusted committees.
            if block.epoch > max_epoch {
                // We don't accept a certificate from a committee in the future.
                warn!(
                    "Postponing received certificate from future epoch {:?}",
                    block.epoch
                );
                // Stop the synchronization here. Do not increment the tracker further so
                // that this certificate can still be downloaded later, once our committee
                // is updated.
                break;
            }
            match committees.get(&block.epoch) {
                Some(committee) => {
                    // This epoch is recognized by our chain. Let's verify the
                    // certificate.
                    certificate.check(committee)?;
                    certificates.push(certificate);
                    new_tracker += 1;
                }
                None => {
                    // This epoch is not recognized any more. Let's skip the certificate.
                    // If a higher block with a recognized epoch comes up later from the
                    // same chain, the call to `receive_certificate` below will download
                    // the skipped certificate again.
                    warn!(
                        "Skipping received certificate from past epoch {:?}",
                        block.epoch
                    );
                    new_tracker += 1;
                }
            }
        }
        Ok((name, new_tracker, certificates))
    }

    #[tracing::instrument(level = "trace", skip(tracker, certificates))]
    /// Processes the result of [`synchronize_received_certificates_from_validator`].
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
            .received_certificate_trackers
            .entry(name)
            .and_modify(|t| {
                // Because several synchronizations could happen in parallel, we need to make
                // sure to never go backward.
                if tracker > *t {
                    *t = tracker;
                }
            })
            .or_insert(tracker);
    }

    #[tracing::instrument(level = "trace")]
    /// Attempts to download new received certificates.
    ///
    /// This is a best effort: it will only find certificates that have been confirmed
    /// amongst sufficiently many validators of the current committee of the target
    /// chain.
    ///
    /// However, this should be the case whenever a sender's chain is still in use and
    /// is regularly upgraded to new committees.
    async fn find_received_certificates(&self) -> Result<(), ChainClientError> {
        // Use network information from the local chain.
        let chain_id = self.chain_id;
        let local_committee = self.local_committee().await?;
        let nodes: Vec<_> = self
            .client
            .validator_node_provider
            .make_nodes(&local_committee)?
            .collect();
        let mut notifications = vec![];
        // Synchronize the state of the admin chain from the network.
        self.synchronize_chain_state(&nodes, self.admin_id(), &mut notifications)
            .await?;
        self.handle_notifications(&mut notifications);
        let node_client = self.client.local_node.clone();
        // Now we should have a complete view of all committees in the system.
        let (committees, max_epoch) = self.known_committees().await?;
        // Proceed to downloading received certificates.
        let result = communicate_with_quorum(
            &nodes,
            &local_committee,
            |_| (),
            |name, node| {
                let tracker = self
                    .state()
                    .received_certificate_trackers
                    .get(&name)
                    .copied()
                    .unwrap_or(0);
                let committees = committees.clone();
                let node_client = node_client.clone();
                Box::pin(Self::synchronize_received_certificates_from_validator(
                    chain_id,
                    name,
                    tracker,
                    committees,
                    max_epoch,
                    node,
                    node_client,
                ))
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

    #[tracing::instrument(level = "trace", skip(user_data))]
    /// Sends money.
    pub async fn transfer(
        &self,
        owner: Option<Owner>,
        amount: Amount,
        recipient: Recipient,
        user_data: UserData,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        // TODO(#467): check the balance of `owner` before signing any block proposal.
        self.execute_operation(Operation::System(SystemOperation::Transfer {
            owner,
            recipient,
            amount,
            user_data,
        }))
        .await
    }

    #[tracing::instrument(level = "trace", skip(user_data))]
    /// Claims money in a remote chain.
    pub async fn claim(
        &self,
        owner: Owner,
        target_id: ChainId,
        recipient: Recipient,
        amount: Amount,
        user_data: UserData,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::Claim {
            owner,
            target_id,
            recipient,
            amount,
            user_data,
        }))
        .await
    }

    #[tracing::instrument(level = "trace", skip(certificate))]
    /// Handles the certificate in the local node and the resulting notifications.
    async fn process_certificate(
        &self,
        certificate: Certificate,
        hashed_certificate_values: Vec<HashedCertificateValue>,
        hashed_blobs: Vec<HashedBlob>,
    ) -> Result<(), LocalNodeError> {
        let mut notifications = vec![];
        let info = self
            .client
            .local_node
            .handle_certificate(
                certificate,
                hashed_certificate_values,
                hashed_blobs,
                &mut notifications,
            )
            .await?
            .info;
        self.handle_notifications(&mut notifications);
        self.update_from_info(&info);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(info))]
    /// Updates the latest block and next block height and round information from the chain info.
    fn update_from_info(&self, info: &ChainInfo) {
        if info.chain_id == self.chain_id {
            let mut state = self.state_mut();
            if info.next_block_height > state.next_block_height {
                state.next_block_height = info.next_block_height;
                state.block_hash = info.block_hash;
                state.timestamp = info.timestamp;
            }
        }
    }

    #[tracing::instrument(level = "trace")]
    /// Requests a leader timeout vote from all validators. If a quorum signs it, creates a
    /// certificate and sends it to all validators, to make them enter the next round.
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
        self.process_certificate(certificate.clone(), vec![], vec![])
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

    #[tracing::instrument(level = "trace", skip_all)]
    /// Downloads and processes any certificates we are missing for the given chain.
    pub async fn synchronize_chain_state(
        &self,
        validators: &[(ValidatorName, impl LocalValidatorNode)],
        chain_id: ChainId,
        notifications: &mut impl Extend<Notification>,
    ) -> Result<Box<ChainInfo>, ChainClientError> {
        let mut futures = vec![];

        for (name, node) in validators {
            let client = self.clone();
            let mut notifications = vec![];
            futures.push(async move {
                (
                    client
                        .try_synchronize_chain_state_from(name, node, chain_id, &mut notifications)
                        .await,
                    notifications,
                )
            });
        }

        for (result, new_notifications) in future::join_all(futures).await {
            if let Err(e) = result {
                error!(?e, "Error synchronizing chain state");
            }

            notifications.extend(new_notifications);
        }

        self.client
            .local_node
            .local_chain_info(chain_id)
            .await
            .map_err(Into::into)
    }

    #[tracing::instrument(level = "trace", skip(self, name, node, chain_id, notifications))]
    /// Downloads any certificates from the specified validator that we are missing for the given
    /// chain, and processes them.
    pub async fn try_synchronize_chain_state_from(
        &self,
        name: &ValidatorName,
        node: &impl LocalValidatorNode,
        chain_id: ChainId,
        notifications: &mut impl Extend<Notification>,
    ) -> Result<(), ChainClientError> {
        let local_info = self.client.local_node.local_chain_info(chain_id).await?;
        let range = BlockHeightRange {
            start: local_info.next_block_height,
            limit: None,
        };
        let query = ChainInfoQuery::new(chain_id)
            .with_sent_certificate_hashes_in_range(range)
            .with_manager_values();
        let info = match node.handle_chain_info_query(query).await {
            Ok(response) if response.check(name).is_ok() => response.info,
            Ok(_) => {
                warn!("Ignoring invalid response from validator {}", name);
                // Give up on this validator.
                return Ok(());
            }
            Err(err) => {
                warn!("Ignoring error from validator {}: {}", name, err);
                return Ok(());
            }
        };

        let certificates = future::try_join_all(
            info.requested_sent_certificate_hashes
                .into_iter()
                .map(move |hash| async move { node.download_certificate(hash).await }),
        )
        .await?;

        if !certificates.is_empty()
            && self
                .client
                .local_node
                .try_process_certificates(name, node, chain_id, certificates, notifications)
                .await
                .is_none()
        {
            return Ok(());
        };
        if let Some(proposal) = info.manager.requested_proposed {
            if proposal.content.block.chain_id != chain_id {
                warn!(
                    "Response from validator {} contains an invalid proposal",
                    name
                );
                return Ok(()); // Give up on this validator.
            }
            let owner = proposal.owner;
            while let Err(original_err) = self
                .client
                .local_node
                .handle_block_proposal(*proposal.clone())
                .await
            {
                if let LocalNodeError::WorkerError(WorkerError::ChainError(chain_error)) =
                    &original_err
                {
                    if let ChainError::ExecutionError(
                        ExecutionError::SystemError(SystemExecutionError::BlobNotFoundOnRead(
                            blob_id,
                        )),
                        _,
                    ) = &**chain_error
                    {
                        self.update_local_node_with_blob_from(*blob_id, name, node)
                            .await?;
                        continue; // We found the missing blob: retry.
                    }
                }
                warn!(
                    "Skipping proposal from {} and validator {}: {}",
                    owner, name, original_err
                );
                break;
            }
        }
        if let Some(cert) = info.manager.requested_locked {
            if !cert.value().is_validated() || cert.value().chain_id() != chain_id {
                warn!(
                    "Response from validator {} contains an invalid locked block",
                    name
                );
                return Ok(()); // Give up on this validator.
            }
            let hash = cert.hash();
            let mut values = vec![];
            let mut blobs = vec![];
            while let Err(original_err) = self
                .client
                .local_node
                .handle_certificate(*cert.clone(), values, blobs, notifications)
                .await
            {
                if let LocalNodeError::WorkerError(
                    WorkerError::ApplicationBytecodesOrBlobsNotFound(locations, blob_ids),
                ) = &original_err
                {
                    values = LocalNodeClient::<S>::try_download_hashed_certificate_values_from(
                        locations, node, name,
                    )
                    .await;
                    blobs = self
                        .find_missing_blobs_in_validator(blob_ids.clone(), chain_id, name, node)
                        .await?;

                    if blobs.len() == blob_ids.len() && values.len() == locations.len() {
                        continue; // We found the missing blobs: retry.
                    }
                }
                warn!(
                    "Skipping certificate {} from validator {}: {}",
                    hash, name, original_err
                );
                break;
            }
        }
        Ok(())
    }

    /// Downloads the blobs from the specified validator and returns them, including blobs that
    /// are still pending the the validator's chain manager.
    pub async fn find_missing_blobs_in_validator(
        &self,
        blob_ids: Vec<BlobId>,
        chain_id: ChainId,
        name: &ValidatorName,
        node: &impl LocalValidatorNode,
    ) -> Result<Vec<HashedBlob>, NodeError> {
        let query = ChainInfoQuery::new(chain_id).with_manager_values();
        let info = match node.handle_chain_info_query(query).await {
            Ok(response) if response.check(name).is_ok() => Some(response.info),
            Ok(_) => {
                warn!("Got invalid response from validator {}", name);
                return Ok(Vec::new());
            }
            Err(err) => {
                warn!("Got error from validator {}: {}", name, err);
                return Ok(Vec::new());
            }
        };

        let mut missing_blobs = blob_ids;
        let mut found_blobs = if let Some(info) = info {
            let new_found_blobs = missing_blobs
                .iter()
                .filter_map(|blob_id| info.manager.pending_blobs.get(blob_id))
                .map(|blob| (blob.id, blob.clone()))
                .collect::<HashMap<_, _>>();
            missing_blobs.retain(|blob_id| !new_found_blobs.contains_key(blob_id));
            new_found_blobs.into_values().collect()
        } else {
            Vec::new()
        };

        found_blobs.extend(
            LocalNodeClient::<S>::try_download_blobs_from(&missing_blobs, name, node).await,
        );

        Ok(found_blobs)
    }

    /// Downloads and processes from the specified validator a confirmed block certificate that
    /// uses the given blob. If this succeeds, the blob will be in our storage.
    async fn update_local_node_with_blob_from(
        &self,
        blob_id: BlobId,
        name: &ValidatorName,
        node: &impl LocalValidatorNode,
    ) -> Result<(), ChainClientError> {
        let Ok(last_used_by_hash) = node.blob_last_used_by(blob_id).await else {
            return Err(ChainClientError::BlobNotFound(blob_id));
        };

        let certificate = node.download_certificate(last_used_by_hash).await?;
        ensure!(
            certificate.requires_blob(&blob_id),
            ChainClientError::InvalidLastUsedByCertificate(blob_id, *name)
        );

        // This will download all ancestors of the certificate and process all of them locally.
        self.receive_certificate(certificate).await?;
        Ok(())
    }

    /// Downloads and processes a confirmed block certificate that uses the given blob.
    /// If this succeeds, the blob will be in our storage.
    async fn receive_certificate_for_blob(&self, blob_id: BlobId) -> Result<(), ChainClientError> {
        let validators = self.validator_nodes().await?;
        let mut tasks = FuturesUnordered::new();
        for (name, node) in validators {
            let node = node.clone();
            tasks.push(async move {
                let last_used_hash = node.blob_last_used_by(blob_id).await.ok()?;
                let cert = node.download_certificate(last_used_hash).await.ok()?;
                if cert.requires_blob(&blob_id) {
                    self.receive_certificate(cert).await.ok()
                } else {
                    warn!(
                        "Got invalid last used by certificate for blob {} from validator {}",
                        blob_id, name
                    );
                    None
                }
            });
        }

        while let Some(result) = tasks.next().await {
            if result.is_some() {
                return Ok(());
            }
        }

        Err(ChainClientError::BlobNotFound(blob_id))
    }

    #[tracing::instrument(level = "trace", skip(block))]
    /// Attempts to execute the block locally. If any incoming message execution fails, that
    /// message is rejected and execution is retried, until the block accepts only messages
    /// that succeed.
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
                    ChainExecutionContext::IncomingMessage(index),
                ) = &**chain_error
                {
                    let message = block
                        .incoming_messages
                        .get_mut(*index as usize)
                        .expect("Message at given index should exist");
                    if message.event.is_protected() {
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

    #[tracing::instrument(level = "trace", skip(block))]
    /// Attempts to execute the block locally. If any attempt to read a blob fails, the blob is
    /// downloaded and execution is retried.
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
            if let Err(LocalNodeError::WorkerError(WorkerError::ChainError(chain_error))) = &result
            {
                if let ChainError::ExecutionError(
                    ExecutionError::SystemError(SystemExecutionError::BlobNotFoundOnRead(blob_id)),
                    _,
                ) = &**chain_error
                {
                    self.receive_certificate_for_blob(*blob_id).await?;
                    continue; // We found the missing blob: retry.
                }
            }
            return Ok(result?);
        }
    }

    #[tracing::instrument(level = "trace", skip(blob_ids))]
    /// Tries to read blobs from either the pending blobs or the local node's cache, or
    /// the chain manager's pending blobs
    async fn read_local_blobs(
        &self,
        blob_ids: impl IntoIterator<Item = BlobId>,
    ) -> Result<Vec<HashedBlob>, LocalNodeError> {
        let mut blobs = Vec::new();
        for blob_id in blob_ids {
            if let Some(blob) = self.client.local_node.recent_blob(&blob_id).await {
                blobs.push(blob);
            } else {
                let blob = self
                    .state()
                    .pending_blobs
                    .get(&blob_id)
                    .ok_or_else(|| LocalNodeError::CannotReadLocalBlob {
                        chain_id: self.chain_id,
                        blob_id,
                    })?
                    .clone();
                self.client.local_node.cache_recent_blob(&blob).await;
                blobs.push(blob);
            }
        }
        Ok(blobs)
    }

    #[tracing::instrument(level = "trace", skip(block, round, manager))]
    /// Executes (or retries) a regular block proposal. Updates local balance.
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
            next_block_height = state.next_block_height;
            block_hash = state.block_hash;
            // In the fast round, we must never make any conflicting proposals.
            if round.is_fast() {
                if let Some(pending) = &self.state().pending_block {
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
        let nodes: Vec<_> = self
            .client
            .validator_node_provider
            .make_nodes(&committee)?
            .collect();
        let values = self
            .client
            .local_node
            .read_or_download_hashed_certificate_values(&nodes, block.bytecode_locations())
            .await?;
        let hashed_blobs = self.read_local_blobs(block.published_blob_ids()).await?;
        // Create the final block proposal.
        let key_pair = self.key_pair().await?;
        let proposal = if let Some(cert) = manager.requested_locked {
            BlockProposal::new_retry(round, *cert, &key_pair, values, hashed_blobs)
        } else {
            BlockProposal::new_initial(round, block.clone(), &key_pair, values, hashed_blobs)
        };
        // Check the final block proposal. This will be cheaper after #1401.
        self.client
            .local_node
            .handle_block_proposal(proposal.clone())
            .await?;
        // Remember what we are trying to do before sending the proposal to the validators.
        self.state_mut().pending_block = Some(block);
        // Send the query to validators.
        let certificate = self
            .submit_block_proposal(&committee, proposal, hashed_value)
            .await?;
        self.clear_pending_block();
        // Communicate the new certificate now.
        let next_block_height = self.state().next_block_height;
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
                let next_block_height = self.state().next_block_height;
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

    #[tracing::instrument(level = "trace", skip(operations))]
    /// Executes a list of operations.
    pub async fn execute_operations(
        &self,
        operations: Vec<Operation>,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.prepare_chain().await?;
        self.execute_without_prepare(operations).await
    }

    #[tracing::instrument(level = "trace", skip(operations))]
    /// Executes a list of operations, without calling `prepare_chain`.
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

    #[tracing::instrument(level = "trace", skip(operation))]
    /// Executes an operation.
    pub async fn execute_operation(
        &self,
        operation: Operation,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.execute_operations(vec![operation]).await
    }

    #[tracing::instrument(level = "trace", skip(operations))]
    /// Executes a new block.
    ///
    /// This must be preceded by a call to `prepare_chain()`.
    async fn execute_block(
        &self,
        operations: Vec<Operation>,
    ) -> Result<ExecuteBlockOutcome, ChainClientError> {
        let block_mutex = Arc::clone(&self.state().preparing_block);
        let _block_guard = block_mutex.lock_owned().await;
        match self.process_pending_block_without_prepare().await? {
            ClientOutcome::Committed(Some(certificate)) => {
                return Ok(ExecuteBlockOutcome::Conflict(certificate))
            }
            ClientOutcome::Committed(None) => {}
            ClientOutcome::WaitForTimeout(timeout) => {
                return Ok(ExecuteBlockOutcome::WaitForTimeout(timeout))
            }
        }
        let incoming_messages = self.pending_messages().await?;
        let confirmed_value = self
            .set_pending_block(incoming_messages, operations)
            .await?;
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

    #[tracing::instrument(level = "trace", skip(incoming_messages, operations))]
    /// Sets the pending block, so that next time `process_pending_block_without_prepare` is
    /// called, it will be proposed to the validators.
    async fn set_pending_block(
        &self,
        incoming_messages: Vec<IncomingMessage>,
        operations: Vec<Operation>,
    ) -> Result<HashedCertificateValue, ChainClientError> {
        let timestamp = self.next_timestamp(&incoming_messages).await;
        let identity = self.identity().await?;
        let previous_block_hash;
        let height;
        {
            let state = self.state();
            previous_block_hash = state.block_hash;
            height = state.next_block_height;
        }
        let block = Block {
            epoch: self.epoch().await?,
            chain_id: self.chain_id,
            incoming_messages,
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
        self.state_mut().pending_block = Some(executed_block.block.clone());
        Ok(HashedCertificateValue::new_confirmed(executed_block))
    }

    #[tracing::instrument(level = "trace", skip(incoming_messages))]
    /// Returns a suitable timestamp for the next block.
    ///
    /// This will usually be the current time according to the local clock, but may be slightly
    /// ahead to make sure it's not earlier than the incoming messages or the previous block.
    async fn next_timestamp(&self, incoming_messages: &[IncomingMessage]) -> Timestamp {
        let local_time = self.storage_client().clock().current_time();
        incoming_messages
            .iter()
            .map(|msg| msg.event.timestamp)
            .max()
            .map_or(local_time, |timestamp| timestamp.max(local_time))
            .max(self.state().timestamp)
    }

    #[tracing::instrument(level = "trace", skip(query))]
    /// Queries an application.
    pub async fn query_application(&self, query: Query) -> Result<Response, ChainClientError> {
        let response = self
            .client
            .local_node
            .query_application(self.chain_id, query)
            .await?;
        Ok(response)
    }

    #[tracing::instrument(level = "trace", skip(query))]
    /// Queries a system application.
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

    #[tracing::instrument(level = "trace", skip(application_id, query))]
    /// Queries a user application.
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

    #[tracing::instrument(level = "trace")]
    /// Obtains the local balance of the chain account after staging the execution of
    /// incoming messages in a new block.
    ///
    /// Does not attempt to synchronize with validators. The result will reflect up to
    /// `max_pending_messages` incoming messages and the execution fees for a single
    /// block.
    pub async fn query_balance(&self) -> Result<Amount, ChainClientError> {
        let (balance, _) = self.query_balances_with_owner(None).await?;
        Ok(balance)
    }

    #[tracing::instrument(level = "trace", skip(owner))]
    /// Obtains the local balance of a user account after staging the execution of
    /// incoming messages in a new block.
    ///
    /// Does not attempt to synchronize with validators. The result will reflect up to
    /// `max_pending_messages` incoming messages and the execution fees for a single
    /// block.
    pub async fn query_owner_balance(&self, owner: Owner) -> Result<Amount, ChainClientError> {
        Ok(self
            .query_balances_with_owner(Some(owner))
            .await?
            .1
            .unwrap_or(Amount::ZERO))
    }

    #[tracing::instrument(level = "trace", skip(owner))]
    /// Obtains the local balance of the chain account and optionally another user after
    /// staging the execution of incoming messages in a new block.
    ///
    /// Does not attempt to synchronize with validators. The result will reflect up to
    /// `max_pending_messages` incoming messages and the execution fees for a single
    /// block.
    async fn query_balances_with_owner(
        &self,
        owner: Option<Owner>,
    ) -> Result<(Amount, Option<Amount>), ChainClientError> {
        let incoming_messages = self.pending_messages().await?;
        let timestamp = self.next_timestamp(&incoming_messages).await;
        let block = Block {
            epoch: self.epoch().await?,
            chain_id: self.chain_id,
            incoming_messages,
            operations: Vec::new(),
            previous_block_hash: self.state().block_hash,
            height: self.state().next_block_height,
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

    #[tracing::instrument(level = "trace")]
    /// Reads the local balance of the chain account.
    ///
    /// Does not process the inbox or attempt to synchronize with validators.
    pub async fn local_balance(&self) -> Result<Amount, ChainClientError> {
        let (balance, _) = self.local_balances_with_owner(None).await?;
        Ok(balance)
    }

    #[tracing::instrument(level = "trace", skip(owner))]
    /// Reads the local balance of a user account.
    ///
    /// Does not process the inbox or attempt to synchronize with validators.
    pub async fn local_owner_balance(&self, owner: Owner) -> Result<Amount, ChainClientError> {
        Ok(self
            .local_balances_with_owner(Some(owner))
            .await?
            .1
            .unwrap_or(Amount::ZERO))
    }

    #[tracing::instrument(level = "trace", skip(owner))]
    /// Reads the local balance of the chain account and optionally another user.
    ///
    /// Does not process the inbox or attempt to synchronize with validators.
    async fn local_balances_with_owner(
        &self,
        owner: Option<Owner>,
    ) -> Result<(Amount, Option<Amount>), ChainClientError> {
        ensure!(
            self.chain_info().await?.next_block_height == self.state().next_block_height,
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

    #[tracing::instrument(level = "trace")]
    /// Attempts to update all validators about the local chain.
    pub async fn update_validators(&self) -> Result<(), ChainClientError> {
        let committee = self.local_committee().await?;
        let next_block_height = self.state().next_block_height;
        self.communicate_chain_updates(
            &committee,
            self.chain_id,
            next_block_height,
            CrossChainMessageDelivery::NonBlocking,
        )
        .await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace")]
    /// Requests a `RegisterApplications` message from another chain so the application can be used
    /// on this one.
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

    #[tracing::instrument(level = "trace", skip(user_data))]
    /// Sends tokens to a chain.
    pub async fn transfer_to_account(
        &self,
        owner: Option<Owner>,
        amount: Amount,
        account: Account,
        user_data: UserData,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.transfer(owner, amount, Recipient::Account(account), user_data)
            .await
    }

    #[tracing::instrument(level = "trace", skip(user_data))]
    /// Burns tokens.
    pub async fn burn(
        &self,
        owner: Option<Owner>,
        amount: Amount,
        user_data: UserData,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.transfer(owner, amount, Recipient::Burn, user_data)
            .await
    }

    #[tracing::instrument(level = "trace")]
    /// Attempts to synchronize chains that have sent us messages and populate our local
    /// inbox.
    ///
    /// To create a block that actually executes the messages in the inbox,
    /// `process_inbox` must be called separately.
    pub async fn synchronize_from_validators(&self) -> Result<Box<ChainInfo>, ChainClientError> {
        self.find_received_certificates().await?;
        self.prepare_chain().await
    }

    #[tracing::instrument(level = "trace")]
    /// Processes the last pending block
    pub async fn process_pending_block(
        &self,
    ) -> Result<ClientOutcome<Option<Certificate>>, ChainClientError> {
        self.find_received_certificates().await?;
        self.prepare_chain().await?;
        self.process_pending_block_without_prepare().await
    }

    #[tracing::instrument(level = "trace")]
    /// Processes the last pending block. Assumes that the local chain is up to date.
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
        let manager = *info.manager;
        // Drop the pending block if it is outdated.
        {
            let state = self.state();
            if let Some(block) = &state.pending_block {
                if block.height != info.next_block_height {
                    drop(state);
                    // Caveat editor: `state` must no longer live before calling
                    // `clear_pending_block`, or we will cause a deadlock.
                    self.clear_pending_block();
                }
            }
        }

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
            .or_else(|| self.state().pending_block.clone())
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

    #[tracing::instrument(level = "trace")]
    /// Clears the information on any operation that previously failed.
    pub fn clear_pending_block(&self) {
        let mut state = self.state_mut();
        state.pending_block = None;
        state.pending_blobs.clear();
    }

    #[tracing::instrument(
        level = "trace",
        skip(certificate),
        fields(certificate_hash = ?certificate.hash()),
    )]
    /// Processes a confirmed block for which this chain is a recipient and updates validators.
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

    #[tracing::instrument(
        level = "trace",
        skip(certificate),
        fields(certificate_hash = ?certificate.hash()),
    )]
    /// Processes confirmed operation for which this chain is a recipient.
    pub async fn receive_certificate(
        &self,
        certificate: Certificate,
    ) -> Result<(), ChainClientError> {
        self.receive_certificate_internal(certificate, ReceiveCertificateMode::NeedsCheck)
            .await
    }

    #[tracing::instrument(level = "trace", skip(key_pair))]
    /// Rotates the key of the chain.
    pub async fn rotate_key_pair(
        &self,
        key_pair: KeyPair,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        let new_public_key = key_pair.public();
        self.state_mut()
            .known_key_pairs
            .insert(new_public_key.into(), key_pair);
        self.transfer_ownership(new_public_key).await
    }

    #[tracing::instrument(level = "trace")]
    /// Transfers ownership of the chain to a single super owner.
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

    #[tracing::instrument(level = "trace")]
    /// Adds another owner to the chain, and turns existing super owners into regular owners.
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

    #[tracing::instrument(level = "trace")]
    /// Changes the ownership of this chain. Fails if it would remove existing owners, unless
    /// `remove_owners` is `true`.
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

    #[tracing::instrument(level = "trace", skip(application_permissions))]
    /// Changes the application permissions configuration on this chain.
    pub async fn change_application_permissions(
        &self,
        application_permissions: ApplicationPermissions,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        let operation = SystemOperation::ChangeApplicationPermissions(application_permissions);
        self.execute_operation(operation.into()).await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    /// Opens a new chain with a derived UID.
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
                admin_id: self.admin_id(),
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
            return Ok(ClientOutcome::Committed((message_id, certificate)));
        }
    }

    #[tracing::instrument(level = "trace")]
    /// Closes the chain (and loses everything in it!!).
    pub async fn close_chain(&self) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::CloseChain))
            .await
    }

    #[tracing::instrument(level = "trace", skip(contract, service))]
    /// Publishes some bytecode.
    pub async fn publish_bytecode(
        &self,
        contract: Bytecode,
        service: Bytecode,
    ) -> Result<ClientOutcome<(BytecodeId, Certificate)>, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::PublishBytecode {
            contract: contract.clone(),
            service: service.clone(),
        }))
        .await?
        .try_map(|certificate| {
            // The first message of the only operation published the bytecode.
            let message_id = certificate
                .value()
                .executed_block()
                .and_then(|executed_block| {
                    executed_block.message_id_for_operation(0, PUBLISH_BYTECODE_MESSAGE_INDEX)
                })
                .ok_or_else(|| ChainClientError::InternalError("Failed to publish bytecode"))?;
            Ok((BytecodeId::new(message_id), certificate))
        })
    }

    #[tracing::instrument(level = "trace", skip(hashed_blob))]
    /// Publishes some blob.
    pub async fn publish_blob(
        &self,
        hashed_blob: HashedBlob,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.client.local_node.cache_recent_blob(&hashed_blob).await;
        self.state_mut()
            .pending_blobs
            .insert(hashed_blob.id(), hashed_blob.clone());
        self.execute_operation(Operation::System(SystemOperation::PublishBlob {
            blob_id: hashed_blob.id(),
        }))
        .await
    }

    /// Adds pending blobs
    pub async fn add_pending_blobs(&mut self, pending_blobs: &[HashedBlob]) {
        for hashed_blob in pending_blobs {
            self.client.local_node.cache_recent_blob(hashed_blob).await;
            self.state_mut()
                .pending_blobs
                .insert(hashed_blob.id(), hashed_blob.clone());
        }
    }

    #[tracing::instrument(
        level = "trace",
        skip(self, parameters, instantiation_argument, required_application_ids)
    )]
    /// Creates an application by instantiating some bytecode.
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
    /// Creates an application by instantiating some bytecode.
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

    #[tracing::instrument(level = "trace", skip(committee))]
    /// Creates a new committee and starts using it (admin chains only).
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

    #[tracing::instrument(level = "trace")]
    /// Creates an empty block to process all incoming messages. This may require several blocks.
    ///
    /// If not all certificates could be processed due to a timeout, the timestamp for when to retry
    /// is returned, too.
    pub async fn process_inbox(
        &self,
    ) -> Result<(Vec<Certificate>, Option<RoundTimeout>), ChainClientError> {
        self.prepare_chain().await?;
        let mut certificates = Vec::new();
        loop {
            let incoming_messages = self.pending_messages().await?;
            if incoming_messages.is_empty() {
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

    #[tracing::instrument(level = "trace")]
    /// Creates an empty block to process all incoming messages. This may require several blocks.
    /// If we are not a chain owner, this doesn't fail, and just returns an empty list.
    pub async fn process_inbox_if_owned(
        &self,
    ) -> Result<(Vec<Certificate>, Option<RoundTimeout>), ChainClientError> {
        match self.process_inbox().await {
            Ok(result) => Ok(result),
            Err(ChainClientError::CannotFindKeyForChain(_)) => Ok((Vec::new(), None)),
            Err(error) => Err(error),
        }
    }

    #[tracing::instrument(level = "trace")]
    /// Starts listening to the admin chain for new committees. (This is only useful for
    /// other genesis chains or for testing.)
    pub async fn subscribe_to_new_committees(
        &self,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::Subscribe {
            chain_id: self.admin_id(),
            channel: SystemChannel::Admin,
        }))
        .await
    }

    #[tracing::instrument(level = "trace")]
    /// Stops listening to the admin chain for new committees. (This is only useful for
    /// testing.)
    pub async fn unsubscribe_from_new_committees(
        &self,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::Unsubscribe {
            chain_id: self.admin_id(),
            channel: SystemChannel::Admin,
        }))
        .await
    }

    #[tracing::instrument(level = "trace")]
    /// Starts listening to the given chain for published bytecodes.
    pub async fn subscribe_to_published_bytecodes(
        &self,
        chain_id: ChainId,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::Subscribe {
            chain_id,
            channel: SystemChannel::PublishedBytecodes,
        }))
        .await
    }

    #[tracing::instrument(level = "trace")]
    /// Stops listening to the given chain for published bytecodes.
    pub async fn unsubscribe_from_published_bytecodes(
        &self,
        chain_id: ChainId,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::Unsubscribe {
            chain_id,
            channel: SystemChannel::PublishedBytecodes,
        }))
        .await
    }

    #[tracing::instrument(level = "trace")]
    /// Deprecates all the configurations of voting rights but the last one (admin chains
    /// only). Currently, each individual chain is still entitled to wait before accepting
    /// this command. However, it is expected that deprecated validators stop functioning
    /// shortly after such command is issued.
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

    #[tracing::instrument(level = "trace", skip(user_data))]
    /// Sends money to a chain.
    /// Do not check balance. (This may block the client)
    /// Do not confirm the transaction.
    pub async fn transfer_to_account_unsafe_unconfirmed(
        &self,
        owner: Option<Owner>,
        amount: Amount,
        account: Account,
        user_data: UserData,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::Transfer {
            owner,
            recipient: Recipient::Account(account),
            amount,
            user_data,
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

    #[tracing::instrument(level = "trace", skip(name, node, local_node, notification))]
    async fn process_notification(
        &self,
        name: ValidatorName,
        node: <P as LocalValidatorNodeProvider>::Node,
        mut local_node: LocalNodeClient<S>,
        notification: Notification,
    ) {
        match notification.reason {
            Reason::NewIncomingMessage { origin, height } => {
                if self
                    .local_next_block_height(origin.sender, &mut local_node)
                    .await
                    > Some(height)
                {
                    debug!("Accepting redundant notification for new message");
                    return;
                }
                if let Err(error) = self
                    .find_received_certificates_from_validator(name, node, local_node.clone())
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
                let mut notifications = vec![];
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
                    .try_synchronize_chain_state_from(&name, &node, chain_id, &mut notifications)
                    .await
                {
                    error!("Fail to process notification: {error}");
                    return;
                }
                self.handle_notifications(&mut notifications);
                let local_height = self
                    .local_next_block_height(chain_id, &mut local_node)
                    .await;
                if local_height <= Some(height) {
                    error!("Fail to synchronize new block after notification");
                }
            }
            Reason::NewRound { height, round } => {
                let mut notifications = vec![];
                let chain_id = notification.chain_id;
                if let Some(info) = self.local_chain_info(chain_id, &mut local_node).await {
                    if (info.next_block_height, info.manager.current_round) >= (height, round) {
                        debug!("Accepting redundant notification for new round");
                        return;
                    }
                }
                if let Err(error) = self
                    .try_synchronize_chain_state_from(&name, &node, chain_id, &mut notifications)
                    .await
                {
                    error!("Fail to process notification: {error}");
                    return;
                }
                self.handle_notifications(&mut notifications);
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

    #[tracing::instrument(level = "trace", fields(chain_id = ?self.chain_id))]
    /// Spawns a task that listens to notifications about the current chain from all validators,
    /// and synchronizes the local state accordingly.
    pub async fn listen(
        &self,
    ) -> Result<(impl Future<Output = ()>, AbortOnDrop, NotificationStream), ChainClientError>
    where
        P: Send + 'static,
    {
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
    ) -> Result<impl Future<Output = ()>, ChainClientError>
    where
        P: Send + 'static,
    {
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
            validator_tasks.push(async move {
                while let Some(notification) = stream.next().await {
                    this.process_notification(name, node.clone(), local_node.clone(), notification)
                        .await;
                }
            });
            entry.insert(abort);
        }
        Ok(validator_tasks.collect())
    }

    #[tracing::instrument(level = "trace", skip(node, node_client))]
    /// Attempts to download new received certificates from a particular validator.
    ///
    /// This is similar to `find_received_certificates` but for only one validator.
    /// We also don't try to synchronize the admin chain.
    pub async fn find_received_certificates_from_validator(
        &self,
        name: ValidatorName,
        node: <P as LocalValidatorNodeProvider>::Node,
        node_client: LocalNodeClient<S>,
    ) -> Result<(), ChainClientError> {
        let ((committees, max_epoch), chain_id, current_tracker) = {
            let (committees, max_epoch) = self.known_committees().await?;
            let chain_id = self.chain_id;
            let current_tracker: u64 = self
                .state()
                .received_certificate_trackers
                .get(&name)
                .copied()
                .unwrap_or(0);
            ((committees, max_epoch), chain_id, current_tracker)
        };
        // Proceed to downloading received certificates.
        let (name, tracker, certificates) =
            ChainClient::<P, S>::synchronize_received_certificates_from_validator(
                chain_id,
                name,
                current_tracker,
                committees,
                max_epoch,
                node,
                node_client,
            )
            .await?;
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

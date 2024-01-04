// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    data_types::{BlockHeightRange, ChainInfo, ChainInfoQuery, ClientOutcome, RoundTimeout},
    local_node::{LocalNodeClient, LocalNodeError},
    node::{
        CrossChainMessageDelivery, NodeError, NotificationStream, ValidatorNode,
        ValidatorNodeProvider,
    },
    notifier::Notifier,
    updater::{communicate_with_quorum, CommunicateAction, CommunicationError, ValidatorUpdater},
    worker::{
        DeliveryNotifiers, Notification, Reason, WorkerError, WorkerState, DEFAULT_VALUE_CACHE_SIZE,
    },
};
use futures::{
    future,
    lock::Mutex,
    stream::{self, AbortHandle, FuturesUnordered, StreamExt},
};
use linera_base::{
    abi::{Abi, ContractAbi},
    crypto::{CryptoHash, KeyPair, PublicKey},
    data_types::{Amount, ArithmeticError, BlockHeight, Round, Timestamp},
    ensure,
    identifiers::{ApplicationId, BytecodeId, ChainId, MessageId, Owner},
};
use linera_chain::{
    data_types::{
        Block, BlockAndRound, BlockProposal, Certificate, CertificateValue, ExecutedBlock,
        HashedValue, IncomingMessage, LiteCertificate, LiteVote, MessageAction,
    },
    ChainError, ChainExecutionContext, ChainStateView,
};
use linera_execution::{
    committee::{Committee, Epoch, ValidatorName},
    system::{
        Account, AdminOperation, Recipient, SystemChannel, SystemOperation, UserData,
        CREATE_APPLICATION_MESSAGE_INDEX, OPEN_CHAIN_MESSAGE_INDEX, PUBLISH_BYTECODE_MESSAGE_INDEX,
    },
    Bytecode, ChainOwnership, Message, Operation, Query, Response, SystemMessage, SystemQuery,
    SystemResponse, UserApplicationId,
};
use linera_storage::Storage;
use linera_views::views::ViewError;
use lru::LruCache;
use std::{
    collections::{hash_map, BTreeMap, HashMap},
    convert::Infallible,
    iter,
    num::NonZeroUsize,
    sync::Arc,
};
use thiserror::Error;
use tracing::{debug, error, info};

#[cfg(any(test, feature = "test"))]
#[path = "unit_tests/client_test_utils.rs"]
pub mod client_test_utils;

#[cfg(test)]
#[path = "unit_tests/client_tests.rs"]
mod client_tests;

/// A builder that creates `ChainClients` which share the cache and notifiers.
pub struct ChainClientBuilder<ValidatorNodeProvider> {
    /// How to talk to the validators.
    validator_node_provider: ValidatorNodeProvider,
    /// Maximum number of pending messages processed at a time in a block.
    max_pending_messages: usize,
    /// Whether to block on cross-chain message delivery.
    cross_chain_message_delivery: CrossChainMessageDelivery,
    /// Cached values by hash.
    recent_values: Arc<tokio::sync::Mutex<LruCache<CryptoHash, HashedValue>>>,
    /// One-shot channels to notify callers when messages of a particular chain have been
    /// delivered.
    delivery_notifiers: Arc<tokio::sync::Mutex<DeliveryNotifiers>>,
    /// References to clients waiting for chain notifications.
    notifier: Arc<Notifier<Notification>>,
}

impl<ValidatorNodeProvider: Clone> ChainClientBuilder<ValidatorNodeProvider> {
    /// Creates a new `ChainClientBuilder` with a new cache and notifiers.
    pub fn new(
        validator_node_provider: ValidatorNodeProvider,
        max_pending_messages: usize,
        cross_chain_message_delivery: CrossChainMessageDelivery,
    ) -> Self {
        let recent_values = Arc::new(tokio::sync::Mutex::new(LruCache::new(
            NonZeroUsize::try_from(DEFAULT_VALUE_CACHE_SIZE).unwrap(),
        )));
        Self {
            validator_node_provider,
            max_pending_messages,
            cross_chain_message_delivery,
            recent_values,
            delivery_notifiers: Arc::new(tokio::sync::Mutex::new(DeliveryNotifiers::default())),
            notifier: Arc::new(Notifier::default()),
        }
    }

    /// Creates a new `ChainClient`.
    #[allow(clippy::too_many_arguments)]
    pub fn build<Storage>(
        &self,
        chain_id: ChainId,
        known_key_pairs: Vec<KeyPair>,
        storage: Storage,
        admin_id: ChainId,
        block_hash: Option<CryptoHash>,
        timestamp: Timestamp,
        next_block_height: BlockHeight,
        pending_block: Option<Block>,
    ) -> ChainClient<ValidatorNodeProvider, Storage> {
        let known_key_pairs = known_key_pairs
            .into_iter()
            .map(|kp| (Owner::from(kp.public()), kp))
            .collect();
        let state = WorkerState::new_for_client(
            format!("Client node {:?}", chain_id),
            storage,
            self.recent_values.clone(),
            self.delivery_notifiers.clone(),
        )
        .with_allow_inactive_chains(true)
        .with_allow_messages_from_deprecated_epochs(true);
        let node_client = LocalNodeClient::new(state, self.notifier.clone());
        ChainClient {
            chain_id,
            known_key_pairs,
            validator_node_provider: self.validator_node_provider.clone(),
            admin_id,
            max_pending_messages: self.max_pending_messages,
            cross_chain_message_delivery: self.cross_chain_message_delivery,
            received_certificate_trackers: HashMap::new(),
            block_hash,
            timestamp,
            next_block_height,
            pending_block,
            node_client,
        }
    }
}

/// Client to operate a chain by interacting with validators and the given local storage
/// implementation.
/// * The chain being operated is called the "local chain" or just the "chain".
/// * As a rule, operations are considered successful (and communication may stop) when
/// they succeeded in gathering a quorum of responses.
pub struct ChainClient<ValidatorNodeProvider, Storage> {
    /// The off-chain chain id.
    chain_id: ChainId,
    /// How to talk to the validators.
    validator_node_provider: ValidatorNodeProvider,
    /// Latest block hash, if any.
    block_hash: Option<CryptoHash>,
    /// The earliest possible timestamp for the next block.
    timestamp: Timestamp,
    /// Sequence number that we plan to use for the next block.
    /// We track this value outside local storage mainly for security reasons.
    next_block_height: BlockHeight,
    /// Pending block.
    pending_block: Option<Block>,
    /// Known key pairs from present and past identities.
    known_key_pairs: BTreeMap<Owner, KeyPair>,
    /// The id of the admin chain.
    admin_id: ChainId,

    /// Maximum number of pending messages processed at a time in a block.
    max_pending_messages: usize,
    /// Whether to block on cross-chain message delivery.
    cross_chain_message_delivery: CrossChainMessageDelivery,
    /// Support synchronization of received certificates.
    received_certificate_trackers: HashMap<ValidatorName, u64>,
    /// Local node to manage the execution state and the local storage of the chains that we are
    /// tracking.
    node_client: LocalNodeClient<Storage>,
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

    #[error("Leader timeout certificate does not match the expected one.")]
    UnexpectedLeaderTimeout,
}

impl From<Infallible> for ChainClientError {
    fn from(infallible: Infallible) -> Self {
        infallible.into()
    }
}

impl<P, S> ChainClient<P, S> {
    pub fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    /// Returns the hash of the latest known block.
    pub fn block_hash(&self) -> Option<CryptoHash> {
        self.block_hash
    }

    /// Returns the earliest possible timestamp for the next block.
    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    pub fn next_block_height(&self) -> BlockHeight {
        self.next_block_height
    }

    pub fn pending_block(&self) -> &Option<Block> {
        &self.pending_block
    }
}

enum ReceiveCertificateMode {
    NeedsCheck,
    AlreadyChecked,
}

impl<P, S> ChainClient<P, S>
where
    P: ValidatorNodeProvider + Sync,
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    /// Obtains a `ChainStateView` for a given `ChainId`.
    pub async fn chain_state_view(
        &self,
    ) -> Result<Arc<ChainStateView<S::Context>>, LocalNodeError> {
        let chain_state_view = self
            .storage_client()
            .await
            .load_chain(self.chain_id)
            .await?;
        Ok(Arc::new(chain_state_view))
    }

    /// Subscribes to notifications from this client's chain.
    pub async fn subscribe(&mut self) -> Result<NotificationStream, LocalNodeError> {
        self.node_client.subscribe(vec![self.chain_id]).await
    }

    /// Returns the storage client used by this client's local node.
    pub async fn storage_client(&self) -> S {
        self.node_client.storage_client().await
    }

    /// Obtains the basic `ChainInfo` data for the local chain.
    async fn chain_info(&mut self) -> Result<Box<ChainInfo>, LocalNodeError> {
        let query = ChainInfoQuery::new(self.chain_id);
        let response = self.node_client.handle_chain_info_query(query).await?;
        Ok(response.info)
    }

    /// Obtains the basic `ChainInfo` data for the local chain, with chain manager values.
    pub async fn chain_info_with_manager_values(
        &mut self,
    ) -> Result<Box<ChainInfo>, LocalNodeError> {
        let query = ChainInfoQuery::new(self.chain_id).with_manager_values();
        let response = self.node_client.handle_chain_info_query(query).await?;
        Ok(response.info)
    }

    /// Obtains up to `self.max_pending_messages` pending messages for the local chain.
    ///
    /// Messages known to be redundant are filtered out: A `RegisterApplications` message whose
    /// entries are already known never needs to be included in a block.
    async fn pending_messages(&mut self) -> Result<Vec<IncomingMessage>, LocalNodeError> {
        self.pending_messages_internal(|_| true).await
    }

    async fn pending_system_messages(&mut self) -> Result<Vec<IncomingMessage>, LocalNodeError> {
        self.pending_messages_internal(|message| {
            matches!(message.event.message, Message::System(_))
        })
        .await
    }

    async fn pending_messages_internal<F: Fn(&IncomingMessage) -> bool>(
        &mut self,
        filter: F,
    ) -> Result<Vec<IncomingMessage>, LocalNodeError> {
        let query = ChainInfoQuery::new(self.chain_id).with_pending_messages();
        let response = self.node_client.handle_chain_info_query(query).await?;
        let mut requested_pending_messages = response.info.requested_pending_messages;
        let mut pending_messages = vec![];
        // The first incoming message of any child chain must be `OpenChain`. We must have it in
        // our inbox, and include it before all other messages.
        if self.next_block_height == BlockHeight::ZERO
            && self
                .chain_state_view()
                .await?
                .execution_state
                .system
                .description
                .get()
                .ok_or_else(|| LocalNodeError::InactiveChain(self.chain_id))?
                .is_child()
        {
            let Some(index) = requested_pending_messages.iter().position(|message| {
                matches!(
                    message.event.message,
                    Message::System(SystemMessage::OpenChain { .. })
                )
            }) else {
                return Err(LocalNodeError::InactiveChain(self.chain_id));
            };
            let open_chain_message = requested_pending_messages.remove(index);
            pending_messages.push(open_chain_message);
        }
        for message in requested_pending_messages {
            if pending_messages.len() >= self.max_pending_messages {
                tracing::warn!(
                    "Limiting block to {} incoming messages",
                    self.max_pending_messages
                );
                break;
            }
            if !filter(&message) {
                continue;
            }
            if let Message::System(SystemMessage::RegisterApplications { applications }) =
                &message.event.message
            {
                let chain_id = self.chain_id;
                if applications
                    .iter()
                    .map(|application| {
                        self.node_client
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

    /// Obtains the set of committees trusted by the local chain.
    async fn committees(&mut self) -> Result<BTreeMap<Epoch, Committee>, LocalNodeError> {
        let (_epoch, committees) = self.epoch_and_committees(self.chain_id).await?;
        Ok(committees)
    }

    /// Obtains the current epoch of the given chain as well as its set of trusted committees.
    async fn epoch_and_committees(
        &mut self,
        chain_id: ChainId,
    ) -> Result<(Option<Epoch>, BTreeMap<Epoch, Committee>), LocalNodeError> {
        let query = ChainInfoQuery::new(chain_id).with_committees();
        let info = self.node_client.handle_chain_info_query(query).await?.info;
        let epoch = info.epoch;
        let committees = info
            .requested_committees
            .ok_or(LocalNodeError::InvalidChainInfoResponse)?;
        Ok((epoch, committees))
    }

    /// Obtains the epochs of the committees trusted by the local chain.
    pub async fn epochs(&mut self) -> Result<Vec<Epoch>, LocalNodeError> {
        let committees = self.committees().await?;
        Ok(committees.into_keys().collect())
    }

    /// Obtains the committee for the current epoch of the local chain.
    pub async fn local_committee(&mut self) -> Result<Committee, LocalNodeError> {
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
    async fn known_committees(
        &mut self,
    ) -> Result<(BTreeMap<Epoch, Committee>, Epoch), LocalNodeError> {
        let (epoch, mut committees) = self.epoch_and_committees(self.chain_id).await?;
        let (admin_epoch, admin_committees) = self.epoch_and_committees(self.admin_id).await?;
        committees.extend(admin_committees);
        let epoch = std::cmp::max(epoch.unwrap_or_default(), admin_epoch.unwrap_or_default());
        Ok((committees, epoch))
    }

    /// Obtains the validators trusted by the local chain.
    async fn validator_nodes(&mut self) -> Result<Vec<(ValidatorName, P::Node)>, ChainClientError> {
        match self.local_committee().await {
            Ok(committee) => Ok(self.validator_node_provider.make_nodes(&committee)?),
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
    async fn epoch(&mut self) -> Result<Epoch, LocalNodeError> {
        self.chain_info()
            .await?
            .epoch
            .ok_or(LocalNodeError::InactiveChain(self.chain_id))
    }

    /// Obtains the identity of the current owner of the chain. HACK: In the case of a
    /// multi-owner chain, we pick one identity for which we know the private key.
    pub async fn identity(&mut self) -> Result<Owner, ChainClientError> {
        let manager = self.chain_info().await?.manager;
        ensure!(
            manager.ownership.is_active(),
            LocalNodeError::InactiveChain(self.chain_id)
        );
        let mut identities = manager
            .ownership
            .all_owners()
            .filter(|owner| self.known_key_pairs.contains_key(owner));
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
    pub async fn key_pair(&mut self) -> Result<&KeyPair, ChainClientError> {
        let id = self.identity().await?;
        Ok(self
            .known_key_pairs
            .get(&id)
            .expect("key should be known at this point"))
    }

    /// Obtains the public key associated to the current identity.
    pub async fn public_key(&mut self) -> Result<PublicKey, ChainClientError> {
        Ok(self.key_pair().await?.public())
    }

    async fn local_chain_info(
        this: Arc<Mutex<Self>>,
        chain_id: ChainId,
        local_node: &mut LocalNodeClient<S>,
    ) -> Option<Box<ChainInfo>> {
        let mut guard = this.lock().await;
        let Ok(info) = local_node.local_chain_info(chain_id).await else {
            error!("Fail to read local chain info for {chain_id}");
            return None;
        };
        // Useful in case `chain_id` is the same as the local chain.
        guard.update_from_info(&info);
        Some(info)
    }

    async fn local_next_block_height(
        this: Arc<Mutex<Self>>,
        chain_id: ChainId,
        local_node: &mut LocalNodeClient<S>,
    ) -> Option<BlockHeight> {
        let info = Self::local_chain_info(this, chain_id, local_node).await?;
        Some(info.next_block_height)
    }

    async fn process_notification<A>(
        this: Arc<Mutex<Self>>,
        name: ValidatorName,
        node: A,
        mut local_node: LocalNodeClient<S>,
        notification: Notification,
    ) where
        A: ValidatorNode + Send + Sync + 'static + Clone,
    {
        match notification.reason {
            Reason::NewIncomingMessage { origin, height } => {
                if Self::local_next_block_height(this.clone(), origin.sender, &mut local_node).await
                    > Some(height)
                {
                    debug!("Accepting redundant notification for new message");
                }
                if let Err(e) = Self::find_received_certificates_from_validator(
                    this.clone(),
                    name,
                    node,
                    local_node.clone(),
                )
                .await
                {
                    error!("Fail to process notification: {e}");
                }
                if Self::local_next_block_height(this, origin.sender, &mut local_node).await
                    <= Some(height)
                {
                    error!("Fail to synchronize new message after notification");
                }
            }
            Reason::NewBlock { height, .. } => {
                let chain_id = notification.chain_id;
                if Self::local_next_block_height(this.clone(), chain_id, &mut local_node).await
                    > Some(height)
                {
                    debug!("Accepting redundant notification for new block");
                }
                match local_node
                    .try_synchronize_chain_state_from(name, node, chain_id)
                    .await
                {
                    Ok(()) => {
                        if Self::local_next_block_height(this, chain_id, &mut local_node).await
                            <= Some(height)
                        {
                            error!("Fail to synchronize new block after notification");
                        }
                    }
                    Err(e) => {
                        error!("Fail to process notification: {e}");
                    }
                }
            }
            Reason::NewRound { height, round } => {
                let chain_id = notification.chain_id;
                if let Some(info) =
                    Self::local_chain_info(this.clone(), chain_id, &mut local_node).await
                {
                    if (info.next_block_height, info.manager.current_round) >= (height, round) {
                        debug!("Accepting redundant notification for new round");
                    }
                }
                if let Err(error) = local_node
                    .try_synchronize_chain_state_from(name, node, chain_id)
                    .await
                {
                    error!("Fail to process notification: {error}");
                }
                let Some(info) =
                    Self::local_chain_info(this.clone(), chain_id, &mut local_node).await
                else {
                    error!("Fail to read local chain info for {chain_id}");
                    return;
                };
                if (info.next_block_height, info.manager.current_round) < (height, round) {
                    error!("Fail to synchronize new block after notification");
                }
            }
        }
    }

    /// Spawns a thread that listens to notifications about the current chain from all validators,
    /// and synchronizes the local state accordingly.
    pub async fn listen(this: Arc<Mutex<Self>>) -> Result<(), ChainClientError>
    where
        P: Send + 'static,
    {
        let mut senders = HashMap::new(); // Senders to cancel notification streams.
        let mut notifications = this.lock().await.subscribe().await?;
        if let Err(err) = Self::update_streams(&this, &mut senders).await {
            error!("Failed to update committee: {}", err);
        }
        tokio::spawn(async move {
            while let Some(notification) = notifications.next().await {
                if matches!(notification.reason, Reason::NewBlock { .. }) {
                    if let Err(err) = Self::update_streams(&this, &mut senders).await {
                        error!("Failed to update committee: {}", err);
                    }
                }
            }
        });
        Ok(())
    }

    async fn update_streams(
        this: &Arc<Mutex<Self>>,
        senders: &mut HashMap<ValidatorName, AbortHandle>,
    ) -> Result<(), ChainClientError>
    where
        P: Send + 'static,
    {
        let (chain_id, nodes, local_node) = {
            let mut guard = this.lock().await;
            let committee = guard.local_committee().await?;
            let nodes: HashMap<_, _> = guard.validator_node_provider.make_nodes(&committee)?;
            (guard.chain_id, nodes, guard.node_client.clone())
        };
        // Drop removed validators.
        senders.retain(|name, abort| {
            if !nodes.contains_key(name) {
                abort.abort();
            }
            !abort.is_aborted()
        });
        // Add tasks for new validators.
        for (name, mut node) in nodes {
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
            let this = this.clone();
            let local_node = local_node.clone();
            tokio::spawn(async move {
                while let Some(notification) = stream.next().await {
                    Self::process_notification(
                        this.clone(),
                        name,
                        node.clone(),
                        local_node.clone(),
                        notification,
                    )
                    .await;
                }
            });
            entry.insert(abort);
        }
        Ok(())
    }

    /// Prepares the chain for the next operation.
    async fn prepare_chain(&mut self) -> Result<Box<ChainInfo>, ChainClientError> {
        // Verify that our local storage contains enough history compared to the
        // expected block height. Otherwise, download the missing history from the
        // network.
        let nodes = self.validator_nodes().await?;
        let mut info = self
            .node_client
            .download_certificates(nodes, self.chain_id, self.next_block_height)
            .await?;
        if info.next_block_height == self.next_block_height {
            // Check that our local node has the expected block hash.
            ensure!(
                self.block_hash == info.block_hash,
                ChainClientError::InternalError("Invalid chain of blocks in local node")
            );
        }
        let ownership = &info.manager.ownership;
        if ownership
            .all_owners()
            .any(|owner| !self.known_key_pairs.contains_key(owner))
        {
            // For chains with any owner other than ourselves, we could be missing recent
            // certificates created by other owners. Further synchronize blocks from the network.
            // This is a best-effort that depends on network conditions.
            let nodes = self.validator_nodes().await?;
            info = self
                .node_client
                .synchronize_chain_state(nodes, self.chain_id)
                .await?;
        }
        self.update_from_info(&info);
        Ok(info)
    }

    /// Submits a validated block for finalization and returns the confirmed block certificate.
    async fn finalize_block(
        &mut self,
        committee: &Committee,
        certificate: Certificate,
    ) -> Result<Certificate, ChainClientError> {
        let block = certificate
            .value()
            .block()
            .ok_or_else(|| {
                ChainClientError::InternalError(
                    "Cannot submit a certificate without a block for finalization",
                )
            })?
            .clone();
        let finalize_action = CommunicateAction::FinalizeBlock {
            certificate,
            delivery: CrossChainMessageDelivery::NonBlocking,
        };
        let certificate = self
            .communicate_chain_updates(committee, block.chain_id, finalize_action)
            .await?
            .ok_or_else(|| {
                ChainClientError::InternalError("Missing confirmed block certificate")
            })?;
        ensure!(
            certificate.value().is_confirmed() && certificate.value().block() == Some(&block),
            ChainClientError::BlockProposalError(
                "A different operation was executed in parallel (consider retrying the operation)"
            )
        );
        Ok(certificate)
    }

    /// Submits a block proposal to the validators. If it is a slow round, also submits the
    /// validated block for finalization. Returns the confirmed block certificate.
    async fn submit_block_proposal(
        &mut self,
        committee: &Committee,
        proposal: BlockProposal,
        hashed_value: HashedValue,
    ) -> Result<Certificate, ChainClientError> {
        let content = proposal.content.clone();
        let submit_action = CommunicateAction::SubmitBlock {
            proposal,
            hashed_value,
        };
        let certificate = self
            .communicate_chain_updates(committee, content.block.chain_id, submit_action)
            .await?
            .ok_or_else(|| ChainClientError::InternalError("Missing certificate"))?;
        ensure!(
            certificate.value().block() == Some(&content.block),
            ChainClientError::BlockProposalError(
                "A different operation was executed in parallel (consider retrying the operation)"
            )
        );
        if content.round.is_fast() {
            ensure!(
                certificate.value().is_confirmed(),
                ChainClientError::InternalError("Certificate is not for a confirmed block")
            );
            Ok(certificate)
        } else {
            ensure!(
                certificate.value().is_validated(),
                ChainClientError::InternalError("Certificate is not for a validated block")
            );
            self.finalize_block(committee, certificate).await
        }
    }

    /// Broadcasts certified blocks and optionally one more block proposal.
    /// The corresponding block heights should be consecutive and increasing.
    async fn communicate_chain_updates(
        &mut self,
        committee: &Committee,
        chain_id: ChainId,
        action: CommunicateAction,
    ) -> Result<Option<Certificate>, ChainClientError> {
        let storage_client = self.storage_client().await;
        let nodes: Vec<_> = self.validator_node_provider.make_nodes(committee)?;
        let results = communicate_with_quorum(
            &nodes,
            committee,
            |value: &Option<LiteVote>| -> Option<_> {
                value
                    .as_ref()
                    .map(|vote| (vote.value.value_hash, vote.round))
            },
            |name, node| {
                let mut updater = ValidatorUpdater {
                    name,
                    node,
                    storage: storage_client.clone(),
                };
                let action = action.clone();
                Box::pin(async move { updater.send_chain_update(chain_id, action).await })
            },
        )
        .await?;
        let (value, round) = match action {
            CommunicateAction::SubmitBlock {
                proposal,
                hashed_value,
            } => {
                let round = proposal.content.round;
                (hashed_value, round)
            }
            CommunicateAction::FinalizeBlock { certificate, .. } => {
                let round = certificate.round;
                let Some(value) = certificate.value.into_confirmed() else {
                    return Err(ChainClientError::ProtocolError(
                        "Unexpected certificate value for finalized block",
                    ));
                };
                (value, round)
            }
            CommunicateAction::RequestLeaderTimeout {
                height,
                round,
                epoch,
            } => {
                let value = HashedValue::from(CertificateValue::LeaderTimeout {
                    chain_id,
                    height,
                    epoch,
                });
                (value, round)
            }
            CommunicateAction::AdvanceToNextBlockHeight { .. } => {
                return Ok(None);
            }
        };
        let votes = match results {
            (Some((votes_hash, votes_round)), votes)
                if votes_hash == value.hash() && votes_round == round =>
            {
                votes
            }
            _ => {
                return Err(ChainClientError::ProtocolError(
                    "Unexpected response from validators",
                ))
            }
        };
        // Certificate is valid because
        // * `communicate_with_quorum` ensured a sufficient "weight" of
        // (non-error) answers were returned by validators.
        // * each answer is a vote signed by the expected validator.
        let certificate = LiteCertificate::try_from_votes(votes.into_iter().flatten())
            .ok_or_else(|| {
                ChainClientError::InternalError("Vote values or rounds don't match; this is a bug")
            })?
            .with_value(value)
            .ok_or_else(|| {
                ChainClientError::ProtocolError("A quorum voted for an unexpected value")
            })?;
        Ok(Some(certificate))
    }

    async fn receive_certificate_internal(
        &mut self,
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
        let nodes: Vec<_> = self.validator_node_provider.make_nodes(remote_committee)?;
        self.node_client
            .download_certificates(nodes.clone(), block.chain_id, block.height)
            .await?;
        // Process the received operations. Download required blobs if necessary.
        if let Err(err) = self.process_certificate(certificate.clone(), vec![]).await {
            if let LocalNodeError::WorkerError(WorkerError::ApplicationBytecodesNotFound(
                locations,
            )) = &err
            {
                let blobs = future::join_all(locations.iter().map(|location| {
                    LocalNodeClient::<S>::download_blob(nodes.clone(), block.chain_id, *location)
                }))
                .await
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
                if !blobs.is_empty() {
                    self.process_certificate(certificate.clone(), blobs).await?;
                }
            }
            // The certificate is not as expected. Give up.
            tracing::warn!("Failed to process network blob",);
            return Err(err.into());
        }
        // Make sure a quorum of validators (according to our new local committee) are up-to-date
        // for data availability.
        let local_committee = self.local_committee().await?;
        self.communicate_chain_updates(
            &local_committee,
            block.chain_id,
            CommunicateAction::AdvanceToNextBlockHeight {
                height: block.height.try_add_one()?,
                delivery: CrossChainMessageDelivery::Blocking,
            },
        )
        .await?;
        Ok(())
    }

    async fn synchronize_received_certificates_from_validator<A>(
        chain_id: ChainId,
        name: ValidatorName,
        tracker: u64,
        committees: BTreeMap<Epoch, Committee>,
        max_epoch: Epoch,
        mut node: A,
        mut node_client: LocalNodeClient<S>,
    ) -> Result<(ValidatorName, u64, Vec<Certificate>), NodeError>
    where
        A: ValidatorNode + Send + Sync + 'static + Clone,
    {
        // Retrieve newly received certificates from this validator.
        let query = ChainInfoQuery::new(chain_id).with_received_log_excluding_first_nth(tracker);
        let response = node.handle_chain_info_query(query).await?;
        // Responses are authenticated for accountability.
        response.check(name)?;
        let mut certificates = Vec::new();
        let mut new_tracker = tracker;
        for entry in response.info.requested_received_log {
            let query = ChainInfoQuery::new(entry.chain_id)
                .with_sent_certificates_in_range(BlockHeightRange::single(entry.height));
            let local_response = node_client
                .handle_chain_info_query(query.clone())
                .await
                .map_err(|error| NodeError::LocalNodeQuery {
                    error: error.to_string(),
                })?;
            if !local_response.info.requested_sent_certificates.is_empty() {
                new_tracker += 1;
                continue;
            }

            let mut response = node.handle_chain_info_query(query).await?;
            let Some(certificate) = response.info.requested_sent_certificates.pop() else {
                break;
            };
            let CertificateValue::ConfirmedBlock { executed_block, .. } = certificate.value()
            else {
                return Err(NodeError::InvalidChainInfoResponse);
            };
            let block = &executed_block.block;
            // Check that certificates are valid w.r.t one of our trusted committees.
            if block.epoch > max_epoch {
                // We don't accept a certificate from a committee in the future.
                tracing::warn!(
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
                    tracing::warn!(
                        "Skipping received certificate from past epoch {:?}",
                        block.epoch
                    );
                    new_tracker += 1;
                }
            }
        }
        Ok((name, new_tracker, certificates))
    }

    /// Processes the result of [`synchronize_received_certificates_from_validator`].
    async fn receive_certificates_from_validator(
        &mut self,
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
                tracing::warn!("Received invalid certificate {hash} from {name}: {e}");
                // Do not update the validator's tracker in case of error.
                // Move on to the next validator.
                return;
            }
        }
        // Update tracker.
        self.received_certificate_trackers
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

    /// Attempts to download new received certificates.
    ///
    /// This is a best effort: it will only find certificates that have been confirmed
    /// amongst sufficiently many validators of the current committee of the target
    /// chain.
    ///
    /// However, this should be the case whenever a sender's chain is still in use and
    /// is regularly upgraded to new committees.
    async fn find_received_certificates(&mut self) -> Result<(), ChainClientError> {
        // Use network information from the local chain.
        let chain_id = self.chain_id;
        let local_committee = self.local_committee().await?;
        let nodes: Vec<_> = self.validator_node_provider.make_nodes(&local_committee)?;
        // Synchronize the state of the admin chain from the network.
        self.node_client
            .synchronize_chain_state(nodes.clone(), self.admin_id)
            .await?;
        let node_client = self.node_client.clone();
        // Now we should have a complete view of all committees in the system.
        let (committees, max_epoch) = self.known_committees().await?;
        // Proceed to downloading received certificates.
        let trackers = &self.received_certificate_trackers;
        let result = communicate_with_quorum(
            &nodes,
            &local_committee,
            |_| (),
            |name, node| {
                let tracker = *trackers.get(&name).unwrap_or(&0);
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

    /// Attempts to download new received certificates from a particular validator.
    ///
    /// This is similar to `find_received_certificates` but for only one validator.
    /// We also don't try to synchronize the admin chain.
    pub async fn find_received_certificates_from_validator<A>(
        this: Arc<Mutex<Self>>,
        name: ValidatorName,
        node: A,
        node_client: LocalNodeClient<S>,
    ) -> Result<(), ChainClientError>
    where
        A: ValidatorNode + Send + Sync + 'static + Clone,
    {
        let ((committees, max_epoch), chain_id, current_tracker) = {
            let mut guard = this.lock().await;
            (
                guard.known_committees().await?,
                guard.chain_id(),
                *guard.received_certificate_trackers.get(&name).unwrap_or(&0),
            )
        };
        // Proceed to downloading received certificates.
        let (name, tracker, certificates) = Self::synchronize_received_certificates_from_validator(
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
        this.lock()
            .await
            .receive_certificates_from_validator(name, tracker, certificates)
            .await;
        Ok(())
    }

    /// Sends money.
    pub async fn transfer(
        &mut self,
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

    /// Claims money in a remote chain.
    pub async fn claim(
        &mut self,
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

    async fn process_certificate(
        &mut self,
        certificate: Certificate,
        blobs: Vec<HashedValue>,
    ) -> Result<(), LocalNodeError> {
        let info = self
            .node_client
            .handle_certificate(certificate, blobs)
            .await?
            .info;
        self.update_from_info(&info);
        Ok(())
    }

    /// Updates the latest block and next block height and round information from the chain info.
    fn update_from_info(&mut self, info: &ChainInfo) {
        if info.chain_id == self.chain_id && info.next_block_height > self.next_block_height {
            self.next_block_height = info.next_block_height;
            self.block_hash = info.block_hash;
            self.timestamp = info.timestamp;
        }
    }

    /// Requests a leader timeout vote from all validators. If a quorum signs it, creates a
    /// certificate and sends it to all validators, to make them enter the next round.
    pub async fn request_leader_timeout(&mut self) -> Result<Certificate, ChainClientError> {
        let chain_id = self.chain_id;
        let query = ChainInfoQuery::new(chain_id).with_committees();
        let info = self.node_client.handle_chain_info_query(query).await?.info;
        let epoch = info.epoch.ok_or(LocalNodeError::InactiveChain(chain_id))?;
        let committee = info
            .requested_committees
            .ok_or(LocalNodeError::InvalidChainInfoResponse)?
            .remove(&epoch)
            .ok_or(LocalNodeError::InactiveChain(chain_id))?;
        let height = info.next_block_height;
        let round = info.manager.current_round;
        let action = CommunicateAction::RequestLeaderTimeout {
            height,
            round,
            epoch,
        };
        let certificate = self
            .communicate_chain_updates(&committee, chain_id, action)
            .await?
            .expect("a certificate");
        let expected_value = CertificateValue::LeaderTimeout {
            height,
            chain_id,
            epoch,
        };
        if *certificate.value() != expected_value || certificate.round != round {
            return Err(ChainClientError::UnexpectedLeaderTimeout);
        }
        self.process_certificate(certificate.clone(), vec![])
            .await?;
        // The block height didn't increase, but this will communicate the timeout as well.
        self.communicate_chain_updates(
            &committee,
            chain_id,
            CommunicateAction::AdvanceToNextBlockHeight {
                height,
                delivery: CrossChainMessageDelivery::NonBlocking,
            },
        )
        .await?;
        Ok(certificate)
    }

    async fn stage_block_execution_and_discard_failing_messages(
        &mut self,
        mut block: Block,
    ) -> Result<ExecutedBlock, ChainClientError> {
        loop {
            let result = self.node_client.stage_block_execution(block.clone()).await;
            if let Err(LocalNodeError::WorkerError(WorkerError::ChainError(chain_error))) = &result
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
            return Ok(result?.0);
        }
    }

    /// Executes (or retries) a regular block proposal. Updates local balance.
    async fn propose_block(&mut self, block: Block) -> Result<Certificate, ChainClientError> {
        ensure!(
            self.pending_block.is_none() || self.pending_block.as_ref() == Some(&block),
            ChainClientError::BlockProposalError(
                "Client state has a different pending block; \
                use the `linera retry-pending-block` command to commit that first"
            )
        );
        ensure!(
            block.height == self.next_block_height,
            ChainClientError::BlockProposalError("Unexpected block height")
        );
        ensure!(
            block.previous_block_hash == self.block_hash,
            ChainClientError::BlockProposalError("Unexpected previous block hash")
        );
        // Gather information on the current local state.
        let manager = self.chain_info_with_manager_values().await?.manager;
        let next_round = if let Some(next_round) = manager.next_round() {
            next_round
        } else if self.pending_block.is_some() {
            manager.current_round
        } else {
            return Err(ChainClientError::BlockProposalError(
                "Cannot propose a block; there is already a proposal in the current round",
            ));
        };
        // Make sure that we follow the steps in the multi-round protocol.
        let validated = manager.highest_validated().cloned();
        if let Some(validated) = &validated {
            ensure!(
                validated.value().block() == Some(&block),
                ChainClientError::BlockProposalError(
                    "A different block has already been validated at this height"
                )
            );
        }
        // Make sure every incoming message succeeds and otherwise remove them.
        // Also, compute the final certified hash while we're at it.
        let executed_block = self
            .stage_block_execution_and_discard_failing_messages(block)
            .await?;
        let block = executed_block.block.clone();
        let hashed_value = if next_round.is_fast() {
            HashedValue::new_confirmed(executed_block)
        } else {
            HashedValue::new_validated(executed_block)
        };
        // Collect the blobs required for execution.
        let committee = self.local_committee().await?;
        let nodes = self.validator_node_provider.make_nodes(&committee)?;
        let blobs = self
            .node_client
            .read_or_download_blobs(nodes, block.bytecode_locations())
            .await?;
        // Create the final block proposal.
        let key_pair = self.key_pair().await?;
        let proposal = BlockProposal::new(
            BlockAndRound {
                block: block.clone(),
                round: next_round,
            },
            key_pair,
            blobs,
            validated,
        );
        // Check the final block proposal. This will be cheaper after #1401.
        self.node_client
            .handle_block_proposal(proposal.clone())
            .await?;
        // Remember what we are trying to do before sending the proposal to the validators.
        self.pending_block = Some(block);
        // Send the query to validators.
        let certificate = self
            .submit_block_proposal(&committee, proposal, hashed_value)
            .await?;
        // Since `handle_block_proposal` succeeded, we have the needed bytecode.
        // Leaving blobs empty.
        self.process_certificate(certificate.clone(), vec![])
            .await?;
        self.pending_block = None;
        // Communicate the new certificate now.
        self.communicate_chain_updates(
            &committee,
            self.chain_id,
            CommunicateAction::AdvanceToNextBlockHeight {
                height: self.next_block_height,
                delivery: self.cross_chain_message_delivery,
            },
        )
        .await?;
        if let Ok(new_committee) = self.local_committee().await {
            if new_committee != committee {
                // If the configuration just changed, communicate to the new committee as well.
                // (This is actually more important that updating the previous committee.)
                self.communicate_chain_updates(
                    &new_committee,
                    self.chain_id,
                    CommunicateAction::AdvanceToNextBlockHeight {
                        height: self.next_block_height,
                        delivery: self.cross_chain_message_delivery,
                    },
                )
                .await?;
            }
        }
        Ok(certificate)
    }

    /// Executes a list of operations.
    pub async fn execute_operations(
        &mut self,
        operations: Vec<Operation>,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.prepare_chain().await?;
        self.execute_with_messages(operations).await
    }

    /// Executes a list of operations, without calling `prepare_chain`.
    pub async fn execute_with_messages(
        &mut self,
        operations: Vec<Operation>,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        loop {
            let messages = self.pending_messages().await?;
            match self.execute_block(messages, operations.clone()).await? {
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
    pub async fn execute_operation(
        &mut self,
        operation: Operation,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.execute_operations(vec![operation]).await
    }

    /// Executes a new block.
    ///
    /// This must be preceded by a call to `prepare_chain()`.
    async fn execute_block(
        &mut self,
        incoming_messages: Vec<IncomingMessage>,
        operations: Vec<Operation>,
    ) -> Result<ExecuteBlockOutcome, ChainClientError> {
        loop {
            let identity = self.identity().await?;
            let info = self.chain_info_with_manager_values().await?;
            let manager = &*info.manager;
            let can_propose = match manager.next_round() {
                Some(Round::Fast) => manager.ownership.super_owners.contains_key(&identity),
                Some(Round::MultiLeader(_)) => true,
                Some(Round::SingleLeader(_)) => manager.leader == Some(identity),
                None => false,
            };
            // If blocks are already validated, try to finalize the highest one.
            if let Some(certificate) = manager.highest_validated() {
                if certificate.round == manager.current_round {
                    let committee = self.local_committee().await?;
                    return match self.finalize_block(&committee, certificate.clone()).await {
                        Ok(certificate) => Ok(ExecuteBlockOutcome::Conflict(certificate)),
                        Err(error) => {
                            tracing::warn!(
                                %error, round = %manager.current_round,
                                "Failed to finalize pending validated block."
                            );
                            // TODO(#1423): The round just ended; or are there other errors?
                            Ok(ExecuteBlockOutcome::wait_for_timeout(&info))
                        }
                    };
                }
                if can_propose {
                    if let Some(block) = certificate.value().block() {
                        return match self.propose_block(block.clone()).await {
                            Ok(certificate) => Ok(ExecuteBlockOutcome::Conflict(certificate)),
                            Err(error @ ChainClientError::CommunicationError(_))
                            | Err(error @ ChainClientError::LocalNodeError(_)) => Err(error),
                            Err(error) => {
                                tracing::warn!(
                                    %error, round = %manager.current_round,
                                    "Failed to re-propose validated block from an earlier round."
                                );
                                // TODO(#1423): The round just ended; or are there other errors?
                                Ok(ExecuteBlockOutcome::wait_for_timeout(&info))
                            }
                        };
                    }
                }
            }
            // Otherwise we can propose a block with our own messages and operations.
            if can_propose {
                if let Some(block) = &self.pending_block {
                    if block.height == self.next_block_height {
                        return match self.propose_block(block.clone()).await {
                            Ok(certificate) => Ok(ExecuteBlockOutcome::Conflict(certificate)),
                            Err(error @ ChainClientError::CommunicationError(_))
                            | Err(error @ ChainClientError::LocalNodeError(_)) => Err(error),
                            Err(error) => {
                                tracing::warn!(
                                    %error, round = %manager.current_round,
                                    "Failed to re-propose local pending block."
                                );
                                // TODO(#1423): The round just ended; or are there other errors?
                                Ok(ExecuteBlockOutcome::wait_for_timeout(&info))
                            }
                        };
                    }
                }
                let timestamp = self.next_timestamp(&incoming_messages).await;
                let block = Block {
                    epoch: self.epoch().await?,
                    chain_id: self.chain_id,
                    incoming_messages,
                    operations,
                    previous_block_hash: self.block_hash,
                    height: self.next_block_height,
                    authenticated_signer: Some(self.identity().await?),
                    timestamp,
                };
                return match self.propose_block(block.clone()).await {
                    Ok(certificate) => Ok(ExecuteBlockOutcome::Executed(certificate)),
                    Err(error @ ChainClientError::CommunicationError(_))
                    | Err(error @ ChainClientError::LocalNodeError(_)) => Err(error),
                    Err(error) => {
                        tracing::warn!(
                            %error, round = %manager.current_round,
                            "Failed to propose new block."
                        );
                        // TODO(#1423): The round just ended; or are there other errors?
                        Ok(ExecuteBlockOutcome::wait_for_timeout(&info))
                    }
                };
            }
            // If there is already a valid proposal in this round, try to finalize it.
            if let Some(proposal) = &manager.requested_proposed {
                if proposal.content.round == manager.current_round {
                    let committee = self.local_committee().await?;
                    let (executed_block, _) = self
                        .node_client
                        .stage_block_execution(proposal.content.block.clone())
                        .await?;
                    let hashed_value = if proposal.content.round.is_fast() {
                        HashedValue::new_confirmed(executed_block)
                    } else {
                        HashedValue::new_validated(executed_block)
                    };
                    return match self
                        .submit_block_proposal(&committee, (**proposal).clone(), hashed_value)
                        .await
                    {
                        Ok(certificate) => Ok(ExecuteBlockOutcome::Conflict(certificate)),
                        Err(error) => {
                            tracing::warn!(
                                %error, round = %manager.current_round,
                                "Failed to propose pending block."
                            );
                            // TODO(#1423): The round just ended; or are there other errors?
                            Ok(ExecuteBlockOutcome::wait_for_timeout(&info))
                        }
                    };
                }
            }
            // But if the current round has not timed out yet, we have to wait.
            if manager.round_timeout > self.storage_client().await.current_time() {
                let timeout = RoundTimeout {
                    timestamp: manager.round_timeout,
                    current_round: manager.current_round,
                    next_block_height: info.next_block_height,
                };
                return Ok(ExecuteBlockOutcome::WaitForTimeout(timeout));
            }
            // If it has timed out, we request a timeout certificate and retry in the next round.
            self.request_leader_timeout().await?;
        }
    }

    /// Returns a suitable timestamp for the next block.
    ///
    /// This will usually be the current time according to the local clock, but may be slightly
    /// ahead to make sure it's not earlier than the incoming messages or the previous block.
    async fn next_timestamp(&self, incoming_messages: &[IncomingMessage]) -> Timestamp {
        let local_time = self.storage_client().await.current_time();
        incoming_messages
            .iter()
            .map(|msg| msg.event.timestamp)
            .max()
            .map_or(local_time, |timestamp| timestamp.max(local_time))
            .max(self.timestamp)
    }

    /// Queries an application.
    pub async fn query_application(&self, query: Query) -> Result<Response, ChainClientError> {
        let response = self
            .node_client
            .query_application(self.chain_id, query)
            .await?;
        Ok(response)
    }

    /// Queries a system application.
    pub async fn query_system_application(
        &mut self,
        query: SystemQuery,
    ) -> Result<SystemResponse, ChainClientError> {
        let response = self
            .node_client
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
    pub async fn query_user_application<A: Abi>(
        &mut self,
        application_id: UserApplicationId<A>,
        query: &A::Query,
    ) -> Result<A::QueryResponse, ChainClientError> {
        let query = Query::user(application_id, query)?;
        let response = self
            .node_client
            .query_application(self.chain_id, query)
            .await?;
        match response {
            Response::User(response) => Ok(serde_json::from_slice(&response)?),
            _ => Err(ChainClientError::InternalError(
                "Unexpected response for user query",
            )),
        }
    }

    pub async fn local_balance(&mut self) -> Result<Amount, ChainClientError> {
        ensure!(
            self.chain_info().await?.next_block_height == self.next_block_height,
            ChainClientError::WalletSynchronizationError
        );
        let incoming_messages = self.pending_system_messages().await?;
        let timestamp = self.next_timestamp(&incoming_messages).await;
        let block = Block {
            epoch: self.epoch().await?,
            chain_id: self.chain_id,
            incoming_messages,
            operations: Vec::new(),
            previous_block_hash: self.block_hash,
            height: self.next_block_height,
            authenticated_signer: None,
            timestamp,
        };
        let (_, response) = self.node_client.stage_block_execution(block).await?;
        Ok(response.info.system_balance)
    }

    /// Attempts to update all validators about the local chain.
    pub async fn update_validators(&mut self) -> Result<(), ChainClientError> {
        let committee = self.local_committee().await?;
        self.communicate_chain_updates(
            &committee,
            self.chain_id,
            CommunicateAction::AdvanceToNextBlockHeight {
                height: self.next_block_height,
                delivery: CrossChainMessageDelivery::NonBlocking,
            },
        )
        .await?;
        Ok(())
    }

    /// Requests a `RegisterApplications` message from another chain so the application can be used
    /// on this one.
    pub async fn request_application(
        &mut self,
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
    pub async fn transfer_to_account(
        &mut self,
        owner: Option<Owner>,
        amount: Amount,
        account: Account,
        user_data: UserData,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.transfer(owner, amount, Recipient::Account(account), user_data)
            .await
    }

    /// Burns tokens.
    pub async fn burn(
        &mut self,
        owner: Option<Owner>,
        amount: Amount,
        user_data: UserData,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.transfer(owner, amount, Recipient::Burn, user_data)
            .await
    }

    /// Attempts to synchronize with validators and re-compute our balance.
    pub async fn synchronize_from_validators(&mut self) -> Result<Amount, ChainClientError> {
        self.find_received_certificates().await?;
        self.prepare_chain().await?;
        self.local_balance().await
    }

    /// Retries the last pending block
    pub async fn retry_pending_block(&mut self) -> Result<Option<Certificate>, ChainClientError> {
        self.find_received_certificates().await?;
        self.prepare_chain().await?;
        match &self.pending_block {
            Some(block) => {
                // Finish executing the previous block.
                let block = block.clone();
                let certificate = self.propose_block(block).await?;
                Ok(Some(certificate))
            }
            None => Ok(None),
        }
    }

    /// Clears the information on any operation that previously failed.
    pub async fn clear_pending_block(&mut self) {
        self.pending_block = None;
    }

    /// Processes confirmed operation for which this chain is a recipient.
    pub async fn receive_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<(), ChainClientError> {
        self.receive_certificate_internal(certificate, ReceiveCertificateMode::NeedsCheck)
            .await
    }

    /// Rotates the key of the chain.
    pub async fn rotate_key_pair(
        &mut self,
        key_pair: KeyPair,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        let new_public_key = key_pair.public();
        self.known_key_pairs.insert(new_public_key.into(), key_pair);
        self.transfer_ownership(new_public_key).await
    }

    /// Transfers ownership of the chain.
    pub async fn transfer_ownership(
        &mut self,
        new_public_key: PublicKey,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::ChangeOwnership {
            super_owners: vec![new_public_key],
            owners: Vec::new(),
            multi_leader_rounds: 2,
        }))
        .await
    }

    /// Adds another owner to the chain.
    ///
    /// If the chain is currently of type `Single`, the existing owner's weight is set to 100, and
    /// the first two rounds are multi-leader.
    pub async fn share_ownership(
        &mut self,
        new_public_key: PublicKey,
        new_weight: u64,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        loop {
            let ownership = self.prepare_chain().await?.manager.ownership;
            ensure!(
                ownership.is_active(),
                ChainError::InactiveChain(self.chain_id)
            );
            let messages = self.pending_messages().await?;
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
            })];
            match self.execute_block(messages, operations).await? {
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

    /// Opens a new chain with a derived UID.
    pub async fn open_chain(
        &mut self,
        ownership: ChainOwnership,
        balance: Amount,
    ) -> Result<ClientOutcome<(MessageId, Certificate)>, ChainClientError> {
        self.prepare_chain().await?;
        loop {
            let (epoch, committees) = self.epoch_and_committees(self.chain_id).await?;
            let epoch = epoch.ok_or(LocalNodeError::InactiveChain(self.chain_id))?;
            let messages = self.pending_messages().await?;
            let certificate = match self
                .execute_block(
                    messages,
                    vec![Operation::System(SystemOperation::OpenChain {
                        ownership: ownership.clone(),
                        committees,
                        admin_id: self.admin_id,
                        epoch,
                        balance,
                    })],
                )
                .await?
            {
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

    /// Closes the chain (and loses everything in it!!).
    pub async fn close_chain(&mut self) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::CloseChain))
            .await
    }

    /// Publishes some bytecode.
    pub async fn publish_bytecode(
        &mut self,
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

    /// Creates an application by instantiating some bytecode.
    pub async fn create_application<A: Abi>(
        &mut self,
        bytecode_id: BytecodeId<A>,
        parameters: &<A as ContractAbi>::Parameters,
        initialization_argument: &A::InitializationArgument,
        required_application_ids: Vec<UserApplicationId>,
    ) -> Result<ClientOutcome<(UserApplicationId<A>, Certificate)>, ChainClientError> {
        let initialization_argument = serde_json::to_vec(initialization_argument)?;
        let parameters = serde_json::to_vec(parameters)?;
        Ok(self
            .create_application_untyped(
                bytecode_id.forget_abi(),
                parameters,
                initialization_argument,
                required_application_ids,
            )
            .await?
            .map(|(app_id, cert)| (app_id.with_abi(), cert)))
    }

    /// Creates an application by instantiating some bytecode.
    pub async fn create_application_untyped(
        &mut self,
        bytecode_id: BytecodeId,
        parameters: Vec<u8>,
        initialization_argument: Vec<u8>,
        required_application_ids: Vec<UserApplicationId>,
    ) -> Result<ClientOutcome<(UserApplicationId, Certificate)>, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::CreateApplication {
            bytecode_id,
            parameters,
            initialization_argument,
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
    pub async fn stage_new_committee(
        &mut self,
        committee: Committee,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        loop {
            self.prepare_chain().await?;
            let epoch = self.epoch().await?;
            let messages = self.pending_messages().await?;
            match self
                .execute_block(
                    messages,
                    vec![Operation::System(SystemOperation::Admin(
                        AdminOperation::CreateCommittee {
                            epoch: epoch.try_add_one()?,
                            committee: committee.clone(),
                        },
                    ))],
                )
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

    /// Creates an empty block to process all incoming messages. This may require several blocks.
    ///
    /// If not all certificates could be processed due to a timeout, the timestamp for when to retry
    /// is returned, too.
    pub async fn process_inbox(
        &mut self,
    ) -> Result<(Vec<Certificate>, Option<RoundTimeout>), ChainClientError> {
        self.prepare_chain().await?;
        let mut certificates = Vec::new();
        loop {
            let incoming_messages = self.pending_messages().await?;
            if incoming_messages.is_empty() {
                break;
            }
            match self.execute_block(incoming_messages, vec![]).await {
                Ok(ExecuteBlockOutcome::Executed(certificate))
                | Ok(ExecuteBlockOutcome::Conflict(certificate)) => certificates.push(certificate),
                Ok(ExecuteBlockOutcome::WaitForTimeout(timeout)) => {
                    return Ok((certificates, Some(timeout)));
                }
                Err(error) => return Err(error),
            };
        }
        Ok((certificates, None))
    }

    /// Creates an empty block to process all incoming messages. This may require several blocks.
    /// If we are not a chain owner, this doesn't fail, and just returns an empty list.
    pub async fn process_inbox_if_owned(
        &mut self,
    ) -> Result<(Vec<Certificate>, Option<RoundTimeout>), ChainClientError> {
        match self.process_inbox().await {
            Ok(result) => Ok(result),
            Err(ChainClientError::CannotFindKeyForChain(_)) => Ok((Vec::new(), None)),
            Err(error) => Err(error),
        }
    }

    /// Starts listening to the admin chain for new committees. (This is only useful for
    /// other genesis chains or for testing.)
    pub async fn subscribe_to_new_committees(
        &mut self,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::Subscribe {
            chain_id: self.admin_id,
            channel: SystemChannel::Admin,
        }))
        .await
    }

    /// Stops listening to the admin chain for new committees. (This is only useful for
    /// testing.)
    pub async fn unsubscribe_from_new_committees(
        &mut self,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::Unsubscribe {
            chain_id: self.admin_id,
            channel: SystemChannel::Admin,
        }))
        .await
    }

    /// Starts listening to the given chain for published bytecodes.
    pub async fn subscribe_to_published_bytecodes(
        &mut self,
        chain_id: ChainId,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::Subscribe {
            chain_id,
            channel: SystemChannel::PublishedBytecodes,
        }))
        .await
    }

    /// Stops listening to the given chain for published bytecodes.
    pub async fn unsubscribe_from_published_bytecodes(
        &mut self,
        chain_id: ChainId,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::Unsubscribe {
            chain_id,
            channel: SystemChannel::PublishedBytecodes,
        }))
        .await
    }

    /// Deprecates all the configurations of voting rights but the last one (admin chains
    /// only). Currently, each individual chain is still entitled to wait before accepting
    /// this command. However, it is expected that deprecated validators stop functioning
    /// shortly after such command is issued.
    pub async fn finalize_committee(
        &mut self,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
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
        self.execute_with_messages(operations).await
    }

    /// Sends money to a chain.
    /// Do not check balance. (This may block the client)
    /// Do not confirm the transaction.
    pub async fn transfer_to_account_unsafe_unconfirmed(
        &mut self,
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

    pub async fn read_value(&self, hash: CryptoHash) -> Result<HashedValue, ViewError> {
        self.storage_client().await.read_value(hash).await
    }

    pub async fn read_values_downward(
        &self,
        from: CryptoHash,
        limit: u32,
    ) -> Result<Vec<HashedValue>, ViewError> {
        self.storage_client()
            .await
            .read_values_downward(from, limit)
            .await
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

impl ExecuteBlockOutcome {
    fn wait_for_timeout(info: &ChainInfo) -> Self {
        // TODO(#1424): Local timeout might not match validators' exactly.
        Self::WaitForTimeout(RoundTimeout {
            timestamp: info.manager.round_timeout,
            current_round: info.manager.current_round,
            next_block_height: info.next_block_height,
        })
    }
}

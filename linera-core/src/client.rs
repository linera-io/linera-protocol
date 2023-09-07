// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    data_types::{BlockHeightRange, ChainInfo, ChainInfoQuery},
    local_node::{LocalNodeClient, LocalNodeError},
    node::{NodeError, NotificationStream, ValidatorNode, ValidatorNodeProvider},
    updater::{communicate_with_quorum, CommunicateAction, CommunicationError, ValidatorUpdater},
    worker::{Notification, Reason, WorkerError, WorkerState},
};
use futures::{
    future,
    lock::Mutex,
    stream::{self, FuturesUnordered, StreamExt},
};
use linera_base::{
    abi::{Abi, ContractAbi},
    crypto::{CryptoHash, KeyPair, PublicKey},
    data_types::{Amount, ArithmeticError, BlockHeight, RoundNumber, Timestamp},
    ensure,
    identifiers::{BytecodeId, ChainId, MessageId, Owner},
};
use linera_chain::{
    data_types::{
        Block, BlockAndRound, BlockProposal, Certificate, CertificateValue, HashedValue,
        IncomingMessage, LiteCertificate, LiteVote,
    },
    ChainError, ChainManagerInfo, ChainStateView,
};
use linera_execution::{
    committee::{Committee, Epoch, ValidatorName},
    system::{Account, AdminOperation, Recipient, SystemChannel, SystemOperation, UserData},
    Bytecode, ChainOwnership, Message, Operation, Query, Response, SystemMessage, SystemQuery,
    SystemResponse, UserApplicationId,
};
use linera_storage::Store;
use linera_views::views::ViewError;
use std::{
    collections::{hash_map, BTreeMap, HashMap},
    iter,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio_stream::Stream;
use tracing::{debug, error, info};

#[cfg(any(test, feature = "test"))]
#[path = "unit_tests/client_test_utils.rs"]
pub mod client_test_utils;

#[cfg(test)]
#[path = "unit_tests/client_tests.rs"]
mod client_tests;

/// Client to operate a chain by interacting with validators and the given local storage
/// implementation.
/// * The chain being operated is called the "local chain" or just the "chain".
/// * As a rule, operations are considered successful (and communication may stop) when
/// they succeeded in gathering a quorum of responses.
pub struct ChainClient<ValidatorNodeProvider, StorageClient> {
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
    /// Round number that we plan to use for the next block.
    next_round: RoundNumber,
    /// Pending block.
    pending_block: Option<Block>,
    /// Known key pairs from present and past identities.
    known_key_pairs: BTreeMap<Owner, KeyPair>,
    /// The id of the admin chain.
    admin_id: ChainId,

    /// Maximum number of pending messages processed at a time in a block.
    max_pending_messages: usize,
    /// Support synchronization of received certificates.
    received_certificate_trackers: HashMap<ValidatorName, u64>,
    /// How much time to wait between attempts when we wait for a cross-chain update.
    cross_chain_delay: Duration,
    /// How many times we are willing to retry a block that depends on cross-chain updates.
    cross_chain_retries: usize,
    /// Local node to manage the execution state and the local storage of the chains that we are
    /// tracking.
    node_client: LocalNodeClient<StorageClient>,
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

    #[error("No key available to interact with single-owner chain {0}")]
    CannotFindKeyForSingleOwnerChain(ChainId),

    #[error("No key available to interact with multi-owner chain {0}")]
    CannotFindKeyForMultiOwnerChain(ChainId),

    #[error("Found several possible identities to interact with multi-owner chain {0}")]
    FoundMultipleKeysForMultiOwnerChain(ChainId),
}

impl<P, S> ChainClient<P, S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chain_id: ChainId,
        known_key_pairs: Vec<KeyPair>,
        validator_node_provider: P,
        storage_client: S,
        admin_id: ChainId,
        max_pending_messages: usize,
        block_hash: Option<CryptoHash>,
        timestamp: Timestamp,
        next_block_height: BlockHeight,
        cross_chain_delay: Duration,
        cross_chain_retries: usize,
    ) -> Self {
        let known_key_pairs = known_key_pairs
            .into_iter()
            .map(|kp| (Owner::from(kp.public()), kp))
            .collect();
        let state = WorkerState::new(format!("Client node {:?}", chain_id), None, storage_client)
            .with_allow_inactive_chains(true)
            .with_allow_messages_from_deprecated_epochs(true);
        let node_client = LocalNodeClient::new(state);
        Self {
            chain_id,
            validator_node_provider,
            block_hash,
            timestamp,
            next_block_height,
            next_round: RoundNumber::default(),
            pending_block: None,
            known_key_pairs,
            admin_id,
            max_pending_messages,
            received_certificate_trackers: HashMap::new(),
            cross_chain_delay,
            cross_chain_retries,
            node_client,
        }
    }

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
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    /// Obtains a `ChainStateView` for a given `ChainId`.
    pub async fn chain_state_view(
        &self,
        chain_id: Option<ChainId>,
    ) -> Result<Arc<ChainStateView<S::Context>>, LocalNodeError> {
        let chain_id = chain_id.unwrap_or(self.chain_id);
        let chain_state_view = self
            .node_client
            .storage_client()
            .await
            .load_chain(chain_id)
            .await?;
        Ok(Arc::new(chain_state_view))
    }

    /// Obtains the basic `ChainInfo` data for the local chain.
    async fn chain_info(&mut self) -> Result<ChainInfo, LocalNodeError> {
        let query = ChainInfoQuery::new(self.chain_id);
        let response = self.node_client.handle_chain_info_query(query).await?;
        Ok(response.info)
    }

    /// Obtains up to `self.max_pending_messages` pending messages for the local chain.
    ///
    /// Messages known to be redundant are filtered out: A `RegisterApplications` message whose
    /// entries are already known never needs to be included in a block.
    async fn pending_messages(&mut self) -> Result<Vec<IncomingMessage>, LocalNodeError> {
        let query = ChainInfoQuery::new(self.chain_id).with_pending_messages();
        let response = self.node_client.handle_chain_info_query(query).await?;
        let mut pending_messages = vec![];
        for message in response.info.requested_pending_messages {
            if pending_messages.len() >= self.max_pending_messages {
                tracing::warn!(
                    "Limiting block from {} to {} incoming messages",
                    pending_messages.len(),
                    self.max_pending_messages
                );
                break;
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
        match self.chain_info().await?.manager {
            ChainManagerInfo::Single(manager) => {
                if !self.known_key_pairs.contains_key(&manager.owner) {
                    return Err(ChainClientError::CannotFindKeyForSingleOwnerChain(
                        self.chain_id,
                    ));
                }
                Ok(manager.owner)
            }
            ChainManagerInfo::Multi(manager) => {
                let mut identities = manager
                    .public_keys
                    .keys()
                    .filter(|owner| self.known_key_pairs.contains_key(owner));
                let Some(identity) = identities.next() else {
                    return Err(ChainClientError::CannotFindKeyForMultiOwnerChain(
                        self.chain_id,
                    ));
                };
                ensure!(
                    identities.next().is_none(),
                    ChainClientError::FoundMultipleKeysForMultiOwnerChain(self.chain_id)
                );
                Ok(*identity)
            }
            ChainManagerInfo::None => Err(LocalNodeError::InactiveChain(self.chain_id).into()),
        }
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
    ) -> Option<ChainInfo> {
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
    ) -> bool
    where
        A: ValidatorNode + Send + Sync + 'static + Clone,
    {
        match notification.reason {
            Reason::NewIncomingMessage { origin, height } => {
                if Self::local_next_block_height(this.clone(), origin.sender, &mut local_node).await
                    > Some(height)
                {
                    debug!("Accepting redundant notification for new message");
                    return true;
                }
                if let Err(e) =
                    Self::find_received_certificates_from_validator(this.clone(), name, node).await
                {
                    error!("Fail to process notification: {e}");
                    return false;
                }
                if Self::local_next_block_height(this, origin.sender, &mut local_node).await
                    <= Some(height)
                {
                    error!("Fail to synchronize new message after notification");
                    return false;
                }
            }
            Reason::NewBlock { height, hash } => {
                let chain_id = notification.chain_id;
                if Self::local_next_block_height(this.clone(), chain_id, &mut local_node).await
                    > Some(height)
                {
                    debug!("Accepting redundant notification for new block");
                    return true;
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
                            return false;
                        }
                        // TODO(#940): Avoid reading the block we just stored.
                        match local_node
                            .storage_client()
                            .await
                            .read_certificate(hash)
                            .await
                        {
                            Ok(certificate)
                                if certificate.value().chain_id() == chain_id
                                    && certificate.value().height() == height => {}
                            Ok(_) => {
                                error!("Certificate hash in notification doesn't match");
                                return false;
                            }
                            Err(error) => {
                                error!(%error, "Could not download certificate with hash {hash}.");
                                return false;
                            }
                        }
                    }
                    Err(e) => {
                        error!("Fail to process notification: {e}");
                        return false;
                    }
                }
            }
            Reason::NewRound { height, round } => {
                let chain_id = notification.chain_id;
                if let Some(info) =
                    Self::local_chain_info(this.clone(), chain_id, &mut local_node).await
                {
                    if (info.next_block_height, info.manager.next_round()) >= (height, round) {
                        debug!("Accepting redundant notification for new round");
                        return true;
                    }
                }
                if let Err(error) = local_node
                    .try_synchronize_chain_state_from(name, node, chain_id)
                    .await
                {
                    error!("Fail to process notification: {error}");
                    return false;
                }
                let Some(info) =
                    Self::local_chain_info(this.clone(), chain_id, &mut local_node).await
                else {
                    error!("Fail to read local chain info for {chain_id}");
                    return false;
                };
                if (info.next_block_height, info.manager.next_round()) < (height, round) {
                    error!("Fail to synchronize new block after notification");
                    return false;
                }
            }
        }
        // Accept the notification.
        true
    }

    /// Listens to notifications about the current chain from all validators.
    pub async fn listen(this: Arc<Mutex<Self>>) -> Result<NotificationStream, ChainClientError>
    where
        P: Send + 'static,
    {
        let mut streams = HashMap::new();
        if let Err(err) = Self::update_streams(&this, &mut streams).await {
            error!("Failed to update committee: {}", err);
        }
        let stream = stream::unfold(streams, move |mut streams| {
            let this = this.clone();
            async move {
                let nexts = streams.values_mut().map(StreamExt::next);
                let notification = future::select_all(nexts).await.0?;
                if matches!(notification.reason, Reason::NewBlock { .. }) {
                    if let Err(err) = Self::update_streams(&this, &mut streams).await {
                        error!("Failed to update committee: {}", err);
                    }
                }
                Some((notification, streams))
            }
        });
        Ok(Box::pin(stream))
    }

    async fn update_streams(
        this: &Arc<Mutex<Self>>,
        streams: &mut HashMap<ValidatorName, Pin<Box<dyn Stream<Item = Notification> + Send>>>,
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
        // Drop notification streams from removed validators.
        streams.retain(|name, _| nodes.contains_key(name));
        // Add notification streams from added validators.
        for (name, mut node) in nodes {
            let hash_map::Entry::Vacant(entry) = streams.entry(name) else {
                continue;
            };
            let stream = match node.subscribe(vec![chain_id]).await {
                Err(e) => {
                    info!("Could not connect to validator {name}: {e:?}");
                    continue;
                }
                Ok(stream) => stream,
            };
            let this = this.clone();
            let local_node = local_node.clone();
            let stream = stream.filter(move |notification| {
                Box::pin(Self::process_notification(
                    this.clone(),
                    name,
                    node.clone(),
                    local_node.clone(),
                    notification.clone(),
                ))
            });
            entry.insert(Box::pin(stream));
        }
        Ok(())
    }

    /// Prepares the chain for the next operation.
    async fn prepare_chain(&mut self) -> Result<ChainInfo, ChainClientError> {
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
        if !matches!(
            &info.manager,
            ChainManagerInfo::Single(single) if self.known_key_pairs.contains_key(&single.owner))
        {
            // For multi-owner chains, or for single-owner chains that are owned by someone else, we
            // could be missing recent certificates created by other owners. Further synchronize
            // blocks from the network. This is a best-effort that depends on network conditions.
            let nodes = self.validator_nodes().await?;
            info = self
                .node_client
                .synchronize_chain_state(nodes, self.chain_id)
                .await?;
        }
        self.update_from_info(&info);
        Ok(info)
    }

    /// Broadcasts certified blocks and optionally one more block proposal.
    /// The corresponding block heights should be consecutive and increasing.
    async fn communicate_chain_updates(
        &mut self,
        committee: &Committee,
        chain_id: ChainId,
        action: CommunicateAction,
    ) -> Result<Option<Certificate>, ChainClientError> {
        let storage_client = self.node_client.storage_client().await;
        let cross_chain_delay = self.cross_chain_delay;
        let cross_chain_retries = self.cross_chain_retries;
        let nodes: Vec<_> = self.validator_node_provider.make_nodes(committee)?;
        let result = communicate_with_quorum(
            &nodes,
            committee,
            |value: &Option<LiteVote>| -> Option<_> {
                value
                    .as_ref()
                    .map(|vote| (vote.value.value_hash, vote.round))
            },
            |name, client| {
                let mut updater = ValidatorUpdater {
                    name,
                    client,
                    store: storage_client.clone(),
                    delay: cross_chain_delay,
                    retries: cross_chain_retries,
                };
                let action = action.clone();
                Box::pin(async move { updater.send_chain_update(chain_id, action).await })
            },
        )
        .await;
        let (value, round) = match action {
            CommunicateAction::SubmitBlockForConfirmation(proposal) => {
                let block = proposal.content.block;
                let (executed_block, _) = self.node_client.stage_block_execution(block).await?;
                (
                    HashedValue::from(CertificateValue::ConfirmedBlock { executed_block }),
                    proposal.content.round,
                )
            }
            CommunicateAction::SubmitBlockForValidation(proposal) => {
                let BlockAndRound { block, round } = proposal.content;
                let (executed_block, _) = self.node_client.stage_block_execution(block).await?;
                (HashedValue::new_validated(executed_block), round)
            }
            CommunicateAction::FinalizeBlock(validity_certificate) => {
                let round = validity_certificate.round;
                let Some(conf_value) = validity_certificate.value.into_confirmed() else {
                    return Err(ChainClientError::ProtocolError(
                        "Unexpected certificate value for finalized block",
                    ));
                };
                (conf_value, round)
            }
            CommunicateAction::AdvanceToNextBlockHeight(_) => {
                return match result {
                    Ok(_) => Ok(None),
                    Err(CommunicationError::Trusted(NodeError::InactiveChain(id)))
                        if id == chain_id =>
                    {
                        Ok(None)
                    }
                    Err(error) => Err(error.into()),
                };
            }
        };
        let votes = match result? {
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
                let blobs: Vec<HashedValue> = future::join_all(locations.iter().map(|location| {
                    LocalNodeClient::<S>::download_blob(nodes.clone(), block.chain_id, *location)
                }))
                .await
                .into_iter()
                .flatten()
                .collect();
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
            CommunicateAction::AdvanceToNextBlockHeight(block.height.try_add_one()?),
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
        mut client: A,
    ) -> Result<(ValidatorName, u64, Vec<Certificate>), NodeError>
    where
        A: ValidatorNode + Send + Sync + 'static + Clone,
    {
        // Retrieve newly received certificates from this validator.
        let query = ChainInfoQuery::new(chain_id).with_received_log_excluding_first_nth(tracker);
        let response = client.handle_chain_info_query(query).await?;
        // Responses are authenticated for accountability.
        response.check(name)?;
        let mut certificates = Vec::new();
        let mut new_tracker = tracker;
        for entry in response.info.requested_received_log {
            let query = ChainInfoQuery::new(entry.chain_id)
                .with_sent_certificates_in_range(BlockHeightRange::single(entry.height));

            let mut response = client.handle_chain_info_query(query).await?;
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
        // Now we should have a complete view of all committees in the system.
        let (committees, max_epoch) = self.known_committees().await?;
        // Proceed to downloading received certificates.
        let trackers = self.received_certificate_trackers.clone();
        let result = communicate_with_quorum(
            &nodes,
            &local_committee,
            |_| (),
            |name, client| {
                let tracker = *trackers.get(&name).unwrap_or(&0);
                let committees = committees.clone();
                Box::pin(Self::synchronize_received_certificates_from_validator(
                    chain_id, name, tracker, committees, max_epoch, client,
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
        client: A,
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
            client,
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
    ) -> Result<Certificate, ChainClientError> {
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
        target: ChainId,
        recipient: Recipient,
        amount: Amount,
        user_data: UserData,
    ) -> Result<Certificate, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::Claim {
            owner,
            target,
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
        if info.chain_id == self.chain_id
            && (info.next_block_height, info.manager.next_round())
                > (self.next_block_height, self.next_round)
        {
            self.block_hash = info.block_hash;
            self.next_block_height = info.next_block_height;
            self.next_round = info.manager.next_round();
            self.timestamp = info.timestamp;
        }
    }

    /// Executes (or retries) a regular block proposal. Updates local balance.
    /// If `with_confirmation` is false, we stop short of executing the finalized block.
    async fn propose_block(&mut self, block: Block) -> Result<Certificate, ChainClientError> {
        let next_round = self.next_round;
        ensure!(
            self.pending_block.is_none() || self.pending_block.as_ref() == Some(&block),
            ChainClientError::BlockProposalError("Client state has a different pending block")
        );
        ensure!(
            block.height == self.next_block_height,
            ChainClientError::BlockProposalError("Unexpected block height")
        );
        ensure!(
            block.previous_block_hash == self.block_hash,
            ChainClientError::BlockProposalError("Unexpected previous block hash")
        );
        // Collect blobs required for execution.
        let committee = self.local_committee().await?;
        let nodes = self.validator_node_provider.make_nodes(&committee)?;
        let blobs = self
            .node_client
            .read_or_download_blobs(nodes, block.bytecode_locations())
            .await?;
        // Build the initial query.
        let manager = self.chain_info().await?.manager;
        let key_pair = self.key_pair().await?;
        let validated = manager.highest_validated().cloned();
        // TODO(#66): return the block that should be proposed instead
        if let Some(validated) = &validated {
            ensure!(
                validated.value().block() == Some(&block),
                ChainClientError::BlockProposalError(
                    "A different block has already been validated at this height"
                )
            );
        }
        let proposal = BlockProposal::new(
            BlockAndRound {
                block: block.clone(),
                round: next_round,
            },
            key_pair,
            blobs,
            validated,
        );
        // Try to execute the block locally first.
        self.node_client
            .handle_block_proposal(proposal.clone())
            .await?;
        // Remember what we are trying to do, before sending the proposal to the validators.
        self.pending_block = Some(block);
        // Send the query to validators.
        let final_certificate = match manager {
            ChainManagerInfo::Multi(_) => {
                // Need two round-trips.
                let certificate = self
                    .communicate_chain_updates(
                        &committee,
                        self.chain_id,
                        CommunicateAction::SubmitBlockForValidation(proposal.clone()),
                    )
                    .await?
                    .expect("a certificate");
                assert!(matches!(
                    certificate.value(),
                    CertificateValue::ValidatedBlock { executed_block, .. }
                        if executed_block.block == proposal.content.block
                ));
                self.communicate_chain_updates(
                    &committee,
                    self.chain_id,
                    CommunicateAction::FinalizeBlock(certificate),
                )
                .await?
                .expect("a certificate")
            }
            ChainManagerInfo::Single(_) => {
                // Only one round-trip is needed
                self.communicate_chain_updates(
                    &committee,
                    self.chain_id,
                    CommunicateAction::SubmitBlockForConfirmation(proposal.clone()),
                )
                .await?
                .expect("a certificate")
            }
            ChainManagerInfo::None => unreachable!("chain is active"),
        };
        // By now the block should be final.
        ensure!(
            matches!(
                final_certificate.value(), CertificateValue::ConfirmedBlock { executed_block, .. }
                    if executed_block.block == proposal.content.block
            ),
            ChainClientError::BlockProposalError(
                "A different operation was executed in parallel (consider retrying the operation)"
            )
        );
        // Since `handle_block_proposal` succeeded, we have the needed bytecode.
        // Leaving blobs empty.
        self.process_certificate(final_certificate.clone(), vec![])
            .await?;
        self.pending_block = None;
        // Communicate the new certificate now.
        self.communicate_chain_updates(
            &committee,
            self.chain_id,
            CommunicateAction::AdvanceToNextBlockHeight(self.next_block_height),
        )
        .await?;
        if let Ok(new_committee) = self.local_committee().await {
            if new_committee != committee {
                // If the configuration just changed, communicate to the new committee as well.
                // (This is actually more important that updating the previous committee.)
                self.communicate_chain_updates(
                    &new_committee,
                    self.chain_id,
                    CommunicateAction::AdvanceToNextBlockHeight(self.next_block_height),
                )
                .await?;
            }
        }
        Ok(final_certificate)
    }

    /// Executes a list of operations.
    pub async fn execute_operations(
        &mut self,
        operations: Vec<Operation>,
    ) -> Result<Certificate, ChainClientError> {
        self.prepare_chain().await?;
        let messages = self.pending_messages().await?;
        self.execute_block(messages, operations).await
    }

    /// Executes an operation.
    pub async fn execute_operation(
        &mut self,
        operation: Operation,
    ) -> Result<Certificate, ChainClientError> {
        self.execute_operations(vec![operation]).await
    }

    /// Executes a new block.
    ///
    /// This must be preceded by a call to `prepare_chain()`.
    async fn execute_block(
        &mut self,
        incoming_messages: Vec<IncomingMessage>,
        operations: Vec<Operation>,
    ) -> Result<Certificate, ChainClientError> {
        let timestamp = self.next_timestamp(&incoming_messages);
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
        let certificate = self.propose_block(block).await?;
        Ok(certificate)
    }

    /// Returns a suitable timestamp for the next block.
    ///
    /// This will usually be the current time according to the local clock, but may be slightly
    /// ahead to make sure it's not earlier than the incoming messages or the previous block.
    fn next_timestamp(&self, incoming_messages: &[IncomingMessage]) -> Timestamp {
        incoming_messages
            .iter()
            .map(|msg| msg.event.timestamp)
            .max()
            .map_or_else(Timestamp::now, |timestamp| timestamp.max(Timestamp::now()))
            .max(self.timestamp)
    }

    /// Queries an application.
    pub async fn query_application(&self, query: &Query) -> Result<Response, ChainClientError> {
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
            .query_application(self.chain_id, &Query::System(query))
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
            .query_application(self.chain_id, &query)
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
        let incoming_messages = self.pending_messages().await?;
        let timestamp = self.next_timestamp(&incoming_messages);
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
            CommunicateAction::AdvanceToNextBlockHeight(self.next_block_height),
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
    ) -> Result<Certificate, ChainClientError> {
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
    ) -> Result<Certificate, ChainClientError> {
        self.transfer(owner, amount, Recipient::Account(account), user_data)
            .await
    }

    /// Burns tokens.
    pub async fn burn(
        &mut self,
        owner: Option<Owner>,
        amount: Amount,
        user_data: UserData,
    ) -> Result<Certificate, ChainClientError> {
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
    ) -> Result<Certificate, ChainClientError> {
        let new_public_key = key_pair.public();
        self.known_key_pairs.insert(new_public_key.into(), key_pair);
        self.transfer_ownership(new_public_key).await
    }

    /// Transfers ownership of the chain.
    pub async fn transfer_ownership(
        &mut self,
        new_public_key: PublicKey,
    ) -> Result<Certificate, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::ChangeOwner {
            new_public_key,
        }))
        .await
    }

    /// Adds another owner to the chain.
    pub async fn share_ownership(
        &mut self,
        new_public_key: PublicKey,
    ) -> Result<Certificate, ChainClientError> {
        let info = self.prepare_chain().await?;
        let messages = self.pending_messages().await?;
        let new_public_keys = match info.manager {
            ChainManagerInfo::None => {
                return Err(ChainError::InactiveChain(self.chain_id).into());
            }
            ChainManagerInfo::Single(manager) => {
                vec![manager.public_key, new_public_key]
            }
            ChainManagerInfo::Multi(manager) => manager
                .public_keys
                .values()
                .cloned()
                .chain(iter::once(new_public_key))
                .collect(),
        };
        self.execute_block(
            messages,
            vec![Operation::System(SystemOperation::ChangeMultipleOwners {
                new_public_keys,
            })],
        )
        .await
    }

    /// Opens a new chain with a derived UID.
    pub async fn open_chain(
        &mut self,
        ownership: ChainOwnership,
    ) -> Result<(MessageId, Certificate), ChainClientError> {
        self.prepare_chain().await?;
        let (epoch, committees) = self.epoch_and_committees(self.chain_id).await?;
        let epoch = epoch.ok_or(LocalNodeError::InactiveChain(self.chain_id))?;
        let messages = self.pending_messages().await?;
        let certificate = self
            .execute_block(
                messages,
                vec![Operation::System(SystemOperation::OpenChain {
                    ownership,
                    committees,
                    admin_id: self.admin_id,
                    epoch,
                })],
            )
            .await?;
        // The second last message created the new chain.
        let message_id = certificate
            .value
            .nth_last_message_id(2)
            .ok_or_else(|| ChainClientError::InternalError("Failed to open a new chain"))?;
        Ok((message_id, certificate))
    }

    /// Closes the chain (and loses everything in it!!).
    pub async fn close_chain(&mut self) -> Result<Certificate, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::CloseChain))
            .await
    }

    /// Publishes some bytecode.
    pub async fn publish_bytecode(
        &mut self,
        contract: Bytecode,
        service: Bytecode,
    ) -> Result<(BytecodeId, Certificate), ChainClientError> {
        let certificate = self
            .execute_operation(Operation::System(SystemOperation::PublishBytecode {
                contract,
                service,
            }))
            .await?;
        // The last message published the bytecode.
        let message_id = certificate
            .value
            .nth_last_message_id(1)
            .ok_or_else(|| ChainClientError::InternalError("Failed to publish bytecode"))?;
        let id = BytecodeId::new(message_id);
        Ok((id, certificate))
    }

    /// Creates an application by instantiating some bytecode.
    pub async fn create_application<A: Abi>(
        &mut self,
        bytecode_id: BytecodeId<A>,
        parameters: &<A as ContractAbi>::Parameters,
        initialization_argument: &A::InitializationArgument,
        required_application_ids: Vec<UserApplicationId>,
    ) -> Result<(UserApplicationId<A>, Certificate), ChainClientError> {
        let initialization_argument = serde_json::to_vec(initialization_argument)?;
        let parameters = serde_json::to_vec(parameters)?;
        let (app_id, cert) = self
            .create_application_untyped(
                bytecode_id.forget_abi(),
                parameters,
                initialization_argument,
                required_application_ids,
            )
            .await?;
        Ok((app_id.with_abi(), cert))
    }

    /// Creates an application by instantiating some bytecode.
    pub async fn create_application_untyped(
        &mut self,
        bytecode_id: BytecodeId,
        parameters: Vec<u8>,
        initialization_argument: Vec<u8>,
        required_application_ids: Vec<UserApplicationId>,
    ) -> Result<(UserApplicationId, Certificate), ChainClientError> {
        let certificate = self
            .execute_operation(Operation::System(SystemOperation::CreateApplication {
                bytecode_id,
                parameters,
                initialization_argument,
                required_application_ids,
            }))
            .await?;
        // The last message created the application.
        let creation = certificate
            .value
            .nth_last_message_id(1)
            .ok_or_else(|| ChainClientError::InternalError("Failed to create application"))?;
        let id = UserApplicationId {
            bytecode_id,
            creation,
        };
        Ok((id, certificate))
    }

    /// Creates a new committee and starts using it (admin chains only).
    pub async fn stage_new_committee(
        &mut self,
        committee: Committee,
    ) -> Result<Certificate, ChainClientError> {
        self.prepare_chain().await?;
        let epoch = self.epoch().await?;
        let messages = self.pending_messages().await?;
        self.execute_block(
            messages,
            vec![Operation::System(SystemOperation::Admin(
                AdminOperation::CreateCommittee {
                    epoch: epoch.try_add_one()?,
                    committee,
                },
            ))],
        )
        .await
    }

    /// Creates an empty block to process all incoming messages. This may require several blocks.
    pub async fn process_inbox(&mut self) -> Result<Vec<Certificate>, ChainClientError> {
        self.prepare_chain().await?;
        let mut certificates = Vec::new();
        loop {
            let incoming_messages = self.pending_messages().await?;
            if incoming_messages.is_empty() {
                break;
            }
            let certificate = self.execute_block(incoming_messages, vec![]).await?;
            certificates.push(certificate);
        }
        Ok(certificates)
    }

    /// Starts listening to the admin chain for new committees. (This is only useful for
    /// other genesis chains or for testing.)
    pub async fn subscribe_to_new_committees(&mut self) -> Result<Certificate, ChainClientError> {
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
    ) -> Result<Certificate, ChainClientError> {
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
    ) -> Result<Certificate, ChainClientError> {
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
    ) -> Result<Certificate, ChainClientError> {
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
    pub async fn finalize_committee(&mut self) -> Result<Certificate, ChainClientError> {
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
        let messages = self.pending_messages().await?;
        self.execute_block(messages, operations).await
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
    ) -> Result<Certificate, ChainClientError> {
        self.execute_operation(Operation::System(SystemOperation::Transfer {
            owner,
            recipient: Recipient::Account(account),
            amount,
            user_data,
        }))
        .await
    }

    pub async fn read_value(&self, hash: CryptoHash) -> Result<HashedValue, ViewError> {
        self.node_client
            .storage_client()
            .await
            .read_value(hash)
            .await
    }

    pub async fn read_values_downward(
        &self,
        from: CryptoHash,
        limit: u32,
    ) -> Result<Vec<HashedValue>, ViewError> {
        self.node_client
            .storage_client()
            .await
            .read_values_downward(from, limit)
            .await
    }
}

// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{hash_map, BTreeMap, HashMap},
    convert::Infallible,
    iter,
    ops::Deref,
    sync::Arc,
};

use futures::{
    future::{self, FusedFuture, Future},
    lock::Mutex,
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
    ChainError, ChainExecutionContext, ChainStateView,
};
use linera_execution::{
    committee::{Committee, Epoch, ValidatorName},
    system::{
        AdminOperation, OpenChainConfig, Recipient, SystemChannel, SystemOperation, UserData,
        CREATE_APPLICATION_MESSAGE_INDEX, OPEN_CHAIN_MESSAGE_INDEX, PUBLISH_BYTECODE_MESSAGE_INDEX,
    },
    Bytecode, BytecodeLocation, ExecutionError, Message, Operation, Query, Response,
    SystemExecutionError, SystemMessage, SystemQuery, SystemResponse, UserApplicationId,
};
use linera_storage::Storage;
use linera_views::views::ViewError;
use serde::Serialize;
use thiserror::Error;
use tokio::sync::OwnedRwLockReadGuard;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, error, info};

use crate::{
    data_types::{
        BlockHeightRange, ChainInfo, ChainInfoQuery, ChainInfoResponse, ClientOutcome, RoundTimeout,
    },
    local_node::{LocalNodeClient, LocalNodeError},
    node::{
        CrossChainMessageDelivery, LocalValidatorNode, LocalValidatorNodeProvider, NodeError,
        NotificationStream, ValidatorNodeProvider,
    },
    notifier::Notifier,
    updater::{communicate_with_quorum, CommunicateAction, CommunicationError, ValidatorUpdater},
    value_cache::ValueCache,
    worker::{DeliveryNotifiers, Notification, Reason, WorkerError, WorkerState},
};

#[cfg(test)]
#[path = "unit_tests/client_tests.rs"]
mod client_tests;

/// A builder that creates `ChainClients` which share the cache and notifiers.
pub struct Client<ValidatorNodeProvider, Storage> {
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
}

impl<P, S: Storage + Clone> Client<P, S> {
    /// Creates a new `Client` with a new cache and notifiers.
    pub fn new(
        validator_node_provider: P,
        storage: S,
        max_pending_messages: usize,
        cross_chain_message_delivery: CrossChainMessageDelivery,
    ) -> Self {
        let state = WorkerState::new_for_client(
            "Client node".to_string(),
            storage.clone(),
            Arc::new(ValueCache::default()),
            Arc::new(ValueCache::default()),
            Arc::new(tokio::sync::Mutex::new(DeliveryNotifiers::default())),
        )
        .with_allow_inactive_chains(true)
        .with_allow_messages_from_deprecated_epochs(true);
        let local_node = LocalNodeClient::new(state);

        Self {
            validator_node_provider,
            local_node,
            max_pending_messages,
            message_policy: MessagePolicy::Accept,
            cross_chain_message_delivery,
            notifier: Arc::new(Notifier::default()),
            storage,
        }
    }

    /// Returns this builder with the given message policy.
    pub fn with_message_policy(mut self, message_policy: MessagePolicy) -> Self {
        self.message_policy = message_policy;
        self
    }

    /// Returns the storage client used by this client's local node.
    pub fn storage_client(&self) -> &S {
        &self.storage
    }

    /// Creates a new `ChainClient`.
    #[allow(clippy::too_many_arguments)]
    pub fn build(
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
        P: Clone,
    {
        let known_key_pairs = known_key_pairs
            .into_iter()
            .map(|kp| (Owner::from(kp.public()), kp))
            .collect();
        ChainClient {
            client: self.clone(),
            chain_id,
            known_key_pairs,
            validator_node_provider: self.validator_node_provider.clone(),
            admin_id,
            max_pending_messages: self.max_pending_messages,
            message_policy: self.message_policy,
            cross_chain_message_delivery: self.cross_chain_message_delivery,
            received_certificate_trackers: HashMap::new(),
            block_hash,
            timestamp,
            next_block_height,
            pending_block,
            pending_blobs,
            notifier: self.notifier.clone(),
        }
    }
}

/// Policies for automatically handling incoming messages.
///
/// These apply to all messages except for the initial `OpenChain`, which is always accepted.
#[derive(Copy, Clone, clap::ValueEnum)]
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
    fn is_ignore(&self) -> bool {
        matches!(self, Self::Ignore)
    }

    fn is_reject(&self) -> bool {
        matches!(self, Self::Reject)
    }
}

/// Client to operate a chain by interacting with validators and the given local storage
/// implementation.
/// * The chain being operated is called the "local chain" or just the "chain".
/// * As a rule, operations are considered successful (and communication may stop) when
/// they succeeded in gathering a quorum of responses.
pub struct ChainClient<ValidatorNodeProvider, Storage> {
    /// The Linera [`Client`] that manages operations on this chain.
    client: Arc<Client<ValidatorNodeProvider, Storage>>,
    /// The off-chain chain ID.
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
    /// The ID of the admin chain.
    admin_id: ChainId,

    /// Maximum number of pending messages processed at a time in a block.
    max_pending_messages: usize,
    /// The policy for automatically handling incoming messages.
    message_policy: MessagePolicy,
    /// Whether to block on cross-chain message delivery.
    cross_chain_message_delivery: CrossChainMessageDelivery,
    /// Support synchronization of received certificates.
    received_certificate_trackers: HashMap<ValidatorName, u64>,
    /// This contains blobs belonging to our `pending_block` that may not even have
    /// been processed by (i.e. been proposed to) our own local chain manager yet.
    pending_blobs: BTreeMap<BlobId, HashedBlob>,

    /// A notifier to receive notifications for this chain.
    notifier: Arc<Notifier<Notification>>,
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
}

impl From<Infallible> for ChainClientError {
    fn from(infallible: Infallible) -> Self {
        match infallible {}
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

    pub fn pending_blobs(&self) -> &BTreeMap<BlobId, HashedBlob> {
        &self.pending_blobs
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
    ViewError: From<S::ContextError>,
{
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

    /// Subscribes to notifications from this client's chain.
    pub async fn subscribe(&mut self) -> Result<NotificationStream, LocalNodeError> {
        Ok(Box::pin(UnboundedReceiverStream::new(
            self.notifier.subscribe(vec![self.chain_id]),
        )))
    }

    /// Returns the storage client used by this client's local node.
    pub fn storage_client(&self) -> S {
        self.client.storage_client().clone()
    }

    /// Obtains the basic `ChainInfo` data for the local chain.
    pub async fn chain_info(&mut self) -> Result<Box<ChainInfo>, LocalNodeError> {
        let query = ChainInfoQuery::new(self.chain_id);
        let response = self
            .client
            .local_node
            .handle_chain_info_query(query)
            .await?;
        Ok(response.info)
    }

    /// Obtains the basic `ChainInfo` data for the local chain, with chain manager values.
    pub async fn chain_info_with_manager_values(
        &mut self,
    ) -> Result<Box<ChainInfo>, LocalNodeError> {
        let query = ChainInfoQuery::new(self.chain_id).with_manager_values();
        let response = self
            .client
            .local_node
            .handle_chain_info_query(query)
            .await?;
        Ok(response.info)
    }

    /// Obtains up to `self.max_pending_messages` pending messages for the local chain.
    ///
    /// Messages known to be redundant are filtered out: A `RegisterApplications` message whose
    /// entries are already known never needs to be included in a block.
    async fn pending_messages(&mut self) -> Result<Vec<IncomingMessage>, ChainClientError> {
        if self.next_block_height != BlockHeight::ZERO && self.message_policy.is_ignore() {
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
            info.next_block_height == self.next_block_height,
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
        if self.message_policy.is_ignore() {
            return Ok(pending_messages); // Ignore messages other than OpenChain.
        }
        for mut message in requested_pending_messages {
            if pending_messages.len() >= self.max_pending_messages {
                tracing::warn!(
                    "Limiting block to {} incoming messages",
                    self.max_pending_messages
                );
                break;
            }
            if self.message_policy.is_reject() {
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

    /// Obtains the set of committees trusted by the local chain.
    async fn committees(&mut self) -> Result<BTreeMap<Epoch, Committee>, LocalNodeError> {
        let (_epoch, committees) = self.epoch_and_committees(self.chain_id).await?;
        Ok(committees)
    }

    /// Obtains the current epoch of the given chain as well as its set of trusted committees.
    pub async fn epoch_and_committees(
        &mut self,
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

    /// Obtains the identity of the current owner of the chain. Returns an error if we have the
    /// private key for more than one identity.
    pub async fn identity(&mut self) -> Result<Owner, ChainClientError> {
        let manager = self.chain_info().await?.manager;
        ensure!(
            manager.ownership.is_active(),
            LocalNodeError::InactiveChain(self.chain_id)
        );
        let mut identities = manager
            .ownership
            .all_owners()
            .chain(&manager.leader)
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

    /// Prepares the chain for the next operation.
    async fn prepare_chain(&mut self) -> Result<Box<ChainInfo>, ChainClientError> {
        // Verify that our local storage contains enough history compared to the
        // expected block height. Otherwise, download the missing history from the
        // network.
        let nodes = self.validator_nodes().await?;
        let mut notifications = vec![];
        let mut info = self
            .client
            .local_node
            .download_certificates(
                nodes,
                self.chain_id,
                self.next_block_height,
                &mut notifications,
            )
            .await?;
        self.notifier.handle_notifications(&notifications);
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
                .client
                .local_node
                .synchronize_chain_state(nodes, self.chain_id, &mut notifications)
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
        let value = certificate.value.validated_to_confirmed().ok_or_else(|| {
            ChainClientError::InternalError(
                "Certificate for finalization must be a validated block",
            )
        })?;
        let finalize_action = CommunicateAction::FinalizeBlock {
            certificate,
            delivery: self.cross_chain_message_delivery,
        };
        let certificate = self
            .communicate_chain_action(committee, finalize_action, value)
            .await?;
        self.receive_certificate_internal(
            certificate.clone(),
            ReceiveCertificateMode::AlreadyChecked,
        )
        .await?;
        Ok(certificate)
    }

    /// Submits a block proposal to the validators. If it is a slow round, also submits the
    /// validated block for finalization. Returns the confirmed block certificate.
    async fn submit_block_proposal(
        &mut self,
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

    async fn chain_managers_pending_blobs(
        &self,
    ) -> Result<BTreeMap<BlobId, HashedBlob>, LocalNodeError> {
        let chain = self.chain_state_view().await?;
        Ok(chain.manager.get().pending_blobs.clone())
    }

    /// Broadcasts certified blocks to validators.
    async fn communicate_chain_updates(
        &mut self,
        committee: &Committee,
        chain_id: ChainId,
        height: BlockHeight,
        delivery: CrossChainMessageDelivery,
    ) -> Result<(), ChainClientError> {
        let local_node = self.client.local_node.clone();
        let chain_manager_pending_blobs = self.chain_managers_pending_blobs().await?;
        let nodes: Vec<_> = self.validator_node_provider.make_nodes(committee)?;
        communicate_with_quorum(
            &nodes,
            committee,
            |_: &()| (),
            |name, node| {
                let mut updater = ValidatorUpdater {
                    name,
                    node,
                    local_node: local_node.clone(),
                    local_node_chain_managers_pending_blobs: chain_manager_pending_blobs.clone(),
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
    async fn communicate_chain_action(
        &mut self,
        committee: &Committee,
        action: CommunicateAction,
        value: HashedCertificateValue,
    ) -> Result<Certificate, ChainClientError> {
        let local_node = self.client.local_node.clone();
        let chain_manager_pending_blobs = self.chain_managers_pending_blobs().await?;
        let nodes: Vec<_> = self.validator_node_provider.make_nodes(committee)?;
        let ((votes_hash, votes_round), votes) = communicate_with_quorum(
            &nodes,
            committee,
            |vote: &LiteVote| (vote.value.value_hash, vote.round),
            |name, node| {
                let mut updater = ValidatorUpdater {
                    name,
                    node,
                    local_node: local_node.clone(),
                    local_node_chain_managers_pending_blobs: chain_manager_pending_blobs.clone(),
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

    async fn find_missing_application_bytecodes(
        &self,
        locations: &[BytecodeLocation],
        nodes: &[(ValidatorName, <P as LocalValidatorNodeProvider>::Node)],
    ) -> Vec<HashedCertificateValue> {
        future::join_all(locations.iter().map(|location| {
            LocalNodeClient::<S>::download_hashed_certificate_value(nodes.to_owned(), *location)
        }))
        .await
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
    }

    async fn find_missing_blobs(
        &self,
        blob_ids: &[BlobId],
        nodes: &[(ValidatorName, <P as LocalValidatorNodeProvider>::Node)],
    ) -> Vec<HashedBlob> {
        future::join_all(
            blob_ids
                .iter()
                .map(|blob_id| LocalNodeClient::<S>::download_blob(nodes.to_owned(), *blob_id)),
        )
        .await
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
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
        let mut notifications = vec![];
        self.client
            .local_node
            .download_certificates(
                nodes.clone(),
                block.chain_id,
                block.height,
                &mut notifications,
            )
            .await?;
        self.notifier.handle_notifications(&notifications);
        // Process the received operations. Download required hashed certificate values if necessary.
        if let Err(err) = self
            .process_certificate(certificate.clone(), vec![], vec![])
            .await
        {
            match &err {
                LocalNodeError::WorkerError(WorkerError::ApplicationBytecodesNotFound(
                    locations,
                )) => {
                    let values = self
                        .find_missing_application_bytecodes(locations, &nodes)
                        .await;

                    ensure!(values.len() == locations.len(), err);
                    self.process_certificate(certificate.clone(), values.clone(), vec![])
                        .await?;
                }
                LocalNodeError::WorkerError(WorkerError::BlobsNotFound(blob_ids)) => {
                    let blobs = self.find_missing_blobs(blob_ids, &nodes).await;

                    ensure!(blobs.len() == blob_ids.len(), err);
                    self.process_certificate(certificate.clone(), vec![], blobs)
                        .await?;
                }
                LocalNodeError::WorkerError(WorkerError::ApplicationBytecodesAndBlobsNotFound(
                    locations,
                    blob_ids,
                )) => {
                    let values = self
                        .find_missing_application_bytecodes(locations, &nodes)
                        .await;
                    let blobs = self.find_missing_blobs(blob_ids, &nodes).await;

                    ensure!(
                        blobs.len() == blob_ids.len() && values.len() == locations.len(),
                        err
                    );
                    self.process_certificate(certificate.clone(), values, blobs)
                        .await?;
                }
                _ => {
                    // The certificate is not as expected. Give up.
                    tracing::warn!("Failed to process network hashed certificate value");
                    return Err(err.into());
                }
            }
        }
        // Make sure a quorum of validators (according to our new local committee) are up-to-date
        // for data availability.
        let local_committee = self.local_committee().await?;
        self.communicate_chain_updates(
            &local_committee,
            block.chain_id,
            block.height.try_add_one()?,
            CrossChainMessageDelivery::Blocking,
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
        node_client: LocalNodeClient<S>,
    ) -> Result<(ValidatorName, u64, Vec<Certificate>), NodeError>
    where
        A: LocalValidatorNode + Clone + 'static,
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
        let mut notifications = vec![];
        // Synchronize the state of the admin chain from the network.
        self.client
            .local_node
            .synchronize_chain_state(nodes.clone(), self.admin_id, &mut notifications)
            .await?;
        self.notifier.handle_notifications(&notifications);
        let node_client = self.client.local_node.clone();
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
        self.notifier.handle_notifications(&notifications);
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

    async fn stage_block_execution_and_discard_failing_messages(
        &mut self,
        mut block: Block,
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
            return Ok(result?);
        }
    }

    /// Tries to read blobs from either the pending blobs or the local node's cache, or
    /// the chain manager's pending blobs
    async fn read_local_blobs(
        &mut self,
        blob_ids: impl IntoIterator<Item = BlobId>,
    ) -> Result<Vec<HashedBlob>, LocalNodeError> {
        let mut blobs = Vec::new();
        for blob_id in blob_ids {
            if let Some(blob) = self.client.local_node.recent_blob(&blob_id).await {
                blobs.push(blob);
            } else if let Some(blob) = self.pending_blobs.get(&blob_id) {
                self.client.local_node.cache_recent_blob(blob).await;
                blobs.push(blob.to_owned());
            } else {
                return Err(LocalNodeError::CannotReadLocalBlob {
                    chain_id: self.chain_id,
                    blob_id,
                });
            }
        }

        Ok(blobs)
    }

    /// Executes (or retries) a regular block proposal. Updates local balance.
    async fn propose_block(
        &mut self,
        block: Block,
        round: Round,
    ) -> Result<Certificate, ChainClientError> {
        ensure!(
            block.height == self.next_block_height,
            ChainClientError::BlockProposalError("Unexpected block height")
        );
        ensure!(
            block.previous_block_hash == self.block_hash,
            ChainClientError::BlockProposalError("Unexpected previous block hash")
        );
        // Gather information on the current local state.
        let manager = *self.chain_info_with_manager_values().await?.manager;
        // In the fast round, we must never make any conflicting proposals.
        if round.is_fast() {
            if let Some(pending) = &self.pending_block {
                ensure!(
                    pending == &block,
                    ChainClientError::BlockProposalError(
                        "Client state has a different pending block; \
                         use the `linera retry-pending-block` command to commit that first"
                    )
                );
            }
        }
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
        let nodes = self
            .validator_node_provider
            .make_nodes::<Vec<_>>(&committee)?;
        let values = self
            .client
            .local_node
            .read_or_download_hashed_certificate_values(nodes.clone(), block.bytecode_locations())
            .await?;
        let hashed_blobs = self.read_local_blobs(block.blob_ids()).await?;
        // Create the final block proposal.
        let key_pair = self.key_pair().await?;
        let proposal = if let Some(cert) = manager.requested_locked {
            BlockProposal::new_retry(round, *cert, key_pair, values, hashed_blobs)
        } else {
            BlockProposal::new_initial(round, block.clone(), key_pair, values, hashed_blobs)
        };
        // Check the final block proposal. This will be cheaper after #1401.
        self.client
            .local_node
            .handle_block_proposal(proposal.clone())
            .await?;
        // Remember what we are trying to do before sending the proposal to the validators.
        self.pending_block = Some(block);
        // Send the query to validators.
        let certificate = self
            .submit_block_proposal(&committee, proposal, hashed_value)
            .await?;
        self.pending_block = None;
        self.pending_blobs.clear();
        // Communicate the new certificate now.
        self.communicate_chain_updates(
            &committee,
            self.chain_id,
            self.next_block_height,
            self.cross_chain_message_delivery,
        )
        .await?;
        if let Ok(new_committee) = self.local_committee().await {
            if new_committee != committee {
                // If the configuration just changed, communicate to the new committee as well.
                // (This is actually more important that updating the previous committee.)
                self.communicate_chain_updates(
                    &new_committee,
                    self.chain_id,
                    self.next_block_height,
                    self.cross_chain_message_delivery,
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
            // TODO(#2066): Remove boxing once the call-stack is shallower
            match Box::pin(self.execute_block(messages, operations.clone())).await? {
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
        match self.process_pending_block_without_prepare().await? {
            ClientOutcome::Committed(Some(certificate)) => {
                return Ok(ExecuteBlockOutcome::Conflict(certificate))
            }
            ClientOutcome::Committed(None) => {}
            ClientOutcome::WaitForTimeout(timeout) => {
                return Ok(ExecuteBlockOutcome::WaitForTimeout(timeout))
            }
        }
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

    async fn set_pending_block(
        &mut self,
        incoming_messages: Vec<IncomingMessage>,
        operations: Vec<Operation>,
    ) -> Result<HashedCertificateValue, ChainClientError> {
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
        // Make sure every incoming message succeeds and otherwise remove them.
        // Also, compute the final certified hash while we're at it.
        let (executed_block, _) = self
            .stage_block_execution_and_discard_failing_messages(block)
            .await?;
        self.pending_block = Some(executed_block.block.clone());
        Ok(HashedCertificateValue::new_confirmed(executed_block))
    }

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
            .max(self.timestamp)
    }

    /// Queries an application.
    pub async fn query_application(&self, query: Query) -> Result<Response, ChainClientError> {
        let response = self
            .client
            .local_node
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
    pub async fn query_user_application<A: Abi>(
        &mut self,
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
    /// `max_pending_messages` incoming messages and the execution fees for a single
    /// block.
    pub async fn query_balance(&mut self) -> Result<Amount, ChainClientError> {
        let (balance, _) = self.query_balances_with_owner(None).await?;
        Ok(balance)
    }

    /// Obtains the local balance of a user account after staging the execution of
    /// incoming messages in a new block.
    ///
    /// Does not attempt to synchronize with validators. The result will reflect up to
    /// `max_pending_messages` incoming messages and the execution fees for a single
    /// block.
    pub async fn query_owner_balance(&mut self, owner: Owner) -> Result<Amount, ChainClientError> {
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
    /// `max_pending_messages` incoming messages and the execution fees for a single
    /// block.
    async fn query_balances_with_owner(
        &mut self,
        owner: Option<Owner>,
    ) -> Result<(Amount, Option<Amount>), ChainClientError> {
        let incoming_messages = self.pending_messages().await?;
        let timestamp = self.next_timestamp(&incoming_messages).await;
        let block = Block {
            epoch: self.epoch().await?,
            chain_id: self.chain_id,
            incoming_messages,
            operations: Vec::new(),
            previous_block_hash: self.block_hash,
            height: self.next_block_height,
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
    pub async fn local_balance(&mut self) -> Result<Amount, ChainClientError> {
        let (balance, _) = self.local_balances_with_owner(None).await?;
        Ok(balance)
    }

    /// Reads the local balance of a user account.
    ///
    /// Does not process the inbox or attempt to synchronize with validators.
    pub async fn local_owner_balance(&mut self, owner: Owner) -> Result<Amount, ChainClientError> {
        Ok(self
            .local_balances_with_owner(Some(owner))
            .await?
            .1
            .unwrap_or(Amount::ZERO))
    }

    /// Reads the local balance of the chain account and optionally another user.
    ///
    /// Does not process the inbox or attempt to synchronize with validators.
    async fn local_balances_with_owner(
        &mut self,
        owner: Option<Owner>,
    ) -> Result<(Amount, Option<Amount>), ChainClientError> {
        ensure!(
            self.chain_info().await?.next_block_height == self.next_block_height,
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
    pub async fn update_validators(&mut self) -> Result<(), ChainClientError> {
        let committee = self.local_committee().await?;
        self.communicate_chain_updates(
            &committee,
            self.chain_id,
            self.next_block_height,
            CrossChainMessageDelivery::NonBlocking,
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

    /// Attempts to synchronize chains that have sent us messages and populate our local
    /// inbox.
    ///
    /// To create a block that actually executes the messages in the inbox,
    /// `process_inbox` must be called separately.
    pub async fn synchronize_from_validators(
        &mut self,
    ) -> Result<Box<ChainInfo>, ChainClientError> {
        self.find_received_certificates().await?;
        self.prepare_chain().await
    }

    /// Processes the last pending block
    pub async fn process_pending_block(
        &mut self,
    ) -> Result<ClientOutcome<Option<Certificate>>, ChainClientError> {
        self.find_received_certificates().await?;
        self.prepare_chain().await?;
        self.process_pending_block_without_prepare().await
    }

    /// Processes the last pending block. Assumes that the local chain is up to date.
    async fn process_pending_block_without_prepare(
        &mut self,
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
        if let Some(block) = &self.pending_block {
            if block.height != info.next_block_height {
                self.pending_block = None;
                self.pending_blobs.clear();
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
        let maybe_block = manager
            .requested_locked
            .as_ref()
            .and_then(|certificate| certificate.value().block())
            .or(manager
                .requested_proposed
                .as_ref()
                .filter(|proposal| proposal.content.round.is_fast())
                .map(|proposal| &proposal.content.block))
            .or(self.pending_block.as_ref());
        let Some(block) = maybe_block else {
            return Ok(ClientOutcome::Committed(None)); // Nothing to propose.
        };
        // If there is a conflicting proposal in the current round, we can only propose if the
        // next round can be started without a timeout, i.e. if we are in a multi-leader round.
        let conflicting_proposal = manager
            .requested_proposed
            .as_ref()
            .map_or(false, |proposal| {
                proposal.content.round == manager.current_round && proposal.content.block != *block
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
            let certificate = self.propose_block(block.clone(), round).await?;
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
    pub fn clear_pending_block(&mut self) {
        self.pending_block = None;
        self.pending_blobs.clear();
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

    /// Transfers ownership of the chain to a single super owner.
    pub async fn transfer_ownership(
        &mut self,
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
                timeout_config: ownership.timeout_config,
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

    /// Changes the ownership of this chain. Fails if it would remove existing owners, unless
    /// `remove_owners` is `true`.
    pub async fn change_ownership(
        &mut self,
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
    pub async fn change_application_permissions(
        &mut self,
        application_permissions: ApplicationPermissions,
    ) -> Result<ClientOutcome<Certificate>, ChainClientError> {
        let operation = SystemOperation::ChangeApplicationPermissions(application_permissions);
        self.execute_operation(operation.into()).await
    }

    /// Opens a new chain with a derived UID.
    pub async fn open_chain(
        &mut self,
        ownership: ChainOwnership,
        application_permissions: ApplicationPermissions,
        balance: Amount,
    ) -> Result<ClientOutcome<(MessageId, Certificate)>, ChainClientError> {
        self.prepare_chain().await?;
        loop {
            let (epoch, committees) = self.epoch_and_committees(self.chain_id).await?;
            let epoch = epoch.ok_or(LocalNodeError::InactiveChain(self.chain_id))?;
            let messages = self.pending_messages().await?;
            let config = OpenChainConfig {
                ownership: ownership.clone(),
                committees,
                admin_id: self.admin_id,
                epoch,
                balance,
                application_permissions: application_permissions.clone(),
            };
            let operation = Operation::System(SystemOperation::OpenChain(config));
            let certificate = match self.execute_block(messages, vec![operation]).await? {
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

    /// Publishes some blob.
    pub async fn publish_blob(
        &mut self,
        hashed_blob: HashedBlob,
    ) -> Result<ClientOutcome<(BlobId, Certificate)>, ChainClientError> {
        self.client.local_node.cache_recent_blob(&hashed_blob).await;
        self.pending_blobs
            .insert(hashed_blob.id(), hashed_blob.clone());
        self.execute_operation(Operation::System(SystemOperation::PublishBlob {
            blob_id: hashed_blob.id(),
        }))
        .await?
        .try_map(|certificate| Ok((hashed_blob.id(), certificate)))
    }

    /// Creates an application by instantiating some bytecode.
    pub async fn create_application<
        A: Abi,
        Parameters: Serialize,
        InstantiationArgument: Serialize,
    >(
        &mut self,
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
    pub async fn create_application_untyped(
        &mut self,
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
                return Ok((certificates, None));
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

    pub async fn read_hashed_certificate_value(
        &self,
        hash: CryptoHash,
    ) -> Result<HashedCertificateValue, ViewError> {
        self.client
            .storage_client()
            .read_hashed_certificate_value(hash)
            .await
    }

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

    /// Wraps this chain client into an `Arc<Mutex<_>>`.
    pub fn into_arc(self) -> ArcChainClient<P, S> {
        ArcChainClient::new(self)
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

/// A chain client in an `Arc<Mutex<_>>`, so it can be used by different tasks and threads.
#[derive(Debug)]
pub struct ArcChainClient<P, S>(pub Arc<Mutex<ChainClient<P, S>>>);

impl<P, S> Deref for ArcChainClient<P, S> {
    type Target = Arc<Mutex<ChainClient<P, S>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<P, S> Clone for ArcChainClient<P, S> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<P, S> ArcChainClient<P, S> {
    pub fn new(client: ChainClient<P, S>) -> Self {
        Self(Arc::new(Mutex::new(client)))
    }
}

impl<P, S> ArcChainClient<P, S>
where
    P: ValidatorNodeProvider + Sync,
    <<P as ValidatorNodeProvider>::Node as crate::node::ValidatorNode>::NotificationStream: Send,
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    async fn local_chain_info(
        &self,
        chain_id: ChainId,
        local_node: &mut LocalNodeClient<S>,
    ) -> Option<Box<ChainInfo>> {
        let mut guard = self.lock().await;
        let Ok(info) = local_node.local_chain_info(chain_id).await else {
            error!("Fail to read local chain info for {chain_id}");
            return None;
        };
        // Useful in case `chain_id` is the same as the local chain.
        guard.update_from_info(&info);
        Some(info)
    }

    async fn local_next_block_height(
        &self,
        chain_id: ChainId,
        local_node: &mut LocalNodeClient<S>,
    ) -> Option<BlockHeight> {
        let info = self.local_chain_info(chain_id, local_node).await?;
        Some(info.next_block_height)
    }

    async fn process_notification(
        &self,
        name: ValidatorName,
        node: <P as ValidatorNodeProvider>::Node,
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
                }
                if let Err(e) = self
                    .find_received_certificates_from_validator(name, node, local_node.clone())
                    .await
                {
                    error!("Fail to process notification: {e}");
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
                }
                local_node
                    .try_synchronize_chain_state_from(name, node, chain_id, &mut notifications)
                    .await
                    .unwrap_or_else(|e| {
                        error!("Fail to process notification: {e}");
                    });
                self.0
                    .lock()
                    .await
                    .notifier
                    .handle_notifications(&notifications);
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
                    }
                }
                if let Err(error) = local_node
                    .try_synchronize_chain_state_from(name, node, chain_id, &mut notifications)
                    .await
                {
                    error!("Fail to process notification: {error}");
                }
                self.0
                    .lock()
                    .await
                    .notifier
                    .handle_notifications(&notifications);
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
        let mut guard = self.lock().await;
        let notifications = guard.subscribe().await?;
        let (abortable_notifications, abort) = stream::abortable(guard.subscribe().await?);
        if let Err(error) = guard.synchronize_from_validators().await {
            error!("Failed to synchronize from validators: {}", error);
        }
        drop(guard);

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
        };

        Ok((update_streams, AbortOnDrop(abort), notifications))
    }

    async fn update_streams(
        &self,
        senders: &mut HashMap<ValidatorName, AbortHandle>,
    ) -> Result<impl Future<Output = ()>, ChainClientError>
    where
        P: Send + 'static,
    {
        let (chain_id, nodes, local_node) = {
            let mut guard = self.lock().await;
            let committee = guard.local_committee().await?;
            let nodes: HashMap<_, _> = guard.validator_node_provider.make_nodes(&committee)?;
            (guard.chain_id, nodes, guard.client.local_node.clone())
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

    /// Attempts to download new received certificates from a particular validator.
    ///
    /// This is similar to `find_received_certificates` but for only one validator.
    /// We also don't try to synchronize the admin chain.
    pub async fn find_received_certificates_from_validator(
        &self,
        name: ValidatorName,
        node: <P as ValidatorNodeProvider>::Node,
        node_client: LocalNodeClient<S>,
    ) -> Result<(), ChainClientError> {
        let ((committees, max_epoch), chain_id, current_tracker) = {
            let mut guard = self.lock().await;
            (
                guard.known_committees().await?,
                guard.chain_id(),
                *guard.received_certificate_trackers.get(&name).unwrap_or(&0),
            )
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
        self.lock()
            .await
            .receive_certificates_from_validator(name, tracker, certificates)
            .await;
        Ok(())
    }
}

/// Wrapper for `AbortHandle` that aborts when its dropped.
#[must_use]
pub struct AbortOnDrop(AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

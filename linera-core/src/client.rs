// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    data_types::{BlockHeightRange, ChainInfo, ChainInfoQuery},
    node::{LocalNodeClient, NodeError, NotificationStream, ValidatorNode},
    updater::{communicate_with_quorum, CommunicateAction, CommunicationError, ValidatorUpdater},
    worker::{Notification, Reason, WorkerState},
};
use anyhow::{anyhow, bail, ensure, Result};
use futures::{
    future,
    lock::Mutex,
    stream::{self, FuturesUnordered, StreamExt},
};
use linera_base::{
    abi::{Abi, ContractAbi},
    crypto::{CryptoHash, KeyPair, PublicKey},
    data_types::{Amount, BlockHeight, RoundNumber, Timestamp},
    identifiers::{BytecodeId, ChainId, MessageId, Owner},
};
use linera_chain::{
    data_types::{
        Block, BlockAndRound, BlockProposal, Certificate, CertificateValue, HashedValue,
        IncomingMessage, LiteVote,
    },
    ChainManagerInfo, ChainStateView,
};
use linera_execution::{
    committee::{Committee, Epoch, ValidatorName},
    system::{Account, AdminOperation, Recipient, SystemChannel, SystemOperation, UserData},
    Bytecode, Message, Operation, Query, Response, SystemMessage, SystemQuery, SystemResponse,
    UserApplicationId,
};
use linera_storage::Store;
use linera_views::views::ViewError;
use std::{
    collections::{hash_map, BTreeMap, HashMap},
    pin::Pin,
    sync::Arc,
    time::Duration,
};
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

/// Turn an address into a validator node (local node or client to a remote node).
#[allow(clippy::result_large_err)]
pub trait ValidatorNodeProvider {
    type Node: ValidatorNode + Clone + Send + Sync + 'static;

    fn make_node(&self, address: &str) -> Result<Self::Node, NodeError>;

    fn make_nodes<I>(&self, committee: &Committee) -> Result<I, NodeError>
    where
        I: FromIterator<(ValidatorName, Self::Node)>,
    {
        committee
            .validators()
            .iter()
            .map(|(name, validator)| {
                let node = self.make_node(&validator.network_address)?;
                Ok((*name, node))
            })
            .collect()
    }
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
    ) -> Result<Arc<ChainStateView<S::Context>>, NodeError> {
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
    async fn chain_info(&mut self) -> Result<ChainInfo, NodeError> {
        let query = ChainInfoQuery::new(self.chain_id);
        let response = self.node_client.handle_chain_info_query(query).await?;
        Ok(response.info)
    }

    /// Obtains up to `self.max_pending_messages` pending messages for the local chain.
    ///
    /// Messages known to be redundant are filtered out: A `RegisterApplications` message whose
    /// entries are already known never needs to be included in a block.
    async fn pending_messages(&mut self) -> Result<Vec<IncomingMessage>, NodeError> {
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
    async fn committees(&mut self) -> Result<BTreeMap<Epoch, Committee>, NodeError> {
        let (_epoch, committees) = self.epoch_and_committees(self.chain_id).await?;
        Ok(committees)
    }

    /// Obtains the current epoch of the given chain as well as its set of trusted committees.
    async fn epoch_and_committees(
        &mut self,
        chain_id: ChainId,
    ) -> Result<(Option<Epoch>, BTreeMap<Epoch, Committee>), NodeError> {
        let query = ChainInfoQuery::new(chain_id).with_committees();
        let info = self.node_client.handle_chain_info_query(query).await?.info;
        let epoch = info.epoch;
        let committees = info
            .requested_committees
            .ok_or(NodeError::InvalidChainInfoResponse)?;
        Ok((epoch, committees))
    }

    /// Obtains the epochs of the committees trusted by the local chain.
    pub async fn epochs(&mut self) -> Result<Vec<Epoch>, NodeError> {
        let committees = self.committees().await?;
        Ok(committees.into_keys().collect())
    }

    /// Obtains the committee for the current epoch of the local chain.
    pub async fn local_committee(&mut self) -> Result<Committee, NodeError> {
        let (epoch, mut committees) = self.epoch_and_committees(self.chain_id).await?;
        committees
            .remove(
                epoch
                    .as_ref()
                    .ok_or(NodeError::InactiveLocalChain(self.chain_id))?,
            )
            .ok_or(NodeError::InactiveLocalChain(self.chain_id))
    }

    /// Obtains all the committees trusted by either the local chain or its admin chain. Also
    /// return the latest trusted epoch.
    async fn known_committees(&mut self) -> Result<(BTreeMap<Epoch, Committee>, Epoch), NodeError> {
        let (epoch, mut committees) = self.epoch_and_committees(self.chain_id).await?;
        let (admin_epoch, admin_committees) = self.epoch_and_committees(self.admin_id).await?;
        committees.extend(admin_committees);
        let epoch = std::cmp::max(epoch.unwrap_or_default(), admin_epoch.unwrap_or_default());
        Ok((committees, epoch))
    }

    /// Obtains the validators trusted by the local chain.
    async fn validator_nodes(&mut self) -> Result<Vec<(ValidatorName, P::Node)>, NodeError> {
        match self.local_committee().await {
            Ok(committee) => self.validator_node_provider.make_nodes(&committee),
            Err(NodeError::InactiveChain(_)) | Err(NodeError::InactiveLocalChain(_)) => {
                Ok(Vec::new())
            }
            Err(e) => Err(e),
        }
    }

    /// Obtains the current epoch of the local chain.
    async fn epoch(&mut self) -> Result<Epoch, anyhow::Error> {
        Ok(self
            .chain_info()
            .await?
            .epoch
            .ok_or(NodeError::InactiveLocalChain(self.chain_id))?)
    }

    /// Obtains the identity of the current owner of the chain. HACK: In the case of a
    /// multi-owner chain, we pick one identity for which we know the private key.
    pub async fn identity(&mut self) -> Result<Owner, anyhow::Error> {
        match self.chain_info().await?.manager {
            ChainManagerInfo::Single(manager) => {
                if !self.known_key_pairs.contains_key(&manager.owner) {
                    bail!(
                        "No key available to interact with single-owner chain {}",
                        self.chain_id
                    );
                }
                Ok(manager.owner)
            }
            ChainManagerInfo::Multi(manager) => {
                let mut identities = manager
                    .owners
                    .keys()
                    .filter(|owner| self.known_key_pairs.contains_key(owner));
                let Some(identity) = identities.next() else {
                    bail!(
                        "Cannot find suitable identity to interact with multi-owner chain {}",
                        self.chain_id
                    );
                };
                ensure!(
                    identities.next().is_none(),
                    anyhow!(
                        "Found several possible identities to interact with multi-owner chain {}",
                        self.chain_id
                    )
                );
                Ok(*identity)
            }
            ChainManagerInfo::None => Err(NodeError::InactiveLocalChain(self.chain_id).into()),
        }
    }

    /// Obtains the key pair associated to the current identity.
    pub async fn key_pair(&mut self) -> Result<&KeyPair> {
        let id = self.identity().await?;
        Ok(self
            .known_key_pairs
            .get(&id)
            .expect("key should be known at this point"))
    }

    /// Obtains the public key associated to the current identity.
    pub async fn public_key(&mut self) -> Result<PublicKey> {
        Ok(self.key_pair().await?.public())
    }

    async fn get_local_next_block_height(
        this: Arc<Mutex<Self>>,
        chain_id: ChainId,
        local_node: &mut LocalNodeClient<S>,
    ) -> Option<BlockHeight> {
        let mut guard = this.lock().await;
        let Ok(info) = local_node.local_chain_info(chain_id).await else {
            error!("Fail to read local chain info for {chain_id}");
            return None;
        };
        // Useful in case `chain_id` is the same as the local chain.
        guard.update_from_info(&info);
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
                if Self::get_local_next_block_height(this.clone(), origin.sender, &mut local_node)
                    .await
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
                if Self::get_local_next_block_height(this, origin.sender, &mut local_node).await
                    <= Some(height)
                {
                    error!("Fail to synchronize new message after notification");
                    return false;
                }
            }
            Reason::NewBlock { height, .. } => {
                let chain_id = notification.chain_id;
                if Self::get_local_next_block_height(this.clone(), chain_id, &mut local_node).await
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
                        if Self::get_local_next_block_height(this, chain_id, &mut local_node).await
                            <= Some(height)
                        {
                            error!("Fail to synchronize new block after notification");
                            return false;
                        }
                    }
                    Err(e) => {
                        error!("Fail to process notification: {e}");
                        return false;
                    }
                }
            }
        }
        // Accept the notification.
        true
    }

    /// Listens to notifications about the current chain from all validators.
    pub async fn listen(this: Arc<Mutex<Self>>) -> Result<NotificationStream>
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
    ) -> Result<(), NodeError>
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
    async fn prepare_chain(&mut self) -> Result<(), NodeError> {
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
            linera_base::ensure!(
                self.block_hash == info.block_hash,
                NodeError::InvalidLocalBlockChaining
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
        Ok(())
    }

    /// Broadcasts certified blocks and optionally one more block proposal.
    /// The corresponding block heights should be consecutive and increasing.
    async fn communicate_chain_updates(
        &mut self,
        committee: &Committee,
        chain_id: ChainId,
        action: CommunicateAction,
    ) -> Result<Option<Certificate>> {
        let storage_client = self.node_client.storage_client().await;
        let cross_chain_delay = self.cross_chain_delay;
        let cross_chain_retries = self.cross_chain_retries;
        let nodes: Vec<_> = self.validator_node_provider.make_nodes(committee)?;
        let result = communicate_with_quorum(
            &nodes,
            committee,
            |value: &Option<LiteVote>| -> Option<_> {
                value.as_ref().map(|vote| vote.value.value_hash)
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
        let votes = match result {
            Ok((_hash, votes)) => votes,
            Err(CommunicationError::Trusted(NodeError::InactiveChain(id)))
                if id == chain_id
                    && matches!(action, CommunicateAction::AdvanceToNextBlockHeight(_)) =>
            {
                // The chain is visibly not active (yet or any more) so there is no need
                // to synchronize block heights.
                return Ok(None);
            }
            Err(CommunicationError::Trusted(err)) => {
                bail!("Failed to communicate with a quorum of validators: {}", err)
            }
            Err(CommunicationError::Sample(errors)) => {
                bail!(
                    "Failed to communicate with a quorum of validators:\n{:#?}",
                    errors
                )
            }
        };
        let signatures: Vec<_> = votes
            .into_iter()
            .filter_map(|vote| vote.map(|vote| (vote.validator, vote.signature)))
            .collect();
        match action {
            CommunicateAction::SubmitBlockForConfirmation(proposal) => {
                let block = proposal.content.block;
                let round = proposal.content.round;
                let (executed_block, _) = self.node_client.stage_block_execution(block).await?;
                let value = HashedValue::from(CertificateValue::ConfirmedBlock { executed_block });
                let certificate = Certificate::new(value, round, signatures);
                // Certificate is valid because
                // * `communicate_with_quorum` ensured a sufficient "weight" of
                // (non-error) answers were returned by validators.
                // * each answer is a vote signed by the expected validator.
                Ok(Some(certificate))
            }
            CommunicateAction::SubmitBlockForValidation(proposal) => {
                let BlockAndRound { block, round } = proposal.content;
                let (executed_block, _) = self.node_client.stage_block_execution(block).await?;
                let value = HashedValue::new_validated(executed_block);
                let certificate = Certificate::new(value, round, signatures);
                Ok(Some(certificate))
            }
            CommunicateAction::FinalizeBlock(validity_certificate) => {
                let round = validity_certificate.round;
                let conf_value = validity_certificate.value.into_confirmed();
                Ok(Some(Certificate::new(conf_value, round, signatures)))
            }
            CommunicateAction::AdvanceToNextBlockHeight(_) => Ok(None),
        }
    }

    async fn receive_certificate_internal(
        &mut self,
        certificate: Certificate,
        mode: ReceiveCertificateMode,
    ) -> Result<()> {
        let CertificateValue::ConfirmedBlock { executed_block, .. } = certificate.value() else {
            bail!("Was expecting a confirmed chain operation");
        };
        let block = &executed_block.block;
        // Verify the certificate before doing any expensive networking.
        let (committees, max_epoch) = self.known_committees().await?;
        ensure!(
            block.epoch <= max_epoch,
            "Cannot accept a certificate from an unknown committee in the future. \
             Please synchronize the local view of the admin chain",
        );
        let remote_committee = committees.get(&block.epoch).ok_or_else(|| {
            anyhow!(
                "Cannot accept a certificate from a committee that was retired. \
                 Try a newer certificate from the same origin"
            )
        })?;
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
            if let NodeError::ApplicationBytecodesNotFound(locations) = &err {
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
            let Some(certificate) = response.info
                .requested_sent_certificates.pop() else {
                break;
            };
            let CertificateValue::ConfirmedBlock { executed_block, .. } = certificate.value() else {
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

    /// Process the result of [`synchronize_received_certificates_from_validator`].
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
    async fn find_received_certificates(&mut self) -> Result<()> {
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
            Err(CommunicationError::Trusted(err)) => {
                bail!("Failed to communicate with a quorum of validators: {}", err)
            }
            Err(CommunicationError::Sample(errors)) => {
                bail!(
                    "Failed to communicate with a quorum of validators:\n{:#?}",
                    errors
                )
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
    ) -> Result<(), NodeError>
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
    ) -> Result<Certificate> {
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
    ) -> Result<Certificate> {
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
    ) -> Result<(), NodeError> {
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

    /// Executes (or retries) a regular block proposal. Update local balance.
    /// If `with_confirmation` is false, we stop short of executing the finalized block.
    async fn propose_block(&mut self, block: Block) -> Result<Certificate> {
        let next_round = self.next_round;
        ensure!(
            matches!(&self.pending_block, None)
                || matches!(&self.pending_block, Some(r) if *r == block),
            "Client state has a different pending block",
        );
        ensure!(
            block.height == self.next_block_height,
            "Unexpected block height"
        );
        ensure!(
            block.previous_block_hash == self.block_hash,
            "Unexpected previous block hash"
        );
        // Remember what we are trying to do
        self.pending_block = Some(block.clone());
        // Collect blobs required for execution.
        let committee = self.local_committee().await?;
        let nodes = self.validator_node_provider.make_nodes(&committee)?;
        let blobs = self
            .node_client
            .read_or_download_blobs(nodes, block.bytecode_locations())
            .await?;
        // Build the initial query.
        let key_pair = self.key_pair().await?;
        let proposal = BlockProposal::new(
            BlockAndRound {
                block,
                round: next_round,
            },
            key_pair,
            blobs,
        );
        // Try to execute the block locally first.
        self.node_client
            .handle_block_proposal(proposal.clone())
            .await?;
        // Send the query to validators.
        let final_certificate = match self.chain_info().await?.manager {
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
            "A different operation was executed in parallel (consider retrying the operation)"
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
    pub async fn execute_operations(&mut self, operations: Vec<Operation>) -> Result<Certificate> {
        self.prepare_chain().await?;
        let messages = self.pending_messages().await?;
        self.execute_block(messages, operations).await
    }

    /// Executes an operation.
    pub async fn execute_operation(&mut self, operation: Operation) -> Result<Certificate> {
        self.execute_operations(vec![operation]).await
    }

    /// Executes a new block.
    ///
    /// This must be preceded by a call to `prepare_chain()`.
    async fn execute_block(
        &mut self,
        incoming_messages: Vec<IncomingMessage>,
        operations: Vec<Operation>,
    ) -> Result<Certificate> {
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
    pub async fn query_application(&mut self, query: &Query) -> Result<Response> {
        let response = self
            .node_client
            .query_application(self.chain_id, query)
            .await?;
        Ok(response)
    }

    /// Queries a system application.
    pub async fn query_system_application(&mut self, query: SystemQuery) -> Result<SystemResponse> {
        let response = self
            .node_client
            .query_application(self.chain_id, &Query::System(query))
            .await?;
        match response {
            Response::System(response) => Ok(response),
            _ => bail!("Unexpected response for system query"),
        }
    }

    /// Queries a user application.
    pub async fn query_user_application<A: Abi>(
        &mut self,
        application_id: UserApplicationId<A>,
        query: &A::Query,
    ) -> Result<A::QueryResponse> {
        let query = Query::user(application_id, query)?;
        let response = self
            .node_client
            .query_application(self.chain_id, &query)
            .await?;
        match response {
            Response::User(response) => Ok(serde_json::from_slice(&response)?),
            _ => bail!("Unexpected response for user query"),
        }
    }

    pub async fn local_balance(&mut self) -> Result<Amount> {
        ensure!(
            self.chain_info().await?.next_block_height == self.next_block_height,
            "The local node is behind the trusted state in wallet and needs synchronization with validators"
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
    pub async fn update_validators(&mut self) -> Result<()> {
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
    ) -> Result<Certificate> {
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
    ) -> Result<Certificate> {
        self.transfer(owner, amount, Recipient::Account(account), user_data)
            .await
    }

    /// Burns tokens.
    pub async fn burn(
        &mut self,
        owner: Option<Owner>,
        amount: Amount,
        user_data: UserData,
    ) -> Result<Certificate> {
        self.transfer(owner, amount, Recipient::Burn, user_data)
            .await
    }

    /// Attempts to synchronize with validators and re-compute our balance.
    pub async fn synchronize_from_validators(&mut self) -> Result<Amount> {
        self.find_received_certificates().await?;
        self.prepare_chain().await?;
        self.local_balance().await
    }

    /// Retries the last pending block
    pub async fn retry_pending_block(&mut self) -> Result<Option<Certificate>> {
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
    pub async fn receive_certificate(&mut self, certificate: Certificate) -> Result<()> {
        self.receive_certificate_internal(certificate, ReceiveCertificateMode::NeedsCheck)
            .await
    }

    /// Rotates the key of the chain.
    pub async fn rotate_key_pair(&mut self, key_pair: KeyPair) -> Result<Certificate> {
        let new_public_key = key_pair.public();
        self.known_key_pairs.insert(new_public_key.into(), key_pair);
        self.transfer_ownership(new_public_key).await
    }

    /// Transfers ownership of the chain.
    pub async fn transfer_ownership(&mut self, new_public_key: PublicKey) -> Result<Certificate> {
        self.execute_operation(Operation::System(SystemOperation::ChangeOwner {
            new_public_key,
        }))
        .await
    }

    /// Adds another owner to the chain.
    pub async fn share_ownership(&mut self, new_public_key: PublicKey) -> Result<Certificate> {
        self.prepare_chain().await?;
        let public_key = self.public_key().await?;
        let messages = self.pending_messages().await?;
        self.execute_block(
            messages,
            vec![Operation::System(SystemOperation::ChangeMultipleOwners {
                new_public_keys: vec![public_key, new_public_key],
            })],
        )
        .await
    }

    /// Opens a new chain with a derived UID.
    pub async fn open_chain(&mut self, public_key: PublicKey) -> Result<(MessageId, Certificate)> {
        self.prepare_chain().await?;
        let (epoch, committees) = self.epoch_and_committees(self.chain_id).await?;
        let epoch = epoch.ok_or(NodeError::InactiveLocalChain(self.chain_id))?;
        let messages = self.pending_messages().await?;
        let certificate = self
            .execute_block(
                messages,
                vec![Operation::System(SystemOperation::OpenChain {
                    public_key,
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
            .ok_or_else(|| anyhow!("Failed to open a new chain"))?;
        Ok((message_id, certificate))
    }

    /// Closes the chain (and loses everything in it!!).
    pub async fn close_chain(&mut self) -> Result<Certificate> {
        self.execute_operation(Operation::System(SystemOperation::CloseChain))
            .await
    }

    /// Publishes some bytecode.
    pub async fn publish_bytecode(
        &mut self,
        contract: Bytecode,
        service: Bytecode,
    ) -> Result<(BytecodeId, Certificate)> {
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
            .ok_or_else(|| anyhow!("Failed to publish bytecode"))?;
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
    ) -> Result<(UserApplicationId<A>, Certificate)> {
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
    ) -> Result<(UserApplicationId, Certificate)> {
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
            .ok_or_else(|| anyhow!("Failed to create application"))?;
        let id = UserApplicationId {
            bytecode_id,
            creation,
        };
        Ok((id, certificate))
    }

    /// Creates a new committee and starts using it (admin chains only).
    pub async fn stage_new_committee(&mut self, committee: Committee) -> Result<Certificate> {
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
    pub async fn process_inbox(&mut self) -> Result<Vec<Certificate>> {
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
    pub async fn subscribe_to_new_committees(&mut self) -> Result<Certificate> {
        self.execute_operation(Operation::System(SystemOperation::Subscribe {
            chain_id: self.admin_id,
            channel: SystemChannel::Admin,
        }))
        .await
    }

    /// Stops listening to the admin chain for new committees. (This is only useful for
    /// testing.)
    pub async fn unsubscribe_from_new_committees(&mut self) -> Result<Certificate> {
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
    ) -> Result<Certificate> {
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
    ) -> Result<Certificate> {
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
    pub async fn finalize_committee(&mut self) -> Result<Certificate> {
        self.prepare_chain().await?;
        let (current_epoch, committees) = self.epoch_and_committees(self.chain_id).await?;
        let current_epoch = current_epoch.ok_or(NodeError::InactiveLocalChain(self.chain_id))?;
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
    ) -> Result<Certificate> {
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

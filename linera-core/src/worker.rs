// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::data_types::{ChainInfo, ChainInfoQuery, ChainInfoResponse, CrossChainRequest};
use async_trait::async_trait;
use futures::{future, FutureExt};
use linera_base::{
    crypto::{CryptoHash, KeyPair},
    data_types::{ArithmeticError, BlockHeight, Timestamp},
    doc_scalar, ensure,
    identifiers::{ChainId, Owner},
};
use linera_chain::{
    data_types::{
        Block, BlockAndRound, BlockProposal, Certificate, CertificateValue, ExecutedBlock,
        HashedValue, IncomingMessage, LiteCertificate, Medium, Origin, Target,
    },
    ChainManagerOutcome, ChainStateView,
};
use linera_execution::{
    committee::{Committee, Epoch},
    BytecodeLocation, Query, Response, UserApplicationDescription, UserApplicationId,
};
use linera_storage::Store;
use linera_views::{
    log_view::LogView,
    views::{RootView, View, ViewError},
};
use lru::LruCache;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    collections::{hash_map, BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::sync::{oneshot, Mutex};
use tracing::{debug, error, instrument, trace, warn};

#[cfg(any(test, feature = "test"))]
use {
    linera_base::identifiers::{Destination, MessageId},
    linera_chain::data_types::ChannelFullName,
    linera_execution::ApplicationRegistryView,
};

#[cfg(test)]
#[path = "unit_tests/worker_tests.rs"]
mod worker_tests;

/// Interface provided by each physical shard (aka "worker") of a validator or a local node.
/// * All commands return either the current chain info or an error.
/// * Repeating commands produces no changes and returns no error.
/// * Some handlers may return cross-chain requests, that is, messages
///   to be communicated to other workers of the same validator.
#[async_trait]
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
        blobs: Vec<HashedValue>,
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
    #[error("{epoch:?} is not recognized by chain {chain_id:}")]
    InvalidEpoch { chain_id: ChainId, epoch: Epoch },

    // Other server-side errors
    #[error("Invalid cross-chain request")]
    InvalidCrossChainRequest,
    #[error("The block does contain the hash that we expected for the previous block")]
    InvalidBlockChaining,
    #[error("The given state hash is not what we computed after executing the block")]
    IncorrectStateHash,
    #[error("The given messages are not what we computed after executing the block")]
    IncorrectMessages,
    #[error("The timestamp of a Tick operation is in the future.")]
    InvalidTimestamp,
    #[error("We don't have the value for the certificate.")]
    MissingCertificateValue,
    #[error("The hash certificate doesn't match its value.")]
    InvalidLiteCertificate,
    #[error("An additional value was provided that is not required: {value_hash}.")]
    UnneededValue { value_hash: CryptoHash },
    #[error("The following values containing application bytecode are missing: {0:?}.")]
    ApplicationBytecodesNotFound(Vec<BytecodeLocation>),
}

impl From<linera_chain::ChainError> for WorkerError {
    fn from(chain_error: linera_chain::ChainError) -> Self {
        WorkerError::ChainError(Box::new(chain_error))
    }
}

const DEFAULT_VALUE_CACHE_SIZE: usize = 1000;

/// State of a worker in a validator or a local node.
#[derive(Clone)]
pub struct WorkerState<StorageClient> {
    /// A name used for logging
    nickname: String,
    /// The signature key pair of the validator. The key may be missing for replicas
    /// without voting rights (possibly with a partial view of chains).
    key_pair: Option<Arc<KeyPair>>,
    /// Access to local persistent storage.
    storage: StorageClient,
    /// Whether inactive chains are allowed in storage.
    allow_inactive_chains: bool,
    /// Whether new messages from deprecated epochs are allowed.
    allow_messages_from_deprecated_epochs: bool,
    /// Blocks with a timestamp this far in the future will still be accepted, but the validator
    /// will wait until that timestamp before voting.
    grace_period_micros: u64,
    /// Cached values by hash.
    recent_values: Arc<Mutex<LruCache<CryptoHash, HashedValue>>>,
    /// One-shot channels to notify callers when messages of a particular chain have been
    /// delivered.
    delivery_notifiers: Arc<Mutex<DeliveryNotifiers>>,
}

type DeliveryNotifiers = HashMap<ChainId, BTreeMap<BlockHeight, Vec<oneshot::Sender<()>>>>;

impl<Client> WorkerState<Client> {
    pub fn new(nickname: String, key_pair: Option<KeyPair>, storage: Client) -> Self {
        let recent_values = Arc::new(Mutex::new(LruCache::new(
            NonZeroUsize::try_from(DEFAULT_VALUE_CACHE_SIZE).unwrap(),
        )));
        WorkerState {
            nickname,
            key_pair: key_pair.map(Arc::new),
            storage,
            allow_inactive_chains: false,
            allow_messages_from_deprecated_epochs: false,
            grace_period_micros: 0,
            recent_values,
            delivery_notifiers: Arc::default(),
        }
    }

    pub fn with_allow_inactive_chains(mut self, value: bool) -> Self {
        self.allow_inactive_chains = value;
        self
    }

    pub fn with_allow_messages_from_deprecated_epochs(mut self, value: bool) -> Self {
        self.allow_messages_from_deprecated_epochs = value;
        self
    }

    /// Returns an instance with the specified grace period, in microseconds.
    ///
    /// Blocks with a timestamp this far in the future will still be accepted, but the validator
    /// will wait until that timestamp before voting.
    pub fn with_grace_period_micros(mut self, grace_period_micros: u64) -> Self {
        self.grace_period_micros = grace_period_micros;
        self
    }

    pub fn nickname(&self) -> &str {
        &self.nickname
    }

    /// Returns the storage client so that it can be manipulated or queried.
    #[cfg(not(feature = "test"))]
    pub(crate) fn storage_client(&self) -> &Client {
        &self.storage
    }

    /// Returns the storage client so that it can be manipulated or queried by tests in other
    /// crates.
    #[cfg(feature = "test")]
    pub fn storage_client(&self) -> &Client {
        &self.storage
    }

    pub(crate) async fn full_certificate(
        &mut self,
        certificate: LiteCertificate<'_>,
    ) -> Result<Certificate, WorkerError> {
        let hash = certificate.value.value_hash;
        let mut recent_values = self.recent_values.lock().await;
        let value = recent_values
            .get(&hash)
            .ok_or(WorkerError::MissingCertificateValue)?;
        certificate
            .with_value(value.clone())
            .ok_or(WorkerError::InvalidLiteCertificate)
    }

    pub(crate) async fn recent_value(&mut self, hash: &CryptoHash) -> Option<HashedValue> {
        self.recent_values.lock().await.get(hash).cloned()
    }
}

impl<Client> WorkerState<Client>
where
    Client: Store + Clone + Send + Sync + 'static,
    ViewError: From<Client::ContextError>,
{
    // NOTE: This only works for non-sharded workers!
    #[cfg(any(test, feature = "test"))]
    pub async fn fully_handle_certificate(
        &mut self,
        certificate: Certificate,
        blobs: Vec<HashedValue>,
    ) -> Result<ChainInfoResponse, WorkerError> {
        self.fully_handle_certificate_with_notifications(certificate, blobs, None)
            .await
    }

    #[inline]
    pub(crate) async fn fully_handle_certificate_with_notifications(
        &mut self,
        certificate: Certificate,
        blobs: Vec<HashedValue>,
        mut notifications: Option<&mut Vec<Notification>>,
    ) -> Result<ChainInfoResponse, WorkerError> {
        let (response, actions) = self.handle_certificate(certificate, blobs, None).await?;
        let mut requests = VecDeque::from(actions.cross_chain_requests);
        while let Some(request) = requests.pop_front() {
            let actions = self.handle_cross_chain_request(request).await?;
            requests.extend(actions.cross_chain_requests);
            if let Some(notifications) = notifications.as_mut() {
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
        let mut chain = self.storage.load_active_chain(block.chain_id).await?;
        let (messages, state_hash) = chain.execute_block(&block).await?;
        let response = ChainInfoResponse::new(&chain, None);
        let executed_block = ExecutedBlock {
            block,
            messages,
            state_hash,
        };
        // Do not save the new state.
        Ok((executed_block, response))
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
        query: &Query,
    ) -> Result<Response, WorkerError> {
        let mut chain = self.storage.load_active_chain(chain_id).await?;
        let response = chain.query_application(query).await?;
        Ok(response)
    }

    pub(crate) async fn describe_application(
        &mut self,
        chain_id: ChainId,
        application_id: UserApplicationId,
    ) -> Result<UserApplicationDescription, WorkerError> {
        let mut chain = self.storage.load_active_chain(chain_id).await?;
        let response = chain.describe_application(application_id).await?;
        Ok(response)
    }

    /// Gets a reference to the [`KeyPair`], if available.
    fn key_pair(&self) -> Option<&KeyPair> {
        self.key_pair.as_ref().map(Arc::as_ref)
    }

    async fn create_cross_chain_request(
        &self,
        confirmed_log: &mut LogView<Client::Context, CryptoHash>,
        height_map: Vec<(Medium, Vec<BlockHeight>)>,
        sender: ChainId,
        recipient: ChainId,
    ) -> Result<CrossChainRequest, WorkerError> {
        let heights =
            BTreeSet::from_iter(height_map.iter().flat_map(|(_, heights)| heights).copied());
        let mut heights_usize = Vec::new();
        for height in heights {
            heights_usize.push(height.try_into()?);
        }
        let values = confirmed_log.multi_get(heights_usize).await?;
        let keys = values.into_iter().flatten();
        let certificates = self.storage.read_certificates(keys).await?;
        Ok(CrossChainRequest::UpdateRecipient {
            height_map,
            sender,
            recipient,
            certificates,
        })
    }

    /// Loads pending cross-chain requests.
    async fn create_network_actions(
        &self,
        chain: &mut ChainStateView<Client::Context>,
    ) -> Result<NetworkActions, WorkerError> {
        let mut heights_by_recipient: BTreeMap<_, BTreeMap<_, _>> = Default::default();
        let targets = chain.outboxes.indices().await?;
        let outboxes = chain.outboxes.try_load_entries(&targets).await?;
        for (target, outbox) in targets.into_iter().zip(outboxes) {
            let heights = outbox.queue.elements().await?;
            heights_by_recipient
                .entry(target.recipient)
                .or_default()
                .insert(target.medium, heights);
        }
        let mut actions = NetworkActions::default();
        let chain_id = chain.chain_id();
        for (recipient, height_map) in heights_by_recipient {
            let request = self
                .create_cross_chain_request(
                    &mut chain.confirmed_log,
                    height_map.into_iter().collect(),
                    chain_id,
                    recipient,
                )
                .await?;
            actions.cross_chain_requests.push(request);
        }
        Ok(actions)
    }

    /// Processes a confirmed block (aka a commit).
    async fn process_confirmed_block(
        &mut self,
        certificate: Certificate,
        blobs: &[HashedValue],
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        let CertificateValue::ConfirmedBlock { executed_block, .. } = certificate.value() else {
            panic!("Expecting a confirmation certificate");
        };
        let ExecutedBlock {
            block,
            messages,
            state_hash,
        } = executed_block;
        let mut chain = self.storage.load_active_chain(block.chain_id).await?;
        // Check that the chain is active and ready for this confirmation.
        let tip = chain.tip_state.get();
        if tip.next_block_height < block.height {
            return Err(WorkerError::MissingEarlierBlocks {
                current_block_height: tip.next_block_height,
            });
        }
        if tip.next_block_height > block.height {
            // Block was already confirmed.
            let info = ChainInfoResponse::new(&chain, self.key_pair());
            let actions = self.create_network_actions(&mut chain).await?;
            self.register_delivery_notifier(
                block.chain_id,
                block.height,
                &actions,
                notify_when_messages_are_delivered,
            )
            .await;
            return Ok((info, actions));
        }
        // Verify the certificate.
        let (epoch, committee) = chain
            .execution_state
            .system
            .current_committee()
            .expect("chain is active");
        ensure!(
            block.epoch == epoch,
            WorkerError::InvalidEpoch {
                chain_id: block.chain_id,
                epoch: block.epoch,
            }
        );
        certificate.check(committee)?;
        // This should always be true for valid certificates.
        ensure!(
            tip.block_hash == block.previous_block_hash,
            WorkerError::InvalidBlockChaining
        );
        // Verify that all required bytecode blobs are available, and no unrelated ones provided.
        self.check_no_missing_bytecode(block, blobs).await?;
        // Persist certificate and blobs.
        for value in blobs {
            self.cache_recent_value(Cow::Borrowed(value)).await;
        }
        let (result_blob, result_certificate) = tokio::join!(
            self.storage.write_values(blobs),
            self.storage.write_certificate(&certificate)
        );
        result_blob?;
        result_certificate?;
        // Execute the block and update inboxes.
        chain.remove_events_from_inboxes(block).await?;
        // We should always agree on the messages and state hash.
        let (verified_messages, verified_state_hash) = chain.execute_block(block).await?;
        ensure!(
            *messages == verified_messages,
            WorkerError::IncorrectMessages
        );
        ensure!(
            *state_hash == verified_state_hash,
            WorkerError::IncorrectStateHash
        );
        // Advance to next block height.
        let tip = chain.tip_state.get_mut();
        tip.block_hash = Some(certificate.hash());
        tip.next_block_height.try_add_assign_one()?;
        chain.confirmed_log.push(certificate.hash());
        let info = ChainInfoResponse::new(&chain, self.key_pair());
        let mut actions = self.create_network_actions(&mut chain).await?;
        actions.notifications.push(Notification {
            chain_id: block.chain_id,
            reason: Reason::NewBlock {
                height: block.height,
                hash: certificate.value.hash(),
            },
        });
        // Persist chain.
        chain.save().await?;
        // Notify the caller when cross-chain messages are delivered.
        self.register_delivery_notifier(
            block.chain_id,
            block.height,
            &actions,
            notify_when_messages_are_delivered,
        )
        .await;
        self.cache_recent_value(Cow::Owned(certificate.value)).await;
        Ok((info, actions))
    }

    /// Returns an error if the block requires bytecode we don't have, or if unrelated bytecode
    /// blobs were provided.
    async fn check_no_missing_bytecode(
        &self,
        block: &Block,
        blobs: &[HashedValue],
    ) -> Result<(), WorkerError> {
        let required_locations = block.bytecode_locations();
        // Find all certificates containing bytecode used when executing this block.
        let mut required_hashes: HashSet<_> = required_locations
            .keys()
            .map(|bytecode_location| bytecode_location.certificate_hash)
            .collect();
        for value in blobs {
            let value_hash = value.hash();
            ensure!(
                required_hashes.remove(&value_hash),
                WorkerError::UnneededValue { value_hash }
            );
        }
        let blob_hashes: HashSet<_> = blobs.iter().map(|blob| blob.hash()).collect();
        let recent_values = self.recent_values.lock().await;
        let tasks: Vec<_> = required_locations
            .into_keys()
            .filter(|location| {
                !recent_values.contains(&location.certificate_hash)
                    && !blob_hashes.contains(&location.certificate_hash)
            })
            .map(|location| {
                // TODO(#515): Don't read the value just to check existence.
                self.storage
                    .read_value(location.certificate_hash)
                    .map(move |result| (location, result))
            })
            .collect();
        let mut locations = vec![];
        for (location, result) in future::join_all(tasks).await {
            match result {
                Ok(_value) => {} // Value is not missing.
                Err(ViewError::NotFound(_)) => locations.push(location),
                Err(err) => Err(err)?,
            }
        }
        if locations.is_empty() {
            Ok(())
        } else {
            Err(WorkerError::ApplicationBytecodesNotFound(
                locations.into_iter().collect(),
            ))
        }
    }

    /// Processes a validated block issued from a multi-owner chain.
    async fn process_validated_block(
        &mut self,
        certificate: Certificate,
    ) -> Result<ChainInfoResponse, WorkerError> {
        let block = match certificate.value() {
            CertificateValue::ValidatedBlock {
                executed_block: ExecutedBlock { block, .. },
            } => block,
            _ => panic!("Expecting a validation certificate"),
        };
        // Check that the chain is active and ready for this confirmation.
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        let mut chain = self.storage.load_active_chain(block.chain_id).await?;
        let (epoch, committee) = chain
            .execution_state
            .system
            .current_committee()
            .expect("chain is active");
        ensure!(
            block.epoch == epoch,
            WorkerError::InvalidEpoch {
                chain_id: block.chain_id,
                epoch: block.epoch,
            }
        );
        certificate.check(committee)?;
        if chain
            .tip_state
            .get()
            .already_validated_block(block.height)?
            || chain
                .manager
                .get_mut()
                .check_validated_block(&certificate)?
                == ChainManagerOutcome::Skip
        {
            // If we just processed the same pending block, return the chain info unchanged.
            return Ok(ChainInfoResponse::new(&chain, self.key_pair()));
        }
        if self
            .cache_recent_value(Cow::Borrowed(&certificate.value))
            .await
        {
            let value = certificate.value.clone().into_confirmed();
            self.cache_recent_value(Cow::Owned(value)).await;
        }
        chain
            .manager
            .get_mut()
            .create_final_vote(certificate, self.key_pair());
        let info = ChainInfoResponse::new(&chain, self.key_pair());
        chain.save().await?;
        Ok(info)
    }

    async fn process_cross_chain_update(
        &mut self,
        origin: &Origin,
        recipient: ChainId,
        certificates: Vec<Certificate>,
    ) -> Result<Option<BlockHeight>, WorkerError> {
        let mut chain = self.storage.load_chain(recipient).await?;
        // Only process certificates with relevant heights and epochs.
        let next_height_to_receive = chain.next_block_height_to_receive(origin).await?;
        let last_anticipated_block_height = chain.last_anticipated_block_height(origin).await?;
        let helper = CrossChainUpdateHelper {
            nickname: &self.nickname,
            allow_messages_from_deprecated_epochs: self.allow_messages_from_deprecated_epochs,
            current_epoch: *chain.execution_state.system.epoch.get(),
            committees: chain.execution_state.system.committees.get(),
        };
        let certificates = helper.select_certificates(
            origin,
            recipient,
            next_height_to_receive,
            last_anticipated_block_height,
            certificates,
        )?;
        let Some(last_updated_height) = certificates.last().map(|cert| cert.value().height())
        else {
            return Ok(None);
        };
        // Process the received messages in certificates.
        self.storage.write_certificates(&certificates).await?;
        for certificate in certificates {
            let hash = certificate.hash();
            match certificate.value.into_inner() {
                CertificateValue::ConfirmedBlock {
                    executed_block:
                        ExecutedBlock {
                            block, messages, ..
                        },
                    ..
                } => {
                    // Update the staged chain state with the received block.
                    chain
                        .receive_block(origin, block.height, block.timestamp, messages, hash)
                        .await?
                }
                value => {
                    error!(?value, "Unexpected value in cross-chain message");
                    continue;
                }
            }
        }
        if !self.allow_inactive_chains && !chain.is_active() {
            // Refuse to create a chain state if the chain is still inactive by
            // now. Accordingly, do not send a confirmation, so that the
            // cross-chain update is retried later.
            warn!(
                "[{}] Refusing to deliver messages to {recipient:?} from {origin:?} \
                at height {last_updated_height} because the recipient is still inactive",
                self.nickname
            );
            return Ok(None);
        }
        // Save the chain.
        chain.save().await?;
        Ok(Some(last_updated_height))
    }

    pub async fn cache_recent_value<'a>(&mut self, value: Cow<'a, HashedValue>) -> bool {
        let hash = value.hash();
        let mut recent_values = self.recent_values.lock().await;
        if recent_values.contains(&hash) {
            return false;
        }
        // Cache the certificate so that clients don't have to send the value again.
        recent_values.push(hash, value.into_owned());
        true
    }

    /// Returns a stored [`Certificate`] for a chain's block.
    #[cfg(any(test, feature = "test"))]
    pub async fn read_certificate(
        &self,
        chain_id: ChainId,
        height: BlockHeight,
    ) -> Result<Option<Certificate>, WorkerError> {
        let chain = self.storage.load_active_chain(chain_id).await?;
        let certificate_hash = match chain.confirmed_log.get(height.try_into()?).await? {
            Some(hash) => hash,
            None => return Ok(None),
        };
        let certificate = self.storage.read_certificate(certificate_hash).await?;
        Ok(Some(certificate))
    }

    /// Returns the application registry for a specific chain.
    ///
    /// # Notes
    ///
    /// The returned [`ApplicationRegistryView`] holds a lock of the chain it belongs to. Incorrect
    /// usage of this method may cause deadlocks.
    #[cfg(any(test, feature = "test"))]
    pub async fn load_application_registry(
        &self,
        chain_id: ChainId,
    ) -> Result<ApplicationRegistryView<Client::Context>, WorkerError> {
        let chain = self.storage.load_active_chain(chain_id).await?;
        Ok(chain.execution_state.system.registry)
    }

    /// Returns an [`IncomingMessage`] that's awaiting to be received by the chain specified by
    /// `chain_id`.
    #[cfg(any(test, feature = "test"))]
    pub async fn find_incoming_message(
        &self,
        chain_id: ChainId,
        message_id: MessageId,
    ) -> Result<Option<IncomingMessage>, WorkerError> {
        let Some(certificate) = self
            .read_certificate(message_id.chain_id, message_id.height)
            .await?
        else {
            return Ok(None);
        };

        let index = usize::try_from(message_id.index).map_err(|_| ArithmeticError::Overflow)?;
        let Some(outgoing_message) = certificate.value().messages().get(index).cloned() else {
            return Ok(None);
        };

        let application_id = outgoing_message.message.application_id();
        let origin = Origin {
            sender: message_id.chain_id,
            medium: match outgoing_message.destination {
                Destination::Recipient(_) => Medium::Direct,
                Destination::Subscribers(name) => Medium::Channel(ChannelFullName {
                    application_id,
                    name,
                }),
            },
        };

        let mut chain = self.storage.load_active_chain(chain_id).await?;
        let mut inbox = chain.inboxes.try_load_entry_mut(&origin).await?;

        let certificate_hash = certificate.hash();
        let Some(event) = inbox
            .added_events
            .iter_mut()
            .await?
            .find(|event| {
                event.certificate_hash == certificate_hash
                    && event.height == message_id.height
                    && event.index == message_id.index
            })
            .cloned()
        else {
            return Ok(None);
        };

        assert_eq!(event.message, outgoing_message.message);

        Ok(Some(IncomingMessage { origin, event }))
    }
}

#[async_trait]
impl<Client> ValidatorWorker for WorkerState<Client>
where
    Client: Store + Clone + Send + Sync + 'static,
    ViewError: From<Client::ContextError>,
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
        let BlockProposal {
            content: BlockAndRound { block, .. },
            owner,
            signature,
            blobs,
            validated: _,
        } = &proposal;
        let chain_id = block.chain_id;
        let mut chain = self.storage.load_active_chain(chain_id).await?;
        // Check the epoch.
        let (epoch, committee) = chain
            .execution_state
            .system
            .current_committee()
            .expect("chain is active");
        ensure!(
            block.epoch == epoch,
            WorkerError::InvalidEpoch { chain_id, epoch }
        );
        if let Some(validated) = &proposal.validated {
            validated.check(committee)?;
        }
        // Check the authentication of the block.
        let public_key = chain
            .manager
            .get()
            .verify_owner(&proposal)
            .ok_or(WorkerError::InvalidOwner)?;
        signature.check(&proposal.content, public_key)?;
        // Check the authentication of the operations in the block.
        if let Some(signer) = block.authenticated_signer {
            ensure!(signer == *owner, WorkerError::InvalidSigner(signer));
        }
        // Check if the chain is ready for this new block proposal.
        // This should always pass for nodes without voting key.
        chain.tip_state.get().verify_block_chaining(block)?;
        if chain.manager.get().check_proposed_block(&proposal)? == ChainManagerOutcome::Skip {
            // If we just processed the same pending block, return the chain info unchanged.
            return Ok((
                ChainInfoResponse::new(&chain, self.key_pair()),
                NetworkActions::default(),
            ));
        }
        // Update the inboxes so that we can verify the provided blobs are legitimately required.
        // Actual execution happens below, after other validity checks.
        chain.remove_events_from_inboxes(block).await?;
        // Verify that all required bytecode blobs are available, and no unrelated ones provided.
        self.check_no_missing_bytecode(block, blobs).await?;
        // Write the values so that the bytecode is available during execution.
        self.storage.write_values(blobs).await?;
        let time_till_block = block.timestamp.saturating_diff_micros(Timestamp::now());
        ensure!(
            time_till_block <= self.grace_period_micros,
            WorkerError::InvalidTimestamp
        );
        if time_till_block > 0 {
            tokio::time::sleep(Duration::from_micros(time_till_block)).await;
        }
        let (messages, state_hash) = {
            let (messages, state_hash) = chain.execute_block(block).await?;
            // Verify that the resulting chain would have no unconfirmed incoming messages.
            chain.validate_incoming_messages().await?;
            // Reset all the staged changes as we were only validating things.
            chain.rollback();
            (messages, state_hash)
        };
        // Create the vote and store it in the chain state.
        let manager = chain.manager.get_mut();
        manager.create_vote(proposal, messages, state_hash, self.key_pair());
        // Cache the value we voted on, so the client doesn't have to send it again.
        if let Some(vote) = manager.pending() {
            self.cache_recent_value(Cow::Borrowed(&vote.value)).await;
        }
        let info = ChainInfoResponse::new(&chain, self.key_pair());
        chain.save().await?;
        // Trigger any outgoing cross-chain messages that haven't been confirmed yet.
        let actions = self.create_network_actions(&mut chain).await?;
        Ok((info, actions))
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
        self.handle_certificate(full_cert, vec![], notify_when_messages_are_delivered)
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
        blobs: Vec<HashedValue>,
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        trace!("{} <-- {:?}", self.nickname, certificate);
        ensure!(
            certificate.value().is_confirmed() || blobs.is_empty(),
            WorkerError::UnneededValue {
                value_hash: blobs[0].hash(),
            }
        );
        let (info, actions) = match certificate.value() {
            CertificateValue::ValidatedBlock { .. } => {
                // Confirm the validated block.
                self.process_validated_block(certificate)
                    .await
                    .map(|info| (info, NetworkActions::default()))?
            }
            CertificateValue::ConfirmedBlock { .. } => {
                // Execute the confirmed block.
                self.process_confirmed_block(
                    certificate,
                    &blobs,
                    notify_when_messages_are_delivered,
                )
                .await?
            }
        };
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
        let mut chain = self.storage.load_chain(query.chain_id).await?;
        let mut info = ChainInfo::from(&chain);
        if query.request_committees {
            info.requested_committees = Some(chain.execution_state.system.committees.get().clone());
        }
        if let Some(next_block_height) = query.test_next_block_height {
            ensure!(
                chain.tip_state.get().next_block_height == next_block_height,
                WorkerError::UnexpectedBlockHeight {
                    expected_block_height: next_block_height,
                    found_block_height: chain.tip_state.get().next_block_height
                }
            );
        }
        if query.request_pending_messages {
            let mut messages = Vec::new();
            let origins = chain.inboxes.indices().await?;
            let inboxes = chain.inboxes.try_load_entries(&origins).await?;
            for (origin, inbox) in origins.into_iter().zip(inboxes) {
                for event in inbox.added_events.elements().await? {
                    messages.push(IncomingMessage {
                        origin: origin.clone(),
                        event: event.clone(),
                    });
                }
            }

            info.requested_pending_messages = messages;
        }
        if let Some(range) = query.request_sent_certificates_in_range {
            let start: usize = range.start.try_into()?;
            let end = match range.limit {
                None => chain.confirmed_log.count(),
                Some(limit) => start
                    .checked_add(usize::try_from(limit).map_err(|_| ArithmeticError::Overflow)?)
                    .ok_or(ArithmeticError::Overflow)?
                    .min(chain.confirmed_log.count()),
            };
            let keys = chain.confirmed_log.read(start..end).await?;
            let certs = self.storage.read_certificates(keys).await?;
            info.requested_sent_certificates = certs;
        }
        if let Some(start) = query.request_received_log_excluding_first_nth {
            let start = usize::try_from(start).map_err(|_| ArithmeticError::Overflow)?;
            info.requested_received_log = chain.received_log.read(start..).await?;
        }
        if let Some(hash) = query.request_blob {
            info.requested_blob = Some(self.storage.read_value(hash).await?);
        }
        if query.request_manager_values {
            info.manager.add_values(chain.manager.get());
        }
        let response = ChainInfoResponse::new(info, self.key_pair());
        trace!("{} --> {:?}", self.nickname, response);
        // Trigger any outgoing cross-chain messages that haven't been confirmed yet.
        let actions = self.create_network_actions(&mut chain).await?;
        Ok((response, actions))
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
                height_map,
                sender,
                recipient,
                certificates,
            } => {
                let mut height_by_origin = Vec::new();
                for (medium, heights) in height_map {
                    let origin = Origin { sender, medium };
                    let app_certificates = certificates
                        .iter()
                        .filter(|cert| heights.binary_search(&cert.value().height()).is_ok())
                        .cloned()
                        .collect();
                    if let Some(height) = self
                        .process_cross_chain_update(&origin, recipient, app_certificates)
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
                let mut chain = self.storage.load_chain(sender).await?;
                let mut chain_state_changed = false;
                for (medium, height) in latest_heights {
                    let target = Target { recipient, medium };
                    if !chain.mark_messages_as_received(target, height).await? {
                        continue;
                    }
                    chain_state_changed = true;
                    if chain.all_messages_delivered_up_to(height) {
                        // Handle delivery notifiers for this chain, if any.
                        if let hash_map::Entry::Occupied(mut map) =
                            self.delivery_notifiers.lock().await.entry(sender)
                        {
                            while let Some(entry) = map.get_mut().first_entry() {
                                if entry.key() > &height {
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
                    }
                }
                if chain_state_changed {
                    // Save the chain state.
                    chain.save().await?;
                }
                Ok(NetworkActions::default())
            }
        }
    }
}

struct CrossChainUpdateHelper<'a> {
    nickname: &'a str,
    allow_messages_from_deprecated_epochs: bool,
    current_epoch: Option<Epoch>,
    committees: &'a BTreeMap<Epoch, Committee>,
}

impl<'a> CrossChainUpdateHelper<'a> {
    /// Checks basic invariants and deals with repeated heights and deprecated epochs.
    /// * Returns a range of certificates that are both new to us and not relying on an
    /// untrusted set of validators.
    /// * In the case of validators, if the epoch(s) of the highest certificates are not
    /// trusted, we only accept certificates that contain messages that were already
    /// executed by anticipation (i.e. received in certified blocks).
    /// * Basic invariants are checked for good measure. We still crucially trust
    /// the worker of the sending chain to have verified and executed the blocks
    /// correctly.
    fn select_certificates(
        &self,
        origin: &'a Origin,
        recipient: ChainId,
        next_height_to_receive: BlockHeight,
        last_anticipated_block_height: Option<BlockHeight>,
        mut certificates: Vec<Certificate>,
    ) -> Result<Vec<Certificate>, WorkerError> {
        let mut latest_height = None;
        let mut skipped_len = 0;
        let mut trusted_len = 0;
        for (i, certificate) in certificates.iter().enumerate() {
            let value = certificate.value();
            // Certificates are confirming blocks.
            ensure!(value.is_confirmed(), WorkerError::InvalidCrossChainRequest);
            // Make sure that the chain_id is correct.
            ensure!(
                origin.sender == value.chain_id(),
                WorkerError::InvalidCrossChainRequest
            );
            // Make sure that heights are increasing.
            ensure!(
                latest_height < Some(value.height()),
                WorkerError::InvalidCrossChainRequest
            );
            latest_height = Some(value.height());
            // Check if the block has been received already.
            if value.height() < next_height_to_receive {
                skipped_len = i + 1;
            }
            // Check if the height is trusted or the epoch is trusted.
            if self.allow_messages_from_deprecated_epochs
                || Some(value.height()) <= last_anticipated_block_height
                || Some(value.epoch()) >= self.current_epoch
                || self.committees.contains_key(&value.epoch())
            {
                trusted_len = i + 1;
            }
        }
        if skipped_len > 0 {
            let sample_value = certificates[skipped_len - 1].value();
            debug!(
                "[{}] Ignoring repeated messages to {recipient:?} from {origin:?} at height {}",
                self.nickname,
                sample_value.height(),
            );
        }
        if skipped_len < certificates.len() && trusted_len < certificates.len() {
            let sample_value = certificates[trusted_len].value();
            warn!(
                "[{}] Refusing messages to {recipient:?} from {origin:?} at height {} \
                 because the epoch {:?} is not trusted any more",
                self.nickname,
                sample_value.height(),
                sample_value.epoch(),
            );
        }
        let certificates = if skipped_len < trusted_len {
            certificates.drain(skipped_len..trusted_len).collect()
        } else {
            vec![]
        };
        Ok(certificates)
    }
}

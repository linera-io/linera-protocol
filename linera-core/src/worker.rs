// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    borrow::Cow,
    collections::{hash_map, BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use futures::{future, FutureExt};
use linera_base::{
    crypto::{CryptoHash, KeyPair},
    data_types::{ArithmeticError, BlockHeight, Round},
    doc_scalar, ensure,
    identifiers::{ChainId, Owner},
};
use linera_chain::{
    data_types::{
        Block, BlockAndRound, BlockExecutionOutcome, BlockProposal, Certificate, CertificateValue,
        ExecutedBlock, HashedCertificateValue, LiteCertificate, Medium, MessageBundle, Origin,
        OutgoingMessage, Target,
    },
    manager, ChainError, ChainStateView,
};
use linera_execution::{
    committee::Epoch, BytecodeLocation, Query, Response, UserApplicationDescription,
    UserApplicationId,
};
use linera_storage::Storage;
use linera_views::{
    log_view::LogView,
    views::{RootView, View, ViewError},
};
use lru::LruCache;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{oneshot, Mutex};
use tracing::{error, instrument, trace, warn};
#[cfg(with_testing)]
use {
    linera_base::identifiers::{BytecodeId, Destination, MessageId},
    linera_chain::data_types::{ChannelFullName, IncomingMessage, MessageAction},
};
#[cfg(with_metrics)]
use {
    linera_base::{prometheus_util, sync::Lazy},
    prometheus::{HistogramVec, IntCounterVec},
};

use crate::{
    chain_worker::{ChainWorkerActor, ChainWorkerConfig, ChainWorkerRequest, ChainWorkerState},
    data_types::{ChainInfoQuery, ChainInfoResponse, CrossChainRequest},
};

#[cfg(test)]
#[path = "unit_tests/worker_tests.rs"]
mod worker_tests;

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
        blobs: Vec<HashedCertificateValue>,
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
        computed: Vec<OutgoingMessage>,
        submitted: Vec<OutgoingMessage>,
    },
    #[error("The given message counts are not what we computed after executing the block")]
    IncorrectMessageCounts,
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
    #[error("The certificate in the block proposal is not a ValidatedBlock")]
    MissingExecutedBlockInProposal,
    #[error("Fast blocks cannot query oracles")]
    FastBlockUsingOracles,
}

impl From<linera_chain::ChainError> for WorkerError {
    fn from(chain_error: linera_chain::ChainError) -> Self {
        WorkerError::ChainError(Box::new(chain_error))
    }
}

pub(crate) const DEFAULT_VALUE_CACHE_SIZE: usize = 1000;

/// State of a worker in a validator or a local node.
#[derive(Clone)]
pub struct WorkerState<StorageClient> {
    /// A name used for logging
    nickname: String,
    /// Access to local persistent storage.
    storage: StorageClient,
    /// Configuration options for the [`ChainWorker`]s.
    chain_worker_config: ChainWorkerConfig,
    /// Blocks with a timestamp this far in the future will still be accepted, but the validator
    /// will wait until that timestamp before voting.
    grace_period: Duration,
    /// Cached values by hash.
    recent_values: Arc<Mutex<LruCache<CryptoHash, HashedCertificateValue>>>,
    /// One-shot channels to notify callers when messages of a particular chain have been
    /// delivered.
    delivery_notifiers: Arc<Mutex<DeliveryNotifiers>>,
}

pub(crate) type DeliveryNotifiers =
    HashMap<ChainId, BTreeMap<BlockHeight, Vec<oneshot::Sender<()>>>>;

impl<StorageClient> WorkerState<StorageClient> {
    pub fn new(nickname: String, key_pair: Option<KeyPair>, storage: StorageClient) -> Self {
        let recent_values = Arc::new(Mutex::new(LruCache::new(
            NonZeroUsize::try_from(DEFAULT_VALUE_CACHE_SIZE).unwrap(),
        )));
        WorkerState {
            nickname,
            storage,
            chain_worker_config: ChainWorkerConfig::default().with_key_pair(key_pair),
            grace_period: Duration::ZERO,
            recent_values,
            delivery_notifiers: Arc::default(),
        }
    }

    pub fn new_for_client(
        nickname: String,
        storage: StorageClient,
        recent_values: Arc<Mutex<LruCache<CryptoHash, HashedCertificateValue>>>,
        delivery_notifiers: Arc<Mutex<DeliveryNotifiers>>,
    ) -> Self {
        WorkerState {
            nickname,
            storage,
            chain_worker_config: ChainWorkerConfig::default(),
            grace_period: Duration::ZERO,
            recent_values,
            delivery_notifiers,
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
        self.grace_period = grace_period;
        self
    }

    pub fn nickname(&self) -> &str {
        &self.nickname
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
        let hash = certificate.value.value_hash;
        let mut recent_values = self.recent_values.lock().await;
        let value = recent_values
            .get(&hash)
            .ok_or(WorkerError::MissingCertificateValue)?;
        certificate
            .with_value(value.clone())
            .ok_or(WorkerError::InvalidLiteCertificate)
    }

    pub(crate) async fn recent_value(
        &mut self,
        hash: &CryptoHash,
    ) -> Option<HashedCertificateValue> {
        self.recent_values.lock().await.get(hash).cloned()
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
        blobs: Vec<HashedCertificateValue>,
    ) -> Result<ChainInfoResponse, WorkerError> {
        self.fully_handle_certificate_with_notifications(certificate, blobs, None)
            .await
    }

    #[inline]
    pub(crate) async fn fully_handle_certificate_with_notifications(
        &mut self,
        certificate: Certificate,
        blobs: Vec<HashedCertificateValue>,
        mut notifications: Option<&mut Vec<Notification>>,
    ) -> Result<ChainInfoResponse, WorkerError> {
        let (response, actions) = self.handle_certificate(certificate, blobs, None).await?;
        if let Some(notifications) = notifications.as_mut() {
            notifications.extend(actions.notifications);
        }
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
        ChainWorkerState::new(
            self.chain_worker_config.clone(),
            self.storage.clone(),
            chain_id,
        )
        .await?
        .read_bytecode_location(bytecode_id)
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

    /// Creates an `UpdateRecipient` request that informs the `recipient` about new
    /// cross-chain messages from `sender`.
    async fn create_cross_chain_request(
        &self,
        confirmed_log: &LogView<StorageClient::Context, CryptoHash>,
        height_map: Vec<(Medium, Vec<BlockHeight>)>,
        sender: ChainId,
        recipient: ChainId,
    ) -> Result<CrossChainRequest, WorkerError> {
        // Load all the certificates we will need, regardless of the medium.
        let heights =
            BTreeSet::from_iter(height_map.iter().flat_map(|(_, heights)| heights).copied());
        let heights_usize = heights
            .iter()
            .copied()
            .map(usize::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let hashes = confirmed_log
            .multi_get(heights_usize.clone())
            .await?
            .into_iter()
            .zip(heights_usize)
            .map(|(maybe_hash, height)| {
                maybe_hash.ok_or_else(|| ViewError::not_found("confirmed log entry", height))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let certificates = self.storage.read_certificates(hashes).await?;
        let certificates = heights
            .into_iter()
            .zip(certificates)
            .collect::<HashMap<_, _>>();
        // For each medium, select the relevant messages.
        let bundle_vecs = height_map
            .into_iter()
            .map(|(medium, heights)| {
                let bundles = heights
                    .into_iter()
                    .map(|height| {
                        certificates
                            .get(&height)?
                            .message_bundle_for(&medium, recipient)
                    })
                    .collect::<Option<_>>()?;
                Some((medium, bundles))
            })
            .collect::<Option<_>>()
            .ok_or_else(|| ChainError::InternalError("missing certificates".to_string()))?;
        Ok(CrossChainRequest::UpdateRecipient {
            sender,
            recipient,
            bundle_vecs,
        })
    }

    /// Loads pending cross-chain requests.
    async fn create_network_actions(
        &self,
        chain: &ChainStateView<StorageClient::Context>,
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
                    &chain.confirmed_log,
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
        blobs: &[HashedCertificateValue],
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        let CertificateValue::ConfirmedBlock { executed_block, .. } = certificate.value() else {
            panic!("Expecting a confirmation certificate");
        };
        let block = &executed_block.block;
        let BlockExecutionOutcome {
            messages,
            message_counts,
            state_hash,
            oracle_records,
        } = &executed_block.outcome;
        let mut chain = self.storage.load_chain(block.chain_id).await?;
        // Check that the chain is active and ready for this confirmation.
        let tip = chain.tip_state.get().clone();
        if tip.next_block_height < block.height {
            return Err(WorkerError::MissingEarlierBlocks {
                current_block_height: tip.next_block_height,
            });
        }
        if tip.next_block_height > block.height {
            // Block was already confirmed.
            let info = ChainInfoResponse::new(&chain, self.key_pair());
            let actions = self.create_network_actions(&chain).await?;
            self.register_delivery_notifier(
                block.chain_id,
                block.height,
                &actions,
                notify_when_messages_are_delivered,
            )
            .await;
            return Ok((info, actions));
        }
        if tip.is_first_block() && !chain.is_active() {
            let local_time = self.storage.clock().current_time();
            for message in &block.incoming_messages {
                if chain
                    .execute_init_message(
                        message.id(),
                        &message.event.message,
                        message.event.timestamp,
                        local_time,
                    )
                    .await?
                {
                    break;
                }
            }
        }
        chain.ensure_is_active()?;
        // Verify the certificate.
        let (epoch, committee) = chain
            .execution_state
            .system
            .current_committee()
            .expect("chain is active");
        Self::check_block_epoch(epoch, block)?;
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
            self.storage.write_hashed_certificate_values(blobs),
            self.storage.write_certificate(&certificate)
        );
        result_blob?;
        result_certificate?;
        // Execute the block and update inboxes.
        chain.remove_events_from_inboxes(block).await?;
        let local_time = self.storage.clock().current_time();
        let verified_outcome = chain
            .execute_block(block, local_time, Some(oracle_records.clone()))
            .await?;
        // We should always agree on the messages and state hash.
        ensure!(
            *messages == verified_outcome.messages,
            WorkerError::IncorrectMessages {
                computed: verified_outcome.messages,
                submitted: messages.clone(),
            }
        );
        ensure!(
            *message_counts == verified_outcome.message_counts,
            WorkerError::IncorrectMessageCounts
        );
        ensure!(
            *state_hash == verified_outcome.state_hash,
            WorkerError::IncorrectStateHash
        );
        // Advance to next block height.
        let tip = chain.tip_state.get_mut();
        tip.block_hash = Some(certificate.hash());
        tip.next_block_height.try_add_assign_one()?;
        tip.num_incoming_messages += block.incoming_messages.len() as u32;
        tip.num_operations += block.operations.len() as u32;
        tip.num_outgoing_messages += messages.len() as u32;
        chain.confirmed_log.push(certificate.hash());
        let info = ChainInfoResponse::new(&chain, self.key_pair());
        let mut actions = self.create_network_actions(&chain).await?;
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

        #[cfg(with_metrics)]
        NUM_BLOCKS.with_label_values(&[]).inc();

        Ok((info, actions))
    }

    /// Returns an error if the block requires bytecode we don't have, or if unrelated bytecode
    /// blobs were provided.
    async fn check_no_missing_bytecode(
        &self,
        block: &Block,
        blobs: &[HashedCertificateValue],
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
        let tasks = required_locations
            .into_keys()
            .filter(|location| {
                !recent_values.contains(&location.certificate_hash)
                    && !blob_hashes.contains(&location.certificate_hash)
            })
            .map(|location| {
                self.storage
                    .contains_hashed_certificate_value(location.certificate_hash)
                    .map(move |result| (location, result))
            })
            .collect::<Vec<_>>();
        let mut locations = vec![];
        for (location, result) in future::join_all(tasks).await {
            match result {
                Ok(true) => {}
                Ok(false) => locations.push(location),
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
    ) -> Result<(ChainInfoResponse, NetworkActions, bool), WorkerError> {
        let block = match certificate.value() {
            CertificateValue::ValidatedBlock {
                executed_block: ExecutedBlock { block, .. },
            } => block,
            _ => panic!("Expecting a validation certificate"),
        };
        let chain_id = block.chain_id;
        let height = block.height;
        // Check that the chain is active and ready for this confirmation.
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        let mut chain = self.storage.load_active_chain(chain_id).await?;
        let (epoch, committee) = chain
            .execution_state
            .system
            .current_committee()
            .expect("chain is active");
        Self::check_block_epoch(epoch, block)?;
        certificate.check(committee)?;
        let mut actions = NetworkActions::default();
        if chain.tip_state.get().already_validated_block(height)?
            || chain.manager.get().check_validated_block(&certificate)? == manager::Outcome::Skip
        {
            // If we just processed the same pending block, return the chain info unchanged.
            return Ok((
                ChainInfoResponse::new(&chain, self.key_pair()),
                actions,
                true,
            ));
        }
        self.cache_validated(&certificate.value).await;
        let old_round = chain.manager.get().current_round;
        chain.manager.get_mut().create_final_vote(
            certificate,
            self.key_pair(),
            self.storage.clock().current_time(),
        );
        let info = ChainInfoResponse::new(&chain, self.key_pair());
        chain.save().await?;
        let round = chain.manager.get().current_round;
        if round > old_round {
            actions.notifications.push(Notification {
                chain_id,
                reason: Reason::NewRound { height, round },
            })
        }
        Ok((info, actions, false))
    }

    /// Processes a leader timeout issued from a multi-owner chain.
    async fn process_timeout(
        &mut self,
        certificate: Certificate,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        let (chain_id, height, epoch) = match certificate.value() {
            CertificateValue::Timeout {
                chain_id,
                height,
                epoch,
                ..
            } => (*chain_id, *height, *epoch),
            _ => panic!("Expecting a leader timeout certificate"),
        };
        // Check that the chain is active and ready for this confirmation.
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        let mut chain = self.storage.load_active_chain(chain_id).await?;
        let (chain_epoch, committee) = chain
            .execution_state
            .system
            .current_committee()
            .expect("chain is active");
        ensure!(
            epoch == chain_epoch,
            WorkerError::InvalidEpoch {
                chain_id,
                chain_epoch,
                epoch
            }
        );
        certificate.check(committee)?;
        let mut actions = NetworkActions::default();
        if chain.tip_state.get().already_validated_block(height)? {
            return Ok((ChainInfoResponse::new(&chain, self.key_pair()), actions));
        }
        let old_round = chain.manager.get().current_round;
        chain
            .manager
            .get_mut()
            .handle_timeout_certificate(certificate.clone(), self.storage.clock().current_time());
        let round = chain.manager.get().current_round;
        if round > old_round {
            actions.notifications.push(Notification {
                chain_id,
                reason: Reason::NewRound { height, round },
            })
        }
        let info = ChainInfoResponse::new(&chain, self.key_pair());
        chain.save().await?;
        Ok((info, actions))
    }

    async fn process_cross_chain_update(
        &mut self,
        origin: Origin,
        recipient: ChainId,
        bundles: Vec<MessageBundle>,
    ) -> Result<Option<BlockHeight>, WorkerError> {
        ChainWorkerState::new(
            self.chain_worker_config.clone(),
            self.storage.clone(),
            recipient,
        )
        .await?
        .process_cross_chain_update(origin, bundles)
        .await
    }

    pub async fn cache_recent_value<'a>(&mut self, value: Cow<'a, HashedCertificateValue>) -> bool {
        let hash = value.hash();
        let mut recent_values = self.recent_values.lock().await;
        if recent_values.contains(&hash) {
            return false;
        }
        // Cache the certificate so that clients don't have to send the value again.
        recent_values.push(hash, value.into_owned());
        true
    }

    /// Caches the validated block and the corresponding confirmed block.
    async fn cache_validated(&mut self, value: &HashedCertificateValue) {
        if self.cache_recent_value(Cow::Borrowed(value)).await {
            if let Some(value) = value.validated_to_confirmed() {
                self.cache_recent_value(Cow::Owned(value)).await;
            }
        }
    }

    /// Returns a stored [`Certificate`] for a chain's block.
    #[cfg(with_testing)]
    pub async fn read_certificate(
        &self,
        chain_id: ChainId,
        height: BlockHeight,
    ) -> Result<Option<Certificate>, WorkerError> {
        ChainWorkerState::new(
            self.chain_worker_config.clone(),
            self.storage.clone(),
            chain_id,
        )
        .await?
        .read_certificate(height)
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
        let index = usize::try_from(message_id.index).map_err(|_| ArithmeticError::Overflow)?;
        let Some(certificate) = self.read_certificate(sender, message_id.height).await? else {
            return Ok(None);
        };
        let Some(messages) = certificate.value().messages() else {
            return Ok(None);
        };
        let Some(outgoing_message) = messages.get(index).cloned() else {
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

        let Some(event) = ChainWorkerState::new(
            self.chain_worker_config.clone(),
            self.storage.clone(),
            chain_id,
        )
        .await?
        .find_event_in_inbox(
            origin.clone(),
            certificate.hash(),
            message_id.height,
            message_id.index,
        )
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

    /// Returns an error if the block is not at the expected epoch.
    fn check_block_epoch(chain_epoch: Epoch, block: &Block) -> Result<(), WorkerError> {
        ensure!(
            block.epoch == chain_epoch,
            WorkerError::InvalidEpoch {
                chain_id: block.chain_id,
                epoch: block.epoch,
                chain_epoch
            }
        );
        Ok(())
    }

    /// Sends a request to the [`ChainWorker`] for a [`ChainId`] and waits for the `Response`.
    async fn query_chain_worker<Response>(
        &self,
        chain_id: ChainId,
        request_builder: impl FnOnce(
            oneshot::Sender<Result<Response, WorkerError>>,
        ) -> ChainWorkerRequest,
    ) -> Result<Response, WorkerError> {
        let chain_actor = ChainWorkerActor::spawn(
            self.chain_worker_config.clone(),
            self.storage.clone(),
            chain_id,
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

impl<StorageClient> WorkerState<StorageClient> {
    /// Gets a reference to the [`KeyPair`], if available.
    fn key_pair(&self) -> Option<&KeyPair> {
        self.chain_worker_config.key_pair()
    }
}

#[async_trait]
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
        let BlockProposal {
            content: BlockAndRound { block, round },
            owner,
            blobs,
            validated,
            signature: _,
        } = &proposal;
        let chain_id = block.chain_id;
        let mut chain = self.storage.load_active_chain(chain_id).await?;
        // Check the epoch.
        let (epoch, committee) = chain
            .execution_state
            .system
            .current_committee()
            .expect("chain is active");
        Self::check_block_epoch(epoch, block)?;
        if let Some(validated) = validated {
            validated.check(committee)?;
        }
        // Check the authentication of the block.
        let public_key = chain
            .manager
            .get()
            .verify_owner(&proposal)
            .ok_or(WorkerError::InvalidOwner)?;
        proposal.check_signature(public_key)?;
        // Check the authentication of the operations in the block.
        if let Some(signer) = block.authenticated_signer {
            ensure!(signer == *owner, WorkerError::InvalidSigner(signer));
        }
        // Check if the chain is ready for this new block proposal.
        // This should always pass for nodes without voting key.
        chain.tip_state.get().verify_block_chaining(block)?;
        if chain.manager.get().check_proposed_block(&proposal)? == manager::Outcome::Skip {
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
        self.storage.write_hashed_certificate_values(blobs).await?;
        let local_time = self.storage.clock().current_time();
        ensure!(
            block.timestamp.duration_since(local_time) <= self.grace_period,
            WorkerError::InvalidTimestamp
        );
        self.storage.clock().sleep_until(block.timestamp).await;
        let local_time = self.storage.clock().current_time();
        let outcome = if let Some(validated) = validated {
            validated
                .value()
                .executed_block()
                .ok_or_else(|| WorkerError::MissingExecutedBlockInProposal)?
                .outcome
                .clone()
        } else {
            chain.execute_block(block, local_time, None).await?
        };
        if round.is_fast() {
            let mut records = outcome.oracle_records.iter();
            ensure!(
                records.all(|record| record.responses.is_empty()),
                WorkerError::FastBlockUsingOracles
            );
        }
        // Check if the counters of tip_state would be valid.
        chain.tip_state.get().verify_counters(block, &outcome)?;
        // Verify that the resulting chain would have no unconfirmed incoming messages.
        chain.validate_incoming_messages().await?;
        // Reset all the staged changes as we were only validating things.
        chain.rollback();
        // Create the vote and store it in the chain state.
        let manager = chain.manager.get_mut();
        #[cfg(with_metrics)]
        let round = proposal.content.round;
        manager.create_vote(proposal, outcome, self.key_pair(), local_time);
        // Cache the value we voted on, so the client doesn't have to send it again.
        if let Some(vote) = manager.pending() {
            self.cache_validated(&vote.value).await;
        }
        let info = ChainInfoResponse::new(&chain, self.key_pair());
        chain.save().await?;
        // Trigger any outgoing cross-chain messages that haven't been confirmed yet.
        let actions = self.create_network_actions(&chain).await?;
        #[cfg(with_metrics)]
        NUM_ROUNDS_IN_BLOCK_PROPOSAL
            .with_label_values(&[round.type_name()])
            .observe(round.number() as f64);
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
        blobs: Vec<HashedCertificateValue>,
        notify_when_messages_are_delivered: Option<oneshot::Sender<()>>,
    ) -> Result<(ChainInfoResponse, NetworkActions), WorkerError> {
        trace!("{} <-- {:?}", self.nickname, certificate);
        ensure!(
            certificate.value().is_confirmed() || blobs.is_empty(),
            WorkerError::UnneededValue {
                value_hash: blobs[0].hash(),
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
                    &blobs,
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
        let result = async move {
            ChainWorkerState::new(
                self.chain_worker_config.clone(),
                self.storage.clone(),
                query.chain_id,
            )
            .await?
            .handle_chain_info_query(query)
            .await
        }
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

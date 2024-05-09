// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    borrow::Cow,
    collections::{hash_map, BTreeMap, BTreeSet, HashMap, VecDeque},
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
        ExecutedBlock, HashedCertificateValue, IncomingMessage, LiteCertificate, Medium,
        MessageAction, MessageBundle, Origin, OutgoingMessage, Target,
    },
    manager, ChainError, ChainStateView,
};
use linera_execution::{
    committee::{Committee, Epoch},
    BytecodeLocation, Query, Response, UserApplicationDescription, UserApplicationId,
};
use linera_storage::Storage;
use linera_views::{
    log_view::LogView,
    views::{RootView, View, ViewError},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{oneshot, Mutex};
use tracing::{debug, error, instrument, trace, warn};
#[cfg(with_testing)]
use {
    linera_base::identifiers::{Destination, MessageId},
    linera_chain::data_types::ChannelFullName,
    linera_execution::ApplicationRegistryView,
};
#[cfg(with_metrics)]
use {
    linera_base::{prometheus_util, sync::Lazy},
    prometheus::{HistogramVec, IntCounterVec},
};

use crate::{
    data_types::{ChainInfo, ChainInfoQuery, ChainInfoResponse, CrossChainRequest},
    value_cache::CertificateValueCache,
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
    grace_period: Duration,
    /// Cached values by hash.
    recent_values: Arc<CertificateValueCache>,
    /// One-shot channels to notify callers when messages of a particular chain have been
    /// delivered.
    delivery_notifiers: Arc<Mutex<DeliveryNotifiers>>,
}

pub(crate) type DeliveryNotifiers =
    HashMap<ChainId, BTreeMap<BlockHeight, Vec<oneshot::Sender<()>>>>;

impl<StorageClient> WorkerState<StorageClient> {
    pub fn new(nickname: String, key_pair: Option<KeyPair>, storage: StorageClient) -> Self {
        WorkerState {
            nickname,
            key_pair: key_pair.map(Arc::new),
            storage,
            allow_inactive_chains: false,
            allow_messages_from_deprecated_epochs: false,
            grace_period: Duration::ZERO,
            recent_values: Arc::new(CertificateValueCache::default()),
            delivery_notifiers: Arc::default(),
        }
    }

    pub fn new_for_client(
        nickname: String,
        storage: StorageClient,
        recent_values: Arc<CertificateValueCache>,
        delivery_notifiers: Arc<Mutex<DeliveryNotifiers>>,
    ) -> Self {
        WorkerState {
            nickname,
            key_pair: None,
            storage,
            allow_inactive_chains: false,
            allow_messages_from_deprecated_epochs: false,
            grace_period: Duration::ZERO,
            recent_values,
            delivery_notifiers,
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
        self.key_pair = key_pair;
        self
    }

    pub(crate) async fn full_certificate(
        &mut self,
        certificate: LiteCertificate<'_>,
    ) -> Result<Certificate, WorkerError> {
        self.recent_values.full_certificate(certificate).await
    }

    pub(crate) async fn recent_value(
        &mut self,
        hash: &CryptoHash,
    ) -> Option<HashedCertificateValue> {
        self.recent_values.get(hash).await
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
    ) -> Result<ChainInfoResponse, WorkerError> {
        self.fully_handle_certificate_with_notifications(
            certificate,
            hashed_certificate_values,
            None,
        )
        .await
    }

    #[inline]
    pub(crate) async fn fully_handle_certificate_with_notifications(
        &mut self,
        certificate: Certificate,
        hashed_certificate_values: Vec<HashedCertificateValue>,
        mut notifications: Option<&mut Vec<Notification>>,
    ) -> Result<ChainInfoResponse, WorkerError> {
        let (response, actions) = self
            .handle_certificate(certificate, hashed_certificate_values, None)
            .await?;
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
        let mut chain = self.storage.load_active_chain(block.chain_id).await?;
        let local_time = self.storage.clock().current_time();
        let signer = block.authenticated_signer;
        let executed_block = chain
            .execute_block(&block, local_time, None)
            .await?
            .with(block);
        let mut response = ChainInfoResponse::new(&chain, None);
        if let Some(signer) = signer {
            response.info.requested_owner_balance =
                chain.execution_state.system.balances.get(&signer).await?;
        }
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
        query: Query,
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
        hashed_certificate_values: &[HashedCertificateValue],
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
        // Verify that all required bytecode hashed certificate values are available, and no unrelated ones provided.
        self.check_no_missing_bytecode(block, hashed_certificate_values)
            .await?;
        // Persist certificate and hashed certificate values.
        for value in hashed_certificate_values {
            self.cache_recent_value(Cow::Borrowed(value)).await;
        }
        let (result_hashed_certificate_value, result_certificate) = tokio::join!(
            self.storage
                .write_hashed_certificate_values(hashed_certificate_values),
            self.storage.write_certificate(&certificate)
        );
        result_hashed_certificate_value?;
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
        self.recent_values
            .insert(Cow::Owned(certificate.value))
            .await;

        #[cfg(with_metrics)]
        NUM_BLOCKS.with_label_values(&[]).inc();

        Ok((info, actions))
    }

    /// Returns an error if the block requires bytecode we don't have, or if unrelated bytecode
    /// hashed certificate values were provided.
    async fn check_no_missing_bytecode(
        &self,
        block: &Block,
        hashed_certificate_values: &[HashedCertificateValue],
    ) -> Result<(), WorkerError> {
        // Find all certificates containing bytecode used when executing this block.
        let mut required_locations_left: HashMap<_, _> = block
            .bytecode_locations()
            .into_keys()
            .map(|bytecode_location| (bytecode_location.certificate_hash, bytecode_location))
            .collect();
        for value in hashed_certificate_values {
            let value_hash = value.hash();
            ensure!(
                required_locations_left.remove(&value_hash).is_some(),
                WorkerError::UnneededValue { value_hash }
            );
        }
        let tasks = self
            .recent_values
            .subtract_cached_items_from::<_, Vec<_>>(
                required_locations_left.into_values(),
                |location| &location.certificate_hash,
            )
            .await
            .into_iter()
            .map(|location| {
                self.storage
                    .contains_hashed_certificate_value(location.certificate_hash)
                    .map(move |result| (location, result))
            })
            .collect::<Vec<_>>();
        let mut missing_locations = vec![];
        for (location, result) in future::join_all(tasks).await {
            match result {
                Ok(true) => {}
                Ok(false) => missing_locations.push(location),
                Err(err) => Err(err)?,
            }
        }
        if missing_locations.is_empty() {
            Ok(())
        } else {
            Err(WorkerError::ApplicationBytecodesNotFound(
                missing_locations.into_iter().collect(),
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
        self.recent_values
            .insert_validated_and_confirmed(&certificate.value)
            .await;
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
        origin: &Origin,
        recipient: ChainId,
        bundles: Vec<MessageBundle>,
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
        let bundles = helper.select_message_bundles(
            origin,
            recipient,
            next_height_to_receive,
            last_anticipated_block_height,
            bundles,
        )?;
        let Some(last_updated_height) = bundles.last().map(|bundle| bundle.height) else {
            return Ok(None);
        };
        // Process the received messages in certificates.
        let local_time = self.storage.clock().current_time();
        for bundle in bundles {
            // Update the staged chain state with the received block.
            chain
                .receive_message_bundle(origin, bundle, local_time)
                .await?
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

    pub async fn cache_recent_value<'a>(&mut self, value: Cow<'a, HashedCertificateValue>) -> bool {
        self.recent_values.insert(value).await
    }

    /// Returns a stored [`Certificate`] for a chain's block.
    #[cfg(with_testing)]
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
    #[cfg(with_testing)]
    pub async fn load_application_registry(
        &self,
        chain_id: ChainId,
    ) -> Result<ApplicationRegistryView<StorageClient::Context>, WorkerError> {
        let chain = self.storage.load_active_chain(chain_id).await?;
        Ok(chain.execution_state.system.registry)
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
        let BlockProposal {
            content: BlockAndRound { block, round },
            owner,
            hashed_certificate_values,
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
        // Update the inboxes so that we can verify the provided hashed certificate values are legitimately required.
        // Actual execution happens below, after other validity checks.
        chain.remove_events_from_inboxes(block).await?;
        // Verify that all required bytecode hashed certificate values are available, and no unrelated ones provided.
        self.check_no_missing_bytecode(block, hashed_certificate_values)
            .await?;
        // Write the values so that the bytecode is available during execution.
        self.storage
            .write_hashed_certificate_values(hashed_certificate_values)
            .await?;
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
            self.recent_values
                .insert_validated_and_confirmed(&vote.value)
                .await;
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
        hashed_certificate_values: Vec<HashedCertificateValue>,
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
        let chain_id = query.chain_id;
        let mut chain = self.storage.load_chain(chain_id).await?;
        if query.request_leader_timeout {
            if let Some(epoch) = chain.execution_state.system.epoch.get() {
                let height = chain.tip_state.get().next_block_height;
                let key_pair = self.key_pair();
                let local_time = self.storage.clock().current_time();
                let manager = chain.manager.get_mut();
                if manager.vote_timeout(chain_id, height, *epoch, key_pair, local_time) {
                    chain.save().await?;
                }
            }
        }
        if query.request_fallback {
            if let (Some(epoch), Some(entry)) = (
                chain.execution_state.system.epoch.get(),
                chain.unskippable.front().await?,
            ) {
                let ownership = chain.execution_state.system.ownership.get();
                let elapsed = self.storage.clock().current_time().delta_since(entry.seen);
                if elapsed >= ownership.timeout_config.fallback_duration {
                    let height = chain.tip_state.get().next_block_height;
                    let key_pair = self.key_pair();
                    let manager = chain.manager.get_mut();
                    if manager.vote_fallback(chain_id, height, *epoch, key_pair) {
                        chain.save().await?;
                    }
                }
            }
        }
        let mut info = ChainInfo::from(&chain);
        if query.request_committees {
            info.requested_committees = Some(chain.execution_state.system.committees.get().clone());
        }
        if let Some(owner) = query.request_owner_balance {
            info.requested_owner_balance =
                chain.execution_state.system.balances.get(&owner).await?;
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
            let action = if *chain.execution_state.system.closed.get() {
                MessageAction::Reject
            } else {
                MessageAction::Accept
            };
            for (origin, inbox) in origins.into_iter().zip(inboxes) {
                for event in inbox.added_events.elements().await? {
                    messages.push(IncomingMessage {
                        origin: origin.clone(),
                        event: event.clone(),
                        action,
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
        if let Some(hash) = query.request_hashed_certificate_value {
            info.requested_hashed_certificate_value =
                Some(self.storage.read_hashed_certificate_value(hash).await?);
        }
        if query.request_manager_values {
            info.manager.add_values(chain.manager.get());
        }
        let response = ChainInfoResponse::new(info, self.key_pair());
        trace!("{} --> {:?}", self.nickname, response);
        // Trigger any outgoing cross-chain messages that haven't been confirmed yet.
        let actions = self.create_network_actions(&chain).await?;
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
                sender,
                recipient,
                bundle_vecs,
            } => {
                let mut height_by_origin = Vec::new();
                for (medium, bundles) in bundle_vecs {
                    let origin = Origin { sender, medium };
                    if let Some(height) = self
                        .process_cross_chain_update(&origin, recipient, bundles)
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
                let mut height_with_fully_delivered_messages = BlockHeight::ZERO;

                for (medium, height) in latest_heights {
                    let target = Target { recipient, medium };
                    let fully_delivered = chain.mark_messages_as_received(target, height).await?
                        && chain.all_messages_delivered_up_to(height);

                    if fully_delivered && height > height_with_fully_delivered_messages {
                        height_with_fully_delivered_messages = height;
                    }
                }

                // Save the chain state.
                chain.save().await?;

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

struct CrossChainUpdateHelper<'a> {
    nickname: &'a str,
    allow_messages_from_deprecated_epochs: bool,
    current_epoch: Option<Epoch>,
    committees: &'a BTreeMap<Epoch, Committee>,
}

impl<'a> CrossChainUpdateHelper<'a> {
    /// Checks basic invariants and deals with repeated heights and deprecated epochs.
    /// * Returns a range of message bundles that are both new to us and not relying on
    /// an untrusted set of validators.
    /// * In the case of validators, if the epoch(s) of the highest bundles are not
    /// trusted, we only accept bundles that contain messages that were already
    /// executed by anticipation (i.e. received in certified blocks).
    /// * Basic invariants are checked for good measure. We still crucially trust
    /// the worker of the sending chain to have verified and executed the blocks
    /// correctly.
    fn select_message_bundles(
        &self,
        origin: &'a Origin,
        recipient: ChainId,
        next_height_to_receive: BlockHeight,
        last_anticipated_block_height: Option<BlockHeight>,
        mut bundles: Vec<MessageBundle>,
    ) -> Result<Vec<MessageBundle>, WorkerError> {
        let mut latest_height = None;
        let mut skipped_len = 0;
        let mut trusted_len = 0;
        for (i, bundle) in bundles.iter().enumerate() {
            // Make sure that heights are increasing.
            ensure!(
                latest_height < Some(bundle.height),
                WorkerError::InvalidCrossChainRequest
            );
            latest_height = Some(bundle.height);
            // Check if the block has been received already.
            if bundle.height < next_height_to_receive {
                skipped_len = i + 1;
            }
            // Check if the height is trusted or the epoch is trusted.
            if self.allow_messages_from_deprecated_epochs
                || Some(bundle.height) <= last_anticipated_block_height
                || Some(bundle.epoch) >= self.current_epoch
                || self.committees.contains_key(&bundle.epoch)
            {
                trusted_len = i + 1;
            }
        }
        if skipped_len > 0 {
            let sample_bundle = &bundles[skipped_len - 1];
            debug!(
                "[{}] Ignoring repeated messages to {recipient:?} from {origin:?} at height {}",
                self.nickname, sample_bundle.height,
            );
        }
        if skipped_len < bundles.len() && trusted_len < bundles.len() {
            let sample_bundle = &bundles[trusted_len];
            warn!(
                "[{}] Refusing messages to {recipient:?} from {origin:?} at height {} \
                 because the epoch {:?} is not trusted any more",
                self.nickname, sample_bundle.height, sample_bundle.epoch,
            );
        }
        let certificates = if skipped_len < trusted_len {
            bundles.drain(skipped_len..trusted_len).collect()
        } else {
            vec![]
        };
        Ok(certificates)
    }
}

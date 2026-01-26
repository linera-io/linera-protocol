// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod state;
use std::{
    collections::{hash_map, BTreeMap, BTreeSet, HashMap},
    convert::Infallible,
    iter,
    sync::Arc,
};

use custom_debug_derive::Debug;
use futures::{
    future::{self, Either, FusedFuture, Future, FutureExt},
    select,
    stream::{self, AbortHandle, FusedStream, FuturesUnordered, StreamExt, TryStreamExt},
};
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use linera_base::{
    abi::Abi,
    crypto::{signer, AccountPublicKey, CryptoHash, Signer, ValidatorPublicKey},
    data_types::{
        Amount, ApplicationPermissions, ArithmeticError, Blob, BlobContent, BlockHeight,
        ChainDescription, Epoch, MessagePolicy, Round, Timestamp,
    },
    ensure,
    identifiers::{
        Account, AccountOwner, ApplicationId, BlobId, BlobType, ChainId, EventId, IndexAndEvent,
        ModuleId, StreamId,
    },
    ownership::{ChainOwnership, TimeoutConfig},
    time::{Duration, Instant},
};
#[cfg(not(target_arch = "wasm32"))]
use linera_base::{data_types::Bytecode, vm::VmRuntime};
use linera_chain::{
    data_types::{BlockProposal, ChainAndHeight, IncomingBundle, ProposedBlock, Transaction},
    manager::LockingBlock,
    types::{
        Block, ConfirmedBlock, ConfirmedBlockCertificate, Timeout, TimeoutCertificate,
        ValidatedBlock,
    },
    ChainError, ChainExecutionContext, ChainStateView,
};
use linera_execution::{
    committee::Committee,
    system::{
        AdminOperation, OpenChainConfig, SystemOperation, EPOCH_STREAM_NAME,
        REMOVED_EPOCH_STREAM_NAME,
    },
    ExecutionError, Operation, Query, QueryOutcome, QueryResponse, SystemQuery, SystemResponse,
};
use linera_storage::{Clock as _, Storage as _};
use linera_views::ViewError;
use rand::seq::SliceRandom;
use serde::Serialize;
pub use state::State;
use thiserror::Error;
use tokio::sync::{mpsc, OwnedRwLockReadGuard};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, trace, warn, Instrument as _};

use super::{
    received_log::ReceivedLogs, validator_trackers::ValidatorTrackers, AbortOnDrop, Client,
    ListeningMode, PendingProposal, ReceiveCertificateMode, TimingType,
};
use crate::{
    data_types::{ChainInfo, ChainInfoQuery, ClientOutcome, RoundTimeout},
    environment::Environment,
    local_node::{LocalChainInfoExt as _, LocalNodeClient, LocalNodeError},
    node::{
        CrossChainMessageDelivery, NodeError, NotificationStream, ValidatorNode,
        ValidatorNodeProvider as _,
    },
    remote_node::RemoteNode,
    updater::{communicate_with_quorum, CommunicateAction, CommunicationError},
    worker::{Notification, Reason, WorkerError},
};

#[derive(Debug, Clone)]
pub struct Options {
    /// Maximum number of pending message bundles processed at a time in a block.
    pub max_pending_message_bundles: usize,
    /// The policy for automatically handling incoming messages.
    pub message_policy: MessagePolicy,
    /// Whether to block on cross-chain message delivery.
    pub cross_chain_message_delivery: CrossChainMessageDelivery,
    /// An additional delay, after reaching a quorum, to wait for additional validator signatures,
    /// as a fraction of time taken to reach quorum.
    pub quorum_grace_period: f64,
    /// The delay when downloading a blob, after which we try a second validator.
    pub blob_download_timeout: Duration,
    /// The delay when downloading a batch of certificates, after which we try a second validator.
    pub certificate_batch_download_timeout: Duration,
    /// Maximum number of certificates that we download at a time from one validator when
    /// synchronizing one of our chains.
    pub certificate_download_batch_size: u64,
    /// Maximum number of sender certificates we try to download and receive in one go
    /// when syncing sender chains.
    pub sender_certificate_download_batch_size: usize,
    /// Maximum number of tasks that can be joined concurrently using buffer_unordered.
    pub max_joined_tasks: usize,
    /// Whether to allow creating blocks in the fast round. Fast blocks have lower latency but
    /// must be used carefully so that there are never any conflicting fast block proposals.
    pub allow_fast_blocks: bool,
}

#[cfg(with_testing)]
impl Options {
    pub fn test_default() -> Self {
        use super::{
            DEFAULT_CERTIFICATE_DOWNLOAD_BATCH_SIZE, DEFAULT_SENDER_CERTIFICATE_DOWNLOAD_BATCH_SIZE,
        };
        use crate::DEFAULT_QUORUM_GRACE_PERIOD;

        Options {
            max_pending_message_bundles: 10,
            message_policy: MessagePolicy::new_accept_all(),
            cross_chain_message_delivery: CrossChainMessageDelivery::NonBlocking,
            quorum_grace_period: DEFAULT_QUORUM_GRACE_PERIOD,
            blob_download_timeout: Duration::from_secs(1),
            certificate_batch_download_timeout: Duration::from_secs(1),
            certificate_download_batch_size: DEFAULT_CERTIFICATE_DOWNLOAD_BATCH_SIZE,
            sender_certificate_download_batch_size: DEFAULT_SENDER_CERTIFICATE_DOWNLOAD_BATCH_SIZE,
            max_joined_tasks: 100,
            allow_fast_blocks: false,
        }
    }
}

/// Client to operate a chain by interacting with validators and the given local storage
/// implementation.
/// * The chain being operated is called the "local chain" or just the "chain".
/// * As a rule, operations are considered successful (and communication may stop) when
///   they succeeded in gathering a quorum of responses.
#[derive(Debug)]
pub struct ChainClient<Env: Environment> {
    /// The Linera [`Client`] that manages operations for this chain client.
    #[debug(skip)]
    pub(crate) client: Arc<Client<Env>>,
    /// The off-chain chain ID.
    chain_id: ChainId,
    /// The client options.
    #[debug(skip)]
    options: Options,
    /// The preferred owner of the chain used to sign proposals.
    /// `None` if we cannot propose on this chain.
    preferred_owner: Option<AccountOwner>,
    /// The next block height as read from the wallet.
    initial_next_block_height: BlockHeight,
    /// The last block hash as read from the wallet.
    initial_block_hash: Option<CryptoHash>,
    /// Optional timing sender for benchmarking.
    timing_sender: Option<mpsc::UnboundedSender<(u64, TimingType)>>,
}

impl<Env: Environment> Clone for ChainClient<Env> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            chain_id: self.chain_id,
            options: self.options.clone(),
            preferred_owner: self.preferred_owner,
            initial_next_block_height: self.initial_next_block_height,
            initial_block_hash: self.initial_block_hash,
            timing_sender: self.timing_sender.clone(),
        }
    }
}

/// Error type for [`ChainClient`].
#[derive(Debug, Error)]
pub enum Error {
    #[error("Local node operation failed: {0}")]
    LocalNodeError(#[from] LocalNodeError),

    #[error("Remote node operation failed: {0}")]
    RemoteNodeError(#[from] NodeError),

    #[error(transparent)]
    ArithmeticError(#[from] ArithmeticError),

    #[error("Missing certificates: {0:?}")]
    ReadCertificatesError(Vec<CryptoHash>),

    #[error("Missing confirmed block: {0:?}")]
    MissingConfirmedBlock(CryptoHash),

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

    #[error("Signer doesn't have key to sign for chain {0}")]
    CannotFindKeyForChain(ChainId),

    #[error("client is not configured to propose on chain {0}")]
    NoAccountKeyConfigured(ChainId),

    #[error("The chain client isn't owner on chain {0}")]
    NotAnOwner(ChainId),

    #[error(transparent)]
    ViewError(#[from] ViewError),

    #[error(
        "Failed to download certificates and update local node to the next height \
         {target_next_block_height} of chain {chain_id}"
    )]
    CannotDownloadCertificates {
        chain_id: ChainId,
        target_next_block_height: BlockHeight,
    },

    #[error(transparent)]
    BcsError(#[from] bcs::Error),

    #[error(
        "Unexpected quorum: validators voted for block hash {hash} in {round}, \
         expected block hash {expected_hash} in {expected_round}"
    )]
    UnexpectedQuorum {
        hash: CryptoHash,
        round: Round,
        expected_hash: CryptoHash,
        expected_round: Round,
    },

    #[error("signer error: {0:?}")]
    Signer(#[source] Box<dyn signer::Error>),

    #[error("Cannot revoke the current epoch {0}")]
    CannotRevokeCurrentEpoch(Epoch),

    #[error("Epoch is already revoked")]
    EpochAlreadyRevoked,

    #[error("Failed to download missing sender blocks from chain {chain_id} at height {height}")]
    CannotDownloadMissingSenderBlock {
        chain_id: ChainId,
        height: BlockHeight,
    },

    #[error(
        "A different block was already committed at this height. \
         The committed certificate hash is {0}"
    )]
    Conflict(CryptoHash),
}

impl From<Infallible> for Error {
    fn from(infallible: Infallible) -> Self {
        match infallible {}
    }
}

impl Error {
    pub fn signer_failure(err: impl signer::Error + 'static) -> Self {
        Self::Signer(Box::new(err))
    }
}

impl<Env: Environment> ChainClient<Env> {
    pub fn new(
        client: Arc<Client<Env>>,
        chain_id: ChainId,
        options: Options,
        initial_block_hash: Option<CryptoHash>,
        initial_next_block_height: BlockHeight,
        preferred_owner: Option<AccountOwner>,
        timing_sender: Option<mpsc::UnboundedSender<(u64, TimingType)>>,
    ) -> Self {
        ChainClient {
            client,
            chain_id,
            options,
            preferred_owner,
            initial_block_hash,
            initial_next_block_height,
            timing_sender,
        }
    }

    /// Returns whether this chain is in follow-only mode.
    pub fn is_follow_only(&self) -> bool {
        self.client.is_chain_follow_only(self.chain_id)
    }

    /// Gets the client mutex from the chain's state.
    #[instrument(level = "trace", skip(self))]
    fn client_mutex(&self) -> Arc<tokio::sync::Mutex<()>> {
        self.client
            .chains
            .pin()
            .get(&self.chain_id)
            .expect("Chain client constructed for invalid chain")
            .client_mutex()
    }

    /// Gets the next pending block.
    #[instrument(level = "trace", skip(self))]
    pub fn pending_proposal(&self) -> Option<PendingProposal> {
        self.client
            .chains
            .pin()
            .get(&self.chain_id)
            .expect("Chain client constructed for invalid chain")
            .pending_proposal()
            .clone()
    }

    /// Updates the chain's state using a closure.
    #[instrument(level = "trace", skip(self, f))]
    fn update_state<F>(&self, f: F)
    where
        F: Fn(&mut State),
    {
        let chains = self.client.chains.pin();
        chains
            .update(self.chain_id, |state| {
                let mut state = state.clone_for_update_unchecked();
                f(&mut state);
                state
            })
            .expect("Chain client constructed for invalid chain");
    }

    /// Gets a reference to the client's signer instance.
    #[instrument(level = "trace", skip(self))]
    pub fn signer(&self) -> &impl Signer {
        self.client.signer()
    }

    /// Returns whether the signer has a key for the given owner.
    pub async fn has_key_for(&self, owner: &AccountOwner) -> Result<bool, Error> {
        self.client.has_key_for(owner).await
    }

    /// Gets a mutable reference to the per-`ChainClient` options.
    #[instrument(level = "trace", skip(self))]
    pub fn options_mut(&mut self) -> &mut Options {
        &mut self.options
    }

    /// Gets a reference to the per-`ChainClient` options.
    #[instrument(level = "trace", skip(self))]
    pub fn options(&self) -> &Options {
        &self.options
    }

    /// Gets the ID of the associated chain.
    #[instrument(level = "trace", skip(self))]
    pub fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    /// Gets a clone of the timing sender for benchmarking.
    pub fn timing_sender(&self) -> Option<mpsc::UnboundedSender<(u64, TimingType)>> {
        self.timing_sender.clone()
    }

    /// Gets the ID of the admin chain.
    #[instrument(level = "trace", skip(self))]
    pub fn admin_chain_id(&self) -> ChainId {
        self.client.admin_chain_id
    }

    /// Gets the currently preferred owner for signing the blocks.
    #[instrument(level = "trace", skip(self))]
    pub fn preferred_owner(&self) -> Option<AccountOwner> {
        self.preferred_owner
    }

    /// Sets the new, preferred owner for signing the blocks.
    #[instrument(level = "trace", skip(self))]
    pub fn set_preferred_owner(&mut self, preferred_owner: AccountOwner) {
        self.preferred_owner = Some(preferred_owner);
    }

    /// Unsets the preferred owner for signing the blocks.
    #[instrument(level = "trace", skip(self))]
    pub fn unset_preferred_owner(&mut self) {
        self.preferred_owner = None;
    }

    /// Obtains a `ChainStateView` for this client's chain.
    #[instrument(level = "trace")]
    pub async fn chain_state_view(
        &self,
    ) -> Result<OwnedRwLockReadGuard<ChainStateView<Env::StorageContext>>, LocalNodeError> {
        self.client.local_node.chain_state_view(self.chain_id).await
    }

    /// Returns chain IDs that this chain subscribes to.
    #[instrument(level = "trace", skip(self))]
    pub async fn event_stream_publishers(
        &self,
    ) -> Result<BTreeMap<ChainId, BTreeSet<StreamId>>, LocalNodeError> {
        let subscriptions = self
            .client
            .local_node
            .get_event_subscriptions(self.chain_id)
            .await?;
        let mut publishers = subscriptions.into_iter().fold(
            BTreeMap::<ChainId, BTreeSet<StreamId>>::new(),
            |mut map, ((chain_id, stream_id), _)| {
                map.entry(chain_id).or_default().insert(stream_id);
                map
            },
        );
        if self.chain_id != self.client.admin_chain_id {
            publishers.insert(
                self.client.admin_chain_id,
                vec![
                    StreamId::system(EPOCH_STREAM_NAME),
                    StreamId::system(REMOVED_EPOCH_STREAM_NAME),
                ]
                .into_iter()
                .collect(),
            );
        }
        Ok(publishers)
    }

    /// Subscribes to notifications from this client's chain.
    #[instrument(level = "trace")]
    pub fn subscribe(&self) -> Result<NotificationStream, LocalNodeError> {
        self.subscribe_to(self.chain_id)
    }

    /// Subscribes to notifications from the specified chain.
    #[instrument(level = "trace")]
    pub fn subscribe_to(&self, chain_id: ChainId) -> Result<NotificationStream, LocalNodeError> {
        Ok(Box::pin(UnboundedReceiverStream::new(
            self.client.notifier.subscribe(vec![chain_id]),
        )))
    }

    /// Returns the storage client used by this client's local node.
    #[instrument(level = "trace")]
    pub fn storage_client(&self) -> &Env::Storage {
        self.client.storage_client()
    }

    /// Obtains the basic `ChainInfo` data for the local chain.
    #[instrument(level = "trace")]
    pub async fn chain_info(&self) -> Result<Box<ChainInfo>, LocalNodeError> {
        let query = ChainInfoQuery::new(self.chain_id);
        let response = self
            .client
            .local_node
            .handle_chain_info_query(query)
            .await?;
        self.client.update_from_info(&response.info);
        Ok(response.info)
    }

    /// Obtains the basic `ChainInfo` data for the local chain, with chain manager values.
    #[instrument(level = "trace")]
    pub async fn chain_info_with_manager_values(&self) -> Result<Box<ChainInfo>, LocalNodeError> {
        let query = ChainInfoQuery::new(self.chain_id)
            .with_manager_values()
            .with_committees();
        let response = self
            .client
            .local_node
            .handle_chain_info_query(query)
            .await?;
        self.client.update_from_info(&response.info);
        Ok(response.info)
    }

    /// Returns the chain's description. Fetches it from the validators if necessary.
    pub async fn get_chain_description(&self) -> Result<ChainDescription, Error> {
        self.client.get_chain_description(self.chain_id).await
    }

    /// Obtains up to `self.options.max_pending_message_bundles` pending message bundles for the
    /// local chain.
    #[instrument(level = "trace")]
    async fn pending_message_bundles(&self) -> Result<Vec<IncomingBundle>, Error> {
        if self.options.message_policy.is_ignore() {
            // Ignore all messages.
            return Ok(Vec::new());
        }

        let query = ChainInfoQuery::new(self.chain_id).with_pending_message_bundles();
        let info = self
            .client
            .local_node
            .handle_chain_info_query(query)
            .await?
            .info;
        if self.preferred_owner.is_some_and(|owner| {
            info.manager
                .ownership
                .is_super_owner_no_regular_owners(&owner)
        }) {
            // There are only super owners; they are expected to sync manually.
            ensure!(
                info.next_block_height >= self.initial_next_block_height,
                Error::WalletSynchronizationError
            );
        }

        Ok(info
            .requested_pending_message_bundles
            .into_iter()
            .filter_map(|bundle| bundle.apply_policy(&self.options.message_policy))
            .take(self.options.max_pending_message_bundles)
            .collect())
    }

    /// Returns an `UpdateStreams` operation that updates this client's chain about new events
    /// in any of the streams its applications are subscribing to. Returns `None` if there are no
    /// new events.
    #[instrument(level = "trace")]
    async fn collect_stream_updates(&self) -> Result<Option<Operation>, Error> {
        // Load all our subscriptions.
        let subscription_map = self
            .client
            .local_node
            .get_event_subscriptions(self.chain_id)
            .await?;
        // Collect the indices of all new events.
        let futures = subscription_map
            .into_iter()
            .filter(|((chain_id, _), _)| {
                self.options
                    .message_policy
                    .restrict_chain_ids_to
                    .as_ref()
                    .is_none_or(|chain_set| chain_set.contains(chain_id))
            })
            .map(|((chain_id, stream_id), subscriptions)| {
                let client = self.client.clone();
                async move {
                    let next_expected_index = client
                        .local_node
                        .get_next_expected_event(chain_id, stream_id.clone())
                        .await?;
                    if let Some(next_index) = next_expected_index
                        .filter(|next_index| *next_index > subscriptions.next_index)
                    {
                        Ok(Some((chain_id, stream_id, next_index)))
                    } else {
                        Ok::<_, Error>(None)
                    }
                }
            });
        let updates = futures::stream::iter(futures)
            .buffer_unordered(self.options.max_joined_tasks)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        if updates.is_empty() {
            return Ok(None);
        }
        Ok(Some(SystemOperation::UpdateStreams(updates).into()))
    }

    #[instrument(level = "trace")]
    async fn chain_info_with_committees(&self) -> Result<Box<ChainInfo>, LocalNodeError> {
        self.client.chain_info_with_committees(self.chain_id).await
    }

    /// Obtains the current epoch of the local chain as well as its set of trusted committees.
    #[instrument(level = "trace")]
    async fn epoch_and_committees(
        &self,
    ) -> Result<(Epoch, BTreeMap<Epoch, Committee>), LocalNodeError> {
        let info = self.chain_info_with_committees().await?;
        let epoch = info.epoch;
        let committees = info.into_committees()?;
        Ok((epoch, committees))
    }

    /// Obtains the committee for the current epoch of the local chain.
    #[instrument(level = "trace")]
    pub async fn local_committee(&self) -> Result<Committee, Error> {
        let info = match self.chain_info_with_committees().await {
            Ok(info) => info,
            Err(LocalNodeError::BlobsNotFound(_)) => {
                self.synchronize_chain_state(self.chain_id).await?;
                self.chain_info_with_committees().await?
            }
            Err(err) => return Err(err.into()),
        };
        Ok(info.into_current_committee()?)
    }

    /// Obtains the committee for the latest epoch on the admin chain.
    #[instrument(level = "trace")]
    pub async fn admin_committee(&self) -> Result<(Epoch, Committee), LocalNodeError> {
        self.client.admin_committee().await
    }

    /// Obtains the identity of the current owner of the chain.
    ///
    /// Returns an error if we don't have the private key for the identity.
    #[instrument(level = "trace")]
    pub async fn identity(&self) -> Result<AccountOwner, Error> {
        let Some(preferred_owner) = self.preferred_owner else {
            return Err(Error::NoAccountKeyConfigured(self.chain_id));
        };
        let manager = self.chain_info().await?.manager;
        ensure!(
            manager.ownership.is_active(),
            LocalNodeError::InactiveChain(self.chain_id)
        );
        let fallback_owners = if manager.ownership.has_fallback() {
            self.local_committee()
                .await?
                .account_keys_and_weights()
                .map(|(key, _)| AccountOwner::from(key))
                .collect()
        } else {
            BTreeSet::new()
        };

        let is_owner = manager.ownership.is_owner(&preferred_owner)
            || fallback_owners.contains(&preferred_owner);

        if !is_owner {
            warn!(
                chain_id = %self.chain_id,
                ownership = ?manager.ownership,
                ?fallback_owners,
                ?preferred_owner,
                "The preferred owner is not configured as an owner of this chain",
            );
            return Err(Error::NotAnOwner(self.chain_id));
        }

        let has_signer = self.has_key_for(&preferred_owner).await?;

        if !has_signer {
            warn!(%self.chain_id, ?preferred_owner,
                "Chain is one of the owners but its Signer instance doesn't contain the key",
            );
            return Err(Error::CannotFindKeyForChain(self.chain_id));
        }

        Ok(preferred_owner)
    }

    /// Prepares the chain for the next operation, i.e. makes sure we have synchronized it up to
    /// its current height.
    #[instrument(level = "trace")]
    pub async fn prepare_chain(&self) -> Result<Box<ChainInfo>, Error> {
        #[cfg(with_metrics)]
        let _latency = super::metrics::PREPARE_CHAIN_LATENCY.measure_latency();

        let mut info = self.synchronize_to_known_height().await?;

        if self.preferred_owner.is_none_or(|owner| {
            !info
                .manager
                .ownership
                .is_super_owner_no_regular_owners(&owner)
        }) {
            // If we are not a super owner or there are regular owners, we could be missing recent
            // certificates created by other clients. Further synchronize blocks from the network.
            // This is a best-effort that depends on network conditions.
            info = self.client.synchronize_chain_state(self.chain_id).await?;
        }

        if info.epoch > self.client.admin_committees().await?.0 {
            self.client
                .synchronize_chain_state(self.client.admin_chain_id)
                .await?;
        }

        self.client.update_from_info(&info);
        Ok(info)
    }

    // Verifies that our local storage contains enough history compared to the
    // known block height. Otherwise, downloads the missing history from the
    // network.
    // The known height only differs if the wallet is ahead of storage.
    async fn synchronize_to_known_height(&self) -> Result<Box<ChainInfo>, Error> {
        let info = self
            .client
            .download_certificates(self.chain_id, self.initial_next_block_height)
            .await?;
        if info.next_block_height == self.initial_next_block_height {
            // Check that our local node has the expected block hash.
            ensure!(
                self.initial_block_hash == info.block_hash,
                Error::InternalError("Invalid chain of blocks in local node")
            );
        }
        Ok(info)
    }

    /// Attempts to update all validators about the local chain.
    #[instrument(level = "trace", skip(old_committee, latest_certificate))]
    pub async fn update_validators(
        &self,
        old_committee: Option<&Committee>,
        latest_certificate: Option<ConfirmedBlockCertificate>,
    ) -> Result<(), Error> {
        let update_validators_start = linera_base::time::Instant::now();
        // Communicate the new certificate now.
        if let Some(old_committee) = old_committee {
            self.communicate_chain_updates(old_committee, latest_certificate.clone())
                .await?
        };
        if let Ok(new_committee) = self.local_committee().await {
            if Some(&new_committee) != old_committee {
                // If the configuration just changed, communicate to the new committee as well.
                // (This is actually more important that updating the previous committee.)
                self.communicate_chain_updates(&new_committee, latest_certificate)
                    .await?;
            }
        }
        self.send_timing(update_validators_start, TimingType::UpdateValidators);
        Ok(())
    }

    /// Broadcasts certified blocks to validators.
    #[instrument(level = "trace", skip(committee))]
    pub async fn communicate_chain_updates(
        &self,
        committee: &Committee,
        latest_certificate: Option<ConfirmedBlockCertificate>,
    ) -> Result<(), Error> {
        let delivery = self.options.cross_chain_message_delivery;
        let height = self.chain_info().await?.next_block_height;
        self.client
            .communicate_chain_updates(
                committee,
                self.chain_id,
                height,
                delivery,
                latest_certificate,
            )
            .await
    }

    /// Synchronizes all chains that any application on this chain subscribes to.
    /// We always consider the admin chain a relevant publishing chain, for new epochs.
    async fn synchronize_publisher_chains(&self) -> Result<(), Error> {
        let subscriptions = self
            .client
            .local_node
            .get_event_subscriptions(self.chain_id)
            .await?;
        let chain_ids = subscriptions
            .iter()
            .map(|((chain_id, _), _)| *chain_id)
            .chain(iter::once(self.client.admin_chain_id))
            .filter(|chain_id| *chain_id != self.chain_id)
            .collect::<BTreeSet<_>>();
        stream::iter(
            chain_ids
                .into_iter()
                .map(|chain_id| self.client.synchronize_chain_state(chain_id)),
        )
        .buffer_unordered(self.options.max_joined_tasks)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
        Ok(())
    }

    /// Attempts to download new received certificates.
    ///
    /// This is a best effort: it will only find certificates that have been confirmed
    /// amongst sufficiently many validators of the current committee of the target
    /// chain.
    ///
    /// However, this should be the case whenever a sender's chain is still in use and
    /// is regularly upgraded to new committees.
    #[instrument(level = "trace")]
    pub async fn find_received_certificates(
        &self,
        cancellation_token: Option<CancellationToken>,
    ) -> Result<(), Error> {
        debug!(chain_id = %self.chain_id, "starting find_received_certificates");
        #[cfg(with_metrics)]
        let _latency = super::metrics::FIND_RECEIVED_CERTIFICATES_LATENCY.measure_latency();
        // Use network information from the local chain.
        let chain_id = self.chain_id;
        let (_, committee) = self.admin_committee().await?;
        let nodes = self.client.make_nodes(&committee)?;

        let trackers = self
            .client
            .local_node
            .get_received_certificate_trackers(chain_id)
            .await?;

        trace!("find_received_certificates: read trackers");

        let received_log_batches = Arc::new(std::sync::Mutex::new(Vec::new()));
        // Proceed to downloading received logs.
        let result = communicate_with_quorum(
            &nodes,
            &committee,
            |_| (),
            |remote_node| {
                let client = &self.client;
                let tracker = trackers.get(&remote_node.public_key).copied().unwrap_or(0);
                let received_log_batches = Arc::clone(&received_log_batches);
                Box::pin(async move {
                    let batch = client
                        .get_received_log_from_validator(chain_id, &remote_node, tracker)
                        .await?;
                    let mut batches = received_log_batches.lock().unwrap();
                    batches.push((remote_node.public_key, batch));
                    Ok(())
                })
            },
            self.options.quorum_grace_period,
        )
        .await;

        if let Err(error) = result {
            error!(
                %error,
                "Failed to synchronize received_logs from at least a quorum of validators",
            );
        }

        let received_logs: Vec<_> = {
            let mut received_log_batches = received_log_batches.lock().unwrap();
            std::mem::take(received_log_batches.as_mut())
        };

        debug!(
            received_logs_len = %received_logs.len(),
            received_logs_total = %received_logs.iter().map(|x| x.1.len()).sum::<usize>(),
            "collected received logs"
        );

        let (received_logs, mut validator_trackers) = {
            (
                ReceivedLogs::from_received_result(received_logs.clone()),
                ValidatorTrackers::new(received_logs, &trackers),
            )
        };

        debug!(
            num_chains = %received_logs.num_chains(),
            num_certs = %received_logs.num_certs(),
            "find_received_certificates: total number of chains and certificates to sync",
        );

        let max_blocks_per_chain =
            self.options.sender_certificate_download_batch_size / self.options.max_joined_tasks * 2;
        for received_log in received_logs.into_batches(
            self.options.sender_certificate_download_batch_size,
            max_blocks_per_chain,
        ) {
            validator_trackers = self
                .receive_sender_certificates(
                    received_log,
                    validator_trackers,
                    &nodes,
                    cancellation_token.clone(),
                )
                .await?;

            self.update_received_certificate_trackers(&validator_trackers)
                .await;
        }

        info!("find_received_certificates finished");

        Ok(())
    }

    async fn update_received_certificate_trackers(&self, trackers: &ValidatorTrackers) {
        let updated_trackers = trackers.to_map();
        trace!(?updated_trackers, "updated tracker values");

        // Update the trackers.
        if let Err(error) = self
            .client
            .local_node
            .update_received_certificate_trackers(self.chain_id, updated_trackers)
            .await
        {
            error!(
                chain_id = %self.chain_id,
                %error,
                "Failed to update the certificate trackers for chain",
            );
        }
    }

    /// Downloads and processes or preprocesses the certificates for blocks sending messages to
    /// this chain that we are still missing.
    async fn receive_sender_certificates(
        &self,
        mut received_logs: ReceivedLogs,
        mut validator_trackers: ValidatorTrackers,
        nodes: &[RemoteNode<Env::ValidatorNode>],
        cancellation_token: Option<CancellationToken>,
    ) -> Result<ValidatorTrackers, Error> {
        debug!(
            num_chains = %received_logs.num_chains(),
            num_certs = %received_logs.num_certs(),
            "receive_sender_certificates: number of chains and certificates to sync",
        );

        // Obtain the next block height we need in the local node, for each chain.
        let local_next_heights = self
            .client
            .local_node
            .next_outbox_heights(received_logs.chains(), self.chain_id)
            .await?;

        validator_trackers.filter_out_already_known(&mut received_logs, local_next_heights);

        debug!(
            remaining_total_certificates = %received_logs.num_certs(),
            "receive_sender_certificates: computed remote_heights"
        );

        let mut other_sender_chains = Vec::new();
        let (sender, mut receiver) = mpsc::unbounded_channel::<ChainAndHeight>();

        let cert_futures = received_logs.heights_per_chain().into_iter().filter_map(
            |(sender_chain_id, remote_heights)| {
                if remote_heights.is_empty() {
                    // Our highest, locally executed block is higher than any block height
                    // from the current batch. Skip this batch, but remember to wait for
                    // the messages to be delivered to the inboxes.
                    other_sender_chains.push(sender_chain_id);
                    return None;
                };
                let remote_heights = remote_heights.into_iter().collect::<Vec<_>>();
                let sender = sender.clone();
                let client = self.client.clone();
                let mut nodes = nodes.to_vec();
                nodes.shuffle(&mut rand::thread_rng());
                let received_logs_ref = &received_logs;
                Some(async move {
                    client
                        .download_and_process_sender_chain(
                            sender_chain_id,
                            &nodes,
                            received_logs_ref,
                            remote_heights,
                            sender,
                        )
                        .await
                })
            },
        );

        let update_trackers = linera_base::task::spawn(async move {
            while let Some(chain_and_height) = receiver.recv().await {
                validator_trackers.downloaded_cert(chain_and_height);
            }
            validator_trackers
        });

        let mut cancellation_future = Box::pin(
            async move {
                if let Some(token) = cancellation_token {
                    token.cancelled().await
                } else {
                    future::pending().await
                }
            }
            .fuse(),
        );

        select! {
            _ = stream::iter(cert_futures)
            .buffer_unordered(self.options.max_joined_tasks)
            .for_each(future::ready)
            => (),
            _ = cancellation_future => ()
        };

        drop(sender);

        let validator_trackers = update_trackers.await;

        debug!(
            num_other_chains = %other_sender_chains.len(),
            "receive_sender_certificates: processing certificates finished"
        );

        // Certificates for these chains were omitted from `certificates` because they were
        // already processed locally. If they were processed in a concurrent task, it is not
        // guaranteed that their cross-chain messages were already handled.
        self.retry_pending_cross_chain_requests(nodes, other_sender_chains)
            .await;

        debug!("receive_sender_certificates: finished processing other_sender_chains");

        Ok(validator_trackers)
    }

    /// Retries cross chain requests on the chains which may have been processed on
    /// another task without the messages being correctly handled.
    async fn retry_pending_cross_chain_requests(
        &self,
        nodes: &[RemoteNode<Env::ValidatorNode>],
        other_sender_chains: Vec<ChainId>,
    ) {
        let stream = FuturesUnordered::from_iter(other_sender_chains.into_iter().map(|chain_id| {
            let local_node = self.client.local_node.clone();
            async move {
                if let Err(error) = match local_node
                    .retry_pending_cross_chain_requests(chain_id)
                    .await
                {
                    Ok(()) => Ok(()),
                    Err(LocalNodeError::BlobsNotFound(blob_ids)) => {
                        if let Err(error) = self
                            .client
                            .update_local_node_with_blobs_from(blob_ids.clone(), nodes)
                            .await
                        {
                            error!(
                                ?blob_ids,
                                %error,
                                "Error while attempting to download blobs during retrying outgoing \
                                messages"
                            );
                        }
                        local_node
                            .retry_pending_cross_chain_requests(chain_id)
                            .await
                    }
                    err => err,
                } {
                    error!(
                        %chain_id,
                        %error,
                        "Failed to retry outgoing messages from chain"
                    );
                }
            }
        }));
        stream.for_each(future::ready).await;
    }

    /// Sends money.
    #[instrument(level = "trace")]
    pub async fn transfer(
        &self,
        owner: AccountOwner,
        amount: Amount,
        recipient: Account,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, Error> {
        // TODO(#467): check the balance of `owner` before signing any block proposal.
        self.execute_operation(SystemOperation::Transfer {
            owner,
            recipient,
            amount,
        })
        .await
    }

    /// Verify if a data blob is readable from storage.
    // TODO(#2490): Consider removing or renaming this.
    #[instrument(level = "trace")]
    pub async fn read_data_blob(
        &self,
        hash: CryptoHash,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, Error> {
        let blob_id = BlobId {
            hash,
            blob_type: BlobType::Data,
        };
        self.execute_operation(SystemOperation::VerifyBlob { blob_id })
            .await
    }

    /// Claims money in a remote chain.
    #[instrument(level = "trace")]
    pub async fn claim(
        &self,
        owner: AccountOwner,
        target_id: ChainId,
        recipient: Account,
        amount: Amount,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, Error> {
        self.execute_operation(SystemOperation::Claim {
            owner,
            target_id,
            recipient,
            amount,
        })
        .await
    }

    /// Requests a leader timeout vote from all validators. If a quorum signs it, creates a
    /// certificate and sends it to all validators, to make them enter the next round.
    #[instrument(level = "trace")]
    pub async fn request_leader_timeout(&self) -> Result<TimeoutCertificate, Error> {
        let chain_id = self.chain_id;
        let info = self.chain_info_with_committees().await?;
        let committee = info.current_committee()?;
        let height = info.next_block_height;
        let round = info.manager.current_round;
        let action = CommunicateAction::RequestTimeout {
            height,
            round,
            chain_id,
        };
        let value = Timeout::new(chain_id, height, info.epoch);
        let certificate = Box::new(
            self.client
                .communicate_chain_action(committee, action, value)
                .await?,
        );
        self.client.process_certificate(certificate.clone()).await?;
        // The block height didn't increase, but this will communicate the timeout as well.
        self.client
            .communicate_chain_updates(
                committee,
                chain_id,
                height,
                CrossChainMessageDelivery::NonBlocking,
                None,
            )
            .await?;
        Ok(*certificate)
    }

    /// Downloads and processes any certificates we are missing for the given chain.
    #[instrument(level = "trace", skip_all)]
    pub async fn synchronize_chain_state(
        &self,
        chain_id: ChainId,
    ) -> Result<Box<ChainInfo>, Error> {
        self.client.synchronize_chain_state(chain_id).await
    }

    /// Downloads and processes any certificates we are missing for this chain, from the given
    /// committee.
    #[instrument(level = "trace", skip_all)]
    pub async fn synchronize_chain_state_from_committee(
        &self,
        committee: Committee,
    ) -> Result<Box<ChainInfo>, Error> {
        self.client
            .synchronize_chain_from_committee(self.chain_id, committee)
            .await
    }

    /// Executes a list of operations.
    #[instrument(level = "trace", skip(operations, blobs))]
    pub async fn execute_operations(
        &self,
        operations: Vec<Operation>,
        blobs: Vec<Blob>,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, Error> {
        let timing_start = linera_base::time::Instant::now();

        let result = loop {
            let execute_block_start = linera_base::time::Instant::now();
            // TODO(#2066): Remove boxing once the call-stack is shallower
            match Box::pin(self.execute_block(operations.clone(), blobs.clone())).await {
                Ok(ClientOutcome::Committed(certificate)) => {
                    self.send_timing(execute_block_start, TimingType::ExecuteBlock);
                    break Ok(ClientOutcome::Committed(certificate));
                }
                Ok(ClientOutcome::WaitForTimeout(timeout)) => {
                    break Ok(ClientOutcome::WaitForTimeout(timeout));
                }
                Ok(ClientOutcome::Conflict(certificate)) => {
                    info!(
                        height = %certificate.block().header.height,
                        "Another block was committed."
                    );
                    break Ok(ClientOutcome::Conflict(certificate));
                }
                Err(Error::CommunicationError(CommunicationError::Trusted(
                    NodeError::UnexpectedBlockHeight {
                        expected_block_height,
                        found_block_height,
                    },
                ))) if expected_block_height > found_block_height => {
                    tracing::info!(
                        "Local state is outdated; synchronizing chain {:.8}",
                        self.chain_id
                    );
                    self.synchronize_chain_state(self.chain_id).await?;
                }
                Err(err) => return Err(err),
            };
        };

        self.send_timing(timing_start, TimingType::ExecuteOperations);

        result
    }

    /// Executes an operation.
    pub async fn execute_operation(
        &self,
        operation: impl Into<Operation>,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, Error> {
        self.execute_operations(vec![operation.into()], vec![])
            .await
    }

    /// Executes a new block.
    ///
    /// This must be preceded by a call to `prepare_chain()`.
    #[instrument(level = "trace", skip(operations, blobs))]
    async fn execute_block(
        &self,
        operations: Vec<Operation>,
        blobs: Vec<Blob>,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, Error> {
        #[cfg(with_metrics)]
        let _latency = super::metrics::EXECUTE_BLOCK_LATENCY.measure_latency();

        let mutex = self.client_mutex();
        let _guard = mutex.lock_owned().await;
        // TODO(#5092): We shouldn't need to call this explicitly.
        match self.process_pending_block_without_prepare().await? {
            ClientOutcome::Committed(Some(certificate)) => {
                return Ok(ClientOutcome::Conflict(Box::new(certificate)))
            }
            ClientOutcome::WaitForTimeout(timeout) => {
                return Ok(ClientOutcome::WaitForTimeout(timeout))
            }
            ClientOutcome::Conflict(certificate) => {
                return Ok(ClientOutcome::Conflict(certificate))
            }
            ClientOutcome::Committed(None) => {}
        }

        // Collect pending messages and epoch changes after acquiring the lock to avoid
        // race conditions where messages valid for one block height are proposed at a
        // different height.
        let transactions = self.prepend_epochs_messages_and_events(operations).await?;

        if transactions.is_empty() {
            return Err(Error::LocalNodeError(LocalNodeError::WorkerError(
                WorkerError::ChainError(Box::new(ChainError::EmptyBlock)),
            )));
        }

        let block = self.new_pending_block(transactions, blobs).await?;

        match self.process_pending_block_without_prepare().await? {
            ClientOutcome::Committed(Some(certificate)) if certificate.block() == &block => {
                Ok(ClientOutcome::Committed(certificate))
            }
            ClientOutcome::Committed(Some(certificate)) => {
                Ok(ClientOutcome::Conflict(Box::new(certificate)))
            }
            // Should be unreachable: We did set a pending block.
            ClientOutcome::Committed(None) => {
                Err(Error::BlockProposalError("Unexpected block proposal error"))
            }
            ClientOutcome::WaitForTimeout(timeout) => Ok(ClientOutcome::WaitForTimeout(timeout)),
            ClientOutcome::Conflict(certificate) => Ok(ClientOutcome::Conflict(certificate)),
        }
    }

    /// Creates a vector of transactions which, in addition to the provided operations,
    /// also contains epoch changes, receiving message bundles and event stream updates
    /// (if there are any to be processed).
    /// This should be called when executing a block, in order to make sure that any pending
    /// messages or events are included in it.
    #[instrument(level = "trace", skip(operations))]
    async fn prepend_epochs_messages_and_events(
        &self,
        operations: Vec<Operation>,
    ) -> Result<Vec<Transaction>, Error> {
        let incoming_bundles = self.pending_message_bundles().await?;
        let stream_updates = self.collect_stream_updates().await?;
        Ok(self
            .collect_epoch_changes()
            .await?
            .into_iter()
            .map(Transaction::ExecuteOperation)
            .chain(
                incoming_bundles
                    .into_iter()
                    .map(Transaction::ReceiveMessages),
            )
            .chain(
                stream_updates
                    .into_iter()
                    .map(Transaction::ExecuteOperation),
            )
            .chain(operations.into_iter().map(Transaction::ExecuteOperation))
            .collect::<Vec<_>>())
    }

    /// Creates a new pending block and handles the proposal in the local node.
    /// Next time `process_pending_block_without_prepare` is called, this block will be proposed
    /// to the validators.
    #[instrument(level = "trace", skip(transactions, blobs))]
    async fn new_pending_block(
        &self,
        transactions: Vec<Transaction>,
        blobs: Vec<Blob>,
    ) -> Result<Block, Error> {
        let identity = self.identity().await?;

        ensure!(
            self.pending_proposal().is_none(),
            Error::BlockProposalError(
                "Client state already has a pending block; \
                use the `linera retry-pending-block` command to commit that first"
            )
        );
        let info = self.chain_info_with_committees().await?;
        let timestamp = self.next_timestamp(&transactions, info.timestamp);
        let proposed_block = ProposedBlock {
            epoch: info.epoch,
            chain_id: self.chain_id,
            transactions,
            previous_block_hash: info.block_hash,
            height: info.next_block_height,
            authenticated_owner: Some(identity),
            timestamp,
        };

        let round = self.round_for_oracle(&info, &identity).await?;
        // Make sure every incoming message succeeds and otherwise remove them.
        // Also, compute the final certified hash while we're at it.
        let (block, _) = self
            .client
            .stage_block_execution_and_discard_failing_messages(
                proposed_block,
                round,
                blobs.clone(),
            )
            .await?;
        let (proposed_block, _) = block.clone().into_proposal();
        self.update_state(|state| {
            state.set_pending_proposal(proposed_block.clone(), blobs.clone())
        });
        Ok(block)
    }

    /// Returns a suitable timestamp for the next block.
    ///
    /// This will usually be the current time according to the local clock, but may be slightly
    /// ahead to make sure it's not earlier than the incoming messages or the previous block.
    #[instrument(level = "trace", skip(transactions))]
    fn next_timestamp(&self, transactions: &[Transaction], block_time: Timestamp) -> Timestamp {
        let local_time = self.storage_client().clock().current_time();
        transactions
            .iter()
            .filter_map(Transaction::incoming_bundle)
            .map(|msg| msg.bundle.timestamp)
            .max()
            .map_or(local_time, |timestamp| timestamp.max(local_time))
            .max(block_time)
    }

    /// Queries an application.
    #[instrument(level = "trace", skip(query))]
    pub async fn query_application(
        &self,
        query: Query,
        block_hash: Option<CryptoHash>,
    ) -> Result<QueryOutcome, Error> {
        loop {
            let result = self
                .client
                .local_node
                .query_application(self.chain_id, query.clone(), block_hash)
                .await;
            if let Err(LocalNodeError::BlobsNotFound(blob_ids)) = &result {
                let validators = self.client.validator_nodes().await?;
                self.client
                    .update_local_node_with_blobs_from(blob_ids.clone(), &validators)
                    .await?;
                continue; // We found the missing blob: retry.
            }
            return Ok(result?);
        }
    }

    /// Queries a system application.
    #[instrument(level = "trace", skip(query))]
    pub async fn query_system_application(
        &self,
        query: SystemQuery,
    ) -> Result<QueryOutcome<SystemResponse>, Error> {
        let QueryOutcome {
            response,
            operations,
        } = self.query_application(Query::System(query), None).await?;
        match response {
            QueryResponse::System(response) => Ok(QueryOutcome {
                response,
                operations,
            }),
            _ => Err(Error::InternalError("Unexpected response for system query")),
        }
    }

    /// Queries a user application.
    #[instrument(level = "trace", skip(application_id, query))]
    #[cfg(with_testing)]
    pub async fn query_user_application<A: Abi>(
        &self,
        application_id: ApplicationId<A>,
        query: &A::Query,
    ) -> Result<QueryOutcome<A::QueryResponse>, Error> {
        let query = Query::user(application_id, query)?;
        let QueryOutcome {
            response,
            operations,
        } = self.query_application(query, None).await?;
        match response {
            QueryResponse::User(response_bytes) => {
                let response = serde_json::from_slice(&response_bytes)?;
                Ok(QueryOutcome {
                    response,
                    operations,
                })
            }
            _ => Err(Error::InternalError("Unexpected response for user query")),
        }
    }

    /// Obtains the local balance of the chain account after staging the execution of
    /// incoming messages in a new block.
    ///
    /// Does not attempt to synchronize with validators. The result will reflect up to
    /// `max_pending_message_bundles` incoming message bundles and the execution fees for a single
    /// block.
    #[instrument(level = "trace")]
    pub async fn query_balance(&self) -> Result<Amount, Error> {
        let (balance, _) = self.query_balances_with_owner(AccountOwner::CHAIN).await?;
        Ok(balance)
    }

    /// Obtains the local balance of an account after staging the execution of incoming messages in
    /// a new block.
    ///
    /// Does not attempt to synchronize with validators. The result will reflect up to
    /// `max_pending_message_bundles` incoming message bundles and the execution fees for a single
    /// block.
    #[instrument(level = "trace", skip(owner))]
    pub async fn query_owner_balance(&self, owner: AccountOwner) -> Result<Amount, Error> {
        if owner.is_chain() {
            self.query_balance().await
        } else {
            Ok(self
                .query_balances_with_owner(owner)
                .await?
                .1
                .unwrap_or(Amount::ZERO))
        }
    }

    /// Obtains the local balance of an account and optionally another user after staging the
    /// execution of incoming messages in a new block.
    ///
    /// Does not attempt to synchronize with validators. The result will reflect up to
    /// `max_pending_message_bundles` incoming message bundles and the execution fees for a single
    /// block.
    #[instrument(level = "trace", skip(owner))]
    pub(crate) async fn query_balances_with_owner(
        &self,
        owner: AccountOwner,
    ) -> Result<(Amount, Option<Amount>), Error> {
        let incoming_bundles = self.pending_message_bundles().await?;
        // Since we disallow empty blocks, and there is no incoming messages,
        // that could change it, we query for the balance immediately.
        if incoming_bundles.is_empty() {
            let chain_balance = self.local_balance().await?;
            let owner_balance = self.local_owner_balance(owner).await?;
            return Ok((chain_balance, Some(owner_balance)));
        }
        let info = self.chain_info().await?;
        let transactions = incoming_bundles
            .into_iter()
            .map(Transaction::ReceiveMessages)
            .collect::<Vec<_>>();
        let timestamp = self.next_timestamp(&transactions, info.timestamp);
        let block = ProposedBlock {
            epoch: info.epoch,
            chain_id: self.chain_id,
            transactions,
            previous_block_hash: info.block_hash,
            height: info.next_block_height,
            authenticated_owner: if owner == AccountOwner::CHAIN {
                None
            } else {
                Some(owner)
            },
            timestamp,
        };
        match self
            .client
            .stage_block_execution_and_discard_failing_messages(block, None, Vec::new())
            .await
        {
            Ok((_, response)) => Ok((
                response.info.chain_balance,
                response.info.requested_owner_balance,
            )),
            Err(Error::LocalNodeError(LocalNodeError::WorkerError(WorkerError::ChainError(
                error,
            )))) if matches!(
                &*error,
                ChainError::ExecutionError(
                    execution_error,
                    ChainExecutionContext::Block
                ) if matches!(
                    **execution_error,
                    ExecutionError::FeesExceedFunding { .. }
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
    #[instrument(level = "trace")]
    pub async fn local_balance(&self) -> Result<Amount, Error> {
        let (balance, _) = self.local_balances_with_owner(AccountOwner::CHAIN).await?;
        Ok(balance)
    }

    /// Reads the local balance of a user account.
    ///
    /// Does not process the inbox or attempt to synchronize with validators.
    #[instrument(level = "trace", skip(owner))]
    pub async fn local_owner_balance(&self, owner: AccountOwner) -> Result<Amount, Error> {
        if owner.is_chain() {
            self.local_balance().await
        } else {
            Ok(self
                .local_balances_with_owner(owner)
                .await?
                .1
                .unwrap_or(Amount::ZERO))
        }
    }

    /// Reads the local balance of the chain account and optionally another user.
    ///
    /// Does not process the inbox or attempt to synchronize with validators.
    #[instrument(level = "trace", skip(owner))]
    pub(crate) async fn local_balances_with_owner(
        &self,
        owner: AccountOwner,
    ) -> Result<(Amount, Option<Amount>), Error> {
        ensure!(
            self.chain_info().await?.next_block_height >= self.initial_next_block_height,
            Error::WalletSynchronizationError
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

    /// Sends tokens to a chain.
    #[instrument(level = "trace")]
    pub async fn transfer_to_account(
        &self,
        from: AccountOwner,
        amount: Amount,
        account: Account,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, Error> {
        self.transfer(from, amount, account).await
    }

    /// Burns tokens (transfer to a special address).
    #[cfg(with_testing)]
    #[instrument(level = "trace")]
    pub async fn burn(
        &self,
        owner: AccountOwner,
        amount: Amount,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, Error> {
        let recipient = Account::burn_address(self.chain_id);
        self.transfer(owner, amount, recipient).await
    }

    #[instrument(level = "trace")]
    pub async fn fetch_chain_info(&self) -> Result<Box<ChainInfo>, Error> {
        let validators = self.client.validator_nodes().await?;
        self.client
            .fetch_chain_info(self.chain_id, &validators)
            .await
    }

    /// Attempts to synchronize chains that have sent us messages and populate our local
    /// inbox.
    ///
    /// To create a block that actually executes the messages in the inbox,
    /// `process_inbox` must be called separately.
    ///
    /// If the chain is in follow-only mode, this only downloads blocks for this chain without
    /// fetching manager values or sender/publisher chains.
    #[instrument(level = "trace")]
    pub async fn synchronize_from_validators(&self) -> Result<Box<ChainInfo>, Error> {
        if self.is_follow_only() {
            return self.client.synchronize_chain_state(self.chain_id).await;
        }
        let info = self.prepare_chain().await?;
        self.synchronize_publisher_chains().await?;
        self.find_received_certificates(None).await?;
        Ok(info)
    }

    /// Processes the last pending block
    #[instrument(level = "trace")]
    pub async fn process_pending_block(
        &self,
    ) -> Result<ClientOutcome<Option<ConfirmedBlockCertificate>>, Error> {
        self.prepare_chain().await?;
        self.process_pending_block_without_prepare().await
    }

    /// Processes the last pending block. Assumes that the local chain is up to date.
    #[instrument(level = "trace")]
    async fn process_pending_block_without_prepare(
        &self,
    ) -> Result<ClientOutcome<Option<ConfirmedBlockCertificate>>, Error> {
        let info = self.request_leader_timeout_if_needed().await?;

        // If there is a validated block in the current round, finalize it.
        if info.manager.has_locking_block_in_current_round()
            && !info.manager.current_round.is_fast()
        {
            return self.finalize_locking_block(info).await;
        }
        let owner = self.identity().await?;

        let local_node = &self.client.local_node;
        // Otherwise we have to re-propose the highest validated block, if there is one.
        let pending_proposal = self.pending_proposal();
        let (block, blobs) = if let Some(locking) = &info.manager.requested_locking {
            match &**locking {
                LockingBlock::Regular(certificate) => {
                    let blob_ids = certificate.block().required_blob_ids();
                    let blobs = local_node
                        .get_locking_blobs(&blob_ids, self.chain_id)
                        .await?
                        .ok_or_else(|| Error::InternalError("Missing local locking blobs"))?;
                    debug!("Retrying locking block from round {}", certificate.round);
                    (certificate.block().clone(), blobs)
                }
                LockingBlock::Fast(proposal) => {
                    let proposed_block = proposal.content.block.clone();
                    let blob_ids = proposed_block.published_blob_ids();
                    let blobs = local_node
                        .get_locking_blobs(&blob_ids, self.chain_id)
                        .await?
                        .ok_or_else(|| Error::InternalError("Missing local locking blobs"))?;
                    let block = self
                        .client
                        .stage_block_execution(proposed_block, None, blobs.clone())
                        .await?
                        .0;
                    debug!("Retrying locking block from fast round.");
                    (block, blobs)
                }
            }
        } else if let Some(pending_proposal) = pending_proposal {
            // Otherwise we are free to propose our own pending block.
            let proposed_block = pending_proposal.block;
            let round = self.round_for_oracle(&info, &owner).await?;
            let (block, _) = self
                .client
                .stage_block_execution(proposed_block, round, pending_proposal.blobs.clone())
                .await?;
            debug!("Proposing the local pending block.");
            (block, pending_proposal.blobs)
        } else {
            return Ok(ClientOutcome::Committed(None)); // Nothing to do.
        };

        let has_oracle_responses = block.has_oracle_responses();
        let (proposed_block, outcome) = block.into_proposal();
        let round = match self
            .round_for_new_proposal(&info, &owner, has_oracle_responses)
            .await?
        {
            Either::Left(round) => round,
            Either::Right(timeout) => return Ok(ClientOutcome::WaitForTimeout(timeout)),
        };
        debug!("Proposing block for round {}", round);

        let already_handled_locally = info
            .manager
            .already_handled_proposal(round, &proposed_block);
        // Create the final block proposal.
        let proposal = if let Some(locking) = info.manager.requested_locking {
            Box::new(match *locking {
                LockingBlock::Regular(cert) => {
                    BlockProposal::new_retry_regular(owner, round, cert, self.signer())
                        .await
                        .map_err(Error::signer_failure)?
                }
                LockingBlock::Fast(proposal) => {
                    BlockProposal::new_retry_fast(owner, round, proposal, self.signer())
                        .await
                        .map_err(Error::signer_failure)?
                }
            })
        } else {
            Box::new(
                BlockProposal::new_initial(owner, round, proposed_block.clone(), self.signer())
                    .await
                    .map_err(Error::signer_failure)?,
            )
        };
        if !already_handled_locally {
            // Check the final block proposal. This will be cheaper after #1401.
            if let Err(err) = local_node.handle_block_proposal(*proposal.clone()).await {
                match err {
                    LocalNodeError::BlobsNotFound(_) => {
                        local_node
                            .handle_pending_blobs(self.chain_id, blobs)
                            .await?;
                        local_node.handle_block_proposal(*proposal.clone()).await?;
                    }
                    err => return Err(err.into()),
                }
            }
        }
        let committee = self.local_committee().await?;
        let block = Block::new(proposed_block, outcome);
        // Send the query to validators.
        let submit_block_proposal_start = linera_base::time::Instant::now();
        let certificate = if round.is_fast() {
            let hashed_value = ConfirmedBlock::new(block);
            self.client
                .submit_block_proposal(&committee, proposal, hashed_value)
                .await?
        } else {
            let hashed_value = ValidatedBlock::new(block);
            let certificate = self
                .client
                .submit_block_proposal(&committee, proposal, hashed_value.clone())
                .await?;
            self.client.finalize_block(&committee, certificate).await?
        };
        self.send_timing(submit_block_proposal_start, TimingType::SubmitBlockProposal);
        debug!(round = %certificate.round, "Sending confirmed block to validators");
        self.update_validators(Some(&committee), Some(certificate.clone()))
            .await?;
        Ok(ClientOutcome::Committed(Some(certificate)))
    }

    fn send_timing(&self, start: Instant, timing_type: TimingType) {
        let Some(sender) = &self.timing_sender else {
            return;
        };
        if let Err(err) = sender.send((start.elapsed().as_millis() as u64, timing_type)) {
            tracing::warn!(%err, "Failed to send timing info");
        }
    }

    /// Requests a leader timeout certificate if the current round has timed out. Returns the
    /// chain info for the (possibly new) current round.
    async fn request_leader_timeout_if_needed(&self) -> Result<Box<ChainInfo>, Error> {
        let mut info = self.chain_info_with_manager_values().await?;
        // If the current round has timed out, we request a timeout certificate and retry in
        // the next round.
        if let Some(round_timeout) = info.manager.round_timeout {
            if round_timeout <= self.storage_client().clock().current_time() {
                if let Err(e) = self.request_leader_timeout().await {
                    info!("Failed to obtain a timeout certificate: {}", e);
                } else {
                    info = self.chain_info_with_manager_values().await?;
                }
            }
        }
        Ok(info)
    }

    /// Finalizes the locking block.
    ///
    /// Panics if there is no locking block; fails if the locking block is not in the current round.
    async fn finalize_locking_block(
        &self,
        info: Box<ChainInfo>,
    ) -> Result<ClientOutcome<Option<ConfirmedBlockCertificate>>, Error> {
        let locking = info
            .manager
            .requested_locking
            .expect("Should have a locking block");
        let LockingBlock::Regular(certificate) = *locking else {
            panic!("Should have a locking validated block");
        };
        debug!(
            round = %certificate.round,
            "Finalizing locking block"
        );
        let committee = self.local_committee().await?;
        let certificate = self
            .client
            .finalize_block(&committee, certificate.clone())
            .await?;
        self.update_validators(Some(&committee), Some(certificate.clone()))
            .await?;
        Ok(ClientOutcome::Committed(Some(certificate)))
    }

    /// Returns the number for the round number oracle to use when staging a block proposal.
    async fn round_for_oracle(
        &self,
        info: &ChainInfo,
        identity: &AccountOwner,
    ) -> Result<Option<u32>, Error> {
        // Pretend we do use oracles: If we don't, the round number is never read anyway.
        match self.round_for_new_proposal(info, identity, true).await {
            // If it is a multi-leader round, use its number for the oracle.
            Ok(Either::Left(round)) => Ok(round.multi_leader()),
            // If there is no suitable round with oracles, use None: If it works without oracles,
            // the block won't read the value. If it returns a timeout, it will be a single-leader
            // round, in which the oracle returns None.
            Err(Error::BlockProposalError(_)) | Ok(Either::Right(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Returns a round in which we can propose a new block or the given one, if possible.
    async fn round_for_new_proposal(
        &self,
        info: &ChainInfo,
        identity: &AccountOwner,
        has_oracle_responses: bool,
    ) -> Result<Either<Round, RoundTimeout>, Error> {
        let manager = &info.manager;
        let seed = manager.seed;
        // If there is a conflicting proposal in the current round, we can only propose if the
        // next round can be started without a timeout, i.e. if we are in a multi-leader round.
        // Similarly, we cannot propose a block that uses oracles in the fast round, and also
        // skip the fast round if fast blocks are not allowed.
        let skip_fast = manager.current_round.is_fast()
            && (has_oracle_responses || !self.options.allow_fast_blocks);
        let conflict = manager
            .requested_signed_proposal
            .as_ref()
            .into_iter()
            .chain(&manager.requested_proposed)
            .any(|proposal| proposal.content.round == manager.current_round)
            || skip_fast;
        let round = if !conflict {
            manager.current_round
        } else if let Some(round) = manager
            .ownership
            .next_round(manager.current_round)
            .filter(|_| manager.current_round.is_multi_leader() || manager.current_round.is_fast())
        {
            round
        } else if let Some(timeout) = info.round_timeout() {
            return Ok(Either::Right(timeout));
        } else {
            return Err(Error::BlockProposalError(
                "Conflicting proposal in the current round",
            ));
        };
        let current_committee = info
            .current_committee()?
            .validators
            .values()
            .map(|v| (AccountOwner::from(v.account_public_key), v.votes))
            .collect();
        if manager.should_propose(identity, round, seed, &current_committee) {
            return Ok(Either::Left(round));
        }
        if let Some(timeout) = info.round_timeout() {
            return Ok(Either::Right(timeout));
        }
        Err(Error::BlockProposalError(
            "Not a leader in the current round",
        ))
    }

    /// Clears the information on any operation that previously failed.
    #[cfg(with_testing)]
    #[instrument(level = "trace")]
    pub fn clear_pending_proposal(&self) {
        self.update_state(|state| state.clear_pending_proposal());
    }

    /// Rotates the key of the chain.
    ///
    /// Replaces current owners of the chain with the new key pair.
    #[instrument(level = "trace")]
    pub async fn rotate_key_pair(
        &self,
        public_key: AccountPublicKey,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, Error> {
        self.transfer_ownership(public_key.into()).await
    }

    /// Transfers ownership of the chain to a single super owner.
    #[instrument(level = "trace")]
    pub async fn transfer_ownership(
        &self,
        new_owner: AccountOwner,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, Error> {
        self.execute_operation(SystemOperation::ChangeOwnership {
            super_owners: vec![new_owner],
            owners: Vec::new(),
            first_leader: None,
            multi_leader_rounds: 2,
            open_multi_leader_rounds: false,
            timeout_config: TimeoutConfig::default(),
        })
        .await
    }

    /// Adds another owner to the chain, and turns existing super owners into regular owners.
    #[instrument(level = "trace")]
    pub async fn share_ownership(
        &self,
        new_owner: AccountOwner,
        new_weight: u64,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, Error> {
        let ownership = self.prepare_chain().await?.manager.ownership;
        ensure!(
            ownership.is_active(),
            ChainError::InactiveChain(self.chain_id)
        );
        let mut owners = ownership.owners.into_iter().collect::<Vec<_>>();
        owners.extend(ownership.super_owners.into_iter().zip(iter::repeat(100)));
        owners.push((new_owner, new_weight));
        let operations = vec![Operation::system(SystemOperation::ChangeOwnership {
            super_owners: Vec::new(),
            owners,
            first_leader: ownership.first_leader,
            multi_leader_rounds: ownership.multi_leader_rounds,
            open_multi_leader_rounds: ownership.open_multi_leader_rounds,
            timeout_config: ownership.timeout_config,
        })];
        self.execute_block(operations, vec![]).await
    }

    /// Returns the current ownership settings on this chain.
    #[instrument(level = "trace")]
    pub async fn query_chain_ownership(&self) -> Result<ChainOwnership, Error> {
        Ok(self
            .client
            .local_node
            .chain_state_view(self.chain_id)
            .await?
            .execution_state
            .system
            .ownership
            .get()
            .clone())
    }

    /// Changes the ownership of this chain. Fails if it would remove existing owners, unless
    /// `remove_owners` is `true`.
    #[instrument(level = "trace")]
    pub async fn change_ownership(
        &self,
        ownership: ChainOwnership,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, Error> {
        self.execute_operation(SystemOperation::ChangeOwnership {
            super_owners: ownership.super_owners.into_iter().collect(),
            owners: ownership.owners.into_iter().collect(),
            first_leader: ownership.first_leader,
            multi_leader_rounds: ownership.multi_leader_rounds,
            open_multi_leader_rounds: ownership.open_multi_leader_rounds,
            timeout_config: ownership.timeout_config.clone(),
        })
        .await
    }

    /// Returns the current application permissions on this chain.
    #[instrument(level = "trace")]
    pub async fn query_application_permissions(&self) -> Result<ApplicationPermissions, Error> {
        Ok(self
            .client
            .local_node
            .chain_state_view(self.chain_id)
            .await?
            .execution_state
            .system
            .application_permissions
            .get()
            .clone())
    }

    /// Changes the application permissions configuration on this chain.
    #[instrument(level = "trace", skip(application_permissions))]
    pub async fn change_application_permissions(
        &self,
        application_permissions: ApplicationPermissions,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, Error> {
        self.execute_operation(SystemOperation::ChangeApplicationPermissions(
            application_permissions,
        ))
        .await
    }

    /// Opens a new chain with a derived UID.
    #[instrument(level = "trace", skip(self))]
    pub async fn open_chain(
        &self,
        ownership: ChainOwnership,
        application_permissions: ApplicationPermissions,
        balance: Amount,
    ) -> Result<ClientOutcome<(ChainDescription, ConfirmedBlockCertificate)>, Error> {
        // Check if we have a key for any owner before consuming ownership.
        let mut has_key = false;
        for owner in ownership.all_owners() {
            if self.has_key_for(owner).await? {
                has_key = true;
                break;
            }
        }
        let config = OpenChainConfig {
            ownership,
            balance,
            application_permissions,
        };
        let operation = Operation::system(SystemOperation::OpenChain(config));
        let certificate = match self.execute_block(vec![operation], vec![]).await? {
            ClientOutcome::Committed(certificate) => certificate,
            ClientOutcome::Conflict(certificate) => {
                return Ok(ClientOutcome::Conflict(certificate));
            }
            ClientOutcome::WaitForTimeout(timeout) => {
                return Ok(ClientOutcome::WaitForTimeout(timeout));
            }
        };
        // The only operation, i.e. the last transaction, created the new chain.
        let chain_blob = certificate
            .block()
            .body
            .blobs
            .last()
            .and_then(|blobs| blobs.last())
            .ok_or_else(|| Error::InternalError("Failed to create a new chain"))?;
        let description = bcs::from_bytes::<ChainDescription>(chain_blob.bytes())?;
        // If we have a key for any owner, add it to the list of tracked chains.
        if has_key {
            self.client
                .extend_chain_mode(description.id(), ListeningMode::FullChain);
            self.client
                .local_node
                .retry_pending_cross_chain_requests(self.chain_id)
                .await?;
        }
        Ok(ClientOutcome::Committed((description, certificate)))
    }

    /// Closes the chain (and loses everything in it!!).
    /// Returns `None` if the chain was already closed.
    #[instrument(level = "trace")]
    pub async fn close_chain(
        &self,
    ) -> Result<ClientOutcome<Option<ConfirmedBlockCertificate>>, Error> {
        match self.execute_operation(SystemOperation::CloseChain).await {
            Ok(outcome) => Ok(outcome.map(Some)),
            Err(Error::LocalNodeError(LocalNodeError::WorkerError(WorkerError::ChainError(
                chain_error,
            )))) if matches!(*chain_error, ChainError::ClosedChain) => {
                Ok(ClientOutcome::Committed(None)) // Chain is already closed.
            }
            Err(error) => Err(error),
        }
    }

    /// Publishes some module.
    #[cfg(not(target_arch = "wasm32"))]
    #[instrument(level = "trace", skip(contract, service))]
    pub async fn publish_module(
        &self,
        contract: Bytecode,
        service: Bytecode,
        vm_runtime: VmRuntime,
    ) -> Result<ClientOutcome<(ModuleId, ConfirmedBlockCertificate)>, Error> {
        let (blobs, module_id) = super::create_bytecode_blobs(contract, service, vm_runtime).await;
        self.publish_module_blobs(blobs, module_id).await
    }

    /// Publishes some module.
    #[cfg(not(target_arch = "wasm32"))]
    #[instrument(level = "trace", skip(blobs, module_id))]
    pub async fn publish_module_blobs(
        &self,
        blobs: Vec<Blob>,
        module_id: ModuleId,
    ) -> Result<ClientOutcome<(ModuleId, ConfirmedBlockCertificate)>, Error> {
        self.execute_operations(
            vec![Operation::system(SystemOperation::PublishModule {
                module_id,
            })],
            blobs,
        )
        .await?
        .try_map(|certificate| Ok((module_id, certificate)))
    }

    /// Publishes some data blobs.
    #[instrument(level = "trace", skip(bytes))]
    pub async fn publish_data_blobs(
        &self,
        bytes: Vec<Vec<u8>>,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, Error> {
        let blobs = bytes.into_iter().map(Blob::new_data);
        let publish_blob_operations = blobs
            .clone()
            .map(|blob| {
                Operation::system(SystemOperation::PublishDataBlob {
                    blob_hash: blob.id().hash,
                })
            })
            .collect();
        self.execute_operations(publish_blob_operations, blobs.collect())
            .await
    }

    /// Publishes some data blob.
    #[instrument(level = "trace", skip(bytes))]
    pub async fn publish_data_blob(
        &self,
        bytes: Vec<u8>,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, Error> {
        self.publish_data_blobs(vec![bytes]).await
    }

    /// Creates an application by instantiating some bytecode.
    #[instrument(
        level = "trace",
        skip(self, parameters, instantiation_argument, required_application_ids)
    )]
    pub async fn create_application<
        A: Abi,
        Parameters: Serialize,
        InstantiationArgument: Serialize,
    >(
        &self,
        module_id: ModuleId<A, Parameters, InstantiationArgument>,
        parameters: &Parameters,
        instantiation_argument: &InstantiationArgument,
        required_application_ids: Vec<ApplicationId>,
    ) -> Result<ClientOutcome<(ApplicationId<A>, ConfirmedBlockCertificate)>, Error> {
        let instantiation_argument = serde_json::to_vec(instantiation_argument)?;
        let parameters = serde_json::to_vec(parameters)?;
        Ok(self
            .create_application_untyped(
                module_id.forget_abi(),
                parameters,
                instantiation_argument,
                required_application_ids,
            )
            .await?
            .map(|(app_id, cert)| (app_id.with_abi(), cert)))
    }

    /// Creates an application by instantiating some bytecode.
    #[instrument(
        level = "trace",
        skip(
            self,
            module_id,
            parameters,
            instantiation_argument,
            required_application_ids
        )
    )]
    pub async fn create_application_untyped(
        &self,
        module_id: ModuleId,
        parameters: Vec<u8>,
        instantiation_argument: Vec<u8>,
        required_application_ids: Vec<ApplicationId>,
    ) -> Result<ClientOutcome<(ApplicationId, ConfirmedBlockCertificate)>, Error> {
        self.execute_operation(SystemOperation::CreateApplication {
            module_id,
            parameters,
            instantiation_argument,
            required_application_ids,
        })
        .await?
        .try_map(|certificate| {
            // The first message of the only operation created the application.
            let mut creation: Vec<_> = certificate
                .block()
                .created_blob_ids()
                .into_iter()
                .filter(|blob_id| blob_id.blob_type == BlobType::ApplicationDescription)
                .collect();
            if creation.len() > 1 {
                return Err(Error::InternalError(
                    "Unexpected number of application descriptions published",
                ));
            }
            let blob_id = creation.pop().ok_or(Error::InternalError(
                "ApplicationDescription blob not found.",
            ))?;
            let id = ApplicationId::new(blob_id.hash);
            Ok((id, certificate))
        })
    }

    /// Creates a new committee and starts using it (admin chains only).
    #[instrument(level = "trace", skip(committee))]
    pub async fn stage_new_committee(
        &self,
        committee: Committee,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, Error> {
        let blob = Blob::new(BlobContent::new_committee(bcs::to_bytes(&committee)?));
        let blob_hash = blob.id().hash;
        match self
            .execute_operations(
                vec![Operation::system(SystemOperation::Admin(
                    AdminOperation::PublishCommitteeBlob { blob_hash },
                ))],
                vec![blob],
            )
            .await?
        {
            ClientOutcome::Committed(_) => {}
            outcome @ ClientOutcome::WaitForTimeout(_) | outcome @ ClientOutcome::Conflict(_) => {
                return Ok(outcome)
            }
        }
        let epoch = self.chain_info().await?.epoch.try_add_one()?;
        self.execute_operation(SystemOperation::Admin(AdminOperation::CreateCommittee {
            epoch,
            blob_hash,
        }))
        .await
    }

    /// Synchronizes the chain with the validators and creates blocks without any operations to
    /// process all incoming messages. This may require several blocks.
    ///
    /// If not all certificates could be processed due to a timeout, the timestamp for when to retry
    /// is returned, too.
    #[instrument(level = "trace")]
    pub async fn process_inbox(
        &self,
    ) -> Result<(Vec<ConfirmedBlockCertificate>, Option<RoundTimeout>), Error> {
        self.prepare_chain().await?;
        self.process_inbox_without_prepare().await
    }

    /// Creates blocks without any operations to process all incoming messages. This may require
    /// several blocks.
    ///
    /// If not all certificates could be processed due to a timeout, the timestamp for when to retry
    /// is returned, too.
    #[instrument(level = "trace")]
    pub async fn process_inbox_without_prepare(
        &self,
    ) -> Result<(Vec<ConfirmedBlockCertificate>, Option<RoundTimeout>), Error> {
        #[cfg(with_metrics)]
        let _latency = super::metrics::PROCESS_INBOX_WITHOUT_PREPARE_LATENCY.measure_latency();

        let mut certificates = Vec::new();
        loop {
            // We provide no operations - this means that the only operations executed
            // will be epoch changes, receiving messages and processing event stream
            // updates, if any are pending.
            match self.execute_block(vec![], vec![]).await {
                Ok(ClientOutcome::Committed(certificate)) => certificates.push(certificate),
                Ok(ClientOutcome::Conflict(certificate)) => certificates.push(*certificate),
                Ok(ClientOutcome::WaitForTimeout(timeout)) => {
                    return Ok((certificates, Some(timeout)));
                }
                // Nothing in the inbox and no stream updates to be processed.
                Err(Error::LocalNodeError(LocalNodeError::WorkerError(
                    WorkerError::ChainError(chain_error),
                ))) if matches!(*chain_error, ChainError::EmptyBlock) => {
                    return Ok((certificates, None));
                }
                Err(error) => return Err(error),
            };
        }
    }

    /// Returns operations to process all pending epoch changes: first the new epochs, in order,
    /// then the removed epochs, in order.
    async fn collect_epoch_changes(&self) -> Result<Vec<Operation>, Error> {
        let (mut min_epoch, mut next_epoch) = {
            let (epoch, committees) = self.epoch_and_committees().await?;
            let min_epoch = *committees.keys().next().unwrap_or(&Epoch::ZERO);
            (min_epoch, epoch.try_add_one()?)
        };
        let mut epoch_change_ops = Vec::new();
        while self
            .has_admin_event(EPOCH_STREAM_NAME, next_epoch.0)
            .await?
        {
            epoch_change_ops.push(Operation::system(SystemOperation::ProcessNewEpoch(
                next_epoch,
            )));
            next_epoch.try_add_assign_one()?;
        }
        while self
            .has_admin_event(REMOVED_EPOCH_STREAM_NAME, min_epoch.0)
            .await?
        {
            epoch_change_ops.push(Operation::system(SystemOperation::ProcessRemovedEpoch(
                min_epoch,
            )));
            min_epoch.try_add_assign_one()?;
        }
        Ok(epoch_change_ops)
    }

    /// Returns whether the system event on the admin chain with the given stream name and key
    /// exists in storage.
    async fn has_admin_event(&self, stream_name: &[u8], index: u32) -> Result<bool, Error> {
        let event_id = EventId {
            chain_id: self.client.admin_chain_id,
            stream_id: StreamId::system(stream_name),
            index,
        };
        Ok(self
            .client
            .storage_client()
            .read_event(event_id)
            .await?
            .is_some())
    }

    /// Returns the indices and events from the storage
    pub async fn events_from_index(
        &self,
        stream_id: StreamId,
        start_index: u32,
    ) -> Result<Vec<IndexAndEvent>, Error> {
        Ok(self
            .client
            .storage_client()
            .read_events_from_index(&self.chain_id, &stream_id, start_index)
            .await?)
    }

    /// Deprecates all the configurations of voting rights up to the given one (admin chains
    /// only). Currently, each individual chain is still entitled to wait before accepting
    /// this command. However, it is expected that deprecated validators stop functioning
    /// shortly after such command is issued.
    #[instrument(level = "trace")]
    pub async fn revoke_epochs(
        &self,
        revoked_epoch: Epoch,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, Error> {
        self.prepare_chain().await?;
        let (current_epoch, committees) = self.epoch_and_committees().await?;
        ensure!(
            revoked_epoch < current_epoch,
            Error::CannotRevokeCurrentEpoch(current_epoch)
        );
        ensure!(
            committees.contains_key(&revoked_epoch),
            Error::EpochAlreadyRevoked
        );
        let operations = committees
            .keys()
            .filter_map(|epoch| {
                if *epoch <= revoked_epoch {
                    Some(Operation::system(SystemOperation::Admin(
                        AdminOperation::RemoveCommittee { epoch: *epoch },
                    )))
                } else {
                    None
                }
            })
            .collect();
        self.execute_operations(operations, vec![]).await
    }

    /// Sends money to a chain.
    /// Do not check balance. (This may block the client)
    /// Do not confirm the transaction.
    #[instrument(level = "trace")]
    pub async fn transfer_to_account_unsafe_unconfirmed(
        &self,
        owner: AccountOwner,
        amount: Amount,
        recipient: Account,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, Error> {
        self.execute_operation(SystemOperation::Transfer {
            owner,
            recipient,
            amount,
        })
        .await
    }

    #[instrument(level = "trace", skip(hash))]
    pub async fn read_confirmed_block(&self, hash: CryptoHash) -> Result<ConfirmedBlock, Error> {
        let block = self
            .client
            .storage_client()
            .read_confirmed_block(hash)
            .await?;
        block.ok_or(Error::MissingConfirmedBlock(hash))
    }

    #[instrument(level = "trace", skip(hash))]
    pub async fn read_certificate(
        &self,
        hash: CryptoHash,
    ) -> Result<ConfirmedBlockCertificate, Error> {
        let certificate = self.client.storage_client().read_certificate(hash).await?;
        certificate.ok_or(Error::ReadCertificatesError(vec![hash]))
    }

    /// Handles any cross-chain requests for any pending outgoing messages.
    #[instrument(level = "trace")]
    pub async fn retry_pending_outgoing_messages(&self) -> Result<(), Error> {
        self.client
            .local_node
            .retry_pending_cross_chain_requests(self.chain_id)
            .await?;
        Ok(())
    }

    #[instrument(level = "trace", skip(local_node))]
    async fn local_chain_info(
        &self,
        chain_id: ChainId,
        local_node: &mut LocalNodeClient<Env::Storage>,
    ) -> Result<Option<Box<ChainInfo>>, Error> {
        match local_node.chain_info(chain_id).await {
            Ok(info) => {
                // Useful in case `chain_id` is the same as a local chain.
                self.client.update_from_info(&info);
                Ok(Some(info))
            }
            Err(LocalNodeError::BlobsNotFound(_) | LocalNodeError::InactiveChain(_)) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    #[instrument(level = "trace", skip(chain_id, local_node))]
    async fn local_next_block_height(
        &self,
        chain_id: ChainId,
        local_node: &mut LocalNodeClient<Env::Storage>,
    ) -> Result<BlockHeight, Error> {
        Ok(self
            .local_chain_info(chain_id, local_node)
            .await?
            .map_or(BlockHeight::ZERO, |info| info.next_block_height))
    }

    /// Returns the next height we expect to receive from the given sender chain, according to the
    /// local inbox.
    #[instrument(level = "trace")]
    async fn local_next_height_to_receive(&self, origin: ChainId) -> Result<BlockHeight, Error> {
        Ok(self
            .client
            .local_node
            .get_inbox_next_height(self.chain_id, origin)
            .await?)
    }

    #[instrument(level = "trace", skip(remote_node, local_node, notification))]
    async fn process_notification(
        &self,
        remote_node: RemoteNode<Env::ValidatorNode>,
        mut local_node: LocalNodeClient<Env::Storage>,
        notification: Notification,
    ) -> Result<(), Error> {
        let dominated = self
            .listening_mode()
            .is_none_or(|mode| !mode.is_relevant(&notification.reason));
        if dominated {
            debug!(
                chain_id = %self.chain_id,
                reason = ?notification.reason,
                "Ignoring notification due to listening mode"
            );
            return Ok(());
        }
        match notification.reason {
            Reason::NewIncomingBundle { origin, height } => {
                if self.local_next_height_to_receive(origin).await? > height {
                    debug!(
                        chain_id = %self.chain_id,
                        "Accepting redundant notification for new message"
                    );
                    return Ok(());
                }
                self.client
                    .download_sender_block_with_sending_ancestors(
                        self.chain_id,
                        origin,
                        height,
                        &remote_node,
                    )
                    .await?;
                if self.local_next_height_to_receive(origin).await? <= height {
                    info!(
                        chain_id = %self.chain_id,
                        "NewIncomingBundle: Fail to synchronize new message after notification"
                    );
                }
            }
            Reason::NewBlock { height, .. } => {
                let chain_id = notification.chain_id;
                if self
                    .local_next_block_height(chain_id, &mut local_node)
                    .await?
                    > height
                {
                    debug!(
                        chain_id = %self.chain_id,
                        "Accepting redundant notification for new block"
                    );
                    return Ok(());
                }
                self.client
                    .synchronize_chain_state_from(&remote_node, chain_id)
                    .await?;
                if self
                    .local_next_block_height(chain_id, &mut local_node)
                    .await?
                    <= height
                {
                    info!("NewBlock: Fail to synchronize new block after notification");
                }
                trace!(
                    chain_id = %self.chain_id,
                    %height,
                    "NewBlock: processed notification",
                );
            }
            Reason::NewEvents { height, hash, .. } => {
                if self
                    .local_next_block_height(notification.chain_id, &mut local_node)
                    .await?
                    > height
                {
                    debug!(
                        chain_id = %self.chain_id,
                        "Accepting redundant notification for new block"
                    );
                    return Ok(());
                }
                trace!(
                    chain_id = %self.chain_id,
                    %height,
                    "NewEvents: processing notification"
                );
                let mut certificates = remote_node.node.download_certificates(vec![hash]).await?;
                // download_certificates ensures that we will get exactly one
                // certificate in the result.
                let certificate = certificates
                    .pop()
                    .expect("download_certificates should have returned one certificate");
                self.client
                    .receive_sender_certificate(
                        certificate,
                        ReceiveCertificateMode::NeedsCheck,
                        None,
                    )
                    .await?;
            }
            Reason::NewRound { height, round } => {
                let chain_id = notification.chain_id;
                if let Some(info) = self.local_chain_info(chain_id, &mut local_node).await? {
                    if (info.next_block_height, info.manager.current_round) >= (height, round) {
                        debug!(
                            chain_id = %self.chain_id,
                            "Accepting redundant notification for new round"
                        );
                        return Ok(());
                    }
                }
                self.client
                    .synchronize_chain_state_from(&remote_node, chain_id)
                    .await?;
                let Some(info) = self.local_chain_info(chain_id, &mut local_node).await? else {
                    error!(
                        chain_id = %self.chain_id,
                        "NewRound: Fail to read local chain info for {chain_id}"
                    );
                    return Ok(());
                };
                if (info.next_block_height, info.manager.current_round) < (height, round) {
                    error!(
                        chain_id = %self.chain_id,
                        "NewRound: Fail to synchronize new block after notification"
                    );
                }
            }
            Reason::BlockExecuted { .. } => {
                // No action needed.
            }
        }
        Ok(())
    }

    /// Returns whether this chain is tracked by the client, i.e. we are updating its inbox.
    pub fn is_tracked(&self) -> bool {
        self.client.is_tracked(self.chain_id)
    }

    /// Returns the listening mode for this chain, if it is tracked.
    pub fn listening_mode(&self) -> Option<ListeningMode> {
        self.client.chain_mode(self.chain_id)
    }

    /// Spawns a task that listens to notifications about the current chain from all validators,
    /// and synchronizes the local state accordingly.
    ///
    /// The listening mode must be set in `Client::chain_modes` before calling this method.
    #[instrument(level = "trace", fields(chain_id = ?self.chain_id))]
    pub async fn listen(
        &self,
    ) -> Result<(impl Future<Output = ()>, AbortOnDrop, NotificationStream), Error> {
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
        let notifications = self.subscribe()?;
        let (abortable_notifications, abort) = stream::abortable(self.subscribe()?);

        // Beware: if this future ceases to make progress, notification processing will
        // deadlock, because of the issue described in
        // https://github.com/linera-io/linera-protocol/pull/1173.

        // TODO(#2013): replace this lock with an asynchronous communication channel

        let mut process_notifications = FuturesUnordered::new();

        match self.update_notification_streams(&mut senders).await {
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
                    match Box::pin(await_while_polling(
                        this.update_notification_streams(&mut senders).fuse(),
                        &mut process_notifications,
                    ))
                    .await
                    {
                        Ok(handler) => process_notifications.push(handler),
                        Err(error) => error!("Failed to update committee: {error}"),
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

    #[instrument(level = "trace", skip(senders))]
    async fn update_notification_streams(
        &self,
        senders: &mut HashMap<ValidatorPublicKey, AbortHandle>,
    ) -> Result<impl Future<Output = ()>, Error> {
        let (nodes, local_node) = {
            let committee = self.local_committee().await?;
            let nodes: HashMap<_, _> = self
                .client
                .validator_node_provider()
                .make_nodes(&committee)?
                .collect();
            (nodes, self.client.local_node.clone())
        };
        // Drop removed validators.
        senders.retain(|validator, abort| {
            if !nodes.contains_key(validator) {
                abort.abort();
            }
            !abort.is_aborted()
        });
        // Add tasks for new validators.
        let validator_tasks = FuturesUnordered::new();
        for (public_key, node) in nodes {
            let hash_map::Entry::Vacant(entry) = senders.entry(public_key) else {
                continue;
            };
            let address = node.address();
            let this = self.clone();
            let stream = stream::once({
                let node = node.clone();
                async move {
                    let stream = node.subscribe(vec![this.chain_id]).await?;
                    // Only now the notification stream is established. We may have missed
                    // notifications since the last time we synchronized.
                    let remote_node = RemoteNode { public_key, node };
                    this.client
                        .synchronize_chain_state_from(&remote_node, this.chain_id)
                        .await?;
                    Ok::<_, Error>(stream)
                }
            })
            .filter_map(move |result| {
                let address = address.clone();
                async move {
                    if let Err(error) = &result {
                        info!(?error, address, "could not connect to validator");
                    } else {
                        debug!(address, "connected to validator");
                    }
                    result.ok()
                }
            })
            .flatten();
            let (stream, abort) = stream::abortable(stream);
            let mut stream = Box::pin(stream);
            let this = self.clone();
            let local_node = local_node.clone();
            let remote_node = RemoteNode { public_key, node };
            validator_tasks.push(async move {
                while let Some(notification) = stream.next().await {
                    if let Err(error) = this
                        .process_notification(
                            remote_node.clone(),
                            local_node.clone(),
                            notification.clone(),
                        )
                        .await
                    {
                        tracing::info!(
                            chain_id = %this.chain_id,
                            address = remote_node.address(),
                            ?notification,
                            %error,
                            "failed to process notification",
                        );
                    }
                }
            });
            entry.insert(abort);
        }
        Ok(validator_tasks.collect())
    }

    /// Attempts to update a validator with the local information.
    #[instrument(level = "trace", skip(remote_node))]
    pub async fn sync_validator(&self, remote_node: Env::ValidatorNode) -> Result<(), Error> {
        let validator_next_block_height = match remote_node
            .handle_chain_info_query(ChainInfoQuery::new(self.chain_id))
            .await
        {
            Ok(info) => info.info.next_block_height,
            Err(NodeError::BlobsNotFound(_)) => BlockHeight::ZERO,
            Err(err) => return Err(err.into()),
        };
        let local_next_block_height = self.chain_info().await?.next_block_height;

        if validator_next_block_height >= local_next_block_height {
            debug!("Validator is up-to-date with local state");
            return Ok(());
        }

        let heights: Vec<_> = (validator_next_block_height.0..local_next_block_height.0)
            .map(BlockHeight)
            .collect();

        let certificates = self
            .client
            .storage_client()
            .read_certificates_by_heights(self.chain_id, &heights)
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        for certificate in certificates {
            match remote_node
                .handle_confirmed_certificate(
                    certificate.clone(),
                    CrossChainMessageDelivery::NonBlocking,
                )
                .await
            {
                Ok(_) => (),
                Err(NodeError::BlobsNotFound(missing_blob_ids)) => {
                    // Upload the missing blobs we have and retry.
                    let missing_blobs: Vec<_> = self
                        .client
                        .storage_client()
                        .read_blobs(&missing_blob_ids)
                        .await?
                        .into_iter()
                        .flatten()
                        .collect();
                    remote_node.upload_blobs(missing_blobs).await?;
                    remote_node
                        .handle_confirmed_certificate(
                            certificate,
                            CrossChainMessageDelivery::NonBlocking,
                        )
                        .await?;
                }
                Err(err) => return Err(err.into()),
            }
        }

        Ok(())
    }
}

#[cfg(with_testing)]
impl<Env: Environment> ChainClient<Env> {
    pub async fn process_notification_from(
        &self,
        notification: Notification,
        validator: (ValidatorPublicKey, &str),
    ) {
        let mut node_list = self
            .client
            .validator_node_provider()
            .make_nodes_from_list(vec![validator])
            .unwrap();
        let (public_key, node) = node_list.next().unwrap();
        let remote_node = RemoteNode { node, public_key };
        let local_node = self.client.local_node.clone();
        self.process_notification(remote_node, local_node, notification)
            .await
            .unwrap();
    }
}

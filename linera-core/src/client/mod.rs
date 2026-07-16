// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashSet},
    slice,
    sync::{Arc, RwLock},
};

use custom_debug_derive::Debug;
use futures::{
    future::{Future, TryFutureExt as _},
    stream::{self, AbortHandle, FuturesOrdered, FuturesUnordered, StreamExt},
};
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use linera_base::{
    crypto::{CryptoHash, Signer as _, ValidatorPublicKey},
    data_types::{
        ApplicationDescription, ArithmeticError, Blob, BlockHeight, ChainDescription, Epoch, Round,
        TimeDelta, Timestamp,
    },
    ensure,
    hashed::Hashed,
    identifiers::{AccountOwner, ApplicationId, BlobId, BlobType, ChainId, EventId, StreamId},
    time::Duration,
};
#[cfg(not(target_arch = "wasm32"))]
use linera_base::{data_types::Bytecode, identifiers::ModuleId, vm::VmRuntime};
use linera_chain::{
    data_types::{
        BlockExecutionOutcome, BlockProposal, BundleExecutionPolicy, ChainAndHeight, LiteVote,
        OriginalProposal, ProposedBlock,
    },
    justification::JustificationChain,
    manager::LockingBlock,
    types::{
        Block, CertificateValue, Certified, ConfirmedBlock, ConfirmedBlockCertificate,
        GenericCertificate, LiteCertificate, Timeout, ValidatedBlock, ValidatedBlockCertificate,
    },
    ChainError, ChainIdSet,
};
use linera_execution::committee::Committee;
use linera_storage::{Arc as CacheArc, Clock as _, ResultReadCertificates, Storage as _};
use rand::seq::SliceRandom;
use received_log::ReceivedLogs;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument, trace, warn};

use crate::{
    data_types::{ChainInfo, ChainInfoQuery, ChainInfoResponse},
    environment::Environment,
    local_node::{LocalNodeClient, LocalNodeError},
    node::{CrossChainMessageDelivery, NodeError, ValidatorNode as _, ValidatorNodeProvider as _},
    notifier::{ChannelNotifier, Notifier as _},
    remote_node::RemoteNode,
    updater::{communicate_with_quorum, CommunicateAction, ValidatorUpdater},
    worker::{Notification, ProcessableCertificate, Reason, WorkerError, WorkerState},
    ChainWorkerConfig, ProcessConfirmedBlockMode, CHAIN_INFO_MAX_RECEIVED_LOG_ENTRIES,
};

/// The client for interacting with a single chain.
pub mod chain_client;
pub use chain_client::ChainClient;

pub use crate::data_types::ClientOutcome;

#[cfg(test)]
#[path = "../unit_tests/client_tests.rs"]
mod client_tests;
pub mod requests_scheduler;

pub use requests_scheduler::{RequestsScheduler, RequestsSchedulerConfig, ScoringWeights};
mod received_log;
mod validator_trackers;

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{
        exponential_bucket_latencies, register_histogram_vec, register_int_counter_vec,
    };
    use prometheus::{HistogramVec, IntCounterVec};

    pub static PROCESS_INBOX_WITHOUT_PREPARE_LATENCY: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "process_inbox_latency",
                "process_inbox latency",
                &[],
                exponential_bucket_latencies(10_000.0),
            )
        });

    pub static PREPARE_CHAIN_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "prepare_chain_latency",
            "prepare_chain latency",
            &[],
            exponential_bucket_latencies(10_000.0),
        )
    });

    pub static SYNCHRONIZE_CHAIN_STATE_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "synchronize_chain_state_latency",
            "synchronize_chain_state latency",
            &[],
            exponential_bucket_latencies(10_000.0),
        )
    });

    pub static EXECUTE_BLOCK_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "execute_block_latency",
            "execute_block latency",
            &[],
            exponential_bucket_latencies(10_000.0),
        )
    });

    pub static FIND_RECEIVED_CERTIFICATES_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "find_received_certificates_latency",
            "find_received_certificates latency",
            &[],
            exponential_bucket_latencies(10_000.0),
        )
    });

    pub static BLOCK_STAGING_FAILURES_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "block_staging_failures_total",
            "Total number of client block staging (execute_block) failures, labelled by error type",
            &["error_type"],
        )
    });
}

/// Default number of certificates to download in a single batch.
pub static DEFAULT_CERTIFICATE_DOWNLOAD_BATCH_SIZE: u64 = 500;
/// Default number of certificates to upload in a single batch.
pub static DEFAULT_CERTIFICATE_UPLOAD_BATCH_SIZE: usize = 500;
/// Default number of sender-chain certificates to download in a single batch.
pub static DEFAULT_SENDER_CERTIFICATE_DOWNLOAD_BATCH_SIZE: usize = 20_000;
/// Default maximum number of concurrent event stream queries.
pub static DEFAULT_MAX_EVENT_STREAM_QUERIES: usize = 1000;
/// Default maximum number of certificate batch downloads to run concurrently.
pub static DEFAULT_MAX_CONCURRENT_BATCH_DOWNLOADS: usize = 1;

/// Identifies which operation a timing measurement refers to.
#[derive(Debug, Clone, Copy)]
#[allow(missing_docs)]
pub enum TimingType {
    ExecuteOperations,
    ExecuteBlock,
    SubmitBlockProposal,
    UpdateValidators,
}

/// Defines what type of notifications we should process for a chain:
/// - do we fully participate in consensus and download sender chains?
/// - or do we only follow the chain's blocks without participating?
/// - or do we only care about blocks containing events from some particular streams?
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListeningMode {
    /// Listen to everything: all blocks for the chain and all blocks from sender chains,
    /// and participate in rounds.
    FullChain,
    /// Listen to all blocks for the chain, but don't download sender chain blocks or participate
    /// in rounds. Use this when interested in the chain's state but not intending to propose
    /// blocks (e.g., because we're not a chain owner).
    FollowChain,
    /// Only listen to blocks which contain events from those streams.
    EventsOnly(BTreeSet<StreamId>),
}

impl PartialOrd for ListeningMode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (ListeningMode::FullChain, ListeningMode::FullChain) => Some(Ordering::Equal),
            (ListeningMode::FullChain, _) => Some(Ordering::Greater),
            (_, ListeningMode::FullChain) => Some(Ordering::Less),
            (ListeningMode::FollowChain, ListeningMode::FollowChain) => Some(Ordering::Equal),
            (ListeningMode::FollowChain, ListeningMode::EventsOnly(_)) => Some(Ordering::Greater),
            (ListeningMode::EventsOnly(_), ListeningMode::FollowChain) => Some(Ordering::Less),
            (ListeningMode::EventsOnly(a), ListeningMode::EventsOnly(b)) => {
                if a == b {
                    Some(Ordering::Equal)
                } else if a.is_superset(b) {
                    Some(Ordering::Greater)
                } else if b.is_superset(a) {
                    Some(Ordering::Less)
                } else {
                    None
                }
            }
        }
    }
}

impl ListeningMode {
    /// Returns whether a notification with this reason should be processed under this listening
    /// mode.
    pub fn is_relevant(&self, reason: &Reason) -> bool {
        match (reason, self) {
            // NewEvents is only processed in EventsOnly mode, the other modes depend on
            // the NewBlock notification.
            (Reason::NewEvents { .. }, ListeningMode::FollowChain | ListeningMode::FullChain) => {
                false
            }
            // FullChain processes everything.
            (_, ListeningMode::FullChain) => true,
            // FollowChain processes new blocks on the chain itself, including blocks that
            // produced events.
            (Reason::NewBlock { .. }, ListeningMode::FollowChain) => true,
            (_, ListeningMode::FollowChain) => false,
            // EventsOnly only processes events from relevant streams.
            (Reason::NewEvents { event_streams, .. }, ListeningMode::EventsOnly(relevant)) => {
                relevant.intersection(event_streams).next().is_some()
            }
            (_, ListeningMode::EventsOnly(_)) => false,
        }
    }

    /// Widens this mode to also cover the given mode, keeping the more inclusive of the two.
    pub fn extend(&mut self, other: Option<ListeningMode>) {
        match (self, other) {
            (_, None) => (),
            (ListeningMode::FullChain, _) => (),
            (mode, Some(ListeningMode::FullChain)) => {
                *mode = ListeningMode::FullChain;
            }
            (ListeningMode::FollowChain, _) => (),
            (mode, Some(ListeningMode::FollowChain)) => {
                *mode = ListeningMode::FollowChain;
            }
            (
                ListeningMode::EventsOnly(self_events),
                Some(ListeningMode::EventsOnly(other_events)),
            ) => {
                self_events.extend(other_events);
            }
        }
    }

    /// Returns whether this mode implies follow-only behavior (i.e., not participating in
    /// consensus rounds).
    pub fn is_follow_only(&self) -> bool {
        !matches!(self, ListeningMode::FullChain)
    }

    /// Returns whether this is a full chain mode (synchronizing sender chains and updating
    /// inboxes).
    pub fn is_full(&self) -> bool {
        matches!(self, ListeningMode::FullChain)
    }

    /// Returns whether this mode requires synchronizing the chain's own state.
    pub fn should_sync_chain_state(&self) -> bool {
        match self {
            ListeningMode::FullChain | ListeningMode::FollowChain => true,
            ListeningMode::EventsOnly(_) => false,
        }
    }
}

/// The per-chain [`ListeningMode`]s tracked by a local node, together with a memoized
/// [`Hashed`] of the fully-tracked subset.
///
/// The hash is the version of the outbox index (see
/// [`ChainStateView::outbox_index_tracked_hash`]). Caching it here — recomputed only when a chain
/// newly becomes `FullChain`, which is rare and client-side — lets every cross-chain operation
/// compare and reconcile the index in `O(1)` instead of rehashing the tracked set each time.
///
/// [`ChainStateView::outbox_index_tracked_hash`]: linera_chain::ChainStateView
#[derive(Debug)]
pub struct ChainModes {
    modes: BTreeMap<ChainId, ListeningMode>,
    full: Arc<Hashed<ChainIdSet>>,
}

impl Default for ChainModes {
    fn default() -> Self {
        Self::new(BTreeMap::new())
    }
}

impl ChainModes {
    /// Builds the listening modes from `modes`, computing the tracked-set hash once.
    pub fn new(modes: BTreeMap<ChainId, ListeningMode>) -> Self {
        let full = Self::compute_full(&modes);
        Self { modes, full }
    }

    fn compute_full(modes: &BTreeMap<ChainId, ListeningMode>) -> Arc<Hashed<ChainIdSet>> {
        Arc::new(Hashed::new(ChainIdSet(
            modes
                .iter()
                .filter(|(_, mode)| mode.is_full())
                .map(|(id, _)| *id)
                .collect(),
        )))
    }

    /// The fully-tracked chains together with their memoized hash (`O(1)` clone).
    pub fn full(&self) -> Arc<Hashed<ChainIdSet>> {
        self.full.clone()
    }

    /// Returns the listening mode for `chain_id`, if it is tracked.
    pub fn get(&self, chain_id: &ChainId) -> Option<&ListeningMode> {
        self.modes.get(chain_id)
    }

    /// Merges `mode` into the entry for `chain_id` — monotonic in the listening-mode order, so it
    /// never weakens an existing entry — and returns the resulting mode. The tracked-set hash is
    /// recomputed only if the chain newly became `FullChain`.
    pub fn extend_mode(&mut self, chain_id: ChainId, mode: ListeningMode) -> ListeningMode {
        let entry = self
            .modes
            .entry(chain_id)
            .or_insert_with(|| ListeningMode::EventsOnly(BTreeSet::new()));
        let was_full = entry.is_full();
        entry.extend(Some(mode));
        let result = entry.clone();
        if !was_full && result.is_full() {
            self.full = Self::compute_full(&self.modes);
        }
        result
    }

    /// Stops tracking `chain_id`, removing its entry entirely. Returns the removed mode, if any.
    /// The tracked-set hash is recomputed only if the removed chain was `FullChain`.
    pub fn remove_mode(&mut self, chain_id: &ChainId) -> Option<ListeningMode> {
        let removed = self.modes.remove(chain_id)?;
        if removed.is_full() {
            self.full = Self::compute_full(&self.modes);
        }
        Some(removed)
    }
}

/// A builder that creates [`ChainClient`]s which share the cache and notifiers.
pub struct Client<Env: Environment> {
    environment: Env,
    /// Local node to manage the execution state and the local storage of the chains that we are
    /// tracking.
    pub local_node: LocalNodeClient<Env::Storage>,
    /// Manages the requests sent to validator nodes.
    requests_scheduler: Arc<RequestsScheduler<Env>>,
    /// The admin chain ID.
    admin_chain_id: ChainId,
    /// Chains that should be tracked by the client, along with their listening mode.
    /// The presence of a chain in this map means it is tracked by the local node.
    chain_modes: Arc<RwLock<ChainModes>>,
    /// References to clients waiting for chain notifications.
    notifier: Arc<ChannelNotifier<Notification>>,
    /// Chain state for the managed chains.
    chains: papaya::HashMap<ChainId, chain_client::State>,
    /// Configuration options.
    options: chain_client::Options,
}

/// Boxed future returned by `receive_sender_certificate`. It is `Send` off the `web`
/// target (where futures must be `Send`) and `?Send` on `web` (single-threaded, where
/// the validator node is not `Sync`).
#[cfg(not(web))]
type ReceiveSenderCertificateFuture<'a> =
    std::pin::Pin<Box<dyn Future<Output = Result<(), chain_client::Error>> + Send + 'a>>;
#[cfg(web)]
type ReceiveSenderCertificateFuture<'a> =
    std::pin::Pin<Box<dyn Future<Output = Result<(), chain_client::Error>> + 'a>>;

impl<Env: Environment> Client<Env> {
    /// Creates a new `Client` with a new cache and notifiers.
    #[instrument(level = "trace", skip_all)]
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        environment: Env,
        admin_chain_id: ChainId,
        long_lived_services: bool,
        chain_modes: impl IntoIterator<Item = (ChainId, ListeningMode)>,
        name: impl Into<String>,
        chain_worker_ttl: Option<Duration>,
        sender_chain_worker_ttl: Option<Duration>,
        cross_chain_batch_size_limit: usize,
        options: chain_client::Options,
        block_cache_size: usize,
        execution_state_cache_size: usize,
        requests_scheduler_config: &requests_scheduler::RequestsSchedulerConfig,
    ) -> Self {
        let mut modes = chain_modes.into_iter().collect::<BTreeMap<_, _>>();
        // The client needs the admin chain fully synced for epoch tracking, so
        // promote it (or insert) into `FullChain`. `extend` is monotonic in the
        // listening mode order, so this never weakens an existing entry.
        modes
            .entry(admin_chain_id)
            .or_insert(ListeningMode::FullChain)
            .extend(Some(ListeningMode::FullChain));
        let chain_modes = Arc::new(RwLock::new(ChainModes::new(modes)));
        let config = ChainWorkerConfig {
            nickname: name.into(),
            long_lived_services,
            allow_inactive_chains: true,
            ttl: chain_worker_ttl,
            sender_chain_ttl: sender_chain_worker_ttl,
            block_cache_size,
            execution_state_cache_size,
            cross_chain_batch_size_limit,
            ..ChainWorkerConfig::default()
        };
        let state = WorkerState::new(
            environment.storage().clone(),
            config,
            Some(chain_modes.clone()),
        );
        let clock = environment.storage().clock().clone();
        let local_node = LocalNodeClient::new(state);
        let requests_scheduler = Arc::new(RequestsScheduler::new(
            vec![],
            requests_scheduler_config,
            clock,
        ));

        Self {
            environment,
            local_node,
            requests_scheduler,
            chains: papaya::HashMap::new(),
            admin_chain_id,
            chain_modes,
            notifier: Arc::new(ChannelNotifier::default()),
            options,
        }
    }

    /// Returns the chain ID of the admin chain.
    pub fn admin_chain_id(&self) -> ChainId {
        self.admin_chain_id
    }

    /// Subscribes to notifications for the given chain IDs.
    pub fn subscribe(
        &self,
        chain_ids: Vec<ChainId>,
    ) -> tokio::sync::mpsc::UnboundedReceiver<Notification> {
        self.notifier.subscribe(chain_ids)
    }

    /// Adds additional chain IDs to an existing subscription.
    pub fn subscribe_extra(
        &self,
        chain_ids: Vec<ChainId>,
        sender: &tokio::sync::mpsc::UnboundedSender<Notification>,
    ) {
        self.notifier.add_sender(chain_ids, sender);
    }

    /// Returns the storage client used by this client's local node.
    pub fn storage_client(&self) -> &Env::Storage {
        self.environment.storage()
    }

    /// Tries to read a certificate from local storage, using the hash if available
    /// (fast path) or falling back to a height-based lookup.
    async fn try_read_local_certificate(
        &self,
        chain_id: ChainId,
        height: BlockHeight,
        hash: Option<CryptoHash>,
    ) -> Result<Option<CacheArc<ConfirmedBlockCertificate>>, chain_client::Error> {
        if let Some(hash) = hash {
            return Ok(self.storage_client().read_certificate(hash).await?);
        }
        let results = self
            .storage_client()
            .read_certificates_by_heights(chain_id, &[height])
            .await?;
        Ok(results.into_iter().next().flatten())
    }

    /// Returns the provider used to connect to validator nodes.
    pub fn validator_node_provider(&self) -> &Env::Network {
        self.environment.network()
    }

    pub(crate) fn options(&self) -> &chain_client::Options {
        &self.options
    }

    /// Handles any pending local cross-chain requests, notifying subscribers.
    pub async fn retry_pending_cross_chain_requests(
        &self,
        sender_chain: ChainId,
    ) -> Result<(), LocalNodeError> {
        self.local_node
            .retry_pending_cross_chain_requests(sender_chain, &self.notifier)
            .await
    }

    /// Returns a reference to the client's [`Signer`][crate::environment::Signer].
    #[instrument(level = "trace", skip(self))]
    pub fn signer(&self) -> &Env::Signer {
        self.environment.signer()
    }

    /// Returns whether the signer has a key for the given owner.
    pub async fn has_key_for(&self, owner: &AccountOwner) -> Result<bool, chain_client::Error> {
        self.signer()
            .contains_key(owner)
            .await
            .map_err(chain_client::Error::signer_failure)
    }

    /// Returns a reference to the client's [`Wallet`][crate::environment::Wallet].
    pub fn wallet(&self) -> &Env::Wallet {
        self.environment.wallet()
    }

    /// Extends the listening mode for a chain, combining with the existing mode if present.
    /// Returns the resulting mode.
    #[instrument(level = "trace", skip(self))]
    pub fn extend_chain_mode(&self, chain_id: ChainId, mode: ListeningMode) -> ListeningMode {
        self.chain_modes
            .write()
            .expect("Panics should not happen while holding a lock to `chain_modes`")
            .extend_mode(chain_id, mode)
    }

    /// Stops tracking a chain, removing its listening mode. Returns the removed mode, if any.
    #[instrument(level = "trace", skip(self))]
    pub fn remove_chain_mode(&self, chain_id: ChainId) -> Option<ListeningMode> {
        self.chain_modes
            .write()
            .expect("Panics should not happen while holding a lock to `chain_modes`")
            .remove_mode(&chain_id)
    }

    /// Returns the listening mode for a chain, if it is tracked.
    pub fn chain_mode(&self, chain_id: ChainId) -> Option<ListeningMode> {
        self.chain_modes
            .read()
            .expect("Panics should not happen while holding a lock to `chain_modes`")
            .get(&chain_id)
            .cloned()
    }

    /// Returns whether a chain is fully tracked by the local node.
    pub fn is_tracked(&self, chain_id: ChainId) -> bool {
        self.chain_modes
            .read()
            .expect("Panics should not happen while holding a lock to `chain_modes`")
            .get(&chain_id)
            .is_some_and(ListeningMode::is_full)
    }

    /// Creates a new `ChainClient`.
    #[expect(clippy::too_many_arguments)]
    #[instrument(level = "trace", skip_all, fields(chain_id, next_block_height))]
    pub fn create_chain_client(
        self: &Arc<Self>,
        chain_id: ChainId,
        block_hash: Option<CryptoHash>,
        next_block_height: BlockHeight,
        pending_proposal: &Option<PendingProposal>,
        preferred_owner: Option<AccountOwner>,
        timing_sender: Option<mpsc::UnboundedSender<(u64, TimingType)>>,
        follow_only: bool,
    ) -> ChainClient<Env> {
        // If the entry already exists we assume that the entry is more up to date than
        // the arguments: If they were read from the wallet file, they might be stale.
        self.chains.pin().get_or_insert_with(chain_id, || {
            chain_client::State::new(pending_proposal.clone(), follow_only)
        });

        ChainClient::new(
            self.clone(),
            chain_id,
            self.options.clone(),
            block_hash,
            next_block_height,
            preferred_owner,
            timing_sender,
        )
    }

    /// Returns whether the given chain is in follow-only mode.
    fn is_chain_follow_only(&self, chain_id: ChainId) -> bool {
        self.chains
            .pin()
            .get(&chain_id)
            .is_some_and(|state| state.is_follow_only())
    }

    /// Sets whether the given chain is in follow-only mode.
    pub fn set_chain_follow_only(&self, chain_id: ChainId, follow_only: bool) {
        self.chains
            .pin()
            .update(chain_id, |state| state.with_follow_only(follow_only));
    }

    /// Fetches the chain description blob if needed, and returns the chain info.
    async fn fetch_chain_info(
        &self,
        chain_id: ChainId,
        validators: &[RemoteNode<Env::ValidatorNode>],
    ) -> Result<Box<ChainInfo>, chain_client::Error> {
        match self.local_node.chain_info(chain_id).await {
            Ok(info) => Ok(info),
            Err(LocalNodeError::BlobsNotFound(blob_ids)) => {
                // Make sure the admin chain is up to date so we can validate the
                // certificate that creates this chain's description blob.
                self.synchronize_chain_state(self.admin_chain_id).await?;
                self.update_local_node_with_blobs_from(blob_ids, validators)
                    .await?;
                Ok(self.local_node.chain_info(chain_id).await?)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Downloads and processes all certificates up to (excluding) the specified height.
    #[instrument(level = "trace", skip(self))]
    async fn download_certificates(
        &self,
        chain_id: ChainId,
        target_next_block_height: BlockHeight,
    ) -> Result<Box<ChainInfo>, chain_client::Error> {
        let validators = self.validator_nodes().await?;
        let mut info = Box::pin(self.fetch_chain_info(chain_id, &validators)).await?;
        if target_next_block_height <= info.next_block_height {
            return Ok(info);
        }
        info = self
            .load_local_certificates(chain_id, target_next_block_height, None)
            .await?;
        let mut next_height = info.next_block_height;
        // Download remaining batches using all validators with staggered fallback.
        while next_height < target_next_block_height {
            let limit = u64::from(target_next_block_height)
                .checked_sub(u64::from(next_height))
                .ok_or(ArithmeticError::Overflow)?
                .min(self.options.certificate_download_batch_size);
            let certificates = self
                .requests_scheduler
                .download_certificates_from_validators(
                    &validators,
                    chain_id,
                    next_height,
                    limit,
                    self.options.certificate_batch_download_hedge_delay,
                )
                .await?;
            let Some(new_info) = self
                .process_certificates(
                    &validators,
                    certificates,
                    None,
                    ProcessConfirmedBlockMode::Execute,
                )
                .await?
            else {
                break;
            };
            assert!(new_info.next_block_height > next_height);
            next_height = new_info.next_block_height;
            info = new_info;
        }
        ensure!(
            target_next_block_height <= info.next_block_height,
            chain_client::Error::CannotDownloadCertificates {
                chain_id,
                target_next_block_height,
            }
        );
        Ok(info)
    }

    /// Loads and processes certificates from local storage for the given chain, from the
    /// current local height up to `end`. Returns the chain info after processing.
    /// If `until_block_time` is `Some`, stops before processing any certificate whose
    /// block timestamp is >= the given value (exclusive).
    async fn load_local_certificates(
        &self,
        chain_id: ChainId,
        end: BlockHeight,
        until_block_time: Option<Timestamp>,
    ) -> Result<Box<ChainInfo>, chain_client::Error> {
        let mut last_info = self.local_node.chain_info(chain_id).await?;
        let next_height = last_info.next_block_height;
        let hashes = self
            .local_node
            .get_preprocessed_block_hashes(chain_id, next_height, end)
            .await?;
        let certificates = self.storage_client().read_certificates(&hashes).await?;
        let certificates = match ResultReadCertificates::new(certificates, hashes) {
            ResultReadCertificates::Certificates(certificates) => certificates,
            ResultReadCertificates::InvalidHashes(hashes) => {
                return Err(chain_client::Error::ReadCertificatesError(hashes))
            }
        };
        for certificate in certificates {
            if let Some(until) = until_block_time {
                if certificate.value().block().header.timestamp >= until {
                    break;
                }
            }
            last_info = self
                .handle_certificate::<ConfirmedBlock>(certificate)
                .await?
                .info;
        }
        Ok(last_info)
    }

    /// Downloads and processes certificates from the given validator.
    ///
    /// Stops when either condition is met:
    /// - `stop`: the local chain has reached that height (exclusive).
    /// - `until_block_time`: the next block's timestamp is >= that value (exclusive).
    #[instrument(level = "trace", skip_all)]
    async fn download_certificates_from(
        &self,
        remote_node: &RemoteNode<Env::ValidatorNode>,
        chain_id: ChainId,
        stop: BlockHeight,
        until_block_time: Option<Timestamp>,
    ) -> Result<Box<ChainInfo>, chain_client::Error> {
        let mut last_info = self
            .load_local_certificates(chain_id, stop, until_block_time)
            .await?;
        let mut next_height = last_info.next_block_height;

        if next_height >= stop {
            return Ok(last_info);
        }

        // Download remaining certificates from the remote node using a pipelined
        // sliding window. A background task downloads up to `max_concurrent_batch_downloads`
        // batches concurrently and sends them through a channel for sequential processing.
        #[cfg(not(web))]
        type CertificateBatchFuture = std::pin::Pin<
            Box<dyn Future<Output = Result<Vec<ConfirmedBlockCertificate>, NodeError>> + Send>,
        >;
        #[cfg(web)]
        type CertificateBatchFuture = std::pin::Pin<
            Box<dyn Future<Output = Result<Vec<ConfirmedBlockCertificate>, NodeError>>>,
        >;

        let max_concurrent = self.options.max_concurrent_batch_downloads;
        let batch_size = self.options.certificate_download_batch_size;
        let (sender, mut receiver) = tokio::sync::mpsc::channel(max_concurrent);
        let scheduler = self.requests_scheduler.clone();
        let remote = remote_node.clone();

        let download_task = linera_base::Task::spawn(async move {
            let mut download_height = next_height;
            let mut in_flight = FuturesOrdered::<CertificateBatchFuture>::new();

            let try_enqueue = |in_flight: &mut FuturesOrdered<CertificateBatchFuture>,
                               download_height: &mut BlockHeight| {
                if *download_height >= stop {
                    return;
                }
                let limit = u64::from(stop)
                    .saturating_sub(u64::from(*download_height))
                    .min(batch_size);
                let height = *download_height;
                let scheduler = scheduler.clone();
                let remote = remote.clone();
                in_flight.push_back(Box::pin(async move {
                    scheduler
                        .download_certificates(&remote, chain_id, height, limit)
                        .await
                }));
                *download_height = BlockHeight(u64::from(*download_height) + limit);
            };

            while in_flight.len() < max_concurrent && download_height < stop {
                try_enqueue(&mut in_flight, &mut download_height);
            }

            while let Some(result) = in_flight.next().await {
                if sender.send(result).await.is_err() {
                    break;
                }
                try_enqueue(&mut in_flight, &mut download_height);
            }
        });

        // Process downloaded batches sequentially.
        while let Some(result) = receiver.recv().await {
            let certificates = result?;
            let Some(info) = self
                .process_certificates(
                    slice::from_ref(remote_node),
                    certificates,
                    until_block_time,
                    ProcessConfirmedBlockMode::Execute,
                )
                .await?
            else {
                break;
            };
            assert!(info.next_block_height >= next_height);
            next_height = info.next_block_height;
            last_info = info;
        }
        // Await the downloader so any panic inside the spawned task surfaces here
        // instead of being silently swallowed when the channel closes.
        download_task.await;
        Ok(last_info)
    }

    async fn download_blobs(
        &self,
        remote_nodes: &[RemoteNode<Env::ValidatorNode>],
        blob_ids: &[BlobId],
    ) -> Result<(), chain_client::Error> {
        let blobs = &self
            .requests_scheduler
            .download_blobs(
                remote_nodes,
                blob_ids,
                self.options.blob_download_hedge_delay,
            )
            .await?
            .ok_or_else(|| {
                chain_client::Error::RemoteNodeError(NodeError::BlobsNotFound(blob_ids.to_vec()))
            })?;
        self.local_node.store_blobs(blobs).await.map_err(Into::into)
    }

    /// Downloads the publisher chain certificates that contain the given events,
    /// using the event block height index on validators. Queries a validator for
    /// the block heights, downloads those certificates, and processes them — all
    /// as one atomic unit per validator attempt, with staggered fallback.
    #[instrument(level = "trace", skip_all)]
    pub(crate) async fn download_certificates_for_events(
        &self,
        event_ids: &[EventId],
    ) -> Result<(), chain_client::Error> {
        let mut validators = self.validator_nodes().await?;
        let hedge_delay = self.options.certificate_batch_download_hedge_delay;
        let mut remaining_event_ids = event_ids.to_vec();

        while !remaining_event_ids.is_empty() {
            let remaining_ref = &remaining_event_ids;
            validators.shuffle(&mut rand::thread_rng());
            let result = communicate_concurrently(
                &validators,
                move |remote_node| {
                    let validator_key = remote_node.public_key;
                    let validator_address = remote_node.address();
                    Box::pin(async move {
                        // Query this validator for the block heights.
                        let heights = remote_node
                            .node
                            .event_block_heights(remaining_ref.to_vec())
                            .await?;

                        // Separate resolved and unresolved events.
                        let mut chain_heights = BTreeMap::<_, BTreeSet<_>>::new();
                        let mut expected_events = BTreeMap::<_, HashSet<EventId>>::new();
                        let mut unresolved = Vec::new();
                        for (event_id, maybe_height) in remaining_ref.iter().zip(heights) {
                            if let Some(height) = maybe_height {
                                chain_heights
                                    .entry(event_id.chain_id)
                                    .or_default()
                                    .insert(height);
                                expected_events
                                    .entry((event_id.chain_id, height))
                                    .or_default()
                                    .insert(event_id.clone());
                            } else {
                                unresolved.push(event_id.clone());
                            }
                        }
                        if chain_heights.is_empty() {
                            // This validator has no useful information.
                            return Err(chain_client::Error::from(NodeError::EventsNotFound(remaining_ref.clone())));
                        }

                        // Download certificates and verify them.
                        let mut checked_certificates = Vec::<ConfirmedBlockCertificate>::new();
                        for (chain_id, heights) in chain_heights {
                            let heights_vec = heights.into_iter().collect::<Vec<_>>();
                            let certificates = self
                                .requests_scheduler
                                .download_certificates_by_heights(
                                    &remote_node,
                                    chain_id,
                                    heights_vec,
                                )
                                .await?;
                            for cert in &certificates {
                                // Verify the block contains the expected events.
                                let block = cert.block();
                                let block_event_ids = block.event_ids().collect::<HashSet<_>>();
                                if let Some(expected_event_ids) =
                                    expected_events.get(&(chain_id, block.header.height))
                                {
                                    if !expected_event_ids.is_subset(&block_event_ids) {
                                        tracing::debug!(
                                            %validator_address, ?expected_event_ids, ?block_event_ids,
                                            "validator lied about events in block."
                                        );
                                        return Err(NodeError::UnexpectedCertificateValue.into());
                                    }
                                }
                            }
                            for cert in certificates {
                                self.check_certificate(&cert).await.map_err(|error| {
                                    tracing::debug!(
                                        %validator_address, %error,
                                        "invalid certificate"
                                    );
                                    error
                                })?;
                                checked_certificates.push(cert);
                            }
                        }
                        Ok((checked_certificates, unresolved, validator_key))
                    })
                },
                hedge_delay,
                self.storage_client().clock(),
            )
            .await;

            match result {
                Ok((certificates, unresolved, validator_key)) => {
                    for certificate in certificates {
                        let mode = ReceiveCertificateMode::AlreadyChecked;
                        self.receive_sender_certificate(
                            self.storage_client().cache_certificate(certificate),
                            mode,
                            None,
                        )
                        .await?;
                    }
                    validators.retain(|node| node.public_key != validator_key);
                    remaining_event_ids = unresolved;
                }
                Err(errors) => {
                    for (validator, error) in &errors {
                        warn!(
                            %validator,
                            %error,
                            "failed to download event certificates from validator",
                        );
                    }
                    // All validators failed; no point retrying.
                    return Err(NodeError::EventsNotFound(remaining_event_ids).into());
                }
            }
        }
        Ok(())
    }

    /// Downloads the checkpoint certificate at `checkpoint_height` from `remote_node`
    /// and processes it locally, if our chain isn't already past that height. The
    /// worker's `process_confirmed_block` recognises the gap-plus-checkpoint case and
    /// installs the chain's execution state from the checkpoint blob before re-running
    /// the certificate.
    ///
    /// The certificate's signatures are still verified against the committee resolved
    /// from the admin chain's epoch event stream, so the remote node is trusted only
    /// to point us at a height — not to forge the snapshot itself.
    #[instrument(level = "trace", skip_all)]
    async fn bootstrap_chain_from_checkpoint(
        &self,
        remote_node: &RemoteNode<Env::ValidatorNode>,
        chain_id: ChainId,
        checkpoint_height: BlockHeight,
    ) -> Result<(), chain_client::Error> {
        let local_next = match self.local_node.chain_info(chain_id).await {
            Ok(info) => info.next_block_height,
            // A freshly-created follower whose storage doesn't yet hold this
            // chain's description blob: treat as height 0 and let the
            // checkpoint cert install the snapshot. The chain description is
            // the only blob `chain_info` ever needs.
            Err(LocalNodeError::BlobsNotFound(_)) => BlockHeight::ZERO,
            Err(err) => return Err(err.into()),
        };
        if local_next > checkpoint_height {
            return Ok(());
        }
        let certificates = remote_node
            .download_certificates_by_heights(chain_id, vec![checkpoint_height])
            .await?;
        if certificates.is_empty() {
            // The validator advertised a checkpoint height it can't actually serve;
            // skip and let the regular sync path take over.
            return Ok(());
        }
        // The first attempt at processing the checkpoint cert will fall into the
        // worker's `BlocksNotFound` pre-check if pre-checkpoint sender blocks are
        // missing; `handle_certificate_with_retry` downloads them by hash and
        // retries, so by the time `process_certificates` returns the chain has
        // both its restored state and every certified sender block in storage.
        self.process_certificates(
            slice::from_ref(remote_node),
            certificates,
            None,
            ProcessConfirmedBlockMode::Execute,
        )
        .await?;
        Ok(())
    }

    /// Tries to process all the certificates, requesting any missing blobs from the given nodes.
    /// Returns the chain info of the last successfully processed certificate.
    /// If `until_block_time` is `Some`, stops before processing any certificate whose
    /// block timestamp is greater or equal than the given value.
    #[instrument(level = "trace", skip_all)]
    async fn process_certificates(
        &self,
        remote_nodes: &[RemoteNode<Env::ValidatorNode>],
        certificates: Vec<ConfirmedBlockCertificate>,
        until_block_time: Option<Timestamp>,
        mode: ProcessConfirmedBlockMode,
    ) -> Result<Option<Box<ChainInfo>>, chain_client::Error> {
        let mut info = None;
        // Blobs created by these certs are already embedded in the downloaded
        // block bodies, so they don't need to be fetched from a validator. The
        // chain worker resolves them from `Block::created_blobs()` during
        // `handle_certificate`.
        let created_blob_ids = certificates
            .iter()
            .flat_map(|certificate| certificate.value().block().created_blob_ids())
            .collect::<BTreeSet<BlobId>>();
        let required_blob_ids = certificates
            .iter()
            .flat_map(|certificate| certificate.value().required_blob_ids())
            .filter(|blob_id| !created_blob_ids.contains(blob_id))
            .collect::<Vec<_>>();

        match self
            .local_node
            .read_blob_states_from_storage(&required_blob_ids)
            .await
        {
            Err(LocalNodeError::BlobsNotFound(blob_ids)) => {
                self.download_blobs(remote_nodes, &blob_ids).await?;
            }
            x => {
                x?;
            }
        }

        for certificate in certificates {
            if let Some(until) = until_block_time {
                if certificate.value().block().header.timestamp >= until {
                    break;
                }
            }
            let response = self
                .handle_certificate_with_retry(&certificate, remote_nodes, mode)
                .await?;
            info = Some(response.info);
        }

        Ok(info)
    }

    /// Calls `handle_confirmed_certificate`, retrying with any missing blobs (downloaded
    /// from `nodes`), any missing events (downloaded from the publisher
    /// chains via the current validators), and — if the certificate's epoch is
    /// revoked — the block's descendants (processed in decreasing height order so
    /// they transitively re-certify the block).
    async fn handle_certificate_with_retry(
        &self,
        certificate: &ConfirmedBlockCertificate,
        nodes: &[RemoteNode<Env::ValidatorNode>],
        mode: ProcessConfirmedBlockMode,
    ) -> Result<ChainInfoResponse, chain_client::Error> {
        let mut downloaded_blobs = HashSet::<BlobId>::new();
        let mut downloaded_blocks = HashSet::<CryptoHash>::new();
        let mut walked_descendants = false;
        let mut events = EventSetDownloader::new(self);
        loop {
            let result = self
                .handle_confirmed_certificate(certificate.clone(), mode)
                .await;
            if let Err(LocalNodeError::WorkerError(WorkerError::EpochRevoked {
                chain_id,
                height,
                ..
            })) = &result
            {
                // The local worker doesn't trust the certificate's revoked epoch on
                // its own. Try to establish trust by processing the block's
                // descendants first.
                if !walked_descendants {
                    walked_descendants = true;
                    if self
                        .preprocess_descendants(*chain_id, *height, nodes)
                        .await?
                    {
                        continue;
                    }
                }
            }
            if let Err(LocalNodeError::BlobsNotFound(blob_ids)) = &result {
                let new_blobs = filter_new(blob_ids, &downloaded_blobs);
                if !new_blobs.is_empty() {
                    self.download_blobs(nodes, &new_blobs).await?;
                    downloaded_blobs.extend(new_blobs);
                    continue;
                }
            }
            if let Err(LocalNodeError::BlocksNotFound(hashes)) = &result {
                let new_blocks = filter_new(hashes, &downloaded_blocks);
                if !new_blocks.is_empty() {
                    self.download_pre_checkpoint_blocks(nodes, &new_blocks)
                        .await?;
                    downloaded_blocks.extend(new_blocks);
                    continue;
                }
            }
            if let Err(LocalNodeError::EventsNotFound(event_ids)) = &result {
                if events.download_new(event_ids).await? {
                    continue;
                }
            }
            return Ok(result?);
        }
    }

    /// Downloads each missing pre-checkpoint sender block from `nodes` and feeds it
    /// through the local worker. The worker's trust-mark accept path verifies the
    /// cert against its own epoch's committee and writes it to storage. Routing
    /// through `handle_certificate_with_retry` ensures the sender block's own
    /// blob/event dependencies (e.g. a `ChainDescription` blob or an admin-chain
    /// epoch event for a revoked epoch) get resolved before the cert is accepted.
    async fn download_pre_checkpoint_blocks(
        &self,
        nodes: &[RemoteNode<Env::ValidatorNode>],
        hashes: &[CryptoHash],
    ) -> Result<(), chain_client::Error> {
        for hash in hashes {
            let mut last_error = None;
            for node in nodes {
                match node.node.download_certificate(*hash).await {
                    Ok(certificate) => {
                        Box::pin(self.handle_certificate_with_retry(
                            &certificate,
                            nodes,
                            ProcessConfirmedBlockMode::Auto,
                        ))
                        .await?;
                        last_error = None;
                        break;
                    }
                    Err(error) => last_error = Some(error),
                }
            }
            if let Some(error) = last_error {
                return Err(error.into());
            }
        }
        Ok(())
    }

    /// Recovers a revoked-epoch block rejected by the local worker by processing the
    /// block's descendants first, in decreasing height order.
    ///
    /// Collects the chain's certificates from `height + 1` up to and including the
    /// first one whose epoch is not revoked (the anchor) — from local storage where
    /// possible, otherwise from `nodes` — and feeds them through the local worker
    /// newest-first: the anchor is trusted based on its own epoch, and each accepted
    /// child then vouches for its parent via `previous_block_hash`. Returns `false`
    /// if no anchor was found, e.g. because the chain's tip is itself in a revoked
    /// epoch.
    async fn preprocess_descendants(
        &self,
        chain_id: ChainId,
        height: BlockHeight,
        nodes: &[RemoteNode<Env::ValidatorNode>],
    ) -> Result<bool, chain_client::Error> {
        let batch_size = self.options.certificate_download_batch_size;
        let mut certificates = Vec::new();
        let mut next_height = height.try_add_one()?;
        'anchor: loop {
            let heights = (0..batch_size)
                .map(|offset| {
                    u64::from(next_height)
                        .checked_add(offset)
                        .map(BlockHeight)
                        .ok_or(ArithmeticError::Overflow)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut batch = self
                .storage_client()
                .read_certificates_by_heights(chain_id, &heights)
                .await?
                .into_iter()
                .map_while(|maybe_certificate| maybe_certificate)
                .collect::<Vec<_>>();
            if batch.is_empty() {
                // Race the validators instead of trying them one by one: a slow or
                // faulty validator must not stall the walk. A validator that
                // returns nothing counts as a failure so the others take over.
                let downloaded = communicate_concurrently(
                    nodes,
                    async move |remote_node| {
                        // Bound the batch by the validator's chain height: a request
                        // including heights the validator doesn't have would fail as
                        // a whole.
                        let info = remote_node
                            .handle_chain_info_query(ChainInfoQuery::new(chain_id))
                            .await?;
                        let batch_end = info
                            .next_block_height
                            .0
                            .min(u64::from(next_height).saturating_add(batch_size));
                        let heights = (u64::from(next_height)..batch_end)
                            .map(BlockHeight)
                            .collect::<Vec<_>>();
                        if heights.is_empty() {
                            return Err(NodeError::MissingCertificateValue);
                        }
                        let downloaded = self
                            .requests_scheduler
                            .download_certificates_by_heights(&remote_node, chain_id, heights)
                            .await?;
                        if downloaded.is_empty() {
                            return Err(NodeError::MissingCertificateValue);
                        }
                        Ok(downloaded)
                    },
                    self.options.certificate_batch_download_hedge_delay,
                    self.storage_client().clock(),
                )
                .await;
                if let Ok(downloaded) = downloaded {
                    batch = downloaded
                        .into_iter()
                        .map(|certificate| self.storage_client().cache_certificate(certificate))
                        .collect();
                }
            }
            if batch.is_empty() {
                return Ok(false);
            }
            for certificate in batch {
                if certificate.block().header.height != next_height {
                    // A gap in the returned heights breaks the vouching chain.
                    return Ok(false);
                }
                let epoch = certificate.block().header.epoch;
                let is_revoked = self
                    .storage_client()
                    .is_epoch_revoked(epoch)
                    .await
                    .map_err(LocalNodeError::from)?;
                certificates.push(certificate);
                if !is_revoked {
                    break 'anchor;
                }
                next_height = next_height.try_add_one()?;
            }
        }
        // Process the descendants newest-first, so that each block is vouched for
        // by the one processed just before it.
        for certificate in certificates.iter().rev() {
            tracing::warn!(
                "DBG preprocess_descendants: processing {}",
                certificate.block().header.height
            );
            Box::pin(self.handle_certificate_with_retry(
                certificate,
                nodes,
                ProcessConfirmedBlockMode::Preprocess,
            ))
            .await?;
        }
        Ok(true)
    }

    async fn handle_certificate<T: ProcessableCertificate>(
        &self,
        certificate: T::Certificate,
    ) -> Result<ChainInfoResponse, LocalNodeError> {
        self.local_node
            .handle_certificate::<T>(certificate, &self.notifier)
            .await
    }

    async fn handle_confirmed_certificate(
        &self,
        certificate: ConfirmedBlockCertificate,
        mode: ProcessConfirmedBlockMode,
    ) -> Result<ChainInfoResponse, LocalNodeError> {
        self.local_node
            .handle_confirmed_certificate(certificate, mode, &self.notifier)
            .await
    }

    /// Obtains the committee for the latest epoch on the admin chain.
    pub async fn admin_committee(&self) -> Result<(Epoch, Arc<Committee>), LocalNodeError> {
        let info = self.local_node.chain_info(self.admin_chain_id).await?;
        let hash = info
            .committee_hash
            .ok_or(LocalNodeError::InactiveChain(self.admin_chain_id))?;
        let committee = self
            .storage_client()
            .get_or_load_committee_by_hash(hash)
            .await?;
        Ok((info.epoch, committee))
    }

    /// Obtains the validators for the latest epoch.
    async fn validator_nodes(
        &self,
    ) -> Result<Vec<RemoteNode<Env::ValidatorNode>>, chain_client::Error> {
        let (_, committee) = self.admin_committee().await?;
        Ok(self.make_nodes(&committee)?)
    }

    /// Creates a [`RemoteNode`] for each validator in the committee.
    fn make_nodes(
        &self,
        committee: &Committee,
    ) -> Result<Vec<RemoteNode<Env::ValidatorNode>>, NodeError> {
        Ok(self
            .validator_node_provider()
            .make_nodes(committee)?
            .map(|(public_key, node)| RemoteNode { public_key, node })
            .collect())
    }

    /// Ensures that the client has the `ChainDescription` blob corresponding to this
    /// client's `ChainId`, and returns the chain description blob.
    pub async fn get_chain_description_blob(
        &self,
        chain_id: ChainId,
    ) -> Result<Arc<Blob>, chain_client::Error> {
        let chain_desc_id = BlobId::new(chain_id.0, BlobType::ChainDescription);
        let blob = self
            .local_node
            .storage_client()
            .read_blob(chain_desc_id)
            .await?;
        if let Some(blob) = blob {
            // We have the blob - return it.
            return Ok(blob.into_std());
        }
        // Recover history from the current validators, according to the admin chain.
        self.synchronize_chain_state(self.admin_chain_id).await?;
        let nodes = self.validator_nodes().await?;
        Ok(self
            .update_local_node_with_blobs_from(vec![chain_desc_id], &nodes)
            .await?
            .pop()
            .unwrap() // Returns exactly as many blobs as passed-in IDs.
            .into_std())
    }

    /// Ensures that the client has the `ChainDescription` blob corresponding to this
    /// client's `ChainId`, and returns the chain description.
    pub async fn get_chain_description(
        &self,
        chain_id: ChainId,
    ) -> Result<ChainDescription, chain_client::Error> {
        let blob = self.get_chain_description_blob(chain_id).await?;
        Ok(bcs::from_bytes(blob.bytes())?)
    }

    /// Ensures that the client has the `ApplicationDescription` blob for the given
    /// application ID, fetching it from the current validators if it is not available
    /// locally, and returns the blob. The application need not be registered on this
    /// client's chain: the description is content-addressed and downloaded by blob ID.
    pub async fn get_application_description_blob(
        &self,
        application_id: ApplicationId,
    ) -> Result<Arc<Blob>, chain_client::Error> {
        let blob_id = application_id.description_blob_id();
        let blob = self.local_node.storage_client().read_blob(blob_id).await?;
        if let Some(blob) = blob {
            // We have the blob - return it.
            return Ok(blob.into_std());
        }
        // Recover the blob from the current validators, according to the admin chain.
        Box::pin(self.synchronize_chain_state(self.admin_chain_id)).await?;
        let nodes = self.validator_nodes().await?;
        Ok(self
            .update_local_node_with_blobs_from(vec![blob_id], &nodes)
            .await?
            .pop()
            .unwrap() // Returns exactly as many blobs as passed-in IDs.
            .into_std())
    }

    /// Returns the `ApplicationDescription` of the given application, fetching its
    /// description blob from the validators if it is not available locally.
    pub async fn get_application_description(
        &self,
        application_id: ApplicationId,
    ) -> Result<ApplicationDescription, chain_client::Error> {
        let blob = self
            .get_application_description_blob(application_id)
            .await?;
        Ok(bcs::from_bytes(blob.bytes())?)
    }

    /// Submits a validated block for finalization and returns the confirmed block certificate.
    #[instrument(level = "trace", skip_all)]
    pub(crate) async fn finalize_block(
        self: &Arc<Self>,
        committee: &Committee,
        certificate: ValidatedBlockCertificate,
    ) -> Result<ConfirmedBlockCertificate, chain_client::Error> {
        debug!(round = %certificate.round(), "Submitting block for confirmation");
        let hashed_value = ConfirmedBlock::new(certificate.block().clone());
        // The full chain of validated quorums for the block: this validated certificate's own
        // quorum as the top link, then the chain below it. Whether the confirmed certificate
        // actually carries it is decided *after* the quorum forms, from the attestation the
        // confirming votes signed (below).
        let full_justification = certificate.full_justification();
        let finalize_action = CommunicateAction::FinalizeBlock {
            certificate: Box::new(certificate),
            delivery: self.options.cross_chain_message_delivery,
        };
        let quorum = self
            .communicate_chain_action(committee, finalize_action, hashed_value)
            .await?;
        // Omit the chain iff the confirming votes attested that this is the chain's first round:
        // such a block is always the lower one in any fork, so it never needs a chain of its own.
        // Deciding from the quorum's signed attestation — rather than our local ownership view,
        // which a concurrently finalized block could have advanced to a different first round —
        // guarantees the certificate we assemble matches what the validators actually signed.
        let justification = if quorum.first_round() {
            JustificationChain::default()
        } else {
            full_justification
        };
        // The confirming votes committed to the chain they were shown; the chain we attach must
        // be that one, or the assembled certificate would fail verification everywhere.
        ensure!(
            quorum.justification_commitment() == justification.commitment(quorum.hash()),
            chain_client::Error::ProtocolError(
                "A quorum confirmed with a justification commitment that does not match the \
                 validated certificate's justification chain",
            )
        );
        let certificate = ConfirmedBlockCertificate::from_parts(quorum, justification);
        self.receive_certificate_with_checked_signatures(
            certificate.clone(),
            ProcessConfirmedBlockMode::Execute,
        )
        .await?;
        Ok(certificate)
    }

    /// Submits a block proposal to the validators.
    #[instrument(level = "trace", skip_all)]
    async fn submit_block_proposal<T: ProcessableCertificate>(
        self: &Arc<Self>,
        committee: Arc<Committee>,
        proposal: Box<BlockProposal>,
        value: T,
    ) -> Result<T::Certificate, chain_client::Error> {
        debug!(
            round = %proposal.content.round,
            "Submitting block proposal to validators"
        );

        // The certificate's justification chain comes from the proposal: a regular retry is
        // justified by the validated certificate it carries (the new top link plus that
        // certificate's own chain); a fresh proposal or fast-round proposal has none.
        let justification = match proposal.original_proposal.as_ref() {
            Some(OriginalProposal::Regular { certificate }) => certificate.full_justification(),
            Some(OriginalProposal::Fast(_)) | None => JustificationChain::default(),
        };

        // Check if the block timestamp is in the future and log INFO.
        let block_timestamp = proposal.content.block.timestamp;
        let local_time = self.local_node.storage_client().clock().current_time();
        if block_timestamp > local_time {
            info!(
                chain_id = %proposal.content.block.chain_id,
                %block_timestamp,
                %local_time,
                "Block timestamp is in the future; waiting until it can be proposed",
            );
        }

        // Create channel for clock skew reports from validators.
        let (clock_skew_sender, mut clock_skew_receiver) = mpsc::unbounded_channel();
        let submit_action = CommunicateAction::SubmitBlock {
            proposal,
            blob_ids: value.required_blob_ids().into_iter().collect(),
            clock_skew_sender,
        };

        // Spawn a task to monitor clock skew reports and warn if threshold is reached.
        let validity_threshold = committee.validity_threshold();
        let committee_clone = committee.clone();
        let clock_skew_check_handle = linera_base::Task::spawn(async move {
            let mut skew_weight = 0u64;
            let mut min_skew = TimeDelta::MAX;
            let mut max_skew = TimeDelta::ZERO;
            while let Some((public_key, clock_skew)) = clock_skew_receiver.recv().await {
                if clock_skew.as_micros() > 0 {
                    skew_weight += committee_clone.weight(&public_key);
                    min_skew = min_skew.min(clock_skew);
                    max_skew = max_skew.max(clock_skew);
                    if skew_weight >= validity_threshold {
                        warn!(
                            skew_weight,
                            validity_threshold,
                            min_skew_ms = min_skew.as_micros() / 1000,
                            max_skew_ms = max_skew.as_micros() / 1000,
                            "A validity threshold of validators reported clock skew; \
                             consider checking your system clock",
                        );
                        return;
                    }
                }
            }
        });

        let quorum = self
            .communicate_chain_action(&committee, submit_action, value)
            .await?;

        clock_skew_check_handle.await;

        // The justification chain comes from our own proposal, but the winning quorum may have
        // been formed from a competing proposal that cited a different certificate: its votes then
        // sign a different unlocking round and justification commitment, and gluing our chain onto
        // them would build a certificate that fails verification downstream. Reject that here with
        // a retryable error rather than assembling a mismatched certificate. (For confirmed and
        // timeout quorums both sides are `None`, so this only bites the validated-retry case it is
        // meant to guard.)
        ensure!(
            quorum.unlocking_round() == justification.top_unlocking_round(),
            chain_client::Error::ProtocolError(
                "A quorum voted with an unlocking round that does not match the proposal's \
                 justification chain",
            )
        );
        ensure!(
            quorum.justification_commitment() == justification.commitment(quorum.hash()),
            chain_client::Error::ProtocolError(
                "A quorum voted with a justification commitment that does not match the \
                 proposal's justification chain",
            )
        );
        let certificate = T::make_certificate(quorum, justification);
        self.handle_certificate::<T>(certificate.clone()).await?;
        Ok(certificate)
    }

    /// Broadcasts certified blocks to validators.
    #[instrument(level = "trace", skip_all, fields(chain_id, block_height, delivery))]
    async fn communicate_chain_updates(
        self: &Arc<Self>,
        committee: &Committee,
        chain_id: ChainId,
        height: BlockHeight,
        delivery: CrossChainMessageDelivery,
        latest_certificate: Option<CacheArc<ConfirmedBlockCertificate>>,
    ) -> Result<(), chain_client::Error> {
        let nodes = self.make_nodes(committee)?;
        communicate_with_quorum(
            &nodes,
            committee,
            |_: &()| (),
            |remote_node| {
                let mut updater = ValidatorUpdater {
                    remote_node,
                    client: self.clone(),
                    admin_chain_id: self.admin_chain_id,
                };
                let certificate = latest_certificate.clone();
                Box::pin(async move {
                    updater
                        .send_chain_information(chain_id, height, delivery, certificate)
                        .await
                })
            },
            self.options.quorum_grace_period,
        )
        .await?;
        Ok(())
    }

    /// Broadcasts certified blocks and optionally a block proposal, certificate or
    /// leader timeout request.
    ///
    /// In that case, it verifies that the validator votes are for the provided value,
    /// and returns a certificate.
    #[instrument(level = "trace", skip_all)]
    async fn communicate_chain_action<T: CertificateValue>(
        self: &Arc<Self>,
        committee: &Committee,
        action: CommunicateAction,
        value: T,
    ) -> Result<GenericCertificate<T>, chain_client::Error> {
        let nodes = self.make_nodes(committee)?;
        // Group votes by their full signed payload: signatures only aggregate into a
        // certificate if they are unanimous on all signed fields, so a vote that diverges in
        // the unlocking round, first-round attestation or justification commitment belongs to
        // a separate candidate quorum.
        let ((votes_hash, votes_round, _, _, _), votes) = communicate_with_quorum(
            &nodes,
            committee,
            |vote: &LiteVote| {
                (
                    vote.value.value_hash,
                    vote.round,
                    vote.unlocking_round,
                    vote.first_round,
                    vote.justification_commitment,
                )
            },
            |remote_node| {
                let mut updater = ValidatorUpdater {
                    remote_node,
                    client: self.clone(),
                    admin_chain_id: self.admin_chain_id,
                };
                let action = action.clone();
                Box::pin(async move { updater.send_chain_update(action).await })
            },
            self.options.quorum_grace_period,
        )
        .await?;
        ensure!(
            (votes_hash, votes_round) == (value.hash(), action.round()),
            chain_client::Error::UnexpectedQuorum {
                hash: votes_hash,
                round: votes_round,
                expected_hash: value.hash(),
                expected_round: action.round(),
            }
        );
        // Certificate is valid because
        // * `communicate_with_quorum` ensured a sufficient "weight" of
        // (non-error) answers were returned by validators.
        // * each answer is a vote signed by the expected validator.
        let certificate = LiteCertificate::try_from_votes(votes)
            .ok_or_else(|| {
                chain_client::Error::InternalError(
                    "Vote values or rounds don't match; this is a bug",
                )
            })?
            .with_value(value)
            .ok_or_else(|| {
                chain_client::Error::ProtocolError("A quorum voted for an unexpected value")
            })?;
        Ok(certificate)
    }

    /// Processes the confirmed block certificate in the local node without checking signatures.
    /// Also downloads and processes all ancestors that are still missing.
    #[instrument(level = "trace", skip_all)]
    async fn receive_certificate_with_checked_signatures(
        &self,
        certificate: ConfirmedBlockCertificate,
        mode: ProcessConfirmedBlockMode,
    ) -> Result<(), chain_client::Error> {
        let block = certificate.block();
        // Recover history from the network.
        self.download_certificates(block.header.chain_id, block.header.height)
            .await?;
        // Process the received operations. Download required hashed certificate values if
        // necessary.
        let nodes = self.validator_nodes().await?;
        self.handle_certificate_with_retry(&certificate, &nodes, mode)
            .await?;
        Ok(())
    }

    /// Processes the confirmed block in the local node. Whether it is fully
    /// executed or only preprocessed depends on the chain's [`ListeningMode`]:
    /// chains we follow get executed, untracked or events-only chains are only
    /// preprocessed.
    #[instrument(level = "trace", skip_all)]
    // Returns a boxed future rather than being an `async fn`: this function recurses
    // (admin-chain sync -> sender-certificate processing -> here), and boxing it at this
    // boundary lets the concurrent admin-chain self-heal below satisfy its future bound
    // despite the recursion. The future is `Send` off `web` (see the type alias).
    fn receive_sender_certificate(
        &self,
        certificate: CacheArc<ConfirmedBlockCertificate>,
        mode: ReceiveCertificateMode,
        nodes: Option<Vec<RemoteNode<Env::ValidatorNode>>>,
    ) -> ReceiveSenderCertificateFuture<'_> {
        Box::pin(async move {
            // Verify the certificate before doing any expensive networking.
            if let ReceiveCertificateMode::NeedsCheck = mode {
                let mut check_result = self.check_certificate(&certificate).await;
                if matches!(
                    check_result,
                    Err(chain_client::Error::CommitteeSynchronizationError)
                ) {
                    // The certificate is from an epoch our local view of the admin chain
                    // hasn't caught up to yet. Catch up and check again instead of failing.
                    // Prefer the nodes that gave us the certificate: they evidently know
                    // the newer epoch even if our own committee view is stale or its
                    // members are unreachable. Race them concurrently rather than blocking
                    // on a slow one, then fall back to the known committee. A sync only
                    // counts if it actually made the epoch known.
                    let admin_chain_id = self.admin_chain_id;
                    let epoch = certificate.block().header.epoch;
                    info!(
                        %epoch,
                        "certificate is from an unknown epoch; synchronizing the admin chain"
                    );
                    let synced_from_serving_node = if let Some(nodes) = &nodes {
                        let certificate = &certificate;
                        communicate_concurrently(
                            nodes,
                            |node| {
                                Box::pin(async move {
                                    self.synchronize_chain_state_from(&node, admin_chain_id)
                                        .await?;
                                    self.check_certificate(certificate).await
                                })
                            },
                            self.options.blob_download_hedge_delay,
                            self.storage_client().clock(),
                        )
                        .await
                        .is_ok()
                    } else {
                        false
                    };
                    if synced_from_serving_node {
                        check_result = self.check_certificate(&certificate).await;
                    }
                    if matches!(
                        check_result,
                        Err(chain_client::Error::CommitteeSynchronizationError)
                    ) {
                        Box::pin(self.synchronize_chain_state(admin_chain_id)).await?;
                        check_result = self.check_certificate(&certificate).await;
                    }
                }
                check_result?;
            }
            // Recover history from the network.
            let nodes = if let Some(nodes) = nodes {
                nodes
            } else {
                self.validator_nodes().await?
            };
            let processing_mode = if self
                .chain_mode(certificate.value().chain_id())
                .is_some_and(|m| m.should_sync_chain_state())
            {
                ProcessConfirmedBlockMode::Auto
            } else {
                ProcessConfirmedBlockMode::Preprocess
            };
            self.handle_certificate_with_retry(&certificate, &nodes, processing_mode)
                .await?;

            Ok(())
        })
    }

    /// Downloads and processes certificates for sender chain blocks.
    #[instrument(level = "debug", skip_all, fields(chain_id = %sender_chain_id))]
    async fn download_and_process_sender_chain(
        &self,
        receiver_chain_id: ChainId,
        sender_chain_id: ChainId,
        nodes: &[RemoteNode<Env::ValidatorNode>],
        received_log: &ReceivedLogs,
        mut remote_heights: Vec<BlockHeight>,
        sender: mpsc::UnboundedSender<ChainAndHeight>,
    ) {
        let mut nodes = nodes.to_vec();
        // Process the lowest block together with its earlier message blocks first:
        // its `previous_message_blocks` chain may reach into revoked epochs, whose
        // received-log entries are pruned by validators and therefore missing from
        // `remote_heights`. The ancestor walk downloads exactly the message-bearing
        // predecessors (re-certifying revoked-epoch ones via the blocks that
        // commit to them), so that the newer blocks' messages can actually be
        // scheduled in the outbox. The downloaded certificates land in storage,
        // where the loop below picks them up.
        if let Some(lowest_height) = remote_heights.first().copied() {
            let received_log = &received_log;
            let result = communicate_concurrently(
                &nodes,
                async move |remote_node| {
                    if !received_log.validator_has_block(
                        &remote_node.public_key,
                        sender_chain_id,
                        lowest_height,
                    ) {
                        // The validator's log didn't contain the block; let the
                        // others handle it.
                        return Err(chain_client::Error::RemoteNodeError(
                            NodeError::MissingCertificateValue,
                        ));
                    }
                    self.download_sender_block_with_sending_ancestors(
                        receiver_chain_id,
                        sender_chain_id,
                        lowest_height,
                        &remote_node,
                    )
                    .await
                },
                self.options.certificate_batch_download_hedge_delay,
                self.storage_client().clock(),
            )
            .await;
            if let Err(errors) = result {
                for (validator, error) in errors {
                    warn!(
                        %validator,
                        %sender_chain_id,
                        %lowest_height,
                        %error,
                        "failed to process the lowest sender block with its \
                         sending ancestors",
                    );
                }
            }
        }
        while !remote_heights.is_empty() {
            // Check local storage first — certificates may already be available from
            // a prior sync cycle, another receiver chain, or a concurrent notification.
            if let Ok(local_certs) = self
                .storage_client()
                .read_certificates_by_heights(sender_chain_id, &remote_heights)
                .await
            {
                let mut still_needed = Vec::new();
                for (height, maybe_cert) in remote_heights.iter().copied().zip(local_certs) {
                    if let Some(certificate) = maybe_cert {
                        let chain_id = certificate.block().header.chain_id;
                        if let Err(error) = sender.send(ChainAndHeight { chain_id, height }) {
                            error!(
                                %chain_id, %height, %error,
                                "failed to send chain and height over the channel",
                            );
                        }
                    } else {
                        still_needed.push(height);
                    }
                }
                remote_heights = still_needed;
                if remote_heights.is_empty() {
                    break;
                }
            }

            let remote_heights_ref = &remote_heights;
            let (certificates, unknown_epoch_heights) = match communicate_concurrently(
                &nodes,
                async move |remote_node| {
                    let mut remote_heights = remote_heights_ref.clone();
                    // No need trying to download certificates the validator didn't have in their
                    // log - we'll retry downloading the remaining ones next time we loop.
                    remote_heights.retain(|height| {
                        received_log.validator_has_block(
                            &remote_node.public_key,
                            sender_chain_id,
                            *height,
                        )
                    });
                    if remote_heights.is_empty() {
                        // It makes no sense to return `Ok(_)` if we aren't going to try downloading
                        // anything from the validator - let the function try the other validators
                        return Err(NodeError::MissingCertificateValue);
                    }
                    let downloaded = self
                        .requests_scheduler
                        .download_certificates_by_heights(
                            &remote_node,
                            sender_chain_id,
                            remote_heights,
                        )
                        .await?;
                    // Set aside the certificates whose epochs we don't know yet:
                    // they are not received - and not re-downloaded either, until a
                    // future sync cycle after our view of the admin chain has
                    // caught up.
                    let mut certificates = Vec::new();
                    let mut unknown_epoch_heights = Vec::new();
                    for cert in downloaded {
                        match self.check_certificate(&cert).await {
                            Ok(()) => certificates.push(cert),
                            Err(chain_client::Error::CommitteeSynchronizationError) => {
                                unknown_epoch_heights.push(cert.block().header.height);
                            }
                            Err(chain_client::Error::RemoteNodeError(error)) => return Err(error),
                            Err(error) => {
                                return Err(NodeError::ResponseHandlingError {
                                    error: error.to_string(),
                                })
                            }
                        }
                    }
                    Ok((certificates, unknown_epoch_heights))
                },
                self.options.certificate_batch_download_hedge_delay,
                self.storage_client().clock(),
            )
            .await
            {
                Ok(result) => result,
                Err(errors) => {
                    let faulty_validators = errors
                        .into_iter()
                        .map(|(validator, error)| {
                            warn!(
                                %validator,
                                %sender_chain_id,
                                %error,
                                "failed to download certificates from validator",
                            );
                            validator
                        })
                        .collect::<BTreeSet<_>>();
                    // filter out faulty validators and retry if any are left
                    nodes.retain(|node| !faulty_validators.contains(&node.public_key));
                    if nodes.is_empty() {
                        info!(
                            chain_id = %sender_chain_id,
                            "could not download certificates for chain - no more correct validators left"
                        );
                        return;
                    }
                    continue;
                }
            };

            trace!(
                num_certificates = %certificates.len(),
                "received certificates",
            );

            let mut to_remove_from_queue = BTreeSet::from_iter(unknown_epoch_heights);

            for certificate in certificates {
                let hash = certificate.hash();
                let chain_id = certificate.block().header.chain_id;
                let height = certificate.block().header.height;
                // We checked the certificates right after downloading them.
                let mode = ReceiveCertificateMode::AlreadyChecked;
                if let Err(error) = self
                    .receive_sender_certificate(
                        self.storage_client().cache_certificate(certificate),
                        mode,
                        None,
                    )
                    .await
                {
                    warn!(%error, %hash, "Received invalid certificate");
                    if matches!(
                        &error,
                        chain_client::Error::LocalNodeError(LocalNodeError::WorkerError(
                            WorkerError::EpochRevoked { .. }
                        ))
                    ) {
                        // The block's epoch is revoked and it could not be
                        // re-certified via descendants; drop it from the queue
                        // instead of re-downloading it indefinitely.
                        to_remove_from_queue.insert(height);
                    }
                } else {
                    to_remove_from_queue.insert(height);
                    if let Err(error) = sender.send(ChainAndHeight { chain_id, height }) {
                        error!(
                            %chain_id,
                            %height,
                            %error,
                            "failed to send chain and height over the channel",
                        );
                    }
                }
            }

            remote_heights.retain(|height| !to_remove_from_queue.contains(height));
        }
        trace!("find_received_certificates: finished processing chain");
    }

    /// Downloads the per-epoch logs of messages received from senders in the given
    /// epochs for a chain from a validator, starting at the given per-epoch
    /// offsets. All epochs are requested in a single query; the validator bounds
    /// the total number of entries per response, so the request is repeated with
    /// advanced offsets until nothing is cut short anymore.
    #[instrument(level = "trace", skip(self, offsets))]
    async fn get_received_log_from_validator(
        &self,
        chain_id: ChainId,
        remote_node: &RemoteNode<Env::ValidatorNode>,
        mut offsets: BTreeMap<Epoch, u64>,
    ) -> Result<BTreeMap<Epoch, Vec<ChainAndHeight>>, chain_client::Error> {
        // Retrieve the list of newly received certificates from this validator.
        let mut remote_logs = BTreeMap::<Epoch, Vec<ChainAndHeight>>::new();
        loop {
            trace!("get_received_log_from_validator: looping");
            let query = ChainInfoQuery::new(chain_id).with_received_logs(offsets.clone());
            let info = remote_node.handle_chain_info_query(query).await?;
            let received_entries: usize = info.requested_received_log.values().map(Vec::len).sum();
            for (epoch, entries) in info.requested_received_log {
                if let Some(offset) = offsets.get_mut(&epoch) {
                    *offset += entries.len() as u64;
                    remote_logs.entry(epoch).or_default().extend(entries);
                }
            }
            trace!(
                remote_node = remote_node.address(),
                %received_entries,
                "get_received_log_from_validator: received log batch",
            );
            // A response below the total bound means no epoch's log was cut short.
            if received_entries < CHAIN_INFO_MAX_RECEIVED_LOG_ENTRIES {
                break;
            }
        }

        trace!(
            remote_node = remote_node.address(),
            num_entries = remote_logs.values().map(Vec::len).sum::<usize>(),
            "get_received_log_from_validator: returning downloaded log",
        );

        Ok(remote_logs)
    }

    /// Downloads a specific sender block and recursively downloads any earlier blocks
    /// that also sent a message to our chain, based on `previous_message_blocks`.
    ///
    /// This ensures that we have all the sender blocks needed to preprocess the target block
    /// and put the messages to our chain into the outbox.
    async fn download_sender_block_with_sending_ancestors(
        &self,
        receiver_chain_id: ChainId,
        sender_chain_id: ChainId,
        height: BlockHeight,
        remote_node: &RemoteNode<Env::ValidatorNode>,
    ) -> Result<(), chain_client::Error> {
        let next_outbox_height = self
            .local_node
            .next_outbox_heights(&[sender_chain_id], receiver_chain_id)
            .await?
            .get(&sender_chain_id)
            .copied()
            .unwrap_or(BlockHeight::ZERO);

        // Recursively collect all certificates we need, following
        // the chain of previous_message_blocks back to next_outbox_height.
        let mut certificates = BTreeMap::new();
        let mut current_height = height;
        // On the first iteration we only have a height; subsequent iterations
        // also carry the hash from `previous_message_blocks`.
        let mut current_hash: Option<CryptoHash> = None;

        // Stop if we've reached the height we've already processed.
        while current_height >= next_outbox_height {
            // Try local storage first — avoids a validator round-trip when
            // the certificate was already downloaded by a prior sync cycle,
            // another receiver chain, or a concurrent notification handler.
            let certificate = if let Some(local) = self
                .try_read_local_certificate(sender_chain_id, current_height, current_hash)
                .await?
            {
                local
            } else {
                let downloaded = self
                    .requests_scheduler
                    .download_certificates_by_heights(
                        remote_node,
                        sender_chain_id,
                        vec![current_height],
                    )
                    .await?;
                let Some(certificate) = downloaded.into_iter().next() else {
                    return Err(chain_client::Error::CannotDownloadMissingSenderBlock {
                        chain_id: sender_chain_id,
                        height: current_height,
                    });
                };
                self.storage_client().cache_certificate(certificate)
            };

            // Validate the certificate.
            self.check_certificate(&certificate).await?;

            // Check if there's a previous message block to our chain.
            let block = certificate.block();
            let next = block
                .body
                .previous_message_blocks
                .get(&receiver_chain_id)
                .map(|(prev_hash, prev_height)| (*prev_hash, *prev_height));

            // Store this certificate.
            certificates.insert(current_height, certificate);

            if let Some((prev_hash, prev_height)) = next {
                // Continue with the previous block (now with its hash for local lookup).
                current_height = prev_height;
                current_hash = Some(prev_hash);
            } else {
                // No more dependencies.
                break;
            }
        }

        if certificates.is_empty() {
            self.retry_pending_cross_chain_requests(sender_chain_id)
                .await?;
        }

        // If the oldest block's epoch has been revoked, preprocess the collected run
        // newest-first before the ascending processing below: each block vouches for
        // the previous message block it commits to, which the local worker would
        // otherwise reject.
        self.recertify_if_epoch_revoked(&certificates, remote_node)
            .await?;

        // Process certificates in ascending block height order (BTreeMap keeps them sorted).
        for certificate in certificates.into_values() {
            self.receive_sender_certificate(
                certificate,
                ReceiveCertificateMode::AlreadyChecked,
                Some(vec![remote_node.clone()]),
            )
            .await?;
        }

        Ok(())
    }

    /// Preprocesses a sparsely collected run of blocks newest-first if the oldest one's
    /// epoch has been revoked, so that the newer blocks vouch for the older ones.
    ///
    /// A revoked epoch's certificate alone doesn't convince the local worker. But
    /// accepting a block marks the hashes it commits to in `previous_message_blocks` and
    /// `previous_event_blocks` as vouched, so the block the oldest one was reached
    /// through makes it acceptable despite its revoked epoch — without downloading the
    /// intermediate blocks that sent no message and published no event. Epoch revocation
    /// is monotone, so only the oldest block needs checking; if even the newest block's
    /// epoch is revoked, the retry logic falls back to walking contiguous descendants.
    async fn recertify_if_epoch_revoked(
        &self,
        certificates: &BTreeMap<BlockHeight, CacheArc<ConfirmedBlockCertificate>>,
        remote_node: &RemoteNode<Env::ValidatorNode>,
    ) -> Result<(), chain_client::Error> {
        let Some(oldest) = certificates.values().next() else {
            return Ok(());
        };
        if !self
            .storage_client()
            .is_epoch_revoked(oldest.block().header.epoch)
            .await
            .map_err(LocalNodeError::from)?
        {
            return Ok(());
        }
        for certificate in certificates.values().rev() {
            Box::pin(self.handle_certificate_with_retry(
                certificate,
                std::slice::from_ref(remote_node),
                ProcessConfirmedBlockMode::Preprocess,
            ))
            .await?;
        }
        Ok(())
    }

    /// Downloads event-bearing blocks for the given streams by walking the
    /// `previous_event_blocks` linked list backwards from `height`, stopping when we
    /// reach blocks that are already executed locally or whose events we already track.
    async fn download_event_bearing_blocks(
        &self,
        publisher_chain_id: ChainId,
        initial_blocks: BTreeSet<(BlockHeight, CryptoHash)>,
        local_next_block_height: BlockHeight,
        subscribed_streams: &BTreeSet<StreamId>,
        remote_node: &RemoteNode<Env::ValidatorNode>,
    ) -> Result<(), chain_client::Error> {
        if initial_blocks.is_empty() {
            return Ok(());
        }

        let mut certificates = BTreeMap::new();
        let mut blocks_to_fetch = initial_blocks;
        let next_expected_events = self
            .local_node
            .next_expected_events(
                publisher_chain_id,
                subscribed_streams.iter().cloned().collect(),
            )
            .await?;

        while let Some((current_height, current_hash)) = blocks_to_fetch.pop_last() {
            if current_height < local_next_block_height {
                continue; // Already executed locally.
            }
            if certificates.contains_key(&current_height) {
                continue;
            }

            let certificate = if let Some(certificate) =
                self.storage_client().read_certificate(current_hash).await?
            {
                certificate
            } else {
                let downloaded = self
                    .requests_scheduler
                    .download_certificates(remote_node, publisher_chain_id, current_height, 1)
                    .await?;
                let Some(certificate) = downloaded.into_iter().next() else {
                    tracing::debug!(
                        validator = remote_node.address(),
                        %publisher_chain_id,
                        height = %current_height,
                        "failed to download event publisher block"
                    );
                    continue;
                };

                self.check_certificate(&certificate).await?;

                self.storage_client().cache_certificate(certificate)
            };

            let block = certificate.block();
            // Walk previous_event_blocks for subscribed streams.
            for stream_id in subscribed_streams {
                if let Some((prev_hash, prev_height)) =
                    block.body.previous_event_blocks.get(stream_id)
                {
                    if next_expected_events.get(stream_id).is_some_and(|index| {
                        block
                            .body
                            .events
                            .iter()
                            .flatten()
                            .find(|event| event.stream_id == *stream_id)
                            .is_some_and(|event| event.index == *index)
                    }) {
                        continue;
                    }
                    if !certificates.contains_key(prev_height) {
                        blocks_to_fetch.insert((*prev_height, *prev_hash));
                    }
                }
            }

            certificates.insert(current_height, certificate);
        }

        // If the oldest block's epoch has been revoked, preprocess the collected run
        // newest-first before the ascending processing below: each block vouches for
        // the previous event blocks it commits to, which the local worker would
        // otherwise reject.
        self.recertify_if_epoch_revoked(&certificates, remote_node)
            .await?;

        // Process in ascending height order.
        for certificate in certificates.into_values() {
            self.receive_sender_certificate(
                certificate,
                ReceiveCertificateMode::AlreadyChecked,
                Some(vec![remote_node.clone()]),
            )
            .await?;
        }

        Ok(())
    }

    /// Queries a validator for event-bearing blocks for the given streams, then downloads
    /// them.
    async fn sync_events_from_node(
        &self,
        chain_id: ChainId,
        stream_ids: &BTreeSet<StreamId>,
        remote_node: &RemoteNode<Env::ValidatorNode>,
    ) -> Result<(), chain_client::Error> {
        let stream_ids_vec = stream_ids.iter().cloned().collect::<Vec<_>>();
        let mut initial_blocks = BTreeSet::new();
        for chunk in stream_ids_vec.chunks(self.options.max_event_stream_queries) {
            let query = ChainInfoQuery::new(chain_id).with_previous_event_blocks(chunk.to_vec());
            let info = remote_node.handle_chain_info_query(query).await?;
            initial_blocks.extend(info.requested_previous_event_blocks.values().copied());
        }
        let local_height = match self.local_node.chain_info(chain_id).await {
            Ok(info) => info.next_block_height,
            Err(LocalNodeError::InactiveChain(_) | LocalNodeError::BlobsNotFound(_)) => {
                BlockHeight::ZERO
            }
            Err(error) => return Err(error.into()),
        };
        self.download_event_bearing_blocks(
            chain_id,
            initial_blocks,
            local_height,
            stream_ids,
            remote_node,
        )
        .await
    }

    /// Verifies the certificate's signatures against the committee of its own epoch.
    ///
    /// The committee is resolved from the admin chain's epoch event stream, which
    /// works even if the epoch has been revoked since. Whether a valid certificate
    /// from a revoked epoch is still a sufficient basis for trusting its block is
    /// not decided here: the worker only accepts such a block if an already-trusted
    /// block vouches for it.
    ///
    /// Returns [`chain_client::Error::CommitteeSynchronizationError`] if the epoch
    /// is not known locally yet, e.g. because our local view of the admin chain is
    /// behind; synchronizing the admin chain and retrying may resolve this.
    #[instrument(
        level = "trace", skip_all,
        fields(certificate_hash = ?incoming_certificate.hash()),
    )]
    async fn check_certificate(
        &self,
        incoming_certificate: &ConfirmedBlockCertificate,
    ) -> Result<(), chain_client::Error> {
        let epoch = incoming_certificate.block().header.epoch;
        let committee = self
            .storage_client()
            .committee_for_epoch(epoch)
            .await
            .map_err(LocalNodeError::from)?
            .ok_or(chain_client::Error::CommitteeSynchronizationError)?;
        incoming_certificate
            .check(&committee)
            .map_err(NodeError::from)?;
        Ok(())
    }

    /// Downloads and processes any certificates we are missing for the given chain.
    ///
    /// Whether manager values are fetched depends on the chain's follow-only state.
    #[instrument(level = "trace", skip_all)]
    async fn synchronize_chain_state(
        &self,
        chain_id: ChainId,
    ) -> Result<Box<ChainInfo>, chain_client::Error> {
        let (_, committee) = self.admin_committee().await?;
        self.synchronize_chain_from_committee(chain_id, committee)
            .await
    }

    /// Downloads certificates for the given chain from the given committee.
    ///
    /// If the chain is not in follow-only mode, also fetches and processes manager values
    /// (timeout certificates, proposals, locking blocks) for consensus participation.
    #[instrument(level = "trace", skip_all)]
    pub(crate) async fn synchronize_chain_from_committee(
        &self,
        chain_id: ChainId,
        committee: Arc<Committee>,
    ) -> Result<Box<ChainInfo>, chain_client::Error> {
        #[cfg(with_metrics)]
        let _latency = if !self.is_chain_follow_only(chain_id) {
            Some(metrics::SYNCHRONIZE_CHAIN_STATE_LATENCY.measure_latency())
        } else {
            None
        };

        let validators = self.make_nodes(&committee)?;
        Box::pin(self.fetch_chain_info(chain_id, &validators)).await?;
        communicate_with_quorum(
            &validators,
            &committee,
            |_: &()| (),
            |remote_node| async move {
                self.synchronize_chain_state_from(&remote_node, chain_id)
                    .await
            },
            self.options.quorum_grace_period,
        )
        .await?;

        self.local_node
            .chain_info(chain_id)
            .await
            .map_err(Into::into)
    }

    /// Downloads any certificates from the specified validator that we are missing for the given
    /// chain.
    ///
    /// If the chain is not in follow-only mode, also fetches and processes manager values
    /// (timeout certificates, proposals, locking blocks) for consensus participation.
    #[instrument(level = "trace", skip(self, remote_node, chain_id))]
    pub(crate) async fn synchronize_chain_state_from(
        &self,
        remote_node: &RemoteNode<Env::ValidatorNode>,
        chain_id: ChainId,
    ) -> Result<(), chain_client::Error> {
        let with_manager_values = !self.is_chain_follow_only(chain_id);
        let query = if with_manager_values {
            ChainInfoQuery::new(chain_id).with_manager_values()
        } else {
            ChainInfoQuery::new(chain_id)
        };
        let query = query.with_latest_checkpoint_height();
        let remote_info = remote_node.handle_chain_info_query(query).await?;

        // If the validator advertises a checkpoint and our local tip is below it, fetch
        // the checkpoint cert and blob and apply the cert directly. The chain worker's
        // `process_confirmed_block` recognises the gap-plus-checkpoint case and installs
        // the chain's execution state from the blob before re-executing the cert. The
        // subsequent `download_certificates_from` call then resumes from the post-
        // checkpoint height and skips downloading any pre-checkpoint blocks.
        if let Some(checkpoint_height) = remote_info.requested_latest_checkpoint_height {
            self.bootstrap_chain_from_checkpoint(remote_node, chain_id, checkpoint_height)
                .await?;
        }

        let local_info = self
            .download_certificates_from(remote_node, chain_id, remote_info.next_block_height, None)
            .await?;

        if !with_manager_values {
            return Ok(());
        }

        // If we are at the same height as the remote node, we also update our chain manager.
        let local_height = local_info.next_block_height;
        if local_height != remote_info.next_block_height {
            debug!(
                remote_node = remote_node.address(),
                remote_height = %remote_info.next_block_height,
                local_height = %local_height,
                "synced from validator, but remote height and local height are different",
            );
            return Ok(());
        };

        if let Some(timeout) = remote_info.manager.timeout {
            self.handle_certificate::<Timeout>(*timeout).await?;
        }
        let mut proposals = Vec::new();
        if let Some(proposal) = remote_info.manager.requested_signed_proposal {
            proposals.push(*proposal);
        }
        if let Some(proposal) = remote_info.manager.requested_proposed {
            proposals.push(*proposal);
        }
        if let Some(locking) = remote_info.manager.requested_locking {
            match *locking {
                LockingBlock::Fast(proposal) => {
                    proposals.push(proposal);
                }
                LockingBlock::Regular(cert) => {
                    let hash = cert.hash();
                    if let Err(error) = self.try_process_locking_block_from(remote_node, cert).await
                    {
                        debug!(
                            remote_node = remote_node.address(),
                            %hash,
                            height = %local_height,
                            %error,
                            "skipping locked block from validator",
                        );
                    }
                }
            }
        }
        'proposal_loop: for proposal in proposals {
            let owner: AccountOwner = proposal.owner();
            if let Err(mut err) = self
                .local_node
                .handle_block_proposal(proposal.clone())
                .await
            {
                if let LocalNodeError::BlobsNotFound(_) = &err {
                    let required_blob_ids = proposal.required_blob_ids().collect::<Vec<_>>();
                    if !required_blob_ids.is_empty() {
                        let mut blobs = Vec::new();
                        for blob_id in required_blob_ids {
                            let blob_content = match self
                                .requests_scheduler
                                .download_pending_blob(remote_node, chain_id, blob_id)
                                .await
                            {
                                Ok(content) => content,
                                Err(error) => {
                                    info!(
                                        remote_node = remote_node.address(),
                                        height = %local_height,
                                        proposer = %owner,
                                        %blob_id,
                                        %error,
                                        "skipping proposal from validator; failed to download blob",
                                    );
                                    continue 'proposal_loop;
                                }
                            };
                            blobs.push(Blob::new(blob_content));
                        }
                        self.local_node
                            .handle_pending_blobs(chain_id, blobs)
                            .await?;
                        // We found the missing blobs: retry.
                        if let Err(new_err) = self
                            .local_node
                            .handle_block_proposal(proposal.clone())
                            .await
                        {
                            err = new_err;
                        } else {
                            continue;
                        }
                    }
                    if let LocalNodeError::BlobsNotFound(blob_ids) = &err {
                        self.update_local_node_with_blobs_from(
                            blob_ids.clone(),
                            slice::from_ref(remote_node),
                        )
                        .await?;
                        // We found the missing blobs: retry.
                        if let Err(new_err) = self
                            .local_node
                            .handle_block_proposal(proposal.clone())
                            .await
                        {
                            err = new_err;
                        } else {
                            continue;
                        }
                    }
                }
                if let LocalNodeError::EventsNotFound(event_ids) = &err {
                    if let Err(error) =
                        Box::pin(self.download_certificates_for_events(event_ids)).await
                    {
                        info!(
                            remote_node = remote_node.address(),
                            height = %local_height,
                            proposer = %owner,
                            %error,
                            "skipping proposal from validator; failed to download events",
                        );
                        continue 'proposal_loop;
                    }
                    // We found the missing publisher chain data: retry.
                    if let Err(new_err) = self
                        .local_node
                        .handle_block_proposal(proposal.clone())
                        .await
                    {
                        err = new_err;
                    } else {
                        continue;
                    }
                }
                while let LocalNodeError::WorkerError(WorkerError::ChainError(chain_err)) = &err {
                    if let ChainError::MissingCrossChainUpdate {
                        chain_id,
                        origin,
                        height,
                    } = &**chain_err
                    {
                        self.download_sender_block_with_sending_ancestors(
                            *chain_id,
                            *origin,
                            *height,
                            remote_node,
                        )
                        .await?;
                        // Retry
                        if let Err(new_err) = self
                            .local_node
                            .handle_block_proposal(proposal.clone())
                            .await
                        {
                            err = new_err;
                        } else {
                            continue 'proposal_loop;
                        }
                    } else {
                        break;
                    }
                }

                debug!(
                    remote_node = remote_node.address(),
                    proposer = %owner,
                    height = %local_height,
                    error = %err,
                    "skipping proposal from validator",
                );
            }
        }
        Ok(())
    }

    async fn try_process_locking_block_from(
        &self,
        remote_node: &RemoteNode<Env::ValidatorNode>,
        certificate: ValidatedBlockCertificate,
    ) -> Result<(), chain_client::Error> {
        let chain_id = certificate.inner().chain_id();
        let mut downloaded_blobs = HashSet::<BlobId>::new();
        let mut events = EventSetDownloader::new(self);
        loop {
            let result = self
                .handle_certificate::<ValidatedBlock>(certificate.clone())
                .await;
            if let Err(LocalNodeError::BlobsNotFound(blob_ids)) = &result {
                let new_blobs = filter_new(blob_ids, &downloaded_blobs);
                if !new_blobs.is_empty() {
                    let mut blobs = Vec::new();
                    for blob_id in &new_blobs {
                        let blob_content = self
                            .requests_scheduler
                            .download_pending_blob(remote_node, chain_id, *blob_id)
                            .await?;
                        blobs.push(Blob::new(blob_content));
                    }
                    self.local_node
                        .handle_pending_blobs(chain_id, blobs)
                        .await?;
                    downloaded_blobs.extend(new_blobs);
                    continue;
                }
            }
            if let Err(LocalNodeError::EventsNotFound(event_ids)) = &result {
                if events.download_new(event_ids).await? {
                    continue;
                }
            }
            result?;
            return Ok(());
        }
    }

    /// Downloads and processes from the specified validators a confirmed block certificates that
    /// use the given blobs. If this succeeds, the blob will be in our storage.
    async fn update_local_node_with_blobs_from(
        &self,
        blob_ids: Vec<BlobId>,
        remote_nodes: &[RemoteNode<Env::ValidatorNode>],
    ) -> Result<Vec<CacheArc<Blob>>, chain_client::Error> {
        let hedge_delay = self.options.blob_download_hedge_delay;
        // Deduplicate IDs.
        let blob_ids = blob_ids.into_iter().collect::<BTreeSet<_>>();
        stream::iter(blob_ids.into_iter().map(|blob_id| {
            communicate_concurrently(
                remote_nodes,
                async move |remote_node| {
                    let certificate = self
                        .requests_scheduler
                        .download_certificate_for_blob(&remote_node, blob_id)
                        .await?;
                    self.receive_sender_certificate(
                        self.storage_client().cache_certificate(certificate),
                        ReceiveCertificateMode::NeedsCheck,
                        Some(vec![remote_node.clone()]),
                    )
                    .await?;
                    let blob = self
                        .local_node
                        .storage_client()
                        .read_blob(blob_id)
                        .await?
                        .ok_or_else(|| LocalNodeError::BlobsNotFound(vec![blob_id]))?;
                    Result::<_, chain_client::Error>::Ok(blob)
                },
                hedge_delay,
                self.storage_client().clock(),
            )
            .map_err(move |errors| {
                for (validator, error) in &errors {
                    warn!(
                        %validator,
                        %blob_id,
                        %error,
                        "failed to download certificate-for-blob from validator",
                    );
                }
                chain_client::Error::CannotDownloadBlob(blob_id)
            })
        }))
        .buffer_unordered(self.options.max_joined_tasks)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect()
    }

    /// Attempts to execute the block locally. If any incoming message execution fails, that
    /// message is rejected and execution is retried, until the block accepts only messages
    /// that succeed.
    ///
    /// Attempts to execute the block locally with a specified policy for handling bundle failures.
    /// If any attempt to read a blob fails, the blob is downloaded and execution is retried.
    ///
    /// Returns the modified block (bundles may be rejected/removed based on the policy)
    /// and the execution result.
    #[instrument(level = "trace", skip(self, block))]
    async fn stage_block_execution(
        &self,
        block: ProposedBlock,
        round: Option<u32>,
        published_blobs: Vec<Blob>,
        policy: BundleExecutionPolicy,
    ) -> Result<(Block, ChainInfoResponse, HashSet<ChainId>), chain_client::Error> {
        let mut events = EventSetDownloader::new(self);
        loop {
            let result = self
                .local_node
                .stage_block_execution(
                    block.clone(),
                    round,
                    published_blobs.clone(),
                    policy.clone(),
                )
                .await;
            if let Err(LocalNodeError::BlobsNotFound(blob_ids)) = &result {
                let validators = self.validator_nodes().await?;
                self.update_local_node_with_blobs_from(blob_ids.clone(), &validators)
                    .await?;
                continue; // We found the missing blob: retry.
            }
            if let Err(LocalNodeError::EventsNotFound(event_ids)) = &result {
                if events.download_new(event_ids).await? {
                    continue; // We downloaded new publisher chain data: retry.
                }
                // All reported events were already downloaded; don't loop forever.
            }
            if let Ok((_, executed_block, _, _, _)) = &result {
                let hash = executed_block.hash();
                let notification = Notification {
                    chain_id: executed_block.header.chain_id,
                    reason: Reason::BlockExecuted {
                        height: executed_block.header.height,
                        hash,
                    },
                };
                self.notifier.notify(&[notification]);
            }
            let (
                _modified_block,
                executed_block,
                response,
                _resource_tracker,
                never_reject_origins,
            ) = result?;
            return Ok((executed_block, response, never_reject_origins));
        }
    }
}

/// Returns the items in `ids` that are not yet present in `already_downloaded`.
fn filter_new<T: Clone + Eq + std::hash::Hash>(
    ids: &[T],
    already_downloaded: &HashSet<T>,
) -> Vec<T> {
    ids.iter()
        .filter(|id| !already_downloaded.contains(*id))
        .cloned()
        .collect()
}

/// Per-call deduplication for an event-download retry loop. Holds the set of
/// events the loop has already downloaded and the [`Client`] used to fetch new
/// ones — call `download_new` each time the inner operation reports
/// `EventsNotFound` and continue the loop only if it returns `true`.
pub(crate) struct EventSetDownloader<'a, Env: Environment> {
    client: &'a Client<Env>,
    downloaded: HashSet<EventId>,
}

impl<'a, Env: Environment> EventSetDownloader<'a, Env> {
    pub(crate) fn new(client: &'a Client<Env>) -> Self {
        Self {
            client,
            downloaded: HashSet::new(),
        }
    }

    /// If any of `event_ids` haven't been downloaded yet, fetches the publisher
    /// certificates that contain them and returns `true`. Returns `false`
    /// (without downloading) if every reported event has already been
    /// downloaded — that prevents an infinite retry loop when the events are
    /// genuinely unavailable.
    pub(crate) async fn download_new(
        &mut self,
        event_ids: &[EventId],
    ) -> Result<bool, chain_client::Error> {
        let new_events = filter_new(event_ids, &self.downloaded);
        if new_events.is_empty() {
            return Ok(false);
        }
        Box::pin(self.client.download_certificates_for_events(&new_events)).await?;
        self.downloaded.extend(new_events);
        Ok(true)
    }
}

/// The clock backing the environment's storage.
///
/// All client-side coordination logic (retries, backoff, request TTLs, the notification
/// circuit breaker) reads time through this clock rather than the wall clock, so it can be
/// driven deterministically by a simulated clock (e.g. `TestClock`) in tests.
pub(crate) type ClockOf<Env> = <<Env as Environment>::Storage as linera_storage::Storage>::Clock;

/// Races `operation` across peers with a hedged, **failure-responsive** fan-out, returning the
/// first `Ok` (or every error if all attempts fail).
///
/// `first_peer` is tried immediately. `next_peer` then supplies additional peers, each started
/// either *immediately* when an in-flight attempt fails (no point waiting once we know an attempt
/// failed), or after the in-flight attempt has run for `hedge_schedule(started)` without answering
/// — hedging against a slow peer by racing an extra request; the slow attempt is **not** cancelled.
/// `hedge_schedule` maps the number of attempts started so far to the delay before starting the
/// next one (e.g. `|k| delay * k` for a linearly-growing stagger). All sleeping is on `clock`, so
/// this runs in (possibly simulated) time; freezing the clock disables the slow-peer hedge while
/// still advancing on every failure.
pub(crate) async fn hedged_fan_out<Peer, T, Err, NextPeer, NextFut, Op, OpFut>(
    first_peer: Peer,
    mut next_peer: NextPeer,
    operation: Op,
    hedge_schedule: impl Fn(usize) -> Duration,
    // `Sync` so `clock.sleep_for(..)` yields a `Send` future, as required when this fan-out is
    // spawned (e.g. background certificate downloads). All storage clocks are `Sync`.
    clock: &(impl linera_storage::Clock + Sync),
) -> Result<T, Vec<Err>>
where
    NextPeer: FnMut() -> NextFut,
    NextFut: Future<Output = Option<Peer>>,
    Op: Fn(Peer) -> OpFut,
    OpFut: Future<Output = Result<T, Err>>,
{
    use futures::future::{select, Either};

    let mut in_flight = FuturesUnordered::new();
    let mut errors = vec![];
    let mut started = 0usize;
    let arm = |started: usize| clock.sleep_for(hedge_schedule(started));

    in_flight.push(operation(first_peer));
    started += 1;
    let mut hedge = arm(started);

    loop {
        if in_flight.is_empty() {
            // Nothing running: start the next peer, or stop if there are none left.
            match next_peer().await {
                Some(peer) => {
                    in_flight.push(operation(peer));
                    started += 1;
                    hedge = arm(started);
                }
                None => return Err(errors),
            }
            continue;
        }
        match select(in_flight.next(), hedge).await {
            // An attempt succeeded.
            Either::Left((Some(Ok(value)), _)) => return Ok(value),
            // An attempt failed: try the next peer right away rather than waiting out the hedge.
            Either::Left((Some(Err(error)), pending_hedge)) => {
                errors.push(error);
                hedge = pending_hedge;
                if let Some(peer) = next_peer().await {
                    in_flight.push(operation(peer));
                    started += 1;
                    hedge = arm(started);
                }
            }
            // All in-flight attempts drained; the loop top decides what to do next.
            Either::Left((None, pending_hedge)) => hedge = pending_hedge,
            // An attempt is slow: hedge by starting another peer, if any remain.
            Either::Right(((), _)) => match next_peer().await {
                Some(peer) => {
                    in_flight.push(operation(peer));
                    started += 1;
                    hedge = arm(started);
                }
                None => break,
            },
        }
    }

    // No more peers to start; just wait for the remaining in-flight attempts.
    while let Some(result) = in_flight.next().await {
        match result {
            Ok(value) => return Ok(value),
            Err(error) => errors.push(error),
        }
    }
    Err(errors)
}

/// Performs `f` on the validators with a hedged, staggered fan-out (see [`hedged_fan_out`]),
/// returning the first `Ok` result, or every `(validator, error)` pair if all of them fail.
///
/// The hedge before starting the n-th validator grows quadratically with `n`, so we stay
/// reluctant to fan out to many validators at once when one is merely slow.
async fn communicate_concurrently<A, E, F, R, V>(
    nodes: &[RemoteNode<A>],
    f: F,
    hedge_delay: Duration,
    clock: &(impl linera_storage::Clock + Sync),
) -> Result<V, Vec<(ValidatorPublicKey, E)>>
where
    F: Clone + FnOnce(RemoteNode<A>) -> R,
    RemoteNode<A>: Clone,
    R: Future<Output = Result<V, E>>,
{
    let mut nodes = nodes.to_vec();
    nodes.shuffle(&mut rand::thread_rng());
    let mut nodes = nodes.into_iter();
    let Some(first_peer) = nodes.next() else {
        return Err(vec![]);
    };
    hedged_fan_out(
        first_peer,
        move || std::future::ready(nodes.next()),
        |node: RemoteNode<A>| {
            let fun = f.clone();
            async move {
                let public_key = node.public_key;
                fun(node).await.map_err(|err| (public_key, err))
            }
        },
        |started| {
            let k = u32::try_from(started).unwrap_or(u32::MAX);
            hedge_delay.saturating_mul(k).saturating_mul(k)
        },
        clock,
    )
    .await
}

/// Wrapper for `AbortHandle` that aborts when its dropped.
#[must_use]
pub struct AbortOnDrop(pub AbortHandle);

impl Drop for AbortOnDrop {
    #[instrument(level = "trace", skip(self))]
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A pending proposed block, together with its published blobs.
#[derive(Clone, Serialize, Deserialize)]
pub struct PendingProposal {
    /// The proposed block.
    pub block: ProposedBlock,
    /// The blobs published by the proposed block.
    pub blobs: Vec<Blob>,
    /// The execution outcome from the AutoRetry execution, used as a sanity check
    /// against the committed execution outcome.
    #[serde(default)]
    pub auto_retry_outcome: Option<BlockExecutionOutcome>,
    /// The round in which this proposal was first submitted, if any.
    #[serde(default)]
    pub round: Option<Round>,
}

enum ReceiveCertificateMode {
    NeedsCheck,
    AlreadyChecked,
}

/// Creates a compressed Contract, Service and bytecode, plus an optional
/// `ApplicationFormats` blob built from the BCS-encoded `Formats` description
/// bytes.
#[cfg(not(target_arch = "wasm32"))]
pub async fn create_bytecode_blobs(
    contract: Bytecode,
    service: Bytecode,
    vm_runtime: VmRuntime,
    formats: Option<Vec<u8>>,
) -> (Vec<Blob>, ModuleId) {
    let formats_blob = formats.map(Blob::new_application_formats);
    let formats_blob_hash = formats_blob.as_ref().map(|blob| blob.id().hash);
    let (mut blobs, module_id) = match vm_runtime {
        VmRuntime::Wasm => {
            let (compressed_contract, compressed_service) =
                tokio::task::spawn_blocking(move || (contract.compress(), service.compress()))
                    .await
                    .expect("Compression should not panic");
            let contract_blob = Blob::new_contract_bytecode(compressed_contract);
            let service_blob = Blob::new_service_bytecode(compressed_service);
            let module_id = ModuleId::new_with_formats(
                contract_blob.id().hash,
                service_blob.id().hash,
                vm_runtime,
                formats_blob_hash,
            );
            (vec![contract_blob, service_blob], module_id)
        }
        VmRuntime::Evm => {
            let compressed_contract = contract.compress();
            let evm_contract_blob = Blob::new_evm_bytecode(compressed_contract);
            let module_id = ModuleId::new_with_formats(
                evm_contract_blob.id().hash,
                evm_contract_blob.id().hash,
                vm_runtime,
                formats_blob_hash,
            );
            (vec![evm_contract_blob], module_id)
        }
    };
    if let Some(blob) = formats_blob {
        blobs.push(blob);
    }
    (blobs, module_id)
}

#[cfg(test)]
mod chain_modes_tests {
    use std::collections::BTreeSet;

    use linera_base::{crypto::CryptoHash, identifiers::ChainId};

    use super::{ChainModes, ListeningMode};

    /// `remove_mode` reports the previous mode (and `None` for an absent chain), and only rehashes
    /// the memoized fully-tracked set when the removed chain was `FullChain` — removing an
    /// `EventsOnly` chain leaves that set untouched.
    #[test]
    fn remove_mode_updates_full_set_only_for_full_chains() {
        let mut modes = ChainModes::default();
        let full = ChainId(CryptoHash::test_hash("full"));
        let events_only = ChainId(CryptoHash::test_hash("events-only"));
        modes.extend_mode(full, ListeningMode::FullChain);
        modes.extend_mode(events_only, ListeningMode::EventsOnly(BTreeSet::new()));

        let full_hash_before = modes.full().hash();
        // Removing the events-only chain returns its mode but doesn't touch the fully-tracked set.
        assert!(matches!(
            modes.remove_mode(&events_only),
            Some(ListeningMode::EventsOnly(_))
        ));
        assert!(modes.get(&events_only).is_none());
        // Removing it again is a no-op.
        assert!(modes.remove_mode(&events_only).is_none());
        assert_eq!(modes.full().hash(), full_hash_before);
        assert_eq!(modes.full().inner().0, BTreeSet::from([full]));

        // Removing the full chain empties and rehashes the fully-tracked set.
        assert_eq!(modes.remove_mode(&full), Some(ListeningMode::FullChain));
        assert_ne!(modes.full().hash(), full_hash_before);
        assert!(modes.full().inner().0.is_empty());
    }
}

#[cfg(test)]
mod communicate_concurrently_tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use linera_base::crypto::ValidatorKeypair;
    use linera_storage::TestClock;

    use super::*;

    fn test_node() -> RemoteNode<()> {
        RemoteNode {
            public_key: ValidatorKeypair::generate().public_key,
            node: (),
        }
    }

    /// When every node fails, all errors are collected and we return promptly — crucially without
    /// ever waiting out the hedge delay, so the frozen clock never advances.
    #[tokio::test]
    async fn does_not_wait_after_failures() {
        let clock = TestClock::new();
        let nodes: Vec<_> = (0..5).map(|_| test_node()).collect();
        let calls = Arc::new(AtomicUsize::new(0));
        let result: Result<(), Vec<(ValidatorPublicKey, &str)>> = communicate_concurrently(
            &nodes,
            {
                let calls = calls.clone();
                move |_node| {
                    let calls = calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Err("unavailable")
                    }
                }
            },
            Duration::from_secs(30),
            &clock,
        )
        .await;
        assert_eq!(result.unwrap_err().len(), 5);
        assert_eq!(calls.load(Ordering::SeqCst), 5);
        // The hedge timer was never needed (every node failed), so virtual time did not advance.
        assert_eq!(clock.current_time(), Timestamp::from(0));
    }

    /// A single working node is reached by failing over the others — again without advancing the
    /// clock, regardless of where the shuffle places it.
    #[tokio::test]
    async fn fails_over_to_a_working_node() {
        let clock = TestClock::new();
        let nodes: Vec<_> = (0..5).map(|_| test_node()).collect();
        let working = nodes[3].public_key;
        let result: Result<u32, Vec<(ValidatorPublicKey, &str)>> = communicate_concurrently(
            &nodes,
            move |node| async move {
                if node.public_key == working {
                    Ok(42)
                } else {
                    Err("unavailable")
                }
            },
            Duration::from_secs(30),
            &clock,
        )
        .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(clock.current_time(), Timestamp::from(0));
    }

    // The tests below exercise the `hedged_fan_out` engine directly — peers are modelled as
    // `usize`, the operation outcome is chosen per peer, and "slowness" is made controllable with
    // notify gates. Driving the (virtual) clock by hand lets us assert the hedging behaviour that
    // was impossible to test deterministically on the wall clock.

    /// Hands out peers `1..n`; `hedged_fan_out` already holds `first_peer = 0`.
    fn peer_source(n: usize) -> impl FnMut() -> std::future::Ready<Option<usize>> {
        let mut next = 1usize;
        move || {
            let peer = (next < n).then_some(next);
            next += 1;
            std::future::ready(peer)
        }
    }

    /// A slow first peer is hedged by starting the next one, but is **not** cancelled: when it
    /// finally answers `Ok`, that answer still wins the race.
    #[tokio::test]
    async fn slow_first_peer_is_hedged_but_not_cancelled() {
        let clock = TestClock::new();
        let delay = Duration::from_secs(1);
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let release0 = Arc::new(tokio::sync::Notify::new());
        let gates = [release0.clone(), Arc::new(tokio::sync::Notify::new())];

        let operation = {
            let order = order.clone();
            move |peer: usize| {
                let started_tx = started_tx.clone();
                let order = order.clone();
                let gate = gates[peer].clone();
                async move {
                    order.lock().unwrap().push(peer);
                    started_tx.send(peer).unwrap();
                    gate.notified().await;
                    if peer == 0 {
                        Ok::<u32, &str>(42)
                    } else {
                        Err("slow loser")
                    }
                }
            }
        };

        let fan = tokio::spawn({
            let clock = clock.clone();
            async move {
                hedged_fan_out(
                    0usize,
                    peer_source(2),
                    operation,
                    move |k| delay * u32::try_from(k).unwrap_or(u32::MAX),
                    &clock,
                )
                .await
            }
        });

        // Peer 0 starts immediately; the hedge for peer 1 is armed at `delay`.
        assert_eq!(started_rx.recv().await, Some(0));
        // Firing the hedge starts peer 1 while peer 0 is still in flight.
        clock.add(TimeDelta::from_duration(delay));
        assert_eq!(started_rx.recv().await, Some(1));
        // Peer 0 was not cancelled when the hedge fired: releasing it now still wins the race.
        release0.notify_one();
        assert_eq!(fan.await.unwrap(), Ok(42));
        assert_eq!(*order.lock().unwrap(), vec![0, 1]);
    }

    /// With every peer hanging, only the (virtual) clock advances the fan-out, and it starts each
    /// peer on the cumulative hedge schedule — here the quadratic one `communicate_concurrently`
    /// uses.
    #[tokio::test]
    async fn hedge_schedule_determines_start_times() {
        let clock = TestClock::new();
        let unit = Duration::from_secs(1);
        let starts = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();

        let operation = {
            let starts = starts.clone();
            let clock = clock.clone();
            move |peer: usize| {
                let started_tx = started_tx.clone();
                let starts = starts.clone();
                let clock = clock.clone();
                async move {
                    starts.lock().unwrap().push((peer, clock.current_time()));
                    started_tx.send(peer).unwrap();
                    std::future::pending::<()>().await;
                    Ok::<u32, &str>(0)
                }
            }
        };

        let fan = tokio::spawn({
            let clock = clock.clone();
            async move {
                hedged_fan_out(
                    0usize,
                    peer_source(4),
                    operation,
                    move |k| {
                        let k = u32::try_from(k).unwrap_or(u32::MAX);
                        unit * k * k
                    },
                    &clock,
                )
                .await
            }
        });

        // peer 0 at t=0; hedge(1)=1·unit, hedge(2)=4·unit, hedge(3)=9·unit, applied cumulatively.
        assert_eq!(started_rx.recv().await, Some(0));
        clock.add(TimeDelta::from_duration(unit));
        assert_eq!(started_rx.recv().await, Some(1));
        clock.add(TimeDelta::from_duration(unit * 4));
        assert_eq!(started_rx.recv().await, Some(2));
        clock.add(TimeDelta::from_duration(unit * 9));
        assert_eq!(started_rx.recv().await, Some(3));

        // Cumulative virtual start times: 0, 1s, 1+4=5s, 5+9=14s (Timestamp is in microseconds).
        assert_eq!(
            *starts.lock().unwrap(),
            vec![
                (0, Timestamp::from(0)),
                (1, Timestamp::from(1_000_000)),
                (2, Timestamp::from(5_000_000)),
                (3, Timestamp::from(14_000_000)),
            ]
        );
        fan.abort();
    }

    /// Freezing the clock disables the slow-peer hedge entirely: a slow first peer never causes a
    /// second one to start, yet the fan-out still completes when that peer eventually answers.
    #[tokio::test]
    async fn frozen_clock_disables_the_hedge() {
        let clock = TestClock::new();
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let release0 = Arc::new(tokio::sync::Notify::new());

        let operation = {
            let release0 = release0.clone();
            move |peer: usize| {
                let started_tx = started_tx.clone();
                let release0 = release0.clone();
                async move {
                    started_tx.send(peer).unwrap();
                    if peer == 0 {
                        release0.notified().await;
                        Ok::<u32, &str>(7)
                    } else {
                        // If a hedge ever wrongly fired, this peer would win instead.
                        Ok(99)
                    }
                }
            }
        };

        let fan = tokio::spawn({
            let clock = clock.clone();
            async move {
                hedged_fan_out(
                    0usize,
                    peer_source(2),
                    operation,
                    move |k| Duration::from_secs(1) * u32::try_from(k).unwrap_or(u32::MAX),
                    &clock,
                )
                .await
            }
        });

        assert_eq!(started_rx.recv().await, Some(0));
        // The clock is frozen, so the hedge can never fire: peer 1 must not start.
        tokio::task::yield_now().await;
        assert!(
            started_rx.try_recv().is_err(),
            "the hedge must not fire while the clock is frozen"
        );
        // Peer 0 still wins, without the clock ever advancing.
        release0.notify_one();
        assert_eq!(fan.await.unwrap(), Ok(7));
        assert_eq!(clock.current_time(), Timestamp::from(0));
    }

    /// On failure the next peer starts immediately rather than waiting out the (here, huge) hedge
    /// delay — failover stays responsive even with a hedge already armed.
    #[tokio::test]
    async fn failure_fails_over_without_waiting_out_the_hedge() {
        let clock = TestClock::new();
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let release0 = Arc::new(tokio::sync::Notify::new());

        let operation = {
            let release0 = release0.clone();
            move |peer: usize| {
                let started_tx = started_tx.clone();
                let release0 = release0.clone();
                async move {
                    started_tx.send(peer).unwrap();
                    match peer {
                        0 => {
                            release0.notified().await;
                            Err::<u32, &str>("dead")
                        }
                        1 => Err("dead"),
                        _ => Ok(55),
                    }
                }
            }
        };

        let fan = tokio::spawn({
            let clock = clock.clone();
            async move {
                hedged_fan_out(
                    0usize,
                    peer_source(3),
                    operation,
                    // A hedge so large that any waiting would be obvious in virtual time.
                    move |k| Duration::from_secs(100) * u32::try_from(k).unwrap_or(u32::MAX),
                    &clock,
                )
                .await
            }
        });

        assert_eq!(started_rx.recv().await, Some(0));
        // Peer 0 fails: peer 1 starts at once, then peer 1 fails and peer 2 starts and succeeds.
        release0.notify_one();
        assert_eq!(started_rx.recv().await, Some(1));
        assert_eq!(started_rx.recv().await, Some(2));
        assert_eq!(fan.await.unwrap(), Ok(55));
        // None of the 100s hedge delays were ever waited out.
        assert_eq!(clock.current_time(), Timestamp::from(0));
    }
}

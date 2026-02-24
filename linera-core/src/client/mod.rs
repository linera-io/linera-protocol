// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{hash_map, BTreeMap, BTreeSet, HashMap},
    convert::Infallible,
    iter,
    sync::{Arc, RwLock},
};

use chain_client_state::ChainClientState;
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
        ChainDescription, Epoch, MessagePolicy, Round, TimeDelta, Timestamp,
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
    data_types::{
        BlockProposal, BundleExecutionPolicy, BundleFailurePolicy, ChainAndHeight, IncomingBundle,
        LiteVote, ProposedBlock, Transaction,
    },
    manager::LockingBlock,
    types::{
        Block, CertificateValue, ConfirmedBlock, ConfirmedBlockCertificate, GenericCertificate,
        LiteCertificate, Timeout, TimeoutCertificate, ValidatedBlock, ValidatedBlockCertificate,
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
use linera_storage::{Clock as _, ResultReadCertificates, Storage as _};
use linera_views::ViewError;
use rand::prelude::SliceRandom as _;
use received_log::ReceivedLogs;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{mpsc, OwnedRwLockReadGuard};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, trace, warn, Instrument as _};
use validator_trackers::ValidatorTrackers;

use crate::{
    data_types::{ChainInfo, ChainInfoQuery, ChainInfoResponse, ClientOutcome, RoundTimeout},
    environment::{wallet::Wallet as _, Environment},
    local_node::{LocalChainInfoExt as _, LocalNodeClient, LocalNodeError},
    node::{
        CrossChainMessageDelivery, NodeError, NotificationStream, ValidatorNode,
        ValidatorNodeProvider as _,
    },
    notifier::{ChannelNotifier, Notifier as _},
    remote_node::RemoteNode,
    updater::{communicate_with_quorum, CommunicateAction, CommunicationError, ValidatorUpdater},
    worker::{Notification, ProcessableCertificate, Reason, WorkerError, WorkerState},
    CHAIN_INFO_MAX_RECEIVED_LOG_ENTRIES,
};

mod chain_client_state;
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

    use linera_base::prometheus_util::{exponential_bucket_latencies, register_histogram_vec};
    use prometheus::HistogramVec;

    pub static PROCESS_INBOX_WITHOUT_PREPARE_LATENCY: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "process_inbox_latency",
                "process_inbox latency",
                &[],
                exponential_bucket_latencies(500.0),
            )
        });

    pub static PREPARE_CHAIN_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "prepare_chain_latency",
            "prepare_chain latency",
            &[],
            exponential_bucket_latencies(500.0),
        )
    });

    pub static SYNCHRONIZE_CHAIN_STATE_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "synchronize_chain_state_latency",
            "synchronize_chain_state latency",
            &[],
            exponential_bucket_latencies(500.0),
        )
    });

    pub static EXECUTE_BLOCK_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "execute_block_latency",
            "execute_block latency",
            &[],
            exponential_bucket_latencies(500.0),
        )
    });

    pub static FIND_RECEIVED_CERTIFICATES_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "find_received_certificates_latency",
            "find_received_certificates latency",
            &[],
            exponential_bucket_latencies(500.0),
        )
    });
}

/// Defines what type of notifications we should process for a chain:
/// - do we fully participate in consensus and download sender chains?
/// - or do we only follow the chain's blocks without participating?
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListeningMode {
    /// Listen to everything: all blocks for the chain and all blocks from sender chains,
    /// and participate in rounds.
    FullChain,
    /// Listen to all blocks for the chain, but don't download sender chain blocks or participate
    /// in rounds. Use this when interested in the chain's state but not intending to propose
    /// blocks (e.g., because we're not a chain owner).
    FollowChain,
}

impl PartialOrd for ListeningMode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;
        match (self, other) {
            (ListeningMode::FullChain, ListeningMode::FullChain) => Some(Ordering::Equal),
            (ListeningMode::FullChain, _) => Some(Ordering::Greater),
            (_, ListeningMode::FullChain) => Some(Ordering::Less),
            (ListeningMode::FollowChain, ListeningMode::FollowChain) => Some(Ordering::Equal),
        }
    }
}

impl ListeningMode {
    /// Returns whether a notification with this reason should be processed under this listening
    /// mode.
    pub fn is_relevant(&self, reason: &Reason) -> bool {
        match (reason, self) {
            // FullChain processes everything.
            (_, ListeningMode::FullChain) => true,
            // FollowChain processes new blocks on the chain itself (including events embedded in
            // NewBlock).
            (Reason::NewBlock { .. }, ListeningMode::FollowChain) => true,
            (_, ListeningMode::FollowChain) => false,
        }
    }

    pub fn extend(&mut self, other: Option<ListeningMode>) {
        match (self, other) {
            (_, None) => (),
            (ListeningMode::FullChain, _) => (),
            (mode, Some(ListeningMode::FullChain)) => {
                *mode = ListeningMode::FullChain;
            }
            (ListeningMode::FollowChain, _) => (),
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
}

/// A builder that creates [`ChainClient`]s which share the cache and notifiers.
pub struct Client<Env: Environment> {
    environment: Env,
    /// Local node to manage the execution state and the local storage of the chains that we are
    /// tracking.
    pub local_node: LocalNodeClient<Env::Storage>,
    /// Manages the requests sent to validator nodes.
    requests_scheduler: RequestsScheduler<Env>,
    /// The admin chain ID.
    admin_chain_id: ChainId,
    /// Chains that should be tracked by the client, along with their listening mode.
    /// The presence of a chain in this map means it is tracked by the local node.
    chain_modes: Arc<RwLock<BTreeMap<ChainId, ListeningMode>>>,
    /// References to clients waiting for chain notifications.
    notifier: Arc<ChannelNotifier<Notification>>,
    /// Chain state for the managed chains.
    chains: papaya::HashMap<ChainId, ChainClientState>,
    /// Configuration options.
    options: ChainClientOptions,
}

impl<Env: Environment> Client<Env> {
    /// Creates a new `Client` with a new cache and notifiers.
    #[expect(clippy::too_many_arguments)]
    #[instrument(level = "trace", skip_all)]
    pub fn new(
        environment: Env,
        admin_chain_id: ChainId,
        long_lived_services: bool,
        chain_modes: impl IntoIterator<Item = (ChainId, ListeningMode)>,
        name: impl Into<String>,
        chain_worker_ttl: Duration,
        sender_chain_worker_ttl: Duration,
        options: ChainClientOptions,
        requests_scheduler_config: requests_scheduler::RequestsSchedulerConfig,
    ) -> Self {
        let chain_modes = Arc::new(RwLock::new(chain_modes.into_iter().collect()));
        let state = WorkerState::new_for_client(
            name.into(),
            environment.storage().clone(),
            chain_modes.clone(),
        )
        .with_long_lived_services(long_lived_services)
        .with_allow_inactive_chains(true)
        .with_allow_messages_from_deprecated_epochs(true)
        .with_chain_worker_ttl(chain_worker_ttl)
        .with_sender_chain_worker_ttl(sender_chain_worker_ttl);
        let local_node = LocalNodeClient::new(state);
        let requests_scheduler = RequestsScheduler::new(vec![], requests_scheduler_config);

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

    /// Returns the storage client used by this client's local node.
    pub fn storage_client(&self) -> &Env::Storage {
        self.environment.storage()
    }

    pub fn validator_node_provider(&self) -> &Env::Network {
        self.environment.network()
    }

    /// Returns a reference to the client's [`Signer`][crate::environment::Signer].
    #[instrument(level = "trace", skip(self))]
    pub fn signer(&self) -> &Env::Signer {
        self.environment.signer()
    }

    /// Returns whether the signer has a key for the given owner.
    pub async fn has_key_for(&self, owner: &AccountOwner) -> Result<bool, ChainClientError> {
        self.signer()
            .contains_key(owner)
            .await
            .map_err(ChainClientError::signer_failure)
    }

    /// Returns a reference to the client's [`Wallet`][crate::environment::Wallet].
    pub fn wallet(&self) -> &Env::Wallet {
        self.environment.wallet()
    }

    /// Returns whether the given chain is in follow-only mode (no owner key in the wallet).
    ///
    /// If the chain is not in the wallet, returns `true` since we don't have an owner key
    /// for it.
    async fn is_chain_follow_only(&self, chain_id: ChainId) -> bool {
        match self.wallet().get(chain_id).await {
            Ok(Some(chain)) => chain.owner.is_none(),
            // Chain not in wallet or error: treat as follow-only.
            Ok(None) | Err(_) => true,
        }
    }

    /// Extends the listening mode for a chain, combining with the existing mode if present.
    /// Returns the resulting mode.
    #[instrument(level = "trace", skip(self))]
    pub fn extend_chain_mode(&self, chain_id: ChainId, mode: ListeningMode) -> ListeningMode {
        let mut chain_modes = self
            .chain_modes
            .write()
            .expect("Panics should not happen while holding a lock to `chain_modes`");
        let entry = chain_modes.entry(chain_id).or_insert(mode.clone());
        entry.extend(Some(mode));
        entry.clone()
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
    #[instrument(level = "trace", skip_all, fields(chain_id, next_block_height))]
    pub fn create_chain_client(
        self: &Arc<Self>,
        chain_id: ChainId,
        block_hash: Option<CryptoHash>,
        next_block_height: BlockHeight,
        pending_proposal: Option<PendingProposal>,
        preferred_owner: Option<AccountOwner>,
        timing_sender: Option<mpsc::UnboundedSender<(u64, TimingType)>>,
    ) -> ChainClient<Env> {
        // If the entry already exists we assume that the entry is more up to date than
        // the arguments: If they were read from the wallet file, they might be stale.
        self.chains
            .pin()
            .get_or_insert_with(chain_id, || ChainClientState::new(pending_proposal.clone()));

        ChainClient {
            client: self.clone(),
            chain_id,
            options: self.options.clone(),
            preferred_owner,
            initial_block_hash: block_hash,
            initial_next_block_height: next_block_height,
            timing_sender,
        }
    }

    /// Fetches the chain description blob if needed, and returns the chain info.
    async fn fetch_chain_info(
        &self,
        chain_id: ChainId,
        validators: &[RemoteNode<Env::ValidatorNode>],
    ) -> Result<Box<ChainInfo>, ChainClientError> {
        match self.local_node.chain_info(chain_id).await {
            Ok(info) => Ok(info),
            Err(LocalNodeError::BlobsNotFound(blob_ids)) => {
                // Make sure the admin chain is up to date.
                Box::pin(self.synchronize_chain_state(self.admin_chain_id)).await?;
                // If the chain is missing then the error is a WorkerError
                // and so a BlobsNotFound
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
    ) -> Result<Box<ChainInfo>, ChainClientError> {
        let mut validators = self.validator_nodes().await?;
        // Sequentially try each validator in random order.
        validators.shuffle(&mut rand::thread_rng());
        let mut info = Box::pin(self.fetch_chain_info(chain_id, &validators)).await?;
        for remote_node in validators {
            if target_next_block_height <= info.next_block_height {
                return Ok(info);
            }
            match self
                .download_certificates_from(&remote_node, chain_id, target_next_block_height)
                .await
            {
                Err(error) => info!(
                    remote_node = remote_node.address(),
                    %error,
                    "failed to download certificates from validator",
                ),
                Ok(Some(new_info)) => info = new_info,
                Ok(None) => {}
            }
        }
        ensure!(
            target_next_block_height <= info.next_block_height,
            ChainClientError::CannotDownloadCertificates {
                chain_id,
                target_next_block_height,
            }
        );
        Ok(info)
    }

    /// Downloads and processes all certificates up to (excluding) the specified height from the
    /// given validator.
    #[instrument(level = "trace", skip_all)]
    async fn download_certificates_from(
        &self,
        remote_node: &RemoteNode<Env::ValidatorNode>,
        chain_id: ChainId,
        stop: BlockHeight,
    ) -> Result<Option<Box<ChainInfo>>, ChainClientError> {
        let mut last_info = None;
        // First load any blocks from local storage, if available.
        let chain_info = self.local_node.chain_info(chain_id).await?;
        let mut next_height = chain_info.next_block_height;
        let hashes = self
            .local_node
            .get_preprocessed_block_hashes(chain_id, next_height, stop)
            .await?;
        let certificates = self.storage_client().read_certificates(&hashes).await?;
        let certificates = match ResultReadCertificates::new(certificates, hashes) {
            ResultReadCertificates::Certificates(certificates) => certificates,
            ResultReadCertificates::InvalidHashes(hashes) => {
                return Err(ChainClientError::ReadCertificatesError(hashes))
            }
        };
        for certificate in certificates {
            last_info = Some(self.handle_certificate(certificate).await?.info);
        }
        // Now download the rest in batches from the remote node.
        while next_height < stop {
            // TODO(#2045): Analyze network errors instead of using a fixed batch size.
            let limit = u64::from(stop)
                .checked_sub(u64::from(next_height))
                .ok_or(ArithmeticError::Overflow)?
                .min(self.options.certificate_download_batch_size);

            let certificates = self
                .requests_scheduler
                .download_certificates(remote_node, chain_id, next_height, limit)
                .await?;
            let Some(info) = self.process_certificates(remote_node, certificates).await? else {
                break;
            };
            assert!(info.next_block_height > next_height);
            next_height = info.next_block_height;
            last_info = Some(info);
        }
        Ok(last_info)
    }

    async fn download_blobs(
        &self,
        remote_nodes: &[RemoteNode<Env::ValidatorNode>],
        blob_ids: &[BlobId],
    ) -> Result<(), ChainClientError> {
        let blobs = &self
            .requests_scheduler
            .download_blobs(remote_nodes, blob_ids, self.options.blob_download_timeout)
            .await?
            .ok_or_else(|| {
                ChainClientError::RemoteNodeError(NodeError::BlobsNotFound(blob_ids.to_vec()))
            })?;
        self.local_node.store_blobs(blobs).await.map_err(Into::into)
    }

    /// Tries to process all the certificates, requesting any missing blobs from the given node.
    /// Returns the chain info of the last successfully processed certificate.
    #[instrument(level = "trace", skip_all)]
    async fn process_certificates(
        &self,
        remote_node: &RemoteNode<Env::ValidatorNode>,
        certificates: Vec<ConfirmedBlockCertificate>,
    ) -> Result<Option<Box<ChainInfo>>, ChainClientError> {
        let mut info = None;
        let required_blob_ids: Vec<_> = certificates
            .iter()
            .flat_map(|certificate| certificate.value().required_blob_ids())
            .collect();

        match self
            .local_node
            .read_blob_states_from_storage(&required_blob_ids)
            .await
        {
            Err(LocalNodeError::BlobsNotFound(blob_ids)) => {
                self.download_blobs(&[remote_node.clone()], &blob_ids)
                    .await?;
            }
            x => {
                x?;
            }
        }

        for certificate in certificates {
            info = Some(
                match self.handle_certificate(certificate.clone()).await {
                    Err(LocalNodeError::BlobsNotFound(blob_ids)) => {
                        self.download_blobs(&[remote_node.clone()], &blob_ids)
                            .await?;
                        self.handle_certificate(certificate).await?
                    }
                    x => x?,
                }
                .info,
            );
        }

        // Done with all certificates.
        Ok(info)
    }

    async fn handle_certificate<T: ProcessableCertificate>(
        &self,
        certificate: GenericCertificate<T>,
    ) -> Result<ChainInfoResponse, LocalNodeError> {
        self.local_node
            .handle_certificate(certificate, &self.notifier)
            .await
    }

    async fn chain_info_with_committees(
        &self,
        chain_id: ChainId,
    ) -> Result<Box<ChainInfo>, LocalNodeError> {
        let query = ChainInfoQuery::new(chain_id).with_committees();
        let info = self.local_node.handle_chain_info_query(query).await?.info;
        Ok(info)
    }

    /// Obtains all the committees trusted by any of the given chains. Also returns the highest
    /// of their epochs.
    #[instrument(level = "trace", skip_all)]
    async fn admin_committees(
        &self,
    ) -> Result<(Epoch, BTreeMap<Epoch, Committee>), LocalNodeError> {
        let info = self.chain_info_with_committees(self.admin_chain_id).await?;
        Ok((info.epoch, info.into_committees()?))
    }

    /// Obtains the committee for the latest epoch on the admin chain.
    pub async fn admin_committee(&self) -> Result<(Epoch, Committee), LocalNodeError> {
        let info = self.chain_info_with_committees(self.admin_chain_id).await?;
        Ok((info.epoch, info.into_current_committee()?))
    }

    /// Obtains the validators for the latest epoch.
    async fn validator_nodes(
        &self,
    ) -> Result<Vec<RemoteNode<Env::ValidatorNode>>, ChainClientError> {
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
    ) -> Result<Blob, ChainClientError> {
        let chain_desc_id = BlobId::new(chain_id.0, BlobType::ChainDescription);
        let blob = self
            .local_node
            .storage_client()
            .read_blob(chain_desc_id)
            .await?;
        if let Some(blob) = blob {
            // We have the blob - return it.
            return Ok(blob);
        }
        // Recover history from the current validators, according to the admin chain.
        Box::pin(self.synchronize_chain_state(self.admin_chain_id)).await?;
        let nodes = self.validator_nodes().await?;
        Ok(self
            .update_local_node_with_blobs_from(vec![chain_desc_id], &nodes)
            .await?
            .pop()
            .unwrap()) // Returns exactly as many blobs as passed-in IDs.
    }

    /// Ensures that the client has the `ChainDescription` blob corresponding to this
    /// client's `ChainId`, and returns the chain description.
    pub async fn get_chain_description(
        &self,
        chain_id: ChainId,
    ) -> Result<ChainDescription, ChainClientError> {
        let blob = self.get_chain_description_blob(chain_id).await?;
        Ok(bcs::from_bytes(blob.bytes())?)
    }

    /// Updates the latest block and next block height and round information from the chain info.
    #[instrument(level = "trace", skip_all, fields(chain_id = format!("{:.8}", info.chain_id)))]
    fn update_from_info(&self, info: &ChainInfo) {
        self.chains.pin().update(info.chain_id, |state| {
            let mut state = state.clone_for_update_unchecked();
            state.update_from_info(info);
            state
        });
    }

    /// Handles the certificate in the local node and the resulting notifications.
    #[instrument(level = "trace", skip_all)]
    async fn process_certificate<T: ProcessableCertificate>(
        &self,
        certificate: Box<GenericCertificate<T>>,
    ) -> Result<(), LocalNodeError> {
        let info = self.handle_certificate(*certificate).await?.info;
        self.update_from_info(&info);
        Ok(())
    }

    /// Submits a validated block for finalization and returns the confirmed block certificate.
    #[instrument(level = "trace", skip_all)]
    async fn finalize_block(
        self: &Arc<Self>,
        committee: &Committee,
        certificate: ValidatedBlockCertificate,
    ) -> Result<ConfirmedBlockCertificate, ChainClientError> {
        debug!(round = %certificate.round, "Submitting block for confirmation");
        let hashed_value = ConfirmedBlock::new(certificate.inner().block().clone());
        let finalize_action = CommunicateAction::FinalizeBlock {
            certificate: Box::new(certificate),
            delivery: self.options.cross_chain_message_delivery,
        };
        let certificate = self
            .communicate_chain_action(committee, finalize_action, hashed_value)
            .await?;
        self.receive_certificate_with_checked_signatures(certificate.clone())
            .await?;
        Ok(certificate)
    }

    /// Submits a block proposal to the validators.
    #[instrument(level = "trace", skip_all)]
    async fn submit_block_proposal<T: ProcessableCertificate>(
        self: &Arc<Self>,
        committee: &Committee,
        proposal: Box<BlockProposal>,
        value: T,
    ) -> Result<GenericCertificate<T>, ChainClientError> {
        debug!(
            round = %proposal.content.round,
            "Submitting block proposal to validators"
        );

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
        let clock_skew_check_handle = linera_base::task::spawn(async move {
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

        let certificate = self
            .communicate_chain_action(committee, submit_action, value)
            .await?;

        clock_skew_check_handle.await;

        self.process_certificate(Box::new(certificate.clone()))
            .await?;
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
        latest_certificate: Option<GenericCertificate<ConfirmedBlock>>,
    ) -> Result<(), ChainClientError> {
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
    ) -> Result<GenericCertificate<T>, ChainClientError> {
        let nodes = self.make_nodes(committee)?;
        let ((votes_hash, votes_round), votes) = communicate_with_quorum(
            &nodes,
            committee,
            |vote: &LiteVote| (vote.value.value_hash, vote.round),
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
            ChainClientError::UnexpectedQuorum {
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
                ChainClientError::InternalError("Vote values or rounds don't match; this is a bug")
            })?
            .with_value(value)
            .ok_or_else(|| {
                ChainClientError::ProtocolError("A quorum voted for an unexpected value")
            })?;
        Ok(certificate)
    }

    /// Processes the confirmed block certificate in the local node without checking signatures.
    /// Also downloads and processes all ancestors that are still missing.
    #[instrument(level = "trace", skip_all)]
    async fn receive_certificate_with_checked_signatures(
        &self,
        certificate: ConfirmedBlockCertificate,
    ) -> Result<(), ChainClientError> {
        let certificate = Box::new(certificate);
        let block = certificate.block();
        // Recover history from the network.
        self.download_certificates(block.header.chain_id, block.header.height)
            .await?;
        // Process the received operations. Download required hashed certificate values if
        // necessary.
        if let Err(err) = self.process_certificate(certificate.clone()).await {
            match &err {
                LocalNodeError::BlobsNotFound(blob_ids) => {
                    self.download_blobs(&self.validator_nodes().await?, blob_ids)
                        .await
                        .map_err(|_| err)?;
                    self.process_certificate(certificate).await?;
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

    /// Processes the confirmed block in the local node, possibly without executing it.
    #[instrument(level = "trace", skip_all)]
    #[allow(dead_code)] // Otherwise CI fails when built for docker.
    async fn receive_sender_certificate(
        &self,
        certificate: ConfirmedBlockCertificate,
        mode: ReceiveCertificateMode,
        nodes: Option<Vec<RemoteNode<Env::ValidatorNode>>>,
    ) -> Result<(), ChainClientError> {
        // Verify the certificate before doing any expensive networking.
        let (max_epoch, committees) = self.admin_committees().await?;
        if let ReceiveCertificateMode::NeedsCheck = mode {
            Self::check_certificate(max_epoch, &committees, &certificate)?.into_result()?;
        }
        // Recover history from the network.
        let nodes = if let Some(nodes) = nodes {
            nodes
        } else {
            self.validator_nodes().await?
        };
        if let Err(err) = self.handle_certificate(certificate.clone()).await {
            match &err {
                LocalNodeError::BlobsNotFound(blob_ids) => {
                    self.download_blobs(&nodes, blob_ids).await?;
                    self.handle_certificate(certificate.clone()).await?;
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

    /// Downloads and processes certificates for sender chain blocks.
    #[instrument(level = "trace", skip_all)]
    async fn download_and_process_sender_chain(
        &self,
        sender_chain_id: ChainId,
        nodes: &[RemoteNode<Env::ValidatorNode>],
        received_log: &ReceivedLogs,
        mut remote_heights: Vec<BlockHeight>,
        sender: mpsc::UnboundedSender<ChainAndHeight>,
    ) {
        let (max_epoch, committees) = match self.admin_committees().await {
            Ok(result) => result,
            Err(error) => {
                error!(%error, %sender_chain_id, "could not read admin committees");
                return;
            }
        };
        let committees_ref = &committees;
        let mut nodes = nodes.to_vec();
        while !remote_heights.is_empty() {
            let remote_heights_ref = &remote_heights;
            nodes.shuffle(&mut rand::thread_rng());
            let certificates = match communicate_concurrently(
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
                        return Err(());
                    }
                    let certificates = self
                        .requests_scheduler
                        .download_certificates_by_heights(
                            &remote_node,
                            sender_chain_id,
                            remote_heights,
                        )
                        .await
                        .map_err(|_| ())?;
                    let mut certificates_with_check_results = vec![];
                    for cert in certificates {
                        if let Ok(check_result) =
                            Self::check_certificate(max_epoch, committees_ref, &cert)
                        {
                            certificates_with_check_results
                                .push((cert, check_result.into_result().is_ok()));
                        } else {
                            // Invalid signature - the validator is faulty
                            return Err(());
                        }
                    }
                    Ok(certificates_with_check_results)
                },
                |errors| {
                    errors
                        .into_iter()
                        .map(|(validator, _error)| validator)
                        .collect::<BTreeSet<_>>()
                },
                self.options.certificate_batch_download_timeout,
            )
            .await
            {
                Ok(certificates_with_check_results) => certificates_with_check_results,
                Err(faulty_validators) => {
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
                chain_id = %sender_chain_id,
                num_certificates = %certificates.len(),
                "received certificates",
            );

            let mut to_remove_from_queue = BTreeSet::new();

            for (certificate, check_result) in certificates {
                let hash = certificate.hash();
                let chain_id = certificate.block().header.chain_id;
                let height = certificate.block().header.height;
                if !check_result {
                    // The certificate was correctly signed, but we were missing a committee to
                    // validate it properly - do not receive it, but also do not attempt to
                    // re-download it.
                    to_remove_from_queue.insert(height);
                    continue;
                }
                // We checked the certificates right after downloading them.
                let mode = ReceiveCertificateMode::AlreadyChecked;
                if let Err(error) = self
                    .receive_sender_certificate(certificate, mode, None)
                    .await
                {
                    warn!(%error, %hash, "Received invalid certificate");
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
        trace!(
            chain_id = %sender_chain_id,
            "find_received_certificates: finished processing chain",
        );
    }

    /// Downloads the log of received messages for a chain from a validator.
    #[instrument(level = "trace", skip(self))]
    async fn get_received_log_from_validator(
        &self,
        chain_id: ChainId,
        remote_node: &RemoteNode<Env::ValidatorNode>,
        tracker: u64,
    ) -> Result<Vec<ChainAndHeight>, ChainClientError> {
        let mut offset = tracker;

        // Retrieve the list of newly received certificates from this validator.
        let mut remote_log = Vec::new();
        loop {
            trace!("get_received_log_from_validator: looping");
            let query = ChainInfoQuery::new(chain_id).with_received_log_excluding_first_n(offset);
            let info = remote_node.handle_chain_info_query(query).await?;
            let received_entries = info.requested_received_log.len();
            offset += received_entries as u64;
            remote_log.extend(info.requested_received_log);
            trace!(
                remote_node = remote_node.address(),
                %received_entries,
                "get_received_log_from_validator: received log batch",
            );
            if received_entries < CHAIN_INFO_MAX_RECEIVED_LOG_ENTRIES {
                break;
            }
        }

        trace!(
            remote_node = remote_node.address(),
            num_entries = remote_log.len(),
            "get_received_log_from_validator: returning downloaded log",
        );

        Ok(remote_log)
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
    ) -> Result<(), ChainClientError> {
        let next_outbox_height = self
            .local_node
            .next_outbox_heights(&[sender_chain_id], receiver_chain_id)
            .await?
            .get(&sender_chain_id)
            .copied()
            .unwrap_or(BlockHeight::ZERO);
        let (max_epoch, committees) = self.admin_committees().await?;

        // Recursively collect all certificates we need, following
        // the chain of previous_message_blocks back to next_outbox_height.
        let mut certificates = BTreeMap::new();
        let mut current_height = height;

        // Stop if we've reached the height we've already processed.
        while current_height >= next_outbox_height {
            // Download the certificate for this height.
            let downloaded = self
                .requests_scheduler
                .download_certificates_by_heights(
                    remote_node,
                    sender_chain_id,
                    vec![current_height],
                )
                .await?;
            let Some(certificate) = downloaded.into_iter().next() else {
                return Err(ChainClientError::CannotDownloadMissingSenderBlock {
                    chain_id: sender_chain_id,
                    height: current_height,
                });
            };

            // Validate the certificate.
            Client::<Env>::check_certificate(max_epoch, &committees, &certificate)?
                .into_result()?;

            // Check if there's a previous message block to our chain.
            let block = certificate.block();
            let next_height = block
                .body
                .previous_message_blocks
                .get(&receiver_chain_id)
                .map(|(_prev_hash, prev_height)| *prev_height);

            // Store this certificate.
            certificates.insert(current_height, certificate);

            if let Some(prev_height) = next_height {
                // Continue with the previous block.
                current_height = prev_height;
            } else {
                // No more dependencies.
                break;
            }
        }

        if certificates.is_empty() {
            self.local_node
                .retry_pending_cross_chain_requests(sender_chain_id)
                .await?;
        }

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

    #[instrument(
        level = "trace", skip_all,
        fields(certificate_hash = ?incoming_certificate.hash()),
    )]
    fn check_certificate(
        highest_known_epoch: Epoch,
        committees: &BTreeMap<Epoch, Committee>,
        incoming_certificate: &ConfirmedBlockCertificate,
    ) -> Result<CheckCertificateResult, NodeError> {
        let block = incoming_certificate.block();
        // Check that certificates are valid w.r.t one of our trusted committees.
        if block.header.epoch > highest_known_epoch {
            return Ok(CheckCertificateResult::FutureEpoch);
        }
        if let Some(known_committee) = committees.get(&block.header.epoch) {
            // This epoch is recognized by our chain. Let's verify the
            // certificate.
            incoming_certificate.check(known_committee)?;
            Ok(CheckCertificateResult::New)
        } else {
            // We don't accept a certificate from a committee that was retired.
            Ok(CheckCertificateResult::OldEpoch)
        }
    }

    /// Downloads and processes any certificates we are missing for the given chain.
    ///
    /// If we are an owner of the chain, also synchronizes the consensus state.
    #[instrument(level = "trace", skip_all)]
    pub(crate) async fn synchronize_chain_state(
        &self,
        chain_id: ChainId,
    ) -> Result<Box<ChainInfo>, ChainClientError> {
        let (_, committee) = self.admin_committee().await?;
        Box::pin(self.synchronize_chain_state_from_committee(chain_id, committee)).await
    }

    /// Downloads certificates for the given chain from the given committee.
    ///
    /// If the chain is not in follow-only mode, also fetches and processes manager values
    /// (timeout certificates, proposals, locking blocks) for consensus participation.
    #[instrument(level = "trace", skip_all)]
    pub async fn synchronize_chain_state_from_committee(
        &self,
        chain_id: ChainId,
        committee: Committee,
    ) -> Result<Box<ChainInfo>, ChainClientError> {
        #[cfg(with_metrics)]
        let _latency = if !self.is_chain_follow_only(chain_id).await {
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
    /// If the chain is owned, also fetches and processes manager values
    /// (timeout certificates, proposals, locking blocks) for consensus participation.
    #[instrument(level = "trace", skip(self, remote_node, chain_id))]
    pub(crate) async fn synchronize_chain_state_from(
        &self,
        remote_node: &RemoteNode<Env::ValidatorNode>,
        chain_id: ChainId,
    ) -> Result<(), ChainClientError> {
        let with_manager_values = !self.is_chain_follow_only(chain_id).await;
        let query = if with_manager_values {
            ChainInfoQuery::new(chain_id).with_manager_values()
        } else {
            ChainInfoQuery::new(chain_id)
        };
        let remote_info = remote_node.handle_chain_info_query(query).await?;
        let local_info = self
            .download_certificates_from(remote_node, chain_id, remote_info.next_block_height)
            .await?;

        if !with_manager_values {
            return Ok(());
        }

        // If we are at the same height as the remote node, we also update our chain manager.
        let local_height = match local_info {
            Some(info) => info.next_block_height,
            None => {
                self.local_node
                    .chain_info(chain_id)
                    .await?
                    .next_block_height
            }
        };
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
            self.handle_certificate(*timeout).await?;
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
            if let Err(mut err) =
                Box::pin(self.local_node.handle_block_proposal(proposal.clone())).await
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
                        if let Err(new_err) =
                            Box::pin(self.local_node.handle_block_proposal(proposal.clone())).await
                        {
                            err = new_err;
                        } else {
                            continue;
                        }
                    }
                    if let LocalNodeError::BlobsNotFound(blob_ids) = &err {
                        self.update_local_node_with_blobs_from(
                            blob_ids.clone(),
                            &[remote_node.clone()],
                        )
                        .await?;
                        // We found the missing blobs: retry.
                        if let Err(new_err) =
                            Box::pin(self.local_node.handle_block_proposal(proposal.clone())).await
                        {
                            err = new_err;
                        } else {
                            continue;
                        }
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
                        if let Err(new_err) =
                            Box::pin(self.local_node.handle_block_proposal(proposal.clone())).await
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
        certificate: GenericCertificate<ValidatedBlock>,
    ) -> Result<(), ChainClientError> {
        let chain_id = certificate.inner().chain_id();
        let certificate = Box::new(certificate);
        match self.process_certificate(certificate.clone()).await {
            Err(LocalNodeError::BlobsNotFound(blob_ids)) => {
                let mut blobs = Vec::new();
                for blob_id in blob_ids {
                    let blob_content = self
                        .requests_scheduler
                        .download_pending_blob(remote_node, chain_id, blob_id)
                        .await?;
                    blobs.push(Blob::new(blob_content));
                }
                self.local_node
                    .handle_pending_blobs(chain_id, blobs)
                    .await?;
                self.process_certificate(certificate).await?;
                Ok(())
            }
            Err(err) => Err(err.into()),
            Ok(()) => Ok(()),
        }
    }

    /// Downloads and processes from the specified validators a confirmed block certificates that
    /// use the given blobs. If this succeeds, the blob will be in our storage.
    async fn update_local_node_with_blobs_from(
        &self,
        blob_ids: Vec<BlobId>,
        remote_nodes: &[RemoteNode<Env::ValidatorNode>],
    ) -> Result<Vec<Blob>, ChainClientError> {
        let timeout = self.options.blob_download_timeout;
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
                        certificate,
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
                    Result::<_, ChainClientError>::Ok(blob)
                },
                move |_| ChainClientError::from(NodeError::BlobsNotFound(vec![blob_id])),
                timeout,
            )
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
    async fn stage_block_execution_with_policy(
        &self,
        block: ProposedBlock,
        round: Option<u32>,
        published_blobs: Vec<Blob>,
        policy: BundleExecutionPolicy,
    ) -> Result<(Block, ChainInfoResponse), ChainClientError> {
        loop {
            let result = self
                .local_node
                .stage_block_execution_with_policy(
                    block.clone(),
                    round,
                    published_blobs.clone(),
                    policy,
                )
                .await;
            if let Err(LocalNodeError::BlobsNotFound(blob_ids)) = &result {
                let validators = self.validator_nodes().await?;
                self.update_local_node_with_blobs_from(blob_ids.clone(), &validators)
                    .await?;
                continue; // We found the missing blob: retry.
            }
            if let Ok((_, executed_block, _, _)) = &result {
                let hash = CryptoHash::new(executed_block);
                let notification = Notification {
                    chain_id: executed_block.header.chain_id,
                    reason: Reason::BlockExecuted {
                        height: executed_block.header.height,
                        hash,
                    },
                };
                self.notifier.notify(&[notification]);
            }
            let (_modified_block, executed_block, response, _resource_tracker) = result?;
            return Ok((executed_block, response));
        }
    }

    /// Attempts to execute the block locally. If any attempt to read a blob fails, the blob is
    /// downloaded and execution is retried.
    #[instrument(level = "trace", skip(self, block))]
    async fn stage_block_execution(
        &self,
        block: ProposedBlock,
        round: Option<u32>,
        published_blobs: Vec<Blob>,
    ) -> Result<(Block, ChainInfoResponse), ChainClientError> {
        loop {
            let result = self
                .local_node
                .stage_block_execution(block.clone(), round, published_blobs.clone())
                .await;
            if let Err(LocalNodeError::BlobsNotFound(blob_ids)) = &result {
                let validators = self.validator_nodes().await?;
                self.update_local_node_with_blobs_from(blob_ids.clone(), &validators)
                    .await?;
                continue; // We found the missing blob: retry.
            }
            if let Ok((block, _, _)) = &result {
                let hash = CryptoHash::new(block);
                let notification = Notification {
                    chain_id: block.header.chain_id,
                    reason: Reason::BlockExecuted {
                        height: block.header.height,
                        hash,
                    },
                };
                self.notifier.notify(&[notification]);
            }
            let (block, response, _resource_tracker) = result?;
            return Ok((block, response));
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TimingType {
    ExecuteOperations,
    ExecuteBlock,
    SubmitBlockProposal,
    UpdateValidators,
}

#[derive(Debug, Clone)]
pub struct ChainClientOptions {
    /// Maximum number of pending message bundles processed at a time in a block.
    pub max_pending_message_bundles: usize,
    /// Maximum number of message bundles to discard from a block proposal due to block limit
    /// errors before discarding all remaining bundles.
    ///
    /// Discarded bundles can be retried in the next block.
    pub max_block_limit_errors: u32,
    /// Maximum number of new stream events processed at a time in a block.
    pub max_new_events_per_block: usize,
    /// Time budget for staging message bundles. When set, limits bundle execution by
    /// wall-clock time, in addition to the count limit from `max_pending_message_bundles`.
    pub staging_bundles_time_budget: Option<Duration>,
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

pub static DEFAULT_CERTIFICATE_DOWNLOAD_BATCH_SIZE: u64 = 500;
pub static DEFAULT_SENDER_CERTIFICATE_DOWNLOAD_BATCH_SIZE: usize = 20_000;

#[cfg(with_testing)]
impl ChainClientOptions {
    pub fn test_default() -> Self {
        use crate::DEFAULT_QUORUM_GRACE_PERIOD;

        ChainClientOptions {
            max_pending_message_bundles: 10,
            max_block_limit_errors: 3,
            max_new_events_per_block: 10,
            staging_bundles_time_budget: None,
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

impl ChainClientOptions {
    /// Builds the [`BundleExecutionPolicy`] based on the client options.
    pub fn bundle_execution_policy(&self) -> BundleExecutionPolicy {
        BundleExecutionPolicy {
            on_failure: BundleFailurePolicy::AutoRetry {
                max_failures: self.max_block_limit_errors,
            },
            time_budget: self.staging_bundles_time_budget,
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
    client: Arc<Client<Env>>,
    /// The off-chain chain ID.
    chain_id: ChainId,
    /// The client options.
    #[debug(skip)]
    options: ChainClientOptions,
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
pub enum ChainClientError {
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

impl From<Infallible> for ChainClientError {
    fn from(infallible: Infallible) -> Self {
        match infallible {}
    }
}

impl ChainClientError {
    pub fn signer_failure(err: impl signer::Error + 'static) -> Self {
        Self::Signer(Box::new(err))
    }
}

impl<Env: Environment> ChainClient<Env> {
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
        F: Fn(&mut ChainClientState),
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

    /// Gets a mutable reference to the per-`ChainClient` options.
    #[instrument(level = "trace", skip(self))]
    pub fn options_mut(&mut self) -> &mut ChainClientOptions {
        &mut self.options
    }

    /// Gets a reference to the per-`ChainClient` options.
    #[instrument(level = "trace", skip(self))]
    pub fn options(&self) -> &ChainClientOptions {
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
    pub async fn event_stream_publishers(&self) -> Result<BTreeSet<ChainId>, LocalNodeError> {
        let subscriptions = self
            .client
            .local_node
            .get_event_subscriptions(self.chain_id)
            .await?;
        let mut publishers = subscriptions
            .into_iter()
            .map(|((chain_id, _), _)| chain_id)
            .collect::<BTreeSet<_>>();
        if self.chain_id != self.client.admin_chain_id {
            publishers.insert(self.client.admin_chain_id);
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
    pub async fn get_chain_description(&self) -> Result<ChainDescription, ChainClientError> {
        self.client.get_chain_description(self.chain_id).await
    }

    /// Prepares the chain for the specified owner.
    ///
    /// Ensures we have the chain description blob, gets chain info, and validates
    /// that the owner can propose on this chain (either by being an owner or via
    /// `open_multi_leader_rounds`).
    pub async fn prepare_for_owner(
        &self,
        owner: AccountOwner,
    ) -> Result<Box<ChainInfo>, ChainClientError> {
        ensure!(
            self.client.has_key_for(&owner).await?,
            ChainClientError::CannotFindKeyForChain(self.chain_id)
        );
        // Ensure we have the chain description blob.
        self.client
            .get_chain_description_blob(self.chain_id)
            .await?;

        // Get chain info.
        let info = self.chain_info().await?;

        // Validate that the owner can propose on this chain.
        ensure!(
            info.manager
                .ownership
                .can_propose_in_multi_leader_round(&owner),
            ChainClientError::NotAnOwner(self.chain_id)
        );

        Ok(info)
    }

    /// Obtains up to `self.options.max_pending_message_bundles` pending message bundles for the
    /// local chain.
    #[instrument(level = "trace")]
    async fn pending_message_bundles(&self) -> Result<Vec<IncomingBundle>, ChainClientError> {
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
                ChainClientError::WalletSynchronizationError
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
    async fn collect_stream_updates(&self) -> Result<Option<Operation>, ChainClientError> {
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
                let previous_index = subscriptions.next_index;
                async move {
                    let next_index = client
                        .local_node
                        .get_stream_event_count(chain_id, stream_id.clone())
                        .await?;
                    if let Some(next_index) =
                        next_index.filter(|next_index| *next_index > previous_index)
                    {
                        Ok(Some((chain_id, stream_id, previous_index, next_index)))
                    } else {
                        Ok::<_, ChainClientError>(None)
                    }
                }
            });
        let all_updates = futures::stream::iter(futures)
            .buffer_unordered(self.options.max_joined_tasks)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        // Apply the max_new_events_per_block limit.
        let max_events = self.options.max_new_events_per_block;
        let mut total_events: usize = 0;
        let mut updates = Vec::new();
        for (chain_id, stream_id, previous_index, next_index) in all_updates {
            let new_events = (next_index - previous_index) as usize;
            if total_events + new_events <= max_events {
                total_events += new_events;
                updates.push((chain_id, stream_id, next_index));
            } else {
                let remaining = max_events.saturating_sub(total_events);
                if remaining > 0 {
                    updates.push((chain_id, stream_id, previous_index + remaining as u32));
                }
                break;
            }
        }
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
    pub async fn local_committee(&self) -> Result<Committee, ChainClientError> {
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
    pub async fn identity(&self) -> Result<AccountOwner, ChainClientError> {
        let Some(preferred_owner) = self.preferred_owner else {
            return Err(ChainClientError::NoAccountKeyConfigured(self.chain_id));
        };
        let manager = self.chain_info().await?.manager;
        ensure!(
            manager.ownership.is_active(),
            LocalNodeError::InactiveChain(self.chain_id)
        );

        // Check if the preferred owner can propose on this chain: either they are an owner,
        // the current leader, or open_multi_leader_rounds is enabled.
        let is_owner = manager
            .ownership
            .can_propose_in_multi_leader_round(&preferred_owner);

        if !is_owner {
            let accepted_owners = manager
                .ownership
                .all_owners()
                .chain(&manager.leader)
                .collect::<Vec<_>>();
            warn!(%self.chain_id, ?accepted_owners, ?preferred_owner,
                "The preferred owner is not configured as an owner of this chain",
            );
            return Err(ChainClientError::NotAnOwner(self.chain_id));
        }

        let has_signer = self
            .signer()
            .contains_key(&preferred_owner)
            .await
            .map_err(ChainClientError::signer_failure)?;

        if !has_signer {
            warn!(%self.chain_id, ?preferred_owner,
                "Chain is one of the owners but its Signer instance doesn't contain the key",
            );
            return Err(ChainClientError::CannotFindKeyForChain(self.chain_id));
        }

        Ok(preferred_owner)
    }

    /// Prepares the chain for the next operation, i.e. makes sure we have synchronized it up to
    /// its current height.
    #[instrument(level = "trace")]
    pub async fn prepare_chain(&self) -> Result<Box<ChainInfo>, ChainClientError> {
        #[cfg(with_metrics)]
        let _latency = metrics::PREPARE_CHAIN_LATENCY.measure_latency();

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
    async fn synchronize_to_known_height(&self) -> Result<Box<ChainInfo>, ChainClientError> {
        let info = self
            .client
            .download_certificates(self.chain_id, self.initial_next_block_height)
            .await?;
        if info.next_block_height == self.initial_next_block_height {
            // Check that our local node has the expected block hash.
            ensure!(
                self.initial_block_hash == info.block_hash,
                ChainClientError::InternalError("Invalid chain of blocks in local node")
            );
        }
        Ok(info)
    }

    /// Attempts to update all validators about the local chain.
    #[instrument(level = "trace", skip(old_committee, latest_certificate))]
    pub async fn update_validators(
        &self,
        old_committee: Option<&Committee>,
        latest_certificate: Option<GenericCertificate<ConfirmedBlock>>,
    ) -> Result<(), ChainClientError> {
        let update_validators_start = linera_base::time::Instant::now();
        // Communicate the new certificate now.
        if let Some(old_committee) = old_committee {
            let old_committee_start = linera_base::time::Instant::now();
            self.communicate_chain_updates(old_committee, latest_certificate.clone())
                .await?;
            tracing::debug!(
                old_committee_ms = old_committee_start.elapsed().as_millis(),
                "communicated chain updates to old committee"
            );
        };
        if let Ok(new_committee) = self.local_committee().await {
            if Some(&new_committee) != old_committee {
                // If the configuration just changed, communicate to the new committee as well.
                // (This is actually more important that updating the previous committee.)
                let new_committee_start = linera_base::time::Instant::now();
                self.communicate_chain_updates(&new_committee, latest_certificate)
                    .await?;
                tracing::debug!(
                    new_committee_ms = new_committee_start.elapsed().as_millis(),
                    "communicated chain updates to new committee"
                );
            }
        }
        self.send_timing(update_validators_start, TimingType::UpdateValidators);
        Ok(())
    }

    /// Broadcasts certified blocks to validators.
    #[instrument(level = "trace", skip(committee, latest_certificate))]
    pub async fn communicate_chain_updates(
        &self,
        committee: &Committee,
        latest_certificate: Option<GenericCertificate<ConfirmedBlock>>,
    ) -> Result<(), ChainClientError> {
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
    async fn synchronize_publisher_chains(&self) -> Result<(), ChainClientError> {
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
    ) -> Result<(), ChainClientError> {
        debug!(chain_id = %self.chain_id, "starting find_received_certificates");
        #[cfg(with_metrics)]
        let _latency = metrics::FIND_RECEIVED_CERTIFICATES_LATENCY.measure_latency();
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
    ) -> Result<ValidatorTrackers, ChainClientError> {
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
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
        // TODO(#467): check the balance of `owner` before signing any block proposal.
        Box::pin(self.execute_operation(SystemOperation::Transfer {
            owner,
            recipient,
            amount,
        }))
        .await
    }

    /// Verify if a data blob is readable from storage.
    // TODO(#2490): Consider removing or renaming this.
    #[instrument(level = "trace")]
    pub async fn read_data_blob(
        &self,
        hash: CryptoHash,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
        let blob_id = BlobId {
            hash,
            blob_type: BlobType::Data,
        };
        Box::pin(self.execute_operation(SystemOperation::VerifyBlob { blob_id })).await
    }

    /// Claims money in a remote chain.
    #[instrument(level = "trace")]
    pub async fn claim(
        &self,
        owner: AccountOwner,
        target_id: ChainId,
        recipient: Account,
        amount: Amount,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
        Box::pin(self.execute_operation(SystemOperation::Claim {
            owner,
            target_id,
            recipient,
            amount,
        }))
        .await
    }

    /// Requests a leader timeout vote from all validators. If a quorum signs it, creates a
    /// certificate and sends it to all validators, to make them enter the next round.
    #[instrument(level = "trace")]
    pub async fn request_leader_timeout(&self) -> Result<TimeoutCertificate, ChainClientError> {
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
    ) -> Result<Box<ChainInfo>, ChainClientError> {
        self.client.synchronize_chain_state(chain_id).await
    }

    /// Downloads and processes any certificates we are missing for this chain, from the given
    /// committee.
    #[instrument(level = "trace", skip_all)]
    pub async fn synchronize_chain_state_from_committee(
        &self,
        committee: Committee,
    ) -> Result<Box<ChainInfo>, ChainClientError> {
        Box::pin(
            self.client
                .synchronize_chain_state_from_committee(self.chain_id, committee),
        )
        .await
    }

    /// Executes a list of operations.
    #[instrument(level = "trace", skip(operations, blobs))]
    pub async fn execute_operations(
        &self,
        operations: Vec<Operation>,
        blobs: Vec<Blob>,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
        let timing_start = linera_base::time::Instant::now();
        tracing::debug!("execute_operations started");

        let result = loop {
            let execute_block_start = linera_base::time::Instant::now();
            // TODO(#2066): Remove boxing once the call-stack is shallower
            tracing::debug!("calling execute_block");
            match Box::pin(self.execute_block(operations.clone(), blobs.clone())).await {
                Ok(ClientOutcome::Committed(certificate)) => {
                    tracing::debug!(
                        execute_block_ms = execute_block_start.elapsed().as_millis(),
                        "execute_block succeeded"
                    );
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
                Err(ChainClientError::CommunicationError(CommunicationError::Trusted(
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
        tracing::debug!(
            total_execute_operations_ms = timing_start.elapsed().as_millis(),
            "execute_operations returning"
        );

        result
    }

    /// Executes an operation.
    pub async fn execute_operation(
        &self,
        operation: impl Into<Operation>,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
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
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
        #[cfg(with_metrics)]
        let _latency = metrics::EXECUTE_BLOCK_LATENCY.measure_latency();

        let mutex = self.client_mutex();
        let lock_start = linera_base::time::Instant::now();
        let _guard = mutex.lock_owned().await;
        tracing::debug!(
            lock_wait_ms = lock_start.elapsed().as_millis(),
            "acquired client_mutex in execute_block"
        );
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
            return Err(ChainClientError::LocalNodeError(
                LocalNodeError::WorkerError(WorkerError::ChainError(Box::new(
                    ChainError::EmptyBlock,
                ))),
            ));
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
            ClientOutcome::Committed(None) => Err(ChainClientError::BlockProposalError(
                "Unexpected block proposal error",
            )),
            ClientOutcome::Conflict(certificate) => Ok(ClientOutcome::Conflict(certificate)),
            ClientOutcome::WaitForTimeout(timeout) => Ok(ClientOutcome::WaitForTimeout(timeout)),
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
    ) -> Result<Vec<Transaction>, ChainClientError> {
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
    ) -> Result<Block, ChainClientError> {
        let identity = self.identity().await?;

        ensure!(
            self.pending_proposal().is_none(),
            ChainClientError::BlockProposalError(
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
            authenticated_signer: Some(identity),
            timestamp,
        };

        let round = self.round_for_oracle(&info, &identity).await?;
        // Make sure every incoming message succeeds and otherwise remove them.
        // Also, compute the final certified hash while we're at it.
        let (block, _) = Box::pin(self.client.stage_block_execution_with_policy(
            proposed_block,
            round,
            blobs.clone(),
            self.options.bundle_execution_policy(),
        ))
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
    ) -> Result<QueryOutcome, ChainClientError> {
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
    ) -> Result<QueryOutcome<SystemResponse>, ChainClientError> {
        let QueryOutcome {
            response,
            operations,
        } = self.query_application(Query::System(query), None).await?;
        match response {
            QueryResponse::System(response) => Ok(QueryOutcome {
                response,
                operations,
            }),
            _ => Err(ChainClientError::InternalError(
                "Unexpected response for system query",
            )),
        }
    }

    /// Queries a user application.
    #[instrument(level = "trace", skip(application_id, query))]
    #[cfg(with_testing)]
    pub async fn query_user_application<A: Abi>(
        &self,
        application_id: ApplicationId<A>,
        query: &A::Query,
    ) -> Result<QueryOutcome<A::QueryResponse>, ChainClientError> {
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
            _ => Err(ChainClientError::InternalError(
                "Unexpected response for user query",
            )),
        }
    }

    /// Obtains the local balance of the chain account after staging the execution of
    /// incoming messages in a new block.
    ///
    /// Does not attempt to synchronize with validators. The result will reflect up to
    /// `max_pending_message_bundles` incoming message bundles and the execution fees for a single
    /// block.
    #[instrument(level = "trace")]
    pub async fn query_balance(&self) -> Result<Amount, ChainClientError> {
        let (balance, _) = Box::pin(self.query_balances_with_owner(AccountOwner::CHAIN)).await?;
        Ok(balance)
    }

    /// Obtains the local balance of an account after staging the execution of incoming messages in
    /// a new block.
    ///
    /// Does not attempt to synchronize with validators. The result will reflect up to
    /// `max_pending_message_bundles` incoming message bundles and the execution fees for a single
    /// block.
    #[instrument(level = "trace", skip(owner))]
    pub async fn query_owner_balance(
        &self,
        owner: AccountOwner,
    ) -> Result<Amount, ChainClientError> {
        if owner.is_chain() {
            Box::pin(self.query_balance()).await
        } else {
            Ok(Box::pin(self.query_balances_with_owner(owner))
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
    async fn query_balances_with_owner(
        &self,
        owner: AccountOwner,
    ) -> Result<(Amount, Option<Amount>), ChainClientError> {
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
            authenticated_signer: if owner == AccountOwner::CHAIN {
                None
            } else {
                Some(owner)
            },
            timestamp,
        };
        match Box::pin(self.client.stage_block_execution_with_policy(
            block,
            None,
            Vec::new(),
            self.options.bundle_execution_policy(),
        ))
        .await
        {
            Ok((_, response)) => Ok((
                response.info.chain_balance,
                response.info.requested_owner_balance,
            )),
            Err(ChainClientError::LocalNodeError(LocalNodeError::WorkerError(
                WorkerError::ChainError(error),
            ))) if matches!(
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
    pub async fn local_balance(&self) -> Result<Amount, ChainClientError> {
        let (balance, _) = self.local_balances_with_owner(AccountOwner::CHAIN).await?;
        Ok(balance)
    }

    /// Reads the local balance of a user account.
    ///
    /// Does not process the inbox or attempt to synchronize with validators.
    #[instrument(level = "trace", skip(owner))]
    pub async fn local_owner_balance(
        &self,
        owner: AccountOwner,
    ) -> Result<Amount, ChainClientError> {
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
    async fn local_balances_with_owner(
        &self,
        owner: AccountOwner,
    ) -> Result<(Amount, Option<Amount>), ChainClientError> {
        ensure!(
            self.chain_info().await?.next_block_height >= self.initial_next_block_height,
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

    /// Sends tokens to a chain.
    #[instrument(level = "trace")]
    pub async fn transfer_to_account(
        &self,
        from: AccountOwner,
        amount: Amount,
        account: Account,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
        self.transfer(from, amount, account).await
    }

    /// Burns tokens (transfer to a special address).
    #[cfg(with_testing)]
    #[instrument(level = "trace")]
    pub async fn burn(
        &self,
        owner: AccountOwner,
        amount: Amount,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
        let recipient = Account::burn_address(self.chain_id);
        self.transfer(owner, amount, recipient).await
    }

    #[instrument(level = "trace")]
    pub async fn fetch_chain_info(&self) -> Result<Box<ChainInfo>, ChainClientError> {
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
    pub async fn synchronize_from_validators(&self) -> Result<Box<ChainInfo>, ChainClientError> {
        if self.preferred_owner.is_none() {
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
    ) -> Result<ClientOutcome<Option<ConfirmedBlockCertificate>>, ChainClientError> {
        self.prepare_chain().await?;
        self.process_pending_block_without_prepare().await
    }

    /// Processes the last pending block. Assumes that the local chain is up to date.
    #[instrument(level = "trace")]
    async fn process_pending_block_without_prepare(
        &self,
    ) -> Result<ClientOutcome<Option<ConfirmedBlockCertificate>>, ChainClientError> {
        let process_start = linera_base::time::Instant::now();
        tracing::debug!("process_pending_block_without_prepare started");
        let info = self.request_leader_timeout_if_needed().await?;

        // If there is a validated block in the current round, finalize it.
        if info.manager.has_locking_block_in_current_round()
            && !info.manager.current_round.is_fast()
        {
            return Box::pin(self.finalize_locking_block(info)).await;
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
                        .ok_or_else(|| {
                            ChainClientError::InternalError("Missing local locking blobs")
                        })?;
                    debug!("Retrying locking block from round {}", certificate.round);
                    (certificate.block().clone(), blobs)
                }
                LockingBlock::Fast(proposal) => {
                    let proposed_block = proposal.content.block.clone();
                    let blob_ids = proposed_block.published_blob_ids();
                    let blobs = local_node
                        .get_locking_blobs(&blob_ids, self.chain_id)
                        .await?
                        .ok_or_else(|| {
                            ChainClientError::InternalError("Missing local locking blobs")
                        })?;
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
                        .map_err(ChainClientError::signer_failure)?
                }
                LockingBlock::Fast(proposal) => {
                    BlockProposal::new_retry_fast(owner, round, proposal, self.signer())
                        .await
                        .map_err(ChainClientError::signer_failure)?
                }
            })
        } else {
            Box::new(
                BlockProposal::new_initial(owner, round, proposed_block.clone(), self.signer())
                    .await
                    .map_err(ChainClientError::signer_failure)?,
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
            Box::pin(
                self.client
                    .submit_block_proposal(&committee, proposal, hashed_value),
            )
            .await?
        } else {
            let hashed_value = ValidatedBlock::new(block);
            let certificate = Box::pin(self.client.submit_block_proposal(
                &committee,
                proposal,
                hashed_value.clone(),
            ))
            .await?;
            Box::pin(self.client.finalize_block(&committee, certificate)).await?
        };
        self.send_timing(submit_block_proposal_start, TimingType::SubmitBlockProposal);
        debug!(round = %certificate.round, "Sending confirmed block to validators");
        let update_start = linera_base::time::Instant::now();
        Box::pin(self.update_validators(Some(&committee), Some(certificate.clone()))).await?;
        tracing::debug!(
            update_validators_ms = update_start.elapsed().as_millis(),
            total_process_ms = process_start.elapsed().as_millis(),
            "process_pending_block_without_prepare completing"
        );
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
    async fn request_leader_timeout_if_needed(&self) -> Result<Box<ChainInfo>, ChainClientError> {
        let mut info = self.chain_info_with_manager_values().await?;
        // If the current round has timed out, we request a timeout certificate and retry in
        // the next round.
        if let Some(round_timeout) = info.manager.round_timeout {
            if round_timeout <= self.storage_client().clock().current_time() {
                if let Err(e) = self.request_leader_timeout().await {
                    debug!("Failed to obtain a timeout certificate: {}", e);
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
    ) -> Result<ClientOutcome<Option<ConfirmedBlockCertificate>>, ChainClientError> {
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
        let certificate =
            Box::pin(self.client.finalize_block(&committee, certificate.clone())).await?;
        Box::pin(self.update_validators(Some(&committee), Some(certificate.clone()))).await?;
        Ok(ClientOutcome::Committed(Some(certificate)))
    }

    /// Returns the number for the round number oracle to use when staging a block proposal.
    async fn round_for_oracle(
        &self,
        info: &ChainInfo,
        identity: &AccountOwner,
    ) -> Result<Option<u32>, ChainClientError> {
        // Pretend we do use oracles: If we don't, the round number is never read anyway.
        match self.round_for_new_proposal(info, identity, true).await {
            // If it is a multi-leader round, use its number for the oracle.
            Ok(Either::Left(round)) => Ok(round.multi_leader()),
            // If there is no suitable round with oracles, use None: If it works without oracles,
            // the block won't read the value. If it returns a timeout, it will be a single-leader
            // round, in which the oracle returns None.
            Err(ChainClientError::BlockProposalError(_)) | Ok(Either::Right(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Returns a round in which we can propose a new block or the given one, if possible.
    async fn round_for_new_proposal(
        &self,
        info: &ChainInfo,
        identity: &AccountOwner,
        has_oracle_responses: bool,
    ) -> Result<Either<Round, RoundTimeout>, ChainClientError> {
        let manager = &info.manager;
        let seed = self
            .client
            .local_node
            .get_manager_seed(self.chain_id)
            .await?;
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
            return Err(ChainClientError::BlockProposalError(
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
        Err(ChainClientError::BlockProposalError(
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
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
        Box::pin(self.transfer_ownership(public_key.into())).await
    }

    /// Transfers ownership of the chain to a single super owner.
    #[instrument(level = "trace")]
    pub async fn transfer_ownership(
        &self,
        new_owner: AccountOwner,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
        Box::pin(self.execute_operation(SystemOperation::ChangeOwnership {
            super_owners: vec![new_owner],
            owners: Vec::new(),
            multi_leader_rounds: 2,
            open_multi_leader_rounds: false,
            timeout_config: TimeoutConfig::default(),
        }))
        .await
    }

    /// Adds another owner to the chain, and turns existing super owners into regular owners.
    #[instrument(level = "trace")]
    pub async fn share_ownership(
        &self,
        new_owner: AccountOwner,
        new_weight: u64,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
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
            multi_leader_rounds: ownership.multi_leader_rounds,
            open_multi_leader_rounds: ownership.open_multi_leader_rounds,
            timeout_config: ownership.timeout_config,
        })];
        match self.execute_block(operations, vec![]).await? {
            ClientOutcome::Committed(certificate) => Ok(ClientOutcome::Committed(certificate)),
            ClientOutcome::Conflict(certificate) => {
                info!(
                    height = %certificate.block().header.height,
                    "Another block was committed."
                );
                Ok(ClientOutcome::Conflict(certificate))
            }
            ClientOutcome::WaitForTimeout(timeout) => Ok(ClientOutcome::WaitForTimeout(timeout)),
        }
    }

    /// Returns the current ownership settings on this chain.
    #[instrument(level = "trace")]
    pub async fn query_chain_ownership(&self) -> Result<ChainOwnership, ChainClientError> {
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
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
        Box::pin(self.execute_operation(SystemOperation::ChangeOwnership {
            super_owners: ownership.super_owners.into_iter().collect(),
            owners: ownership.owners.into_iter().collect(),
            multi_leader_rounds: ownership.multi_leader_rounds,
            open_multi_leader_rounds: ownership.open_multi_leader_rounds,
            timeout_config: ownership.timeout_config.clone(),
        }))
        .await
    }

    /// Returns the current application permissions on this chain.
    #[instrument(level = "trace")]
    pub async fn query_application_permissions(
        &self,
    ) -> Result<ApplicationPermissions, ChainClientError> {
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
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
        Box::pin(
            self.execute_operation(SystemOperation::ChangeApplicationPermissions(
                application_permissions,
            )),
        )
        .await
    }

    /// Opens a new chain with a derived UID.
    #[instrument(level = "trace", skip(self))]
    pub async fn open_chain(
        &self,
        ownership: ChainOwnership,
        application_permissions: ApplicationPermissions,
        balance: Amount,
    ) -> Result<ClientOutcome<(ChainDescription, ConfirmedBlockCertificate)>, ChainClientError>
    {
        let config = OpenChainConfig {
            ownership: ownership.clone(),
            balance,
            application_permissions: application_permissions.clone(),
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
            .ok_or_else(|| ChainClientError::InternalError("Failed to create a new chain"))?;
        let description = bcs::from_bytes::<ChainDescription>(chain_blob.bytes())?;
        // If we have a key for any owner, add it to the list of tracked chains.
        for owner in ownership.all_owners() {
            if self.client.has_key_for(owner).await? {
                self.client
                    .extend_chain_mode(description.id(), ListeningMode::FullChain);
                break;
            }
        }
        self.client
            .local_node
            .retry_pending_cross_chain_requests(self.chain_id)
            .await?;
        Ok(ClientOutcome::Committed((description, certificate)))
    }

    /// Closes the chain (and loses everything in it!!).
    /// Returns `None` if the chain was already closed.
    #[instrument(level = "trace")]
    pub async fn close_chain(
        &self,
    ) -> Result<ClientOutcome<Option<ConfirmedBlockCertificate>>, ChainClientError> {
        match Box::pin(self.execute_operation(SystemOperation::CloseChain)).await {
            Ok(outcome) => Ok(outcome.map(Some)),
            Err(ChainClientError::LocalNodeError(LocalNodeError::WorkerError(
                WorkerError::ChainError(chain_error),
            ))) if matches!(*chain_error, ChainError::ClosedChain) => {
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
    ) -> Result<ClientOutcome<(ModuleId, ConfirmedBlockCertificate)>, ChainClientError> {
        let (blobs, module_id) = create_bytecode_blobs(contract, service, vm_runtime).await;
        Box::pin(self.publish_module_blobs(blobs, module_id)).await
    }

    /// Publishes some module.
    #[cfg(not(target_arch = "wasm32"))]
    #[instrument(level = "trace", skip(blobs, module_id))]
    pub async fn publish_module_blobs(
        &self,
        blobs: Vec<Blob>,
        module_id: ModuleId,
    ) -> Result<ClientOutcome<(ModuleId, ConfirmedBlockCertificate)>, ChainClientError> {
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
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
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
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
        Box::pin(self.publish_data_blobs(vec![bytes])).await
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
    ) -> Result<ClientOutcome<(ApplicationId<A>, ConfirmedBlockCertificate)>, ChainClientError>
    {
        let instantiation_argument = serde_json::to_vec(instantiation_argument)?;
        let parameters = serde_json::to_vec(parameters)?;
        Ok(Box::pin(self.create_application_untyped(
            module_id.forget_abi(),
            parameters,
            instantiation_argument,
            required_application_ids,
        ))
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
    ) -> Result<ClientOutcome<(ApplicationId, ConfirmedBlockCertificate)>, ChainClientError> {
        Box::pin(self.execute_operation(SystemOperation::CreateApplication {
            module_id,
            parameters,
            instantiation_argument,
            required_application_ids,
        }))
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
                return Err(ChainClientError::InternalError(
                    "Unexpected number of application descriptions published",
                ));
            }
            let blob_id = creation.pop().ok_or(ChainClientError::InternalError(
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
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
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
            outcome @ ClientOutcome::WaitForTimeout(_) => return Ok(outcome),
            outcome @ ClientOutcome::Conflict(_) => return Ok(outcome),
        }
        let epoch = Box::pin(self.chain_info()).await?.epoch.try_add_one()?;
        Box::pin(
            self.execute_operation(SystemOperation::Admin(AdminOperation::CreateCommittee {
                epoch,
                blob_hash,
            })),
        )
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
    ) -> Result<(Vec<ConfirmedBlockCertificate>, Option<RoundTimeout>), ChainClientError> {
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
    ) -> Result<(Vec<ConfirmedBlockCertificate>, Option<RoundTimeout>), ChainClientError> {
        #[cfg(with_metrics)]
        let _latency = metrics::PROCESS_INBOX_WITHOUT_PREPARE_LATENCY.measure_latency();

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
                Err(ChainClientError::LocalNodeError(LocalNodeError::WorkerError(
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
    async fn collect_epoch_changes(&self) -> Result<Vec<Operation>, ChainClientError> {
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
    async fn has_admin_event(
        &self,
        stream_name: &[u8],
        index: u32,
    ) -> Result<bool, ChainClientError> {
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
    ) -> Result<Vec<IndexAndEvent>, ChainClientError> {
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
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
        self.prepare_chain().await?;
        let (current_epoch, committees) = self.epoch_and_committees().await?;
        ensure!(
            revoked_epoch < current_epoch,
            ChainClientError::CannotRevokeCurrentEpoch(current_epoch)
        );
        ensure!(
            committees.contains_key(&revoked_epoch),
            ChainClientError::EpochAlreadyRevoked
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
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
        Box::pin(self.execute_operation(SystemOperation::Transfer {
            owner,
            recipient,
            amount,
        }))
        .await
    }

    #[instrument(level = "trace", skip(hash))]
    pub async fn read_confirmed_block(
        &self,
        hash: CryptoHash,
    ) -> Result<ConfirmedBlock, ChainClientError> {
        let block = self
            .client
            .storage_client()
            .read_confirmed_block(hash)
            .await?;
        block.ok_or(ChainClientError::MissingConfirmedBlock(hash))
    }

    #[instrument(level = "trace", skip(hash))]
    pub async fn read_certificate(
        &self,
        hash: CryptoHash,
    ) -> Result<ConfirmedBlockCertificate, ChainClientError> {
        let certificate = self.client.storage_client().read_certificate(hash).await?;
        certificate.ok_or(ChainClientError::ReadCertificatesError(vec![hash]))
    }

    /// Handles any cross-chain requests for any pending outgoing messages.
    #[instrument(level = "trace")]
    pub async fn retry_pending_outgoing_messages(&self) -> Result<(), ChainClientError> {
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
    ) -> Result<Option<Box<ChainInfo>>, ChainClientError> {
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
    ) -> Result<BlockHeight, ChainClientError> {
        Ok(self
            .local_chain_info(chain_id, local_node)
            .await?
            .map_or(BlockHeight::ZERO, |info| info.next_block_height))
    }

    /// Returns the next height we expect to receive from the given sender chain, according to the
    /// local inbox.
    #[instrument(level = "trace")]
    async fn local_next_height_to_receive(
        &self,
        origin: ChainId,
    ) -> Result<BlockHeight, ChainClientError> {
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
    ) -> Result<(), ChainClientError> {
        let dominated = self
            .listening_mode()
            .is_none_or(|mode| !mode.is_relevant(&notification.reason));
        if dominated {
            debug!(
                chain_id = %self.chain_id,
                reason = ?notification.reason,
                listening_mode = ?self.listening_mode(),
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
                    error!("NewBlock: Fail to synchronize new block after notification");
                }
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
                    info!(
                        chain_id = %self.chain_id,
                        "NewRound: Fail to synchronize new block after notification"
                    );
                }
            }
            Reason::BlockExecuted { .. } => {
                // Ignored.
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
    ) -> Result<(impl Future<Output = ()>, AbortOnDrop, NotificationStream), ChainClientError> {
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
    ) -> Result<impl Future<Output = ()>, ChainClientError> {
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
            let address = node.address();
            let hash_map::Entry::Vacant(entry) = senders.entry(public_key) else {
                continue;
            };
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
                    Ok::<_, ChainClientError>(stream)
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
    pub async fn sync_validator(
        &self,
        remote_node: Env::ValidatorNode,
    ) -> Result<(), ChainClientError> {
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

/// Performs `f` in parallel on multiple nodes, starting with a quadratically increasing delay on
/// each subsequent node. Returns error `err` if all of the nodes fail.
async fn communicate_concurrently<'a, A, E1, E2, F, G, R, V>(
    nodes: &[RemoteNode<A>],
    f: F,
    err: G,
    timeout: Duration,
) -> Result<V, E2>
where
    F: Clone + FnOnce(RemoteNode<A>) -> R,
    RemoteNode<A>: Clone,
    G: FnOnce(Vec<(ValidatorPublicKey, E1)>) -> E2,
    R: Future<Output = Result<V, E1>> + 'a,
{
    let mut stream = nodes
        .iter()
        .zip(0..)
        .map(|(remote_node, i)| {
            let fun = f.clone();
            let node = remote_node.clone();
            async move {
                linera_base::time::timer::sleep(timeout * i * i).await;
                fun(node).await.map_err(|err| (remote_node.public_key, err))
            }
        })
        .collect::<FuturesUnordered<_>>();
    let mut errors = vec![];
    while let Some(maybe_result) = stream.next().await {
        match maybe_result {
            Ok(result) => return Ok(result),
            Err(error) => errors.push(error),
        };
    }
    Err(err(errors))
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
    pub block: ProposedBlock,
    pub blobs: Vec<Blob>,
}

enum ReceiveCertificateMode {
    NeedsCheck,
    AlreadyChecked,
}

enum CheckCertificateResult {
    OldEpoch,
    New,
    FutureEpoch,
}

impl CheckCertificateResult {
    fn into_result(self) -> Result<(), ChainClientError> {
        match self {
            Self::OldEpoch => Err(ChainClientError::CommitteeDeprecationError),
            Self::New => Ok(()),
            Self::FutureEpoch => Err(ChainClientError::CommitteeSynchronizationError),
        }
    }
}

/// Creates a compressed Contract, Service and bytecode.
#[cfg(not(target_arch = "wasm32"))]
pub async fn create_bytecode_blobs(
    contract: Bytecode,
    service: Bytecode,
    vm_runtime: VmRuntime,
) -> (Vec<Blob>, ModuleId) {
    match vm_runtime {
        VmRuntime::Wasm => {
            let (compressed_contract, compressed_service) =
                tokio::task::spawn_blocking(move || (contract.compress(), service.compress()))
                    .await
                    .expect("Compression should not panic");
            let contract_blob = Blob::new_contract_bytecode(compressed_contract);
            let service_blob = Blob::new_service_bytecode(compressed_service);
            let module_id =
                ModuleId::new(contract_blob.id().hash, service_blob.id().hash, vm_runtime);
            (vec![contract_blob, service_blob], module_id)
        }
        VmRuntime::Evm => {
            let compressed_contract = contract.compress();
            let evm_contract_blob = Blob::new_evm_bytecode(compressed_contract);
            let module_id = ModuleId::new(
                evm_contract_blob.id().hash,
                evm_contract_blob.id().hash,
                vm_runtime,
            );
            (vec![evm_contract_blob], module_id)
        }
    }
}

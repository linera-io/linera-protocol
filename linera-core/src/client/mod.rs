// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
};

use custom_debug_derive::Debug;
use futures::{
    future::Future,
    stream::{self, AbortHandle, FuturesUnordered, StreamExt},
};
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use linera_base::{
    crypto::{CryptoHash, Signer as _, ValidatorPublicKey},
    data_types::{ArithmeticError, Blob, BlockHeight, ChainDescription, Epoch, TimeDelta},
    ensure,
    identifiers::{AccountOwner, BlobId, BlobType, ChainId, StreamId},
    time::Duration,
};
#[cfg(not(target_arch = "wasm32"))]
use linera_base::{data_types::Bytecode, identifiers::ModuleId, vm::VmRuntime};
use linera_chain::{
    data_types::{
        BlockProposal, ChainAndHeight, LiteVote, MessageAction, ProposedBlock, Transaction,
    },
    manager::LockingBlock,
    types::{
        Block, CertificateValue, ConfirmedBlock, ConfirmedBlockCertificate, GenericCertificate,
        LiteCertificate, ValidatedBlock, ValidatedBlockCertificate,
    },
    ChainError, ChainExecutionContext,
};
use linera_execution::committee::Committee;
use linera_storage::{ResultReadCertificates, Storage as _};
use rand::{
    distributions::{Distribution, WeightedIndex},
    seq::SliceRandom,
};
use received_log::ReceivedLogs;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument, trace, warn};

use crate::{
    data_types::{ChainInfo, ChainInfoQuery, ChainInfoResponse},
    environment::Environment,
    local_node::{LocalChainInfoExt as _, LocalNodeClient, LocalNodeError},
    node::{CrossChainMessageDelivery, NodeError, ValidatorNodeProvider as _},
    notifier::{ChannelNotifier, Notifier as _},
    remote_node::RemoteNode,
    updater::{communicate_with_quorum, CommunicateAction, ValidatorUpdater},
    worker::{Notification, ProcessableCertificate, Reason, WorkerError, WorkerState},
    CHAIN_INFO_MAX_RECEIVED_LOG_ENTRIES,
};

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

pub static DEFAULT_CERTIFICATE_DOWNLOAD_BATCH_SIZE: u64 = 500;
pub static DEFAULT_SENDER_CERTIFICATE_DOWNLOAD_BATCH_SIZE: usize = 20_000;

#[derive(Debug, Clone, Copy)]
pub enum TimingType {
    ExecuteOperations,
    ExecuteBlock,
    SubmitBlockProposal,
    UpdateValidators,
}

/// Defines how we listen to a chain:
/// - do we care about every block notification?
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
            (ListeningMode::EventsOnly(events_a), ListeningMode::EventsOnly(events_b)) => {
                if events_a.is_superset(events_b) {
                    Some(Ordering::Greater)
                } else if events_b.is_superset(events_a) {
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
            // FullChain processes everything.
            (_, ListeningMode::FullChain) => true,
            // FollowChain processes new blocks on the chain itself, including blocks that
            // produced events.
            (Reason::NewBlock { .. }, ListeningMode::FollowChain) => true,
            (Reason::NewEvents { .. }, ListeningMode::FollowChain) => true,
            (_, ListeningMode::FollowChain) => false,
            // EventsOnly only processes events from relevant streams.
            (Reason::NewEvents { event_streams, .. }, ListeningMode::EventsOnly(relevant)) => {
                relevant.intersection(event_streams).next().is_some()
            }
            (_, ListeningMode::EventsOnly(_)) => false,
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
    chains: papaya::HashMap<ChainId, chain_client::State>,
    /// Configuration options.
    options: chain_client::Options,
}

impl<Env: Environment> Client<Env> {
    /// Creates a new `Client` with a new cache and notifiers.
    #[instrument(level = "trace", skip_all)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        environment: Env,
        admin_chain_id: ChainId,
        long_lived_services: bool,
        chain_modes: impl IntoIterator<Item = (ChainId, ListeningMode)>,
        name: impl Into<String>,
        chain_worker_ttl: Duration,
        sender_chain_worker_ttl: Duration,
        options: chain_client::Options,
        block_cache_size: usize,
        execution_state_cache_size: usize,
        requests_scheduler_config: requests_scheduler::RequestsSchedulerConfig,
    ) -> Self {
        let chain_modes = Arc::new(RwLock::new(chain_modes.into_iter().collect()));
        let state = WorkerState::new_for_client(
            name.into(),
            environment.storage().clone(),
            chain_modes.clone(),
            block_cache_size,
            execution_state_cache_size,
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
    #[expect(clippy::too_many_arguments)]
    #[instrument(level = "trace", skip_all, fields(chain_id, next_block_height))]
    pub fn create_chain_client(
        self: &Arc<Self>,
        chain_id: ChainId,
        block_hash: Option<CryptoHash>,
        next_block_height: BlockHeight,
        pending_proposal: Option<PendingProposal>,
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
        self.chains.pin().update(chain_id, |state| {
            let mut state = state.clone_for_update_unchecked();
            state.set_follow_only(follow_only);
            state
        });
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
                // Make sure the admin chain is up to date.
                self.synchronize_chain_state(self.admin_chain_id).await?;
                // If the chain is missing then the error is a WorkerError
                // and so a BlobsNotFound
                self.update_local_node_with_blobs_from(blob_ids, validators)
                    .await?;
                Ok(self.local_node.chain_info(chain_id).await?)
            }
            Err(err) => Err(err.into()),
        }
    }

    fn weighted_select(
        remaining_validators: &mut Vec<RemoteNode<Env::ValidatorNode>>,
        remaining_weights: &mut Vec<u64>,
    ) -> Option<RemoteNode<Env::ValidatorNode>> {
        if remaining_weights.is_empty() {
            return None;
        }
        let dist = WeightedIndex::new(remaining_weights.clone()).unwrap();
        let idx = dist.sample(&mut rand::thread_rng());
        remaining_weights.remove(idx);
        Some(remaining_validators.remove(idx))
    }

    /// Downloads and processes all certificates up to (excluding) the specified height.
    #[instrument(level = "trace", skip(self))]
    async fn download_certificates(
        &self,
        chain_id: ChainId,
        target_next_block_height: BlockHeight,
    ) -> Result<Box<ChainInfo>, chain_client::Error> {
        let (_, committee) = self.admin_committee().await?;
        let mut remaining_validators = self.make_nodes(&committee)?;
        let mut info = self
            .fetch_chain_info(chain_id, &remaining_validators)
            .await?;
        // Determining the weights of the validators
        let mut remaining_weights = remaining_validators
            .iter()
            .map(|validator| {
                let validator_state = committee.validators.get(&validator.public_key).unwrap();
                validator_state.votes
            })
            .collect::<Vec<_>>();

        while let Some(remote_node) =
            Self::weighted_select(&mut remaining_validators, &mut remaining_weights)
        {
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
            chain_client::Error::CannotDownloadCertificates {
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
    ) -> Result<Option<Box<ChainInfo>>, chain_client::Error> {
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
                return Err(chain_client::Error::ReadCertificatesError(hashes))
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
    ) -> Result<(), chain_client::Error> {
        let blobs = &self
            .requests_scheduler
            .download_blobs(remote_nodes, blob_ids, self.options.blob_download_timeout)
            .await?
            .ok_or_else(|| {
                chain_client::Error::RemoteNodeError(NodeError::BlobsNotFound(blob_ids.to_vec()))
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
    ) -> Result<Option<Box<ChainInfo>>, chain_client::Error> {
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
                self.download_blobs(std::slice::from_ref(remote_node), &blob_ids)
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
                        self.download_blobs(std::slice::from_ref(remote_node), &blob_ids)
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
    /// client's `ChainId`.
    pub async fn get_chain_description(
        &self,
        chain_id: ChainId,
    ) -> Result<ChainDescription, chain_client::Error> {
        let chain_desc_id = BlobId::new(chain_id.0, BlobType::ChainDescription);
        let blob = self
            .local_node
            .storage_client()
            .read_blob(chain_desc_id)
            .await?;
        if let Some(blob) = blob {
            // We have the blob - return it.
            return Ok(bcs::from_bytes(blob.bytes())?);
        };
        // Recover history from the current validators, according to the admin chain.
        self.synchronize_chain_state(self.admin_chain_id).await?;
        let nodes = self.validator_nodes().await?;
        let blob = self
            .update_local_node_with_blobs_from(vec![chain_desc_id], &nodes)
            .await?
            .pop()
            .unwrap(); // Returns exactly as many blobs as passed-in IDs.
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
    pub(crate) async fn finalize_block(
        self: &Arc<Self>,
        committee: &Committee,
        certificate: ValidatedBlockCertificate,
    ) -> Result<ConfirmedBlockCertificate, chain_client::Error> {
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
    pub(crate) async fn submit_block_proposal<T: ProcessableCertificate>(
        self: &Arc<Self>,
        committee: &Committee,
        proposal: Box<BlockProposal>,
        value: T,
    ) -> Result<GenericCertificate<T>, chain_client::Error> {
        use linera_storage::Clock as _;

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
                "Block timestamp is in the future; waiting for validators",
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
    ) -> Result<(), chain_client::Error> {
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
    ) -> Result<(), chain_client::Error> {
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
    ) -> Result<Vec<ChainAndHeight>, chain_client::Error> {
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
    ) -> Result<(), chain_client::Error> {
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
                return Err(chain_client::Error::CannotDownloadMissingSenderBlock {
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
        committee: Committee,
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
                            std::slice::from_ref(remote_node),
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
        certificate: GenericCertificate<ValidatedBlock>,
    ) -> Result<(), chain_client::Error> {
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
    ) -> Result<Vec<Blob>, chain_client::Error> {
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
                    Result::<_, chain_client::Error>::Ok(blob)
                },
                move |_| chain_client::Error::from(NodeError::BlobsNotFound(vec![blob_id])),
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
    // TODO(#2806): Measure how failing messages affect the execution times.
    #[tracing::instrument(level = "trace", skip(self, block))]
    async fn stage_block_execution_and_discard_failing_messages(
        &self,
        mut block: ProposedBlock,
        round: Option<u32>,
        published_blobs: Vec<Blob>,
    ) -> Result<(Block, ChainInfoResponse), chain_client::Error> {
        loop {
            let result = self
                .stage_block_execution(block.clone(), round, published_blobs.clone())
                .await;
            if let Err(chain_client::Error::LocalNodeError(LocalNodeError::WorkerError(
                WorkerError::ChainError(chain_error),
            ))) = &result
            {
                if let ChainError::ExecutionError(
                    error,
                    ChainExecutionContext::IncomingBundle(index),
                ) = &**chain_error
                {
                    let transaction = block
                        .transactions
                        .get_mut(*index as usize)
                        .expect("Transaction at given index should exist");
                    let Transaction::ReceiveMessages(incoming_bundle) = transaction else {
                        panic!(
                            "Expected incoming bundle at transaction index {}, found operation",
                            index
                        );
                    };
                    ensure!(
                        !incoming_bundle.bundle.is_protected(),
                        chain_client::Error::BlockProposalError(
                            "Protected incoming message failed to execute locally"
                        )
                    );
                    if incoming_bundle.action == MessageAction::Reject {
                        return result;
                    }
                    // Reject the faulty message from the block and continue.
                    // TODO(#1420): This is potentially a bit heavy-handed for
                    // retryable errors.
                    info!(
                        %error, %index, origin = ?incoming_bundle.origin,
                        "Message bundle failed to execute locally and will be rejected."
                    );
                    incoming_bundle.action = MessageAction::Reject;
                    continue;
                }
            }
            return result;
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
    ) -> Result<(Block, ChainInfoResponse), chain_client::Error> {
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
    fn into_result(self) -> Result<(), chain_client::Error> {
        match self {
            Self::OldEpoch => Err(chain_client::Error::CommitteeDeprecationError),
            Self::New => Ok(()),
            Self::FutureEpoch => Err(chain_client::Error::CommitteeSynchronizationError),
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

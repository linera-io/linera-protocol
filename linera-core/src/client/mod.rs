// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashSet},
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
    crypto::{CryptoHash, Signer, ValidatorPublicKey},
    data_types::{ArithmeticError, Blob, BlockHeight, ChainDescription, Epoch},
    ensure,
    identifiers::{AccountOwner, BlobId, BlobType, ChainId, ModuleId, StreamId},
    time::Duration,
};
#[cfg(not(target_arch = "wasm32"))]
use linera_base::{data_types::Bytecode, vm::VmRuntime};
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
    data_types::{ChainInfo, ChainInfoQuery, ChainInfoResponse, RoundTimeout},
    environment::Environment,
    local_node::{LocalChainInfoExt as _, LocalNodeClient, LocalNodeError},
    node::{CrossChainMessageDelivery, NodeError, ValidatorNode, ValidatorNodeProvider as _},
    notifier::ChannelNotifier,
    remote_node::RemoteNode,
    updater::{communicate_with_quorum, CommunicateAction, ValidatorUpdater},
    worker::{Notification, ProcessableCertificate, WorkerError, WorkerState},
    CHAIN_INFO_MAX_RECEIVED_LOG_ENTRIES,
};

mod chain_client;
pub use chain_client::*;
#[cfg(test)]
#[path = "../unit_tests/client_tests.rs"]
mod client_tests;
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

/// Defines how we listen to a chain:
/// - do we care about every block notification?
/// - or do we only care about blocks containing events from some particular streams?
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListeningMode {
    /// Listen to everything.
    FullChain,
    /// Only listen to blocks which contain events from those streams.
    EventsOnly(BTreeSet<StreamId>),
}

impl PartialOrd for ListeningMode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (ListeningMode::FullChain, ListeningMode::FullChain) => Some(Ordering::Equal),
            (ListeningMode::FullChain, _) => Some(Ordering::Greater),
            (_, ListeningMode::FullChain) => Some(Ordering::Less),
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
    pub fn extend(&mut self, other: Option<ListeningMode>) {
        match (self, other) {
            (_, None) => (),
            (ListeningMode::FullChain, _) => (),
            (mode, Some(ListeningMode::FullChain)) => {
                *mode = ListeningMode::FullChain;
            }
            (
                ListeningMode::EventsOnly(self_events),
                Some(ListeningMode::EventsOnly(other_events)),
            ) => {
                self_events.extend(other_events);
            }
        }
    }
}

/// A builder that creates [`ChainClient`]s which share the cache and notifiers.
pub struct Client<Env: Environment> {
    environment: Env,
    /// Local node to manage the execution state and the local storage of the chains that we are
    /// tracking.
    local_node: LocalNodeClient<Env::Storage>,
    /// The admin chain ID.
    admin_id: ChainId,
    /// Chains that should be tracked by the client.
    // TODO(#2412): Merge with set of chains the client is receiving notifications from validators
    tracked_chains: Arc<RwLock<HashSet<ChainId>>>,
    /// References to clients waiting for chain notifications.
    notifier: Arc<ChannelNotifier<Notification>>,
    /// Chain state for the managed chains.
    chains: papaya::HashMap<ChainId, chain_client::State>,
    /// Configuration options.
    options: ChainClientOptions,
}

impl<Env: Environment> Client<Env> {
    /// Creates a new `Client` with a new cache and notifiers.
    #[instrument(level = "trace", skip_all)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        environment: Env,
        admin_id: ChainId,
        long_lived_services: bool,
        tracked_chains: impl IntoIterator<Item = ChainId>,
        name: impl Into<String>,
        chain_worker_ttl: Duration,
        sender_chain_worker_ttl: Duration,
        options: ChainClientOptions,
        block_cache_size: usize,
        execution_state_cache_size: usize,
    ) -> Self {
        let tracked_chains = Arc::new(RwLock::new(tracked_chains.into_iter().collect()));
        let state = WorkerState::new_for_client(
            name.into(),
            environment.storage().clone(),
            tracked_chains.clone(),
            block_cache_size,
            execution_state_cache_size,
        )
        .with_long_lived_services(long_lived_services)
        .with_allow_inactive_chains(true)
        .with_allow_messages_from_deprecated_epochs(true)
        .with_chain_worker_ttl(chain_worker_ttl)
        .with_sender_chain_worker_ttl(sender_chain_worker_ttl);
        let local_node = LocalNodeClient::new(state);

        Self {
            environment,
            local_node,
            chains: papaya::HashMap::new(),
            admin_id,
            tracked_chains,
            notifier: Arc::new(ChannelNotifier::default()),
            options,
        }
    }

    /// Returns the storage client used by this client's local node.
    pub fn storage_client(&self) -> &Env::Storage {
        self.environment.storage()
    }

    pub fn validator_node_provider(&self) -> &Env::Network {
        self.environment.network()
    }

    /// Returns a reference to the [`Signer`] of the client.
    #[instrument(level = "trace", skip(self))]
    pub fn signer(&self) -> &impl Signer {
        self.environment.signer()
    }

    /// Adds a chain to the set of chains tracked by the local node.
    #[instrument(level = "trace", skip(self))]
    pub fn track_chain(&self, chain_id: ChainId) {
        self.tracked_chains
            .write()
            .expect("Panics should not happen while holding a lock to `tracked_chains`")
            .insert(chain_id);
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
        self.chains.pin().get_or_insert_with(chain_id, || {
            chain_client::State::new(pending_proposal.clone())
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
                self.synchronize_chain_state(self.admin_id).await?;
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
    ) -> Result<Box<ChainInfo>, ChainClientError> {
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
                Err(err) => info!(
                    "Failed to download certificates from validator {:?}: {err}",
                    remote_node.public_key
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
        let certificates = self
            .storage_client()
            .read_certificates(hashes.clone())
            .await?;
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
            let certificates = remote_node
                .query_certificates_from(chain_id, next_height, limit)
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
        remote_node: &RemoteNode<impl ValidatorNode>,
        blob_ids: impl IntoIterator<Item = BlobId>,
    ) -> Result<(), ChainClientError> {
        self.local_node
            .store_blobs(
                &futures::stream::iter(blob_ids.into_iter().map(|blob_id| async move {
                    remote_node.try_download_blob(blob_id).await.unwrap()
                }))
                .buffer_unordered(self.options.max_joined_tasks)
                .collect::<Vec<_>>()
                .await,
            )
            .await
            .map_err(Into::into)
    }

    /// Tries to process all the certificates, requesting any missing blobs from the given node.
    /// Returns the chain info of the last successfully processed certificate.
    #[instrument(level = "trace", skip_all)]
    async fn process_certificates(
        &self,
        remote_node: &RemoteNode<impl ValidatorNode>,
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
                self.download_blobs(remote_node, blob_ids).await?;
            }
            x => {
                x?;
            }
        }

        for certificate in certificates {
            info = Some(
                match self.handle_certificate(certificate.clone()).await {
                    Err(LocalNodeError::BlobsNotFound(blob_ids)) => {
                        self.download_blobs(remote_node, blob_ids).await?;
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
        let info = self.chain_info_with_committees(self.admin_id).await?;
        Ok((info.epoch, info.into_committees()?))
    }

    /// Obtains the committee for the latest epoch on the admin chain.
    pub async fn admin_committee(&self) -> Result<(Epoch, Committee), LocalNodeError> {
        let info = self.chain_info_with_committees(self.admin_id).await?;
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
    /// client's `ChainId`.
    pub async fn get_chain_description(
        &self,
        chain_id: ChainId,
    ) -> Result<ChainDescription, ChainClientError> {
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
        self.synchronize_chain_state(self.admin_id).await?;
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
    async fn finalize_block(
        &self,
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
        &self,
        committee: &Committee,
        proposal: Box<BlockProposal>,
        value: T,
    ) -> Result<GenericCertificate<T>, ChainClientError> {
        debug!(
            round = %proposal.content.round,
            "Submitting block proposal to validators"
        );
        let submit_action = CommunicateAction::SubmitBlock {
            proposal,
            blob_ids: value.required_blob_ids().into_iter().collect(),
        };
        let certificate = self
            .communicate_chain_action(committee, submit_action, value)
            .await?;
        self.process_certificate(Box::new(certificate.clone()))
            .await?;
        Ok(certificate)
    }

    /// Broadcasts certified blocks to validators.
    #[instrument(level = "trace", skip_all, fields(chain_id, block_height, delivery))]
    async fn communicate_chain_updates(
        &self,
        committee: &Committee,
        chain_id: ChainId,
        height: BlockHeight,
        delivery: CrossChainMessageDelivery,
    ) -> Result<(), ChainClientError> {
        let nodes = self.make_nodes(committee)?;
        communicate_with_quorum(
            &nodes,
            committee,
            |_: &()| (),
            |remote_node| {
                let mut updater = ValidatorUpdater {
                    remote_node,
                    local_node: self.local_node.clone(),
                    admin_id: self.admin_id,
                };
                Box::pin(async move {
                    updater
                        .send_chain_information(chain_id, height, delivery)
                        .await
                })
            },
            self.options.grace_period,
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
        &self,
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
                    local_node: self.local_node.clone(),
                    admin_id: self.admin_id,
                };
                let action = action.clone();
                Box::pin(async move { updater.send_chain_update(action).await })
            },
            self.options.grace_period,
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
                    let blobs = RemoteNode::download_blobs(
                        blob_ids,
                        &self.validator_nodes().await?,
                        self.options.blob_download_timeout,
                    )
                    .await
                    .ok_or(err)?;
                    self.local_node.store_blobs(&blobs).await?;
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
                    let blobs = RemoteNode::download_blobs(
                        blob_ids,
                        &nodes,
                        self.options.blob_download_timeout,
                    )
                    .await
                    .ok_or(err)?;
                    self.local_node.store_blobs(&blobs).await?;
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
                    let certificates = remote_node
                        .download_certificates_by_heights(sender_chain_id, remote_heights)
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
                ?remote_node,
                %received_entries,
                "get_received_log_from_validator: received log batch",
            );
            if received_entries < CHAIN_INFO_MAX_RECEIVED_LOG_ENTRIES {
                break;
            }
        }

        trace!(
            ?remote_node,
            num_entries = %remote_log.len(),
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
            let downloaded = remote_node
                .download_certificates_by_heights(sender_chain_id, vec![current_height])
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
    #[instrument(level = "trace", skip_all)]
    async fn synchronize_chain_state(
        &self,
        chain_id: ChainId,
    ) -> Result<Box<ChainInfo>, ChainClientError> {
        let (_, committee) = self.admin_committee().await?;
        self.synchronize_chain_state_from_committee(chain_id, committee)
            .await
    }

    /// Downloads and processes any certificates we are missing for the given chain, from the given
    /// committee.
    #[instrument(level = "trace", skip_all)]
    pub async fn synchronize_chain_state_from_committee(
        &self,
        chain_id: ChainId,
        committee: Committee,
    ) -> Result<Box<ChainInfo>, ChainClientError> {
        #[cfg(with_metrics)]
        let _latency = metrics::SYNCHRONIZE_CHAIN_STATE_LATENCY.measure_latency();

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
            self.options.grace_period,
        )
        .await?;

        self.local_node
            .chain_info(chain_id)
            .await
            .map_err(Into::into)
    }

    /// Downloads any certificates from the specified validator that we are missing for the given
    /// chain, and processes them.
    #[instrument(level = "trace", skip(self, remote_node, chain_id))]
    async fn synchronize_chain_state_from(
        &self,
        remote_node: &RemoteNode<Env::ValidatorNode>,
        chain_id: ChainId,
    ) -> Result<(), ChainClientError> {
        let mut local_info = self.local_node.chain_info(chain_id).await?;
        let query = ChainInfoQuery::new(chain_id).with_manager_values();
        let remote_info = remote_node.handle_chain_info_query(query).await?;
        if let Some(new_info) = self
            .download_certificates_from(remote_node, chain_id, remote_info.next_block_height)
            .await?
        {
            local_info = new_info;
        };

        // If we are at the same height as the remote node, we also update our chain manager.
        if local_info.next_block_height != remote_info.next_block_height {
            debug!(
                "Synced from validator {}; but remote height is {} and local height is {}",
                remote_node.public_key, remote_info.next_block_height, local_info.next_block_height
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
                    if let Err(err) = self.try_process_locking_block_from(remote_node, cert).await {
                        debug!(
                            "Skipping locked block {hash} from validator {} at height {}: {err}",
                            remote_node.public_key, local_info.next_block_height,
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
                            let blob_content = match remote_node
                                .node
                                .download_pending_blob(chain_id, blob_id)
                                .await
                            {
                                Ok(content) => content,
                                Err(err) => {
                                    info!(
                                        "Skipping proposal from {owner} and validator {} at \
                                        height {}; failed to download {blob_id}: {err}",
                                        remote_node.public_key, local_info.next_block_height
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
                            &[remote_node.clone()],
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
                    "Skipping proposal from {owner} and validator {} at height {}: {err}",
                    remote_node.public_key, local_info.next_block_height
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
                    let blob_content = remote_node
                        .node
                        .download_pending_blob(chain_id, blob_id)
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
                    let certificate = remote_node.download_certificate_for_blob(blob_id).await?;
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
    // TODO(#2806): Measure how failing messages affect the execution times.
    #[tracing::instrument(level = "trace", skip(self, block))]
    async fn stage_block_execution_and_discard_failing_messages(
        &self,
        mut block: ProposedBlock,
        round: Option<u32>,
        published_blobs: Vec<Blob>,
    ) -> Result<(Block, ChainInfoResponse), ChainClientError> {
        loop {
            let result = self
                .stage_block_execution(block.clone(), round, published_blobs.clone())
                .await;
            if let Err(ChainClientError::LocalNodeError(LocalNodeError::WorkerError(
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
                    let Transaction::ReceiveMessages(message) = transaction else {
                        panic!(
                            "Expected incoming bundle at transaction index {}, found operation",
                            index
                        );
                    };
                    ensure!(
                        !message.bundle.is_protected(),
                        ChainClientError::BlockProposalError(
                            "Protected incoming message failed to execute locally"
                        )
                    );
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
            return Ok(result?);
        }
    }
}

/// Performs `f` in parallel on multiple nodes, starting with a quadratically increasing delay on
/// each subsequent node. Returns error `err` is all of the nodes fail.
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

/// The outcome of trying to commit a list of incoming messages and operations to the chain.
#[derive(Debug)]
enum ExecuteBlockOutcome {
    /// A block with the messages and operations was committed.
    Executed(ConfirmedBlockCertificate),
    /// A different block was already proposed and got committed. Check whether the messages and
    /// operations are still suitable, and try again at the next block height.
    Conflict(ConfirmedBlockCertificate),
    /// We are not the round leader and cannot do anything. Try again at the specified time or
    /// or whenever the round or block height changes.
    WaitForTimeout(RoundTimeout),
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

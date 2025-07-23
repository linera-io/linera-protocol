// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{hash_map, BTreeMap, BTreeSet, HashMap, HashSet},
    convert::Infallible,
    iter,
    ops::{Deref, DerefMut},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use chain_client_state::ChainClientState;
use custom_debug_derive::Debug;
use dashmap::{
    mapref::one::{MappedRef as DashMapMappedRef, Ref as DashMapRef, RefMut as DashMapRefMut},
    DashMap,
};
use futures::{
    future::{self, Either, FusedFuture, Future},
    stream::{self, AbortHandle, FusedStream, FuturesUnordered, StreamExt},
};
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use linera_base::{
    abi::Abi,
    crypto::{signer, AccountPublicKey, CryptoHash, Signer, ValidatorPublicKey},
    data_types::{
        Amount, ApplicationPermissions, ArithmeticError, Blob, BlobContent, BlockHeight,
        ChainDescription, Epoch, Round, Timestamp,
    },
    ensure,
    identifiers::{
        Account, AccountOwner, ApplicationId, BlobId, BlobType, ChainId, EventId, IndexAndEvent,
        ModuleId, StreamId,
    },
    ownership::{ChainOwnership, TimeoutConfig},
};
#[cfg(not(target_arch = "wasm32"))]
use linera_base::{data_types::Bytecode, vm::VmRuntime};
use linera_chain::{
    data_types::{
        BlockProposal, ChainAndHeight, IncomingBundle, LiteVote, MessageAction, ProposedBlock,
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
        AdminOperation, OpenChainConfig, Recipient, SystemOperation, EPOCH_STREAM_NAME,
        REMOVED_EPOCH_STREAM_NAME,
    },
    ExecutionError, Operation, Query, QueryOutcome, QueryResponse, SystemQuery, SystemResponse,
};
use linera_storage::{Clock as _, ResultReadCertificates, Storage as _};
use linera_views::ViewError;
use rand::prelude::SliceRandom as _;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{mpsc, OwnedRwLockReadGuard};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, error, info, instrument, warn, Instrument as _};

use crate::{
    data_types::{
        BlockHeightRange, ChainInfo, ChainInfoQuery, ChainInfoResponse, ClientOutcome, RoundTimeout,
    },
    environment::Environment,
    local_node::{LocalChainInfoExt as _, LocalNodeClient, LocalNodeError},
    node::{
        CrossChainMessageDelivery, NodeError, NotificationStream, ValidatorNode,
        ValidatorNodeProvider as _,
    },
    notifier::ChannelNotifier,
    remote_node::RemoteNode,
    updater::{communicate_with_quorum, CommunicateAction, CommunicationError, ValidatorUpdater},
    worker::{Notification, ProcessableCertificate, Reason, WorkerError, WorkerState},
};

mod chain_client_state;
#[cfg(test)]
#[path = "../unit_tests/client_tests.rs"]
mod client_tests;

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
    chains: DashMap<ChainId, ChainClientState>,
    /// Configuration options.
    options: ChainClientOptions,
}

impl<Env: Environment> Client<Env> {
    /// Creates a new `Client` with a new cache and notifiers.
    #[instrument(level = "trace", skip_all)]
    pub fn new(
        environment: Env,
        admin_id: ChainId,
        long_lived_services: bool,
        tracked_chains: impl IntoIterator<Item = ChainId>,
        name: impl Into<String>,
        chain_worker_ttl: Duration,
        options: ChainClientOptions,
    ) -> Self {
        let tracked_chains = Arc::new(RwLock::new(tracked_chains.into_iter().collect()));
        let state = WorkerState::new_for_client(
            name.into(),
            environment.storage().clone(),
            tracked_chains.clone(),
        )
        .with_long_lived_services(long_lived_services)
        .with_allow_inactive_chains(true)
        .with_allow_messages_from_deprecated_epochs(true)
        .with_chain_worker_ttl(chain_worker_ttl);
        let local_node = LocalNodeClient::new(state);

        Self {
            environment,
            local_node,
            chains: DashMap::new(),
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

    /// Returns a reference to the [`LocalNodeClient`] of the client.
    #[instrument(level = "trace", skip(self))]
    pub fn local_node(&self) -> &LocalNodeClient<Env::Storage> {
        &self.local_node
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
        if let dashmap::mapref::entry::Entry::Vacant(e) = self.chains.entry(chain_id) {
            e.insert(ChainClientState::new(pending_proposal));
        }

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
        let mut info = self.fetch_chain_info(chain_id, &validators).await?;
        for remote_node in validators {
            if target_next_block_height <= info.next_block_height {
                return Ok(info);
            }
            match self
                .download_certificates_from(&remote_node, chain_id, target_next_block_height)
                .await
            {
                Err(err) => warn!(
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
        let mut hashes = Vec::new();
        let mut next_height = BlockHeight::ZERO;
        {
            let chain = self.local_node.chain_state_view(chain_id).await?;
            next_height = next_height.max(chain.tip_state.get().next_block_height);
            while next_height < stop {
                let Some(hash) = chain.preprocessed_blocks.get(&next_height).await? else {
                    break;
                };
                hashes.push(hash);
                next_height = next_height.try_add_one()?;
            }
        }
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
            last_info = Some(self.handle_certificate(Box::new(certificate)).await?.info);
        }
        // Now download the rest in batches from the remote node.
        while next_height < stop {
            // TODO(#2045): Analyze network errors instead of guessing the batch size.
            let limit = u64::from(stop)
                .checked_sub(u64::from(next_height))
                .ok_or(ArithmeticError::Overflow)?
                .min(1000);
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

    /// Tries to process all the certificates, requesting any missing blobs from the given node.
    /// Returns the chain info of the last successfully processed certificate.
    #[instrument(level = "trace", skip_all)]
    async fn process_certificates(
        &self,
        remote_node: &RemoteNode<impl ValidatorNode>,
        certificates: Vec<ConfirmedBlockCertificate>,
    ) -> Result<Option<Box<ChainInfo>>, ChainClientError> {
        let mut info = None;
        for certificate in certificates {
            let certificate = Box::new(certificate);
            let mut result = self.handle_certificate(certificate.clone()).await;

            if let Err(LocalNodeError::BlobsNotFound(blob_ids)) = &result {
                future::try_join_all(blob_ids.iter().map(|blob_id| async move {
                    let blob_certificate =
                        remote_node.download_certificate_for_blob(*blob_id).await?;
                    self.receive_sender_certificate(
                        blob_certificate,
                        ReceiveCertificateMode::NeedsCheck,
                        None,
                    )
                    .await?;
                    Result::<(), ChainClientError>::Ok(())
                }))
                .await?;
                result = self.handle_certificate(certificate.clone()).await;
            }

            info = Some(result?.info);
        }
        // Done with all certificates.
        Ok(info)
    }

    async fn handle_certificate<T: ProcessableCertificate>(
        &self,
        certificate: Box<GenericCertificate<T>>,
    ) -> Result<ChainInfoResponse, LocalNodeError> {
        self.local_node
            .handle_certificate(*certificate, &self.notifier)
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
        if let Some(mut state) = self.chains.get_mut(&info.chain_id) {
            state.value_mut().update_from_info(info);
        }
    }

    /// Handles the certificate in the local node and the resulting notifications.
    #[instrument(level = "trace", skip_all)]
    async fn process_certificate<T: ProcessableCertificate>(
        &self,
        certificate: Box<GenericCertificate<T>>,
    ) -> Result<(), LocalNodeError> {
        let info = self.handle_certificate(certificate).await?.info;
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
        self.receive_certificate_and_update_validators(
            certificate.clone(),
            ReceiveCertificateMode::AlreadyChecked,
        )
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
        let local_node = self.local_node.clone();
        let nodes = self.make_nodes(committee)?;
        communicate_with_quorum(
            &nodes,
            committee,
            |_: &()| (),
            |remote_node| {
                let mut updater = ValidatorUpdater {
                    remote_node,
                    local_node: local_node.clone(),
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
        let local_node = self.local_node.clone();
        let nodes = self.make_nodes(committee)?;
        let ((votes_hash, votes_round), votes) = communicate_with_quorum(
            &nodes,
            committee,
            |vote: &LiteVote| (vote.value.value_hash, vote.round),
            |remote_node| {
                let mut updater = ValidatorUpdater {
                    remote_node,
                    local_node: local_node.clone(),
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

    /// Processes the confirmed block certificate and its ancestors in the local node, then
    /// updates the validators up to that certificate.
    #[instrument(level = "trace", skip_all)]
    async fn receive_certificate_and_update_validators(
        &self,
        certificate: ConfirmedBlockCertificate,
        mode: ReceiveCertificateMode,
    ) -> Result<(), ChainClientError> {
        let block_chain_id = certificate.block().header.chain_id;
        let block_height = certificate.block().header.height;

        self.receive_certificate(certificate, mode).await?;

        // Make sure a quorum of validators (according to the chain's new committee) are up-to-date
        // for data availability.
        let local_committee = self
            .chain_info_with_committees(block_chain_id)
            .await?
            .into_current_committee()?;
        self.communicate_chain_updates(
            &local_committee,
            block_chain_id,
            block_height.try_add_one()?,
            CrossChainMessageDelivery::Blocking,
        )
        .await?;
        Ok(())
    }

    /// Processes the confirmed block certificate in the local node. Also downloads and processes
    /// all ancestors that are still missing.
    #[instrument(level = "trace", skip_all)]
    async fn receive_certificate(
        &self,
        certificate: ConfirmedBlockCertificate,
        mode: ReceiveCertificateMode,
    ) -> Result<(), ChainClientError> {
        let certificate = Box::new(certificate);
        let block = certificate.block();

        // Verify the certificate before doing any expensive networking.
        let (max_epoch, committees) = self.admin_committees().await?;
        if let ReceiveCertificateMode::NeedsCheck = mode {
            Self::check_certificate(max_epoch, &committees, &certificate)?.into_result()?;
        }
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

    /// Processes the confirmed block in the local node without executing it.
    #[instrument(level = "trace", skip_all)]
    #[allow(dead_code)] // Otherwise CI fails when built for docker.
    async fn receive_sender_certificate(
        &self,
        certificate: ConfirmedBlockCertificate,
        mode: ReceiveCertificateMode,
        nodes: Option<Vec<RemoteNode<Env::ValidatorNode>>>,
    ) -> Result<(), ChainClientError> {
        let certificate = Box::new(certificate);

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

    /// Downloads and preprocesses all confirmed block certificates that sent any message to this
    /// chain.
    #[instrument(level = "trace", skip(self))]
    async fn synchronize_received_certificates_from_validator(
        &self,
        chain_id: ChainId,
        remote_node: &RemoteNode<Env::ValidatorNode>,
    ) -> Result<ReceivedCertificatesFromValidator, ChainClientError> {
        let mut tracker = self
            .local_node
            .chain_state_view(chain_id)
            .await?
            .received_certificate_trackers
            .get()
            .get(&remote_node.public_key)
            .copied()
            .unwrap_or(0);
        let (max_epoch, committees) = self.admin_committees().await?;

        // Retrieve the list of newly received certificates from this validator.
        let query = ChainInfoQuery::new(chain_id).with_received_log_excluding_first_n(tracker);
        let info = remote_node.handle_chain_info_query(query).await?;
        let remote_log = info.requested_received_log;
        let remote_heights = Self::heights_per_chain(&remote_log);

        // Obtain the next block height we need in the local node, for each chain.
        let local_next_heights = self
            .local_node
            .next_outbox_heights(remote_heights.keys(), chain_id)
            .await?;

        // We keep track of the height we've successfully downloaded and checked, per chain.
        let mut downloaded_heights = BTreeMap::new();
        // And we make a list of chains we already fully have locally. We need to make sure to
        // put all their sent messages into the inbox.
        let mut other_sender_chains = Vec::new();

        let certificate_hashes = future::try_join_all(remote_heights.into_iter().filter_map(
            |(sender_chain_id, remote_heights)| {
                let local_next = *local_next_heights.get(&sender_chain_id)?;
                if let Ok(height) = local_next.try_sub_one() {
                    downloaded_heights.insert(sender_chain_id, height);
                }
                let remote_heights = remote_heights
                    .into_iter()
                    .filter(|h| *h >= local_next)
                    .collect::<Vec<_>>();
                if remote_heights.is_empty() {
                    // Our highest, locally executed block is higher than any block height
                    // from the current batch. Skip this batch, but remember to wait for
                    // the messages to be delivered to the inboxes.
                    other_sender_chains.push(sender_chain_id);
                    return None;
                };
                let height0 = *remote_heights.first()?;
                let height1 = *remote_heights.last()?;

                // Find the hashes of the blocks we need.
                let range = BlockHeightRange::multi(height0, height1.0 + 1 - height0.0);
                Some(async move {
                    let hashes = remote_node
                        .fetch_sent_certificate_hashes(sender_chain_id, range)
                        .await?;
                    Ok::<_, ChainClientError>(
                        remote_heights
                            .into_iter()
                            .filter_map(move |h| hashes.get((h.0 - height0.0) as usize).copied()),
                    )
                })
            },
        ))
        .await?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

        let local_certificates = self
            .storage_client()
            .read_certificates(certificate_hashes.clone())
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let local_certificate_hashes = local_certificates
            .iter()
            .map(|cert| cert.hash())
            .collect::<HashSet<_>>();

        // Download the block certificates.
        let remote_certificates = remote_node
            .download_certificates(
                certificate_hashes
                    .into_iter()
                    .filter(|hash| !local_certificate_hashes.contains(hash))
                    .collect(),
            )
            .await?;
        let mut certificates_by_height_by_chain = BTreeMap::new();

        for confirmed_block_certificate in local_certificates {
            let block_header = &confirmed_block_certificate.inner().block().header;
            let sender_chain_id = block_header.chain_id;
            let height = block_header.height;
            certificates_by_height_by_chain
                .entry(sender_chain_id)
                .or_insert_with(BTreeMap::new)
                .insert(height, confirmed_block_certificate);
        }

        // Check the signatures and keep only the ones that are valid.
        for confirmed_block_certificate in remote_certificates {
            let block_header = &confirmed_block_certificate.inner().block().header;
            let sender_chain_id = block_header.chain_id;
            let height = block_header.height;
            let epoch = block_header.epoch;
            match Self::check_certificate(max_epoch, &committees, &confirmed_block_certificate)? {
                CheckCertificateResult::FutureEpoch => {
                    warn!(
                        "Postponing received certificate from {sender_chain_id:.8} at height \
                         {height} from future epoch {epoch}"
                    );
                    // Do not process this certificate now. It can still be
                    // downloaded later, once our committee is updated.
                }
                CheckCertificateResult::OldEpoch => {
                    // This epoch is not recognized any more. Let's skip the certificate.
                    // If a higher block with a recognized epoch comes up later from the
                    // same chain, the call to `receive_certificate` below will download
                    // the skipped certificate again.
                    warn!("Skipping received certificate from past epoch {epoch:?}");
                }
                CheckCertificateResult::New => {
                    certificates_by_height_by_chain
                        .entry(sender_chain_id)
                        .or_insert_with(BTreeMap::new)
                        .insert(height, confirmed_block_certificate);
                }
            }
        }

        // Increase the tracker up to the first position we haven't downloaded.
        for entry in remote_log {
            if certificates_by_height_by_chain
                .get(&entry.chain_id)
                .is_some_and(|certs| certs.contains_key(&entry.height))
            {
                tracker += 1;
            } else {
                break;
            }
        }

        for (sender_chain_id, certs) in &mut certificates_by_height_by_chain {
            if certs
                .values()
                .any(|cert| !cert.block().recipients().contains(&chain_id))
            {
                warn!(
                    "Skipping received certificates from chain {sender_chain_id:.8}:
                    No messages for {chain_id:.8}."
                );
                certs.clear();
            }
        }

        Ok(ReceivedCertificatesFromValidator {
            public_key: remote_node.public_key,
            tracker,
            certificates: certificates_by_height_by_chain
                .into_values()
                .flat_map(BTreeMap::into_values)
                .collect(),
            other_sender_chains,
        })
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

    /// Given a set of chain ID-block height pairs, returns a map that assigns to each chain ID
    /// the set of heights. The returned map contains no empty values.
    fn heights_per_chain(
        remote_log: &[ChainAndHeight],
    ) -> BTreeMap<ChainId, BTreeSet<BlockHeight>> {
        remote_log.iter().fold(
            BTreeMap::<ChainId, BTreeSet<_>>::new(),
            |mut chain_to_info, entry| {
                chain_to_info
                    .entry(entry.chain_id)
                    .or_default()
                    .insert(entry.height);
                chain_to_info
            },
        )
    }

    /// Downloads and processes any certificates we are missing for the given chain.
    #[instrument(level = "trace", skip_all)]
    async fn synchronize_chain_state(
        &self,
        chain_id: ChainId,
    ) -> Result<Box<ChainInfo>, ChainClientError> {
        #[cfg(with_metrics)]
        let _latency = metrics::SYNCHRONIZE_CHAIN_STATE_LATENCY.measure_latency();

        let (_, committee) = self.admin_committee().await?;
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
            return Ok(());
        };

        if let Some(timeout) = remote_info.manager.timeout {
            self.handle_certificate(Box::new(*timeout)).await?;
        }
        let mut proposals = Vec::new();
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
                        warn!(
                            "Skipping certificate {hash} from validator {}: {err}",
                            remote_node.public_key
                        );
                    }
                }
            }
        }
        for proposal in proposals {
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
                                    let public_key = &remote_node.public_key;
                                    warn!("Skipping proposal from {owner} and validator {public_key}: {err}");
                                    continue;
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

                let public_key = &remote_node.public_key;
                warn!("Skipping proposal from {owner} and validator {public_key}: {err}");
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

    /// Downloads and processes from the specified validator a confirmed block certificates that
    /// use the given blobs. If this succeeds, the blob will be in our storage.
    async fn update_local_node_with_blobs_from(
        &self,
        blob_ids: Vec<BlobId>,
        remote_nodes: &[RemoteNode<Env::ValidatorNode>],
    ) -> Result<Vec<Blob>, ChainClientError> {
        let timeout = self.options.blob_download_timeout;
        future::try_join_all(blob_ids.into_iter().map(|blob_id| async move {
            let mut stream = remote_nodes
                .iter()
                .zip(0..)
                .map(|(remote_node, i)| async move {
                    linera_base::time::timer::sleep(timeout * i * i).await;
                    let certificate = remote_node.download_certificate_for_blob(blob_id).await?;
                    // This will download all ancestors of the certificate and process all of them locally.
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
                })
                .collect::<FuturesUnordered<_>>();
            while let Some(maybe_blob) = stream.next().await {
                if let Ok(blob) = maybe_blob {
                    return Ok(blob);
                }
            }
            Err(LocalNodeError::BlobsNotFound(vec![blob_id]).into())
        }))
        .await
    }

    /// Downloads and processes confirmed block certificates that use the given blobs.
    /// If this succeeds, the blobs will be in our storage.
    async fn receive_certificates_for_blobs(
        &self,
        blob_ids: Vec<BlobId>,
    ) -> Result<(), ChainClientError> {
        // Deduplicate IDs.
        let blob_ids = blob_ids.into_iter().collect::<BTreeSet<_>>();
        let validators = self.validator_nodes().await?;

        let mut missing_blobs = Vec::new();
        for blob_id in blob_ids {
            let mut certificate_stream = validators
                .iter()
                .map(|remote_node| async move {
                    let cert = remote_node.download_certificate_for_blob(blob_id).await?;
                    Ok::<_, NodeError>((remote_node.clone(), cert))
                })
                .collect::<FuturesUnordered<_>>();
            loop {
                let Some(result) = certificate_stream.next().await else {
                    missing_blobs.push(blob_id);
                    break;
                };
                if let Ok((remote_node, cert)) = result {
                    if self
                        .receive_sender_certificate(
                            cert,
                            ReceiveCertificateMode::NeedsCheck,
                            Some(vec![remote_node]),
                        )
                        .await
                        .is_ok()
                    {
                        break;
                    }
                }
            }
        }

        if missing_blobs.is_empty() {
            Ok(())
        } else {
            Err(NodeError::BlobsNotFound(missing_blobs).into())
        }
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
                    let message = block
                        .incoming_bundles
                        .get_mut(*index as usize)
                        .expect("Message at given index should exist");
                    if message.bundle.is_protected() {
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
                self.receive_certificates_for_blobs(blob_ids.clone())
                    .await?;
                continue; // We found the missing blob: retry.
            }
            return Ok(result?);
        }
    }
}

/// Policies for automatically handling incoming messages.
#[derive(Clone, Debug)]
pub struct MessagePolicy {
    /// The blanket policy applied to all messages.
    blanket: BlanketMessagePolicy,
    /// A collection of chains which restrict the origin of messages to be
    /// accepted. `Option::None` means that messages from all chains are accepted. An empty
    /// `HashSet` denotes that messages from no chains are accepted.
    restrict_chain_ids_to: Option<HashSet<ChainId>>,
}

#[derive(Copy, Clone, Debug, clap::ValueEnum)]
pub enum BlanketMessagePolicy {
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
    pub fn new(
        blanket: BlanketMessagePolicy,
        restrict_chain_ids_to: Option<HashSet<ChainId>>,
    ) -> Self {
        Self {
            blanket,
            restrict_chain_ids_to,
        }
    }

    #[cfg(with_testing)]
    pub fn new_accept_all() -> Self {
        Self {
            blanket: BlanketMessagePolicy::Accept,
            restrict_chain_ids_to: None,
        }
    }

    #[instrument(level = "trace", skip(self))]
    fn must_handle(&self, bundle: &mut IncomingBundle) -> bool {
        if self.is_reject() {
            if bundle.bundle.is_skippable() {
                return false;
            } else if bundle.bundle.is_tracked() {
                bundle.action = MessageAction::Reject;
            }
        }
        match &self.restrict_chain_ids_to {
            None => true,
            Some(chains) => chains.contains(&bundle.origin),
        }
    }

    #[instrument(level = "trace", skip(self))]
    fn is_ignore(&self) -> bool {
        matches!(self.blanket, BlanketMessagePolicy::Ignore)
    }

    #[instrument(level = "trace", skip(self))]
    fn is_reject(&self) -> bool {
        matches!(self.blanket, BlanketMessagePolicy::Reject)
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
    /// The policy for automatically handling incoming messages.
    pub message_policy: MessagePolicy,
    /// Whether to block on cross-chain message delivery.
    pub cross_chain_message_delivery: CrossChainMessageDelivery,
    /// An additional delay, after reaching a quorum, to wait for additional validator signatures,
    /// as a fraction of time taken to reach quorum.
    pub grace_period: f64,
    /// The delay when downloading a blob, after which we try a second validator.
    pub blob_download_timeout: Duration,
}

#[cfg(with_testing)]
impl ChainClientOptions {
    pub fn test_default() -> Self {
        use crate::DEFAULT_GRACE_PERIOD;

        ChainClientOptions {
            max_pending_message_bundles: 10,
            message_policy: MessagePolicy::new_accept_all(),
            cross_chain_message_delivery: CrossChainMessageDelivery::NonBlocking,
            grace_period: DEFAULT_GRACE_PERIOD,
            blob_download_timeout: Duration::from_secs(1),
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
         {target_next_block_height} of chain {chain_id:?}"
    )]
    CannotDownloadCertificates {
        chain_id: ChainId,
        target_next_block_height: BlockHeight,
    },

    #[error(transparent)]
    BcsError(#[from] bcs::Error),

    #[error(
        "Unexpected quorum: validators voted for block {hash} in {round}, \
         expected block {expected_hash} in {expected_round}"
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

// We never want to pass the DashMap references over an `await` point, for fear of
// deadlocks. The following construct will cause a (relatively) helpful error if we do.

pub struct Unsend<T> {
    inner: T,
    _phantom: std::marker::PhantomData<*mut u8>,
}

impl<T> Unsend<T> {
    fn new(inner: T) -> Self {
        Self {
            inner,
            _phantom: Default::default(),
        }
    }
}

impl<T: Deref> Deref for Unsend<T> {
    type Target = T::Target;
    fn deref(&self) -> &T::Target {
        self.inner.deref()
    }
}

impl<T: DerefMut> DerefMut for Unsend<T> {
    fn deref_mut(&mut self) -> &mut T::Target {
        self.inner.deref_mut()
    }
}

pub type ChainGuard<'a, T> = Unsend<DashMapRef<'a, ChainId, T>>;
pub type ChainGuardMut<'a, T> = Unsend<DashMapRefMut<'a, ChainId, T>>;
pub type ChainGuardMapped<'a, T> = Unsend<DashMapMappedRef<'a, ChainId, ChainClientState, T>>;

impl<Env: Environment> ChainClient<Env> {
    /// Gets a shared reference to the chain's state.
    #[instrument(level = "trace", skip(self))]
    pub fn state(&self) -> ChainGuard<ChainClientState> {
        Unsend::new(
            self.client
                .chains
                .get(&self.chain_id)
                .expect("Chain client constructed for invalid chain"),
        )
    }

    /// Gets a mutable reference to the state.
    /// Beware: this will block any other reference to any chain's state!
    #[instrument(level = "trace", skip(self))]
    fn state_mut(&self) -> ChainGuardMut<ChainClientState> {
        Unsend::new(
            self.client
                .chains
                .get_mut(&self.chain_id)
                .expect("Chain client constructed for invalid chain"),
        )
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
    pub fn admin_id(&self) -> ChainId {
        self.client.admin_id
    }

    /// Gets a guarded reference to the next pending block.
    #[instrument(level = "trace", skip(self))]
    pub fn pending_proposal(&self) -> ChainGuardMapped<Option<PendingProposal>> {
        Unsend::new(self.state().inner.map(|state| state.pending_proposal()))
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
        let mut publishers = self
            .chain_state_view()
            .await?
            .execution_state
            .system
            .event_subscriptions
            .indices()
            .await?
            .into_iter()
            .map(|(chain_id, _)| chain_id)
            .collect::<BTreeSet<_>>();
        if self.chain_id != self.client.admin_id {
            publishers.insert(self.client.admin_id);
        }
        Ok(publishers)
    }

    /// Subscribes to notifications from this client's chain.
    #[instrument(level = "trace")]
    pub async fn subscribe(&self) -> Result<NotificationStream, LocalNodeError> {
        self.subscribe_to(self.chain_id).await
    }

    /// Subscribes to notifications from the specified chain.
    #[instrument(level = "trace")]
    pub async fn subscribe_to(
        &self,
        chain_id: ChainId,
    ) -> Result<NotificationStream, LocalNodeError> {
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
    async fn chain_info_with_manager_values(&self) -> Result<Box<ChainInfo>, LocalNodeError> {
        let query = ChainInfoQuery::new(self.chain_id).with_manager_values();
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

    /// Obtains up to `self.options.max_pending_message_bundles` pending message bundles for the
    /// local chain.
    #[instrument(level = "trace")]
    pub async fn pending_message_bundles(&self) -> Result<Vec<IncomingBundle>, ChainClientError> {
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
        {
            ensure!(
                self.has_other_owners(&info.manager.ownership)
                    || info.next_block_height >= self.initial_next_block_height,
                ChainClientError::WalletSynchronizationError
            );
        }

        Ok(info
            .requested_pending_message_bundles
            .into_iter()
            .filter_map(|mut bundle| {
                self.options
                    .message_policy
                    .must_handle(&mut bundle)
                    .then_some(bundle)
            })
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
            .chain_state_view()
            .await?
            .execution_state
            .system
            .event_subscriptions
            .index_values()
            .await?;
        // Collect the indices of all new events.
        let futures = subscription_map
            .into_iter()
            .map(|((chain_id, stream_id), subscriptions)| {
                let client = self.client.clone();
                async move {
                    let chain = client.local_node.chain_state_view(chain_id).await?;
                    if let Some(next_index) = chain
                        .execution_state
                        .stream_event_counts
                        .get(&stream_id)
                        .await?
                        .filter(|next_index| *next_index > subscriptions.next_index)
                    {
                        Ok(Some((chain_id, stream_id, next_index)))
                    } else {
                        Ok::<_, ChainClientError>(None)
                    }
                }
            });
        let updates = future::try_join_all(futures)
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
    pub async fn local_committee(&self) -> Result<Committee, LocalNodeError> {
        self.chain_info_with_committees()
            .await?
            .into_current_committee()
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

        let is_owner = manager
            .ownership
            .all_owners()
            .chain(&manager.leader)
            .any(|owner| *owner == preferred_owner);

        if !is_owner {
            let accepted_owners = manager
                .ownership
                .all_owners()
                .chain(&manager.leader)
                .collect::<Vec<_>>();
            warn!(%self.chain_id, ?accepted_owners, ?preferred_owner,
                "Chain has multiple owners configured but none is preferred owner",
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
    /// its current height and are not missing any received messages from the inbox.
    #[instrument(level = "trace")]
    pub async fn prepare_chain(&self) -> Result<Box<ChainInfo>, ChainClientError> {
        #[cfg(with_metrics)]
        let _latency = metrics::PREPARE_CHAIN_LATENCY.measure_latency();

        let mut info = self.synchronize_to_known_height().await?;

        if self.has_other_owners(&info.manager.ownership) {
            // For chains with any owner other than ourselves, we could be missing recent
            // certificates created by other owners. Further synchronize blocks from the network.
            // This is a best-effort that depends on network conditions.
            info = self.client.synchronize_chain_state(self.chain_id).await?;
        }

        if info.epoch > self.client.admin_committees().await?.0 {
            self.client
                .synchronize_chain_state(self.client.admin_id)
                .await?;
        }

        let result = self
            .chain_state_view()
            .await?
            .validate_incoming_bundles()
            .await;
        if matches!(result, Err(ChainError::MissingCrossChainUpdate { .. })) {
            self.find_received_certificates().await?;
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

    /// Submits a fast block proposal to the validators.
    ///
    /// This must only be used with valid epoch and super owner.
    #[instrument(level = "trace", skip(committee, operations))]
    pub async fn submit_fast_block_proposal(
        &self,
        committee: &Committee,
        operations: &[Operation],
        incoming_bundles: &[IncomingBundle],
        super_owner: AccountOwner,
    ) -> Result<(u64, u64, u64, u64), ChainClientError> {
        let creating_proposal_start = Instant::now();
        let info = self.chain_info().await?;
        let timestamp = self.next_timestamp(incoming_bundles, info.timestamp);
        let proposed_block = ProposedBlock {
            epoch: info.epoch,
            chain_id: self.chain_id,
            incoming_bundles: incoming_bundles.to_vec(),
            operations: operations.to_vec(),
            previous_block_hash: info.block_hash,
            height: info.next_block_height,
            authenticated_signer: Some(super_owner),
            timestamp,
        };
        let proposal = Box::new(
            BlockProposal::new_initial(
                super_owner,
                Round::Fast,
                proposed_block.clone(),
                self.signer(),
            )
            .await
            .map_err(ChainClientError::signer_failure)?,
        );
        let creating_proposal_ms = creating_proposal_start.elapsed().as_millis() as u64;
        let stage_block_execution_start = Instant::now();
        let block = self
            .client
            .local_node
            .stage_block_execution(proposed_block, None, Vec::new())
            .await?
            .0;
        let stage_block_execution_ms = stage_block_execution_start.elapsed().as_millis() as u64;
        let creating_confirmed_block_start = Instant::now();
        let value = ConfirmedBlock::new(block);
        let creating_confirmed_block_ms =
            creating_confirmed_block_start.elapsed().as_millis() as u64;
        let submitting_block_proposal_start = Instant::now();
        self.client
            .submit_block_proposal(committee, proposal, value)
            .await?;
        let submitting_block_proposal_ms =
            submitting_block_proposal_start.elapsed().as_millis() as u64;
        Ok((
            creating_proposal_ms,
            stage_block_execution_ms,
            creating_confirmed_block_ms,
            submitting_block_proposal_ms,
        ))
    }

    /// Attempts to update all validators about the local chain.
    #[instrument(level = "trace", skip(old_committee))]
    pub async fn update_validators(
        &self,
        old_committee: Option<&Committee>,
    ) -> Result<(), ChainClientError> {
        // Communicate the new certificate now.
        if let Some(old_committee) = old_committee {
            self.communicate_chain_updates(old_committee).await?
        };
        if let Ok(new_committee) = self.local_committee().await {
            if Some(&new_committee) != old_committee {
                // If the configuration just changed, communicate to the new committee as well.
                // (This is actually more important that updating the previous committee.)
                self.communicate_chain_updates(&new_committee).await?;
            }
        }
        Ok(())
    }

    /// Broadcasts certified blocks to validators.
    #[instrument(level = "trace", skip(committee))]
    pub async fn communicate_chain_updates(
        &self,
        committee: &Committee,
    ) -> Result<(), ChainClientError> {
        let delivery = self.options.cross_chain_message_delivery;
        let height = self.chain_info().await?.next_block_height;
        self.client
            .communicate_chain_updates(committee, self.chain_id, height, delivery)
            .await
    }

    /// Processes the results of [`synchronize_received_certificates_from_validator`] and updates
    /// the trackers for the validators.
    #[tracing::instrument(level = "trace", skip(received_certificates_batches))]
    async fn receive_certificates_from_validators(
        &self,
        received_certificates_batches: Vec<ReceivedCertificatesFromValidator>,
    ) {
        let validator_count = received_certificates_batches.len();
        let mut other_sender_chains = BTreeSet::new();
        let mut certificates =
            BTreeMap::<ChainId, BTreeMap<BlockHeight, ConfirmedBlockCertificate>>::new();
        let mut new_trackers = BTreeMap::new();
        for response in received_certificates_batches {
            other_sender_chains.extend(response.other_sender_chains);
            new_trackers.insert(response.public_key, response.tracker);
            for certificate in response.certificates {
                certificates
                    .entry(certificate.block().header.chain_id)
                    .or_default()
                    .insert(certificate.block().header.height, certificate);
            }
        }
        let certificate_count = certificates.values().map(BTreeMap::len).sum::<usize>();

        tracing::info!(
            "Received {certificate_count} certificates from {validator_count} validator(s)."
        );

        // Process the certificates sorted by chain and in ascending order of block height.
        let stream = FuturesUnordered::from_iter(certificates.into_values().map(|certificates| {
            let client = self.client.clone();
            async move {
                for certificate in certificates.into_values() {
                    let hash = certificate.hash();
                    let mode = ReceiveCertificateMode::AlreadyChecked;
                    if let Err(err) = client
                        .receive_sender_certificate(certificate, mode, None)
                        .await
                    {
                        error!("Received invalid certificate {hash}: {err}");
                    }
                }
            }
        }));
        stream.for_each(future::ready).await;

        // Certificates for these chains were omitted from `certificates` because they were
        // already processed locally. If they were processed in a concurrent task, it is not
        // guaranteed that their cross-chain messages were already handled.
        let stream = FuturesUnordered::from_iter(other_sender_chains.into_iter().map(|chain_id| {
            let local_node = self.client.local_node.clone();
            async move {
                if let Err(error) = local_node
                    .retry_pending_cross_chain_requests(chain_id)
                    .await
                {
                    error!("Failed to retry outgoing messages from {chain_id}: {error}");
                }
            }
        }));
        stream.for_each(future::ready).await;

        // Update the trackers.
        if let Err(error) = self
            .client
            .local_node
            .update_received_certificate_trackers(self.chain_id, new_trackers)
            .await
        {
            error!(
                "Failed to update the certificate trackers for chain {:.8}: {error}",
                self.chain_id
            );
        }
    }

    /// Synchronizes all chains that any application on this chain subscribes to.
    /// We always consider the admin chain a relevant publishing chain, for new epochs.
    async fn synchronize_publisher_chains(&self) -> Result<(), ChainClientError> {
        let chain_ids = self
            .chain_state_view()
            .await?
            .execution_state
            .system
            .event_subscriptions
            .indices()
            .await?
            .iter()
            .map(|(chain_id, _)| *chain_id)
            .chain(iter::once(self.client.admin_id))
            .filter(|chain_id| *chain_id != self.chain_id)
            .collect::<BTreeSet<_>>();
        future::try_join_all(
            chain_ids
                .into_iter()
                .map(|chain_id| self.client.synchronize_chain_state(chain_id)),
        )
        .await?;
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
    async fn find_received_certificates(&self) -> Result<(), ChainClientError> {
        #[cfg(with_metrics)]
        let _latency = metrics::FIND_RECEIVED_CERTIFICATES_LATENCY.measure_latency();

        // Use network information from the local chain.
        let chain_id = self.chain_id;
        let (_, committee) = self.admin_committee().await?;
        let nodes = self.client.make_nodes(&committee)?;
        // Proceed to downloading received certificates.
        let result = communicate_with_quorum(
            &nodes,
            &committee,
            |_| (),
            |remote_node| {
                let client = &self.client;
                Box::pin(async move {
                    client
                        .synchronize_received_certificates_from_validator(chain_id, &remote_node)
                        .await
                })
            },
            self.options.grace_period,
        )
        .await;
        let received_certificate_batches = match result {
            Ok(((), received_certificate_batches)) => received_certificate_batches,
            Err(CommunicationError::Trusted(NodeError::InactiveChain(id))) if id == chain_id => {
                // The chain is visibly not active (yet or any more) so there is no need
                // to synchronize received certificates.
                return Ok(());
            }
            Err(error) => {
                return Err(error.into());
            }
        };
        self.receive_certificates_from_validators(received_certificate_batches)
            .await;
        Ok(())
    }

    /// Sends money.
    #[instrument(level = "trace")]
    pub async fn transfer(
        &self,
        owner: AccountOwner,
        amount: Amount,
        recipient: Recipient,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
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
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
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
        recipient: Recipient,
        amount: Amount,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
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

    /// Executes a list of operations.
    #[instrument(level = "trace", skip(operations, blobs))]
    pub async fn execute_operations(
        &self,
        operations: Vec<Operation>,
        blobs: Vec<Blob>,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
        let timing_start = std::time::Instant::now();

        let result = loop {
            let execute_block_start = std::time::Instant::now();
            // TODO(#2066): Remove boxing once the call-stack is shallower
            match Box::pin(self.execute_block(operations.clone(), blobs.clone())).await? {
                ExecuteBlockOutcome::Executed(certificate) => {
                    if let Some(sender) = &self.timing_sender {
                        let _ = sender.send((
                            execute_block_start.elapsed().as_millis() as u64,
                            TimingType::ExecuteBlock,
                        ));
                    }
                    break Ok(ClientOutcome::Committed(certificate));
                }
                ExecuteBlockOutcome::WaitForTimeout(timeout) => {
                    break Ok(ClientOutcome::WaitForTimeout(timeout));
                }
                ExecuteBlockOutcome::Conflict(certificate) => {
                    info!(
                        height = %certificate.block().header.height,
                        "Another block was committed; retrying."
                    );
                }
            };
        };

        if let Some(sender) = &self.timing_sender {
            let _ = sender.send((
                timing_start.elapsed().as_millis() as u64,
                TimingType::ExecuteOperations,
            ));
        }

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
    ) -> Result<ExecuteBlockOutcome, ChainClientError> {
        #[cfg(with_metrics)]
        let _latency = metrics::EXECUTE_BLOCK_LATENCY.measure_latency();

        let mutex = self.state().client_mutex();
        let _guard = mutex.lock_owned().await;
        match self.process_pending_block_without_prepare().await? {
            ClientOutcome::Committed(Some(certificate)) => {
                return Ok(ExecuteBlockOutcome::Conflict(certificate))
            }
            ClientOutcome::WaitForTimeout(timeout) => {
                return Ok(ExecuteBlockOutcome::WaitForTimeout(timeout))
            }
            ClientOutcome::Committed(None) => {}
        }

        let incoming_bundles = self.pending_message_bundles().await?;
        let identity = self.identity().await?;
        let confirmed_value = self
            .new_pending_block(incoming_bundles, operations, blobs, identity)
            .await?;

        match self.process_pending_block_without_prepare().await? {
            ClientOutcome::Committed(Some(certificate))
                if certificate.block() == confirmed_value.block() =>
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

    /// Creates a new pending block and handles the proposal in the local node.
    /// Next time `process_pending_block_without_prepare` is called, this block will be proposed
    /// to the validators.
    #[instrument(level = "trace", skip(incoming_bundles, operations, blobs))]
    async fn new_pending_block(
        &self,
        incoming_bundles: Vec<IncomingBundle>,
        operations: Vec<Operation>,
        blobs: Vec<Blob>,
        identity: AccountOwner,
    ) -> Result<ConfirmedBlock, ChainClientError> {
        ensure!(
            self.state().pending_proposal().is_none(),
            ChainClientError::BlockProposalError(
                "Client state already has a pending block; \
                    use the `linera retry-pending-block` command to commit that first"
            )
        );
        let info = self.chain_info().await?;
        let timestamp = self.next_timestamp(&incoming_bundles, info.timestamp);
        let proposed_block = ProposedBlock {
            epoch: info.epoch,
            chain_id: self.chain_id,
            incoming_bundles,
            operations,
            previous_block_hash: info.block_hash,
            height: info.next_block_height,
            authenticated_signer: Some(identity),
            timestamp,
        };

        // Use the round number assuming there are oracle responses.
        // Using the round number during execution counts as an oracle.
        // Accessing the round number in single-leader rounds where we are not the leader
        // is not currently supported.
        let round = match Self::round_for_new_proposal(&info, &identity, true)? {
            Either::Left(round) => round.multi_leader(),
            Either::Right(_) => None,
        };
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
        self.state_mut().set_pending_proposal(proposed_block, blobs);
        Ok(ConfirmedBlock::new(block))
    }

    /// Returns a suitable timestamp for the next block.
    ///
    /// This will usually be the current time according to the local clock, but may be slightly
    /// ahead to make sure it's not earlier than the incoming messages or the previous block.
    #[instrument(level = "trace", skip(incoming_bundles))]
    fn next_timestamp(
        &self,
        incoming_bundles: &[IncomingBundle],
        block_time: Timestamp,
    ) -> Timestamp {
        let local_time = self.storage_client().clock().current_time();
        incoming_bundles
            .iter()
            .map(|msg| msg.bundle.timestamp)
            .max()
            .map_or(local_time, |timestamp| timestamp.max(local_time))
            .max(block_time)
    }

    /// Queries an application.
    #[instrument(level = "trace", skip(query))]
    pub async fn query_application(&self, query: Query) -> Result<QueryOutcome, ChainClientError> {
        loop {
            let result = self
                .client
                .local_node
                .query_application(self.chain_id, query.clone())
                .await;
            if let Err(LocalNodeError::BlobsNotFound(blob_ids)) = &result {
                self.client
                    .receive_certificates_for_blobs(blob_ids.clone())
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
        } = self.query_application(Query::System(query)).await?;
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
    pub async fn query_user_application<A: Abi>(
        &self,
        application_id: ApplicationId<A>,
        query: &A::Query,
    ) -> Result<QueryOutcome<A::QueryResponse>, ChainClientError> {
        let query = Query::user(application_id, query)?;
        let QueryOutcome {
            response,
            operations,
        } = self.query_application(query).await?;
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
    pub async fn query_owner_balance(
        &self,
        owner: AccountOwner,
    ) -> Result<Amount, ChainClientError> {
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
        let timestamp = self.next_timestamp(&incoming_bundles, info.timestamp);
        let block = ProposedBlock {
            epoch: info.epoch,
            chain_id: self.chain_id,
            incoming_bundles,
            operations: Vec::new(),
            previous_block_hash: info.block_hash,
            height: info.next_block_height,
            authenticated_signer: if owner == AccountOwner::CHAIN {
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
        self.transfer(from, amount, Recipient::Account(account))
            .await
    }

    /// Burns tokens.
    #[instrument(level = "trace")]
    pub async fn burn(
        &self,
        owner: AccountOwner,
        amount: Amount,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
        self.transfer(owner, amount, Recipient::Burn).await
    }

    /// Attempts to synchronize chains that have sent us messages and populate our local
    /// inbox.
    ///
    /// To create a block that actually executes the messages in the inbox,
    /// `process_inbox` must be called separately.
    #[instrument(level = "trace")]
    pub async fn synchronize_from_validators(&self) -> Result<Box<ChainInfo>, ChainClientError> {
        let info = self.prepare_chain().await?;
        self.synchronize_publisher_chains().await?;
        self.find_received_certificates().await?;
        Ok(info)
    }

    /// Processes the last pending block
    #[instrument(level = "trace")]
    pub async fn process_pending_block(
        &self,
    ) -> Result<ClientOutcome<Option<ConfirmedBlockCertificate>>, ChainClientError> {
        self.synchronize_from_validators().await?;
        self.process_pending_block_without_prepare().await
    }

    /// Processes the last pending block. Assumes that the local chain is up to date.
    #[instrument(level = "trace")]
    async fn process_pending_block_without_prepare(
        &self,
    ) -> Result<ClientOutcome<Option<ConfirmedBlockCertificate>>, ChainClientError> {
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
        let pending_proposal = self.state().pending_proposal().clone();
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
            // Use the round number assuming there are oracle responses.
            // Using the round number during execution counts as an oracle.
            let proposed_block = pending_proposal.block;
            let round = match Self::round_for_new_proposal(&info, &owner, true)? {
                Either::Left(round) => round.multi_leader(),
                Either::Right(_) => None,
            };
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
        let round = match Self::round_for_new_proposal(&info, &owner, has_oracle_responses)? {
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
        let submit_block_proposal_start = std::time::Instant::now();
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
        if let Some(sender) = &self.timing_sender {
            let _ = sender.send((
                submit_block_proposal_start.elapsed().as_millis() as u64,
                TimingType::SubmitBlockProposal,
            ));
        }

        debug!(round = %certificate.round, "Sending confirmed block to validators");
        let update_validators_start = std::time::Instant::now();
        self.update_validators(Some(&committee)).await?;
        if let Some(sender) = &self.timing_sender {
            let _ = sender.send((
                update_validators_start.elapsed().as_millis() as u64,
                TimingType::UpdateValidators,
            ));
        }
        Ok(ClientOutcome::Committed(Some(certificate)))
    }

    /// Requests a leader timeout certificate if the current round has timed out. Returns the
    /// chain info for the (possibly new) current round.
    async fn request_leader_timeout_if_needed(&self) -> Result<Box<ChainInfo>, ChainClientError> {
        let mut info = self.chain_info_with_manager_values().await?;
        // If the current round has timed out, we request a timeout certificate and retry in
        // the next round.
        if let Some(round_timeout) = info.manager.round_timeout {
            if round_timeout <= self.storage_client().clock().current_time() {
                self.request_leader_timeout().await?;
                info = self.chain_info_with_manager_values().await?;
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
        match self
            .client
            .finalize_block(&committee, certificate.clone())
            .await
        {
            Ok(certificate) => Ok(ClientOutcome::Committed(Some(certificate))),
            Err(ChainClientError::CommunicationError(error)) => {
                // Communication errors in this case often mean that someone else already
                // finalized the block or started another round.
                let timestamp = info.manager.round_timeout.ok_or(error)?;
                Ok(ClientOutcome::WaitForTimeout(RoundTimeout {
                    timestamp,
                    current_round: info.manager.current_round,
                    next_block_height: info.next_block_height,
                }))
            }
            Err(error) => Err(error),
        }
    }

    /// Returns a round in which we can propose a new block or the given one, if possible.
    fn round_for_new_proposal(
        info: &ChainInfo,
        identity: &AccountOwner,
        has_oracle_responses: bool,
    ) -> Result<Either<Round, RoundTimeout>, ChainClientError> {
        let manager = &info.manager;
        // If there is a conflicting proposal in the current round, we can only propose if the
        // next round can be started without a timeout, i.e. if we are in a multi-leader round.
        // Similarly, we cannot propose a block that uses oracles in the fast round.
        let conflict = manager
            .requested_proposed
            .as_ref()
            .is_some_and(|proposal| proposal.content.round == manager.current_round)
            || (manager.current_round.is_fast() && has_oracle_responses);
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
        if manager.can_propose(identity, round) {
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
    #[instrument(level = "trace")]
    pub fn clear_pending_proposal(&self) {
        self.state_mut().clear_pending_proposal();
    }

    /// Processes a confirmed block for which this chain is a recipient and updates validators.
    #[instrument(
        level = "trace",
        skip(certificate),
        fields(certificate_hash = ?certificate.hash()),
    )]
    pub async fn receive_certificate_and_update_validators(
        &self,
        certificate: ConfirmedBlockCertificate,
    ) -> Result<(), ChainClientError> {
        self.client
            .receive_certificate_and_update_validators(
                certificate,
                ReceiveCertificateMode::NeedsCheck,
            )
            .await
    }

    /// Rotates the key of the chain.
    ///
    /// Replaces current owners of the chain with the new key pair.
    #[instrument(level = "trace")]
    pub async fn rotate_key_pair(
        &self,
        public_key: AccountPublicKey,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
        self.transfer_ownership(public_key.into()).await
    }

    /// Transfers ownership of the chain to a single super owner.
    #[instrument(level = "trace")]
    pub async fn transfer_ownership(
        &self,
        new_owner: AccountOwner,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
        self.execute_operation(SystemOperation::ChangeOwnership {
            super_owners: vec![new_owner],
            owners: Vec::new(),
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
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
        loop {
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
                ExecuteBlockOutcome::Executed(certificate) => {
                    return Ok(ClientOutcome::Committed(certificate));
                }
                ExecuteBlockOutcome::Conflict(certificate) => {
                    info!(
                        height = %certificate.block().header.height,
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
    #[instrument(level = "trace")]
    pub async fn change_ownership(
        &self,
        ownership: ChainOwnership,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
        self.execute_operation(SystemOperation::ChangeOwnership {
            super_owners: ownership.super_owners.into_iter().collect(),
            owners: ownership.owners.into_iter().collect(),
            multi_leader_rounds: ownership.multi_leader_rounds,
            open_multi_leader_rounds: ownership.open_multi_leader_rounds,
            timeout_config: ownership.timeout_config.clone(),
        })
        .await
    }

    /// Changes the application permissions configuration on this chain.
    #[instrument(level = "trace", skip(application_permissions))]
    pub async fn change_application_permissions(
        &self,
        application_permissions: ApplicationPermissions,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
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
    ) -> Result<ClientOutcome<(ChainDescription, ConfirmedBlockCertificate)>, ChainClientError>
    {
        loop {
            let config = OpenChainConfig {
                ownership: ownership.clone(),
                balance,
                application_permissions: application_permissions.clone(),
            };
            let operation = Operation::system(SystemOperation::OpenChain(config));
            let certificate = match self.execute_block(vec![operation], vec![]).await? {
                ExecuteBlockOutcome::Executed(certificate) => certificate,
                ExecuteBlockOutcome::Conflict(_) => continue,
                ExecuteBlockOutcome::WaitForTimeout(timeout) => {
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
            // Add the new chain to the list of tracked chains
            self.client.track_chain(description.id());
            self.client
                .local_node
                .retry_pending_cross_chain_requests(self.chain_id)
                .await?;
            return Ok(ClientOutcome::Committed((description, certificate)));
        }
    }

    /// Closes the chain (and loses everything in it!!).
    /// Returns `None` if the chain was already closed.
    #[instrument(level = "trace")]
    pub async fn close_chain(
        &self,
    ) -> Result<ClientOutcome<Option<ConfirmedBlockCertificate>>, ChainClientError> {
        match self.execute_operation(SystemOperation::CloseChain).await {
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
        self.publish_module_blobs(blobs, module_id).await
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
    ) -> Result<ClientOutcome<(ApplicationId<A>, ConfirmedBlockCertificate)>, ChainClientError>
    {
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
    ) -> Result<ClientOutcome<(ApplicationId, ConfirmedBlockCertificate)>, ChainClientError> {
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

        let mut epoch_change_ops = self.collect_epoch_changes().await?.into_iter();

        let mut certificates = Vec::new();
        loop {
            let incoming_bundles = self.pending_message_bundles().await?;
            let stream_updates = self.collect_stream_updates().await?;
            let block_operations = stream_updates
                .into_iter()
                .chain(epoch_change_ops.next())
                .collect::<Vec<_>>();
            if incoming_bundles.is_empty() && block_operations.is_empty() {
                return Ok((certificates, None));
            }
            match self.execute_block(block_operations, vec![]).await {
                Ok(ExecuteBlockOutcome::Executed(certificate))
                | Ok(ExecuteBlockOutcome::Conflict(certificate)) => certificates.push(certificate),
                Ok(ExecuteBlockOutcome::WaitForTimeout(timeout)) => {
                    return Ok((certificates, Some(timeout)));
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
            chain_id: self.client.admin_id,
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
        account: Account,
    ) -> Result<ClientOutcome<ConfirmedBlockCertificate>, ChainClientError> {
        self.execute_operation(SystemOperation::Transfer {
            owner,
            recipient: Recipient::Account(account),
            amount,
        })
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
    ) -> Option<Box<ChainInfo>> {
        let Ok(info) = local_node.chain_info(chain_id).await else {
            error!("Fail to read local chain info for {chain_id}");
            return None;
        };
        // Useful in case `chain_id` is the same as the local chain.
        self.client.update_from_info(&info);
        Some(info)
    }

    #[instrument(level = "trace", skip(chain_id, local_node))]
    async fn local_next_block_height(
        &self,
        chain_id: ChainId,
        local_node: &mut LocalNodeClient<Env::Storage>,
    ) -> BlockHeight {
        let maybe_info = self.local_chain_info(chain_id, local_node).await;
        maybe_info.map_or(BlockHeight::ZERO, |info| info.next_block_height)
    }

    #[instrument(level = "trace", skip(remote_node, local_node, notification))]
    async fn process_notification(
        &self,
        remote_node: RemoteNode<Env::ValidatorNode>,
        mut local_node: LocalNodeClient<Env::Storage>,
        notification: Notification,
    ) {
        match notification.reason {
            Reason::NewIncomingBundle { origin, height } => {
                if self.local_next_block_height(origin, &mut local_node).await > height {
                    debug!(
                        chain_id = %self.chain_id,
                        "Accepting redundant notification for new message"
                    );
                    return;
                }
                if let Err(error) = self
                    .find_received_certificates_from_validator(remote_node)
                    .await
                {
                    error!(
                        chain_id = %self.chain_id,
                        "NewIncomingBundle: Fail to process notification: {error}"
                    );
                    return;
                }
                if self.local_next_block_height(origin, &mut local_node).await <= height {
                    error!(
                        chain_id = %self.chain_id,
                        "NewIncomingBundle: Fail to synchronize new message after notification"
                    );
                }
            }
            Reason::NewBlock { height, .. } => {
                let chain_id = notification.chain_id;
                if self
                    .local_next_block_height(chain_id, &mut local_node)
                    .await
                    > height
                {
                    debug!(
                        chain_id = %self.chain_id,
                        "Accepting redundant notification for new block"
                    );
                    return;
                }
                if let Err(error) = self
                    .client
                    .synchronize_chain_state_from(&remote_node, chain_id)
                    .await
                {
                    error!(
                        chain_id = %self.chain_id,
                        "NewBlock: Fail to process notification: {error}"
                    );
                    return;
                }
                let local_height = self
                    .local_next_block_height(chain_id, &mut local_node)
                    .await;
                if local_height <= height {
                    error!("NewBlock: Fail to synchronize new block after notification");
                }
            }
            Reason::NewRound { height, round } => {
                let chain_id = notification.chain_id;
                if let Some(info) = self.local_chain_info(chain_id, &mut local_node).await {
                    if (info.next_block_height, info.manager.current_round) >= (height, round) {
                        debug!(
                            chain_id = %self.chain_id,
                            "Accepting redundant notification for new round"
                        );
                        return;
                    }
                }
                if let Err(error) = self
                    .client
                    .synchronize_chain_state_from(&remote_node, chain_id)
                    .await
                {
                    error!(
                        chain_id = %self.chain_id,
                        "NewRound: Fail to process notification: {error}"
                    );
                    return;
                }
                let Some(info) = self.local_chain_info(chain_id, &mut local_node).await else {
                    error!(
                        chain_id = %self.chain_id,
                        "NewRound: Fail to read local chain info for {chain_id}"
                    );
                    return;
                };
                if (info.next_block_height, info.manager.current_round) < (height, round) {
                    error!(
                        chain_id = %self.chain_id,
                        "NewRound: Fail to synchronize new block after notification"
                    );
                }
            }
        }
    }

    /// Returns whether this chain is tracked by the client, i.e. we are updating its inbox.
    pub fn is_tracked(&self) -> bool {
        self.client
            .tracked_chains
            .read()
            .unwrap()
            .contains(&self.chain_id)
    }

    /// Spawns a task that listens to notifications about the current chain from all validators,
    /// and synchronizes the local state accordingly.
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
        let notifications = self.subscribe().await?;
        let (abortable_notifications, abort) = stream::abortable(self.subscribe().await?);
        let sync_result = if self.is_tracked() {
            self.synchronize_from_validators().await
        } else {
            self.synchronize_chain_state(self.chain_id).await
        };
        if let Err(error) = sync_result {
            error!("Failed to synchronize from validators: {}", error);
        }

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
                    match await_while_polling(
                        this.update_notification_streams(&mut senders).fuse(),
                        &mut process_notifications,
                    )
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
        let (chain_id, nodes, local_node) = {
            let committee = self.local_committee().await?;
            let nodes: HashMap<_, _> = self
                .client
                .validator_node_provider()
                .make_nodes(&committee)?
                .collect();
            (self.chain_id, nodes, self.client.local_node.clone())
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
            let stream = stream::once({
                let node = node.clone();
                async move { node.subscribe(vec![chain_id]).await }
            })
            .filter_map(move |result| async move {
                if let Err(error) = &result {
                    warn!(?error, "Could not connect to validator {public_key}");
                } else {
                    info!("Connected to validator {public_key}");
                }
                result.ok()
            })
            .flatten();
            let (stream, abort) = stream::abortable(stream);
            let mut stream = Box::pin(stream);
            let this = self.clone();
            let local_node = local_node.clone();
            let remote_node = RemoteNode { public_key, node };
            validator_tasks.push(async move {
                while let Some(notification) = stream.next().await {
                    this.process_notification(
                        remote_node.clone(),
                        local_node.clone(),
                        notification,
                    )
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
    #[instrument(level = "trace")]
    async fn find_received_certificates_from_validator(
        &self,
        remote_node: RemoteNode<Env::ValidatorNode>,
    ) -> Result<(), ChainClientError> {
        let chain_id = self.chain_id;
        // Proceed to downloading received certificates.
        let received_certificates = self
            .client
            .synchronize_received_certificates_from_validator(chain_id, &remote_node)
            .await?;
        // Process received certificates. If the client state has changed during the
        // network calls, we should still be fine.
        self.receive_certificates_from_validators(vec![received_certificates])
            .await;
        Ok(())
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
            Ok(info) => info.info.next_block_height.0,
            Err(NodeError::BlobsNotFound(_)) => 0,
            Err(err) => return Err(err.into()),
        };
        let local_chain_state = self.chain_info().await?;

        let Some(missing_certificate_count) = local_chain_state
            .next_block_height
            .0
            .checked_sub(validator_next_block_height)
            .filter(|count| *count > 0)
        else {
            debug!("Validator is up-to-date with local state");
            return Ok(());
        };

        let missing_certificates_end = usize::try_from(local_chain_state.next_block_height.0)
            .expect("`usize` should be at least `u64`");
        let missing_certificates_start = missing_certificates_end
            - usize::try_from(missing_certificate_count).expect("`usize` should be at least `u64`");

        let missing_certificate_hashes = self
            .chain_state_view()
            .await?
            .confirmed_log
            .read(missing_certificates_start..missing_certificates_end)
            .await?;

        let certificates = self
            .client
            .storage_client()
            .read_certificates(missing_certificate_hashes.clone())
            .await?;
        let certificates =
            match ResultReadCertificates::new(certificates, missing_certificate_hashes) {
                ResultReadCertificates::Certificates(certificates) => certificates,
                ResultReadCertificates::InvalidHashes(hashes) => {
                    return Err(ChainClientError::ReadCertificatesError(hashes))
                }
            };
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

    /// Returns whether the given ownership includes anyone whose secret key we don't have.
    fn has_other_owners(&self, ownership: &ChainOwnership) -> bool {
        ownership
            .all_owners()
            .any(|owner| Some(owner) != self.preferred_owner.as_ref())
    }
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

/// The result of `synchronize_received_certificates_from_validator`.
struct ReceivedCertificatesFromValidator {
    /// The name of the validator we downloaded from.
    public_key: ValidatorPublicKey,
    /// The new tracker value for that validator.
    tracker: u64,
    /// The downloaded certificates. The signatures were already checked and they are ready
    /// to be processed.
    certificates: Vec<ConfirmedBlockCertificate>,
    /// Sender chains that were already up to date locally. We need to ensure their messages
    /// are delivered.
    other_sender_chains: Vec<ChainId>,
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

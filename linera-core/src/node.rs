// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    data_types::{BlockHeightRange, ChainInfo, ChainInfoQuery, ChainInfoResponse},
    notifier::Notifier,
    worker::{Notification, ValidatorWorker, WorkerError, WorkerState},
};
use async_trait::async_trait;
use futures::{future, lock::Mutex, Stream};
use linera_base::{
    crypto::CryptoError,
    data_types::{ArithmeticError, BlockHeight},
    identifiers::{ChainId, MessageId},
};
use linera_chain::{
    data_types::{
        Block, BlockProposal, Certificate, ExecutedBlock, HashedValue, LiteCertificate, Origin,
    },
    ChainError, ChainManagerInfo,
};
use linera_execution::{
    committee::ValidatorName, BytecodeLocation, Query, Response, UserApplicationDescription,
    UserApplicationId,
};
use linera_storage::Store;
use linera_views::views::ViewError;
use rand::prelude::SliceRandom;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, pin::Pin, sync::Arc};
use thiserror::Error;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// A pinned [`Stream`] of Notifications.
pub type NotificationStream = Pin<Box<dyn Stream<Item = Notification> + Send>>;

/// How to communicate with a validator or a local node.
#[async_trait]
pub trait ValidatorNode {
    /// Proposes a new block.
    async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Processes a certificate without a value.
    async fn handle_lite_certificate(
        &mut self,
        certificate: LiteCertificate<'_>,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Processes a certificate.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
        blobs: Vec<HashedValue>,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Handles information queries for this chain.
    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Subscribes to receiving notifications for a collection of chains.
    async fn subscribe(&mut self, chains: Vec<ChainId>) -> Result<NotificationStream, NodeError>;
}

/// Error type for node queries.
///
/// This error is meant to be serialized over the network and aggregated by clients (i.e.
/// clients will track validator votes on each error value).
#[derive(Eq, PartialEq, Clone, Debug, Serialize, Deserialize, Error, Hash)]
pub enum NodeError {
    #[error("Cryptographic error: {error}")]
    CryptoError { error: String },

    #[error("Arithmetic error: {error}")]
    ArithmeticError { error: String },

    #[error("Error while accessing storage: {error}")]
    ViewError { error: String },

    #[error("Chain error: {error}")]
    ChainError { error: String },

    #[error("Worker error: {error}")]
    WorkerError { error: String },

    #[error("Grpc error: {error}")]
    GrpcError { error: String },

    // This error must be normalized during conversions.
    #[error("The chain {0:?} is not active in validator")]
    InactiveChain(ChainId),

    // This error must be normalized during conversions.
    #[error(
        "Cannot vote for block proposal of chain {chain_id:?} because a message \
         from chain {origin:?} at height {height:?} has not been received yet"
    )]
    MissingCrossChainUpdate {
        chain_id: ChainId,
        origin: Origin,
        height: BlockHeight,
    },

    // This error must be normalized during conversions.
    #[error("The following values containing application bytecode are missing: {0:?}.")]
    ApplicationBytecodesNotFound(Vec<BytecodeLocation>),

    #[error(
        "Failed to download the requested certificate(s) for chain {chain_id:?} \
         in order to advance to the next height {target_next_block_height}"
    )]
    CannotDownloadCertificates {
        chain_id: ChainId,
        target_next_block_height: BlockHeight,
    },

    #[error("Validator's response to block proposal failed to include a vote")]
    MissingVoteInValidatorResponse,

    // This should go to ClientError.
    #[error(
        "Failed to update validator because our local node doesn't have an active chain {0:?}"
    )]
    InactiveLocalChain(ChainId),

    // This should go to ClientError.
    #[error("The block does contain the hash that we expected for the previous block")]
    InvalidLocalBlockChaining,

    // This should go to ClientError.
    #[error("The given chain info response is invalid")]
    InvalidChainInfoResponse,

    #[error(
        "Failed to submit block proposal: chain {chain_id:?} was still inactive \
         after validator synchronization and {retries} retries"
    )]
    ProposedBlockToInactiveChain { chain_id: ChainId, retries: usize },

    #[error(
        "Failed to submit block proposal: chain {chain_id:?} was still missing messages \
         after validator synchronization and {retries} retries"
    )]
    ProposedBlockWithLaggingMessages { chain_id: ChainId, retries: usize },

    #[error(
        "Failed to submit block proposal: chain {chain_id:?} was still missing application bytecodes \
         after validator synchronization and {retries} retries"
    )]
    ProposedBlockWithLaggingBytecode { chain_id: ChainId, retries: usize },

    // Networking and sharding.
    // TODO(#258): Those probably belong to linera-service.
    #[error("Cannot deserialize")]
    InvalidDecoding,
    #[error("Unexpected message")]
    UnexpectedMessage,
    #[error("Network error while querying service: {error}")]
    ClientIoError { error: String },
    #[error("Failed to resolve validator address: {address}")]
    CannotResolveValidatorAddress { address: String },
    #[error("Subscription error due to incorrect transport. Was expecting gRPC, instead found: {transport}")]
    SubscriptionError { transport: String },
    #[error("Failed to subscribe; tonic status: {status}")]
    SubscriptionFailed { status: String },
    #[error("We don't have the value for the certificate.")]
    MissingCertificateValue,
}

impl From<ViewError> for NodeError {
    fn from(error: ViewError) -> Self {
        Self::ViewError {
            error: error.to_string(),
        }
    }
}

impl From<ArithmeticError> for NodeError {
    fn from(error: ArithmeticError) -> Self {
        Self::ArithmeticError {
            error: error.to_string(),
        }
    }
}

impl From<CryptoError> for NodeError {
    fn from(error: CryptoError) -> Self {
        Self::CryptoError {
            error: error.to_string(),
        }
    }
}

impl From<ChainError> for NodeError {
    fn from(error: ChainError) -> Self {
        match error {
            ChainError::MissingCrossChainUpdate {
                chain_id,
                origin,
                height,
            } => Self::MissingCrossChainUpdate {
                chain_id,
                origin: *origin,
                height,
            },
            ChainError::InactiveChain(chain_id) => Self::InactiveChain(chain_id),
            error => Self::ChainError {
                error: error.to_string(),
            },
        }
    }
}

impl From<WorkerError> for NodeError {
    fn from(error: WorkerError) -> Self {
        match error {
            WorkerError::ChainError(error) => (*error).into(),
            WorkerError::MissingCertificateValue => Self::MissingCertificateValue,
            WorkerError::ApplicationBytecodesNotFound(locations) => {
                NodeError::ApplicationBytecodesNotFound(locations)
            }
            error => Self::WorkerError {
                error: error.to_string(),
            },
        }
    }
}

/// A local replica, typically used by clients.
pub struct LocalNode<S> {
    state: WorkerState<S>,
}

#[derive(Clone)]
pub struct LocalNodeClient<S> {
    node: Arc<Mutex<LocalNode<S>>>,
    notifier: Notifier<Notification>,
}

#[async_trait]
impl<S> ValidatorNode for LocalNodeClient<S>
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, NodeError> {
        let mut node = self.node.lock().await;
        // In local nodes, we can trust fully_handle_certificate to carry all actions eventually.
        let (response, _actions) = node.state.handle_block_proposal(proposal).await?;
        Ok(response)
    }

    async fn handle_lite_certificate(
        &mut self,
        certificate: LiteCertificate<'_>,
    ) -> Result<ChainInfoResponse, NodeError> {
        let mut node = self.node.lock().await;
        let mut notifications = Vec::new();
        let full_cert = node.state.full_certificate(certificate).await?;
        let response = node
            .state
            .fully_handle_certificate_with_notifications(
                full_cert,
                vec![],
                Some(&mut notifications),
            )
            .await?;
        for notification in notifications {
            self.notifier
                .notify(&notification.chain_id.clone(), notification);
        }
        Ok(response)
    }

    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
        blobs: Vec<HashedValue>,
    ) -> Result<ChainInfoResponse, NodeError> {
        let mut node = self.node.lock().await;
        let mut notifications = Vec::new();
        let response = node
            .state
            .fully_handle_certificate_with_notifications(
                certificate,
                blobs,
                Some(&mut notifications),
            )
            .await?;
        for notification in notifications {
            self.notifier
                .notify(&notification.chain_id.clone(), notification);
        }
        Ok(response)
    }

    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, NodeError> {
        let node = self.node.lock().await;
        // In local nodes, we can trust fully_handle_certificate to carry all actions eventually.
        let (response, _actions) = node.state.handle_chain_info_query(query).await?;
        Ok(response)
    }

    async fn subscribe(&mut self, chains: Vec<ChainId>) -> Result<NotificationStream, NodeError> {
        let rx = self.notifier.subscribe(chains);
        Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    }
}

impl<S> LocalNodeClient<S> {
    pub fn new(state: WorkerState<S>) -> Self {
        let node = LocalNode { state };
        Self {
            node: Arc::new(Mutex::new(node)),
            notifier: Notifier::default(),
        }
    }
}

impl<S> LocalNodeClient<S>
where
    S: Clone,
{
    pub(crate) async fn storage_client(&self) -> S {
        let node = self.node.lock().await;
        node.state.storage_client().clone()
    }
}

impl<S> LocalNodeClient<S>
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    pub(crate) async fn stage_block_execution(
        &self,
        block: Block,
    ) -> Result<(ExecutedBlock, ChainInfoResponse), NodeError> {
        let mut node = self.node.lock().await;
        let (executed_block, info) = node.state.stage_block_execution(block).await?;
        Ok((executed_block, info))
    }

    async fn try_process_certificates<A>(
        &mut self,
        name: ValidatorName,
        client: &mut A,
        chain_id: ChainId,
        certificates: Vec<Certificate>,
    ) -> Option<ChainInfo>
    where
        A: ValidatorNode + Send + Sync + 'static + Clone,
    {
        let mut info = None;
        for certificate in certificates {
            let hash = certificate.hash();
            if !certificate.value().is_confirmed() || certificate.value().chain_id() != chain_id {
                // The certificate is not as expected. Give up.
                tracing::warn!("Failed to process network certificate {}", hash);
                return info;
            }
            let mut result = self.handle_certificate(certificate.clone(), vec![]).await;
            if let Err(NodeError::ApplicationBytecodesNotFound(locations)) = &result {
                let chain_id = certificate.value().chain_id();
                let mut blobs = Vec::new();
                for maybe_blob in future::join_all(locations.iter().map(|location| {
                    let mut client = client.clone();
                    async move {
                        Self::try_download_blob_from(name, &mut client, chain_id, *location).await
                    }
                }))
                .await
                {
                    if let Some(blob) = maybe_blob {
                        blobs.push(blob);
                    } else {
                        // The certificate is not as expected. Give up.
                        tracing::warn!("Failed to process network blob");
                        return info;
                    }
                }
                result = self.handle_certificate(certificate.clone(), blobs).await;
            }
            match result {
                Ok(response) => info = Some(response.info),
                Err(error) => {
                    // The certificate is not as expected. Give up.
                    tracing::warn!("Failed to process network certificate {}: {}", hash, error);
                    return info;
                }
            };
        }
        // Done with all certificates.
        info
    }

    pub(crate) async fn local_chain_info(
        &mut self,
        chain_id: ChainId,
    ) -> Result<ChainInfo, NodeError> {
        let query = ChainInfoQuery::new(chain_id);
        Ok(self.handle_chain_info_query(query).await?.info)
    }

    pub async fn query_application(
        &self,
        chain_id: ChainId,
        query: &Query,
    ) -> Result<Response, NodeError> {
        let mut node = self.node.lock().await;
        let response = node.state.query_application(chain_id, query).await?;
        Ok(response)
    }

    pub async fn describe_application(
        &self,
        chain_id: ChainId,
        application_id: UserApplicationId,
    ) -> Result<UserApplicationDescription, NodeError> {
        let mut node = self.node.lock().await;
        let response = node
            .state
            .describe_application(chain_id, application_id)
            .await?;
        Ok(response)
    }

    pub async fn download_certificates<A>(
        &mut self,
        mut validators: Vec<(ValidatorName, A)>,
        chain_id: ChainId,
        target_next_block_height: BlockHeight,
    ) -> Result<ChainInfo, NodeError>
    where
        A: ValidatorNode + Send + Sync + 'static + Clone,
    {
        // Sequentially try each validator in random order.
        validators.shuffle(&mut rand::thread_rng());
        for (name, client) in validators {
            let info = self.local_chain_info(chain_id).await?;
            if target_next_block_height <= info.next_block_height {
                return Ok(info);
            }
            self.try_download_certificates_from(
                name,
                client,
                chain_id,
                info.next_block_height,
                target_next_block_height,
            )
            .await?;
        }
        let info = self.local_chain_info(chain_id).await?;
        if target_next_block_height <= info.next_block_height {
            Ok(info)
        } else {
            Err(NodeError::CannotDownloadCertificates {
                chain_id,
                target_next_block_height,
            })
        }
    }

    /// Downloads and stores the specified blobs, unless they are already in the cache or storage.
    ///
    /// Does not fail if a blob can't be downloaded; it just gets omitted from the result.
    pub async fn read_or_download_blobs<A>(
        &mut self,
        validators: Vec<(ValidatorName, A)>,
        blob_locations: impl IntoIterator<Item = (BytecodeLocation, ChainId)>,
    ) -> Result<Vec<HashedValue>, NodeError>
    where
        A: ValidatorNode + Send + Sync + 'static + Clone,
    {
        let mut blobs = vec![];
        let mut tasks = vec![];
        let mut node = self.node.lock().await;
        for (location, chain_id) in blob_locations {
            if let Some(blob) = node.state.recent_value(&location.certificate_hash).await {
                blobs.push(blob);
            } else {
                let validators = validators.clone();
                let storage = node.state.storage_client().clone();
                tasks.push(Self::read_or_download_blob(
                    storage, validators, chain_id, location,
                ));
            }
        }
        drop(node); // Free the lock while awaiting the tasks.
        if tasks.is_empty() {
            return Ok(blobs);
        }
        let results = future::join_all(tasks).await;
        let mut node = self.node.lock().await;
        for result in results {
            if let Some(blob) = result? {
                node.state.cache_recent_value(Cow::Borrowed(&blob)).await;
                blobs.push(blob);
            }
        }
        Ok(blobs)
    }

    pub async fn read_or_download_blob<A>(
        storage: S,
        validators: Vec<(ValidatorName, A)>,
        chain_id: ChainId,
        location: BytecodeLocation,
    ) -> Result<Option<HashedValue>, NodeError>
    where
        A: ValidatorNode + Send + Sync + 'static + Clone,
    {
        match storage.read_value(location.certificate_hash).await {
            Ok(blob) => return Ok(Some(blob)),
            Err(ViewError::NotFound(..)) => {}
            Err(err) => Err(err)?,
        }
        match Self::download_blob(validators, chain_id, location).await {
            Some(blob) => {
                storage.write_value(&blob).await?;
                Ok(Some(blob))
            }
            None => Ok(None),
        }
    }

    /// Obtains the certificate containing the specified message.
    pub async fn certificate_for(
        &mut self,
        message_id: &MessageId,
    ) -> Result<Certificate, NodeError> {
        let query = ChainInfoQuery::new(message_id.chain_id)
            .with_sent_certificates_in_range(BlockHeightRange::single(message_id.height));
        let info = self.handle_chain_info_query(query).await?.info;
        let certificate = info
            .requested_sent_certificates
            .into_iter()
            .find(|certificate| certificate.value().has_message(message_id))
            .ok_or_else(|| {
                ViewError::not_found("could not find certificate with message {}", message_id)
            })?;
        Ok(certificate)
    }

    async fn try_download_certificates_from<A>(
        &mut self,
        name: ValidatorName,
        mut client: A,
        chain_id: ChainId,
        start: BlockHeight,
        stop: BlockHeight,
    ) -> Result<(), NodeError>
    where
        A: ValidatorNode + Send + Sync + 'static + Clone,
    {
        let limit = u64::from(stop)
            .checked_sub(u64::from(start))
            .ok_or(ArithmeticError::Overflow)?;
        let range = BlockHeightRange {
            start,
            limit: Some(limit),
        };
        let query = ChainInfoQuery::new(chain_id).with_sent_certificates_in_range(range);
        if let Ok(response) = client.handle_chain_info_query(query).await {
            if response.check(name).is_ok() {
                let ChainInfo {
                    requested_sent_certificates,
                    ..
                } = response.info;
                self.try_process_certificates(
                    name,
                    &mut client,
                    chain_id,
                    requested_sent_certificates,
                )
                .await;
            }
        }
        Ok(())
    }

    pub async fn synchronize_chain_state<A>(
        &mut self,
        validators: Vec<(ValidatorName, A)>,
        chain_id: ChainId,
    ) -> Result<ChainInfo, NodeError>
    where
        A: ValidatorNode + Send + Sync + 'static + Clone,
    {
        let futures: Vec<_> = validators
            .into_iter()
            .map(|(name, client)| {
                let mut node = self.clone();
                async move {
                    node.try_synchronize_chain_state_from(name, client, chain_id)
                        .await
                }
            })
            .collect();
        futures::future::join_all(futures).await;
        let info = self.local_chain_info(chain_id).await?;
        Ok(info)
    }

    pub async fn try_synchronize_chain_state_from<A>(
        &mut self,
        name: ValidatorName,
        mut client: A,
        chain_id: ChainId,
    ) -> Result<(), NodeError>
    where
        A: ValidatorNode + Send + Sync + 'static + Clone,
    {
        let local_info = self.local_chain_info(chain_id).await?;
        let range = BlockHeightRange {
            start: local_info.next_block_height,
            limit: None,
        };
        let query = ChainInfoQuery::new(chain_id)
            .with_sent_certificates_in_range(range)
            .with_manager_values();
        let info = match client.handle_chain_info_query(query).await {
            Ok(response) if response.check(name).is_ok() => response.info,
            Ok(_) => {
                tracing::warn!("Ignoring invalid response from validator");
                // Give up on this validator.
                return Ok(());
            }
            Err(err) => {
                tracing::warn!("Ignoring error from validator: {}", err);
                return Ok(());
            }
        };
        if self
            .try_process_certificates(
                name,
                &mut client,
                chain_id,
                info.requested_sent_certificates,
            )
            .await
            .is_none()
        {
            return Ok(());
        };
        if let ChainManagerInfo::Multi(manager) = info.manager {
            if let Some(proposal) = manager.requested_proposed {
                if proposal.content.block.chain_id == chain_id {
                    let owner = proposal.owner;
                    if let Err(error) = self.handle_block_proposal(proposal).await {
                        tracing::warn!("Skipping proposal from {}: {}", owner, error);
                    }
                }
            }
            if let Some(cert) = manager.requested_locked {
                if cert.value().is_validated() && cert.value().chain_id() == chain_id {
                    let hash = cert.hash();
                    if let Err(error) = self.handle_certificate(cert, vec![]).await {
                        tracing::warn!("Skipping certificate {}: {}", hash, error);
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn download_blob<A>(
        mut validators: Vec<(ValidatorName, A)>,
        chain_id: ChainId,
        location: BytecodeLocation,
    ) -> Option<HashedValue>
    where
        A: ValidatorNode + Send + Sync + 'static + Clone,
    {
        // Sequentially try each validator in random order.
        validators.shuffle(&mut rand::thread_rng());
        for (name, mut client) in validators {
            if let Some(blob) =
                Self::try_download_blob_from(name, &mut client, chain_id, location).await
            {
                return Some(blob);
            }
        }
        None
    }

    async fn try_download_blob_from<A>(
        name: ValidatorName,
        client: &mut A,
        chain_id: ChainId,
        location: BytecodeLocation,
    ) -> Option<HashedValue>
    where
        A: ValidatorNode + Send + Sync + 'static + Clone,
    {
        let query = ChainInfoQuery::new(chain_id).with_blob(location.certificate_hash);
        if let Ok(response) = client.handle_chain_info_query(query).await {
            if response.check(name).is_ok() {
                return response.info.requested_blob;
            }
        }
        None
    }
}

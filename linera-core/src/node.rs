// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    data_types::{BlockHeightRange, ChainInfo, ChainInfoQuery, ChainInfoResponse},
    notifier::Notifier,
    worker::{Notification, ValidatorWorker, WorkerError, WorkerState},
};
use async_trait::async_trait;
use futures::{lock::Mutex, Stream};
use linera_base::{
    crypto::CryptoError,
    data_types::{ArithmeticError, BlockHeight, ChainId, ValidatorName},
};
use linera_chain::{
    data_types::{Block, BlockProposal, Certificate, LiteCertificate, Origin, Value},
    ChainError, ChainManagerInfo,
};
use linera_execution::{
    ApplicationId, BytecodeLocation, Destination, Effect, ExecutionError, Query, Response,
    UserApplicationId,
};
use linera_storage::Store;
use linera_views::views::ViewError;
use rand::prelude::SliceRandom;
use serde::{Deserialize, Serialize};
use std::{pin::Pin, sync::Arc};
use thiserror::Error;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// A pinned [`Stream`] of Notifications.
pub type NotificationStream = Pin<Box<dyn Stream<Item = Notification> + Send>>;

/// How to communicate with a validator or a local node.
#[async_trait]
pub trait ValidatorNode {
    /// Propose a new block.
    async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Process a certificate without a value.
    async fn handle_lite_certificate(
        &mut self,
        certificate: LiteCertificate,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Process a certificate.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Handle information queries for this chain.
    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Subscribe to receiving notifications for a collection of chains.
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
         from chain {origin:?} at height {height:?} (application {application_id:?}) \
         has not been received yet"
    )]
    MissingCrossChainUpdate {
        chain_id: ChainId,
        application_id: Box<ApplicationId>,
        origin: Origin,
        height: BlockHeight,
    },

    // This error must be normalized during conversions.
    #[error("Failed to load bytecode of {application_id:?} from storage {bytecode_location:?}")]
    ApplicationBytecodeNotFound {
        application_id: UserApplicationId,
        bytecode_location: BytecodeLocation,
    },

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

    // Networking and sharding. TODO: those probably belong to linera-service
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
                application_id,
                origin,
                height,
            } => Self::MissingCrossChainUpdate {
                chain_id,
                application_id: Box::new(application_id),
                origin,
                height,
            },
            ChainError::InactiveChain(chain_id) => Self::InactiveChain(chain_id),
            ChainError::ExecutionError(ExecutionError::ApplicationBytecodeNotFound(app)) => {
                NodeError::ApplicationBytecodeNotFound {
                    application_id: (&*app).into(),
                    bytecode_location: app.bytecode_location,
                }
            }
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
        let node = self.node.clone();
        let mut node = node.lock().await;
        let response = node.state.handle_block_proposal(proposal).await?;
        Ok(response)
    }

    async fn handle_lite_certificate(
        &mut self,
        certificate: LiteCertificate,
    ) -> Result<ChainInfoResponse, NodeError> {
        let node = self.node.clone();
        let mut node = node.lock().await;
        let mut notifications = Vec::new();
        let value = node
            .state
            .recent_value(&certificate.value.value_hash)
            .ok_or(NodeError::MissingCertificateValue)?
            .clone();
        let full_cert = certificate
            .with_value(value)
            .ok_or(WorkerError::InvalidLiteCertificate)?;
        let response = node
            .state
            .fully_handle_certificate_with_notifications(full_cert, Some(&mut notifications))
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
    ) -> Result<ChainInfoResponse, NodeError> {
        let node = self.node.clone();
        let mut node = node.lock().await;
        let mut notifications = Vec::new();
        let response = node
            .state
            .fully_handle_certificate_with_notifications(certificate, Some(&mut notifications))
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
        let response = self
            .node
            .clone()
            .lock()
            .await
            .state
            .handle_chain_info_query(query)
            .await?;
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
            notifier: Default::default(),
        }
    }
}

impl<S> LocalNodeClient<S>
where
    S: Clone,
{
    pub(crate) async fn storage_client(&self) -> S {
        let node = self.node.clone();
        let node = node.lock().await;
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
        block: &Block,
    ) -> Result<(Vec<(ApplicationId, Destination, Effect)>, ChainInfoResponse), NodeError> {
        let (effects, info) = self
            .node
            .clone()
            .lock()
            .await
            .state
            .stage_block_execution(block)
            .await?;
        Ok((effects, info))
    }

    async fn try_process_certificates(
        &mut self,
        chain_id: ChainId,
        certificates: Vec<Certificate>,
    ) -> Option<ChainInfo> {
        let mut info = None;
        for certificate in certificates {
            if let Value::ConfirmedBlock { block, .. } = &certificate.value {
                if block.chain_id == chain_id {
                    match self.handle_certificate(certificate.clone()).await {
                        Ok(response) => {
                            info = Some(response.info);
                            // Continue with the next certificate.
                            continue;
                        }
                        Err(e) => {
                            // The certificate is not as expected. Give up.
                            log::warn!(
                                "Failed to process network certificate {}: {}",
                                certificate.hash,
                                e
                            );
                            return info;
                        }
                    }
                }
            }
            // The certificate is not as expected. Give up.
            log::warn!("Failed to process network certificate {}", certificate.hash);
            return info;
        }
        // Done with all certificates.
        info
    }

    async fn local_chain_info(&mut self, chain_id: ChainId) -> Result<ChainInfo, NodeError> {
        let query = ChainInfoQuery::new(chain_id);
        Ok(self.handle_chain_info_query(query).await?.info)
    }

    pub async fn query_application(
        &mut self,
        chain_id: ChainId,
        application_id: ApplicationId,
        query: &Query,
    ) -> Result<Response, NodeError> {
        let node = self.node.clone();
        let mut node = node.lock().await;
        let response = node
            .state
            .query_application(chain_id, application_id, query)
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
        let range = BlockHeightRange {
            start,
            limit: Some(usize::from(stop) - usize::from(start)),
        };
        let query = ChainInfoQuery::new(chain_id).with_sent_certificates_in_range(range);
        if let Ok(response) = client.handle_chain_info_query(query).await {
            if response.check(name).is_ok() {
                let ChainInfo {
                    requested_sent_certificates,
                    ..
                } = response.info;
                self.try_process_certificates(chain_id, requested_sent_certificates)
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
                log::warn!("Ignoring invalid response from validator");
                // Give up on this validator.
                return Ok(());
            }
            Err(err) => {
                log::warn!("Ignoring error from validator: {}", err);
                return Ok(());
            }
        };
        if self
            .try_process_certificates(chain_id, info.requested_sent_certificates)
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
                        log::warn!("Skipping proposal from {}: {}", owner, error);
                    }
                }
            }
            if let Some(cert) = manager.requested_locked {
                if let Value::ValidatedBlock { block, .. } = &cert.value {
                    if block.chain_id == chain_id {
                        let hash = cert.hash;
                        if let Err(error) = self.handle_certificate(cert).await {
                            log::warn!("Skipping certificate {}: {}", hash, error);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

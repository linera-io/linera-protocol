// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashSet, VecDeque},
    future::Future,
    sync::Arc,
};

use futures::{future, stream::FuturesUnordered};
use linera_base::{
    data_types::{ArithmeticError, Blob, BlockHeight, UserApplicationDescription},
    identifiers::{BlobId, ChainId, MessageId, UserApplicationId},
};
use linera_chain::{
    data_types::{
        Block, BlockProposal, Certificate, CertificateValue, ExecutedBlock, LiteCertificate,
    },
    ChainError, ChainStateView,
};
use linera_execution::{ExecutionError, Query, Response, SystemExecutionError};
use linera_storage::Storage;
use linera_views::views::ViewError;
use rand::{prelude::SliceRandom, thread_rng};
use thiserror::Error;
use tokio::sync::OwnedRwLockReadGuard;
use tracing::{instrument, warn};

use crate::{
    data_types::{BlockHeightRange, ChainInfo, ChainInfoQuery, ChainInfoResponse},
    node::{NodeError, ValidatorNode},
    notifier::Notifier,
    remote_node::RemoteNode,
    worker::{WorkerError, WorkerState},
};

/// A local node with a single worker, typically used by clients.
pub struct LocalNode<S>
where
    S: Storage,
{
    state: WorkerState<S>,
}

/// A client to a local node.
#[derive(Clone)]
pub struct LocalNodeClient<S>
where
    S: Storage,
{
    node: Arc<LocalNode<S>>,
}

/// Error type for the operations on a local node.
#[derive(Debug, Error)]
pub enum LocalNodeError {
    #[error(transparent)]
    ArithmeticError(#[from] ArithmeticError),

    #[error(transparent)]
    ViewError(#[from] linera_views::views::ViewError),

    #[error("Local node operation failed: {0}")]
    WorkerError(#[from] WorkerError),

    #[error(
        "Failed to download certificates and update local node to the next height \
         {target_next_block_height} of chain {chain_id:?}"
    )]
    CannotDownloadCertificates {
        chain_id: ChainId,
        target_next_block_height: BlockHeight,
    },

    #[error("Failed to read blob {blob_id:?} of chain {chain_id:?}")]
    CannotReadLocalBlob { chain_id: ChainId, blob_id: BlobId },

    #[error("The local node doesn't have an active chain {0:?}")]
    InactiveChain(ChainId),

    #[error("The chain info response received from the local node is invalid")]
    InvalidChainInfoResponse,

    #[error(transparent)]
    NodeError(#[from] NodeError),
}

impl LocalNodeError {
    pub fn get_blobs_not_found(&self) -> Option<Vec<BlobId>> {
        match self {
            LocalNodeError::WorkerError(WorkerError::ChainError(chain_error)) => {
                match &**chain_error {
                    ChainError::ExecutionError(execution_error, _) => match **execution_error {
                        ExecutionError::SystemError(SystemExecutionError::BlobNotFoundOnRead(
                            blob_id,
                        ))
                        | ExecutionError::ViewError(ViewError::BlobNotFoundOnRead(blob_id)) => {
                            Some(vec![blob_id])
                        }
                        _ => None,
                    },
                    _ => None,
                }
            }
            LocalNodeError::WorkerError(WorkerError::BlobsNotFound(blob_ids)) => {
                Some(blob_ids.clone())
            }
            LocalNodeError::NodeError(NodeError::BlobNotFoundOnRead(blob_id)) => {
                Some(vec![*blob_id])
            }
            LocalNodeError::NodeError(NodeError::BlobsNotFound(blob_ids)) => Some(blob_ids.clone()),
            _ => None,
        }
    }
}

impl<S> LocalNodeClient<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    #[instrument(level = "trace", skip_all)]
    pub async fn handle_block_proposal(
        &self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, LocalNodeError> {
        // In local nodes, we can trust fully_handle_certificate to carry all actions eventually.
        let (response, _actions) = self.node.state.handle_block_proposal(proposal).await?;
        Ok(response)
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn handle_lite_certificate(
        &self,
        certificate: LiteCertificate<'_>,
        notifier: &impl Notifier,
    ) -> Result<ChainInfoResponse, LocalNodeError> {
        let full_cert = self.node.state.full_certificate(certificate).await?;
        let response = self
            .node
            .state
            .fully_handle_certificate_with_notifications(full_cert, vec![], notifier)
            .await?;
        Ok(response)
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn handle_certificate(
        &self,
        certificate: Certificate,
        blobs: Vec<Blob>,
        notifier: &impl Notifier,
    ) -> Result<ChainInfoResponse, LocalNodeError> {
        let response = Box::pin(self.node.state.fully_handle_certificate_with_notifications(
            certificate,
            blobs,
            notifier,
        ))
        .await?;
        Ok(response)
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn handle_chain_info_query(
        &self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, LocalNodeError> {
        // In local nodes, we can trust fully_handle_certificate to carry all actions eventually.
        let (response, _actions) = self.node.state.handle_chain_info_query(query).await?;
        Ok(response)
    }
}

impl<S> LocalNodeClient<S>
where
    S: Storage,
{
    #[instrument(level = "trace", skip_all)]
    pub fn new(state: WorkerState<S>) -> Self {
        Self {
            node: Arc::new(LocalNode { state }),
        }
    }
}

impl<S> LocalNodeClient<S>
where
    S: Storage + Clone,
{
    #[instrument(level = "trace", skip_all)]
    pub(crate) fn storage_client(&self) -> S {
        self.node.state.storage_client().clone()
    }
}

impl<S> LocalNodeClient<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    #[instrument(level = "trace", skip_all)]
    pub async fn stage_block_execution(
        &self,
        block: Block,
    ) -> Result<(ExecutedBlock, ChainInfoResponse), LocalNodeError> {
        let (executed_block, info) = self.node.state.stage_block_execution(block).await?;
        Ok((executed_block, info))
    }

    // Given a list of missing `BlobId`s and a `Certificate` for a block:
    // - Makes sure they're required by the block provided
    // - Makes sure there's no duplicate blobs being requested
    // - Searches for the blob in different places of the local node: blob cache,
    //   chain manager's pending blobs, and blob storage.
    pub async fn find_missing_blobs(
        &self,
        certificate: &Certificate,
        missing_blob_ids: &Vec<BlobId>,
        chain_id: ChainId,
    ) -> Result<Vec<Blob>, NodeError> {
        if missing_blob_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Find the missing blobs locally and retry.
        let required = match certificate.value() {
            CertificateValue::ConfirmedBlock { executed_block, .. }
            | CertificateValue::ValidatedBlock { executed_block, .. } => {
                executed_block.required_blob_ids()
            }
            CertificateValue::Timeout { .. } => HashSet::new(),
        };
        for blob_id in missing_blob_ids {
            if !required.contains(blob_id) {
                warn!(
                    "validator requested blob {:?} but it is not required",
                    blob_id
                );
                return Err(NodeError::InvalidChainInfoResponse);
            }
        }
        let mut unique_missing_blob_ids = missing_blob_ids.iter().cloned().collect::<HashSet<_>>();
        if missing_blob_ids.len() > unique_missing_blob_ids.len() {
            warn!("blobs requested by validator contain duplicates");
            return Err(NodeError::InvalidChainInfoResponse);
        }

        let chain_manager_pending_blobs = self
            .chain_state_view(chain_id)
            .await?
            .manager
            .get()
            .pending_blobs
            .clone();
        let mut found_blobs = Vec::new();
        for blob_id in missing_blob_ids {
            if let Some(blob) = chain_manager_pending_blobs.get(blob_id).cloned() {
                found_blobs.push(blob);
                unique_missing_blob_ids.remove(blob_id);
            }
        }

        let storage = self.storage_client();
        found_blobs.extend(
            storage
                .read_blobs(&unique_missing_blob_ids.into_iter().collect::<Vec<_>>())
                .await?
                .into_iter()
                .flatten(),
        );
        Ok(found_blobs)
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn try_process_certificates(
        &self,
        remote_node: &RemoteNode<impl ValidatorNode>,
        chain_id: ChainId,
        certificates: Vec<Certificate>,
        notifier: &impl Notifier,
    ) -> Option<Box<ChainInfo>> {
        let mut info = None;
        for certificate in certificates {
            let hash = certificate.hash();
            if !certificate.value().is_confirmed() || certificate.value().chain_id() != chain_id {
                // The certificate is not as expected. Give up.
                warn!("Failed to process network certificate {}", hash);
                return info;
            }
            let mut result = self
                .handle_certificate(certificate.clone(), vec![], notifier)
                .await;

            result = match &result {
                Err(err) => {
                    if let Some(blob_ids) = err.get_blobs_not_found() {
                        let blobs = remote_node.try_download_blobs(blob_ids.as_slice()).await;
                        if blobs.len() != blob_ids.len() {
                            result
                        } else {
                            self.handle_certificate(certificate, blobs, notifier).await
                        }
                    } else {
                        result
                    }
                }
                _ => result,
            };

            match result {
                Ok(response) => info = Some(response.info),
                Err(error) => {
                    // The certificate is not as expected. Give up.
                    warn!("Failed to process network certificate {}: {}", hash, error);
                    return info;
                }
            };
        }
        // Done with all certificates.
        info
    }

    /// Returns a read-only view of the [`ChainStateView`] of a chain referenced by its
    /// [`ChainId`].
    ///
    /// The returned view holds a lock on the chain state, which prevents the local node from
    /// changing the state of that chain.
    #[instrument(level = "trace", skip(self))]
    pub async fn chain_state_view(
        &self,
        chain_id: ChainId,
    ) -> Result<OwnedRwLockReadGuard<ChainStateView<S::Context>>, WorkerError> {
        self.node.state.chain_state_view(chain_id).await
    }

    #[instrument(level = "trace", skip(self))]
    pub(crate) async fn local_chain_info(
        &self,
        chain_id: ChainId,
    ) -> Result<Box<ChainInfo>, LocalNodeError> {
        let query = ChainInfoQuery::new(chain_id);
        Ok(self.handle_chain_info_query(query).await?.info)
    }

    #[instrument(level = "trace", skip(self, query))]
    pub async fn query_application(
        &self,
        chain_id: ChainId,
        query: Query,
    ) -> Result<Response, LocalNodeError> {
        let response = self.node.state.query_application(chain_id, query).await?;
        Ok(response)
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn describe_application(
        &self,
        chain_id: ChainId,
        application_id: UserApplicationId,
    ) -> Result<UserApplicationDescription, LocalNodeError> {
        let response = self
            .node
            .state
            .describe_application(chain_id, application_id)
            .await?;
        Ok(response)
    }

    /// Downloads and processes all certificates up to (excluding) the specified height.
    #[instrument(level = "trace", skip(self, validators, notifier))]
    pub async fn download_certificates(
        &self,
        validators: &[RemoteNode<impl ValidatorNode>],
        chain_id: ChainId,
        target_next_block_height: BlockHeight,
        notifier: &impl Notifier,
    ) -> Result<Box<ChainInfo>, LocalNodeError> {
        // Sequentially try each validator in random order.
        let mut validators = validators.iter().collect::<Vec<_>>();
        validators.shuffle(&mut thread_rng());
        for remote_node in validators {
            let info = self.local_chain_info(chain_id).await?;
            if target_next_block_height <= info.next_block_height {
                return Ok(info);
            }
            self.try_download_certificates_from(
                remote_node,
                chain_id,
                info.next_block_height,
                target_next_block_height,
                notifier,
            )
            .await?;
        }
        let info = self.local_chain_info(chain_id).await?;
        if target_next_block_height <= info.next_block_height {
            Ok(info)
        } else {
            Err(LocalNodeError::CannotDownloadCertificates {
                chain_id,
                target_next_block_height,
            })
        }
    }

    /// Obtains the certificate containing the specified message.
    #[instrument(level = "trace", skip(self))]
    pub async fn certificate_for(
        &self,
        message_id: &MessageId,
    ) -> Result<Certificate, LocalNodeError> {
        let query = ChainInfoQuery::new(message_id.chain_id)
            .with_sent_certificate_hashes_in_range(BlockHeightRange::single(message_id.height));
        let info = self.handle_chain_info_query(query).await?.info;
        let certificates = self
            .storage_client()
            .read_certificates(info.requested_sent_certificate_hashes)
            .await?;
        let certificate = certificates
            .into_iter()
            .find(|certificate| certificate.value().has_message(message_id))
            .ok_or_else(|| {
                ViewError::not_found("could not find certificate with message {}", message_id)
            })?;
        Ok(certificate)
    }

    /// Downloads and processes all certificates up to (excluding) the specified height from the
    /// given validator.
    #[instrument(level = "trace", skip_all)]
    async fn try_download_certificates_from(
        &self,
        remote_node: &RemoteNode<impl ValidatorNode>,
        chain_id: ChainId,
        mut start: BlockHeight,
        stop: BlockHeight,
        notifier: &impl Notifier,
    ) -> Result<(), LocalNodeError> {
        while start < stop {
            // TODO(#2045): Analyze network errors instead of guessing the batch size.
            let limit = u64::from(stop)
                .checked_sub(u64::from(start))
                .ok_or(ArithmeticError::Overflow)?
                .min(1000);
            let Some(certificates) = remote_node
                .try_query_certificates_from(chain_id, start, limit)
                .await?
            else {
                break;
            };
            let Some(info) = self
                .try_process_certificates(remote_node, chain_id, certificates, notifier)
                .await
            else {
                break;
            };
            assert!(info.next_block_height > start);
            start = info.next_block_height;
        }
        Ok(())
    }

    #[instrument(level = "trace", skip(validators))]
    async fn download_blob(
        validators: &[RemoteNode<impl ValidatorNode>],
        blob_id: BlobId,
    ) -> Option<Blob> {
        // Sequentially try each validator in random order.
        let mut validators = validators.iter().collect::<Vec<_>>();
        validators.shuffle(&mut thread_rng());
        for remote_node in validators {
            if let Some(blob) = remote_node.try_download_blob(blob_id).await {
                return Some(blob);
            }
        }
        None
    }

    #[instrument(level = "trace", skip(validators))]
    pub async fn download_certificate_for_blob_from_validators_futures(
        validators: &[RemoteNode<impl ValidatorNode>],
        blob_id: BlobId,
    ) -> FuturesUnordered<impl Future<Output = Option<Certificate>> + '_> {
        let futures = FuturesUnordered::new();

        let mut validators = validators.iter().collect::<Vec<_>>();
        validators.shuffle(&mut thread_rng());
        for remote_node in validators {
            futures.push(async move {
                remote_node
                    .download_certificate_for_blob(blob_id)
                    .await
                    .ok()
            });
        }

        futures
    }

    #[instrument(level = "trace", skip(nodes))]
    pub async fn download_blobs(
        blob_ids: &[BlobId],
        nodes: &[RemoteNode<impl ValidatorNode>],
    ) -> Vec<Blob> {
        future::join_all(
            blob_ids
                .iter()
                .map(|blob_id| Self::download_blob(nodes, *blob_id)),
        )
        .await
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
    }

    /// Handles any pending local cross-chain requests.
    #[instrument(level = "trace", skip(self))]
    pub async fn retry_pending_cross_chain_requests(
        &self,
        sender_chain: ChainId,
    ) -> Result<(), LocalNodeError> {
        let (_response, actions) = self
            .node
            .state
            .handle_chain_info_query(ChainInfoQuery::new(sender_chain))
            .await?;
        let mut requests = VecDeque::from_iter(actions.cross_chain_requests);
        while let Some(request) = requests.pop_front() {
            let new_actions = self.node.state.handle_cross_chain_request(request).await?;
            requests.extend(new_actions.cross_chain_requests);
        }
        Ok(())
    }
}

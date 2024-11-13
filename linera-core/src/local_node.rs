// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};

use linera_base::{
    data_types::{ArithmeticError, Blob, UserApplicationDescription},
    identifiers::{BlobId, ChainId, MessageId, UserApplicationId},
};
use linera_chain::{
    data_types::{Block, BlockProposal, ExecutedBlock, LiteCertificate},
    types::{Certificate, CertificateValue, ConfirmedBlockCertificate},
    ChainError, ChainStateView,
};
use linera_execution::{ExecutionError, Query, Response, SystemExecutionError};
use linera_storage::Storage;
use linera_views::views::ViewError;
use thiserror::Error;
use tokio::sync::OwnedRwLockReadGuard;
use tracing::{instrument, warn};

use crate::{
    data_types::{BlockHeightRange, ChainInfo, ChainInfoQuery, ChainInfoResponse},
    node::NodeError,
    notifier::Notifier,
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

    #[error("Failed to read blob {blob_id:?} of chain {chain_id:?}")]
    CannotReadLocalBlob { chain_id: ChainId, blob_id: BlobId },

    #[error("The local node doesn't have an active chain {0:?}")]
    InactiveChain(ChainId),

    #[error("The chain info response received from the local node is invalid")]
    InvalidChainInfoResponse,
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

    /// Given a list of missing `BlobId`s and a `Certificate` for a block:
    /// - Makes sure they're required by the block provided
    /// - Makes sure there's no duplicate blobs being requested
    /// - Searches for the blob in different places of the local node: blob cache,
    ///   chain manager's pending blobs, and blob storage.
    /// - Returns `None` if not all blobs could be found.
    pub async fn find_missing_blobs(
        &self,
        certificate: &Certificate,
        missing_blob_ids: &Vec<BlobId>,
        chain_id: ChainId,
    ) -> Result<Option<Vec<Blob>>, NodeError> {
        if missing_blob_ids.is_empty() {
            return Ok(Some(Vec::new()));
        }

        // Find the missing blobs locally and retry.
        let required = match certificate.inner() {
            CertificateValue::ConfirmedBlock(confirmed) => confirmed.inner().required_blob_ids(),
            CertificateValue::ValidatedBlock(validated) => validated.inner().required_blob_ids(),
            CertificateValue::Timeout(_) => HashSet::new(),
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

        let mut chain_manager_pending_blobs = self
            .chain_state_view(chain_id)
            .await?
            .manager
            .get()
            .pending_blobs
            .clone();
        let mut found_blobs = Vec::new();
        for blob_id in missing_blob_ids {
            if let Some(blob) = chain_manager_pending_blobs.remove(blob_id) {
                found_blobs.push(blob);
                unique_missing_blob_ids.remove(blob_id);
            }
        }

        let storage = self.storage_client();
        let Some(read_blobs) = storage
            .read_blobs(&unique_missing_blob_ids.into_iter().collect::<Vec<_>>())
            .await?
            .into_iter()
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        found_blobs.extend(read_blobs);
        Ok(Some(found_blobs))
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

    /// Obtains the certificate containing the specified message.
    #[instrument(level = "trace", skip(self))]
    pub async fn certificate_for(
        &self,
        message_id: &MessageId,
    ) -> Result<ConfirmedBlockCertificate, LocalNodeError> {
        let query = ChainInfoQuery::new(message_id.chain_id)
            .with_sent_certificate_hashes_in_range(BlockHeightRange::single(message_id.height));
        let info = self.handle_chain_info_query(query).await?.info;
        let certificates = self
            .storage_client()
            .read_certificates(info.requested_sent_certificate_hashes)
            .await?;
        let certificate = certificates
            .into_iter()
            .find(|certificate| certificate.has_message(message_id))
            .ok_or_else(|| {
                ViewError::not_found("could not find certificate with message {}", message_id)
            })?;
        Ok(certificate)
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

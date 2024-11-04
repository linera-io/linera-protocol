// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    sync::Arc,
};

use futures::{stream, StreamExt as _, TryStreamExt as _};
use linera_base::{
    data_types::{ArithmeticError, Blob, BlockHeight, UserApplicationDescription},
    identifiers::{BlobId, ChainId, MessageId, UserApplicationId},
};
use linera_chain::{
    data_types::{Block, BlockProposal, ExecutedBlock},
    types::{Certificate, CertificateValue, ConfirmedBlockCertificate, LiteCertificate},
    ChainStateView,
};
use linera_execution::{Query, Response};
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
    ViewError(ViewError),

    #[error("Local node operation failed: {0}")]
    WorkerError(WorkerError),

    #[error("Failed to read blobs {blob_ids:?} of chain {chain_id:?}")]
    CannotReadLocalBlobs {
        chain_id: ChainId,
        blob_ids: Vec<BlobId>,
    },

    #[error("The local node doesn't have an active chain {0:?}")]
    InactiveChain(ChainId),

    #[error("The chain info response received from the local node is invalid")]
    InvalidChainInfoResponse,

    #[error("Blobs not found: {0:?}")]
    BlobsNotFound(Vec<BlobId>),
}

impl From<WorkerError> for LocalNodeError {
    fn from(error: WorkerError) -> Self {
        match error {
            WorkerError::BlobsNotFound(blob_ids) => LocalNodeError::BlobsNotFound(blob_ids),
            error => LocalNodeError::WorkerError(error),
        }
    }
}

impl From<ViewError> for LocalNodeError {
    fn from(error: ViewError) -> Self {
        match error {
            ViewError::BlobsNotFound(blob_ids) => LocalNodeError::BlobsNotFound(blob_ids),
            error => LocalNodeError::ViewError(error),
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
    /// - Searches for the blob in different places of the local node: blob cache,
    ///   chain manager's pending blobs, and blob storage.
    /// - Returns `Some` for blobs that were found and `None` for the ones still missing.
    pub async fn find_missing_blobs(
        &self,
        missing_blob_ids: Vec<BlobId>,
        certificate: &Certificate,
    ) -> Result<Vec<Option<Blob>>, NodeError> {
        if missing_blob_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Find the missing blobs locally and retry.
        match certificate.inner() {
            CertificateValue::ConfirmedBlock(confirmed) => {
                self.check_missing_blob_ids(
                    &confirmed.inner().required_blob_ids(),
                    &missing_blob_ids,
                )?;
                let storage = self.storage_client();
                Ok(storage.read_blobs(&missing_blob_ids).await?)
            }
            CertificateValue::ValidatedBlock(validated) => {
                self.check_missing_blob_ids(
                    &validated.inner().required_blob_ids(),
                    &missing_blob_ids,
                )?;
                let chain_id = validated.inner().block.chain_id;
                let chain = self.chain_state_view(chain_id).await?;
                let manager = chain.manager.get();
                let locked_blobs = manager
                    .locked_published_blobs
                    .iter()
                    .chain(manager.locked_used_blobs.iter())
                    .map(|(k, v)| (k, v.clone()))
                    .collect::<BTreeMap<_, _>>();

                Ok(missing_blob_ids
                    .iter()
                    .map(|blob_id| locked_blobs.get(blob_id).cloned())
                    .collect())
            }
            CertificateValue::Timeout(_) => {
                warn!(
                    "validator requested blobs {:?} but they're not required",
                    missing_blob_ids
                );
                Err(NodeError::UnexpectedEntriesInBlobsNotFound(
                    missing_blob_ids,
                ))
            }
        }
    }

    pub fn check_missing_blob_ids(
        &self,
        required_blob_ids: &HashSet<BlobId>,
        missing_blob_ids: &Vec<BlobId>,
    ) -> Result<(), NodeError> {
        let mut unexpected_blob_ids = Vec::new();
        for blob_id in missing_blob_ids {
            if !required_blob_ids.contains(blob_id) {
                warn!(
                    "validator requested blob {:?} but it is not required",
                    blob_id
                );

                unexpected_blob_ids.push(*blob_id);
            }
        }

        if !unexpected_blob_ids.is_empty() {
            return Err(NodeError::UnexpectedEntriesInBlobsNotFound(
                unexpected_blob_ids,
            ));
        }

        let unique_missing_blob_ids = missing_blob_ids.iter().cloned().collect::<HashSet<_>>();
        if missing_blob_ids.len() > unique_missing_blob_ids.len() {
            warn!("blobs requested by validator contain duplicates");
            return Err(NodeError::DuplicatesInBlobsNotFound);
        }

        Ok(())
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
    pub(crate) async fn chain_info(
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

    /// Given a list of chain IDs, returns a map that assigns to each of them the next block
    /// height, i.e. the lowest block height that we have not processed in the local node yet.
    ///
    /// It makes at most `chain_worker_limit` requests to the local node in parallel.
    pub async fn next_block_heights(
        &self,
        chain_ids: impl IntoIterator<Item = &ChainId>,
        chain_worker_limit: usize,
    ) -> Result<BTreeMap<ChainId, BlockHeight>, LocalNodeError> {
        let futures = chain_ids
            .into_iter()
            .map(|chain_id| async move {
                let local_info = self.chain_info(*chain_id).await?;
                Ok::<_, LocalNodeError>((*chain_id, local_info.next_block_height))
            })
            .collect::<Vec<_>>();
        stream::iter(futures)
            .buffer_unordered(chain_worker_limit)
            .try_collect()
            .await
    }
}

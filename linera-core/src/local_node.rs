// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

use futures::{future::Either, stream, StreamExt as _, TryStreamExt as _};
use linera_base::{
    data_types::{ArithmeticError, Blob, BlockHeight, UserApplicationDescription},
    identifiers::{BlobId, ChainId, MessageId, UserApplicationId},
};
use linera_chain::{
    data_types::{BlockProposal, ExecutedBlock, ProposedBlock},
    types::{ConfirmedBlockCertificate, GenericCertificate, LiteCertificate},
    ChainStateView,
};
use linera_execution::{committee::ValidatorName, Query, Response};
use linera_storage::Storage;
use linera_views::views::ViewError;
use thiserror::Error;
use tokio::sync::OwnedRwLockReadGuard;
use tracing::{instrument, warn};

use crate::{
    data_types::{BlockHeightRange, ChainInfo, ChainInfoQuery, ChainInfoResponse},
    notifier::Notifier,
    worker::{ProcessableCertificate, WorkerError, WorkerState},
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

    #[error("Failed to read blob {blob_id:?} of chain {chain_id:?}")]
    CannotReadLocalBlob { chain_id: ChainId, blob_id: BlobId },

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
        match self.node.state.full_certificate(certificate).await? {
            Either::Left(confirmed) => Ok(self.handle_certificate(confirmed, notifier).await?),
            Either::Right(validated) => Ok(self.handle_certificate(validated, notifier).await?),
        }
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn handle_certificate<T>(
        &self,
        certificate: GenericCertificate<T>,
        notifier: &impl Notifier,
    ) -> Result<ChainInfoResponse, LocalNodeError>
    where
        T: ProcessableCertificate,
    {
        Ok(Box::pin(
            self.node
                .state
                .fully_handle_certificate_with_notifications(certificate, notifier),
        )
        .await?)
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
        block: ProposedBlock,
    ) -> Result<(ExecutedBlock, ChainInfoResponse), LocalNodeError> {
        Ok(self.node.state.stage_block_execution(block).await?)
    }

    /// Reads blobs from storage.
    pub async fn read_blobs_from_storage(
        &self,
        blob_ids: &[BlobId],
    ) -> Result<Option<Vec<Blob>>, LocalNodeError> {
        let storage = self.storage_client();
        Ok(storage.read_blobs(blob_ids).await?.into_iter().collect())
    }

    /// Looks for the specified blobs in the local chain manager's locked blobs.
    /// Returns `Ok(None)` if any of the blobs is not found.
    pub async fn get_locked_blobs(
        &self,
        blob_ids: &[BlobId],
        chain_id: ChainId,
    ) -> Result<Option<Vec<Blob>>, LocalNodeError> {
        let chain = self.chain_state_view(chain_id).await?;
        let mut blobs = Vec::new();
        for blob_id in blob_ids {
            match chain.manager.locked_blobs.get(blob_id).await? {
                None => return Ok(None),
                Some(blob) => blobs.push(blob),
            }
        }
        Ok(Some(blobs))
    }

    /// Writes the given blobs to storage if there is an appropriate blob state.
    pub async fn store_blobs(&self, blobs: &[Blob]) -> Result<(), LocalNodeError> {
        let storage = self.storage_client();
        storage.maybe_write_blobs(blobs).await?;
        Ok(())
    }

    pub async fn handle_pending_blobs(
        &self,
        chain_id: ChainId,
        blobs: Vec<Blob>,
    ) -> Result<(), LocalNodeError> {
        for blob in blobs {
            self.node.state.handle_pending_blob(chain_id, blob).await?;
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
    ) -> Result<OwnedRwLockReadGuard<ChainStateView<S::Context>>, LocalNodeError> {
        Ok(self.node.state.chain_state_view(chain_id).await?)
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

    pub async fn update_received_certificate_trackers(
        &self,
        chain_id: ChainId,
        new_trackers: BTreeMap<ValidatorName, u64>,
    ) -> Result<(), LocalNodeError> {
        self.node
            .state
            .update_received_certificate_trackers(chain_id, new_trackers)
            .await?;
        Ok(())
    }
}

// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

use futures::{stream::FuturesUnordered, TryStreamExt as _};
use linera_base::{
    crypto::ValidatorPublicKey,
    data_types::{ArithmeticError, Blob, BlockHeight, Epoch},
    identifiers::{BlobId, ChainId},
};
use linera_chain::{
    data_types::{BlockProposal, ProposedBlock},
    types::{Block, GenericCertificate},
    ChainStateView,
};
use linera_execution::{committee::Committee, BlobState, Query, QueryOutcome};
use linera_storage::Storage;
use linera_views::ViewError;
use thiserror::Error;
use tokio::sync::OwnedRwLockReadGuard;
use tracing::{instrument, warn};

use crate::{
    data_types::{ChainInfo, ChainInfoQuery, ChainInfoResponse},
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
    ViewError(#[from] ViewError),

    #[error("Worker operation failed: {0}")]
    WorkerError(WorkerError),

    #[error("The local node doesn't have an active chain {0}")]
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

    #[instrument(level = "trace", skip_all)]
    pub fn new(state: WorkerState<S>) -> Self {
        Self {
            node: Arc::new(LocalNode { state }),
        }
    }

    #[instrument(level = "trace", skip_all)]
    pub(crate) fn storage_client(&self) -> S {
        self.node.state.storage_client().clone()
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn stage_block_execution(
        &self,
        block: ProposedBlock,
        round: Option<u32>,
        published_blobs: Vec<Blob>,
    ) -> Result<(Block, ChainInfoResponse), LocalNodeError> {
        Ok(self
            .node
            .state
            .stage_block_execution(block, round, published_blobs)
            .await?)
    }

    /// Reads blobs from storage.
    pub async fn read_blobs_from_storage(
        &self,
        blob_ids: &[BlobId],
    ) -> Result<Option<Vec<Blob>>, LocalNodeError> {
        let storage = self.storage_client();
        Ok(storage.read_blobs(blob_ids).await?.into_iter().collect())
    }

    /// Reads blob states from storage
    pub async fn read_blob_states_from_storage(
        &self,
        blob_ids: &[BlobId],
    ) -> Result<Vec<BlobState>, LocalNodeError> {
        let storage = self.storage_client();
        let mut blobs_not_found = Vec::new();
        let mut blob_states = Vec::new();
        for (blob_state, blob_id) in storage
            .read_blob_states(blob_ids)
            .await?
            .into_iter()
            .zip(blob_ids)
        {
            match blob_state {
                None => blobs_not_found.push(*blob_id),
                Some(blob_state) => blob_states.push(blob_state),
            }
        }
        if !blobs_not_found.is_empty() {
            return Err(LocalNodeError::BlobsNotFound(blobs_not_found));
        }
        Ok(blob_states)
    }

    /// Looks for the specified blobs in the local chain manager's locking blobs.
    /// Returns `Ok(None)` if any of the blobs is not found.
    pub async fn get_locking_blobs(
        &self,
        blob_ids: impl IntoIterator<Item = &BlobId>,
        chain_id: ChainId,
    ) -> Result<Option<Vec<Blob>>, LocalNodeError> {
        let chain = self.chain_state_view(chain_id).await?;
        let mut blobs = Vec::new();
        for blob_id in blob_ids {
            match chain.manager.locking_blobs.get(blob_id).await? {
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
    ) -> Result<QueryOutcome, LocalNodeError> {
        let outcome = self.node.state.query_application(chain_id, query).await?;
        Ok(outcome)
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
    /// height to schedule, i.e. the lowest block height for which we haven't added the messages
    /// to `receiver_id` to the outbox yet.
    pub async fn next_outbox_heights(
        &self,
        chain_ids: impl IntoIterator<Item = &ChainId>,
        receiver_id: ChainId,
    ) -> Result<BTreeMap<ChainId, BlockHeight>, LocalNodeError> {
        let futures =
            FuturesUnordered::from_iter(chain_ids.into_iter().map(|chain_id| async move {
                let chain = self.chain_state_view(*chain_id).await?;
                let mut next_height = chain.tip_state.get().next_block_height;
                if let Some(outbox) = chain.outboxes.try_load_entry(&receiver_id).await? {
                    next_height = next_height.max(*outbox.next_height_to_schedule.get());
                }
                Ok::<_, LocalNodeError>((*chain_id, next_height))
            }));
        futures.try_collect().await
    }

    pub async fn update_received_certificate_trackers(
        &self,
        chain_id: ChainId,
        new_trackers: BTreeMap<ValidatorPublicKey, u64>,
    ) -> Result<(), LocalNodeError> {
        self.node
            .state
            .update_received_certificate_trackers(chain_id, new_trackers)
            .await?;
        Ok(())
    }
}

/// Extension trait for [`ChainInfo`]s from our local node. These should always be valid and
/// contain the requested information.
pub trait LocalChainInfoExt {
    /// Returns the requested map of committees.
    fn into_committees(self) -> Result<BTreeMap<Epoch, Committee>, LocalNodeError>;

    /// Returns the current committee.
    fn into_current_committee(self) -> Result<Committee, LocalNodeError>;

    /// Returns a reference to the current committee.
    fn current_committee(&self) -> Result<&Committee, LocalNodeError>;
}

impl LocalChainInfoExt for ChainInfo {
    fn into_committees(self) -> Result<BTreeMap<Epoch, Committee>, LocalNodeError> {
        self.requested_committees
            .ok_or(LocalNodeError::InvalidChainInfoResponse)
    }

    fn into_current_committee(self) -> Result<Committee, LocalNodeError> {
        self.requested_committees
            .ok_or(LocalNodeError::InvalidChainInfoResponse)?
            .remove(&self.epoch)
            .ok_or(LocalNodeError::InactiveChain(self.chain_id))
    }

    fn current_committee(&self) -> Result<&Committee, LocalNodeError> {
        self.requested_committees
            .as_ref()
            .ok_or(LocalNodeError::InvalidChainInfoResponse)?
            .get(&self.epoch)
            .ok_or(LocalNodeError::InactiveChain(self.chain_id))
    }
}

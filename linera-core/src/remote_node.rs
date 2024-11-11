// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, fmt};

use futures::{stream::FuturesUnordered, StreamExt};
use linera_base::{
    crypto::CryptoHash,
    data_types::{Blob, BlockHeight},
    ensure,
    identifiers::{BlobId, ChainId},
};
use linera_chain::{
    data_types::{BlockProposal, Certificate, LiteCertificate},
    types::ConfirmedBlockCertificate,
};
use linera_execution::committee::ValidatorName;
use rand::seq::SliceRandom as _;
use tracing::{instrument, warn};

use crate::{
    data_types::{BlockHeightRange, ChainInfo, ChainInfoQuery, ChainInfoResponse},
    node::{CrossChainMessageDelivery, NodeError, ValidatorNode},
};

/// A validator node together with the validator's name.
#[derive(Clone)]
pub struct RemoteNode<N> {
    pub name: ValidatorName,
    pub node: N,
}

impl<N> fmt::Debug for RemoteNode<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteNode")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

#[allow(clippy::result_large_err)]
impl<N: ValidatorNode> RemoteNode<N> {
    pub(crate) async fn handle_chain_info_query(
        &self,
        query: ChainInfoQuery,
    ) -> Result<Box<ChainInfo>, NodeError> {
        let chain_id = query.chain_id;
        let response = self.node.handle_chain_info_query(query).await?;
        self.check_and_return_info(response, chain_id)
    }

    #[instrument(level = "trace")]
    pub(crate) async fn handle_block_proposal(
        &self,
        proposal: Box<BlockProposal>,
    ) -> Result<Box<ChainInfo>, NodeError> {
        let chain_id = proposal.content.block.chain_id;
        let response = self.node.handle_block_proposal(*proposal).await?;
        self.check_and_return_info(response, chain_id)
    }

    #[instrument(level = "trace")]
    pub(crate) async fn handle_certificate(
        &self,
        certificate: Certificate,
        blobs: Vec<Blob>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<Box<ChainInfo>, NodeError> {
        let chain_id = certificate.value().chain_id();
        let response = self
            .node
            .handle_certificate(certificate, blobs, delivery)
            .await?;
        self.check_and_return_info(response, chain_id)
    }

    #[instrument(level = "trace")]
    pub(crate) async fn handle_lite_certificate(
        &self,
        certificate: LiteCertificate<'_>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<Box<ChainInfo>, NodeError> {
        let chain_id = certificate.value.chain_id;
        let response = self
            .node
            .handle_lite_certificate(certificate, delivery)
            .await?;
        self.check_and_return_info(response, chain_id)
    }

    fn check_and_return_info(
        &self,
        response: ChainInfoResponse,
        chain_id: ChainId,
    ) -> Result<Box<ChainInfo>, NodeError> {
        let manager = &response.info.manager;
        let proposed = manager.requested_proposed.as_ref();
        let locked = manager.requested_locked.as_ref();
        ensure!(
            proposed.map_or(true, |proposal| proposal.content.block.chain_id == chain_id)
                && locked.map_or(true, |cert| cert.executed_block().block.chain_id
                    == chain_id)
                && response.check(&self.name).is_ok(),
            NodeError::InvalidChainInfoResponse
        );
        Ok(response.info)
    }

    #[instrument(level = "trace", skip_all)]
    pub(crate) async fn try_query_certificates_from(
        &self,
        chain_id: ChainId,
        start: BlockHeight,
        limit: u64,
    ) -> Result<Option<Vec<Certificate>>, NodeError> {
        tracing::debug!(name = ?self.name, ?chain_id, ?start, ?limit, "Querying certificates");
        let range = BlockHeightRange {
            start,
            limit: Some(limit),
        };
        let query = ChainInfoQuery::new(chain_id).with_sent_certificate_hashes_in_range(range);
        if let Ok(info) = self.handle_chain_info_query(query).await {
            let certificates = self
                .node
                .download_certificates(info.requested_sent_certificate_hashes)
                .await?;
            Ok(Some(certificates))
        } else {
            Ok(None)
        }
    }

    #[instrument(level = "trace")]
    pub(crate) async fn download_certificate_for_blob(
        &self,
        blob_id: BlobId,
    ) -> Result<ConfirmedBlockCertificate, NodeError> {
        let last_used_hash = self.node.blob_last_used_by(blob_id).await?;
        let certificate = self.node.download_certificate(last_used_hash).await?;
        if !certificate.requires_blob(&blob_id) {
            warn!(
                "Got invalid last used by certificate for blob {} from validator {}",
                blob_id, self.name
            );
            return Err(NodeError::InvalidCertificateForBlob(blob_id));
        }
        certificate.try_into().map_err(|_| NodeError::ChainError {
            error: "Expected ConfirmedBlock certificate".to_string(),
        })
    }

    /// Tries to download the given blobs from this node. Returns `None` if not all could be found.
    #[instrument(level = "trace")]
    pub(crate) async fn try_download_blobs(&self, blob_ids: &[BlobId]) -> Option<Vec<Blob>> {
        let mut stream = blob_ids
            .iter()
            .map(|blob_id| self.try_download_blob(*blob_id))
            .collect::<FuturesUnordered<_>>();
        let mut blobs = Vec::new();
        while let Some(maybe_blob) = stream.next().await {
            blobs.push(maybe_blob?);
        }
        Some(blobs)
    }

    #[instrument(level = "trace")]
    async fn try_download_blob(&self, blob_id: BlobId) -> Option<Blob> {
        match self.node.download_blob_content(blob_id).await {
            Ok(blob) => {
                let blob = blob.with_blob_id_checked(blob_id);

                if blob.is_none() {
                    tracing::info!("Validator {} sent an invalid blob {blob_id}.", self.name);
                }

                blob
            }
            Err(error) => {
                tracing::debug!(
                    "Failed to fetch blob {blob_id} from validator {}: {error}",
                    self.name
                );
                None
            }
        }
    }

    /// Downloads the blobs from the specified validator and returns them, including blobs that
    /// are still pending in the validator's chain manager. Returns `None` if not all of them
    /// could be found.
    pub(crate) async fn find_missing_blobs(
        &self,
        blob_ids: Vec<BlobId>,
        chain_id: ChainId,
    ) -> Result<Option<Vec<Blob>>, NodeError> {
        let query = ChainInfoQuery::new(chain_id).with_manager_values();
        let info = match self.handle_chain_info_query(query).await {
            Ok(info) => Some(info),
            Err(err) => {
                warn!("Got error from validator {}: {}", self.name, err);
                return Ok(None);
            }
        };

        let mut missing_blobs = blob_ids;
        let mut found_blobs = if let Some(info) = info {
            let new_found_blobs = missing_blobs
                .iter()
                .filter_map(|blob_id| info.manager.pending_blobs.get(blob_id))
                .map(|blob| (blob.id(), blob.clone()))
                .collect::<HashMap<_, _>>();
            missing_blobs.retain(|blob_id| !new_found_blobs.contains_key(blob_id));
            new_found_blobs.into_values().collect()
        } else {
            Vec::new()
        };

        if let Some(blobs) = self.try_download_blobs(&missing_blobs).await {
            found_blobs.extend(blobs);
            Ok(Some(found_blobs))
        } else {
            Ok(None)
        }
    }

    /// Returns the list of certificate hashes on the given chain in the given range of heights.
    /// Returns an error if the number of hashes does not match the size of the range.
    #[instrument(level = "trace")]
    pub(crate) async fn fetch_sent_certificate_hashes(
        &self,
        chain_id: ChainId,
        range: BlockHeightRange,
    ) -> Result<Vec<CryptoHash>, NodeError> {
        let query =
            ChainInfoQuery::new(chain_id).with_sent_certificate_hashes_in_range(range.clone());
        let response = self.handle_chain_info_query(query).await?;
        let hashes = response.requested_sent_certificate_hashes;

        if range
            .limit
            .is_some_and(|limit| hashes.len() as u64 != limit)
        {
            warn!(
                ?range,
                received_num = hashes.len(),
                "Validator sent invalid number of certificate hashes."
            );
            return Err(NodeError::InvalidChainInfoResponse);
        }
        Ok(hashes)
    }

    #[instrument(level = "trace", skip(validators))]
    async fn download_blob(validators: &[Self], blob_id: BlobId) -> Option<Blob> {
        // Sequentially try each validator in random order.
        let mut validators = validators.iter().collect::<Vec<_>>();
        validators.shuffle(&mut rand::thread_rng());
        for remote_node in validators {
            if let Some(blob) = remote_node.try_download_blob(blob_id).await {
                return Some(blob);
            }
        }
        None
    }

    /// Downloads the blobs with the given IDs. This is done in one concurrent task per block.
    /// Each task goes through the validators sequentially in random order and tries to download
    /// it. Returns `None` if it couldn't find all blobs.
    #[instrument(level = "trace", skip(validators))]
    pub async fn download_blobs(blob_ids: &[BlobId], validators: &[Self]) -> Option<Vec<Blob>> {
        let mut stream = blob_ids
            .iter()
            .map(|blob_id| Self::download_blob(validators, *blob_id))
            .collect::<FuturesUnordered<_>>();
        let mut blobs = Vec::new();
        while let Some(maybe_blob) = stream.next().await {
            blobs.push(maybe_blob?);
        }
        Some(blobs)
    }
}

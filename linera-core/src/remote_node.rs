// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, fmt};

use futures::future;
use linera_base::{
    crypto::CryptoHash,
    data_types::{Blob, BlockHeight},
    ensure,
    identifiers::{BlobId, ChainId},
};
use linera_chain::data_types::{BlockProposal, Certificate, LiteCertificate};
use linera_execution::committee::ValidatorName;
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
                && locked.map_or(true, |cert| cert.value().is_validated()
                    && cert.value().chain_id() == chain_id)
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
    ) -> Result<Certificate, NodeError> {
        let last_used_hash = self.node.blob_last_used_by(blob_id).await?;
        let certificate = self.node.download_certificate(last_used_hash).await?;
        if !certificate.requires_blob(&blob_id) {
            warn!(
                "Got invalid last used by certificate for blob {} from validator {}",
                blob_id, self.name
            );
            return Err(NodeError::InvalidCertificateForBlob(blob_id));
        }
        Ok(certificate)
    }

    #[instrument(level = "trace")]
    pub(crate) async fn try_download_blobs(&self, blob_ids: &[BlobId]) -> Vec<Blob> {
        future::join_all(
            blob_ids
                .iter()
                .map(|blob_id| self.try_download_blob(*blob_id)),
        )
        .await
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
    }

    #[instrument(level = "trace")]
    pub(crate) async fn try_download_blob(&self, blob_id: BlobId) -> Option<Blob> {
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
    /// are still pending the the validator's chain manager.
    pub(crate) async fn find_missing_blobs(
        &self,
        blob_ids: Vec<BlobId>,
        chain_id: ChainId,
    ) -> Result<Vec<Blob>, NodeError> {
        let query = ChainInfoQuery::new(chain_id).with_manager_values();
        let info = match self.handle_chain_info_query(query).await {
            Ok(info) => Some(info),
            Err(err) => {
                warn!("Got error from validator {}: {}", self.name, err);
                return Ok(Vec::new());
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

        found_blobs.extend(self.try_download_blobs(&missing_blobs).await);

        Ok(found_blobs)
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
}

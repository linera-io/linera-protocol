// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

use custom_debug_derive::Debug;
use futures::{future::try_join_all, stream::FuturesUnordered, StreamExt};
use linera_base::{
    crypto::CryptoHash,
    data_types::{Blob, BlockHeight},
    ensure,
    identifiers::{BlobId, ChainId},
};
use linera_chain::{
    data_types::BlockProposal,
    types::{
        CertificateValue, ConfirmedBlockCertificate, GenericCertificate, LiteCertificate,
        TimeoutCertificate, ValidatedBlockCertificate,
    },
};
use linera_execution::committee::ValidatorName;
use rand::seq::SliceRandom as _;
use tracing::{instrument, warn};

use crate::{
    data_types::{BlockHeightRange, ChainInfo, ChainInfoQuery, ChainInfoResponse},
    node::{CrossChainMessageDelivery, NodeError, ValidatorNode},
};

/// A validator node together with the validator's name.
#[derive(Clone, Debug)]
pub struct RemoteNode<N> {
    pub name: ValidatorName,
    #[debug(skip)]
    pub node: N,
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

    pub(crate) async fn handle_timeout_certificate(
        &self,
        certificate: TimeoutCertificate,
    ) -> Result<Box<ChainInfo>, NodeError> {
        let chain_id = certificate.inner().chain_id();
        let response = self.node.handle_timeout_certificate(certificate).await?;
        self.check_and_return_info(response, chain_id)
    }

    pub(crate) async fn handle_confirmed_certificate(
        &self,
        certificate: ConfirmedBlockCertificate,
        delivery: CrossChainMessageDelivery,
    ) -> Result<Box<ChainInfo>, NodeError> {
        let chain_id = certificate.inner().chain_id();
        let response = self
            .node
            .handle_confirmed_certificate(certificate, delivery)
            .await?;
        self.check_and_return_info(response, chain_id)
    }

    pub(crate) async fn handle_validated_certificate(
        &self,
        certificate: ValidatedBlockCertificate,
    ) -> Result<Box<ChainInfo>, NodeError> {
        let chain_id = certificate.inner().chain_id();
        let response = self.node.handle_validated_certificate(certificate).await?;
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

    pub(crate) async fn handle_optimized_validated_certificate(
        &mut self,
        certificate: &ValidatedBlockCertificate,
        delivery: CrossChainMessageDelivery,
    ) -> Result<Box<ChainInfo>, NodeError> {
        if certificate.is_signed_by(&self.name) {
            let result = self
                .handle_lite_certificate(certificate.lite_certificate(), delivery)
                .await;
            match result {
                Err(NodeError::MissingCertificateValue) => {
                    warn!(
                        "Validator {} forgot a certificate value that they signed before",
                        self.name
                    );
                }
                _ => return result,
            }
        }
        self.handle_validated_certificate(certificate.clone()).await
    }

    pub(crate) async fn handle_optimized_confirmed_certificate(
        &mut self,
        certificate: &ConfirmedBlockCertificate,
        delivery: CrossChainMessageDelivery,
    ) -> Result<Box<ChainInfo>, NodeError> {
        if certificate.is_signed_by(&self.name) {
            let result = self
                .handle_lite_certificate(certificate.lite_certificate(), delivery)
                .await;
            match result {
                Err(NodeError::MissingCertificateValue) => {
                    warn!(
                        "Validator {} forgot a certificate value that they signed before",
                        self.name
                    );
                }
                _ => return result,
            }
        }
        self.handle_confirmed_certificate(certificate.clone(), delivery)
            .await
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
                && locked.map_or(true, |cert| cert.chain_id() == chain_id)
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
    ) -> Result<Option<Vec<ConfirmedBlockCertificate>>, NodeError> {
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
                .await?
                .into_iter()
                .map(|c| {
                    ConfirmedBlockCertificate::try_from(c)
                        .map_err(|_| NodeError::InvalidChainInfoResponse)
                })
                .collect::<Result<_, _>>()?;
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
        Ok(certificate)
    }

    /// Uploads the blobs to the validator.
    #[instrument(level = "trace")]
    pub(crate) async fn upload_blobs(&self, blobs: Vec<Blob>) -> Result<(), NodeError> {
        let tasks = blobs
            .into_iter()
            .map(|blob| self.node.upload_blob(blob.into()));
        try_join_all(tasks).await?;
        Ok(())
    }

    /// Sends a pending validated block's blobs to the validator.
    #[instrument(level = "trace")]
    pub(crate) async fn send_pending_blobs(
        &self,
        chain_id: ChainId,
        blobs: Vec<Blob>,
    ) -> Result<(), NodeError> {
        let tasks = blobs
            .into_iter()
            .map(|blob| self.node.handle_pending_blob(chain_id, blob.into_content()));
        try_join_all(tasks).await?;
        Ok(())
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
        match self.node.download_blob(blob_id).await {
            Ok(blob) => {
                let blob = Blob::new(blob);
                if blob.id() != blob_id {
                    tracing::info!("Validator {} sent an invalid blob {blob_id}.", self.name);
                    None
                } else {
                    Some(blob)
                }
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

    #[instrument(level = "trace")]
    pub async fn download_certificates(
        &self,
        hashes: Vec<CryptoHash>,
    ) -> Result<Vec<ConfirmedBlockCertificate>, NodeError> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        self.node.download_certificates(hashes).await
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

    /// Checks that requesting these blobs when trying to handle this certificate is legitimate,
    /// i.e. that there are no duplicates and the blobs are actually required.
    pub fn check_blobs_not_found<T: CertificateValue>(
        &self,
        certificate: &GenericCertificate<T>,
        blob_ids: &[BlobId],
    ) -> Result<(), NodeError> {
        ensure!(!blob_ids.is_empty(), NodeError::EmptyBlobsNotFound);
        let required = certificate.inner().required_blob_ids();
        let name = &self.name;
        for blob_id in blob_ids {
            if !required.contains(blob_id) {
                warn!("validator {name} requested blob {blob_id:?} but it is not required");
                return Err(NodeError::UnexpectedEntriesInBlobsNotFound);
            }
        }
        let unique_missing_blob_ids = blob_ids.iter().cloned().collect::<HashSet<_>>();
        if blob_ids.len() > unique_missing_blob_ids.len() {
            warn!("blobs requested by validator {name} contain duplicates");
            return Err(NodeError::DuplicatesInBlobsNotFound);
        }
        Ok(())
    }
}

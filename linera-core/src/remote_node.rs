// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashSet, VecDeque};

use custom_debug_derive::Debug;
use futures::future::try_join_all;
use linera_base::{
    crypto::ValidatorPublicKey,
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
use tracing::{debug, info, instrument};

use crate::{
    data_types::{ChainInfo, ChainInfoQuery, ChainInfoResponse},
    node::{CrossChainMessageDelivery, NodeError, ValidatorNode},
};

/// A validator node together with the validator's name.
#[derive(Clone, Debug)]
pub struct RemoteNode<N> {
    pub public_key: ValidatorPublicKey,
    #[debug(skip)]
    pub node: N,
}

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
        if certificate.is_signed_by(&self.public_key) {
            let result = self
                .handle_lite_certificate(certificate.lite_certificate(), delivery)
                .await;
            match result {
                Err(NodeError::MissingCertificateValue) => {
                    debug!(
                        address = self.address(),
                        certificate_hash = %certificate.hash(),
                        "validator forgot a validated block value that they signed before",
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
        if certificate.is_signed_by(&self.public_key) {
            let result = self
                .handle_lite_certificate(certificate.lite_certificate(), delivery)
                .await;
            match result {
                Err(NodeError::MissingCertificateValue) => {
                    debug!(
                        address = self.address(),
                        certificate_hash = %certificate.hash(),
                        "validator forgot a confirmed block value that they signed before",
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
        let locking = manager.requested_locking.as_ref();
        ensure!(
            proposed.is_none_or(|proposal| proposal.content.block.chain_id == chain_id)
                && locking.is_none_or(|cert| cert.chain_id() == chain_id)
                && response.check(self.public_key).is_ok(),
            NodeError::InvalidChainInfoResponse
        );
        Ok(response.info)
    }

    #[instrument(level = "trace")]
    pub(crate) async fn download_certificate_for_blob(
        &self,
        blob_id: BlobId,
    ) -> Result<ConfirmedBlockCertificate, NodeError> {
        let certificate = self.node.blob_last_used_by_certificate(blob_id).await?;
        if !certificate.block().requires_or_creates_blob(&blob_id) {
            info!(
                address = self.address(),
                %blob_id,
                "got invalid last used by certificate for blob from validator",
            );
            return Err(NodeError::InvalidCertificateForBlob(blob_id));
        }
        Ok(certificate)
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

    #[instrument(level = "trace")]
    pub async fn download_blob(&self, blob_id: BlobId) -> Result<Option<Blob>, NodeError> {
        match self.node.download_blob(blob_id).await {
            Ok(blob) => {
                let blob = Blob::new(blob);
                if blob.id() != blob_id {
                    tracing::info!(
                        address = self.address(),
                        %blob_id,
                        "validator sent an invalid blob.",
                    );
                    Ok(None)
                } else {
                    Ok(Some(blob))
                }
            }
            Err(NodeError::BlobsNotFound(_error)) => {
                tracing::debug!(
                    ?blob_id,
                    address = self.address(),
                    "validator is missing the blob",
                );
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Downloads a list of certificates from the given chain.
    #[instrument(level = "trace")]
    pub async fn download_certificates_by_heights(
        &self,
        chain_id: ChainId,
        heights: Vec<BlockHeight>,
    ) -> Result<Vec<ConfirmedBlockCertificate>, NodeError> {
        let mut expected_heights = VecDeque::from(heights.clone());
        let certificates = self
            .node
            .download_certificates_by_heights(chain_id, heights)
            .await?;

        if certificates.len() > expected_heights.len() {
            return Err(NodeError::TooManyCertificatesReturned {
                chain_id,
                remote_node: Box::new(self.public_key),
            });
        }

        for certificate in &certificates {
            ensure!(
                certificate.inner().chain_id() == chain_id,
                NodeError::UnexpectedCertificateValue
            );
            if let Some(expected_height) = expected_heights.pop_front() {
                ensure!(
                    expected_height == certificate.inner().height(),
                    NodeError::UnexpectedCertificateValue
                );
            } else {
                return Err(NodeError::UnexpectedCertificateValue);
            }
        }

        ensure!(
            expected_heights.is_empty(),
            NodeError::MissingCertificatesByHeights {
                chain_id,
                heights: expected_heights.into_iter().collect(),
            }
        );
        Ok(certificates)
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
        for blob_id in blob_ids {
            if !required.contains(blob_id) {
                info!(
                    address = self.address(),
                    %blob_id,
                    "validator requested blob but it is not required",
                );
                return Err(NodeError::UnexpectedEntriesInBlobsNotFound);
            }
        }
        let unique_missing_blob_ids = blob_ids.iter().copied().collect::<HashSet<_>>();
        if blob_ids.len() > unique_missing_blob_ids.len() {
            info!(
                address = self.address(),
                "blobs requested by validator contain duplicates",
            );
            return Err(NodeError::DuplicatesInBlobsNotFound);
        }
        Ok(())
    }

    /// Returns the validator's URL.
    pub fn address(&self) -> String {
        self.node.address()
    }
}

impl<N: ValidatorNode> PartialEq for RemoteNode<N> {
    fn eq(&self, other: &Self) -> bool {
        self.public_key == other.public_key
    }
}

impl<N: ValidatorNode> Eq for RemoteNode<N> {}

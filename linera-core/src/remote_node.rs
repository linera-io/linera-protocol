// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashSet, VecDeque},
    time::Duration,
};

use custom_debug_derive::Debug;
use futures::{future::try_join_all, stream::FuturesUnordered, StreamExt};
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
use rand::seq::SliceRandom as _;
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
                        "Validator {} forgot a certificate value that they signed before",
                        self.public_key
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
                        "Validator {} forgot a certificate value that they signed before",
                        self.public_key
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

    #[instrument(level = "trace", skip_all)]
    pub(crate) async fn query_certificates_from(
        &self,
        chain_id: ChainId,
        start: BlockHeight,
        limit: u64,
    ) -> Result<Vec<ConfirmedBlockCertificate>, NodeError> {
        tracing::debug!(name = ?self.public_key, ?chain_id, ?start, ?limit, "Querying certificates");
        let heights = (start.0..start.0 + limit)
            .map(BlockHeight)
            .collect::<Vec<_>>();
        self.download_certificates_by_heights(chain_id, heights)
            .await
    }

    #[instrument(level = "trace")]
    pub(crate) async fn download_certificate_for_blob(
        &self,
        blob_id: BlobId,
    ) -> Result<ConfirmedBlockCertificate, NodeError> {
        let certificate = self.node.blob_last_used_by_certificate(blob_id).await?;
        if !certificate.block().requires_or_creates_blob(&blob_id) {
            info!(
                "Got invalid last used by certificate for blob {} from validator {}",
                blob_id, self.public_key
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
    pub async fn try_download_blob(&self, blob_id: BlobId) -> Option<Blob> {
        match self.node.download_blob(blob_id).await {
            Ok(blob) => {
                let blob = Blob::new(blob);
                if blob.id() != blob_id {
                    tracing::info!(
                        "Validator {} sent an invalid blob {blob_id}.",
                        self.public_key
                    );
                    None
                } else {
                    Some(blob)
                }
            }
            Err(error) => {
                tracing::debug!(
                    "Failed to fetch blob {blob_id} from validator {}: {error}",
                    self.public_key
                );
                None
            }
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

    /// Downloads a blob, but does not verify if it has actually been published and
    /// accepted by a quorum of validators.
    #[instrument(level = "trace", skip(validators))]
    pub async fn download_blob(
        validators: &[Self],
        blob_id: BlobId,
        timeout: Duration,
    ) -> Option<Blob> {
        // Sequentially try each validator in random order.
        let mut validators = validators.iter().collect::<Vec<_>>();
        validators.shuffle(&mut rand::thread_rng());
        let mut stream = validators
            .into_iter()
            .zip(0..)
            .map(|(remote_node, i)| async move {
                linera_base::time::timer::sleep(timeout * i * i).await;
                remote_node.try_download_blob(blob_id).await
            })
            .collect::<FuturesUnordered<_>>();
        while let Some(maybe_blob) = stream.next().await {
            if let Some(blob) = maybe_blob {
                return Some(blob);
            }
        }
        None
    }

    /// Downloads the blobs with the given IDs. This is done in one concurrent task per block.
    /// Each task goes through the validators sequentially in random order and tries to download
    /// it. Returns `None` if it couldn't find all blobs.
    #[instrument(level = "trace", skip(validators))]
    pub async fn download_blobs(
        blob_ids: &[BlobId],
        validators: &[Self],
        timeout: Duration,
    ) -> Option<Vec<Blob>> {
        use futures::FutureExt as _;
        use std::collections::{HashMap, VecDeque};

        let validators: HashMap<_, _> = validators.into_iter().map(|v| (&v.public_key, v)).collect();

        // randomize the blobs
        // assign slots to validators
        // sort the validators by slots
        // for each blob, ready a timeout event with no validator
        // for each event:
        // - if okay, amend the completed blobs and add a slot to the validator (and cancel)
        // - if cancel, add a slot to the validator
        // - if timeout or error, put the task back on the queue (but don't increase slots)
        // - if validator slots and tasks in queue, assign task to validator and start new task and timeout
        // if incomplete, return None, else return all blobs

        enum Event {
            Complete {
                node: ValidatorPublicKey,
                // `None` if the task was canceled (another task already completed the blob).
                blob: Option<Blob>,
            },
            Incomplete {
                blob_id: BlobId,
            },
        }

        let mut events = FuturesUnordered::default();
        for blob_id in blob_ids {
            let blob_id = *blob_id;
            events.push(async move {
                Event::Incomplete { blob_id }
            }.boxed_local());
        }

        let num_slots = blob_ids.len().div_ceil(validators.len());
        let mut slots: HashMap<_, _> = validators.keys().map(|v| (**v, num_slots)).collect();
        let mut remaining_blobs = VecDeque::new();
        let mut completed_blobs = HashMap::new();

        while let Some(event) = events.next().await {
            match event {
                Event::Complete { node, blob } => {
                    // TODO cancel blob
                    *slots.entry(node).or_default() += 1;

                    if let Some(blob) = blob {
                        completed_blobs.insert(blob.id(), blob);
                    }
                }

                Event::Incomplete { blob_id } => {
                    remaining_blobs.push_back(blob_id);
                }
            }

            let mut available_validators = slots
                .iter_mut()
                .filter(|(_name, slots)| **slots > 0)
                .collect::<VecDeque<_>>();

            loop {
                let Some((node, slots)) = available_validators.front_mut() else { break };
                assert!(**slots > 0);
                let Some(blob_id) = remaining_blobs.pop_front() else { break };
                let node = **node;
                let validator = validators.get(&node).unwrap();

                events.push(async move {
                    if let Some(blob) = validator.try_download_blob(blob_id).await {
                        Event::Complete { node, blob: Some(blob) }
                    } else {
                        Event::Incomplete { blob_id }
                    }
                }.boxed_local());

                events.push(async move {
                    linera_base::time::timer::sleep(timeout).await;
                    Event::Incomplete { blob_id }
                }.boxed_local());

                **slots -= 1;
                if **slots == 0 { available_validators.pop_front(); }
            }
        }

        if !remaining_blobs.is_empty() {
            None
        } else {
            let mut blobs = Vec::new();
            for blob_id in blob_ids {
                blobs.push(completed_blobs.remove(blob_id).expect("missing blob"));
            }
            Some(blobs)
        }
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
        let public_key = &self.public_key;
        for blob_id in blob_ids {
            if !required.contains(blob_id) {
                info!("validator {public_key} requested blob {blob_id:?} but it is not required");
                return Err(NodeError::UnexpectedEntriesInBlobsNotFound);
            }
        }
        let unique_missing_blob_ids = blob_ids.iter().copied().collect::<HashSet<_>>();
        if blob_ids.len() > unique_missing_blob_ids.len() {
            info!("blobs requested by validator {public_key} contain duplicates");
            return Err(NodeError::DuplicatesInBlobsNotFound);
        }
        Ok(())
    }
}

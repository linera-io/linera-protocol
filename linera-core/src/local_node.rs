// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Cow, sync::Arc};

use futures::future;
use linera_base::{
    data_types::{ArithmeticError, Blob, BlockHeight, HashedBlob},
    identifiers::{BlobId, ChainId, MessageId},
};
use linera_chain::{
    data_types::{
        Block, BlockProposal, Certificate, ExecutedBlock, HashedCertificateValue, LiteCertificate,
    },
    ChainStateView,
};
use linera_execution::{
    committee::ValidatorName, BytecodeLocation, Query, Response, UserApplicationDescription,
    UserApplicationId,
};
use linera_storage::Storage;
use linera_views::views::ViewError;
use rand::prelude::SliceRandom;
use thiserror::Error;
use tokio::sync::OwnedRwLockReadGuard;

use crate::{
    data_types::{BlockHeightRange, ChainInfo, ChainInfoQuery, ChainInfoResponse},
    node::{LocalValidatorNode, NodeError},
    value_cache::ValueCache,
    worker::{Notification, WorkerError, WorkerState},
};

/// A local node with a single worker, typically used by clients.
pub struct LocalNode<S>
where
    S: Storage,
    ViewError: From<S::StoreError>,
{
    state: WorkerState<S>,
}

/// A client to a local node.
#[derive(Clone)]
pub struct LocalNodeClient<S>
where
    S: Storage,
    ViewError: From<S::StoreError>,
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

impl<S> LocalNodeClient<S>
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::StoreError>,
{
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn handle_block_proposal(
        &self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, LocalNodeError> {
        // In local nodes, we can trust fully_handle_certificate to carry all actions eventually.
        let (response, _actions) = self.node.state.handle_block_proposal(proposal).await?;
        Ok(response)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn handle_lite_certificate(
        &self,
        certificate: LiteCertificate<'_>,
        notifications: &mut impl Extend<Notification>,
    ) -> Result<ChainInfoResponse, LocalNodeError> {
        let full_cert = self.node.state.full_certificate(certificate).await?;
        let response = self
            .node
            .state
            .fully_handle_certificate_with_notifications(
                full_cert,
                vec![],
                vec![],
                Some(notifications),
            )
            .await?;
        Ok(response)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn handle_certificate(
        &self,
        certificate: Certificate,
        hashed_certificate_values: Vec<HashedCertificateValue>,
        hashed_blobs: Vec<HashedBlob>,
        notifications: &mut impl Extend<Notification>,
    ) -> Result<ChainInfoResponse, LocalNodeError> {
        let response = Box::pin(self.node.state.fully_handle_certificate_with_notifications(
            certificate,
            hashed_certificate_values,
            hashed_blobs,
            Some(notifications),
        ))
        .await?;
        Ok(response)
    }

    #[tracing::instrument(level = "trace", skip_all)]
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
    ViewError: From<S::StoreError>,
{
    #[tracing::instrument(level = "trace", skip_all)]
    pub fn new(state: WorkerState<S>) -> Self {
        Self {
            node: Arc::new(LocalNode { state }),
        }
    }
}

impl<S> LocalNodeClient<S>
where
    S: Storage + Clone,
    ViewError: From<S::StoreError>,
{
    #[tracing::instrument(level = "trace", skip_all)]
    pub(crate) fn storage_client(&self) -> S {
        self.node.state.storage_client().clone()
    }
}

impl<S> LocalNodeClient<S>
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::StoreError>,
{
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn stage_block_execution(
        &self,
        block: Block,
    ) -> Result<(ExecutedBlock, ChainInfoResponse), LocalNodeError> {
        let (executed_block, info) = self.node.state.stage_block_execution(block).await?;
        Ok((executed_block, info))
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn find_missing_application_bytecodes(
        &self,
        locations: &[BytecodeLocation],
        node: &impl LocalValidatorNode,
        name: &ValidatorName,
    ) -> Vec<HashedCertificateValue> {
        future::join_all(locations.iter().map(|location| async move {
            Self::try_download_hashed_certificate_value_from(node, name, *location).await
        }))
        .await
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn find_missing_blobs(
        &self,
        blob_ids: &[BlobId],
        node: &impl LocalValidatorNode,
        name: &ValidatorName,
    ) -> Vec<HashedBlob> {
        future::join_all(
            blob_ids.iter().map(|blob_id| async move {
                Self::try_download_blob_from(name, node, *blob_id).await
            }),
        )
        .await
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn try_process_certificates(
        &self,
        name: &ValidatorName,
        node: &impl LocalValidatorNode,
        chain_id: ChainId,
        certificates: Vec<Certificate>,
        notifications: &mut impl Extend<Notification>,
    ) -> Option<Box<ChainInfo>> {
        let mut info = None;
        for certificate in certificates {
            let hash = certificate.hash();
            if !certificate.value().is_confirmed() || certificate.value().chain_id() != chain_id {
                // The certificate is not as expected. Give up.
                tracing::warn!("Failed to process network certificate {}", hash);
                return info;
            }
            let mut result = self
                .handle_certificate(certificate.clone(), vec![], vec![], notifications)
                .await;

            result = match &result {
                Err(LocalNodeError::WorkerError(
                    WorkerError::ApplicationBytecodesOrBlobsNotFound(locations, blob_ids),
                )) => {
                    let values = self
                        .find_missing_application_bytecodes(locations, node, name)
                        .await;
                    let blobs = self.find_missing_blobs(blob_ids, node, name).await;
                    if values.len() != locations.len() || blobs.len() != blob_ids.len() {
                        result
                    } else {
                        self.handle_certificate(certificate, values, blobs, notifications)
                            .await
                    }
                }
                _ => result,
            };

            match result {
                Ok(response) => info = Some(response.info),
                Err(error) => {
                    // The certificate is not as expected. Give up.
                    tracing::warn!("Failed to process network certificate {}: {}", hash, error);
                    return info;
                }
            };
        }
        // Done with all certificates.
        info
    }

    #[tracing::instrument(level = "trace", skip(self))]
    /// Returns a read-only view of the [`ChainStateView`] of a chain referenced by its
    /// [`ChainId`].
    ///
    /// The returned view holds a lock on the chain state, which prevents the local node from
    /// changing the state of that chain.
    pub async fn chain_state_view(
        &self,
        chain_id: ChainId,
    ) -> Result<OwnedRwLockReadGuard<ChainStateView<S::Context>>, WorkerError> {
        self.node.state.chain_state_view(chain_id).await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub(crate) async fn local_chain_info(
        &self,
        chain_id: ChainId,
    ) -> Result<Box<ChainInfo>, LocalNodeError> {
        let query = ChainInfoQuery::new(chain_id);
        Ok(self.handle_chain_info_query(query).await?.info)
    }

    #[tracing::instrument(level = "trace", skip(self, query))]
    pub async fn query_application(
        &self,
        chain_id: ChainId,
        query: Query,
    ) -> Result<Response, LocalNodeError> {
        let response = self.node.state.query_application(chain_id, query).await?;
        Ok(response)
    }

    #[tracing::instrument(level = "trace", skip(self))]
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

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn recent_blob(&self, blob_id: &BlobId) -> Option<HashedBlob> {
        self.node.state.recent_blob(blob_id).await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn recent_hashed_blobs(&self) -> Arc<ValueCache<BlobId, HashedBlob>> {
        self.node.state.recent_hashed_blobs()
    }

    #[tracing::instrument(level = "trace", skip(self, hashed_blob), fields(blob_id = ?hashed_blob.id))]
    pub async fn cache_recent_blob(&self, hashed_blob: &HashedBlob) -> bool {
        self.node
            .state
            .cache_recent_blob(Cow::Borrowed(hashed_blob))
            .await
    }

    #[tracing::instrument(level = "trace", skip(self, validators, notifications))]
    pub async fn download_certificates(
        &self,
        validators: &[(ValidatorName, impl LocalValidatorNode)],
        chain_id: ChainId,
        target_next_block_height: BlockHeight,
        notifications: &mut impl Extend<Notification>,
    ) -> Result<Box<ChainInfo>, LocalNodeError> {
        // Sequentially try each validator in random order.
        let mut validators: Vec<_> = validators.iter().collect();
        validators.shuffle(&mut rand::thread_rng());
        for (name, node) in validators {
            let info = self.local_chain_info(chain_id).await?;
            if target_next_block_height <= info.next_block_height {
                return Ok(info);
            }
            self.try_download_certificates_from(
                name,
                node,
                chain_id,
                info.next_block_height,
                target_next_block_height,
                notifications,
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

    #[tracing::instrument(level = "trace", skip_all)]
    /// Downloads and stores the specified hashed certificate values, unless they are already in the cache or storage.
    ///
    /// Does not fail if a hashed certificate value can't be downloaded; it just gets omitted from the result.
    pub async fn read_or_download_hashed_certificate_values(
        &self,
        validators: &[(ValidatorName, impl LocalValidatorNode)],
        hashed_certificate_value_locations: impl IntoIterator<Item = BytecodeLocation>,
    ) -> Result<Vec<HashedCertificateValue>, LocalNodeError> {
        let mut values = vec![];
        let mut tasks = vec![];
        for location in hashed_certificate_value_locations {
            if let Some(value) = self
                .node
                .state
                .recent_hashed_certificate_value(&location.certificate_hash)
                .await
            {
                values.push(value);
            } else {
                tasks.push(Self::read_or_download_hashed_certificate_value(
                    self.storage_client(),
                    validators,
                    location,
                ));
            }
        }
        if tasks.is_empty() {
            return Ok(values);
        }
        let results = future::join_all(tasks).await;
        for result in results {
            if let Some(value) = result? {
                self.node
                    .state
                    .cache_recent_hashed_certificate_value(Cow::Borrowed(&value))
                    .await;
                values.push(value);
            }
        }
        Ok(values)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn read_or_download_hashed_certificate_value(
        storage: S,
        validators: &[(ValidatorName, impl LocalValidatorNode)],
        location: BytecodeLocation,
    ) -> Result<Option<HashedCertificateValue>, LocalNodeError> {
        match storage
            .read_hashed_certificate_value(location.certificate_hash)
            .await
        {
            Ok(hashed_certificate_value) => return Ok(Some(hashed_certificate_value)),
            Err(ViewError::NotFound(..)) => {}
            Err(err) => Err(err)?,
        }
        match Self::download_hashed_certificate_value(validators, location).await {
            Some(hashed_certificate_value) => {
                storage
                    .write_hashed_certificate_value(&hashed_certificate_value)
                    .await?;
                Ok(Some(hashed_certificate_value))
            }
            None => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    /// Obtains the certificate containing the specified message.
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

    #[tracing::instrument(level = "trace", skip_all)]
    async fn try_download_certificates_from(
        &self,
        name: &ValidatorName,
        node: &impl LocalValidatorNode,
        chain_id: ChainId,
        mut start: BlockHeight,
        stop: BlockHeight,
        notifications: &mut impl Extend<Notification>,
    ) -> Result<(), LocalNodeError> {
        while start < stop {
            // TODO(#2045): Analyze network errors instead of guessing the batch size.
            let limit = u64::from(stop)
                .checked_sub(u64::from(start))
                .ok_or(ArithmeticError::Overflow)?
                .min(1000);
            let Some(certificates) = self
                .try_query_certificates_from(name, node, chain_id, start, limit)
                .await?
            else {
                break;
            };
            let Some(info) = self
                .try_process_certificates(name, node, chain_id, certificates, notifications)
                .await
            else {
                break;
            };
            assert!(info.next_block_height > start);
            start = info.next_block_height;
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn try_query_certificates_from(
        &self,
        name: &ValidatorName,
        node: &impl LocalValidatorNode,
        chain_id: ChainId,
        start: BlockHeight,
        limit: u64,
    ) -> Result<Option<Vec<Certificate>>, LocalNodeError> {
        tracing::debug!(?name, ?chain_id, ?start, ?limit, "Querying certificates");
        let range = BlockHeightRange {
            start,
            limit: Some(limit),
        };
        let query = ChainInfoQuery::new(chain_id).with_sent_certificate_hashes_in_range(range);
        if let Ok(response) = node.handle_chain_info_query(query).await {
            if response.check(name).is_err() {
                return Ok(None);
            }
            let ChainInfo {
                requested_sent_certificate_hashes,
                ..
            } = *response.info;

            let certificates = future::try_join_all(
                requested_sent_certificate_hashes
                    .into_iter()
                    .map(|hash| node.download_certificate(hash)),
            )
            .await?;
            Ok(Some(certificates))
        } else {
            Ok(None)
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn synchronize_chain_state(
        &self,
        validators: &[(ValidatorName, impl LocalValidatorNode)],
        chain_id: ChainId,
        notifications: &mut impl Extend<Notification>,
    ) -> Result<Box<ChainInfo>, LocalNodeError> {
        let mut futures = vec![];

        for (name, node) in validators {
            let client = self.clone();
            let mut notifications = vec![];
            futures.push(async move {
                (
                    client
                        .try_synchronize_chain_state_from(name, node, chain_id, &mut notifications)
                        .await,
                    notifications,
                )
            });
        }

        for (result, notifications_) in future::join_all(futures).await {
            if let Err(e) = result {
                tracing::error!(?e, "Error synchronizing chain state");
            }

            notifications.extend(notifications_);
        }

        self.local_chain_info(chain_id).await
    }

    #[tracing::instrument(level = "trace", skip(self, name, node, chain_id, notifications))]
    pub async fn try_synchronize_chain_state_from(
        &self,
        name: &ValidatorName,
        node: &impl LocalValidatorNode,
        chain_id: ChainId,
        notifications: &mut impl Extend<Notification>,
    ) -> Result<(), LocalNodeError> {
        let local_info = self.local_chain_info(chain_id).await?;
        let range = BlockHeightRange {
            start: local_info.next_block_height,
            limit: None,
        };
        let query = ChainInfoQuery::new(chain_id)
            .with_sent_certificate_hashes_in_range(range)
            .with_manager_values();
        let info = match node.handle_chain_info_query(query).await {
            Ok(response) if response.check(name).is_ok() => response.info,
            Ok(_) => {
                tracing::warn!("Ignoring invalid response from validator");
                // Give up on this validator.
                return Ok(());
            }
            Err(err) => {
                tracing::warn!("Ignoring error from validator: {}", err);
                return Ok(());
            }
        };

        let certificates = future::try_join_all(
            info.requested_sent_certificate_hashes
                .into_iter()
                .map(move |hash| async move { node.download_certificate(hash).await }),
        )
        .await?;

        if !certificates.is_empty()
            && self
                .try_process_certificates(name, node, chain_id, certificates, notifications)
                .await
                .is_none()
        {
            return Ok(());
        };
        if let Some(proposal) = info.manager.requested_proposed {
            if proposal.content.block.chain_id == chain_id {
                let owner = proposal.owner;
                if let Err(error) = self.handle_block_proposal(*proposal).await {
                    tracing::warn!("Skipping proposal from {}: {}", owner, error);
                }
            }
        }
        if let Some(cert) = info.manager.requested_locked {
            if cert.value().is_validated() && cert.value().chain_id() == chain_id {
                let hash = cert.hash();
                if let Err(error) = self
                    .handle_certificate(*cert, vec![], vec![], notifications)
                    .await
                {
                    tracing::warn!("Skipping certificate {}: {}", hash, error);
                }
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(validators, location))]
    pub async fn download_hashed_certificate_value(
        validators: &[(ValidatorName, impl LocalValidatorNode)],
        location: BytecodeLocation,
    ) -> Option<HashedCertificateValue> {
        // Sequentially try each validator in random order, to improve efficiency.
        let mut validators: Vec<_> = validators.iter().collect();
        validators.shuffle(&mut rand::thread_rng());
        for (name, node) in validators {
            if let Some(value) =
                Self::try_download_hashed_certificate_value_from(node, name, location).await
            {
                return Some(value);
            }
        }
        None
    }

    #[tracing::instrument(level = "trace", skip(validators))]
    pub async fn download_blob(
        validators: &[(ValidatorName, impl LocalValidatorNode)],
        blob_id: BlobId,
    ) -> Option<HashedBlob> {
        // Sequentially try each validator in random order.
        let mut validators: Vec<_> = validators.iter().collect();
        validators.shuffle(&mut rand::thread_rng());
        for (name, node) in validators {
            if let Some(blob) = Self::try_download_blob_from(name, node, blob_id).await {
                return Some(blob);
            }
        }
        None
    }

    #[tracing::instrument(level = "trace", skip(name, node))]
    async fn try_download_blob_from(
        name: &ValidatorName,
        node: &impl LocalValidatorNode,
        blob_id: BlobId,
    ) -> Option<HashedBlob> {
        match node.download_blob(blob_id).await.map(Blob::into_hashed) {
            Ok(hashed_blob) if hashed_blob.id() == blob_id => Some(hashed_blob),
            Ok(_) => {
                tracing::info!("Validator {name} sent an invalid blob {blob_id}.");
                None
            }
            Err(error) => {
                tracing::debug!("Failed to fetch blob {blob_id} from validator {name}: {error}");
                None
            }
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn try_download_hashed_certificate_value_from(
        node: &impl LocalValidatorNode,
        name: &ValidatorName,
        location: BytecodeLocation,
    ) -> Option<HashedCertificateValue> {
        match node
            .download_certificate_value(location.certificate_hash)
            .await
        {
            Ok(hashed_certificate_value) => Some(hashed_certificate_value),
            Err(error) => {
                tracing::debug!(
                    "Failed to fetch certificate value {location:?} from validator {name}: {error}"
                );
                None
            }
        }
    }
}

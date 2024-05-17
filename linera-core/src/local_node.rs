// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Cow, collections::VecDeque, sync::Arc};

use futures::{future, lock::Mutex};
use linera_base::{
    data_types::{ArithmeticError, BlockHeight},
    identifiers::{ChainId, MessageId},
};
use linera_chain::data_types::{
    Block, BlockProposal, Certificate, ExecutedBlock, HashedValue, LiteCertificate,
};
use linera_execution::{
    committee::ValidatorName, BytecodeLocation, Query, Response, UserApplicationDescription,
    UserApplicationId,
};
use linera_storage::Storage;
use linera_views::views::ViewError;
use rand::prelude::SliceRandom;
use thiserror::Error;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::{
    data_types::{BlockHeightRange, ChainInfo, ChainInfoQuery, ChainInfoResponse},
    node::{LocalValidatorNode, NotificationStream},
    notifier::Notifier,
    worker::{Notification, ValidatorWorker, WorkerError, WorkerState},
};

/// A local node with a single worker, typically used by clients.
pub struct LocalNode<S> {
    state: WorkerState<S>,
    notifier: Arc<Notifier<Notification>>,
}

/// A client to a local node.
#[derive(Clone)]
pub struct LocalNodeClient<S> {
    node: Arc<Mutex<LocalNode<S>>>,
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

    #[error("The local node doesn't have an active chain {0:?}")]
    InactiveChain(ChainId),

    #[error("The chain info response received from the local node is invalid")]
    InvalidChainInfoResponse,
}

impl<S> LocalNodeClient<S>
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    pub async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, LocalNodeError> {
        let mut node = self.node.lock().await;
        // In local nodes, we can trust fully_handle_certificate to carry all actions eventually.
        let (response, _actions) = node.state.handle_block_proposal(proposal).await?;
        Ok(response)
    }

    pub async fn handle_lite_certificate(
        &mut self,
        certificate: LiteCertificate<'_>,
    ) -> Result<ChainInfoResponse, LocalNodeError> {
        let mut node = self.node.lock().await;
        let mut notifications = Vec::new();
        let full_cert = node.state.full_certificate(certificate).await?;
        let response = node
            .state
            .fully_handle_certificate_with_notifications(full_cert, &[], Some(&mut notifications))
            .await?;
        node.notifier.handle_notifications(&notifications);
        Ok(response)
    }

    pub async fn handle_certificate(
        &mut self,
        certificate: Certificate,
        blobs: &[HashedValue],
    ) -> Result<ChainInfoResponse, LocalNodeError> {
        let mut node = self.node.lock().await;
        let mut notifications = Vec::new();
        let response = node
            .state
            .fully_handle_certificate_with_notifications(
                certificate,
                blobs,
                Some(&mut notifications),
            )
            .await?;
        node.notifier.handle_notifications(&notifications);
        Ok(response)
    }

    pub async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, LocalNodeError> {
        let node = self.node.lock().await;
        // In local nodes, we can trust fully_handle_certificate to carry all actions eventually.
        let (response, _actions) = node.state.handle_chain_info_query(query).await?;
        Ok(response)
    }

    pub async fn subscribe(
        &mut self,
        chains: Vec<ChainId>,
    ) -> Result<NotificationStream, LocalNodeError> {
        let node = self.node.lock().await;
        let rx = node.notifier.subscribe(chains);
        Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    }
}

impl<S> LocalNodeClient<S> {
    pub fn new(state: WorkerState<S>, notifier: Arc<Notifier<Notification>>) -> Self {
        let node = LocalNode { state, notifier };

        Self {
            node: Arc::new(Mutex::new(node)),
        }
    }
}

impl<S> LocalNodeClient<S>
where
    S: Clone,
{
    pub(crate) async fn storage_client(&self) -> S {
        let node = self.node.lock().await;
        node.state.storage_client().clone()
    }
}

impl<S> LocalNodeClient<S>
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    pub(crate) async fn stage_block_execution(
        &self,
        block: Block,
    ) -> Result<(ExecutedBlock, ChainInfoResponse), LocalNodeError> {
        let mut node = self.node.lock().await;
        let (executed_block, info) = node.state.stage_block_execution(block).await?;
        Ok((executed_block, info))
    }

    async fn try_process_certificates<A>(
        &mut self,
        name: ValidatorName,
        node: &mut A,
        chain_id: ChainId,
        certificates: Vec<Certificate>,
    ) -> Option<Box<ChainInfo>>
    where
        A: LocalValidatorNode + Clone + 'static,
    {
        let mut info = None;
        for certificate in certificates {
            let hash = certificate.hash();
            if !certificate.value().is_confirmed() || certificate.value().chain_id() != chain_id {
                // The certificate is not as expected. Give up.
                tracing::warn!("Failed to process network certificate {}", hash);
                return info;
            }
            let mut result = self.handle_certificate(certificate.clone(), &[]).await;
            if let Err(LocalNodeError::WorkerError(WorkerError::ApplicationBytecodesNotFound(
                locations,
            ))) = &result
            {
                let blobs = Self::load_missing_bytecodes(name, node, chain_id, locations).await;
                result = self.handle_certificate(certificate.clone(), &blobs).await;
            }
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

    /// Uses the provided `node` to fetch the certificates that contain the needed bytecodes.
    async fn load_missing_bytecodes<A>(
        name: ValidatorName,
        node: &mut A,
        chain_id: ChainId,
        locations: &[BytecodeLocation],
    ) -> Vec<HashedValue>
    where
        A: LocalValidatorNode + Clone + 'static,
    {
        let maybe_blobs =
            future::join_all(
                locations.iter().map(|location| {
                    let mut node = node.clone();
                    async move {
                        Self::try_download_blob_from(name, &mut node, chain_id, *location).await
                    }
                }),
            )
            .await;
        maybe_blobs
            .into_iter()
            .filter_map(|maybe_blob| {
                if maybe_blob.is_none() {
                    tracing::warn!("Failed to process network blob");
                }
                maybe_blob
            })
            .collect()
    }

    pub(crate) async fn local_chain_info(
        &mut self,
        chain_id: ChainId,
    ) -> Result<Box<ChainInfo>, LocalNodeError> {
        let query = ChainInfoQuery::new(chain_id);
        Ok(self.handle_chain_info_query(query).await?.info)
    }

    pub async fn query_application(
        &self,
        chain_id: ChainId,
        query: Query,
    ) -> Result<Response, LocalNodeError> {
        let mut node = self.node.lock().await;
        let response = node.state.query_application(chain_id, query).await?;
        Ok(response)
    }

    pub async fn describe_application(
        &self,
        chain_id: ChainId,
        application_id: UserApplicationId,
    ) -> Result<UserApplicationDescription, LocalNodeError> {
        let mut node = self.node.lock().await;
        let response = node
            .state
            .describe_application(chain_id, application_id)
            .await?;
        Ok(response)
    }

    pub async fn download_certificates<A>(
        &mut self,
        mut validators: Vec<(ValidatorName, A)>,
        chain_id: ChainId,
        target_next_block_height: BlockHeight,
    ) -> Result<Box<ChainInfo>, LocalNodeError>
    where
        A: LocalValidatorNode + Clone + 'static,
    {
        // Sequentially try each validator in random order.
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

    /// Downloads and stores the specified blobs, unless they are already in the cache or storage.
    ///
    /// Does not fail if a blob can't be downloaded; it just gets omitted from the result.
    pub async fn read_or_download_blobs<A>(
        &mut self,
        validators: Vec<(ValidatorName, A)>,
        blob_locations: impl IntoIterator<Item = (BytecodeLocation, ChainId)>,
    ) -> Result<Vec<HashedValue>, LocalNodeError>
    where
        A: LocalValidatorNode + Clone + 'static,
    {
        let mut blobs = vec![];
        let mut tasks = vec![];
        let mut node = self.node.lock().await;
        for (location, chain_id) in blob_locations {
            if let Some(blob) = node.state.recent_value(&location.certificate_hash).await {
                blobs.push(blob);
            } else {
                let validators = validators.clone();
                let storage = node.state.storage_client().clone();
                tasks.push(Self::read_or_download_blob(
                    storage, validators, chain_id, location,
                ));
            }
        }
        drop(node); // Free the lock while awaiting the tasks.
        if tasks.is_empty() {
            return Ok(blobs);
        }
        let results = future::join_all(tasks).await;
        let mut node = self.node.lock().await;
        for result in results {
            if let Some(blob) = result? {
                node.state.cache_recent_value(Cow::Borrowed(&blob)).await;
                blobs.push(blob);
            }
        }
        Ok(blobs)
    }

    pub async fn read_or_download_blob<A>(
        storage: S,
        validators: Vec<(ValidatorName, A)>,
        chain_id: ChainId,
        location: BytecodeLocation,
    ) -> Result<Option<HashedValue>, LocalNodeError>
    where
        A: LocalValidatorNode + Clone + 'static,
    {
        match storage.read_value(location.certificate_hash).await {
            Ok(blob) => return Ok(Some(blob)),
            Err(ViewError::NotFound(..)) => {}
            Err(err) => Err(err)?,
        }
        match Self::download_blob(validators, chain_id, location).await {
            Some(blob) => {
                storage.write_value(&blob).await?;
                Ok(Some(blob))
            }
            None => Ok(None),
        }
    }

    /// Obtains the certificate containing the specified message.
    pub async fn certificate_for(
        &mut self,
        message_id: &MessageId,
    ) -> Result<Certificate, LocalNodeError> {
        let query = ChainInfoQuery::new(message_id.chain_id)
            .with_sent_certificates_in_range(BlockHeightRange::single(message_id.height));
        let info = self.handle_chain_info_query(query).await?.info;
        let certificate = info
            .requested_sent_certificates
            .into_iter()
            .find(|certificate| certificate.value().has_message(message_id))
            .ok_or_else(|| {
                ViewError::not_found("could not find certificate with message {}", message_id)
            })?;
        Ok(certificate)
    }

    async fn try_download_certificates_from<A>(
        &mut self,
        name: ValidatorName,
        mut node: A,
        chain_id: ChainId,
        start: BlockHeight,
        stop: BlockHeight,
    ) -> Result<(), LocalNodeError>
    where
        A: LocalValidatorNode + Clone + 'static,
    {
        let limit = u64::from(stop)
            .checked_sub(u64::from(start))
            .ok_or(ArithmeticError::Overflow)?;
        let range = BlockHeightRange {
            start,
            limit: Some(limit),
        };
        let query = ChainInfoQuery::new(chain_id).with_sent_certificates_in_range(range);
        if let Ok(response) = node.handle_chain_info_query(query).await {
            if response.check(name).is_ok() {
                let ChainInfo {
                    requested_sent_certificates,
                    ..
                } = *response.info;
                self.try_process_certificates(
                    name,
                    &mut node,
                    chain_id,
                    requested_sent_certificates,
                )
                .await;
            }
        }
        Ok(())
    }

    pub async fn synchronize_chain_state<A>(
        &mut self,
        validators: Vec<(ValidatorName, A)>,
        chain_id: ChainId,
    ) -> Result<Box<ChainInfo>, LocalNodeError>
    where
        A: LocalValidatorNode + Clone + 'static,
    {
        let futures = validators
            .into_iter()
            .map(|(name, node)| {
                let mut client = self.clone();
                async move {
                    client
                        .try_synchronize_chain_state_from(name, node, chain_id)
                        .await
                }
            })
            .collect::<Vec<_>>();
        futures::future::join_all(futures).await;
        let info = self.local_chain_info(chain_id).await?;
        Ok(info)
    }

    pub async fn try_synchronize_chain_state_from<A>(
        &mut self,
        name: ValidatorName,
        mut node: A,
        chain_id: ChainId,
    ) -> Result<(), LocalNodeError>
    where
        A: LocalValidatorNode + Clone + 'static,
    {
        let local_info = self.local_chain_info(chain_id).await?;
        let range = BlockHeightRange {
            start: local_info.next_block_height,
            limit: None,
        };
        let query = ChainInfoQuery::new(chain_id)
            .with_sent_certificates_in_range(range)
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
        if !info.requested_sent_certificates.is_empty()
            && self
                .try_process_certificates(
                    name,
                    &mut node,
                    chain_id,
                    info.requested_sent_certificates,
                )
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
                if let Err(error) = self.handle_certificate(*cert, &[]).await {
                    tracing::warn!("Skipping certificate {}: {}", hash, error);
                }
            }
        }
        Ok(())
    }

    pub async fn download_blob<A>(
        mut validators: Vec<(ValidatorName, A)>,
        chain_id: ChainId,
        location: BytecodeLocation,
    ) -> Option<HashedValue>
    where
        A: LocalValidatorNode + Clone + 'static,
    {
        // Sequentially try each validator in random order.
        validators.shuffle(&mut rand::thread_rng());
        for (name, mut node) in validators {
            if let Some(blob) =
                Self::try_download_blob_from(name, &mut node, chain_id, location).await
            {
                return Some(blob);
            }
        }
        None
    }

    async fn try_download_blob_from<A>(
        name: ValidatorName,
        node: &mut A,
        chain_id: ChainId,
        location: BytecodeLocation,
    ) -> Option<HashedValue>
    where
        A: LocalValidatorNode + Clone + 'static,
    {
        let query = ChainInfoQuery::new(chain_id).with_blob(location.certificate_hash);
        if let Ok(response) = node.handle_chain_info_query(query).await {
            if response.check(name).is_ok() {
                return response.info.requested_blob;
            }
        }
        None
    }

    /// Adds a chain to the set of chains tracked by this node.
    pub async fn track_chain(&mut self, chain_id: ChainId) {
        self.node.lock().await.state.track_chain(chain_id)
    }

    /// Handles any pending local cross-chain requests.
    pub async fn retry_pending_cross_chain_requests(
        &mut self,
        sender_chain: ChainId,
    ) -> Result<(), LocalNodeError> {
        let mut node = self.node.lock().await;
        let (_response, actions) = node
            .state
            .handle_chain_info_query(ChainInfoQuery::new(sender_chain))
            .await?;
        let mut requests = VecDeque::from_iter(actions.cross_chain_requests);
        while let Some(request) = requests.pop_front() {
            let new_actions = node.state.handle_cross_chain_request(request).await?;
            requests.extend(new_actions.cross_chain_requests);
        }
        Ok(())
    }
}

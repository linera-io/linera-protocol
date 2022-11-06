// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    messages::{BlockHeightRange, ChainInfo, ChainInfoQuery, ChainInfoResponse},
    worker::{ValidatorWorker, WorkerState},
};
use async_trait::async_trait;
use futures::lock::Mutex;
use linera_base::{
    crypto::CryptoError,
    error::Error,
    messages::{ApplicationId, BlockHeight, ChainId, Origin, ValidatorName},
};
use linera_chain::{
    messages::{Block, BlockProposal, Certificate, Value},
    ChainError, ChainManager,
};
use linera_storage::Store;
use linera_views::views::ViewError;
use rand::prelude::SliceRandom;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

/// How to communicate with a validator or a local node.
#[async_trait]
pub trait ValidatorNode {
    /// Propose a new block.
    async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Process a certificate.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Handle information queries for this chain.
    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, NodeError>;
}

/// Error type for node queries.
///
/// This error is meant to be serialized over the network and aggregated by clients (i.e.
/// clients will track validator votes on each error value).
// TODO(#148): We should have more entries here and never create a `WorkerError` outside the worker.
#[derive(Eq, PartialEq, Clone, Debug, Serialize, Deserialize, Error, Hash)]
pub enum NodeError {
    #[error(
        "Failed to download the requested certificate(s) for chain {chain_id:?} \
         in order to advance to the next height {target_next_block_height}"
    )]
    CannotDownloadCertificates {
        chain_id: ChainId,
        target_next_block_height: BlockHeight,
    },

    #[error("Validator's response to block proposal failed to include a vote")]
    MissingVoteInValidatorResponse,

    #[error(
        "Failed to update validator because our local node doesn't have an active chain {0:?}"
    )]
    InactiveLocalChain(ChainId),

    #[error(
        "Failed to submit block proposal: chain {chain_id:?} was still inactive \
         after validator synchronization and {retries} retries"
    )]
    ProposedBlockToInactiveChain { chain_id: ChainId, retries: usize },

    #[error(
        "Failed to submit block proposal: chain {chain_id:?} was still missing messages \
         after validator synchronization and {retries} retries"
    )]
    ProposedBlockWithLaggingMessages { chain_id: ChainId, retries: usize },

    #[error(
        "Cannot vote for block proposal of chain {chain_id:?} because a message \
         from chain {origin:?} at height {height:?} (application {application_id:?}) \
         has not been received yet"
    )]
    MissingCrossChainUpdate {
        chain_id: ChainId,
        application_id: ApplicationId,
        origin: Origin,
        height: BlockHeight,
    },

    #[error("Cannot confirm a block before its predecessors: {current_block_height:?}")]
    MissingEarlierBlocks { current_block_height: BlockHeight },

    #[error(
        "Was expecting block height {expected_block_height} but found {found_block_height} instead"
    )]
    UnexpectedBlockHeight {
        expected_block_height: BlockHeight,
        found_block_height: BlockHeight,
    },

    #[error("Error while accessing storage view: {error}")]
    ViewError { error: String },

    #[error("{0}")]
    WorkerError(#[from] linera_base::error::Error),

    #[error("Cryptographic error: {0}")]
    CryptoError(#[from] CryptoError),

    #[error("Chain error: {error}")]
    ChainError { error: String },
}

impl From<ViewError> for NodeError {
    fn from(error: ViewError) -> Self {
        Self::ViewError {
            error: error.to_string(),
        }
    }
}

impl From<ChainError> for NodeError {
    fn from(error: ChainError) -> Self {
        match error {
            ChainError::MissingCrossChainUpdate {
                chain_id,
                application_id,
                origin,
                height,
            } => Self::MissingCrossChainUpdate {
                chain_id,
                application_id,
                origin,
                height,
            },
            ChainError::MissingEarlierBlocks {
                current_block_height,
            } => Self::MissingEarlierBlocks {
                current_block_height,
            },
            ChainError::UnexpectedBlockHeight {
                expected_block_height,
                found_block_height,
            } => Self::UnexpectedBlockHeight {
                expected_block_height,
                found_block_height,
            },
            error => Self::ChainError {
                error: error.to_string(),
            },
        }
    }
}

/// A local replica, typically used by clients.
pub struct LocalNode<S> {
    state: WorkerState<S>,
}

#[derive(Clone)]
pub struct LocalNodeClient<S>(Arc<Mutex<LocalNode<S>>>);

#[async_trait]
impl<S> ValidatorNode for LocalNodeClient<S>
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, NodeError> {
        let node = self.0.clone();
        let mut node = node.lock().await;
        let response = node.state.handle_block_proposal(proposal).await?;
        Ok(response)
    }

    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<ChainInfoResponse, NodeError> {
        let node = self.0.clone();
        let mut node = node.lock().await;
        let response = node.state.fully_handle_certificate(certificate).await?;
        Ok(response)
    }

    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, NodeError> {
        let response = self
            .0
            .clone()
            .lock()
            .await
            .state
            .handle_chain_info_query(query)
            .await?;
        Ok(response)
    }
}

impl<S> LocalNodeClient<S> {
    pub fn new(state: WorkerState<S>) -> Self {
        let node = LocalNode { state };
        Self(Arc::new(Mutex::new(node)))
    }
}

impl<S> LocalNodeClient<S>
where
    S: Clone,
{
    pub(crate) async fn storage_client(&self) -> S {
        let node = self.0.clone();
        let node = node.lock().await;
        node.state.storage_client().clone()
    }
}

impl<S> LocalNodeClient<S>
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    pub(crate) async fn stage_block_execution(
        &self,
        block: &Block,
    ) -> Result<ChainInfoResponse, NodeError> {
        let info = self
            .0
            .clone()
            .lock()
            .await
            .state
            .stage_block_execution(block)
            .await?;
        Ok(info)
    }

    async fn try_process_certificates(
        &mut self,
        chain_id: ChainId,
        certificates: Vec<Certificate>,
    ) -> Result<Option<ChainInfo>, NodeError> {
        let mut info = None;
        for certificate in certificates {
            if let Value::ConfirmedBlock { block, .. } = &certificate.value {
                if block.chain_id == chain_id {
                    match self.handle_certificate(certificate.clone()).await {
                        Ok(response) => {
                            info = Some(response.info);
                            // Continue with the next certificate.
                            continue;
                        }
                        Err(
                            e @ (NodeError::WorkerError(Error::InvalidCertificate)
                            | NodeError::MissingEarlierBlocks { .. }),
                        ) => {
                            // The certificate is not as expected. Give up.
                            log::warn!(
                                "Failed to process network certificate {}: {}",
                                certificate.hash,
                                e
                            );
                            return Ok(info);
                        }
                        Err(e) => {
                            // Something is wrong: a valid certificate with the
                            // right block height should not fail to execute.
                            return Err(e);
                        }
                    }
                }
            }
            // The certificate is not as expected. Give up.
            log::warn!("Failed to process network certificate {}", certificate.hash);
            return Ok(info);
        }
        // Done with all certificates.
        Ok(info)
    }

    async fn local_chain_info(&mut self, chain_id: ChainId) -> Result<ChainInfo, NodeError> {
        let query = ChainInfoQuery::new(chain_id);
        Ok(self.handle_chain_info_query(query).await?.info)
    }

    pub async fn download_certificates<A>(
        &mut self,
        mut validators: Vec<(ValidatorName, A)>,
        chain_id: ChainId,
        target_next_block_height: BlockHeight,
    ) -> Result<ChainInfo, NodeError>
    where
        A: ValidatorNode + Send + Sync + 'static + Clone,
    {
        // Sequentially try each validator in random order.
        validators.shuffle(&mut rand::thread_rng());
        for (name, client) in validators {
            let info = self.local_chain_info(chain_id).await?;
            if target_next_block_height <= info.next_block_height {
                return Ok(info);
            }
            self.try_download_certificates_from(
                name,
                client,
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
            Err(NodeError::CannotDownloadCertificates {
                chain_id,
                target_next_block_height,
            })
        }
    }

    async fn try_download_certificates_from<A>(
        &mut self,
        name: ValidatorName,
        mut client: A,
        chain_id: ChainId,
        start: BlockHeight,
        stop: BlockHeight,
    ) -> Result<(), NodeError>
    where
        A: ValidatorNode + Send + Sync + 'static + Clone,
    {
        let range = BlockHeightRange {
            start,
            limit: Some(usize::from(stop) - usize::from(start)),
        };
        let query = ChainInfoQuery::new(chain_id).with_sent_certificates_in_range(range);
        if let Ok(response) = client.handle_chain_info_query(query).await {
            if response.check(name).is_ok() {
                let ChainInfo {
                    requested_sent_certificates,
                    ..
                } = response.info;
                self.try_process_certificates(chain_id, requested_sent_certificates)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn synchronize_chain_state<A>(
        &mut self,
        validators: Vec<(ValidatorName, A)>,
        chain_id: ChainId,
    ) -> Result<ChainInfo, NodeError>
    where
        A: ValidatorNode + Send + Sync + 'static + Clone,
    {
        let futures: Vec<_> = validators
            .into_iter()
            .map(|(name, client)| {
                let mut node = self.clone();
                async move {
                    node.try_synchronize_chain_state_from(name, client, chain_id)
                        .await
                }
            })
            .collect();
        futures::future::join_all(futures).await;
        let info = self.local_chain_info(chain_id).await?;
        Ok(info)
    }

    pub async fn try_synchronize_chain_state_from<A>(
        &mut self,
        name: ValidatorName,
        mut client: A,
        chain_id: ChainId,
    ) -> Result<(), NodeError>
    where
        A: ValidatorNode + Send + Sync + 'static + Clone,
    {
        let local_info = self.local_chain_info(chain_id).await?;
        let range = BlockHeightRange {
            start: local_info.next_block_height,
            limit: None,
        };
        let query = ChainInfoQuery::new(chain_id).with_sent_certificates_in_range(range);
        let info = match client.handle_chain_info_query(query).await {
            Ok(response) if response.check(name).is_ok() => response.info,
            Ok(_) => {
                log::warn!("Ignoring invalid response from validator");
                // Give up on this validator.
                return Ok(());
            }
            Err(err) => {
                log::warn!("Ignoring error from validator: {}", err);
                return Ok(());
            }
        };
        if self
            .try_process_certificates(chain_id, info.requested_sent_certificates)
            .await?
            .is_none()
        {
            return Ok(());
        };
        if let ChainManager::Multi(manager) = info.manager {
            if let Some(proposal) = manager.proposed {
                if proposal.content.block.chain_id == chain_id {
                    let owner = proposal.owner;
                    if let Err(error) = self.handle_block_proposal(proposal).await {
                        log::warn!("Skipping proposal from {}: {}", owner, error);
                    }
                }
            }
            if let Some(cert) = manager.locked {
                if let Value::ValidatedBlock { block, .. } = &cert.value {
                    if block.chain_id == chain_id {
                        let hash = cert.hash;
                        if let Err(error) = self.handle_certificate(cert).await {
                            log::warn!("Skipping certificate {}: {}", hash, error);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

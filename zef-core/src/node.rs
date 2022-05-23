// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::worker::{ValidatorWorker, WorkerState};
use async_trait::async_trait;
use futures::lock::Mutex;
use rand::prelude::SliceRandom;
use std::sync::Arc;
use zef_base::{base_types::*, error::Error, manager::ChainManager, messages::*};
use zef_storage::Storage;

/// How to communicate with a validator or a local node.
#[async_trait]
pub trait ValidatorNode {
    /// Propose a new block.
    async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, Error>;

    /// Process a certificate.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<ChainInfoResponse, Error>;

    /// Handle information queries for this chain.
    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, Error>;
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
    S: Storage + Clone + 'static,
{
    async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, Error> {
        let node = self.0.clone();
        let mut node = node.lock().await;
        node.state.handle_block_proposal(proposal).await
    }

    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<ChainInfoResponse, Error> {
        let node = self.0.clone();
        let mut node = node.lock().await;
        node.state.fully_handle_certificate(certificate).await
    }

    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, Error> {
        self.0
            .clone()
            .lock()
            .await
            .state
            .handle_chain_info_query(query)
            .await
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
    S: Storage + Clone + 'static,
{
    pub(crate) async fn stage_block_execution(
        &self,
        block: &Block,
    ) -> Result<ChainInfoResponse, Error> {
        self.0
            .clone()
            .lock()
            .await
            .state
            .stage_block_execution(block)
            .await
    }

    async fn try_process_certificates(
        &mut self,
        chain_id: ChainId,
        certificates: Vec<Certificate>,
    ) -> Result<Option<ChainInfo>, Error> {
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
                            e @ (Error::InvalidCertificate | Error::MissingEarlierBlocks { .. }),
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

    pub async fn local_chain_info(&mut self, chain_id: ChainId) -> Result<ChainInfo, Error> {
        let query = ChainInfoQuery {
            chain_id,
            check_next_block_height: None,
            query_committees: false,
            query_pending_messages: false,
            query_sent_certificates_in_range: None,
            query_received_certificates_excluding_first_nth: None,
        };
        Ok(self.handle_chain_info_query(query).await?.info)
    }

    pub async fn download_certificates<A>(
        &mut self,
        mut validators: Vec<(ValidatorName, A)>,
        chain_id: ChainId,
        target_next_block_height: BlockHeight,
    ) -> Result<ChainInfo, Error>
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
            Err(Error::ClientErrorWhileQueryingCertificate)
        }
    }

    pub async fn try_download_certificates_from<A>(
        &mut self,
        name: ValidatorName,
        mut client: A,
        chain_id: ChainId,
        start: BlockHeight,
        stop: BlockHeight,
    ) -> Result<(), Error>
    where
        A: ValidatorNode + Send + Sync + 'static + Clone,
    {
        let range = BlockHeightRange {
            start,
            limit: Some(usize::from(stop) - usize::from(start)),
        };
        let query = ChainInfoQuery {
            chain_id,
            check_next_block_height: None,
            query_committees: false,
            query_pending_messages: false,
            query_sent_certificates_in_range: Some(range),
            query_received_certificates_excluding_first_nth: None,
        };
        if let Ok(response) = client.handle_chain_info_query(query).await {
            if response.check(name).is_ok() {
                let ChainInfo {
                    queried_sent_certificates,
                    ..
                } = response.info;
                self.try_process_certificates(chain_id, queried_sent_certificates)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn synchronize_chain_state<A>(
        &mut self,
        validators: Vec<(ValidatorName, A)>,
        chain_id: ChainId,
    ) -> Result<ChainInfo, Error>
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
    ) -> Result<(), Error>
    where
        A: ValidatorNode + Send + Sync + 'static + Clone,
    {
        let local_info = self.local_chain_info(chain_id).await?;
        let range = BlockHeightRange {
            start: local_info.next_block_height,
            limit: None,
        };
        let query = ChainInfoQuery {
            chain_id,
            check_next_block_height: None,
            query_committees: false,
            query_pending_messages: false,
            query_sent_certificates_in_range: Some(range),
            query_received_certificates_excluding_first_nth: None,
        };
        let info = match client.handle_chain_info_query(query).await {
            Ok(response) if response.check(name).is_ok() => response.info,
            _ => {
                // Give up on this validator.
                log::warn!("Ignoring validator response");
                return Ok(());
            }
        };
        if self
            .try_process_certificates(chain_id, info.queried_sent_certificates)
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

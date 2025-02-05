// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    crypto::CryptoHash,
    data_types::BlobContent,
    identifiers::{BlobId, ChainId},
};
use linera_chain::{
    data_types::BlockProposal,
    types::{
        ConfirmedBlockCertificate, LiteCertificate, TimeoutCertificate, ValidatedBlockCertificate,
    },
};
use linera_core::{
    data_types::{ChainInfoQuery, ChainInfoResponse},
    node::{CrossChainMessageDelivery, NodeError, NotificationStream, ValidatorNode},
};

use crate::grpc::GrpcClient;
#[cfg(with_simple_network)]
use crate::simple::SimpleClient;

#[derive(Clone)]
pub enum Client {
    Grpc(GrpcClient),
    #[cfg(with_simple_network)]
    Simple(SimpleClient),
}

impl From<GrpcClient> for Client {
    fn from(client: GrpcClient) -> Self {
        Self::Grpc(client)
    }
}

#[cfg(with_simple_network)]
impl From<SimpleClient> for Client {
    fn from(client: SimpleClient) -> Self {
        Self::Simple(client)
    }
}

impl ValidatorNode for Client {
    type NotificationStream = NotificationStream;

    async fn handle_block_proposal(
        &self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, NodeError> {
        match self {
            Client::Grpc(grpc_client) => grpc_client.handle_block_proposal(proposal).await,

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => simple_client.handle_block_proposal(proposal).await,
        }
    }

    async fn handle_lite_certificate(
        &self,
        certificate: LiteCertificate<'_>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<ChainInfoResponse, NodeError> {
        match self {
            Client::Grpc(grpc_client) => {
                grpc_client
                    .handle_lite_certificate(certificate, delivery)
                    .await
            }

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => {
                simple_client
                    .handle_lite_certificate(certificate, delivery)
                    .await
            }
        }
    }

    async fn handle_timeout_certificate(
        &self,
        certificate: TimeoutCertificate,
    ) -> Result<ChainInfoResponse, NodeError> {
        match self {
            Client::Grpc(grpc_client) => grpc_client.handle_timeout_certificate(certificate).await,

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => {
                simple_client.handle_timeout_certificate(certificate).await
            }
        }
    }

    async fn handle_confirmed_certificate(
        &self,
        certificate: ConfirmedBlockCertificate,
        delivery: CrossChainMessageDelivery,
    ) -> Result<ChainInfoResponse, NodeError> {
        match self {
            Client::Grpc(grpc_client) => {
                grpc_client
                    .handle_confirmed_certificate(certificate, delivery)
                    .await
            }

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => {
                simple_client
                    .handle_confirmed_certificate(certificate, delivery)
                    .await
            }
        }
    }

    async fn handle_validated_certificate(
        &self,
        certificate: ValidatedBlockCertificate,
    ) -> Result<ChainInfoResponse, NodeError> {
        match self {
            Client::Grpc(grpc_client) => {
                grpc_client.handle_validated_certificate(certificate).await
            }

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => {
                simple_client
                    .handle_validated_certificate(certificate)
                    .await
            }
        }
    }

    async fn handle_chain_info_query(
        &self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, NodeError> {
        match self {
            Client::Grpc(grpc_client) => grpc_client.handle_chain_info_query(query).await,

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => simple_client.handle_chain_info_query(query).await,
        }
    }

    async fn subscribe(&self, chains: Vec<ChainId>) -> Result<Self::NotificationStream, NodeError> {
        Ok(match self {
            Client::Grpc(grpc_client) => Box::pin(grpc_client.subscribe(chains).await?),

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => Box::pin(simple_client.subscribe(chains).await?),
        })
    }

    async fn get_version_info(&self) -> Result<linera_version::VersionInfo, NodeError> {
        Ok(match self {
            Client::Grpc(grpc_client) => grpc_client.get_version_info().await?,

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => simple_client.get_version_info().await?,
        })
    }

    async fn get_genesis_config_hash(&self) -> Result<CryptoHash, NodeError> {
        Ok(match self {
            Client::Grpc(grpc_client) => grpc_client.get_genesis_config_hash().await?,

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => simple_client.get_genesis_config_hash().await?,
        })
    }

    async fn upload_blob(&self, content: BlobContent) -> Result<BlobId, NodeError> {
        Ok(match self {
            Client::Grpc(grpc_client) => grpc_client.upload_blob(content).await?,

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => simple_client.upload_blob(content).await?,
        })
    }

    async fn download_blob(&self, blob_id: BlobId) -> Result<BlobContent, NodeError> {
        Ok(match self {
            Client::Grpc(grpc_client) => grpc_client.download_blob(blob_id).await?,

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => simple_client.download_blob(blob_id).await?,
        })
    }

    async fn download_pending_blob(
        &self,
        chain_id: ChainId,
        blob_id: BlobId,
    ) -> Result<BlobContent, NodeError> {
        Ok(match self {
            Client::Grpc(grpc_client) => {
                grpc_client.download_pending_blob(chain_id, blob_id).await?
            }

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => {
                simple_client
                    .download_pending_blob(chain_id, blob_id)
                    .await?
            }
        })
    }

    async fn handle_pending_blob(
        &self,
        chain_id: ChainId,
        blob: BlobContent,
    ) -> Result<ChainInfoResponse, NodeError> {
        Ok(match self {
            Client::Grpc(grpc_client) => grpc_client.handle_pending_blob(chain_id, blob).await?,

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => {
                simple_client.handle_pending_blob(chain_id, blob).await?
            }
        })
    }

    async fn download_certificate(
        &self,
        hash: CryptoHash,
    ) -> Result<ConfirmedBlockCertificate, NodeError> {
        Ok(match self {
            Client::Grpc(grpc_client) => grpc_client.download_certificate(hash).await?,

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => simple_client.download_certificate(hash).await?,
        })
    }

    async fn download_certificates(
        &self,
        hashes: Vec<CryptoHash>,
    ) -> Result<Vec<ConfirmedBlockCertificate>, NodeError> {
        Ok(match self {
            Client::Grpc(grpc_client) => grpc_client.download_certificates(hashes).await?,

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => simple_client.download_certificates(hashes).await?,
        })
    }

    async fn blob_last_used_by(&self, blob_id: BlobId) -> Result<CryptoHash, NodeError> {
        Ok(match self {
            Client::Grpc(grpc_client) => grpc_client.blob_last_used_by(blob_id).await?,

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => simple_client.blob_last_used_by(blob_id).await?,
        })
    }

    async fn missing_blob_ids(&self, blob_ids: Vec<BlobId>) -> Result<Vec<BlobId>, NodeError> {
        Ok(match self {
            Client::Grpc(grpc_client) => grpc_client.missing_blob_ids(blob_ids).await?,

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => simple_client.missing_blob_ids(blob_ids).await?,
        })
    }
}

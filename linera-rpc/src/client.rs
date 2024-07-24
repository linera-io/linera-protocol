// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    crypto::CryptoHash,
    data_types::{Blob, HashedBlob},
    identifiers::{BlobId, ChainId},
};
use linera_chain::data_types::{
    BlockProposal, Certificate, HashedCertificateValue, LiteCertificate,
};
#[cfg(web)]
use linera_core::node::{
    LocalNotificationStream as NotificationStream, LocalValidatorNode as ValidatorNode,
};
#[cfg(not(web))]
use linera_core::node::{NotificationStream, ValidatorNode};
use linera_core::{
    data_types::{ChainInfoQuery, ChainInfoResponse},
    node::{CrossChainMessageDelivery, NodeError},
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

    async fn handle_certificate(
        &self,
        certificate: Certificate,
        hashed_certificate_values: Vec<HashedCertificateValue>,
        hashed_blobs: Vec<HashedBlob>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<ChainInfoResponse, NodeError> {
        match self {
            Client::Grpc(grpc_client) => {
                grpc_client
                    .handle_certificate(
                        certificate,
                        hashed_certificate_values,
                        hashed_blobs,
                        delivery,
                    )
                    .await
            }

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => {
                simple_client
                    .handle_certificate(
                        certificate,
                        hashed_certificate_values,
                        hashed_blobs,
                        delivery,
                    )
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

    async fn download_blob(&self, blob_id: BlobId) -> Result<Blob, NodeError> {
        Ok(match self {
            Client::Grpc(grpc_client) => grpc_client.download_blob(blob_id).await?,

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => simple_client.download_blob(blob_id).await?,
        })
    }

    async fn download_certificate_value(
        &self,
        hash: CryptoHash,
    ) -> Result<HashedCertificateValue, NodeError> {
        Ok(match self {
            Client::Grpc(grpc_client) => grpc_client.download_certificate_value(hash).await?,

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => simple_client.download_certificate_value(hash).await?,
        })
    }

    async fn download_certificate(&self, hash: CryptoHash) -> Result<Certificate, NodeError> {
        Ok(match self {
            Client::Grpc(grpc_client) => grpc_client.download_certificate(hash).await?,

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => simple_client.download_certificate(hash).await?,
        })
    }

    async fn blob_last_used_by(&self, blob_id: BlobId) -> Result<CryptoHash, NodeError> {
        Ok(match self {
            Client::Grpc(grpc_client) => grpc_client.blob_last_used_by(blob_id).await?,

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => simple_client.blob_last_used_by(blob_id).await?,
        })
    }
}

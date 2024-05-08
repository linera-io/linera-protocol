// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::identifiers::ChainId;
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
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, NodeError> {
        match self {
            Client::Grpc(grpc_client) => grpc_client.handle_block_proposal(proposal).await,

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => simple_client.handle_block_proposal(proposal).await,
        }
    }

    async fn handle_lite_certificate(
        &mut self,
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
        &mut self,
        certificate: Certificate,
        hashed_certificate_values: Vec<HashedCertificateValue>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<ChainInfoResponse, NodeError> {
        match self {
            Client::Grpc(grpc_client) => {
                grpc_client
                    .handle_certificate(certificate, hashed_certificate_values, delivery)
                    .await
            }

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => {
                simple_client
                    .handle_certificate(certificate, hashed_certificate_values, delivery)
                    .await
            }
        }
    }

    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, NodeError> {
        match self {
            Client::Grpc(grpc_client) => grpc_client.handle_chain_info_query(query).await,

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => simple_client.handle_chain_info_query(query).await,
        }
    }

    async fn subscribe(
        &mut self,
        chains: Vec<ChainId>,
    ) -> Result<Self::NotificationStream, NodeError> {
        Ok(match self {
            Client::Grpc(grpc_client) => Box::pin(grpc_client.subscribe(chains).await?),

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => Box::pin(simple_client.subscribe(chains).await?),
        })
    }

    async fn get_version_info(&mut self) -> Result<linera_version::VersionInfo, NodeError> {
        Ok(match self {
            Client::Grpc(grpc_client) => grpc_client.get_version_info().await?,

            #[cfg(with_simple_network)]
            Client::Simple(simple_client) => simple_client.get_version_info().await?,
        })
    }
}

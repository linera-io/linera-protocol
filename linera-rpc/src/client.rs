// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;

use linera_base::data_types::ChainId;
use linera_chain::data_types::{BlockProposal, Certificate};
use linera_core::{
    data_types::{ChainInfoQuery, ChainInfoResponse},
    node::{NodeError, ValidatorNode},
    worker::Notification,
};

use crate::{grpc_network::GrpcClient, simple_network::SimpleClient};

#[derive(Clone)]
pub enum Client {
    Grpc(GrpcClient),
    Simple(SimpleClient),
}

impl From<GrpcClient> for Client {
    fn from(client: GrpcClient) -> Self {
        Self::Grpc(client)
    }
}

impl From<SimpleClient> for Client {
    fn from(client: SimpleClient) -> Self {
        Self::Simple(client)
    }
}

#[async_trait]
impl ValidatorNode for Client {
    type NotificationStream = Pin<Box<dyn Stream<Item = Notification>>>;

    async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, NodeError> {
        match self {
            Client::Grpc(grpc_client) => grpc_client.handle_block_proposal(proposal).await,
            Client::Simple(simple_client) => simple_client.handle_block_proposal(proposal).await,
        }
    }

    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<ChainInfoResponse, NodeError> {
        match self {
            Client::Grpc(grpc_client) => grpc_client.handle_certificate(certificate).await,
            Client::Simple(simple_client) => simple_client.handle_certificate(certificate).await,
        }
    }

    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, NodeError> {
        match self {
            Client::Grpc(grpc_client) => grpc_client.handle_chain_info_query(query).await,
            Client::Simple(simple_client) => simple_client.handle_chain_info_query(query).await,
        }
    }

    async fn subscribe(
        &mut self,
        chains: Vec<ChainId>,
    ) -> Result<Self::NotificationStream, NodeError> {
        Ok(match self {
            Client::Grpc(grpc_client) => Box::pin(grpc_client.subscribe(chains).await?),
            Client::Simple(simple_client) => Box::pin(simple_client.subscribe(chains).await?),
        })
    }
}

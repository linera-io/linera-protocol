use crate::{
    client_delegate, codec,
    config::{ValidatorPublicNetworkConfig, ValidatorPublicNetworkPreConfig},
    grpc_network::{
        grpc::{chain_info_result::Inner, validator_node_client::ValidatorNodeClient},
        GrpcError,
    },
    transport::TransportProtocol,
    Message,
};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use linera_chain::messages::{BlockProposal, Certificate};
use linera_core::{
    messages::{ChainInfoQuery, ChainInfoResponse},
    node::{NodeError, ValidatorNode},
};
use std::time::Duration;
use tokio::time;
use tonic::{transport::Channel, Request};

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
}

#[derive(Clone)]
pub struct SimpleClient {
    network: ValidatorPublicNetworkPreConfig<TransportProtocol>,
    send_timeout: Duration,
    recv_timeout: Duration,
}

impl SimpleClient {
    pub(crate) fn new(
        network: ValidatorPublicNetworkPreConfig<TransportProtocol>,
        send_timeout: Duration,
        recv_timeout: Duration,
    ) -> Self {
        Self {
            network,
            send_timeout,
            recv_timeout,
        }
    }

    async fn send_recv_internal(&mut self, message: Message) -> Result<Message, codec::Error> {
        let address = format!("{}:{}", self.network.host, self.network.port);
        let mut stream = self.network.protocol.connect(address).await?;
        // Send message
        time::timeout(self.send_timeout, stream.send(message))
            .await
            .map_err(|timeout| codec::Error::Io(timeout.into()))??;
        // Wait for reply
        time::timeout(self.recv_timeout, stream.next())
            .await
            .map_err(|timeout| codec::Error::Io(timeout.into()))?
            .transpose()?
            .ok_or_else(|| codec::Error::Io(std::io::ErrorKind::UnexpectedEof.into()))
    }

    async fn send_recv_info(&mut self, message: Message) -> Result<ChainInfoResponse, NodeError> {
        match self.send_recv_internal(message).await {
            Ok(Message::ChainInfoResponse(response)) => Ok(*response),
            Ok(Message::Error(error)) => Err(*error),
            Ok(_) => Err(NodeError::UnexpectedMessage),
            Err(error) => match error {
                codec::Error::Io(io_error) => Err(NodeError::ClientIoError {
                    error: format!("{}", io_error),
                }),
                _ => Err(NodeError::InvalidDecoding),
            },
        }
    }
}

#[async_trait]
impl ValidatorNode for SimpleClient {
    /// Initiate a new block.
    async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.send_recv_info(proposal.into()).await
    }

    /// Process a certificate.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.send_recv_info(certificate.into()).await
    }

    /// Handle information queries for this chain.
    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.send_recv_info(query.into()).await
    }
}

#[derive(Clone)]
pub struct GrpcClient(ValidatorNodeClient<Channel>);

impl GrpcClient {
    pub(crate) async fn new(network: ValidatorPublicNetworkConfig) -> Result<Self, GrpcError> {
        Ok(Self(ValidatorNodeClient::connect(network.address()).await?))
    }
}

#[async_trait]
impl ValidatorNode for GrpcClient {
    async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, NodeError> {
        client_delegate!(self, handle_block_proposal, proposal)
    }

    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<ChainInfoResponse, NodeError> {
        client_delegate!(self, handle_certificate, certificate)
    }

    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, NodeError> {
        client_delegate!(self, handle_chain_info_query, query)
    }
}

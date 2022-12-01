use crate::{
    client::Client,
    config::{ValidatorPublicNetworkConfig, ValidatorPublicNetworkPreConfig},
    grpc_network::GrpcClient,
    simple_network::SimpleClient,
};
use async_trait::async_trait;
use linera_core::{client::ValidatorNodeProvider, node::NodeError};
use std::{str::FromStr, time::Duration};

/// A general node provider which delegates node provision to the underlying
/// node provider according to the `ValidatorPublicNetworkConfig`.
pub struct NodeProvider {
    grpc: GrpcNodeProvider,
    simple: SimpleNodeProvider,
}

impl NodeProvider {
    pub fn new(grpc: GrpcNodeProvider, simple: SimpleNodeProvider) -> Self {
        Self { grpc, simple }
    }
}

#[async_trait]
impl ValidatorNodeProvider for NodeProvider {
    type Node = Client;

    async fn make_node(&self, address: &str) -> anyhow::Result<Self::Node, NodeError> {
        let client = match &address.to_lowercase() {
            address if address.starts_with("tcp") || address.starts_with("upd") => {
                Client::Simple(self.simple.make_node(address).await?)
            }
            address if address.starts_with("grpc") => {
                Client::Grpc(self.grpc.make_node(address).await?)
            }
            _ => {
                return Err(NodeError::CannotResolveValidatorAddress {
                    address: address.to_string(),
                })
            }
        };

        Ok(client)
    }
}

pub struct GrpcNodeProvider {}

#[async_trait]
impl ValidatorNodeProvider for GrpcNodeProvider {
    type Node = GrpcClient;

    async fn make_node(&self, address: &str) -> anyhow::Result<Self::Node, NodeError> {
        let network = ValidatorPublicNetworkConfig::from_str(address).map_err(|_| {
            NodeError::CannotResolveValidatorAddress {
                address: address.to_string(),
            }
        })?;

        let client = GrpcClient::new(network)
            .await
            .map_err(|e| NodeError::GrpcError {
                error: format!(
                    "could not initialise gRPC client for address {} with error: {}",
                    address, e
                ),
            })?;

        Ok(client)
    }
}

/// A client without an address - serves as a client factory.
pub struct SimpleNodeProvider {
    send_timeout: Duration,
    recv_timeout: Duration,
}

impl SimpleNodeProvider {
    pub fn new(send_timeout: Duration, recv_timeout: Duration) -> Self {
        Self {
            send_timeout,
            recv_timeout,
        }
    }
}

#[async_trait]
impl ValidatorNodeProvider for SimpleNodeProvider {
    type Node = SimpleClient;

    async fn make_node(&self, address: &str) -> Result<Self::Node, NodeError> {
        let network = ValidatorPublicNetworkPreConfig::from_str(address).map_err(|_| {
            NodeError::CannotResolveValidatorAddress {
                address: address.to_string(),
            }
        })?;

        let client = SimpleClient::new(network, self.send_timeout, self.recv_timeout);

        Ok(client)
    }
}

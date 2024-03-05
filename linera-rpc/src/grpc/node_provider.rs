use super::Client;

use crate::{config::ValidatorPublicNetworkConfig, node_provider::NodeOptions};

use linera_core::node::{NodeError, ValidatorNodeProvider};

use std::str::FromStr as _;

#[derive(Copy, Clone)]
pub struct NodeProvider(NodeOptions);

impl NodeProvider {
    pub fn new(options: NodeOptions) -> Self {
        Self(options)
    }
}

impl ValidatorNodeProvider for NodeProvider {
    type Node = Client;

    fn make_node(&self, address: &str) -> anyhow::Result<Self::Node, NodeError> {
        let network = ValidatorPublicNetworkConfig::from_str(address).map_err(|_| {
            NodeError::CannotResolveValidatorAddress {
                address: address.to_string(),
            }
        })?;

        let client = Client::new(network, self.0).map_err(|e| NodeError::GrpcError {
            error: format!(
                "could not initialize gRPC client for address {} : {}",
                address, e
            ),
        })?;

        Ok(client)
    }
}

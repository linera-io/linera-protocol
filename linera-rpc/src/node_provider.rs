// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    client::Client,
    config::{ValidatorPublicNetworkConfig, ValidatorPublicNetworkPreConfig},
    grpc_network::GrpcClient,
    simple_network::SimpleClient,
};
use linera_core::{client::ValidatorNodeProvider, node::NodeError};
use std::{str::FromStr, time::Duration};

/// A general node provider which delegates node provision to the underlying
/// node provider according to the `ValidatorPublicNetworkConfig`.
#[derive(Copy, Clone)]
pub struct NodeProvider {
    grpc: GrpcNodeProvider,
    simple: SimpleNodeProvider,
}

impl NodeProvider {
    pub fn new(options: NodeOptions) -> Self {
        let grpc = GrpcNodeProvider::new(options);
        let simple = SimpleNodeProvider::new(options);
        Self { grpc, simple }
    }
}

impl ValidatorNodeProvider for NodeProvider {
    type Node = Client;

    fn make_node(&self, address: &str) -> anyhow::Result<Self::Node, NodeError> {
        let client = match &address.to_lowercase() {
            address if address.starts_with("tcp") || address.starts_with("upd") => {
                Client::Simple(self.simple.make_node(address)?)
            }
            address if address.starts_with("grpc") => Client::Grpc(self.grpc.make_node(address)?),
            _ => {
                return Err(NodeError::CannotResolveValidatorAddress {
                    address: address.to_string(),
                })
            }
        };

        Ok(client)
    }
}

#[derive(Copy, Clone)]
pub struct NodeOptions {
    pub send_timeout: Duration,
    pub recv_timeout: Duration,
    pub notification_retry_delay: Duration,
    pub notification_retries: u32,
    pub wait_for_outgoing_messages: bool,
}

#[derive(Copy, Clone)]
pub struct GrpcNodeProvider(NodeOptions);

impl GrpcNodeProvider {
    pub fn new(options: NodeOptions) -> Self {
        Self(options)
    }
}

impl ValidatorNodeProvider for GrpcNodeProvider {
    type Node = GrpcClient;

    fn make_node(&self, address: &str) -> anyhow::Result<Self::Node, NodeError> {
        let network = ValidatorPublicNetworkConfig::from_str(address).map_err(|_| {
            NodeError::CannotResolveValidatorAddress {
                address: address.to_string(),
            }
        })?;

        let client = GrpcClient::new(network, self.0).map_err(|e| NodeError::GrpcError {
            error: format!(
                "could not initialize gRPC client for address {} : {}",
                address, e
            ),
        })?;
        Ok(client)
    }
}

/// A client without an address - serves as a client factory.
#[derive(Copy, Clone)]
pub struct SimpleNodeProvider(NodeOptions);

impl SimpleNodeProvider {
    pub fn new(options: NodeOptions) -> Self {
        Self(options)
    }
}

impl ValidatorNodeProvider for SimpleNodeProvider {
    type Node = SimpleClient;

    fn make_node(&self, address: &str) -> Result<Self::Node, NodeError> {
        let network = ValidatorPublicNetworkPreConfig::from_str(address).map_err(|_| {
            NodeError::CannotResolveValidatorAddress {
                address: address.to_string(),
            }
        })?;

        let client = SimpleClient::new(
            network,
            self.0.send_timeout,
            self.0.recv_timeout,
            self.0.wait_for_outgoing_messages,
        );

        Ok(client)
    }
}

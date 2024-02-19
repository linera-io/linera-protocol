// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{client::Client, grpc};
#[cfg(with_simple_network)]
use crate::simple;

use linera_core::node::{NodeError, ValidatorNodeProvider};

use std::time::Duration;

/// A general node provider which delegates node provision to the underlying
/// node provider according to the `ValidatorPublicNetworkConfig`.
#[derive(Copy, Clone)]
pub struct NodeProvider {
    grpc: grpc::NodeProvider,
    #[cfg(with_simple_network)]
    simple: simple::NodeProvider,
}

impl NodeProvider {
    pub fn new(options: NodeOptions) -> Self {
        Self {
            grpc: grpc::NodeProvider::new(options),
            #[cfg(with_simple_network)]
            simple: simple::NodeProvider::new(options),
        }
    }
}

impl ValidatorNodeProvider for NodeProvider {
    type Node = Client;

    fn make_node(&self, address: &str) -> anyhow::Result<Self::Node, NodeError> {
        let client = match &address.to_lowercase() {
            address if address.starts_with("tcp") || address.starts_with("udp") => {
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
}

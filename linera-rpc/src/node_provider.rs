// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use linera_core::node::{LocalValidatorNodeProvider, NodeError};

#[cfg(with_simple_network)]
use crate::simple::SimpleNodeProvider;
use crate::{client::Client, grpc::GrpcNodeProvider};

/// A general node provider which delegates node provision to the underlying
/// node provider according to the `ValidatorPublicNetworkConfig`.
#[derive(Copy, Clone)]
pub struct NodeProvider {
    grpc: GrpcNodeProvider,
    #[cfg(with_simple_network)]
    simple: SimpleNodeProvider,
}

impl NodeProvider {
    pub fn new(options: NodeOptions) -> Self {
        Self {
            grpc: GrpcNodeProvider::new(options),
            #[cfg(with_simple_network)]
            simple: SimpleNodeProvider::new(options),
        }
    }
}

impl LocalValidatorNodeProvider for NodeProvider {
    type Node = Client;

    fn make_node(&self, address: &str) -> anyhow::Result<Self::Node, NodeError> {
        let address = address.to_lowercase();

        #[cfg(with_simple_network)]
        if address.starts_with("tcp") || address.starts_with("udp") {
            return Ok(Client::Simple(self.simple.make_node(&address)?));
        }

        if address.starts_with("grpc") {
            return Ok(Client::Grpc(self.grpc.make_node(&address)?));
        }

        Err(NodeError::CannotResolveValidatorAddress { address })
    }
}

#[derive(Copy, Clone)]
pub struct NodeOptions {
    pub send_timeout: Duration,
    pub recv_timeout: Duration,
    pub retry_delay: Duration,
    pub max_retries: u32,
}

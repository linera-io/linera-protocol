// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::Client;

use crate::{config::ValidatorPublicNetworkPreConfig, node_provider::NodeOptions};

use linera_core::node::{NodeError, ValidatorNodeProvider};

use std::str::FromStr as _;

/// A client without an address - serves as a client factory.
#[derive(Copy, Clone)]
pub struct NodeProvider(NodeOptions);

impl NodeProvider {
    pub fn new(options: NodeOptions) -> Self {
        Self(options)
    }
}

impl ValidatorNodeProvider for NodeProvider {
    type Node = Client;

    fn make_node(&self, address: &str) -> Result<Self::Node, NodeError> {
        let network = ValidatorPublicNetworkPreConfig::from_str(address).map_err(|_| {
            NodeError::CannotResolveValidatorAddress {
                address: address.to_string(),
            }
        })?;

        let client = Client::new(network, self.0.send_timeout, self.0.recv_timeout);

        Ok(client)
    }
}

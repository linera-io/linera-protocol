// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::str::FromStr as _;

use linera_core::node::{NodeError, ValidatorNodeProvider};

use super::SimpleClient;
use crate::{config::ValidatorPublicNetworkPreConfig, node_provider::NodeOptions};

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

        let client = SimpleClient::new(network, self.0.send_timeout, self.0.recv_timeout);

        Ok(client)
    }
}

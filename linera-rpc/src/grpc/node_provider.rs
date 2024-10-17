// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::str::FromStr as _;

use linera_core::node::{NodeError, ValidatorNodeProvider};

use super::GrpcClient;
use crate::{config::ValidatorPublicNetworkConfig, node_provider::NodeOptions};

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

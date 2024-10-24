// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::str::FromStr as _;

use linera_core::node::{NodeError, ValidatorNodeProvider};

use super::GrpcClient;
use crate::{config::ValidatorPublicNetworkConfig, grpc::pool::GrpcConnectionPool, node_provider::NodeOptions};

#[derive(Clone)]
pub struct GrpcNodeProvider {
    pool: GrpcConnectionPool,
    options: NodeOptions,
}

impl GrpcNodeProvider {
    pub fn new(options: NodeOptions) -> Self {
        let pool = GrpcConnectionPool::default();
        Self{ pool, options }
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

        let client = GrpcClient::new(network, self.options).map_err(|e| NodeError::GrpcError {
            error: format!(
                "could not initialize gRPC client for address {} : {}",
                address, e
            ),
        })?;

        Ok(client)
    }
}

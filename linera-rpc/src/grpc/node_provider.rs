// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{str::FromStr as _, sync::Arc};

use linera_base::time::Instant;
use linera_core::node::{NodeError, ValidatorNodeProvider};

use super::GrpcClient;
use crate::{
    config::ValidatorPublicNetworkConfig,
    grpc::{pool::GrpcConnectionPool, transport},
    node_provider::NodeOptions,
};

/// A node provider that creates gRPC clients backed by a shared connection pool.
#[derive(Clone)]
pub struct GrpcNodeProvider {
    pool: GrpcConnectionPool,
    options: NodeOptions,
    /// Shared across all `GrpcClient` instances. When a subscription to a validator
    /// fails, the failure time is recorded here so that other chains (which share the
    /// same provider) skip retrying the same dead validator.
    subscription_cooldowns: Arc<papaya::HashMap<String, Instant>>,
}

impl GrpcNodeProvider {
    /// Creates a new [`GrpcNodeProvider`] with the given node options.
    pub fn new(options: NodeOptions) -> Self {
        let pool = GrpcConnectionPool::new(transport::Options::from(&options));
        Self {
            pool,
            options,
            subscription_cooldowns: Arc::new(papaya::HashMap::new()),
        }
    }
}

impl ValidatorNodeProvider for GrpcNodeProvider {
    type Node = GrpcClient;

    fn make_node(&self, address: &str) -> Result<Self::Node, NodeError> {
        let network = ValidatorPublicNetworkConfig::from_str(address).map_err(|_| {
            NodeError::CannotResolveValidatorAddress {
                address: address.to_string(),
            }
        })?;
        let http_address = network.http_address();
        let channel =
            self.pool
                .channel(http_address.clone())
                .map_err(|error| NodeError::GrpcError {
                    error: format!("error creating channel: {error}"),
                })?;

        Ok(GrpcClient::new(
            http_address,
            channel,
            self.options,
            self.subscription_cooldowns.clone(),
        ))
    }
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::time::Duration;
use linera_core::node::{NodeError, ValidatorNodeProvider};

use super::GrpcClient;
use crate::{
    grpc::{pool::GrpcConnectionPool, transport::Options},
    node_provider::NodeOptions,
};

#[derive(Clone)]
pub struct GrpcNodeProvider {
    pool: GrpcConnectionPool,
    retry_delay: Duration,
    max_retries: u32,
}

impl GrpcNodeProvider {
    pub fn new(options: NodeOptions) -> Self {
        let transport_options = Options::from(&options);
        let retry_delay = options.retry_delay;
        let max_retries = options.max_retries;
        let pool = GrpcConnectionPool::new(transport_options);
        Self {
            pool,
            retry_delay,
            max_retries,
        }
    }
}

impl ValidatorNodeProvider for GrpcNodeProvider {
    type Node = GrpcClient;

    fn make_node(&self, address: &str) -> Result<Self::Node, NodeError> {
        let address = address.to_string();
        let channel = self
            .pool
            .channel(address.clone())
            .map_err(|error| NodeError::GrpcError {
                error: format!("error creating channel: {}", error),
            })?;

        Ok(GrpcClient::new(
            address.to_string(),
            channel,
            self.retry_delay,
            self.max_retries,
        ))
    }
}

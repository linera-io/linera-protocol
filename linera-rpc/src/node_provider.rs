// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use linera_base::time::Duration;
use linera_core::node::{NodeError, ValidatorNodeProvider};

#[cfg(with_simple_network)]
use crate::simple::SimpleNodeProvider;
use crate::{client::Client, grpc::GrpcNodeProvider};

/// A general node provider which delegates node provision to the underlying
/// node provider according to the `ValidatorPublicNetworkConfig`.
#[derive(Clone)]
pub struct NodeProvider {
    grpc: GrpcNodeProvider,
    #[cfg(with_simple_network)]
    simple: SimpleNodeProvider,
    client_pool: Arc<Mutex<HashMap<String, Client>>>,
}

impl NodeProvider {
    pub fn new(options: NodeOptions) -> Self {
        Self {
            grpc: GrpcNodeProvider::new(options),
            #[cfg(with_simple_network)]
            simple: SimpleNodeProvider::new(options),
            client_pool: Default::default(),
        }
    }
}

impl ValidatorNodeProvider for NodeProvider {
    type Node = Client;

    fn make_node(&self, address: &str) -> anyhow::Result<Self::Node, NodeError> {
        let address = address.to_lowercase();
        let mut pool = self.client_pool.lock().unwrap();
        if let Some(client) = pool.get(&address) {
            return Ok(client.clone());
        }

        #[cfg(with_simple_network)]
        if address.starts_with("tcp") || address.starts_with("udp") {
            let client = Client::Simple(self.simple.make_node(&address)?);
            pool.insert(address, client.clone());
            return Ok(client);
        }

        if address.starts_with("grpc") {
            let client = Client::Grpc(self.grpc.make_node(&address)?);
            pool.insert(address, client.clone());
            return Ok(client);
        }

        Err(NodeError::CannotResolveValidatorAddress { address })
    }
}

#[derive(Copy, Clone)]
pub struct NodeOptions {
    pub send_timeout: Duration,
    pub recv_timeout: Duration,
    pub notification_retry_delay: Duration,
    pub notification_retries: u32,
}

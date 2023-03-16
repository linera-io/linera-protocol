// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::grpc_network::GrpcError;
use dashmap::DashMap;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

/// A pool of transport channels to be used by Grpc.
#[derive(Clone, Default)]
pub struct ConnectionPool {
    connect_timeout: Option<Duration>,
    timeout: Option<Duration>,
    channels: DashMap<String, Channel>,
}

impl ConnectionPool {
    pub fn with_connect_timeout(mut self, connect_timeout: impl Into<Option<Duration>>) -> Self {
        self.connect_timeout = connect_timeout.into();
        self
    }

    pub fn with_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.timeout = timeout.into();
        self
    }

    /// Obtains a channel for the current address. Either clones an existing one (thereby
    /// reusing the connection), or creates one if needed. New channels do not create a
    /// connection immediately.
    pub fn channel(&self, address: String) -> Result<Channel, GrpcError> {
        let channel = self
            .channels
            .entry(address.clone())
            .or_try_insert_with(|| {
                let mut endpoint = Endpoint::from_shared(address)?;
                if let Some(timeout) = self.connect_timeout {
                    endpoint = endpoint.connect_timeout(timeout);
                }
                if let Some(timeout) = self.timeout {
                    endpoint = endpoint.timeout(timeout);
                }
                Ok::<_, GrpcError>(endpoint.connect_lazy())
            })?;
        Ok(channel.clone())
    }
}

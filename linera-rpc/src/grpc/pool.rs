// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::time::Duration;

use super::{transport, GrpcError};

/// A pool of transport channels to be used by gRPC.
#[derive(Clone, Default)]
pub struct GrpcConnectionPool {
    options: transport::Options,
    channels: papaya::HashMap<String, transport::Channel>,
}

impl GrpcConnectionPool {
    pub fn new(options: transport::Options) -> Self {
        Self {
            options,
            channels: papaya::HashMap::default(),
        }
    }

    pub fn with_connect_timeout(mut self, connect_timeout: impl Into<Option<Duration>>) -> Self {
        self.options.connect_timeout = connect_timeout.into();
        self
    }

    pub fn with_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.options.timeout = timeout.into();
        self
    }

    /// Obtains a channel for the current address. Either clones an existing one (thereby
    /// reusing the connection), or creates one if needed. New channels do not create a
    /// connection immediately and will automatically reconnect when needed.
    pub fn channel(&self, address: String) -> Result<transport::Channel, GrpcError> {
        let pinned = self.channels.pin();
        if let Some(channel) = pinned.get(&address) {
            return Ok(channel.clone());
        }
        let channel = transport::create_channel(address.clone(), &self.options)?;
        Ok(pinned.get_or_insert(address, channel).clone())
    }

    /// Removes a channel from the pool, forcing a new connection to be created on the next request.
    /// This should be called when a channel is known to be broken (e.g., received GOAWAY).
    pub fn invalidate_channel(&self, address: &str) {
        self.channels.pin().remove(address);
    }
}

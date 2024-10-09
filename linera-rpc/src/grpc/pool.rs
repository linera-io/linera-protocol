// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;
use linera_base::time::Duration;

use super::{transport, GrpcError};

/// A pool of transport channels to be used by gRPC.
#[derive(Clone, Default)]
pub struct GrpcConnectionPool {
    options: transport::Options,
    channels: DashMap<String, transport::Channel>,
}

impl GrpcConnectionPool {
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
    /// connection immediately.
    pub fn channel(&self, address: String) -> Result<transport::Channel, GrpcError> {
        Ok(self
            .channels
            .entry(address.clone())
            .or_try_insert_with(|| transport::create_channel(address, &self.options))?
            .clone())
    }
}

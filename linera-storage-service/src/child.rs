// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::client::storage_service_check_endpoint;
use anyhow::Result;
use linera_service::util::CommandExt;
use std::time::Duration;
use tokio::process::{Child, Command};

/// Configuration for a storage service running as a child process
pub struct StorageServiceSpanner {
    endpoint: String,
    binary: String,
}

/// A storage service running as a child process.
///
/// The guard preserves the child from destruction and destroys it when
/// it drops out of scope.
pub struct StorageServiceGuard {
    _child: Child,
}

impl StorageServiceSpanner {
    /// Creates a new `StorageServiceChild`
    pub fn new(endpoint: String, binary: String) -> Self {
        Self { endpoint, binary }
    }

    async fn command(&self) -> Result<Command> {
        let mut command = Command::new(&self.binary);
        command.args(["memory", "--endpoint", &self.endpoint]);
        command.kill_on_drop(true);
        Ok(command)
    }
    pub async fn run_service(&self) -> Result<StorageServiceGuard> {
        let mut command = self.command().await?;
        let _child = command.spawn_into()?;
        let guard = StorageServiceGuard { _child };
        // We iterate until the child is spanned and can be accessed.
        // We add an additional waiting period to avoid problems.
        let mut wait_period = 1;
        loop {
            let result = storage_service_check_endpoint(self.endpoint.clone()).await;
            if result.is_ok() {
                return Ok(guard);
            }
            tokio::time::sleep(Duration::from_secs(wait_period)).await;
            wait_period += 1;
        }
    }
}

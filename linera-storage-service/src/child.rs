// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::client::storage_service_check_endpoint;
use anyhow::{bail, Result};
use linera_base::command::CommandExt;
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
    pub fn new(endpoint: &str, binary: String) -> Self {
        Self {
            endpoint: endpoint.to_string(),
            binary,
        }
    }

    async fn command(&self) -> Command {
        let mut command = Command::new(&self.binary);
        command.args(["memory", "--endpoint", &self.endpoint]);
        command.kill_on_drop(true);
        command
    }
    pub async fn run_service(&self) -> Result<StorageServiceGuard> {
        let mut command = self.command().await;
        let _child = command.spawn_into()?;
        let guard = StorageServiceGuard { _child };
        // We iterate until the child is spanned and can be accessed.
        // We add an additional waiting period to avoid problems.
        for i in 1..10 {
            let result = storage_service_check_endpoint(&self.endpoint).await;
            if result.is_ok() {
                return Ok(guard);
            }
            tokio::time::sleep(Duration::from_secs(i)).await;
        }
        bail!("Failed to start child server");
    }
}

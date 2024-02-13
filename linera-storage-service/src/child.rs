// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use linera_service::util::CommandExt;
use std::time::Duration;
use tokio::process::{Child, Command};

/// Configuration for a storage service running as a child process
pub struct StorageServiceChild {
    endpoint: String,
    binary: String,
}

/// The tests being done have to run in parallel.
/// For that we spanned a storage-service per test.
/// In order to avoid collision, we use a different port per test.
///
/// A storage service running as a child process.
///
/// The guard preserves the child from destruction and destroys it when
/// it drops out of scope.
pub struct ChildGuard {
    _child: Child,
}

impl StorageServiceChild {
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

    pub async fn run_service(&self) -> Result<ChildGuard> {
        let mut command = self.command().await?;
        let _child = command.spawn_into()?;
        let guard = ChildGuard { _child };
        tokio::time::sleep(Duration::from_secs(5)).await;
        Ok(guard)
    }
}

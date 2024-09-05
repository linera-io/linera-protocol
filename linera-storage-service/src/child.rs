// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{bail, Result};
use linera_base::{command::CommandExt, time::Duration};
use port_selector::random_free_tcp_port;
use tokio::process::{Child, Command};

use crate::client::{storage_service_check_absence, storage_service_check_validity};

pub async fn get_free_port() -> Result<u16> {
    for i in 1..10 {
        let port = random_free_tcp_port();
        if let Some(port) = port {
            return Ok(port);
        }
        linera_base::time::timer::sleep(Duration::from_secs(i)).await;
    }
    bail!("Failed to obtain a port");
}

pub async fn get_free_endpoint() -> Result<String> {
    let port = get_free_port().await?;
    Ok(format!("127.0.0.1:{}", port))
}

/// Configuration for a storage service running as a child process
pub struct StorageService {
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

impl StorageService {
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

    /// Waits for the absence of the endpoint. If a child is terminated
    /// then it might take time to wait for its absence.
    async fn wait_for_absence(&self) -> Result<()> {
        for i in 1..10 {
            if storage_service_check_absence(&self.endpoint).await? {
                return Ok(());
            }
            linera_base::time::timer::sleep(Duration::from_secs(i)).await;
        }
        bail!("Failed to start child server");
    }

    pub async fn run(&self) -> Result<StorageServiceGuard> {
        self.wait_for_absence().await?;
        let mut command = self.command().await;
        let _child = command.spawn_into()?;
        let guard = StorageServiceGuard { _child };
        // We iterate until the child is spanned and can be accessed.
        // We add an additional waiting period to avoid problems.
        for i in 1..10 {
            let result = storage_service_check_validity(&self.endpoint).await;
            if result.is_ok() {
                return Ok(guard);
            }
            linera_base::time::timer::sleep(Duration::from_secs(i)).await;
        }
        bail!("Failed to start child server");
    }
}

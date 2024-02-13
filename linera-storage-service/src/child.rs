// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use linera_service::util::CommandExt;
use std::time::Duration;
use tokio::{
    process::{Child, Command},
    sync::{Semaphore, SemaphorePermit},
};

/// Configuration for a storage service running as a child process
pub struct StorageServiceChild {
    endpoint: String,
    binary: String,
}

/// The stores created by the `create_shared_test_store`
/// are all pointing to the same storage.
/// This is in contrast to other storage that are not
/// persistent (e.g. memory) or uses a random table name
/// (e.g. RocksDB, DynamoDB, ScyllaDB)
/// This requires two changes:
/// * Only one test being run at a time with a semaphore.
/// * After the test, the storage is cleaned
static SHARED_STORE_SEMAPHORE: Semaphore = Semaphore::const_new(1);

/// A storage service running as a child process.
///
/// The guard serves two purposes:
/// * It protects the child from destruction and ends it on drop
/// * It make sure that only one server is ever created.
pub struct ChildGuard<'a> {
    _child: Child,
    _lock: SemaphorePermit<'a>,
}

impl<'a> StorageServiceChild {
    /// The constructor of the storage service child
    pub fn new(endpoint: String, binary: String) -> Self {
        Self { endpoint, binary }
    }

    async fn command(&self) -> Result<Command> {
        let mut command = Command::new(&self.binary);
        command.args(["memory", "--endpoint", &self.endpoint]);
        command.kill_on_drop(true);
        Ok(command)
    }

    pub async fn run_service(&self) -> Result<ChildGuard<'a>> {
        let _lock: SemaphorePermit<'a> = SHARED_STORE_SEMAPHORE.acquire().await?;
        let mut command = self.command().await?;
        let _child = command.spawn_into()?;
        let guard = ChildGuard { _child, _lock };
        tokio::time::sleep(Duration::from_secs(10)).await;
        Ok(guard)
    }
}

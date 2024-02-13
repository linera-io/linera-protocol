// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use linera_service::util::CommandExt;
use tokio::{
    process::{Child, Command},
    sync::{Semaphore, SemaphorePermit},
};

/// The storage service spanner
pub struct StorageServiceChild {
    endpoint: String,
    binary: String,
}

/// The stores created by the `create_shared_test_store`
/// are all pointing to the same storage.
/// This is in contrast to other storage that are not
/// persistent (e.g. memory) or uses a random table_name
/// (e.g. RocksDb, DynamoDb, ScyllaDb)
/// This requires two changes:
/// * Only one test being run at a time with a semaphore.
/// * After the test, the storage is cleaned
static SHARED_STORE_SEMAPHORE: Semaphore = Semaphore::const_new(1);

/// The guard implements two things:
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
        println!("Child : command endpoint = {}", self.endpoint);
        let mut command = Command::new(&self.binary);
        command.args(["memory", "--endpoint", &self.endpoint]);
        Ok(command)
    }

    pub async fn run_service(&self) -> Result<ChildGuard<'a>> {
        println!("Child : run_service, begin");
        let _lock: SemaphorePermit<'a> = SHARED_STORE_SEMAPHORE.acquire().await?;
        let mut command = self.command().await?;
        println!("Child : run_service, we have command");
        println!("command={:?}", command);
        let _child = command.spawn_into()?;
        println!("Child : run_service, we have child");
        let guard = ChildGuard { _child, _lock };
        Ok(guard)
    }
}

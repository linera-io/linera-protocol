// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::db_storage::{DbStorage, DbStorageInner, WallClock};
use linera_execution::{ExecutionRuntimeConfig, WasmRuntime};
use linera_views::{
    common::TableStatus,
    rocks_db::{RocksDbContextError, RocksDbStore, RocksDbStoreConfig},
};
use std::sync::Arc;

#[cfg(any(test, feature = "test"))]
use {
    crate::db_storage::TestClock, linera_views::rocks_db::create_rocks_db_common_config,
    tempfile::TempDir, test_utils::get_namespace,
};

#[cfg(test)]
#[path = "unit_tests/rocks_db.rs"]
mod tests;

type RocksDbStorageInner = DbStorageInner<RocksDbStore>;

impl RocksDbStorageInner {
    #[cfg(any(test, feature = "test"))]
    pub async fn new_for_testing(
        store_config: RocksDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), RocksDbContextError> {
        let (client, table_status) = RocksDbStore::new_for_testing(store_config, namespace).await?;
        let storage = Self::new(client, wasm_runtime);
        Ok((storage, table_status))
    }

    async fn initialize(
        store_config: RocksDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, RocksDbContextError> {
        let store = RocksDbStore::initialize(store_config, namespace).await?;
        let storage = Self::new(store, wasm_runtime);
        Ok(storage)
    }

    async fn make(
        store_config: RocksDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), RocksDbContextError> {
        let (client, table_status) = RocksDbStore::new(store_config, namespace).await?;
        let storage = Self::new(client, wasm_runtime);
        Ok((storage, table_status))
    }
}

pub type RocksDbStorage<C> = DbStorage<RocksDbStore, C>;

#[cfg(any(test, feature = "test"))]
impl RocksDbStorage<TestClock> {
    pub async fn make_test_storage(wasm_runtime: Option<WasmRuntime>) -> (Self, TempDir) {
        let dir = TempDir::new().unwrap();
        let path_buf = dir.path().to_path_buf();
        let common_config = create_rocks_db_common_config();
        let store_config = RocksDbStoreConfig {
            path_buf,
            common_config,
        };
        let namespace = get_namespace();
        let (storage, _) = RocksDbStorage::new_for_testing(
            store_config,
            &namespace,
            wasm_runtime,
            TestClock::new(),
        )
        .await
        .expect("storage");
        (storage, dir)
    }

    pub async fn new_for_testing(
        store_config: RocksDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
        clock: TestClock,
    ) -> Result<(Self, TableStatus), RocksDbContextError> {
        let (storage, table_status) =
            RocksDbStorageInner::new_for_testing(store_config, namespace, wasm_runtime).await?;
        let storage = RocksDbStorage {
            client: Arc::new(storage),
            clock,
            execution_runtime_config: ExecutionRuntimeConfig::default(),
        };
        Ok((storage, table_status))
    }
}

impl RocksDbStorage<WallClock> {
    pub async fn initialize(
        store_config: RocksDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, RocksDbContextError> {
        let storage =
            RocksDbStorageInner::initialize(store_config, namespace, wasm_runtime).await?;
        let storage = RocksDbStorage {
            client: Arc::new(storage),
            clock: WallClock,
            execution_runtime_config: ExecutionRuntimeConfig::default(),
        };
        Ok(storage)
    }

    pub async fn new(
        store_config: RocksDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), RocksDbContextError> {
        let (storage, table_status) =
            RocksDbStorageInner::make(store_config, namespace, wasm_runtime).await?;
        let storage = RocksDbStorage {
            client: Arc::new(storage),
            clock: WallClock,
            execution_runtime_config: ExecutionRuntimeConfig::default(),
        };
        Ok((storage, table_status))
    }
}

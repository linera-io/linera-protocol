// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::db_storage::{DbStorage, DbStorageInner, WallClock};
use linera_execution::{ExecutionRuntimeConfig, WasmRuntime};
use linera_views::scylla_db::{ScyllaDbContextError, ScyllaDbStore, ScyllaDbStoreConfig};
use std::sync::Arc;
use linera_views::common::AdminKeyValueStore;

#[cfg(any(test, feature = "test"))]
use {
    crate::db_storage::TestClock, linera_views::scylla_db::create_scylla_db_common_config,
    linera_views::test_utils::get_namespace,
};

#[cfg(test)]
#[path = "unit_tests/scylla_db.rs"]
mod tests;

type ScyllaDbStorageInner = DbStorageInner<ScyllaDbStore>;

impl ScyllaDbStorageInner {
    #[cfg(any(test, feature = "test"))]
    pub async fn new_for_testing(
        store_config: ScyllaDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, ScyllaDbContextError> {
        let store = ScyllaDbStore::new_for_testing(store_config, namespace).await?;
        let storage = Self::new(store, wasm_runtime);
        Ok(storage)
    }

    async fn initialize(
        store_config: ScyllaDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, ScyllaDbContextError> {
        let store = ScyllaDbStore::initialize(&store_config, namespace).await?;
        let storage = Self::new(store, wasm_runtime);
        Ok(storage)
    }

    async fn make(
        store_config: ScyllaDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, ScyllaDbContextError> {
        let store = ScyllaDbStore::new(store_config, namespace).await?;
        let storage = Self::new(store, wasm_runtime);
        Ok(storage)
    }
}

pub type ScyllaDbStorage<C> = DbStorage<ScyllaDbStore, C>;

#[cfg(any(test, feature = "test"))]
impl ScyllaDbStorage<TestClock> {
    pub async fn make_test_storage(wasm_runtime: Option<WasmRuntime>) -> Self {
        let uri = "localhost:9042".to_string();
        let namespace = get_namespace();
        let common_config = create_scylla_db_common_config();
        let store_config = ScyllaDbStoreConfig { uri, common_config };
        ScyllaDbStorage::new_for_testing(store_config, &namespace, wasm_runtime, TestClock::new())
            .await
            .expect("client")
    }

    pub async fn new_for_testing(
        store_config: ScyllaDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
        clock: TestClock,
    ) -> Result<Self, ScyllaDbContextError> {
        let storage =
            ScyllaDbStorageInner::new_for_testing(store_config, namespace, wasm_runtime).await?;
        let storage = ScyllaDbStorage {
            client: Arc::new(storage),
            clock,
            execution_runtime_config: ExecutionRuntimeConfig::default(),
        };
        Ok(storage)
    }
}

impl ScyllaDbStorage<WallClock> {
    pub async fn initialize(
        store_config: ScyllaDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, ScyllaDbContextError> {
        let storage =
            ScyllaDbStorageInner::initialize(store_config, namespace, wasm_runtime).await?;
        let storage = ScyllaDbStorage {
            client: Arc::new(storage),
            clock: WallClock,
            execution_runtime_config: ExecutionRuntimeConfig::default(),
        };
        Ok(storage)
    }

    pub async fn new(
        store_config: ScyllaDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, ScyllaDbContextError> {
        let storage = ScyllaDbStorageInner::make(store_config, namespace, wasm_runtime).await?;
        let storage = ScyllaDbStorage {
            client: Arc::new(storage),
            clock: WallClock,
            execution_runtime_config: ExecutionRuntimeConfig::default(),
        };
        Ok(storage)
    }
}

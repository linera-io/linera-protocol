// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::db_storage::{DbStorage, DbStorageInner, WallClock};
use linera_execution::WasmRuntime;
use linera_views::{
    common::TableStatus,
    scylla_db::{ScyllaDbContextError, ScyllaDbStore, ScyllaDbStoreConfig},
};
use std::sync::Arc;

#[cfg(any(test, feature = "test"))]
use {
    crate::db_storage::TestClock, linera_views::scylla_db::create_scylla_db_common_config,
    linera_views::test_utils::get_table_name,
};

#[cfg(test)]
#[path = "unit_tests/scylla_db.rs"]
mod tests;

type ScyllaDbStorageInner = DbStorageInner<ScyllaDbStore>;

impl ScyllaDbStorageInner {
    #[cfg(any(test, feature = "test"))]
    pub async fn new_for_testing(
        store_config: ScyllaDbStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), ScyllaDbContextError> {
        let (store, table_status) = ScyllaDbStore::new_for_testing(store_config).await?;
        let storage = Self::new(store, wasm_runtime);
        Ok((storage, table_status))
    }

    async fn initialize(
        store_config: ScyllaDbStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, ScyllaDbContextError> {
        let store = ScyllaDbStore::initialize(store_config).await?;
        let storage = Self::new(store, wasm_runtime);
        Ok(storage)
    }

    async fn make(
        store_config: ScyllaDbStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), ScyllaDbContextError> {
        let (store, table_status) = ScyllaDbStore::new(store_config).await?;
        let storage = Self::new(store, wasm_runtime);
        Ok((storage, table_status))
    }
}

pub type ScyllaDbStorage<C> = DbStorage<ScyllaDbStore, C>;

#[cfg(any(test, feature = "test"))]
impl ScyllaDbStorage<TestClock> {
    pub async fn make_test_storage(wasm_runtime: Option<WasmRuntime>) -> Self {
        let uri = "localhost:9042".to_string();
        let table_name = get_table_name();
        let common_config = create_scylla_db_common_config();
        let store_config = ScyllaDbStoreConfig {
            uri,
            table_name,
            common_config,
        };
        let (client, _) =
            ScyllaDbStorage::new_for_testing(store_config, wasm_runtime, TestClock::new())
                .await
                .expect("client");
        client
    }

    pub async fn new_for_testing(
        store_config: ScyllaDbStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
        clock: TestClock,
    ) -> Result<(Self, TableStatus), ScyllaDbContextError> {
        let (storage, table_status) =
            ScyllaDbStorageInner::new_for_testing(store_config, wasm_runtime).await?;
        let storage = ScyllaDbStorage {
            client: Arc::new(storage),
            clock,
        };
        Ok((storage, table_status))
    }
}

impl ScyllaDbStorage<WallClock> {
    pub async fn initialize(
        store_config: ScyllaDbStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, ScyllaDbContextError> {
        let storage = ScyllaDbStorageInner::initialize(store_config, wasm_runtime).await?;
        let storage = ScyllaDbStorage {
            client: Arc::new(storage),
            clock: WallClock,
        };
        Ok(storage)
    }

    pub async fn new(
        store_config: ScyllaDbStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), ScyllaDbContextError> {
        let (storage, table_status) =
            ScyllaDbStorageInner::make(store_config, wasm_runtime).await?;
        let storage = ScyllaDbStorage {
            client: Arc::new(storage),
            clock: WallClock,
        };
        Ok((storage, table_status))
    }
}

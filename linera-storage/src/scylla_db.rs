// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::db_store::{DbStore, DbStoreInner, WallClock};
use linera_execution::WasmRuntime;
use linera_views::{
    common::TableStatus,
    scylla_db::{ScyllaDbClient, ScyllaDbContextError, ScyllaDbKvStoreConfig},
};
use std::sync::Arc;

#[cfg(any(test, feature = "test"))]
use {
    crate::db_store::TestClock, linera_views::scylla_db::create_scylla_db_common_config,
    linera_views::test_utils::get_table_name,
};

#[cfg(test)]
#[path = "unit_tests/scylla_db.rs"]
mod tests;

type ScyllaDbStoreInner = DbStoreInner<ScyllaDbClient>;

impl ScyllaDbStoreInner {
    #[cfg(any(test, feature = "test"))]
    pub async fn new_for_testing(
        store_config: ScyllaDbKvStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), ScyllaDbContextError> {
        let (client, table_status) = ScyllaDbClient::new_for_testing(store_config).await?;
        let store = Self::new(client, wasm_runtime);
        Ok((store, table_status))
    }

    async fn initialize(
        store_config: ScyllaDbKvStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, ScyllaDbContextError> {
        let client = ScyllaDbClient::initialize(store_config).await?;
        let store = Self::new(client, wasm_runtime);
        Ok(store)
    }

    async fn make(
        store_config: ScyllaDbKvStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), ScyllaDbContextError> {
        let (client, table_status) = ScyllaDbClient::new(store_config).await?;
        let store = Self::new(client, wasm_runtime);
        Ok((store, table_status))
    }
}

pub type ScyllaDbStore<C> = DbStore<ScyllaDbClient, C>;

#[cfg(any(test, feature = "test"))]
impl ScyllaDbStore<TestClock> {
    pub async fn make_test_store(wasm_runtime: Option<WasmRuntime>) -> Self {
        let uri = "localhost:9042".to_string();
        let table_name = get_table_name();
        let common_config = create_scylla_db_common_config();
        let store_config = ScyllaDbKvStoreConfig {
            uri,
            table_name,
            common_config,
        };
        let (client, _) =
            ScyllaDbStore::new_for_testing(store_config, wasm_runtime, TestClock::new())
                .await
                .expect("client");
        client
    }

    pub async fn new_for_testing(
        store_config: ScyllaDbKvStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
        clock: TestClock,
    ) -> Result<(Self, TableStatus), ScyllaDbContextError> {
        let (store, table_status) =
            ScyllaDbStoreInner::new_for_testing(store_config, wasm_runtime).await?;
        let store = ScyllaDbStore {
            client: Arc::new(store),
            clock,
        };
        Ok((store, table_status))
    }
}

impl ScyllaDbStore<WallClock> {
    pub async fn initialize(
        store_config: ScyllaDbKvStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, ScyllaDbContextError> {
        let store = ScyllaDbStoreInner::initialize(store_config, wasm_runtime).await?;
        let store = ScyllaDbStore {
            client: Arc::new(store),
            clock: WallClock,
        };
        Ok(store)
    }

    pub async fn new(
        store_config: ScyllaDbKvStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), ScyllaDbContextError> {
        let (store, table_status) = ScyllaDbStoreInner::make(store_config, wasm_runtime).await?;
        let store = ScyllaDbStore {
            client: Arc::new(store),
            clock: WallClock,
        };
        Ok((store, table_status))
    }
}

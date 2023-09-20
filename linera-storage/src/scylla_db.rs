// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain_guards::ChainGuards, DbStore, DbStoreClient, WallClock};
use linera_execution::WasmRuntime;
use linera_views::{
    common::TableStatus,
    scylla_db::{ScyllaDbClient, ScyllaDbContextError, ScyllaDbKvStoreConfig},
};
use std::sync::Arc;

#[cfg(any(test, feature = "test"))]
use {
    crate::TestClock, linera_views::common::get_table_name,
    linera_views::scylla_db::create_scylla_db_common_config,
};

#[cfg(test)]
#[path = "unit_tests/scylla_db.rs"]
mod tests;

type ScyllaDbStore = DbStore<ScyllaDbClient>;

impl ScyllaDbStore {
    #[cfg(any(test, feature = "test"))]
    pub async fn new_for_testing(
        store_config: ScyllaDbKvStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), ScyllaDbContextError> {
        let (client, table_status) = ScyllaDbClient::new_for_testing(store_config).await?;
        let store = Self {
            client,
            guards: ChainGuards::default(),
            user_applications: Arc::default(),
            wasm_runtime,
        };
        Ok((store, table_status))
    }

    pub async fn initialize(
        store_config: ScyllaDbKvStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, ScyllaDbContextError> {
        let client = ScyllaDbClient::initialize(store_config).await?;
        let store = Self {
            client,
            guards: ChainGuards::default(),
            user_applications: Arc::default(),
            wasm_runtime,
        };
        Ok(store)
    }

    pub async fn new(
        store_config: ScyllaDbKvStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), ScyllaDbContextError> {
        let (client, table_status) = ScyllaDbClient::new(store_config).await?;
        let store = Self {
            client,
            guards: ChainGuards::default(),
            user_applications: Arc::default(),
            wasm_runtime,
        };
        Ok((store, table_status))
    }
}

pub type ScyllaDbStoreClient<C> = DbStoreClient<ScyllaDbClient, C>;

#[cfg(any(test, feature = "test"))]
impl ScyllaDbStoreClient<TestClock> {
    pub async fn make_test_client(wasm_runtime: Option<WasmRuntime>) -> Self {
        let uri = "localhost:9042".to_string();
        let table_name = get_table_name().await;
        let common_config = create_scylla_db_common_config();
        let store_config = ScyllaDbKvStoreConfig {
            uri,
            table_name,
            common_config,
        };
        let (client, _) =
            ScyllaDbStoreClient::new_for_testing(store_config, wasm_runtime, TestClock::new())
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
            ScyllaDbStore::new_for_testing(store_config, wasm_runtime).await?;
        let store_client = ScyllaDbStoreClient {
            client: Arc::new(store),
            clock,
        };
        Ok((store_client, table_status))
    }
}

impl ScyllaDbStoreClient<WallClock> {
    pub async fn initialize(
        store_config: ScyllaDbKvStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, ScyllaDbContextError> {
        let store = ScyllaDbStore::initialize(store_config, wasm_runtime).await?;
        let store_client = ScyllaDbStoreClient {
            client: Arc::new(store),
            clock: WallClock,
        };
        Ok(store_client)
    }

    pub async fn new(
        store_config: ScyllaDbKvStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), ScyllaDbContextError> {
        let (store, table_status) = ScyllaDbStore::new(store_config, wasm_runtime).await?;
        let store_client = ScyllaDbStoreClient {
            client: Arc::new(store),
            clock: WallClock,
        };
        Ok((store_client, table_status))
    }
}

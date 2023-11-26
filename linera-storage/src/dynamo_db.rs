// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::db_storage::{DbStorage, DbStorageInner, WallClock};
use linera_execution::WasmRuntime;
use linera_views::{
    common::TableStatus,
    store::dynamo_db::{DynamoDbContextError, DynamoDbStore, DynamoDbStoreConfig},
};
use std::sync::Arc;

#[cfg(any(test, feature = "test"))]
use {
    crate::db_storage::TestClock,
    linera_views::{
        store::dynamo_db::{create_dynamo_db_common_config, LocalStackTestContext},
        test_utils::get_table_name,
    },
};

#[cfg(test)]
#[path = "unit_tests/dynamo_db.rs"]
mod tests;

type DynamoDbStorageInner = DbStorageInner<DynamoDbStore>;

impl DynamoDbStorageInner {
    #[cfg(any(test, feature = "test"))]
    pub async fn new_for_testing(
        store_config: DynamoDbStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        let (store, table_status) = DynamoDbStore::new_for_testing(store_config).await?;
        let storage = Self::new(store, wasm_runtime);
        Ok((storage, table_status))
    }

    async fn initialize(
        store_config: DynamoDbStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, DynamoDbContextError> {
        let store = DynamoDbStore::initialize(store_config).await?;
        let storage = Self::new(store, wasm_runtime);
        Ok(storage)
    }

    async fn make(
        store_config: DynamoDbStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        let (store, table_status) = DynamoDbStore::new(store_config).await?;
        let storage = Self::new(store, wasm_runtime);
        Ok((storage, table_status))
    }
}

pub type DynamoDbStorage<C> = DbStorage<DynamoDbStore, C>;

#[cfg(any(test, feature = "test"))]
impl DynamoDbStorage<TestClock> {
    pub async fn make_test_storage(wasm_runtime: Option<WasmRuntime>) -> Self {
        let table = get_table_name();
        let table_name = table.parse().expect("Invalid table name");
        let localstack = LocalStackTestContext::new().await.expect("localstack");
        let common_config = create_dynamo_db_common_config();
        let store_config = DynamoDbStoreConfig {
            config: localstack.dynamo_db_config(),
            table_name,
            common_config,
        };
        let (client, _) =
            DynamoDbStorage::new_for_testing(store_config, wasm_runtime, TestClock::new())
                .await
                .expect("client and table_name");
        client
    }

    pub async fn new_for_testing(
        store_config: DynamoDbStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
        clock: TestClock,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        let (storage, table_status) =
            DynamoDbStorageInner::new_for_testing(store_config, wasm_runtime).await?;
        let storage = DynamoDbStorage {
            client: Arc::new(storage),
            clock,
        };
        Ok((storage, table_status))
    }
}

impl DynamoDbStorage<WallClock> {
    pub async fn initialize(
        store_config: DynamoDbStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, DynamoDbContextError> {
        let storage = DynamoDbStorageInner::initialize(store_config, wasm_runtime).await?;
        let storage = DynamoDbStorage {
            client: Arc::new(storage),
            clock: WallClock,
        };
        Ok(storage)
    }

    pub async fn new(
        store_config: DynamoDbStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        let (storage, table_status) =
            DynamoDbStorageInner::make(store_config, wasm_runtime).await?;
        let storage = DynamoDbStorage {
            client: Arc::new(storage),
            clock: WallClock,
        };
        Ok((storage, table_status))
    }
}

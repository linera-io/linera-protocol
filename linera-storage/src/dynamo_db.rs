// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::db_storage::{DbStorage, DbStorageInner, WallClock};
use linera_execution::{ExecutionRuntimeConfig, WasmRuntime};
use linera_views::{
    common::TableStatus,
    dynamo_db::{DynamoDbContextError, DynamoDbStore, DynamoDbStoreConfig},
};
use std::sync::Arc;

#[cfg(any(test, feature = "test"))]
use {
    crate::db_storage::TestClock,
    linera_views::{
        dynamo_db::{create_dynamo_db_common_config, LocalStackTestContext},
        test_utils::get_namespace,
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
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        let (store, table_status) = DynamoDbStore::new_for_testing(store_config, namespace).await?;
        let storage = Self::new(store, wasm_runtime);
        Ok((storage, table_status))
    }

    async fn initialize(
        store_config: DynamoDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, DynamoDbContextError> {
        let store = DynamoDbStore::initialize(store_config, namespace).await?;
        let storage = Self::new(store, wasm_runtime);
        Ok(storage)
    }

    async fn make(
        store_config: DynamoDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        let (store, table_status) = DynamoDbStore::new(store_config, namespace).await?;
        let storage = Self::new(store, wasm_runtime);
        Ok((storage, table_status))
    }
}

pub type DynamoDbStorage<C> = DbStorage<DynamoDbStore, C>;

#[cfg(any(test, feature = "test"))]
impl DynamoDbStorage<TestClock> {
    pub async fn make_test_storage(wasm_runtime: Option<WasmRuntime>) -> Self {
        let namespace = get_namespace();
        let localstack = LocalStackTestContext::new().await.expect("localstack");
        let common_config = create_dynamo_db_common_config();
        let store_config = DynamoDbStoreConfig {
            config: localstack.dynamo_db_config(),
            common_config,
        };
        let (client, _) = DynamoDbStorage::new_for_testing(
            store_config,
            &namespace,
            wasm_runtime,
            TestClock::new(),
        )
        .await
        .expect("client and table_name");
        client
    }

    pub async fn new_for_testing(
        store_config: DynamoDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
        clock: TestClock,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        let (storage, table_status) =
            DynamoDbStorageInner::new_for_testing(store_config, namespace, wasm_runtime).await?;
        let storage = DynamoDbStorage {
            client: Arc::new(storage),
            clock,
            execution_runtime_config: ExecutionRuntimeConfig::default(),
        };
        Ok((storage, table_status))
    }
}

impl DynamoDbStorage<WallClock> {
    pub async fn initialize(
        store_config: DynamoDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, DynamoDbContextError> {
        let storage =
            DynamoDbStorageInner::initialize(store_config, namespace, wasm_runtime).await?;
        let storage = DynamoDbStorage {
            client: Arc::new(storage),
            clock: WallClock,
            execution_runtime_config: ExecutionRuntimeConfig::default(),
        };
        Ok(storage)
    }

    pub async fn new(
        store_config: DynamoDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        let (storage, table_status) =
            DynamoDbStorageInner::make(store_config, namespace, wasm_runtime).await?;
        let storage = DynamoDbStorage {
            client: Arc::new(storage),
            clock: WallClock,
            execution_runtime_config: ExecutionRuntimeConfig::default(),
        };
        Ok((storage, table_status))
    }
}

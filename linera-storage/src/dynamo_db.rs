// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::db_storage::{DbStorage, DbStorageInner, WallClock};
use linera_execution::{ExecutionRuntimeConfig, WasmRuntime};
use linera_views::dynamo_db::{DynamoDbContextError, DynamoDbStore, DynamoDbStoreConfig};
use std::sync::Arc;
use linera_views::common::AdminKeyValueStore;

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
    ) -> Result<Self, DynamoDbContextError> {
        let store = DynamoDbStore::new_for_testing(store_config, namespace).await?;
        let storage = Self::new(store, wasm_runtime);
        Ok(storage)
    }

    async fn initialize(
        store_config: DynamoDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, DynamoDbContextError> {
        let store = DynamoDbStore::initialize(&store_config, namespace).await?;
        let storage = Self::new(store, wasm_runtime);
        Ok(storage)
    }

    async fn make(
        store_config: DynamoDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, DynamoDbContextError> {
        let store = DynamoDbStore::new(store_config, namespace).await?;
        let storage = Self::new(store, wasm_runtime);
        Ok(storage)
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
        DynamoDbStorage::new_for_testing(store_config, &namespace, wasm_runtime, TestClock::new())
            .await
            .expect("client and table_name")
    }

    pub async fn new_for_testing(
        store_config: DynamoDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
        clock: TestClock,
    ) -> Result<Self, DynamoDbContextError> {
        let storage =
            DynamoDbStorageInner::new_for_testing(store_config, namespace, wasm_runtime).await?;
        let storage = DynamoDbStorage {
            client: Arc::new(storage),
            clock,
            execution_runtime_config: ExecutionRuntimeConfig::default(),
        };
        Ok(storage)
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
    ) -> Result<Self, DynamoDbContextError> {
        let storage = DynamoDbStorageInner::make(store_config, namespace, wasm_runtime).await?;
        let storage = DynamoDbStorage {
            client: Arc::new(storage),
            clock: WallClock,
            execution_runtime_config: ExecutionRuntimeConfig::default(),
        };
        Ok(storage)
    }
}

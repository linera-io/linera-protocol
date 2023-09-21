// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain_guards::ChainGuards, DbStore, DbStoreInner, WallClock};
use dashmap::DashMap;
use linera_execution::WasmRuntime;
use linera_views::{
    common::TableStatus,
    dynamo_db::{DynamoDbClient, DynamoDbContextError, DynamoDbKvStoreConfig},
};
use std::sync::Arc;
#[cfg(any(test, feature = "test"))]
use {
    crate::TestClock,
    linera_views::{
        dynamo_db::{create_dynamo_db_common_config, LocalStackTestContext},
        test_utils::get_table_name,
    },
};

#[cfg(test)]
#[path = "unit_tests/dynamo_db.rs"]
mod tests;

type DynamoDbStoreInner = DbStoreInner<DynamoDbClient>;

impl DynamoDbStoreInner {
    #[cfg(any(test, feature = "test"))]
    pub async fn new_for_testing(
        store_config: DynamoDbKvStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        let (client, table_status) = DynamoDbClient::new_for_testing(store_config).await?;
        let store = Self {
            client,
            guards: ChainGuards::default(),
            user_applications: Arc::new(DashMap::new()),
            wasm_runtime,
        };
        Ok((store, table_status))
    }

    pub async fn initialize(
        store_config: DynamoDbKvStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, DynamoDbContextError> {
        let client = DynamoDbClient::initialize(store_config).await?;
        let store = Self {
            client,
            guards: ChainGuards::default(),
            user_applications: Arc::new(DashMap::new()),
            wasm_runtime,
        };
        Ok(store)
    }

    pub async fn new(
        store_config: DynamoDbKvStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        let (client, table_status) = DynamoDbClient::new(store_config).await?;
        let store = Self {
            client,
            guards: ChainGuards::default(),
            user_applications: Arc::new(DashMap::new()),
            wasm_runtime,
        };
        Ok((store, table_status))
    }
}

pub type DynamoDbStore<C> = DbStore<DynamoDbClient, C>;

#[cfg(any(test, feature = "test"))]
impl DynamoDbStore<TestClock> {
    pub async fn make_test_store(wasm_runtime: Option<WasmRuntime>) -> Self {
        let table = get_table_name();
        let table_name = table.parse().expect("Invalid table name");
        let localstack = LocalStackTestContext::new().await.expect("localstack");
        let common_config = create_dynamo_db_common_config();
        let store_config = DynamoDbKvStoreConfig {
            config: localstack.dynamo_db_config(),
            table_name,
            common_config,
        };
        let (client, _) =
            DynamoDbStore::new_for_testing(store_config, wasm_runtime, TestClock::new())
                .await
                .expect("client and table_name");
        client
    }

    pub async fn new_for_testing(
        store_config: DynamoDbKvStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
        clock: TestClock,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        let (store, table_status) =
            DynamoDbStoreInner::new_for_testing(store_config, wasm_runtime).await?;
        let store = DynamoDbStore {
            client: Arc::new(store),
            clock,
        };
        Ok((store, table_status))
    }
}

impl DynamoDbStore<WallClock> {
    pub async fn initialize(
        store_config: DynamoDbKvStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, DynamoDbContextError> {
        let store = DynamoDbStoreInner::initialize(store_config, wasm_runtime).await?;
        let store = DynamoDbStore {
            client: Arc::new(store),
            clock: WallClock,
        };
        Ok(store)
    }

    pub async fn new(
        store_config: DynamoDbKvStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        let (store, table_status) = DynamoDbStoreInner::new(store_config, wasm_runtime).await?;
        let store = DynamoDbStore {
            client: Arc::new(store),
            clock: WallClock,
        };
        Ok((store, table_status))
    }
}

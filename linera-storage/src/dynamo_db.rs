// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain_guards::ChainGuards, DbStore, DbStoreClient, WallClock};
use dashmap::DashMap;
use linera_execution::WasmRuntime;
use linera_views::{
    common::{CommonStoreConfig, TableStatus},
    dynamo_db::{Config, DynamoDbClient, DynamoDbContextError, TableName},
};
use std::sync::Arc;
#[cfg(any(test, feature = "test"))]
use {
    crate::TestClock,
    linera_views::{
        common::get_table_name,
        dynamo_db::{create_dynamo_db_common_config, LocalStackTestContext},
    },
};

#[cfg(test)]
#[path = "unit_tests/dynamo_db.rs"]
mod tests;

type DynamoDbStore = DbStore<DynamoDbClient>;

impl DynamoDbStore {
    #[cfg(any(test, feature = "test"))]
    pub async fn new_for_testing(
        config: Config,
        table: TableName,
        common_config: CommonStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        let (client, table_status) =
            DynamoDbClient::new_for_testing(config, table, common_config).await?;
        let store = Self {
            client,
            guards: ChainGuards::default(),
            user_applications: Arc::new(DashMap::new()),
            wasm_runtime,
        };
        Ok((store, table_status))
    }

    pub async fn new(
        config: Config,
        table: TableName,
        common_config: CommonStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        let (client, table_status) = DynamoDbClient::new(config, table, common_config).await?;
        let store = Self {
            client,
            guards: ChainGuards::default(),
            user_applications: Arc::new(DashMap::new()),
            wasm_runtime,
        };
        Ok((store, table_status))
    }
}

pub type DynamoDbStoreClient<C> = DbStoreClient<DynamoDbClient, C>;

#[cfg(any(test, feature = "test"))]
impl DynamoDbStoreClient<TestClock> {
    pub async fn make_test_client(wasm_runtime: Option<WasmRuntime>) -> Self {
        let table = get_table_name().await;
        let table_name = table.parse().expect("Invalid table name");
        let localstack = LocalStackTestContext::new().await.expect("localstack");
        let common_config = create_dynamo_db_common_config();
        let (client, _) = DynamoDbStoreClient::new_for_testing(
            localstack.dynamo_db_config(),
            table_name,
            common_config,
            wasm_runtime,
        )
        .await
        .expect("client and table_name");
        client
    }

    pub async fn new_for_testing(
        config: impl Into<Config>,
        table: TableName,
        common_config: CommonStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        let (store, table_status) =
            DynamoDbStore::new_for_testing(config.into(), table, common_config, wasm_runtime)
                .await?;
        let store_client = DynamoDbStoreClient {
            client: Arc::new(store),
            clock: TestClock::new(),
        };
        Ok((store_client, table_status))
    }
}

impl DynamoDbStoreClient<WallClock> {
    pub async fn new(
        config: impl Into<Config>,
        table: TableName,
        common_config: CommonStoreConfig,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        let (store, table_status) =
            DynamoDbStore::new(config.into(), table, common_config, wasm_runtime).await?;
        let store_client = DynamoDbStoreClient {
            client: Arc::new(store),
            clock: WallClock,
        };
        Ok((store_client, table_status))
    }
}

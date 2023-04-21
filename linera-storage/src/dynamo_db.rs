// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain_guards::ChainGuards, DbStore, DbStoreClient};
use dashmap::DashMap;
use futures::Future;
use linera_execution::WasmRuntime;
use linera_views::dynamo_db::{
    Config, DynamoDbClient, DynamoDbContextError, TableName, TableStatus,
};
use std::sync::Arc;

#[cfg(test)]
#[path = "unit_tests/dynamo_db.rs"]
mod tests;

type DynamoDbStore = DbStore<DynamoDbClient>;

pub type DynamoDbStoreClient = DbStoreClient<DynamoDbClient>;

impl DynamoDbStoreClient {
    pub async fn new(
        table: TableName,
        cache_size: usize,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        Self::with_store(DynamoDbStore::new(table, cache_size, wasm_runtime)).await
    }

    pub async fn from_config(
        config: impl Into<Config>,
        table: TableName,
        cache_size: usize,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        Self::with_store(DynamoDbStore::from_config(
            config.into(),
            table,
            cache_size,
            wasm_runtime,
        ))
        .await
    }

    pub async fn with_localstack(
        table: TableName,
        cache_size: usize,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        Self::with_store(DynamoDbStore::with_localstack(
            table,
            cache_size,
            wasm_runtime,
        ))
        .await
    }

    async fn with_store<E>(
        store_creator: impl Future<Output = Result<(DynamoDbStore, TableStatus), E>>,
    ) -> Result<(Self, TableStatus), E> {
        let (store, table_status) = store_creator.await?;
        let client = DynamoDbStoreClient {
            client: Arc::new(store),
        };
        Ok((client, table_status))
    }
}

impl DynamoDbStore {
    pub async fn new(
        table: TableName,
        cache_size: usize,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        Self::with_client(|| DynamoDbClient::new(table, cache_size), wasm_runtime).await
    }

    pub async fn from_config(
        config: Config,
        table: TableName,
        cache_size: usize,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        Self::with_client(
            || DynamoDbClient::from_config(config, table, cache_size),
            wasm_runtime,
        )
        .await
    }

    pub async fn with_localstack(
        table: TableName,
        cache_size: usize,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        Self::with_client(
            || DynamoDbClient::with_localstack(table, cache_size),
            wasm_runtime,
        )
        .await
    }

    async fn with_client<F, E>(
        create_client: impl FnOnce() -> F,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), E>
    where
        F: Future<Output = Result<(DynamoDbClient, TableStatus), E>>,
    {
        let (client, table_status) = create_client().await?;
        Ok((
            Self {
                client,
                guards: ChainGuards::default(),
                user_applications: Arc::new(DashMap::new()),
                wasm_runtime,
            },
            table_status,
        ))
    }
}

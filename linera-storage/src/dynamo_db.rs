// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain_guards::ChainGuards, DbStore, DbStoreClient};
use dashmap::DashMap;
use futures::Future;
use linera_execution::WasmRuntime;
use linera_views::{
    dynamo_db::{
        Config, DynamoDbClient, DynamoDbContextError, TableName, TableStatus,
        TEST_DYNAMO_DB_MAX_CONCURRENT_QUERIES, TEST_DYNAMO_DB_MAX_STREAM_QUERIES,
    },
    lru_caching::TEST_CACHE_SIZE,
    test_utils::LocalStackTestContext,
};
use std::sync::Arc;

#[cfg(test)]
#[path = "unit_tests/dynamo_db.rs"]
mod tests;

type DynamoDbStore = DbStore<DynamoDbClient>;

impl DynamoDbStore {
    pub async fn new(
        table: TableName,
        max_concurrent_queries: Option<usize>,
        max_stream_queries: usize,
        cache_size: usize,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        Self::with_client(
            || {
                DynamoDbClient::new(
                    table,
                    max_concurrent_queries,
                    max_stream_queries,
                    cache_size,
                )
            },
            wasm_runtime,
        )
        .await
    }

    pub async fn from_config(
        config: Config,
        table: TableName,
        max_concurrent_queries: Option<usize>,
        max_stream_queries: usize,
        cache_size: usize,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        Self::with_client(
            || {
                DynamoDbClient::from_config(
                    config,
                    table,
                    max_concurrent_queries,
                    max_stream_queries,
                    cache_size,
                )
            },
            wasm_runtime,
        )
        .await
    }

    pub async fn with_localstack(
        table: TableName,
        max_concurrent_queries: Option<usize>,
        max_stream_queries: usize,
        cache_size: usize,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        Self::with_client(
            || {
                DynamoDbClient::with_localstack(
                    table,
                    max_concurrent_queries,
                    max_stream_queries,
                    cache_size,
                )
            },
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

pub type DynamoDbStoreClient = DbStoreClient<DynamoDbClient>;

impl DynamoDbStoreClient {

    #[cfg(any(test, feature = "test"))]
    pub async fn new_test() -> Self {
        let table = "linera".parse().expect("Invalid table name");
        let localstack = LocalStackTestContext::new().await.expect("localstack");
        let (client, _) = DynamoDbStoreClient::from_config(
            localstack.dynamo_db_config(),
            table,
            Some(TEST_DYNAMO_DB_MAX_CONCURRENT_QUERIES),
            TEST_DYNAMO_DB_MAX_STREAM_QUERIES,
            TEST_CACHE_SIZE,
            None,
        )
            .await
            .expect("client and table_name");
        client
    }

    pub async fn new(
        table: TableName,
        max_concurrent_queries: Option<usize>,
        max_stream_queries: usize,
        cache_size: usize,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        Self::with_store(DynamoDbStore::new(
            table,
            max_concurrent_queries,
            max_stream_queries,
            cache_size,
            wasm_runtime,
        ))
        .await
    }

    pub async fn from_config(
        config: impl Into<Config>,
        table: TableName,
        max_concurrent_queries: Option<usize>,
        max_stream_queries: usize,
        cache_size: usize,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        Self::with_store(DynamoDbStore::from_config(
            config.into(),
            table,
            max_concurrent_queries,
            max_stream_queries,
            cache_size,
            wasm_runtime,
        ))
        .await
    }

    pub async fn with_localstack(
        table: TableName,
        max_concurrent_queries: Option<usize>,
        max_stream_queries: usize,
        cache_size: usize,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        Self::with_store(DynamoDbStore::with_localstack(
            table,
            max_concurrent_queries,
            max_stream_queries,
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

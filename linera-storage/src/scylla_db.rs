// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain_guards::ChainGuards, DbStore, DbStoreClient};
use linera_execution::WasmRuntime;
use linera_views::scylla_db::{get_table_name, ScyllaDbClient, ScyllaDbContextError};
use std::sync::Arc;

#[cfg(any(test, feature = "test"))]
use linera_views::{
    lru_caching::TEST_CACHE_SIZE,
    scylla_db::{TEST_SCYLLA_DB_MAX_CONCURRENT_QUERIES, TEST_SCYLLA_DB_MAX_STREAM_QUERIES},
};

#[cfg(test)]
#[path = "unit_tests/scylla_db.rs"]
mod tests;

type ScyllaDbStore = DbStore<ScyllaDbClient>;

impl ScyllaDbStore {
    pub async fn new(
        restart_database: bool,
        uri: &str,
        table_name: String,
        max_concurrent_queries: Option<usize>,
        max_stream_queries: usize,
        cache_size: usize,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, ScyllaDbContextError> {
        let client = ScyllaDbClient::new(
            restart_database,
            uri,
            table_name,
            max_concurrent_queries,
            max_stream_queries,
            cache_size,
        )
        .await?;
        Ok(Self {
            client,
            guards: ChainGuards::default(),
            user_applications: Arc::default(),
            wasm_runtime,
        })
    }
}

pub type ScyllaDbStoreClient = DbStoreClient<ScyllaDbClient>;

impl ScyllaDbStoreClient {
    #[cfg(any(test, feature = "test"))]
    pub async fn make_test_client(wasm_runtime: Option<WasmRuntime>) -> ScyllaDbStoreClient {
        let restart_database = true;
        let uri = "localhost:9042";
        let table_name = get_table_name().await;
        let max_concurrent_queries = Some(TEST_SCYLLA_DB_MAX_CONCURRENT_QUERIES);
        let max_stream_queries = TEST_SCYLLA_DB_MAX_STREAM_QUERIES;
        let cache_size = TEST_CACHE_SIZE;
        ScyllaDbStoreClient::new(
            restart_database,
            uri,
            table_name,
            max_concurrent_queries,
            max_stream_queries,
            cache_size,
            wasm_runtime,
        )
        .await
        .expect("client")
    }

    pub async fn new(
        restart_database: bool,
        uri: &str,
        table_name: String,
        max_concurrent_queries: Option<usize>,
        max_stream_queries: usize,
        cache_size: usize,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, ScyllaDbContextError> {
        let store = ScyllaDbStore::new(
            restart_database,
            uri,
            table_name,
            max_concurrent_queries,
            max_stream_queries,
            cache_size,
            wasm_runtime,
        )
        .await?;
        Ok(ScyllaDbStoreClient {
            client: Arc::new(store),
        })
    }
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain_guards::ChainGuards, DbStore, DbStoreClient};
use linera_execution::WasmRuntime;
use linera_views::rocks_db::RocksDbClient;
use std::{path::PathBuf, sync::Arc};
#[cfg(any(test, feature = "test"))]
use {
    linera_views::{lru_caching::TEST_CACHE_SIZE, rocks_db::TEST_ROCKS_DB_MAX_STREAM_QUERIES},
    tempfile::TempDir,
};

#[cfg(test)]
#[path = "unit_tests/rocks_db.rs"]
mod tests;

type RocksDbStore = DbStore<RocksDbClient>;

impl RocksDbStore {
    pub fn new(
        dir: PathBuf,
        wasm_runtime: Option<WasmRuntime>,
        max_stream_queries: usize,
        cache_size: usize,
    ) -> Self {
        let client = RocksDbClient::new(dir, max_stream_queries, cache_size);
        Self {
            client,
            guards: ChainGuards::default(),
            user_applications: Arc::default(),
            wasm_runtime,
        }
    }
}

pub type RocksDbStoreClient = DbStoreClient<RocksDbClient>;

impl RocksDbStoreClient {
    #[cfg(any(test, feature = "test"))]
    pub async fn new_test() -> RocksDbStoreClient {
        let dir = TempDir::new().unwrap();
        RocksDbStoreClient::new(
            dir.path().to_path_buf(),
            None,
            TEST_ROCKS_DB_MAX_STREAM_QUERIES,
            TEST_CACHE_SIZE,
        )
    }

    pub fn new(
        path: PathBuf,
        wasm_runtime: Option<WasmRuntime>,
        max_stream_queries: usize,
        cache_size: usize,
    ) -> Self {
        RocksDbStoreClient {
            client: Arc::new(RocksDbStore::new(
                path,
                wasm_runtime,
                max_stream_queries,
                cache_size,
            )),
        }
    }
}

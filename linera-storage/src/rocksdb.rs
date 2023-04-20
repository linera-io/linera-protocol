// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain_guards::ChainGuards, DbStore, DbStoreClient};
use linera_execution::WasmRuntime;
use linera_views::rocksdb::RocksdbClient;
use std::{path::PathBuf, sync::Arc};

#[cfg(test)]
#[path = "unit_tests/rocksdb.rs"]
mod tests;

type RocksdbStore = DbStore<RocksdbClient>;

pub type RocksdbStoreClient = DbStoreClient<RocksdbClient>;

impl RocksdbStoreClient {
    pub fn new(path: PathBuf, wasm_runtime: Option<WasmRuntime>, cache_size: usize) -> Self {
        RocksdbStoreClient {
            client: Arc::new(RocksdbStore::new(path, wasm_runtime, cache_size)),
        }
    }
}

impl RocksdbStore {
    pub fn new(dir: PathBuf, wasm_runtime: Option<WasmRuntime>, cache_size: usize) -> Self {
        let client = RocksdbClient::new(dir, cache_size);
        Self {
            client,
            guards: ChainGuards::default(),
            user_applications: Arc::default(),
            wasm_runtime,
        }
    }
}

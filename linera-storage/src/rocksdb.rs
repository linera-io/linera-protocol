// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain_guards::ChainGuards, DbStore, DbStoreClient};
use linera_execution::WasmRuntime;
use linera_views::rocks_db::RocksDbClient;
use std::{path::PathBuf, sync::Arc};

#[cfg(test)]
#[path = "unit_tests/rocksdb.rs"]
mod tests;

type RocksDbStore = DbStore<RocksDbClient>;

pub type RocksDbStoreClient = DbStoreClient<RocksDbClient>;

impl RocksDbStoreClient {
    pub fn new(path: PathBuf, wasm_runtime: Option<WasmRuntime>, cache_size: usize) -> Self {
        RocksDbStoreClient {
            client: Arc::new(RocksDbStore::new(path, wasm_runtime, cache_size)),
        }
    }
}

impl RocksDbStore {
    pub fn new(dir: PathBuf, wasm_runtime: Option<WasmRuntime>, cache_size: usize) -> Self {
        let client = RocksDbClient::new(dir, cache_size);
        Self {
            client,
            guards: ChainGuards::default(),
            user_applications: Arc::default(),
            wasm_runtime,
        }
    }
}

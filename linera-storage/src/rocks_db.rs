// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain_guards::ChainGuards, DbStore, DbStoreClient, WallClock};
use linera_execution::WasmRuntime;
use linera_views::{
    common::{CommonStoreConfig, TableStatus},
    rocks_db::{RocksDbClient, RocksDbContextError},
};
use std::{path::PathBuf, sync::Arc};

#[cfg(any(test, feature = "test"))]
use {linera_views::rocks_db::create_rocks_db_common_config, tempfile::TempDir};

#[cfg(test)]
#[path = "unit_tests/rocks_db.rs"]
mod tests;

type RocksDbStore = DbStore<RocksDbClient>;

impl RocksDbStore {
    #[cfg(any(test, feature = "test"))]
    pub async fn new_for_testing(
        dir: PathBuf,
        wasm_runtime: Option<WasmRuntime>,
        common_config: CommonStoreConfig,
    ) -> Result<(Self, TableStatus), RocksDbContextError> {
        let (client, table_status) = RocksDbClient::new_for_testing(dir, common_config).await?;
        let store = Self {
            client,
            guards: ChainGuards::default(),
            user_applications: Arc::default(),
            wasm_runtime,
        };
        Ok((store, table_status))
    }

    pub async fn new(
        dir: PathBuf,
        wasm_runtime: Option<WasmRuntime>,
        common_config: CommonStoreConfig,
    ) -> Result<(Self, TableStatus), RocksDbContextError> {
        let (client, table_status) = RocksDbClient::new(dir, common_config).await?;
        let store = Self {
            client,
            guards: ChainGuards::default(),
            user_applications: Arc::default(),
            wasm_runtime,
        };
        Ok((store, table_status))
    }
}

pub type RocksDbStoreClient<C> = DbStoreClient<RocksDbClient, C>;

#[cfg(any(test, feature = "test"))]
impl RocksDbStoreClient<crate::TestClock> {
    pub async fn make_test_client(wasm_runtime: Option<WasmRuntime>) -> Self {
        let dir = TempDir::new().unwrap();
        let common_config = create_rocks_db_common_config();
        let (store_client, _) = RocksDbStoreClient::new_for_testing(
            dir.path().to_path_buf(),
            wasm_runtime,
            common_config,
        )
        .await
        .expect("store_client");
        store_client
    }

    #[cfg(any(test, feature = "test"))]
    pub async fn new_for_testing(
        path: PathBuf,
        wasm_runtime: Option<WasmRuntime>,
        common_config: CommonStoreConfig,
    ) -> Result<(Self, TableStatus), RocksDbContextError> {
        let (store, table_status) =
            RocksDbStore::new_for_testing(path, wasm_runtime, common_config).await?;
        let store_client = RocksDbStoreClient {
            client: Arc::new(store),
            clock: crate::TestClock::new(),
        };
        Ok((store_client, table_status))
    }
}

impl RocksDbStoreClient<WallClock> {
    pub async fn new(
        path: PathBuf,
        wasm_runtime: Option<WasmRuntime>,
        common_config: CommonStoreConfig,
    ) -> Result<(Self, TableStatus), RocksDbContextError> {
        let (store, table_status) = RocksDbStore::new(path, wasm_runtime, common_config).await?;
        let store_client = RocksDbStoreClient {
            client: Arc::new(store),
            clock: WallClock,
        };
        Ok((store_client, table_status))
    }
}

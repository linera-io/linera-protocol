// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::db_storage::{DbStorage, DbStorageInner};
use linera_execution::WasmRuntime;
use linera_views::rocks_db::{RocksDbContextError, RocksDbStore, RocksDbStoreConfig};

#[cfg(any(test, feature = "test"))]
use {
    crate::db_storage::TestClock, linera_views::rocks_db::create_rocks_db_test_config,
    linera_views::test_utils::get_namespace, tempfile::TempDir,
};

#[cfg(test)]
#[path = "unit_tests/rocks_db.rs"]
mod tests;

type RocksDbStorageInner = DbStorageInner<RocksDbStore>;

pub type RocksDbStorage<C> = DbStorage<RocksDbStore, C>;

#[cfg(any(test, feature = "test"))]
impl RocksDbStorage<TestClock> {
    pub async fn make_test_storage(wasm_runtime: Option<WasmRuntime>) -> (Self, TempDir) {
        let (store_config, dir) = create_rocks_db_test_config().await;
        let namespace = get_namespace();
        let storage = RocksDbStorage::new_for_testing(
            store_config,
            &namespace,
            wasm_runtime,
            TestClock::new(),
        )
        .await
        .expect("storage");
        (storage, dir)
    }

    pub async fn new_for_testing(
        store_config: RocksDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
        clock: TestClock,
    ) -> Result<Self, RocksDbContextError> {
        let storage =
            RocksDbStorageInner::new_for_testing(store_config, namespace, wasm_runtime).await?;
        Ok(Self::create(storage, clock))
    }
}

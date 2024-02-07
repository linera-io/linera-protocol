// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::db_storage::{DbStorage, DbStorageInner};
use linera_execution::WasmRuntime;
use linera_views::scylla_db::{ScyllaDbContextError, ScyllaDbStore, ScyllaDbStoreConfig};

#[cfg(any(test, feature = "test"))]
use {
    crate::db_storage::TestClock, linera_views::scylla_db::create_scylla_db_test_config,
    linera_views::test_utils::get_namespace,
};

#[cfg(test)]
#[path = "unit_tests/scylla_db.rs"]
mod tests;

type ScyllaDbStorageInner = DbStorageInner<ScyllaDbStore>;

pub type ScyllaDbStorage<C> = DbStorage<ScyllaDbStore, C>;

#[cfg(any(test, feature = "test"))]
impl ScyllaDbStorage<TestClock> {
    pub async fn make_test_storage(wasm_runtime: Option<WasmRuntime>) -> Self {
        let store_config = create_scylla_db_test_config().await;
        let namespace = get_namespace();
        ScyllaDbStorage::new_for_testing(store_config, &namespace, wasm_runtime, TestClock::new())
            .await
            .expect("storage")
    }

    pub async fn new_for_testing(
        store_config: ScyllaDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
        clock: TestClock,
    ) -> Result<Self, ScyllaDbContextError> {
        let storage =
            ScyllaDbStorageInner::new_for_testing(store_config, namespace, wasm_runtime).await?;
        Ok(Self::create(storage, clock))
    }
}

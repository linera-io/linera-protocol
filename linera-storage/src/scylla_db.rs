// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::db_storage::DbStorage;
use linera_views::scylla_db::ScyllaDbStore;

#[cfg(any(test, feature = "test"))]
use {
    crate::db_storage::{DbStorageInner, TestClock},
    linera_execution::WasmRuntime,
    linera_views::scylla_db::{
        create_scylla_db_test_config, ScyllaDbContextError, ScyllaDbStoreConfig,
    },
    linera_views::test_utils::generate_test_namespace,
};

#[cfg(test)]
#[path = "unit_tests/scylla_db.rs"]
mod tests;


pub type ScyllaDbStorage<C> = DbStorage<ScyllaDbStore, C>;

#[cfg(any(test, feature = "test"))]
impl ScyllaDbStorage<TestClock> {
    pub async fn make_test_storage(wasm_runtime: Option<WasmRuntime>) -> Self {
        let store_config = create_scylla_db_test_config().await;
        let namespace = generate_test_namespace();
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
            DbStorageInner::<ScyllaDbStore>::new_for_testing(store_config, namespace, wasm_runtime)
                .await?;
        Ok(Self::create(storage, clock))
    }
}

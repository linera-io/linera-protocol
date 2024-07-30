// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::scylla_db::ScyllaDbStore;
#[cfg(with_testing)]
use {
    crate::db_storage::{DbStorageInner, TestClock},
    linera_execution::WasmRuntime,
    linera_views::scylla_db::{
        create_scylla_db_test_config, ScyllaDbStoreConfig, ScyllaDbStoreError,
    },
    linera_views::test_utils::generate_test_namespace,
};

use crate::db_storage::DbStorage;

pub type ScyllaDbStorage<C> = DbStorage<ScyllaDbStore, C>;

#[cfg(with_testing)]
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
    ) -> Result<Self, ScyllaDbStoreError> {
        let storage =
            DbStorageInner::<ScyllaDbStore>::new_for_testing(store_config, namespace, wasm_runtime)
                .await?;
        Ok(Self::create(storage, clock))
    }
}

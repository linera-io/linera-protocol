// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::scylla_db::ScyllaDbStore;
#[cfg(with_testing)]
use {
    crate::db_storage::{DbStorageInner, TestClock},
    linera_execution::WasmRuntime,
    linera_views::scylla_db::{ScyllaDbStoreConfig, ScyllaDbStoreError},
    linera_views::common::{AdminKeyValueStore as _},
    linera_views::test_utils::generate_test_namespace,
};

use crate::db_storage::DbStorage;

pub type ScyllaDbStorage<C> = DbStorage<ScyllaDbStore, C>;

#[cfg(with_testing)]
impl ScyllaDbStorage<TestClock> {
    pub async fn make_test_storage(wasm_runtime: Option<WasmRuntime>) -> Self {
        let store_config = ScyllaDbStore::get_test_config().await.expect("config");
        let namespace = generate_test_namespace();
        let root_key = &[];
        ScyllaDbStorage::new_for_testing(
            store_config,
            &namespace,
            root_key,
            wasm_runtime,
            TestClock::new(),
        )
        .await
        .expect("storage")
    }

    pub async fn new_for_testing(
        store_config: ScyllaDbStoreConfig,
        namespace: &str,
        root_key: &[u8],
        wasm_runtime: Option<WasmRuntime>,
        clock: TestClock,
    ) -> Result<Self, ScyllaDbStoreError> {
        let storage = DbStorageInner::<ScyllaDbStore>::new_for_testing(
            store_config,
            namespace,
            root_key,
            wasm_runtime,
        )
        .await?;
        Ok(Self::create(storage, clock))
    }
}

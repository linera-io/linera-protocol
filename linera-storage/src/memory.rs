// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::memory::MemoryStore;
#[cfg(with_testing)]
use {
    crate::db_storage::{DbStorageInner, TestClock},
    linera_execution::WasmRuntime,
    linera_views::{
        memory::{create_memory_store_test_config, MemoryStoreConfig, MemoryStoreError},
        test_utils::generate_test_namespace,
    },
};

use crate::db_storage::DbStorage;

pub type MemoryStorage<C> = DbStorage<MemoryStore, C>;

#[cfg(with_testing)]
impl MemoryStorage<TestClock> {
    pub async fn make_test_storage(wasm_runtime: Option<WasmRuntime>) -> Self {
        let store_config = create_memory_store_test_config();
        let namespace = generate_test_namespace();
        MemoryStorage::new_for_testing(store_config, &namespace, wasm_runtime, TestClock::new())
            .await
            .expect("storage")
    }

    pub async fn new_for_testing(
        store_config: MemoryStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
        clock: TestClock,
    ) -> Result<Self, MemoryStoreError> {
        let storage =
            DbStorageInner::<MemoryStore>::new_for_testing(store_config, namespace, wasm_runtime)
                .await?;
        Ok(Self::create(storage, clock))
    }
}

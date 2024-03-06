// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::db_storage::DbStorage;
use linera_views::memory::MemoryStore;
#[cfg(any(test, feature = "test"))]
use {
    crate::db_storage::DbStorageInner,
    linera_execution::WasmRuntime,
    linera_views::memory::{MemoryContextError, MemoryStoreConfig},
};

pub type MemoryStorage<C> = DbStorage<MemoryStore, C>;

#[cfg(any(test, feature = "test"))]
use crate::db_storage::TestClock;

#[cfg(any(test, feature = "test"))]
impl MemoryStorage<crate::TestClock> {
    pub async fn make_test_storage(wasm_runtime: Option<WasmRuntime>) -> Self {
        let clock = crate::TestClock::new();
        let max_stream_queries = linera_views::memory::TEST_MEMORY_MAX_STREAM_QUERIES;
        MemoryStorage::new_for_testing(wasm_runtime, max_stream_queries, clock)
            .await
            .expect("storage")
    }

    pub async fn new_for_testing(
        wasm_runtime: Option<WasmRuntime>,
        max_stream_queries: usize,
        clock: TestClock,
    ) -> Result<Self, MemoryContextError> {
        let store_config = MemoryStoreConfig::new(max_stream_queries);
        let namespace = "unused_namespace";
        let storage =
            DbStorageInner::<MemoryStore>::make(store_config, namespace, wasm_runtime).await?;
        Ok(Self::create(storage, clock))
    }
}

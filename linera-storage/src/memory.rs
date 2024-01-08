// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::db_storage::{DbStorage, DbStorageInner};
use linera_execution::{ExecutionRuntimeConfig, WasmRuntime};
use linera_views::memory::{create_memory_store_stream_queries, MemoryStore};
use std::sync::Arc;

type MemoryStorageInner = DbStorageInner<MemoryStore>;

impl MemoryStorageInner {
    pub fn make(wasm_runtime: Option<WasmRuntime>, max_stream_queries: usize) -> Self {
        let store = create_memory_store_stream_queries(max_stream_queries);
        Self::new(store, wasm_runtime)
    }
}

pub type MemoryStorage<C> = DbStorage<MemoryStore, C>;

#[cfg(any(test, feature = "test"))]
impl MemoryStorage<crate::TestClock> {
    pub async fn make_test_storage(wasm_runtime: Option<WasmRuntime>) -> Self {
        let clock = crate::TestClock::new();
        let max_stream_queries = linera_views::memory::TEST_MEMORY_MAX_STREAM_QUERIES;
        MemoryStorage::new(wasm_runtime, max_stream_queries, clock)
    }
}

impl<C> MemoryStorage<C> {
    pub fn new(
        wasm_runtime: Option<WasmRuntime>,
        max_stream_queries: usize,
        clock: C,
    ) -> Self {
        Self {
            client: Arc::new(MemoryStorageInner::make(wasm_runtime, max_stream_queries)),
            clock,
            execution_runtime_config: ExecutionRuntimeConfig::default(),
        }
    }
}

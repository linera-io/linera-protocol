// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::db_storage::{DbStorage, DbStorageInner};
use linera_execution::{ExecutionRuntimeConfig, WasmRuntime};
use linera_views::memory::{MemoryContextError, MemoryStore, MemoryStoreConfig};
use std::sync::Arc;

type MemoryStorageInner = DbStorageInner<MemoryStore>;

pub type MemoryStorage<C> = DbStorage<MemoryStore, C>;

#[cfg(any(test, feature = "test"))]
impl MemoryStorage<crate::TestClock> {
    pub async fn make_test_storage(wasm_runtime: Option<WasmRuntime>) -> Self {
        let clock = crate::TestClock::new();
        let max_stream_queries = linera_views::memory::TEST_MEMORY_MAX_STREAM_QUERIES;
        MemoryStorage::new(wasm_runtime, max_stream_queries, clock)
            .await
            .expect("storage")
    }
}

impl<C> MemoryStorage<C> {
    pub async fn new(
        wasm_runtime: Option<WasmRuntime>,
        max_stream_queries: usize,
        clock: C,
    ) -> Result<Self, MemoryContextError> {
        let store_config = MemoryStoreConfig::new(max_stream_queries);
        let namespace = "unused_namespace";
        let storage = MemoryStorageInner::make(store_config, namespace, wasm_runtime).await?;
        Ok(Self {
            client: Arc::new(storage),
            clock,
            execution_runtime_config: ExecutionRuntimeConfig::default(),
        })
    }
}

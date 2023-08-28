// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain_guards::ChainGuards, DbStore, DbStoreClient};
use linera_execution::WasmRuntime;
use linera_views::memory::{
    create_memory_client_stream_queries, MemoryClient, TEST_MEMORY_MAX_STREAM_QUERIES,
};
use std::sync::Arc;

type MemoryStore = DbStore<MemoryClient>;

impl MemoryStore {
    pub fn new(wasm_runtime: Option<WasmRuntime>, max_stream_queries: usize) -> Self {
        let client = create_memory_client_stream_queries(max_stream_queries);
        Self {
            client,
            guards: ChainGuards::default(),
            user_applications: Arc::default(),
            wasm_runtime,
        }
    }
}

pub type MemoryStoreClient = DbStoreClient<MemoryClient>;

impl MemoryStoreClient {
    pub async fn make_test_client(wasm_runtime: Option<WasmRuntime>) -> MemoryStoreClient {
        MemoryStoreClient::new(wasm_runtime, TEST_MEMORY_MAX_STREAM_QUERIES)
    }

    pub fn new(wasm_runtime: Option<WasmRuntime>, max_stream_queries: usize) -> Self {
        Self {
            client: Arc::new(MemoryStore::new(wasm_runtime, max_stream_queries)),
        }
    }
}

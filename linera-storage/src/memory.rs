// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain_guards::ChainGuards, DbStore, DbStoreClient};
use linera_execution::WasmRuntime;
use linera_views::memory::{create_memory_test_client, MemoryClient};
use std::sync::Arc;

type MemoryStore = DbStore<MemoryClient>;

pub type MemoryStoreClient = DbStoreClient<MemoryClient>;

impl MemoryStoreClient {
    pub fn new(wasm_runtime: Option<WasmRuntime>) -> Self {
        Self {
            client: Arc::new(MemoryStore::new(wasm_runtime)),
        }
    }
}

impl MemoryStore {
    pub fn new(wasm_runtime: Option<WasmRuntime>) -> Self {
        let client = create_memory_test_client();
        Self {
            client,
            guards: ChainGuards::default(),
            user_applications: Arc::default(),
            wasm_runtime,
        }
    }
}

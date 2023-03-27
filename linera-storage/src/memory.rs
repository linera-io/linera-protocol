// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    chain_guards::ChainGuards,
};
use async_lock::{Mutex, RwLock};
use linera_execution::WasmRuntime;
use linera_views::{
    memory::MemoryClient,
};
use std::{collections::BTreeMap, sync::Arc};
use crate::DbStoreClient;
use crate::DbStore;

type MemoryStore = DbStore<MemoryClient>;

pub type MemoryStoreClient = DbStoreClient<MemoryClient>;

impl MemoryStoreClient {
    pub async fn new(wasm_runtime: Option<WasmRuntime>) -> Self {
        DbStoreClient { client: Arc::new(MemoryStore::new(wasm_runtime).await) }
    }
}

impl MemoryStore {
    pub async fn new(wasm_runtime: Option<WasmRuntime>) -> Self {
        let state = Arc::new(Mutex::new(BTreeMap::new()));
        let guard = state.lock_arc().await;
        let client = Arc::new(RwLock::new(guard));
        Self {
            client,
            guards: ChainGuards::default(),
            user_applications: Arc::default(),
            wasm_runtime,
        }
    }
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain_guards::ChainGuards, DbStore, DbStoreClient};
use async_lock::{Mutex, RwLock};
use futures::FutureExt;
use linera_execution::WasmRuntime;
use linera_views::memory::MemoryClient;
use std::{collections::BTreeMap, sync::Arc};

type MemoryStore = DbStore<MemoryClient>;

pub type MemoryStoreClient = DbStoreClient<MemoryClient>;

impl MemoryStoreClient {
    pub fn new(wasm_runtime: Option<WasmRuntime>) -> Self {
        DbStoreClient {
            client: Arc::new(MemoryStore::new(wasm_runtime)),
        }
    }
}

impl MemoryStore {
    pub fn new(wasm_runtime: Option<WasmRuntime>) -> Self {
        let state = Arc::new(Mutex::new(BTreeMap::new()));
        let guard = state
            .lock_arc()
            .now_or_never()
            .expect("We should be able to acquire what we just created");
        let client = Arc::new(RwLock::new(guard));
        Self {
            client,
            guards: ChainGuards::default(),
            user_applications: Arc::default(),
            wasm_runtime,
        }
    }
}

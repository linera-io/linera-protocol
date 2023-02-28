// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{ChainRuntimeContext, ChainStateView, Store};
use async_lock::Mutex;
use async_trait::async_trait;
use dashmap::DashMap;
use linera_base::{crypto::CryptoHash, data_types::ChainId};
use linera_chain::data_types::{Certificate, HashedValue, LiteCertificate, Value};
use linera_execution::{UserApplicationCode, UserApplicationId, WasmRuntime};
use linera_views::{
    memory::{MemoryContext, MemoryContextError, MemoryStoreMap},
    views::{View, ViewError},
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone)]
struct MemoryStore {
    chains: DashMap<ChainId, Arc<Mutex<MemoryStoreMap>>>,
    certificates: DashMap<CryptoHash, LiteCertificate>,
    values: DashMap<CryptoHash, Value>,
    user_applications: Arc<DashMap<UserApplicationId, UserApplicationCode>>,
    wasm_runtime: Option<WasmRuntime>,
}

#[derive(Clone)]
pub struct MemoryStoreClient(Arc<MemoryStore>);

impl MemoryStoreClient {
    pub fn new(wasm_runtime: Option<WasmRuntime>) -> Self {
        MemoryStoreClient(Arc::new(MemoryStore::new(wasm_runtime)))
    }
}

impl MemoryStore {
    pub fn new(wasm_runtime: Option<WasmRuntime>) -> Self {
        Self {
            chains: DashMap::new(),
            certificates: DashMap::new(),
            values: DashMap::new(),
            user_applications: Arc::default(),
            wasm_runtime,
        }
    }
}

#[async_trait]
impl Store for MemoryStoreClient {
    type Context = MemoryContext<ChainRuntimeContext<Self>>;
    type ContextError = MemoryContextError;

    async fn load_chain(&self, id: ChainId) -> Result<ChainStateView<Self::Context>, ViewError> {
        let state = self
            .0
            .chains
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(BTreeMap::new())))
            .clone();
        log::trace!("Acquiring lock on {:?}", id);
        let runtime_context = ChainRuntimeContext {
            store: self.clone(),
            chain_id: id,
            user_applications: self.0.user_applications.clone(),
            chain_guard: None,
        };
        let db_context = MemoryContext::new(state.lock_arc().await, runtime_context);
        ChainStateView::load(db_context).await
    }

    async fn read_value(&self, hash: CryptoHash) -> Result<HashedValue, ViewError> {
        Ok(self
            .0
            .values
            .get(&hash)
            .ok_or_else(|| ViewError::not_found("value for hash", hash))?
            .value()
            .clone()
            .with_hash_unchecked(hash))
    }

    async fn write_value(&self, value: HashedValue) -> Result<(), ViewError> {
        self.0.values.insert(value.hash(), value.into());
        Ok(())
    }

    async fn read_certificate(&self, hash: CryptoHash) -> Result<Certificate, ViewError> {
        let maybe_cert = self.0.certificates.get(&hash);
        let cert = maybe_cert.ok_or_else(|| ViewError::not_found("certificate for hash", hash))?;
        let maybe_value = self.0.values.get(&hash);
        let value = maybe_value.ok_or_else(|| ViewError::not_found("value for hash", hash))?;
        let cert = cert.value().clone();
        let value = value.value().clone().with_hash_unchecked(hash);
        Ok(cert
            .with_value(value)
            .ok_or(ViewError::InconsistentEntries)?)
    }

    async fn write_certificate(&self, certificate: Certificate) -> Result<(), ViewError> {
        let (cert, value) = certificate.split();
        self.0.values.insert(cert.value.value_hash, value.into());
        self.0.certificates.insert(cert.value.value_hash, cert);
        Ok(())
    }

    fn wasm_runtime(&self) -> Option<WasmRuntime> {
        self.0.wasm_runtime
    }
}

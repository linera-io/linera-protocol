// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    chain_guards::ChainGuards, ChainRuntimeContext, ChainStateView, Store,
    READ_CERTIFICATE_COUNTER, READ_VALUE_COUNTER, WRITE_CERTIFICATE_COUNTER, WRITE_VALUE_COUNTER,
};
use async_lock::{Mutex, RwLock};
use async_trait::async_trait;
use dashmap::DashMap;
use linera_base::{crypto::CryptoHash, identifiers::ChainId};
use linera_chain::data_types::{Certificate, HashedValue, LiteCertificate, Value};
use linera_execution::{UserApplicationCode, UserApplicationId, WasmRuntime};
use linera_views::{
    batch::Batch,
    common::{ContextFromDb, KeyValueStoreClient},
    memory::{MemoryClient, MemoryContextError},
    views::{View, ViewError},
};
use metrics::increment_counter;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

struct MemoryStore {
    client: MemoryClient,
    guards: ChainGuards,
    user_applications: Arc<DashMap<UserApplicationId, UserApplicationCode>>,
    wasm_runtime: Option<WasmRuntime>,
}
#[derive(Clone)]
pub struct MemoryStoreClient(Arc<MemoryStore>);

impl MemoryStoreClient {
    pub async fn new(wasm_runtime: Option<WasmRuntime>) -> Self {
        MemoryStoreClient(Arc::new(MemoryStore::new(wasm_runtime).await))
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

#[derive(Debug, Serialize, Deserialize)]
enum BaseKey {
    ChainState(ChainId),
    Certificate(CryptoHash),
    Value(CryptoHash),
}

#[async_trait]
impl Store for MemoryStoreClient {
    type Context = ContextFromDb<ChainRuntimeContext<Self>, MemoryClient>;
    type ContextError = MemoryContextError;

    async fn load_chain(&self, id: ChainId) -> Result<ChainStateView<Self::Context>, ViewError> {
        tracing::trace!("Acquiring lock on {:?}", id);
        let guard = self.0.guards.guard(id).await;
        let runtime_context = ChainRuntimeContext {
            store: self.clone(),
            chain_id: id,
            user_applications: self.0.user_applications.clone(),
            chain_guard: Some(Arc::new(guard)),
        };
        let client = self.0.client.clone();
        let base_key = bcs::to_bytes(&BaseKey::ChainState(id))?;
        let context = ContextFromDb::create(client, base_key, runtime_context).await?;
        ChainStateView::load(context).await
    }

    async fn read_value(&self, hash: CryptoHash) -> Result<HashedValue, ViewError> {
        let value_key = bcs::to_bytes(&BaseKey::Value(hash))?;
        let maybe_value: Option<Value> = self.0.client.read_key(&value_key).await?;
        let id = match &maybe_value {
            Some(value) => value.block.chain_id.to_string(),
            None => "not found".to_string(),
        };
        increment_counter!(READ_VALUE_COUNTER, &[("chain_id", id)]);
        let value = maybe_value.ok_or_else(|| ViewError::not_found("value for hash", hash))?;
        Ok(value.with_hash_unchecked(hash))
    }

    async fn write_value(&self, value: HashedValue) -> Result<(), ViewError> {
        let id = value.block().chain_id.to_string();
        increment_counter!(WRITE_VALUE_COUNTER, &[("chain_id", id)]);
        let value_key = bcs::to_bytes(&BaseKey::Value(value.hash()))?;
        let mut batch = Batch::new();
        batch.put_key_value(value_key.to_vec(), &value)?;
        self.0.client.write_batch(batch, &[]).await?;
        Ok(())
    }

    async fn read_certificate(&self, hash: CryptoHash) -> Result<Certificate, ViewError> {
        let cert_key = bcs::to_bytes(&BaseKey::Certificate(hash))?;
        let value_key = bcs::to_bytes(&BaseKey::Value(hash))?;
        let (cert_result, value_result) = tokio::join!(
            self.0.client.read_key::<LiteCertificate>(&cert_key),
            self.0.client.read_key::<Value>(&value_key)
        );
        if let Ok(maybe_value) = &value_result {
            let id = match maybe_value {
                Some(value) => value.block.chain_id.to_string(),
                None => "not found".to_string(),
            };
            increment_counter!(READ_CERTIFICATE_COUNTER, &[("chain_id", id)]);
        };
        let value: Value =
            value_result?.ok_or_else(|| ViewError::not_found("value for hash", hash))?;
        let cert: LiteCertificate =
            cert_result?.ok_or_else(|| ViewError::not_found("certificate for hash", hash))?;
        Ok(cert
            .with_value(value.with_hash_unchecked(hash))
            .ok_or(ViewError::InconsistentEntries)?)
    }

    async fn write_certificate(&self, certificate: Certificate) -> Result<(), ViewError> {
        let id = certificate.value.block().chain_id.to_string();
        increment_counter!(WRITE_CERTIFICATE_COUNTER, &[("chain_id", id)]);
        let hash = certificate.value.hash();
        let cert_key = bcs::to_bytes(&BaseKey::Certificate(hash))?;
        let value_key = bcs::to_bytes(&BaseKey::Value(hash))?;
        let (cert, value) = certificate.split();
        let mut batch = Batch::new();
        batch.put_key_value(cert_key.to_vec(), &cert)?;
        batch.put_key_value(value_key.to_vec(), &value)?;
        self.0.client.write_batch(batch, &[]).await?;
        Ok(())
    }

    fn wasm_runtime(&self) -> Option<WasmRuntime> {
        self.0.wasm_runtime
    }
}

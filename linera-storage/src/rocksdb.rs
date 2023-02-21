// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain_guards::ChainGuards, ChainRuntimeContext, ChainStateView, Store};
use async_trait::async_trait;
use dashmap::DashMap;
use linera_base::{crypto::CryptoHash, data_types::ChainId};
use linera_chain::data_types::{Certificate, LiteCertificate, Value};
use linera_execution::{UserApplicationCode, UserApplicationId, WasmRuntime};
use linera_views::{
    common::{Batch, KeyValueStoreClient},
    rocksdb::{RocksdbClient, RocksdbContext, RocksdbContextError, DB},
    views::{View, ViewError},
};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc};

#[cfg(test)]
#[path = "unit_tests/rocksdb.rs"]
mod tests;

struct RocksdbStore {
    db: RocksdbClient,
    guards: ChainGuards,
    user_applications: Arc<DashMap<UserApplicationId, UserApplicationCode>>,
    #[cfg_attr(not(any(feature = "wasmer", feature = "wasmtime")), allow(dead_code))]
    wasm_runtime: Option<WasmRuntime>,
}

#[derive(Clone)]
pub struct RocksdbStoreClient(Arc<RocksdbStore>);

impl RocksdbStoreClient {
    pub fn new(path: PathBuf, wasm_runtime: Option<WasmRuntime>) -> Self {
        RocksdbStoreClient(Arc::new(RocksdbStore::new(path, wasm_runtime)))
    }
}

impl RocksdbStore {
    pub fn new(dir: PathBuf, wasm_runtime: Option<WasmRuntime>) -> Self {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, dir).unwrap();
        Self {
            db: Arc::new(db),
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
impl Store for RocksdbStoreClient {
    type Context = RocksdbContext<ChainRuntimeContext<Self>>;
    type ContextError = RocksdbContextError;

    async fn load_chain(&self, id: ChainId) -> Result<ChainStateView<Self::Context>, ViewError> {
        let db = self.0.db.clone();
        let base_key = bcs::to_bytes(&BaseKey::ChainState(id))?;
        log::trace!("Acquiring lock on {:?}", id);
        let guard = self.0.guards.guard(id).await;
        let runtime_context = ChainRuntimeContext {
            store: self.clone(),
            chain_id: id,
            user_applications: self.0.user_applications.clone(),
            chain_guard: Some(Arc::new(guard)),
        };
        let db_context = RocksdbContext::new(db, base_key, runtime_context);
        ChainStateView::load(db_context).await
    }

    async fn read_value(&self, hash: CryptoHash) -> Result<Value, ViewError> {
        let value_key = bcs::to_bytes(&BaseKey::Value(hash))?;
        let maybe_value = self.0.db.read_key(&value_key).await?;
        maybe_value.ok_or_else(|| ViewError::NotFound(format!("value for hash {:?}", hash)))
    }

    async fn write_value(&self, value: Value) -> Result<(), ViewError> {
        let value_key = bcs::to_bytes(&BaseKey::Value(value.hash()))?;
        let mut batch = Batch::default();
        batch.put_key_value(value_key.to_vec(), &value)?;
        self.0.db.write_batch(batch).await?;
        Ok(())
    }

    async fn read_certificate(&self, hash: CryptoHash) -> Result<Certificate, ViewError> {
        let cert_key = bcs::to_bytes(&BaseKey::Certificate(hash))?;
        let value_key = bcs::to_bytes(&BaseKey::Value(hash))?;
        let maybe_cert = self.0.db.read_key(&cert_key).await?;
        let cert: LiteCertificate =
            maybe_cert.ok_or_else(|| ViewError::not_found("certificate for hash", hash))?;
        let maybe_value = self.0.db.read_key(&value_key).await?;
        let value: Value =
            maybe_value.ok_or_else(|| ViewError::not_found("value for hash", hash))?;
        Ok(cert
            .clone()
            .with_value(value)
            .ok_or(ViewError::InconsistentEntries)?)
    }

    async fn write_certificate(&self, certificate: Certificate) -> Result<(), ViewError> {
        let hash = certificate.value.hash();
        let cert_key = bcs::to_bytes(&BaseKey::Certificate(hash))?;
        let value_key = bcs::to_bytes(&BaseKey::Value(hash))?;
        let (cert, value) = certificate.split();
        let mut batch = Batch::default();
        batch.put_key_value(cert_key.to_vec(), &cert)?;
        batch.put_key_value(value_key.to_vec(), &value)?;
        self.0.db.write_batch(batch).await?;
        Ok(())
    }

    #[cfg(any(feature = "wasmer", feature = "wasmtime"))]
    fn wasm_runtime(&self) -> WasmRuntime {
        self.0.wasm_runtime.unwrap_or_default()
    }
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain_guards::ChainGuards, ChainRuntimeContext, ChainStateView, Store};
use async_trait::async_trait;
use dashmap::DashMap;
use futures::Future;
use linera_base::{crypto::CryptoHash, data_types::ChainId};
use linera_chain::data_types::{Certificate, HashedValue, LiteCertificate, Value};
use linera_execution::{UserApplicationCode, UserApplicationId, WasmRuntime};
use linera_views::{
    common::{Batch, Context},
    dynamo_db::{
        Config, CreateTableError, DynamoDbContext, DynamoDbContextError, LocalStackError,
        TableName, TableStatus,
    },
    map_view::MapView,
    views::{View, ViewError},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[cfg(test)]
#[path = "unit_tests/dynamo_db.rs"]
mod tests;

struct DynamoDbStore {
    context: DynamoDbContext<()>,
    guards: ChainGuards,
    user_applications: Arc<DashMap<UserApplicationId, UserApplicationCode>>,
    #[cfg_attr(not(any(feature = "wasmer", feature = "wasmtime")), allow(dead_code))]
    wasm_runtime: Option<WasmRuntime>,
}

#[derive(Clone)]
pub struct DynamoDbStoreClient(Arc<DynamoDbStore>);

impl DynamoDbStoreClient {
    pub async fn new(
        table: TableName,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), CreateTableError> {
        Self::with_store(DynamoDbStore::new(table, wasm_runtime)).await
    }

    pub async fn from_config(
        config: impl Into<Config>,
        table: TableName,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), CreateTableError> {
        Self::with_store(DynamoDbStore::from_config(
            config.into(),
            table,
            wasm_runtime,
        ))
        .await
    }

    pub async fn with_localstack(
        table: TableName,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), LocalStackError> {
        Self::with_store(DynamoDbStore::with_localstack(table, wasm_runtime)).await
    }

    async fn with_store<E>(
        store_creator: impl Future<Output = Result<(DynamoDbStore, TableStatus), E>>,
    ) -> Result<(Self, TableStatus), E> {
        let (store, table_status) = store_creator.await?;
        let client = DynamoDbStoreClient(Arc::new(store));
        Ok((client, table_status))
    }
}

impl DynamoDbStore {
    pub async fn new(
        table: TableName,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), CreateTableError> {
        Self::with_context(
            |key_prefix, extra| DynamoDbContext::new(table, key_prefix, extra),
            wasm_runtime,
        )
        .await
    }

    pub async fn from_config(
        config: Config,
        table: TableName,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), CreateTableError> {
        Self::with_context(
            |key_prefix, extra| DynamoDbContext::from_config(config, table, key_prefix, extra),
            wasm_runtime,
        )
        .await
    }

    pub async fn with_localstack(
        table: TableName,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), LocalStackError> {
        Self::with_context(
            |key_prefix, extra| DynamoDbContext::with_localstack(table, key_prefix, extra),
            wasm_runtime,
        )
        .await
    }

    async fn with_context<F, E>(
        create_context: impl FnOnce(Vec<u8>, ()) -> F,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), E>
    where
        F: Future<Output = Result<(DynamoDbContext<()>, TableStatus), E>>,
    {
        let empty_prefix = vec![];
        let (context, table_status) = create_context(empty_prefix, ()).await?;
        Ok((
            Self {
                context,
                guards: ChainGuards::default(),
                user_applications: Arc::new(DashMap::new()),
                wasm_runtime,
            },
            table_status,
        ))
    }

    /// Obtain a [`MapView`] of values.
    async fn values(&self) -> Result<MapView<DynamoDbContext<()>, CryptoHash, Value>, ViewError> {
        MapView::load(self.context.clone_with_sub_scope(&BaseKey::Value, ())?).await
    }

    /// Obtain a [`MapView`] of certificates.
    async fn certificates(
        &self,
    ) -> Result<MapView<DynamoDbContext<()>, CryptoHash, LiteCertificate>, ViewError> {
        MapView::load(
            self.context
                .clone_with_sub_scope(&BaseKey::Certificate, ())?,
        )
        .await
    }
}

/// The key type used to distinguish certificates and chain states.
///
/// Allows selecting a stored sub-view, either a chain state view or the [`MapView`] of
/// certificates.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
enum BaseKey {
    ChainState(ChainId),
    Certificate,
    Value,
}

#[async_trait]
impl Store for DynamoDbStoreClient {
    type Context = DynamoDbContext<ChainRuntimeContext<Self>>;
    type ContextError = DynamoDbContextError;

    async fn load_chain(&self, id: ChainId) -> Result<ChainStateView<Self::Context>, ViewError> {
        log::trace!("Acquiring lock on {:?}", id);
        let guard = self.0.guards.guard(id).await;
        let runtime_context = ChainRuntimeContext {
            store: self.clone(),
            chain_id: id,
            user_applications: self.0.user_applications.clone(),
            chain_guard: Some(Arc::new(guard)),
        };
        let db_context = self
            .0
            .context
            .clone_with_sub_scope(&BaseKey::ChainState(id), runtime_context)?;
        ChainStateView::load(db_context).await
    }

    async fn read_value(&self, hash: CryptoHash) -> Result<HashedValue, ViewError> {
        let values = self.0.values().await?;
        let maybe_value = values.get(&hash).await?;
        let value = maybe_value.ok_or_else(|| ViewError::not_found("value for hash", hash))?;
        Ok(value.with_hash_unchecked(hash))
    }

    async fn write_value(&self, value: HashedValue) -> Result<(), ViewError> {
        let mut values = self.0.values().await?;
        values.insert(&value.hash(), value.into())?;
        let mut batch = Batch::default();
        values.flush(&mut batch)?;
        self.0.context.write_batch(batch).await?;
        Ok(())
    }

    async fn read_certificate(&self, hash: CryptoHash) -> Result<Certificate, ViewError> {
        let (value_result, cert_result) = tokio::join!(
            async move {
                self.0
                    .values()
                    .await?
                    .get(&hash)
                    .await?
                    .ok_or_else(|| ViewError::not_found("value for hash", hash))
            },
            async move {
                self.0
                    .certificates()
                    .await?
                    .get(&hash)
                    .await?
                    .ok_or_else(|| ViewError::not_found("certificate for hash", hash))
            }
        );
        cert_result?
            .with_value(value_result?.with_hash_unchecked(hash))
            .ok_or(ViewError::InconsistentEntries)
    }

    async fn write_certificate(&self, certificate: Certificate) -> Result<(), ViewError> {
        let (cert, value) = certificate.split();
        let hash = value.hash();
        let (certs_result, values_result) = tokio::join!(self.0.certificates(), self.0.values());
        let mut certificates = certs_result?;
        let mut values = values_result?;
        certificates.insert(&hash, cert)?;
        values.insert(&hash, value.into())?;
        let mut batch = Batch::default();
        certificates.flush(&mut batch)?;
        values.flush(&mut batch)?;
        self.0.context.write_batch(batch).await?;
        Ok(())
    }

    #[cfg(any(feature = "wasmer", feature = "wasmtime"))]
    fn wasm_runtime(&self) -> WasmRuntime {
        self.0.wasm_runtime.unwrap_or_default()
    }
}

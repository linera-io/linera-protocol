// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    chain_guards::ChainGuards, ChainRuntimeContext, ChainStateView, Store,
    READ_CERTIFICATE_COUNTER, READ_VALUE_COUNTER, WRITE_CERTIFICATE_COUNTER, WRITE_VALUE_COUNTER,
};
use async_trait::async_trait;
use dashmap::DashMap;
use futures::Future;
use linera_base::{crypto::CryptoHash, data_types::ChainId};
use linera_chain::data_types::{Certificate, HashedValue, LiteCertificate, Value};
use linera_execution::{UserApplicationCode, UserApplicationId, WasmRuntime};
use linera_views::{
    batch::Batch,
    common::Context,
    dynamo_db::{Config, DynamoDbContext, DynamoDbContextError, TableName, TableStatus},
    map_view::MapView,
    views::{View, ViewError},
};
use metrics::increment_counter;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[cfg(test)]
#[path = "unit_tests/dynamo_db.rs"]
mod tests;

struct DynamoDbStore {
    context: DynamoDbContext<()>,
    guards: ChainGuards,
    user_applications: Arc<DashMap<UserApplicationId, UserApplicationCode>>,
    wasm_runtime: Option<WasmRuntime>,
}

#[derive(Clone)]
pub struct DynamoDbStoreClient(Arc<DynamoDbStore>);

impl DynamoDbStoreClient {
    pub async fn new(
        table: TableName,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        Self::with_store(DynamoDbStore::new(table, wasm_runtime)).await
    }

    pub async fn from_config(
        config: impl Into<Config>,
        table: TableName,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
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
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
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
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
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
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        Self::with_context(
            |key_prefix, extra| DynamoDbContext::from_config(config, table, key_prefix, extra),
            wasm_runtime,
        )
        .await
    }

    pub async fn with_localstack(
        table: TableName,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
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
        MapView::load(
            self.context
                .clone_with_sub_scope(&BaseKey::Value, ())
                .await?,
        )
        .await
    }

    /// Obtain a [`MapView`] of certificates.
    async fn certificates(
        &self,
    ) -> Result<MapView<DynamoDbContext<()>, CryptoHash, LiteCertificate>, ViewError> {
        MapView::load(
            self.context
                .clone_with_sub_scope(&BaseKey::Certificate, ())
                .await?,
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
        tracing::trace!("Acquiring lock on {:?}", id);
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
            .clone_with_sub_scope(&BaseKey::ChainState(id), runtime_context)
            .await?;
        ChainStateView::load(db_context).await
    }

    async fn read_value(&self, hash: CryptoHash) -> Result<HashedValue, ViewError> {
        let values = self.0.values().await?;
        let maybe_value = values.get(&hash).await?;
        let id = match &maybe_value {
            Some(value) => format!("{}", value.block.chain_id),
            None => "not found".to_string(),
        };
        increment_counter!(READ_VALUE_COUNTER, &[("chain_id", id)]);
        let value = maybe_value.ok_or_else(|| ViewError::not_found("value for hash", hash))?;
        Ok(value.with_hash_unchecked(hash))
    }

    async fn write_value(&self, value: HashedValue) -> Result<(), ViewError> {
        let id = format!("{}", value.block().chain_id);
        increment_counter!(WRITE_VALUE_COUNTER, &[("chain_id", id)]);
        let mut values = self.0.values().await?;
        values.insert(&value.hash(), value.into())?;
        let mut batch = Batch::new();
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
        if let Some(id) = match &value_result {
            Ok(value) => Some(format!("{}", value.block.chain_id)),
            Err(ViewError::NotFound(_)) => Some("not found".to_string()),
            Err(_) => None,
        } {
            increment_counter!(READ_CERTIFICATE_COUNTER, &[("chain_id", id)]);
        }
        cert_result?
            .with_value(value_result?.with_hash_unchecked(hash))
            .ok_or(ViewError::InconsistentEntries)
    }

    async fn write_certificate(&self, certificate: Certificate) -> Result<(), ViewError> {
        let id = format!("{}", certificate.value.block().chain_id);
        increment_counter!(WRITE_CERTIFICATE_COUNTER, &[("chain_id", id,)]);
        let (cert, value) = certificate.split();
        let hash = value.hash();
        let (certs_result, values_result) = tokio::join!(self.0.certificates(), self.0.values());
        let mut certificates = certs_result?;
        let mut values = values_result?;
        certificates.insert(&hash, cert)?;
        values.insert(&hash, value.into())?;
        let mut batch = Batch::new();
        certificates.flush(&mut batch)?;
        values.flush(&mut batch)?;
        self.0.context.write_batch(batch).await?;
        Ok(())
    }

    fn wasm_runtime(&self) -> Option<WasmRuntime> {
        self.0.wasm_runtime
    }
}

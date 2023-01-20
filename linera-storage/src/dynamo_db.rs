// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain_guards::ChainGuards, ChainRuntimeContext, ChainStateView, Store};
use async_trait::async_trait;
use dashmap::DashMap;
use futures::Future;
use linera_base::{crypto::HashValue, data_types::ChainId};
use linera_chain::data_types::Certificate;
use linera_execution::{UserApplicationCode, UserApplicationId};
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
}

#[derive(Clone)]
pub struct DynamoDbStoreClient(Arc<DynamoDbStore>);

impl DynamoDbStoreClient {
    pub async fn new(table: TableName) -> Result<(Self, TableStatus), CreateTableError> {
        Self::with_store(DynamoDbStore::new(table)).await
    }

    pub async fn from_config(
        config: impl Into<Config>,
        table: TableName,
    ) -> Result<(Self, TableStatus), CreateTableError> {
        let (store, table_status) = DynamoDbStore::from_config(config.into(), table).await?;
        let client = DynamoDbStoreClient(Arc::new(store));
        Ok((client, table_status))
    }

    pub async fn with_localstack(table: TableName) -> Result<(Self, TableStatus), LocalStackError> {
        let (store, table_status) = DynamoDbStore::with_localstack(table).await?;
        let client = DynamoDbStoreClient(Arc::new(store));
        Ok((client, table_status))
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
    pub async fn new(table: TableName) -> Result<(Self, TableStatus), CreateTableError> {
        Self::with_context(|key_prefix, extra| DynamoDbContext::new(table, key_prefix, extra)).await
    }

    pub async fn from_config(
        config: Config,
        table: TableName,
    ) -> Result<(Self, TableStatus), CreateTableError> {
        Self::with_context(|key_prefix, extra| {
            DynamoDbContext::from_config(config, table, key_prefix, extra)
        })
        .await
    }

    pub async fn with_localstack(table: TableName) -> Result<(Self, TableStatus), LocalStackError> {
        Self::with_context(|key_prefix, extra| {
            DynamoDbContext::with_localstack(table, key_prefix, extra)
        })
        .await
    }

    async fn with_context<F, E>(
        create_context: impl FnOnce(Vec<u8>, ()) -> F,
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
            },
            table_status,
        ))
    }

    /// Obtain a [`MapView`] of certificates.
    async fn certificates(
        &self,
    ) -> Result<MapView<DynamoDbContext<()>, HashValue, Certificate>, ViewError> {
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

    async fn read_certificate(&self, hash: HashValue) -> Result<Certificate, ViewError> {
        self.0
            .certificates()
            .await?
            .get(&hash)
            .await?
            .ok_or_else(|| ViewError::NotFound(format!("certificate for hash {:?}", hash)))
    }

    async fn write_certificate(&self, certificate: Certificate) -> Result<(), ViewError> {
        let mut certificates = self.0.certificates().await?;
        let hash = certificate.hash;
        certificates.insert(&hash, certificate)?;
        let mut batch = Batch::default();
        certificates.flush(&mut batch)?;
        self.0.context.write_batch(batch).await?;
        Ok(())
    }
}

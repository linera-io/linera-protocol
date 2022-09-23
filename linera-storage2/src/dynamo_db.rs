// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain::ChainStateView, view::StorageView, Store};
use async_trait::async_trait;
use futures::Future;
use linera_base::{
    crypto::HashValue,
    messages::{Certificate, ChainId},
};
use linera_views::{
    dynamo_db::{self, Config, DynamoDbContext, DynamoDbContextError, TableName, TableStatus},
    views::View,
};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{Mutex, OwnedMutexGuard};

struct DynamoDbStore {
    storage: StorageView<DynamoDbContext>,
}

#[derive(Clone)]
pub struct DynamoDbStoreClient(Arc<Mutex<DynamoDbStore>>);

impl DynamoDbStoreClient {
    pub async fn new(table: TableName) -> Result<(Self, TableStatus), CreateStoreError> {
        Self::with_store(DynamoDbStore::new(table)).await
    }

    pub async fn from_config(
        config: impl Into<Config>,
        table: TableName,
    ) -> Result<(Self, TableStatus), CreateStoreError> {
        let (store, table_status) = DynamoDbStore::from_config(config.into(), table).await?;
        let client = DynamoDbStoreClient(Arc::new(Mutex::new(store)));
        Ok((client, table_status))
    }

    pub async fn with_localstack(table: TableName) -> Result<(Self, TableStatus), LocalStackError> {
        let (store, table_status) = DynamoDbStore::with_localstack(table).await?;
        let client = DynamoDbStoreClient(Arc::new(Mutex::new(store)));
        Ok((client, table_status))
    }

    async fn with_store<E>(
        store_creator: impl Future<Output = Result<(DynamoDbStore, TableStatus), E>>,
    ) -> Result<(Self, TableStatus), E> {
        let (store, table_status) = store_creator.await?;
        let client = DynamoDbStoreClient(Arc::new(Mutex::new(store)));
        Ok((client, table_status))
    }
}

impl DynamoDbStore {
    pub async fn new(table: TableName) -> Result<(Self, TableStatus), CreateStoreError> {
        Self::with_context(|lock, key_prefix| DynamoDbContext::new(table, lock, key_prefix)).await
    }

    pub async fn from_config(
        config: Config,
        table: TableName,
    ) -> Result<(Self, TableStatus), CreateStoreError> {
        Self::with_context(|lock, key_prefix| {
            DynamoDbContext::from_config(config, table, lock, key_prefix)
        })
        .await
    }

    pub async fn with_localstack(table: TableName) -> Result<(Self, TableStatus), LocalStackError> {
        Self::with_context(|lock, key_prefix| {
            DynamoDbContext::with_localstack(table, lock, key_prefix)
        })
        .await
    }

    async fn with_context<F, CreateContextError, StoreError>(
        create_context: impl FnOnce(OwnedMutexGuard<()>, Vec<u8>) -> F,
    ) -> Result<(Self, TableStatus), StoreError>
    where
        F: Future<Output = Result<(DynamoDbContext, TableStatus), CreateContextError>>,
        StoreError: From<DynamoDbContextError> + From<CreateContextError>,
    {
        let dummy_lock = Arc::new(Mutex::new(())).lock_owned().await;
        let empty_prefix = vec![];
        let (context, table_status) = create_context(dummy_lock, empty_prefix).await?;
        let storage = StorageView::load(context).await?;
        Ok((Self { storage }, table_status))
    }
}

#[async_trait]
impl Store for DynamoDbStoreClient {
    type Context = DynamoDbContext;
    type Error = DynamoDbContextError;

    async fn load_chain(
        &self,
        id: ChainId,
    ) -> Result<ChainStateView<Self::Context>, DynamoDbContextError> {
        self.0.lock().await.storage.load_chain(id).await
    }

    async fn read_certificate(&self, hash: HashValue) -> Result<Certificate, DynamoDbContextError> {
        self.0
            .lock()
            .await
            .storage
            .read_certificate(hash)
            .await?
            .ok_or_else(|| {
                DynamoDbContextError::NotFound(format!("certificate for hash {:?}", hash))
            })
    }

    async fn write_certificate(
        &self,
        certificate: Certificate,
    ) -> Result<(), DynamoDbContextError> {
        self.0
            .lock()
            .await
            .storage
            .write_certificate(certificate)
            .await
    }
}

/// Error when creating a [`DynamoDbStore`] instance using a LocalStack instance.
#[derive(Debug, Error)]
pub enum LocalStackError {
    #[error(transparent)]
    Create(#[from] dynamo_db::LocalStackError),

    #[error("Failed to load storage information")]
    Load(#[from] DynamoDbContextError),
}

/// Error when creating a [`DynamoDbStore`] instance.
#[derive(Debug, Error)]
pub enum CreateStoreError {
    #[error(transparent)]
    Create(#[from] dynamo_db::CreateTableError),

    #[error("Failed to load storage information")]
    Load(#[from] DynamoDbContextError),
}

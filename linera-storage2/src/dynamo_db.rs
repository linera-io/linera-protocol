// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::view::StorageView;
use futures::Future;
use linera_views::{
    dynamo_db::{self, Config, DynamoDbContext, DynamoDbContextError, TableName, TableStatus},
    views::View,
};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{Mutex, OwnedMutexGuard};

#[derive(Clone)]
pub struct DynamoDbStoreClient();

impl DynamoDbStoreClient {
    pub async fn new(
        table: TableName,
    ) -> Result<(Arc<Mutex<StorageView<DynamoDbContext>>>, TableStatus), CreateStoreError> {
        Self::with_context(|lock, key_prefix| DynamoDbContext::new(table, lock, key_prefix)).await
    }

    pub async fn from_config(
        config: impl Into<Config>,
        table: TableName,
    ) -> Result<(Arc<Mutex<StorageView<DynamoDbContext>>>, TableStatus), CreateStoreError> {
        Self::with_context(|lock, key_prefix| {
            DynamoDbContext::from_config(config, table, lock, key_prefix)
        })
        .await
    }

    pub async fn with_localstack(
        table: TableName,
    ) -> Result<(Arc<Mutex<StorageView<DynamoDbContext>>>, TableStatus), LocalStackError> {
        Self::with_context(|lock, key_prefix| {
            DynamoDbContext::with_localstack(table, lock, key_prefix)
        })
        .await
    }

    async fn with_context<F, CreateContextError, StoreError>(
        create_context: impl FnOnce(OwnedMutexGuard<()>, Vec<u8>) -> F,
    ) -> Result<(Arc<Mutex<StorageView<DynamoDbContext>>>, TableStatus), StoreError>
    where
        F: Future<Output = Result<(DynamoDbContext, TableStatus), CreateContextError>>,
        StoreError: From<DynamoDbContextError> + From<CreateContextError>,
    {
        let dummy_lock = Arc::new(Mutex::new(())).lock_owned().await;
        let empty_prefix = vec![];
        let (context, table_status) = create_context(dummy_lock, empty_prefix).await?;
        let storage = StorageView::load(context).await?;
        Ok((Arc::new(Mutex::new(storage)), table_status))
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

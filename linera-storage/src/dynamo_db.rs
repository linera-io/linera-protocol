// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{ChainStateView, Store};
use async_trait::async_trait;
use futures::Future;
use linera_base::{
    crypto::HashValue,
    messages::{Certificate, ChainId},
};
use linera_views::{
    dynamo_db::{
        Config, CreateTableError, DynamoDbContext, DynamoDbContextError, LocalStackError,
        TableName, TableStatus,
    },
    views::{MapView, View},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Mutex, OwnedMutexGuard};

struct DynamoDbStore {
    context: DynamoDbContext<()>,
    locks: HashMap<ChainId, Arc<Mutex<()>>>,
}

#[derive(Clone)]
pub struct DynamoDbStoreClient(Arc<Mutex<DynamoDbStore>>);

impl DynamoDbStoreClient {
    pub async fn new(table: TableName) -> Result<(Self, TableStatus), CreateTableError> {
        Self::with_store(DynamoDbStore::new(table)).await
    }

    pub async fn from_config(
        config: impl Into<Config>,
        table: TableName,
    ) -> Result<(Self, TableStatus), CreateTableError> {
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
    pub async fn new(table: TableName) -> Result<(Self, TableStatus), CreateTableError> {
        Self::with_context(|lock, key_prefix, extra| {
            DynamoDbContext::new(table, lock, key_prefix, extra)
        })
        .await
    }

    pub async fn from_config(
        config: Config,
        table: TableName,
    ) -> Result<(Self, TableStatus), CreateTableError> {
        Self::with_context(|lock, key_prefix, extra| {
            DynamoDbContext::from_config(config, table, lock, key_prefix, extra)
        })
        .await
    }

    pub async fn with_localstack(table: TableName) -> Result<(Self, TableStatus), LocalStackError> {
        Self::with_context(|lock, key_prefix, extra| {
            DynamoDbContext::with_localstack(table, lock, key_prefix, extra)
        })
        .await
    }

    async fn with_context<F, E>(
        create_context: impl FnOnce(OwnedMutexGuard<()>, Vec<u8>, ()) -> F,
    ) -> Result<(Self, TableStatus), E>
    where
        F: Future<Output = Result<(DynamoDbContext<()>, TableStatus), E>>,
    {
        let dummy_lock = Arc::new(Mutex::new(())).lock_owned().await;
        let empty_prefix = vec![];
        let (context, table_status) = create_context(dummy_lock, empty_prefix, ()).await?;
        Ok((
            Self {
                context,
                locks: HashMap::new(),
            },
            table_status,
        ))
    }

    /// Obtain a [`MapView`] of certificates.
    async fn certificates(
        &mut self,
    ) -> Result<MapView<DynamoDbContext<()>, HashValue, Certificate>, DynamoDbContextError> {
        let dummy_lock = Arc::new(Mutex::new(())).lock_owned().await;
        MapView::load(
            self.context
                .clone_with_sub_scope(dummy_lock, &BaseKey::Certificate, ()),
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
    type Context = DynamoDbContext<ChainId>;
    type Error = DynamoDbContextError;

    async fn load_chain(
        &self,
        id: ChainId,
    ) -> Result<ChainStateView<Self::Context>, DynamoDbContextError> {
        let lock = self
            .0
            .lock()
            .await
            .locks
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        log::trace!("Acquiring lock on {:?}", id);
        let locked = lock.lock_owned().await;
        let chain_context =
            self.0
                .lock()
                .await
                .context
                .clone_with_sub_scope(locked, &BaseKey::ChainState(id), id);
        ChainStateView::load(chain_context).await
    }

    async fn read_certificate(&self, hash: HashValue) -> Result<Certificate, DynamoDbContextError> {
        let mut store = self.0.lock().await;
        store
            .certificates()
            .await?
            .get(&hash)
            .await?
            .ok_or_else(|| {
                DynamoDbContextError::NotFound(format!("certificate for hash {:?}", hash))
            })
    }

    async fn write_certificate(
        &self,
        certificate: Certificate,
    ) -> Result<(), DynamoDbContextError> {
        let mut store = self.0.lock().await;
        let mut certificates = store.certificates().await?;
        certificates.insert(certificate.hash, certificate);
        certificates.commit(&mut ()).await
    }
}

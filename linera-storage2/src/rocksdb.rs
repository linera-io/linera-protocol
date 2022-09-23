// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain::ChainStateView, view::StorageView, Store};
use async_trait::async_trait;
use linera_base::{
    crypto::HashValue,
    messages::{Certificate, ChainId},
};
use linera_views::{
    rocksdb::{KeyValueOperations, RocksdbContext, RocksdbViewError, DB},
    views::View,
};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::Mutex;

struct RocksdbStore {
    db: Arc<DB>,
    storage: StorageView<RocksdbContext>,
}

#[derive(Clone)]
pub struct RocksdbStoreClient(Arc<Mutex<RocksdbStore>>);

impl RocksdbStoreClient {
    pub async fn new(path: PathBuf) -> Result<Self, RocksdbViewError> {
        Ok(RocksdbStoreClient(Arc::new(Mutex::new(
            RocksdbStore::new(path).await?,
        ))))
    }
}

impl RocksdbStore {
    pub async fn new(dir: PathBuf) -> Result<Self, RocksdbViewError> {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        let db = Arc::new(rocksdb::DB::open(&options, dir).unwrap());
        let dummy_lock = Arc::new(Mutex::new(()));
        let context = RocksdbContext::new(db.clone(), dummy_lock.lock_owned().await, vec![]);
        let storage = StorageView::load(context).await?;
        Ok(Self { db, storage })
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum BaseKey {
    ChainState(ChainId),
    Certificate(HashValue),
}

#[async_trait]
impl Store for RocksdbStoreClient {
    type Context = RocksdbContext;
    type Error = RocksdbViewError;

    async fn load_chain(
        &self,
        id: ChainId,
    ) -> Result<ChainStateView<Self::Context>, RocksdbViewError> {
        self.0.lock().await.storage.load_chain(id).await
    }

    async fn read_certificate(&self, hash: HashValue) -> Result<Certificate, RocksdbViewError> {
        let key = bcs::to_bytes(&BaseKey::Certificate(hash))?;
        let store = self.0.clone();
        let store = store.lock().await;
        let value = store.db.read_key(&key).await?.ok_or_else(|| {
            RocksdbViewError::NotFound(format!("certificate for hash {:?}", hash))
        })?;
        Ok(value)
    }

    async fn write_certificate(&self, certificate: Certificate) -> Result<(), RocksdbViewError> {
        let key = bcs::to_bytes(&BaseKey::Certificate(certificate.hash))?;
        let store = self.0.clone();
        let store = store.lock().await;
        store.db.write_key(&key, &certificate).await
    }
}

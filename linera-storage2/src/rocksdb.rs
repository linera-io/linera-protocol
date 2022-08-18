// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    chain::{ChainStateView, ChainStateViewContext},
    Store,
};
use async_trait::async_trait;
use linera_base::{
    crypto::HashValue,
    messages::{Certificate, ChainId},
};
use linera_views::{
    rocksdb::{KeyValueOperations, RocksdbContext, RocksdbViewError},
    views::View,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::sync::Mutex;

struct RocksdbStore {
    db: Arc<rocksdb::DB>,
    locks: HashMap<ChainId, Arc<Mutex<()>>>,
}

#[derive(Clone)]
pub struct RocksdbStoreClient(Arc<Mutex<RocksdbStore>>);

impl RocksdbStoreClient {
    pub fn new(path: PathBuf) -> Self {
        RocksdbStoreClient(Arc::new(Mutex::new(RocksdbStore::new(path))))
    }
}

impl RocksdbStore {
    pub fn new(dir: PathBuf) -> Self {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        let db = rocksdb::DB::open(&options, dir).unwrap();
        Self {
            db: Arc::new(db),
            locks: HashMap::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum BaseKey {
    ChainState(ChainId),
    Certificate(HashValue),
}

impl ChainStateViewContext for RocksdbContext<ChainId> {}

#[async_trait]
impl Store for RocksdbStoreClient {
    type Context = RocksdbContext<ChainId>;

    async fn load_chain(
        &self,
        id: ChainId,
    ) -> Result<ChainStateView<Self::Context>, RocksdbViewError> {
        let (db, lock) = {
            let store = self.0.clone();
            let mut store = store.lock().await;
            // FIXME: we are never cleaning up locks.
            (
                store.db.clone(),
                store
                    .locks
                    .entry(id)
                    .or_insert_with(|| Arc::new(Mutex::new(())))
                    .clone(),
            )
        };
        log::trace!("Acquiring lock on {:?}", id);
        let base_key = bcs::to_bytes(&BaseKey::ChainState(id))?;
        let context = RocksdbContext::new(db, lock.lock_owned().await, base_key, id);
        ChainStateView::load(context).await
    }

    async fn read_certificate(&self, hash: HashValue) -> Result<Certificate, RocksdbViewError> {
        let store = self.0.clone();
        let store = store.lock().await;
        let key = bcs::to_bytes(&BaseKey::Certificate(hash))?;
        let value = store.db.read_key(&key).await?.ok_or_else(|| {
            RocksdbViewError::NotFound(format!("certificate for hash {:?}", hash))
        })?;
        Ok(value)
    }

    async fn write_certificate(&self, certificate: Certificate) -> Result<(), RocksdbViewError> {
        let store = self.0.clone();
        let store = store.lock().await;
        let key = bcs::to_bytes(&BaseKey::Certificate(certificate.hash))?;
        store.db.write_key(&key, &certificate).await
    }
}

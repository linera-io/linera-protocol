// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{ChainStateView, Store};
use async_trait::async_trait;
use dashmap::DashMap;
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
    locks: DashMap<ChainId, Arc<Mutex<()>>>,
}

#[derive(Clone)]
pub struct RocksdbStoreClient(Arc<RocksdbStore>);

impl RocksdbStoreClient {
    pub fn new(path: PathBuf) -> Self {
        RocksdbStoreClient(Arc::new(RocksdbStore::new(path)))
    }
}

impl RocksdbStore {
    pub fn new(dir: PathBuf) -> Self {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, dir).unwrap();
        Self {
            db: Arc::new(db),
            locks: DashMap::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum BaseKey {
    ChainState(ChainId),
    Certificate(HashValue),
}

#[async_trait]
impl Store for RocksdbStoreClient {
    type Context = RocksdbContext<ChainId>;
    type Error = RocksdbViewError;

    async fn load_chain(
        &self,
        id: ChainId,
    ) -> Result<ChainStateView<Self::Context>, RocksdbViewError> {
        let (db, lock) = {
            let chain_guard = self
                .0
                .locks
                .entry(id)
                .or_insert_with(|| Arc::new(Mutex::new(())));
            // FIXME(#119): we are never cleaning up locks.
            (self.0.db.clone(), chain_guard.clone())
        };
        log::trace!("Acquiring lock on {:?}", id);
        let base_key = bcs::to_bytes(&BaseKey::ChainState(id))?;
        let context = RocksdbContext::new(db, lock.lock_owned().await, base_key, id);
        ChainStateView::load(context).await
    }

    async fn read_certificate(&self, hash: HashValue) -> Result<Certificate, RocksdbViewError> {
        let key = bcs::to_bytes(&BaseKey::Certificate(hash))?;
        let value = self.0.db.read_key(&key).await?.ok_or_else(|| {
            RocksdbViewError::NotFound(format!("certificate for hash {:?}", hash))
        })?;
        Ok(value)
    }

    async fn write_certificate(&self, certificate: Certificate) -> Result<(), RocksdbViewError> {
        let key = bcs::to_bytes(&BaseKey::Certificate(certificate.hash))?;
        self.0.db.write_key(&key, &certificate).await
    }
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain::ChainStateView, Store};
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
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

pub struct RocksdbStore {
    db: Arc<rocksdb::DB>,
    locks: HashMap<ChainId, Arc<Mutex<()>>>,
}

impl RocksdbStore {
    pub fn new(db: rocksdb::DB) -> Self {
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

#[async_trait]
impl Store for RocksdbStore {
    type Context = RocksdbContext<ChainId>;

    async fn load_chain(
        &mut self,
        id: ChainId,
    ) -> Result<ChainStateView<Self::Context>, RocksdbViewError> {
        let lock = self
            .locks
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        log::trace!("Acquiring lock on {:?}", id);
        let base_key = bcs::to_bytes(&BaseKey::ChainState(id))?;
        let context = RocksdbContext::new(self.db.clone(), lock.lock_owned().await, base_key, id);
        ChainStateView::load(context).await
    }

    async fn read_certificate(&mut self, hash: HashValue) -> Result<Certificate, RocksdbViewError> {
        let key = bcs::to_bytes(&BaseKey::Certificate(hash))?;
        let value = self.db.read_key(&key).await?.ok_or_else(|| {
            RocksdbViewError::NotFound(format!("certificate for hash {:?}", hash))
        })?;
        Ok(value)
    }

    async fn write_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<(), RocksdbViewError> {
        let key = bcs::to_bytes(&BaseKey::Certificate(certificate.hash))?;
        self.db.write_key(&key, &certificate).await
    }
}

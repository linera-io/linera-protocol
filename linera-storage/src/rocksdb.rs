// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain_guards::ChainGuards, ChainRuntimeContext, ChainStateView, Store};
use async_trait::async_trait;
use dashmap::DashMap;
use linera_base::{
    crypto::HashValue,
    messages::{ApplicationId, ChainId},
};
use linera_chain::messages::Certificate;
use linera_execution::UserApplicationCode;
use linera_views::{
    rocksdb::{RocksdbContext, RocksdbContextError, RocksdbContainer, DB},
    common::{KeyValueOperations, Batch, put_item_batch},
    views::{View, ViewError},
};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc};

#[cfg(test)]
#[path = "unit_tests/rocksdb.rs"]
mod tests;

struct RocksdbStore {
    db: RocksdbContainer,
    guards: ChainGuards,
    user_applications: Arc<DashMap<ApplicationId, UserApplicationCode>>,
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
            guards: ChainGuards::default(),
            user_applications: Arc::default(),
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
    type Context = RocksdbContext<ChainRuntimeContext>;
    type ContextError = RocksdbContextError;

    async fn load_chain(&self, id: ChainId) -> Result<ChainStateView<Self::Context>, ViewError> {
        let db = self.0.db.clone();
        let base_key = bcs::to_bytes(&BaseKey::ChainState(id))?;
        log::trace!("Acquiring lock on {:?}", id);
        let guard = self.0.guards.guard(id).await;
        let runtime_context = ChainRuntimeContext {
            chain_id: id,
            user_applications: self.0.user_applications.clone(),
            chain_guard: Some(Arc::new(guard)),
        };
        let db_context = RocksdbContext::new(db, base_key, runtime_context);
        ChainStateView::load(db_context).await
    }

    async fn read_certificate(&self, hash: HashValue) -> Result<Certificate, ViewError> {
        let key = bcs::to_bytes(&BaseKey::Certificate(hash))?;
        let value = self
            .0
            .db
            .read_key(&key)
            .await?
            .ok_or_else(|| ViewError::NotFound(format!("certificate for hash {:?}", hash)))?;
        Ok(value)
    }

    async fn write_certificate(&self, certificate: Certificate) -> Result<(), ViewError> {
        let key = bcs::to_bytes(&BaseKey::Certificate(certificate.hash))?;
        let mut batch = Batch::default();
	put_item_batch(&mut batch, key.to_vec(), &certificate)?;
        self.0.db.write_batch(batch).await?;
        Ok(())
    }
}

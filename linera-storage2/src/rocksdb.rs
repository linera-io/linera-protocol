// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain::ChainStateView, Store};
use async_trait::async_trait;
use linera_base::messages::ChainId;
use linera_views::{
    rocksdb::{RocksdbContext, RocksdbViewError},
    views::View,
};
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
            .or_insert_with(|| Arc::new(Mutex::new(())));
        log::trace!("Acquiring lock on {:?}", id);
        let context = RocksdbContext::new(
            self.db.clone(),
            lock.clone().lock_owned().await,
            bcs::to_bytes(&id)?,
            id,
        );
        ChainStateView::load(context).await
    }
}

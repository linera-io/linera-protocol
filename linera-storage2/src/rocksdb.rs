// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::view::StorageView;
use linera_views::{
    rocksdb::{RocksdbContext, RocksdbViewError, DB},
    views::View,
};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct RocksdbStoreClient;

impl RocksdbStoreClient {
    pub async fn new(
        path: PathBuf,
    ) -> Result<Arc<Mutex<StorageView<RocksdbContext>>>, RocksdbViewError> {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        let db = Arc::new(DB::open(&options, path).unwrap());
        let dummy_lock = Arc::new(Mutex::new(()));
        let context = RocksdbContext::new(db, dummy_lock.lock_owned().await, vec![]);
        let storage = StorageView::load(context).await?;
        Ok(Arc::new(Mutex::new(storage)))
    }
}

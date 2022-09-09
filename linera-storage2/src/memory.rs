// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::view::StorageView;
use linera_views::{
    memory::{MemoryContext, MemoryViewError},
    views::View,
};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Mutex;

pub struct MemoryStoreClient;

impl MemoryStoreClient {
    pub async fn new() -> Result<Arc<Mutex<StorageView<MemoryContext>>>, MemoryViewError> {
        let dummy_lock = Arc::new(Mutex::new(BTreeMap::new()));
        let context = MemoryContext::new(dummy_lock.lock_owned().await);
        let storage = StorageView::load(context).await?;
        Ok(Arc::new(Mutex::new(storage)))
    }
}

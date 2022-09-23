// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain::ChainStateView, view::StorageView, Store};
use async_trait::async_trait;
use linera_base::{
    crypto::HashValue,
    messages::{Certificate, ChainId},
};
use linera_views::{
    memory::{MemoryContext, MemoryViewError},
    views::View,
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::sync::Mutex;

struct MemoryStore {
    storage: StorageView<MemoryContext>,
    certificates: HashMap<HashValue, Certificate>,
}

impl MemoryStore {
    pub async fn new() -> Result<Self, MemoryViewError> {
        let dummy_lock = Arc::new(Mutex::new(BTreeMap::new()));
        let context = MemoryContext::new(dummy_lock.lock_owned().await);
        let storage = StorageView::load(context).await?;
        Ok(MemoryStore {
            storage,
            certificates: HashMap::new(),
        })
    }
}

#[derive(Clone)]
pub struct MemoryStoreClient(Arc<Mutex<MemoryStore>>);

impl MemoryStoreClient {
    pub async fn new() -> Result<Self, MemoryViewError> {
        Ok(MemoryStoreClient(Arc::new(Mutex::new(
            MemoryStore::new().await?,
        ))))
    }
}

#[async_trait]
impl Store for MemoryStoreClient {
    type Context = MemoryContext;
    type Error = MemoryViewError;

    async fn load_chain(
        &self,
        id: ChainId,
    ) -> Result<ChainStateView<Self::Context>, MemoryViewError> {
        self.0.lock().await.storage.load_chain(id).await
    }

    async fn read_certificate(&self, hash: HashValue) -> Result<Certificate, MemoryViewError> {
        let store = self.0.clone();
        let store = store.lock().await;
        store
            .certificates
            .get(&hash)
            .cloned()
            .ok_or_else(|| MemoryViewError::NotFound(format!("certificate for hash {:?}", hash)))
    }

    async fn write_certificate(&self, certificate: Certificate) -> Result<(), MemoryViewError> {
        let store = self.0.clone();
        let mut store = store.lock().await;
        store.certificates.insert(certificate.hash, certificate);
        Ok(())
    }
}

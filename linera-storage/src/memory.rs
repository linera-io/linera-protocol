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
    memory::{MemoryContext, MemoryStoreMap, MemoryViewError},
    views::View,
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::sync::Mutex;

#[derive(Default, Clone)]
struct MemoryStore {
    chains: HashMap<ChainId, Arc<Mutex<MemoryStoreMap>>>,
    certificates: DashMap<HashValue, Certificate>,
}

#[derive(Clone, Default)]
pub struct MemoryStoreClient(Arc<Mutex<MemoryStore>>);

#[async_trait]
impl Store for MemoryStoreClient {
    type Context = MemoryContext<ChainId>;
    type Error = MemoryViewError;

    async fn load_chain(
        &self,
        id: ChainId,
    ) -> Result<ChainStateView<Self::Context>, MemoryViewError> {
        let state = {
            let store = self.0.clone();
            let mut store = store.lock().await;
            store
                .chains
                .entry(id)
                .or_insert_with(|| Arc::new(Mutex::new(BTreeMap::new())))
                .clone()
        };
        log::trace!("Acquiring lock on {:?}", id);
        let context = MemoryContext::new(state.lock_owned().await, id);
        ChainStateView::load(context).await
    }

    async fn read_certificate(&self, hash: HashValue) -> Result<Certificate, MemoryViewError> {
        let store = self.0.clone();
        let store = store.lock().await;
        let entry = store
            .certificates
            .get(&hash)
            .ok_or_else(|| MemoryViewError::NotFound(format!("certificate for hash {:?}", hash)))?;
        Ok(entry.value().clone())
    }

    async fn write_certificate(&self, certificate: Certificate) -> Result<(), MemoryViewError> {
        let store = self.0.clone();
        let store = store.lock().await;
        store.certificates.insert(certificate.hash, certificate);
        Ok(())
    }
}

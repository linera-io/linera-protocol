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
    certificates: HashMap<HashValue, Certificate>,
}

#[derive(Clone, Default)]
pub struct MemoryStoreClient(Arc<Mutex<MemoryStore>>);

impl ChainStateViewContext for MemoryContext<ChainId> {}

#[async_trait]
impl Store for MemoryStoreClient {
    type Context = MemoryContext<ChainId>;

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

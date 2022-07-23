// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::Storage;
use async_trait::async_trait;
use linera_base::{
    chain::{ChainState, ChainView},
    crypto::HashValue,
    error::Error,
    messages::{Certificate, ChainId},
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

#[cfg(test)]
use linera_base::{committee::Committee, crypto::PublicKey, manager::BlockManager};

#[cfg(test)]
#[path = "unit_tests/memory_storage_tests.rs"]
mod memory_storage_tests;

/// Vanilla in-memory key-value store.
#[derive(Debug, Clone, Default)]
pub struct InMemoryStore {
    chains: HashMap<ChainId, Arc<ChainState>>,
    certificates: HashMap<HashValue, Certificate>,
}

/// The corresponding vanilla client.
#[derive(Clone, Default)]
pub struct InMemoryStoreClient(Arc<Mutex<InMemoryStore>>);

impl InMemoryStoreClient {
    /// Create a distinct copy of the data.
    pub async fn copy(&self) -> Self {
        let store = self.0.clone();
        let store = store.lock().await;
        let chains = store.chains.clone();
        let certificates = store.certificates.clone();
        let store = InMemoryStore {
            chains,
            certificates,
        };
        Self(Arc::new(Mutex::new(store)))
    }
}

#[async_trait]
impl Storage for InMemoryStoreClient {
    type Base = ChainState;

    async fn read_chain_or_default(&mut self, id: ChainId) -> Result<ChainView<Self::Base>, Error> {
        let store = self.0.clone();
        let chain = store
            .lock()
            .await
            .chains
            .entry(id)
            .or_insert_with(|| Arc::new(ChainState::new(id)))
            .as_ref()
            .clone();
        Ok(chain.into())
    }

    async fn reset_view(&mut self, view: &mut ChainView<Self::Base>) -> Result<(), Error> {
        view.reset();
        Ok(())
    }

    async fn write_chain(&mut self, view: ChainView<Self::Base>) -> Result<(), Error> {
        let store = self.0.clone();
        let state = view.save();
        store
            .lock()
            .await
            .chains
            .insert(state.chain_id(), Arc::new(state));
        Ok(())
    }

    async fn remove_chain(&mut self, id: ChainId) -> Result<(), Error> {
        let store = self.0.clone();
        store.lock().await.chains.remove(&id);
        Ok(())
    }

    #[cfg(any(test, feature = "test"))]
    async fn export_chain_state(&mut self, id: ChainId) -> Result<Option<ChainState>, Error> {
        let store = self.0.clone();
        let chain = store.lock().await.chains.get(&id).map(Arc::as_ref).cloned();
        Ok(chain)
    }

    async fn read_certificate(&mut self, hash: HashValue) -> Result<Certificate, Error> {
        let store = self.0.clone();
        let value = store.lock().await.certificates.get(&hash).cloned();
        value.ok_or(Error::MissingCertificate { hash })
    }

    async fn write_certificate(&mut self, value: Certificate) -> Result<(), Error> {
        let store = self.0.clone();
        store.lock().await.certificates.insert(value.hash, value);
        Ok(())
    }
}

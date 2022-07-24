// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::Storage;
use async_trait::async_trait;
use futures::future::join_all;
use linera_base::{
    chain::{ChainState, ChainView},
    crypto::HashValue,
    error::Error,
    messages::{Certificate, ChainId},
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Mutex, OwnedMutexGuard};

#[cfg(test)]
use linera_base::{committee::Committee, crypto::PublicKey, manager::BlockManager};

#[cfg(test)]
#[path = "unit_tests/memory_storage_tests.rs"]
mod memory_storage_tests;

/// Vanilla in-memory key-value store.
#[derive(Debug, Clone, Default)]
pub struct InMemoryStore {
    chains: HashMap<ChainId, Arc<Mutex<ChainState>>>,
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
        let futures = store
            .chains
            .iter()
            .map(|(id, state)| async { (*id, Arc::new(Mutex::new(state.lock().await.clone()))) });
        let chains = join_all(futures).await.into_iter().collect();
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
    type Base = OwnedMutexGuard<ChainState>;

    async fn read_chain_or_default(&mut self, id: ChainId) -> Result<ChainView<Self::Base>, Error> {
        let store = self.0.clone();
        let mut store = store.lock().await;
        let chain = store
            .chains
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(ChainState::new(id))));
        log::trace!("Acquiring lock on {:?}", id);
        Ok(chain.clone().lock_owned().await.into())
    }

    async fn reset_view(&mut self, view: &mut ChainView<Self::Base>) -> Result<(), Error> {
        view.reset();
        Ok(())
    }

    async fn write_chain(&mut self, view: ChainView<Self::Base>) -> Result<(), Error> {
        view.save();
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
        let store = store.lock().await;
        match store.chains.get(&id) {
            Some(chain) => {
                let state = chain.clone().lock_owned().await.clone();
                Ok(Some(state))
            }
            None => Ok(None),
        }
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

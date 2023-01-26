// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{ChainRuntimeContext, ChainStateView, Store};
use async_trait::async_trait;
use dashmap::DashMap;
use linera_base::{crypto::CryptoHash, data_types::ChainId};
use linera_chain::data_types::Certificate;
use linera_execution::{UserApplicationCode, UserApplicationId};
use linera_views::{
    memory::{MemoryContext, MemoryContextError, MemoryStoreMap},
    views::{View, ViewError},
};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Mutex;

#[derive(Clone, Default)]
struct MemoryStore {
    chains: DashMap<ChainId, Arc<Mutex<MemoryStoreMap>>>,
    certificates: DashMap<CryptoHash, Certificate>,
    user_applications: Arc<DashMap<UserApplicationId, UserApplicationCode>>,
}

#[derive(Clone, Default)]
pub struct MemoryStoreClient(Arc<MemoryStore>);

#[async_trait]
impl Store for MemoryStoreClient {
    type Context = MemoryContext<ChainRuntimeContext<Self>>;
    type ContextError = MemoryContextError;

    async fn load_chain(&self, id: ChainId) -> Result<ChainStateView<Self::Context>, ViewError> {
        let state = self
            .0
            .chains
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(BTreeMap::new())))
            .clone();
        log::trace!("Acquiring lock on {:?}", id);
        let runtime_context = ChainRuntimeContext {
            store: self.clone(),
            chain_id: id,
            user_applications: self.0.user_applications.clone(),
            chain_guard: None,
        };
        let db_context = MemoryContext::new(state.lock_owned().await, runtime_context);
        ChainStateView::load(db_context).await
    }

    async fn read_certificate(&self, hash: CryptoHash) -> Result<Certificate, ViewError> {
        let entry = self
            .0
            .certificates
            .get(&hash)
            .ok_or_else(|| ViewError::NotFound(format!("certificate for hash {:?}", hash)))?;
        Ok(entry.value().clone())
    }

    async fn write_certificate(&self, certificate: Certificate) -> Result<(), ViewError> {
        self.0.certificates.insert(certificate.hash, certificate);
        Ok(())
    }
}

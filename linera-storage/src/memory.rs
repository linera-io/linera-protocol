// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{ChainRuntimeContext, ChainStateView, Store};
use async_trait::async_trait;
use dashmap::DashMap;
use linera_base::{
    crypto::HashValue,
    messages::{ApplicationId, ChainId},
};
use linera_chain::messages::Certificate;
use linera_execution::UserApplicationCode;
use linera_views::{
    memory::{MemoryContext, MemoryStoreMap, MemoryViewError},
    views::View,
};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Mutex;

#[derive(Clone, Default)]
struct MemoryStore {
    chains: DashMap<ChainId, Arc<Mutex<MemoryStoreMap>>>,
    certificates: DashMap<HashValue, Certificate>,
    user_applications: Arc<DashMap<ApplicationId, UserApplicationCode>>,
}

#[derive(Clone, Default)]
pub struct MemoryStoreClient(Arc<MemoryStore>);

#[async_trait]
impl Store for MemoryStoreClient {
    type Context = MemoryContext<ChainRuntimeContext>;
    type Error = MemoryViewError;

    async fn load_chain(
        &self,
        id: ChainId,
    ) -> Result<ChainStateView<Self::Context>, MemoryViewError> {
        let state = self
            .0
            .chains
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(BTreeMap::new())))
            .clone();
        log::trace!("Acquiring lock on {:?}", id);
        let runtime_context = ChainRuntimeContext {
            chain_id: id,
            user_applications: self.0.user_applications.clone(),
            chain_guard: None,
        };
        let db_context = MemoryContext::new(state.lock_owned().await, runtime_context);
        ChainStateView::load(db_context).await
    }

    async fn read_certificate(&self, hash: HashValue) -> Result<Certificate, MemoryViewError> {
        let entry =
            self.0.certificates.get(&hash).ok_or_else(|| {
                MemoryViewError::NotFound(format!("certificate for hash {:?}", hash))
            })?;
        Ok(entry.value().clone())
    }

    async fn write_certificate(&self, certificate: Certificate) -> Result<(), MemoryViewError> {
        self.0.certificates.insert(certificate.hash, certificate);
        Ok(())
    }
}

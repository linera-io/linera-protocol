// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain::ChainStateView, Store};
use async_trait::async_trait;
use linera_base::{
    crypto::HashValue,
    messages::{Certificate, ChainId},
};
use linera_views::{
    memory::{EntryMap, MemoryContext, MemoryViewError},
    views::View,
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::sync::Mutex;

#[derive(Default)]
pub struct MemoryStore {
    states: HashMap<ChainId, Arc<Mutex<EntryMap>>>,
    certificates: HashMap<HashValue, Certificate>,
}

#[async_trait]
impl Store for MemoryStore {
    type Context = MemoryContext<ChainId>;

    async fn load_chain(
        &mut self,
        id: ChainId,
    ) -> Result<ChainStateView<Self::Context>, MemoryViewError> {
        let state = self
            .states
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(BTreeMap::new())));
        log::trace!("Acquiring lock on {:?}", id);
        let context = MemoryContext::new(state.clone().lock_owned().await, id);
        ChainStateView::load(context).await
    }

    async fn read_certificate(&mut self, hash: HashValue) -> Result<Certificate, MemoryViewError> {
        self.certificates
            .get(&hash)
            .cloned()
            .ok_or_else(|| MemoryViewError::NotFound(format!("certificate for hash {:?}", hash)))
    }

    async fn write_certificate(&mut self, certificate: Certificate) -> Result<(), MemoryViewError> {
        let hash = certificate.hash;
        self.certificates.insert(hash, certificate);
        Ok(())
    }
}

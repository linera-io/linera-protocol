// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod chain_guards;
mod dynamo_db;
mod memory;
mod rocksdb;

pub use crate::{
    dynamo_db::DynamoDbStoreClient, memory::MemoryStoreClient, rocksdb::RocksdbStoreClient,
};

use async_trait::async_trait;
use chain_guards::ChainGuard;
use dashmap::DashMap;
use futures::future;
use linera_base::{
    committee::Committee,
    crypto::HashValue,
    ensure,
    messages::{ApplicationId, ChainDescription, ChainId, Epoch, Owner},
};
use linera_chain::{messages::Certificate, ChainStateView, ChainStateViewContext};
use linera_execution::{
    system::Balance, ChainOwnership, ExecutionRuntimeContext, UserApplicationCode,
};
use linera_views::views::ViewError;
use std::{fmt::Debug, sync::Arc};

/// Communicate with a persistent storage using the "views" abstraction.
#[async_trait]
pub trait Store {
    /// The low-level storage implementation in use.
    type Context: ChainStateViewContext<Extra = ChainRuntimeContext, Error = Self::ContextError>;

    /// Alias to provide simpler trait bounds `ViewError: From<Self::ContextError>`
    type ContextError: std::error::Error + Debug + Sync + Send;

    /// Load the view of a chain state.
    async fn load_chain(&self, id: ChainId) -> Result<ChainStateView<Self::Context>, ViewError>;

    /// Read the certificate with the given hash.
    async fn read_certificate(&self, hash: HashValue) -> Result<Certificate, ViewError>;

    /// Write the given certificate.
    async fn write_certificate(&self, certificate: Certificate) -> Result<(), ViewError>;

    /// Load the view of a chain state and check that it is active.
    async fn load_active_chain(
        &self,
        id: ChainId,
    ) -> Result<ChainStateView<Self::Context>, linera_base::error::Error>
    where
        ViewError: From<Self::ContextError>,
    {
        let chain = self.load_chain(id).await?;
        ensure!(
            chain.is_active(),
            linera_base::error::Error::InactiveChain(id)
        );
        Ok(chain)
    }

    /// Read a number of certificates in parallel.
    async fn read_certificates<I: IntoIterator<Item = HashValue> + Send>(
        &self,
        keys: I,
    ) -> Result<Vec<Certificate>, ViewError>
    where
        Self: Clone + Send + 'static,
    {
        let mut tasks = Vec::new();
        for key in keys {
            // TODO: remove clone using scoped threads
            let client = self.clone();
            tasks.push(tokio::task::spawn(async move {
                client.read_certificate(key).await
            }));
        }
        let results = future::join_all(tasks).await;
        let mut certs = Vec::new();
        for result in results {
            certs.push(result.expect("storage access should not cancel or crash")?);
        }
        Ok(certs)
    }

    /// Initialize a chain in a simple way (used for testing and to create a genesis state).
    async fn create_chain(
        &self,
        committee: Committee,
        admin_id: ChainId,
        description: ChainDescription,
        owner: Owner,
        balance: Balance,
    ) -> Result<(), linera_base::error::Error>
    where
        ViewError: From<Self::ContextError>,
    {
        let id = description.into();
        let mut chain = self.load_chain(id).await?;
        ensure!(
            !chain.is_active(),
            linera_base::error::Error::InactiveChain(id)
        );
        let system_state = &mut chain.execution_state.system;
        system_state.description.set(Some(description));
        system_state.epoch.set(Some(Epoch::from(0)));
        system_state.admin_id.set(Some(admin_id));
        system_state
            .committees
            .get_mut()
            .insert(Epoch::from(0), committee);
        system_state.ownership.set(ChainOwnership::single(owner));
        system_state.balance.set(balance);
        let state_hash = chain.execution_state.hash_value().await?;
        chain.execution_state_hash.set(Some(state_hash));
        chain
            .manager
            .get_mut()
            .reset(&ChainOwnership::single(owner));
        chain.save().await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct ChainRuntimeContext {
    pub chain_id: ChainId,
    pub user_applications: Arc<DashMap<ApplicationId, UserApplicationCode>>,
    pub chain_guard: Option<Arc<ChainGuard>>,
}

impl ExecutionRuntimeContext for ChainRuntimeContext {
    fn new(chain_id: ChainId) -> Self {
        Self {
            chain_id,
            user_applications: Arc::default(),
            chain_guard: None,
        }
    }

    fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    fn user_applications(&self) -> &Arc<DashMap<ApplicationId, UserApplicationCode>> {
        &self.user_applications
    }
}

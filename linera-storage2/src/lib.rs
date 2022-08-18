// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod chain;
mod memory;
mod rocksdb;

pub use crate::{memory::MemoryStoreClient, rocksdb::RocksdbStoreClient};

use async_trait::async_trait;
use futures::future;
use linera_base::{
    committee::Committee,
    crypto::HashValue,
    ensure,
    manager::ChainManager,
    messages::{Certificate, ChainDescription, ChainId, Epoch, Owner},
    system::Balance,
};
use linera_views::views::{Context, View};

/// Communicate with a persistent storage using the "views" abstraction.
#[async_trait]
pub trait Store {
    /// The `context` data-type provided by the storage implementation in use.
    type Context: chain::ChainStateViewContext<Extra = ChainId>;

    /// Load the view of a chain state.
    async fn load_chain(
        &self,
        id: ChainId,
    ) -> Result<chain::ChainStateView<Self::Context>, <Self::Context as Context>::Error>;

    async fn read_certificate(
        &self,
        hash: HashValue,
    ) -> Result<Certificate, <Self::Context as Context>::Error>;

    async fn write_certificate(
        &self,
        certificate: Certificate,
    ) -> Result<(), <Self::Context as Context>::Error>;

    /// Load the view of a chain state and check that it is active.
    async fn load_active_chain(
        &self,
        id: ChainId,
    ) -> Result<chain::ChainStateView<Self::Context>, linera_base::error::Error>
    where
        linera_base::error::Error: From<<Self::Context as Context>::Error>,
    {
        let chain = self.load_chain(id).await?;
        ensure!(
            chain.is_active(),
            linera_base::error::Error::InactiveChain(id)
        );
        Ok(chain)
    }

    async fn read_certificates<I: IntoIterator<Item = HashValue> + Send>(
        &self,
        keys: I,
    ) -> Result<Vec<Certificate>, <Self::Context as Context>::Error>
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

    async fn create_chain(
        &self,
        committee: Committee,
        admin_id: ChainId,
        description: ChainDescription,
        owner: Owner,
        balance: Balance,
    ) -> Result<(), <Self::Context as Context>::Error> {
        let mut chain = self.load_chain(description.into()).await?;
        let state = chain.execution_state.get_mut();
        state.system.description = Some(description);
        state.system.epoch = Some(Epoch::from(0));
        state.system.admin_id = Some(admin_id);
        state.system.committees.insert(Epoch::from(0), committee);
        state.system.manager = ChainManager::single(owner);
        state.system.balance = balance;
        chain
            .execution_state_hash
            .set(Some(HashValue::new(&*state)));
        chain.commit().await
    }
}

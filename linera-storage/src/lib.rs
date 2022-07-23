// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod memory_storage;
mod rocksdb_storage;
mod s3_storage;

pub use memory_storage::*;
pub use rocksdb_storage::*;
pub use s3_storage::*;

#[cfg(any(test, feature = "test"))]
pub use s3_storage::s3_storage_tests::LocalStackTestContext;

use async_trait::async_trait;
use futures::future;
use linera_base::{
    chain::{ChainState, ChainView},
    committee::Committee,
    crypto::HashValue,
    ensure,
    error::Error,
    execution::Balance,
    manager::BlockManager,
    messages::{Certificate, ChainDescription, ChainId, Epoch, Owner},
};

/// How to communicate with a persistent storage.
/// * Writes should be blocking until they are completed.
/// * Reads should be optimized to hit a local cache.
#[async_trait]
pub trait Storage {
    type Base;

    async fn read_chain_or_default(
        &mut self,
        chain_id: ChainId,
    ) -> Result<ChainView<Self::Base>, Error>;

    async fn reset_view(&mut self, view: &mut ChainView<Self::Base>) -> Result<(), Error>;

    async fn write_chain(&mut self, view: ChainView<Self::Base>) -> Result<(), Error>;

    async fn remove_chain(&mut self, chain_id: ChainId) -> Result<(), Error>;

    async fn read_certificate(&mut self, hash: HashValue) -> Result<Certificate, Error>;

    async fn read_certificates<I: Iterator<Item = HashValue> + Send>(
        &self,
        keys: I,
    ) -> Result<Vec<Certificate>, Error>
    where
        Self: Clone + Send + 'static,
    {
        let mut tasks = Vec::new();
        for key in keys {
            let mut client = self.clone();
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

    async fn write_certificate(&mut self, certificate: Certificate) -> Result<(), Error>;

    #[cfg(any(test, feature = "test"))]
    async fn export_chain_state(&mut self, id: ChainId) -> Result<Option<ChainState>, Error>;

    async fn read_active_chain(&mut self, id: ChainId) -> Result<ChainView<Self::Base>, Error> {
        let view = self.read_chain_or_default(id).await?;
        ensure!(view.is_active(), Error::InactiveChain(id));
        Ok(view)
    }

    async fn initialize_chain(
        &mut self,
        committee: Committee,
        admin_id: ChainId,
        description: ChainDescription,
        owner: Owner,
        balance: Balance,
    ) -> Result<(), Error>
    where
        Self: Clone + Send + 'static,
        Self::Base: Send,
    {
        let chain_id = description.into();
        let mut view = self.read_chain_or_default(chain_id).await?;
        *view.description_mut() = Some(description);
        view.state_mut().epoch = Some(Epoch::from(0));
        view.state_mut().admin_id = Some(admin_id);
        view.state_mut()
            .committees
            .insert(Epoch::from(0), committee);
        view.state_mut().manager = BlockManager::single(owner);
        view.state_mut().balance = balance;
        *view.state_hash_mut() = HashValue::new(view.state());
        self.write_chain(view).await
    }
}

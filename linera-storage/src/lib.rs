// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod memory_storage;
mod rocksdb_storage;
mod s3_storage;

pub use memory_storage::*;
pub use rocksdb_storage::*;
pub use s3_storage::*;

use async_trait::async_trait;
use futures::future;
use linera_base::{
    chain::ChainState,
    crypto::HashValue,
    ensure,
    error::Error,
    messages::{Certificate, ChainId},
};

/// How to communicate with a persistent storage.
/// * Writes should be blocking until they are completed.
/// * Reads should be optimized to hit a local cache.
#[async_trait]
pub trait Storage {
    async fn read_active_chain(&mut self, id: ChainId) -> Result<ChainState, Error> {
        let chain = self.read_chain_or_default(id).await?;
        ensure!(chain.is_active(), Error::InactiveChain(id));
        Ok(chain)
    }

    async fn read_chain_or_default(&mut self, chain_id: ChainId) -> Result<ChainState, Error>;

    async fn write_chain(&mut self, state: ChainState) -> Result<(), Error>;

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
}

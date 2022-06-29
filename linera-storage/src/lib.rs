// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod file_storage;
mod memory_storage;
mod rocksdb_storage;

pub use file_storage::*;
pub use memory_storage::*;
pub use rocksdb_storage::*;

use async_trait::async_trait;
use dyn_clone::DynClone;
use futures::future;
use std::ops::DerefMut;
use zef_base::{
    chain::ChainState,
    crypto::HashValue,
    ensure,
    error::Error,
    messages::{Certificate, ChainId},
};
use std::ops::DerefMut;

/// How to communicate with a persistent storage.
/// * Writes should be blocking until they are completed.
/// * Reads should be optimized to hit a local cache.
#[async_trait]
pub trait Storage: DynClone + Send + Sync {
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

dyn_clone::clone_trait_object!(Storage);

#[async_trait]
impl Storage for Box<dyn Storage> {
    async fn read_chain_or_default(&mut self, id: ChainId) -> Result<ChainState, Error> {
        self.deref_mut().read_chain_or_default(id).await
    }

    async fn write_chain(&mut self, value: ChainState) -> Result<(), Error> {
        self.deref_mut().write_chain(value).await
    }

    async fn remove_chain(&mut self, id: ChainId) -> Result<(), Error> {
        self.deref_mut().remove_chain(id).await
    }

    async fn read_certificate(&mut self, hash: HashValue) -> Result<Certificate, Error> {
        self.deref_mut().read_certificate(hash).await
    }

    async fn write_certificate(&mut self, value: Certificate) -> Result<(), Error> {
        self.deref_mut().write_certificate(value).await
    }
}

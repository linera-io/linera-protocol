// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod chain;
mod memory;
mod rocksdb;

pub use crate::{memory::MemoryStore, rocksdb::RocksdbStore};

use async_trait::async_trait;
use futures::future;
use linera_base::{
    crypto::HashValue,
    execution::ExecutionState,
    messages::{ApplicationId, BlockHeight, Certificate, ChainId, Origin},
};
use linera_views::{
    hash::HashingContext,
    views::{
        AppendOnlyLogOperations, CollectionOperations, Context, MapOperations, QueueOperations,
        RegisterOperations, ScopedOperations,
    },
};

/// Communicate with a persistent storage using the "views" abstraction.
#[async_trait]
pub trait Store {
    /// The `context` data-type provided by the storage implementation in use.
    type Context: Context<Extra = ChainId>
        + HashingContext
        + Send
        + Sync
        + Clone
        + 'static
        + ScopedOperations
        + RegisterOperations<ExecutionState>
        + RegisterOperations<Option<HashValue>>
        + RegisterOperations<chain::ChainingState>
        + AppendOnlyLogOperations<HashValue>
        + CollectionOperations<ApplicationId>
        + QueueOperations<BlockHeight>
        + RegisterOperations<BlockHeight>
        + QueueOperations<chain::Event>
        + MapOperations<ChainId, ()>
        + CollectionOperations<ChainId>
        + RegisterOperations<Option<BlockHeight>>
        + CollectionOperations<Origin>
        + CollectionOperations<ChainId>
        + CollectionOperations<String>;

    /// Load the view of a chain state.
    async fn load_chain(
        &mut self,
        id: ChainId,
    ) -> Result<chain::ChainStateView<Self::Context>, <Self::Context as Context>::Error>;

    async fn read_certificate(
        &mut self,
        hash: HashValue,
    ) -> Result<Certificate, <Self::Context as Context>::Error>;

    async fn read_certificates<I: Iterator<Item = HashValue> + Send>(
        &self,
        keys: I,
    ) -> Result<Vec<Certificate>, <Self::Context as Context>::Error>
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

    async fn write_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<(), <Self::Context as Context>::Error>;
}

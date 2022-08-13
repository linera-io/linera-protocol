// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod chain;
mod memory;
mod rocksdb;

pub use crate::{memory::MemoryStore, rocksdb::RocksdbStore};

use async_trait::async_trait;
use linera_base::{
    crypto::HashValue,
    execution::ExecutionState,
    messages::{ApplicationId, BlockHeight, ChainId, Origin},
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
}

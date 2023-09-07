// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{aggregator::FullStorageConfig, config::GenesisConfig};
use anyhow::bail;
use async_trait::async_trait;
use linera_execution::WasmRuntime;
use linera_storage::{MemoryStoreClient, Store, WallClock};
use linera_views::views::ViewError;

#[cfg(feature = "rocksdb")]
use linera_storage::RocksDbStoreClient;

#[cfg(feature = "aws")]
use linera_storage::DynamoDbStoreClient;

#[cfg(feature = "scylladb")]
use linera_storage::ScyllaDbStoreClient;

#[async_trait]
pub trait Runnable {
    type Output;

    async fn run<S>(self, storage: S) -> Result<Self::Output, anyhow::Error>
    where
        S: Store + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>;
}

// The design is that the initialization of the accounts should be separate
// from the running of the database.
// However, that does not apply to the memory client which must be initialized
// in the same context in which it is used.
#[allow(unused_variables)]
pub async fn run_with_storage<Job>(
    config: FullStorageConfig,
    genesis_config: &GenesisConfig,
    wasm_runtime: Option<WasmRuntime>,
    job: Job,
) -> Result<Job::Output, anyhow::Error>
where
    Job: Runnable,
{
    match config {
        FullStorageConfig::Memory(kv_config) => {
            let mut client = MemoryStoreClient::new(
                wasm_runtime,
                kv_config.common_config.max_stream_queries,
                WallClock,
            );
            genesis_config.initialize_store(&mut client).await?;
            job.run(client).await
        }
        #[cfg(feature = "rocksdb")]
        FullStorageConfig::RocksDb(kv_config) => {
            let (client, table_status) = RocksDbStoreClient::new(kv_config, wasm_runtime).await?;
            job.run(client).await
        }
        #[cfg(feature = "aws")]
        FullStorageConfig::DynamoDb(kv_config) => {
            let (client, table_status) = DynamoDbStoreClient::new(kv_config, wasm_runtime).await?;
            job.run(client).await
        }
        #[cfg(feature = "scylladb")]
        FullStorageConfig::ScyllaDb(kv_config) => {
            let (client, table_status) = ScyllaDbStoreClient::new(kv_config, wasm_runtime).await?;
            job.run(client).await
        }
    }
}

#[allow(unused_variables)]
pub async fn full_initialize_storage(
    config: FullStorageConfig,
    genesis_config: &GenesisConfig,
) -> Result<(), anyhow::Error> {
    match config {
        FullStorageConfig::Memory(_) => {
            bail!("The initialization should not be called for memory");
        }
        #[cfg(feature = "rocksdb")]
        FullStorageConfig::RocksDb(kv_config) => {
            let wasm_runtime = None;
            let mut client = RocksDbStoreClient::initialize(kv_config, wasm_runtime).await?;
            genesis_config.initialize_store(&mut client).await
        }
        #[cfg(feature = "aws")]
        FullStorageConfig::DynamoDb(kv_config) => {
            let wasm_runtime = None;
            let mut client = DynamoDbStoreClient::initialize(kv_config, wasm_runtime).await?;
            genesis_config.initialize_store(&mut client).await
        }
        #[cfg(feature = "scylladb")]
        FullStorageConfig::ScyllaDb(kv_config) => {
            let wasm_runtime = None;
            let mut client = ScyllaDbStoreClient::initialize(kv_config, wasm_runtime).await?;
            genesis_config.initialize_store(&mut client).await
        }
    }
}

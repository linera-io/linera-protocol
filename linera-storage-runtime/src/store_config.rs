// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use anyhow::anyhow;
use async_trait::async_trait;
use linera_client::config::GenesisConfig;
use linera_execution::WasmRuntime;
use linera_storage::{DbStorage, Storage, StorageCacheConfig};
#[cfg(feature = "storage-service")]
use linera_storage_service::client::StorageServiceDatabase;
#[cfg(feature = "dynamodb")]
use linera_views::dynamo_db::DynamoDbDatabase;
#[cfg(feature = "rocksdb")]
use linera_views::rocks_db::RocksDbDatabase;
#[cfg(feature = "scylladb")]
use linera_views::scylla_db::ScyllaDbDatabase;
use linera_views::{
    memory::MemoryDatabase,
    store::{KeyValueDatabase, KeyValueStore},
};
use serde::{Deserialize, Serialize};
#[cfg(all(feature = "rocksdb", feature = "scylladb"))]
use {linera_storage::ChainStatesFirstAssignment, linera_views::backends::dual::DualDatabase};

/// The configuration of the key value store in use.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum StoreConfig {
    /// The memory key value store
    Memory {
        config: linera_views::memory::MemoryStoreConfig,
        namespace: String,
        genesis_path: PathBuf,
    },
    /// The storage service key-value store
    #[cfg(feature = "storage-service")]
    StorageService {
        config: linera_storage_service::common::StorageServiceStoreConfig,
        namespace: String,
    },
    /// The RocksDB key value store
    #[cfg(feature = "rocksdb")]
    RocksDb {
        config: linera_views::rocks_db::RocksDbStoreConfig,
        namespace: String,
    },
    /// The DynamoDB key value store
    #[cfg(feature = "dynamodb")]
    DynamoDb {
        config: linera_views::dynamo_db::DynamoDbStoreConfig,
        namespace: String,
    },
    /// The ScyllaDB key value store
    #[cfg(feature = "scylladb")]
    ScyllaDb {
        config: linera_views::scylla_db::ScyllaDbStoreConfig,
        namespace: String,
    },
    #[cfg(all(feature = "rocksdb", feature = "scylladb"))]
    DualRocksDbScyllaDb {
        config: linera_views::backends::dual::DualStoreConfig<
            linera_views::rocks_db::RocksDbStoreConfig,
            linera_views::scylla_db::ScyllaDbStoreConfig,
        >,
        namespace: String,
    },
}

#[async_trait]
pub trait Runnable {
    type Output;

    async fn run<S>(self, storage: S) -> Self::Output
    where
        S: Storage + Clone + Send + Sync + 'static;
}

#[async_trait]
pub trait RunnableWithStore {
    type Output;

    async fn run<D>(
        self,
        config: D::Config,
        namespace: String,
        cache_sizes: StorageCacheConfig,
    ) -> Result<Self::Output, anyhow::Error>
    where
        D: KeyValueDatabase + Clone + Send + Sync + 'static,
        D::Store: KeyValueStore + Clone + Send + Sync + 'static,
        D::Error: Send + Sync;
}

/// Reads a JSON value from a file at the given path.
fn read_json<T: serde::de::DeserializeOwned>(path: impl Into<PathBuf>) -> anyhow::Result<T> {
    Ok(serde_json::from_reader(fs_err::File::open(path.into())?)?)
}

impl StoreConfig {
    pub async fn run_with_storage<Job>(
        self,
        wasm_runtime: Option<WasmRuntime>,
        allow_application_logs: bool,
        cache_sizes: StorageCacheConfig,
        job: Job,
    ) -> Result<Job::Output, anyhow::Error>
    where
        Job: Runnable,
    {
        match self {
            StoreConfig::Memory {
                config,
                namespace,
                genesis_path,
            } => {
                let mut storage = DbStorage::<MemoryDatabase, _>::maybe_create_and_connect(
                    &config,
                    &namespace,
                    wasm_runtime,
                    cache_sizes,
                )
                .await?
                .with_allow_application_logs(allow_application_logs);
                let genesis_config = read_json::<GenesisConfig>(genesis_path)?;
                // Memory storage must be initialized every time.
                genesis_config.initialize_storage(&mut storage).await?;
                Ok(job.run(storage).await)
            }
            #[cfg(feature = "storage-service")]
            StoreConfig::StorageService { config, namespace } => {
                let storage = DbStorage::<StorageServiceDatabase, _>::connect(
                    &config,
                    &namespace,
                    wasm_runtime,
                    cache_sizes,
                )
                .await?
                .with_allow_application_logs(allow_application_logs);
                Ok(job.run(storage).await)
            }
            #[cfg(feature = "rocksdb")]
            StoreConfig::RocksDb { config, namespace } => {
                let storage = DbStorage::<RocksDbDatabase, _>::connect(
                    &config,
                    &namespace,
                    wasm_runtime,
                    cache_sizes,
                )
                .await?
                .with_allow_application_logs(allow_application_logs);
                Ok(job.run(storage).await)
            }
            #[cfg(feature = "dynamodb")]
            StoreConfig::DynamoDb { config, namespace } => {
                let storage = DbStorage::<DynamoDbDatabase, _>::connect(
                    &config,
                    &namespace,
                    wasm_runtime,
                    cache_sizes,
                )
                .await?
                .with_allow_application_logs(allow_application_logs);
                Ok(job.run(storage).await)
            }
            #[cfg(feature = "scylladb")]
            StoreConfig::ScyllaDb { config, namespace } => {
                let storage = DbStorage::<ScyllaDbDatabase, _>::connect(
                    &config,
                    &namespace,
                    wasm_runtime,
                    cache_sizes,
                )
                .await?
                .with_allow_application_logs(allow_application_logs);
                Ok(job.run(storage).await)
            }
            #[cfg(all(feature = "rocksdb", feature = "scylladb"))]
            StoreConfig::DualRocksDbScyllaDb { config, namespace } => {
                let storage =
                    DbStorage::<
                        DualDatabase<RocksDbDatabase, ScyllaDbDatabase, ChainStatesFirstAssignment>,
                        _,
                    >::connect(&config, &namespace, wasm_runtime, cache_sizes)
                    .await?
                    .with_allow_application_logs(allow_application_logs);
                Ok(job.run(storage).await)
            }
        }
    }

    #[allow(unused_variables)]
    pub async fn run_with_store<Job>(
        self,
        cache_sizes: StorageCacheConfig,
        job: Job,
    ) -> Result<Job::Output, anyhow::Error>
    where
        Job: RunnableWithStore,
    {
        match self {
            StoreConfig::Memory { .. } => {
                Err(anyhow!("Cannot run admin operations on the memory store"))
            }
            #[cfg(feature = "storage-service")]
            StoreConfig::StorageService { config, namespace } => Ok(job
                .run::<StorageServiceDatabase>(config, namespace, cache_sizes)
                .await?),
            #[cfg(feature = "rocksdb")]
            StoreConfig::RocksDb { config, namespace } => Ok(job
                .run::<RocksDbDatabase>(config, namespace, cache_sizes)
                .await?),
            #[cfg(feature = "dynamodb")]
            StoreConfig::DynamoDb { config, namespace } => Ok(job
                .run::<DynamoDbDatabase>(config, namespace, cache_sizes)
                .await?),
            #[cfg(feature = "scylladb")]
            StoreConfig::ScyllaDb { config, namespace } => Ok(job
                .run::<ScyllaDbDatabase>(config, namespace, cache_sizes)
                .await?),
            #[cfg(all(feature = "rocksdb", feature = "scylladb"))]
            StoreConfig::DualRocksDbScyllaDb { config, namespace } => Ok(job
                .run::<DualDatabase<RocksDbDatabase, ScyllaDbDatabase, ChainStatesFirstAssignment>>(
                    config,
                    namespace,
                    cache_sizes,
                )
                .await?),
        }
    }
}

pub struct StorageMigration;

#[async_trait]
impl RunnableWithStore for StorageMigration {
    type Output = ();

    async fn run<D>(
        self,
        _config: D::Config,
        _namespace: String,
        _cache_sizes: StorageCacheConfig,
    ) -> Result<Self::Output, anyhow::Error>
    where
        D: KeyValueDatabase + Clone + Send + Sync + 'static,
        D::Store: KeyValueStore + Clone + Send + Sync + 'static,
        D::Error: Send + Sync,
    {
        // Storage migration is not yet available on main.
        Ok(())
    }
}

pub struct AssertStorageV1;

#[async_trait]
impl RunnableWithStore for AssertStorageV1 {
    type Output = ();

    async fn run<D>(
        self,
        _config: D::Config,
        _namespace: String,
        _cache_sizes: StorageCacheConfig,
    ) -> Result<Self::Output, anyhow::Error>
    where
        D: KeyValueDatabase + Clone + Send + Sync + 'static,
        D::Store: KeyValueStore + Clone + Send + Sync + 'static,
        D::Error: Send + Sync,
    {
        // Storage migration assertion is not yet available on main.
        Ok(())
    }
}

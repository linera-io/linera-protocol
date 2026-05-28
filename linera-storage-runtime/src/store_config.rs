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
        /// The store configuration.
        config: linera_views::memory::MemoryStoreConfig,
        /// The namespace used.
        namespace: String,
        /// The path to the genesis configuration used to initialize the store.
        genesis_path: PathBuf,
    },
    /// The storage service key-value store
    #[cfg(feature = "storage-service")]
    StorageService {
        /// The store configuration.
        config: linera_storage_service::common::StorageServiceStoreConfig,
        /// The namespace used.
        namespace: String,
    },
    /// The RocksDB key value store
    #[cfg(feature = "rocksdb")]
    RocksDb {
        /// The store configuration.
        config: linera_views::rocks_db::RocksDbStoreConfig,
        /// The namespace used.
        namespace: String,
    },
    /// The ScyllaDB key value store
    #[cfg(feature = "scylladb")]
    ScyllaDb {
        /// The store configuration.
        config: linera_views::scylla_db::ScyllaDbStoreConfig,
        /// The namespace used.
        namespace: String,
    },
    /// The dual RocksDB / ScyllaDB key value store
    #[cfg(all(feature = "rocksdb", feature = "scylladb"))]
    DualRocksDbScyllaDb {
        /// The store configuration.
        config: linera_views::backends::dual::DualStoreConfig<
            linera_views::rocks_db::RocksDbStoreConfig,
            linera_views::scylla_db::ScyllaDbStoreConfig,
        >,
        /// The namespace used.
        namespace: String,
    },
}

/// A job that can be run against a high-level [`Storage`].
#[async_trait]
pub trait Runnable {
    /// The type produced by running the job.
    type Output;

    /// Runs the job against the given `storage`.
    async fn run<S>(self, storage: S) -> Self::Output
    where
        S: Storage + Clone + Send + Sync + 'static;
}

/// A job that can be run against a low-level key-value store.
#[async_trait]
pub trait RunnableWithStore {
    /// The type produced by running the job.
    type Output;

    /// Runs the job against a store built from the given configuration.
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
    /// Connects to the configured storage and runs the given [`Runnable`] job against it.
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

    /// Connects to the configured key-value store and runs the given
    /// [`RunnableWithStore`] job against it.
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

/// A [`RunnableWithStore`] job that migrates the storage schema.
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

/// A [`RunnableWithStore`] job that asserts the storage is at schema version 1.
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

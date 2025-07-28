// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use clap::Parser as _;
use linera_views::{
    lru_caching::StorageCacheConfig,
    rocks_db::{
        PathWithGuard, RocksDbDatabase, RocksDbSpawnMode, RocksDbStoreConfig,
        RocksDbStoreInternalConfig,
    },
    store::KeyValueDatabase as _,
};

use crate::{
    common::IndexerError,
    runner::{IndexerConfig, Runner},
};

#[derive(clap::Parser, Clone, Debug)]
#[command(version = linera_version::VersionInfo::default_clap_str())]
pub struct RocksDbConfig {
    /// RocksDB storage path
    #[arg(long, default_value = "./indexer.db")]
    pub storage: PathBuf,

    #[arg(long, default_value = "linera")]
    pub namespace: String,

    /// The maximal number of simultaneous queries to the database
    #[arg(long)]
    max_concurrent_queries: Option<usize>,

    /// The maximal number of simultaneous stream queries to the database
    #[arg(long, default_value = "10")]
    pub max_stream_queries: usize,

    /// The maximal memory used in the storage cache.
    #[arg(long, default_value = "10000000")]
    pub max_cache_size: usize,

    /// The maximal size of an entry in the storage cache.
    #[arg(long, default_value = "1000000")]
    pub max_entry_size: usize,

    /// The maximal number of entries in the storage cache.
    #[arg(long, default_value = "1000")]
    pub max_cache_entries: usize,
}

pub type RocksDbRunner = Runner<RocksDbDatabase, RocksDbConfig>;

impl RocksDbRunner {
    pub async fn load() -> Result<Self, IndexerError> {
        let config = IndexerConfig::<RocksDbConfig>::parse();
        let storage_cache_config = StorageCacheConfig {
            max_cache_size: config.client.max_cache_size,
            max_entry_size: config.client.max_entry_size,
            max_cache_entries: config.client.max_cache_entries,
        };
        let path_buf = config.client.storage.as_path().to_path_buf();
        let path_with_guard = PathWithGuard::new(path_buf);
        // The tests are run in single threaded mode, therefore we need
        // to use the safe default value of SpawnBlocking.
        let spawn_mode = RocksDbSpawnMode::SpawnBlocking;
        let inner_config = RocksDbStoreInternalConfig {
            spawn_mode,
            path_with_guard,
            max_stream_queries: config.client.max_stream_queries,
        };
        let store_config = RocksDbStoreConfig {
            inner_config,
            storage_cache_config,
        };
        let namespace = config.client.namespace.clone();
        let database = RocksDbDatabase::maybe_create_and_connect(&store_config, &namespace).await?;
        Self::new(config, database).await
    }
}

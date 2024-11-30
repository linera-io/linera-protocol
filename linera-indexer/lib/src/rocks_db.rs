// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use clap::Parser as _;
use linera_views::{
    rocks_db::{PathWithGuard, RocksDbSpawnMode, RocksDbStore, RocksDbStoreConfig},
    store::{AdminKeyValueStore, CommonStoreConfig},
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
    /// The maximal number of entries in the storage cache.
    #[arg(long, default_value = "1000")]
    cache_size: usize,
}

pub type RocksDbRunner = Runner<RocksDbStore, RocksDbConfig>;

impl RocksDbRunner {
    pub async fn load() -> Result<Self, IndexerError> {
        let config = IndexerConfig::<RocksDbConfig>::parse();
        let common_config = CommonStoreConfig {
            max_concurrent_queries: config.client.max_concurrent_queries,
            max_stream_queries: config.client.max_stream_queries,
            cache_size: config.client.cache_size,
        };
        let path_buf = config.client.storage.as_path().to_path_buf();
        let path_with_guard = PathWithGuard::new(path_buf);
        // The tests are run in single threaded mode, therefore we need
        // to use the safe default value of SpawnBlocking.
        let spawn_mode = RocksDbSpawnMode::SpawnBlocking;
        let store_config = RocksDbStoreConfig::new(spawn_mode, path_with_guard, common_config);
        let namespace = config.client.namespace.clone();
        let root_key = &[];
        let store =
            RocksDbStore::maybe_create_and_connect(&store_config, &namespace, root_key).await?;
        Self::new(config, store).await
    }
}

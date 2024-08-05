// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use clap::Parser as _;
use linera_views::{
    common::{AdminKeyValueStore, CommonStoreConfig},
    rocks_db::{RocksDbStore, RocksDbStoreConfig},
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
    pub table: String,
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
        let store_config = RocksDbStoreConfig {
            path_buf: config.client.storage.as_path().to_path_buf(),
            common_config,
        };
        let namespace = config.client.table.clone();
        let root_key = &[];
        let store =
            RocksDbStore::maybe_create_and_connect(&store_config, &namespace, root_key).await?;
        Self::new(config, store).await
    }
}

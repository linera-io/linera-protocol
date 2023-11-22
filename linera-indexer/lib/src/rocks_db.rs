// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    common::IndexerError,
    runner::{IndexerConfig, Runner},
};
use linera_views::{
    common::CommonStoreConfig,
    rocks_db::{RocksDbClient, RocksDbStoreConfig},
};
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(StructOpt, Clone, Debug)]
pub struct RocksDbConfig {
    /// RocksDB storage path
    #[structopt(long, default_value = "./indexer.db")]
    pub storage: PathBuf,
    /// The maximal number of simultaneous queries to the database
    #[structopt(long)]
    max_concurrent_queries: Option<usize>,
    /// The maximal number of simultaneous stream queries to the database
    #[structopt(long, default_value = "10")]
    pub max_stream_queries: usize,
    /// The maximal number of entries in the storage cache.
    #[structopt(long, default_value = "1000")]
    cache_size: usize,
}

pub type RocksDbRunner = Runner<RocksDbClient, RocksDbConfig>;

impl RocksDbRunner {
    pub async fn load() -> Result<Self, IndexerError> {
        let config = IndexerConfig::<RocksDbConfig>::from_args();
        let common_config = CommonStoreConfig {
            max_concurrent_queries: config.client.max_concurrent_queries,
            max_stream_queries: config.client.max_stream_queries,
            cache_size: config.client.cache_size,
        };
        let store_config = RocksDbStoreConfig {
            path_buf: config.client.storage.as_path().to_path_buf(),
            common_config,
        };
        let store = RocksDbClient::initialize(store_config).await?;
        Self::new(config, store).await
    }
}

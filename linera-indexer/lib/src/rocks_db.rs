// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    common::IndexerError,
    runner::{IndexerConfig, Runner},
};
use linera_views::{common::CommonStoreConfig, rocks_db::RocksDbClient};
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
    /// Do not create a table if one is missing
    #[structopt(long = "do not create a database if missing")]
    skip_table_creation_when_missing: bool,
}

pub type RocksDbRunner = Runner<RocksDbClient, RocksDbConfig>;

impl RocksDbRunner {
    pub async fn load() -> Result<Self, IndexerError> {
        let config = IndexerConfig::<RocksDbConfig>::from_args();
        let (client, _) = RocksDbClient::new(
            &config.client.storage,
            CommonStoreConfig {
                max_concurrent_queries: config.client.max_concurrent_queries,
                max_stream_queries: config.client.max_stream_queries,
                cache_size: config.client.cache_size,
                create_if_missing: !config.client.skip_table_creation_when_missing,
            },
        )
        .await?;
        Self::new(config, client).await
    }
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    common::IndexerError,
    runner::{IndexerConfig, Runner},
};
use linera_views::{
    common::CommonStoreConfig,
    scylla_db::{ScyllaDbStore, ScyllaDbStoreConfig},
};

#[derive(clap::Parser, Clone, Debug)]
#[command(version = linera_base::VersionInfo::default_str())]
pub struct ScyllaDbConfig {
    /// ScyllaDB address
    #[arg(long, default_value = "localhost:9042")]
    pub uri: String,
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

pub type ScyllaDbRunner = Runner<ScyllaDbStore, ScyllaDbConfig>;

impl ScyllaDbRunner {
    pub async fn load() -> Result<Self, IndexerError> {
        let config = <IndexerConfig<ScyllaDbConfig> as clap::Parser>::parse();
        let common_config = CommonStoreConfig {
            max_concurrent_queries: config.client.max_concurrent_queries,
            max_stream_queries: config.client.max_stream_queries,
            cache_size: config.client.cache_size,
        };
        let store_config = ScyllaDbStoreConfig {
            uri: config.client.uri.clone(),
            table_name: config.client.table.clone(),
            common_config,
        };
        let (store, _) = ScyllaDbStore::new(store_config).await?;
        Self::new(config, store).await
    }
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::{
    lru_caching::StorageCacheConfig,
    scylla_db::{ScyllaDbStore, ScyllaDbStoreConfig},
    store::{AdminKeyValueStore, CommonStoreConfig},
};

use crate::{
    common::IndexerError,
    runner::{IndexerConfig, Runner},
};

#[derive(clap::Parser, Clone, Debug)]
#[command(version = linera_version::VersionInfo::default_clap_str())]
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

pub type ScyllaDbRunner = Runner<ScyllaDbStore, ScyllaDbConfig>;

impl ScyllaDbRunner {
    pub async fn load() -> Result<Self, IndexerError> {
        let config = <IndexerConfig<ScyllaDbConfig> as clap::Parser>::parse();
        let storage_cache_config = StorageCacheConfig {
            max_cache_size: config.client.max_cache_size,
            max_entry_size: config.client.max_entry_size,
            max_cache_entries: config.client.max_cache_entries,
        };
        let common_config = CommonStoreConfig {
            max_concurrent_queries: config.client.max_concurrent_queries,
            max_stream_queries: config.client.max_stream_queries,
            storage_cache_config,
        };
        let namespace = config.client.table.clone();
        let store_config = ScyllaDbStoreConfig::new(config.client.uri.clone(), common_config);
        let store = ScyllaDbStore::connect(&store_config, &namespace).await?;
        Self::new(config, store).await
    }
}

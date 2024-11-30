// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::{
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
        let namespace = config.client.table.clone();
        let root_key = &[];
        let store_config = ScyllaDbStoreConfig::new(config.client.uri.clone(), common_config);
        let store = ScyllaDbStore::connect(&store_config, &namespace, root_key).await?;
        Self::new(config, store).await
    }
}

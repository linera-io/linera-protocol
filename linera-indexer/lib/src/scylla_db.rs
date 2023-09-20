// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    common::IndexerError,
    runner::{IndexerConfig, Runner},
};
use linera_views::{
    common::CommonStoreConfig,
    scylla_db::{ScyllaDbClient, ScyllaDbKvStoreConfig},
};
use structopt::StructOpt;

#[derive(StructOpt, Clone, Debug)]
pub struct ScyllaDbConfig {
    /// ScyllaDB address
    #[structopt(long, default_value = "localhost:9042")]
    pub uri: String,
    #[structopt(long, default_value = "linera")]
    pub table: String,
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

pub type ScyllaDbRunner = Runner<ScyllaDbClient, ScyllaDbConfig>;

impl ScyllaDbRunner {
    pub async fn load() -> Result<Self, IndexerError> {
        let config = IndexerConfig::<ScyllaDbConfig>::from_args();
        let common_config = CommonStoreConfig {
            max_concurrent_queries: config.client.max_concurrent_queries,
            max_stream_queries: config.client.max_stream_queries,
            cache_size: config.client.cache_size,
        };
        let store_config = ScyllaDbKvStoreConfig {
            uri: config.client.uri.clone(),
            table_name: config.client.table.clone(),
            common_config,
        };
        let (store, _) = ScyllaDbClient::new(store_config).await?;
        Self::new(config, store).await
    }
}

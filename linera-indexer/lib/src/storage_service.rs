// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use clap::Parser as _;
use linera_storage_service::{client::ServiceStoreClient, common::ServiceStoreConfig};
use linera_views::store::{AdminKeyValueStore, CommonStoreConfig};

use crate::{
    common::IndexerError,
    runner::{IndexerConfig, Runner},
};

#[derive(clap::Parser, Clone, Debug)]
#[command(version = linera_version::VersionInfo::default_clap_str())]
pub struct ServiceConfig {
    /// ServiceStorage endpoint
    #[arg(long, default_value = "127.0.0.1:1235")]
    pub endpoint: String,
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

pub type StorageServiceRunner = Runner<ServiceStoreClient, ServiceConfig>;

impl StorageServiceRunner {
    pub async fn load() -> Result<Self, IndexerError> {
        let config = IndexerConfig::<ServiceConfig>::parse();
        let common_config = CommonStoreConfig {
            max_concurrent_queries: config.client.max_concurrent_queries,
            max_stream_queries: config.client.max_stream_queries,
            cache_size: config.client.cache_size,
        };
        let store_config = ServiceStoreConfig {
            endpoint: config.client.endpoint.clone(),
            common_config,
        };
        let namespace = config.client.table.clone();
        let root_key = &[];
        let store = ServiceStoreClient::maybe_create_and_connect(&store_config, &namespace, root_key).await?;
        Self::new(config, store).await
    }
}

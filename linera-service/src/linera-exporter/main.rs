// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use anyhow::Result;
use exporter_service::ExporterContext;
use futures::FutureExt;
use linera_client::config::{BlockExporterConfig, GenesisConfig};
use linera_sdk::views::ViewError;
use linera_service::{storage::StorageConfigNamespace, util};
use linera_views::{lru_caching::StorageCacheConfig, store::CommonStoreConfig};

#[allow(dead_code)]
mod exporter_service;
#[allow(dead_code)]
mod state;

#[derive(thiserror::Error, Debug)]
pub(crate) enum ExporterError {
    #[error("Recieved an invalid notification.")]
    BadNotification,

    #[error("unable to load the exporter state")]
    StateError(ViewError),

    #[error("generic storage error: {0}")]
    StorageError(#[from] ViewError),

    #[error("generic error: {0}")]
    GenericError(Box<dyn std::error::Error + Send + Sync + 'static>),
}

/// Options for running the linera block exporter.
#[derive(clap::Parser, Debug, Clone)]
#[command(
    name = "Linera Exporter",
    version = linera_version::VersionInfo::default_clap_str(),
)]
struct ExporterOptions {
    /// Path to the TOML file dedcribing the configuration for the block exporter.
    config_path: PathBuf,

    /// Storage configuration for the blockchain history, chain states and binary blobs.
    #[arg(long = "storage")]
    storage_config: StorageConfigNamespace,

    /// The number of Tokio worker threads to use.
    #[arg(long)]
    tokio_threads: Option<usize>,

    /// The maximal number of simultaneous queries to the database
    #[arg(long)]
    max_concurrent_queries: Option<usize>,

    /// The maximal number of stream queries to the database
    #[arg(long, default_value = "10")]
    max_stream_queries: usize,

    /// The maximal memory used in the storage cache.
    #[arg(long, default_value = "10000000")]
    max_cache_size: usize,

    /// The maximal size of an entry in the storage cache.
    #[arg(long, default_value = "1000000")]
    max_entry_size: usize,

    /// The maximal number of entries in the storage cache.
    #[arg(long, default_value = "1000")]
    max_cache_entries: usize,

    /// Path to the file describing the initial user chains (aka genesis state)
    #[arg(long = "genesis")]
    genesis_config_path: PathBuf,
}

fn main() -> Result<()> {
    linera_base::tracing::init("linera-exporter");
    let options = <ExporterOptions as clap::Parser>::parse();
    options.run()
}

impl ExporterOptions {
    fn run(&self) -> anyhow::Result<()> {
        let config_string = fs_err::read_to_string(&self.config_path)
            .expect("Unable to read the configuration file");
        let config: BlockExporterConfig =
            toml::from_str(&config_string).expect("Invalid configuration file format");

        let storage_cache_config = StorageCacheConfig {
            max_cache_size: self.max_cache_size,
            max_entry_size: self.max_entry_size,
            max_cache_entries: self.max_cache_entries,
        };
        let common_config = CommonStoreConfig {
            max_concurrent_queries: self.max_concurrent_queries,
            max_stream_queries: self.max_stream_queries,
            storage_cache_config,
        };

        let mut runtime_builder = match self.tokio_threads {
            None | Some(1) => tokio::runtime::Builder::new_current_thread(),
            Some(worker_threads) => {
                let mut builder = tokio::runtime::Builder::new_multi_thread();
                builder.worker_threads(worker_threads);
                builder
            }
        };

        let genesis_config: GenesisConfig = util::read_json(&self.genesis_config_path)?;

        let future = async {
            let context =
                ExporterContext::new(config.id, config.service_config, config.destination_config);
            let storage_config = self
                .storage_config
                .add_common_config(common_config)
                .await
                .unwrap();
            storage_config
                .run_with_storage(&genesis_config, None, context)
                .boxed()
                .await
        };

        let runtime = runtime_builder.enable_all().build()?;
        runtime.block_on(future)?.map_err(|e| e.into())
    }
}

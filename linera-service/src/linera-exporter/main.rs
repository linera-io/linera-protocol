// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use anyhow::Result;
use exporter_service::ExporterContext;
use futures::FutureExt;
use linera_client::config::BlockExporterConfig;
use linera_sdk::views::ViewError;
use linera_service::storage::StorageConfigNamespace;
use linera_views::{lru_caching::StorageCacheConfig, store::CommonStoreConfig};
use storage::run_exporter_with_storage;

#[allow(dead_code)]
mod exporter_service;
#[allow(dead_code)]
mod state;
#[allow(dead_code)]
mod storage;

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

trait Generic {
    type Output;
    fn into_unknown(self) -> Self::Output;
}

impl<T, E> Generic for core::result::Result<T, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    type Output = Result<T, ExporterError>;

    fn into_unknown(self) -> Self::Output {
        self.map_err(|e| (Box::new(e) as Box<dyn std::error::Error + Send + Sync>))
            .map_err(ExporterError::GenericError)
    }
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
            max_cache_size: config.limits.max_cache_size,
            max_entry_size: config.limits.max_entry_size,
            max_cache_entries: config.limits.max_cache_entries,
        };
        let common_config = CommonStoreConfig {
            max_concurrent_queries: config.limits.max_concurrent_queries,
            max_stream_queries: config.limits.max_stream_queries,
            storage_cache_config,
        };

        let mut runtime_builder = match config.limits.tokio_threads {
            None | Some(1) => tokio::runtime::Builder::new_current_thread(),
            Some(worker_threads) => {
                let mut builder = tokio::runtime::Builder::new_multi_thread();
                builder.worker_threads(worker_threads);
                builder
            }
        };

        let future = async {
            let context =
                ExporterContext::from_config(config.service_config, config.destination_config);
            let full_storage_config = self
                .storage_config
                .add_common_config(common_config)
                .await
                .unwrap();
            run_exporter_with_storage(full_storage_config, context)
                .boxed()
                .await
        };

        let runtime = runtime_builder.enable_all().build()?;
        runtime.block_on(future)?;

        Ok(())
    }
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use anyhow::Result;
use exporter_service::ExporterContext;
use futures::FutureExt;
use linera_client::config::BlockExporterConfig;
use linera_sdk::views::ViewError;
use linera_service::storage::{CommonStorageOptions, StorageConfig};

#[allow(dead_code)]
mod exporter_service;
#[allow(dead_code)]
mod state;

#[derive(thiserror::Error, Debug)]
pub(crate) enum ExporterError {
    #[error("received an invalid notification.")]
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
    /// Path to the TOML file describing the configuration for the block exporter.
    config_path: PathBuf,

    /// Storage configuration for the blockchain history, chain states and binary blobs.
    #[arg(long = "storage")]
    storage_config: StorageConfig,

    /// Common storage options.
    #[command(flatten)]
    common_storage_options: CommonStorageOptions,

    /// The number of Tokio worker threads to use.
    #[arg(long)]
    tokio_threads: Option<usize>,
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

        let mut runtime_builder = match self.tokio_threads {
            None | Some(1) => tokio::runtime::Builder::new_current_thread(),
            Some(worker_threads) => {
                let mut builder = tokio::runtime::Builder::new_multi_thread();
                builder.worker_threads(worker_threads);
                builder
            }
        };

        let future = async {
            let context =
                ExporterContext::new(config.id, config.service_config, config.destination_config);
            let store_config = self
                .storage_config
                .add_common_storage_options(&self.common_storage_options)
                .await
                .unwrap();
            store_config.run_with_storage(None, context).boxed().await
        };

        let runtime = runtime_builder.enable_all().build()?;
        runtime.block_on(future)?.map_err(|e| e.into())
    }
}

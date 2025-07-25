// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{path::PathBuf, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use common::{ExporterCancellationSignal, ExporterError};
use exporter_service::ExporterService;
use futures::FutureExt;
use linera_base::listen_for_shutdown_signals;
#[cfg(with_metrics)]
use linera_metrics::prometheus_server;
use linera_rpc::NodeOptions;
use linera_service::{
    config::BlockExporterConfig,
    storage::{CommonStorageOptions, Runnable, StorageConfig},
    util,
};
use linera_storage::Storage;
use runloops::start_block_processor_task;
use tokio_util::sync::CancellationToken;

mod common;
mod exporter_service;
#[cfg(with_metrics)]
mod metrics;
mod runloops;
mod state;
mod storage;

#[cfg(test)]
mod test_utils;
#[cfg(test)]
mod tests;

#[cfg(not(feature = "metrics"))]
const IS_WITH_METRICS: bool = false;
#[cfg(feature = "metrics")]
const IS_WITH_METRICS: bool = true;

/// Options for running the linera block exporter.
#[derive(clap::Parser, Debug, Clone)]
#[command(
    name = "Linera Exporter",
    version = linera_version::VersionInfo::default_clap_str(),
)]
struct ExporterOptions {
    /// Path to the TOML file describing the configuration for the block exporter.
    #[arg(long)]
    config_path: PathBuf,

    /// Storage configuration for the blockchain history, chain states and binary blobs.
    #[arg(long = "storage")]
    storage_config: StorageConfig,

    /// Common storage options.
    #[command(flatten)]
    common_storage_options: CommonStorageOptions,

    /// Maximum number of threads to use for exporters
    #[arg(long, default_value = "16")]
    max_exporter_threads: usize,

    /// Timeout in milliseconds for sending queries.
    #[arg(long = "send-timeout-ms", default_value = "4000", value_parser = util::parse_millis)]
    pub send_timeout: Duration,

    /// Timeout in milliseconds for receiving responses.
    #[arg(long = "recv-timeout-ms", default_value = "4000", value_parser = util::parse_millis)]
    pub recv_timeout: Duration,

    /// Delay increment for retrying to connect to a destination.
    #[arg(
        long = "retry-delay-ms",
        default_value = "1000",
        value_parser = util::parse_millis
    )]
    pub retry_delay: Duration,

    /// Number of times to retry connecting to a destination.
    #[arg(long, default_value = "10")]
    pub max_retries: u32,

    /// Port for the metrics server.
    #[arg(long)]
    pub metrics_port: Option<u16>,
}

struct ExporterContext {
    node_options: NodeOptions,
    config: BlockExporterConfig,
}

#[async_trait]
impl Runnable for ExporterContext {
    type Output = Result<(), ExporterError>;

    async fn run<S>(self, storage: S) -> Self::Output
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        let shutdown_notifier = CancellationToken::new();
        tokio::spawn(listen_for_shutdown_signals(shutdown_notifier.clone()));

        #[cfg(with_metrics)]
        prometheus_server::start_metrics(self.config.metrics_address(), shutdown_notifier.clone());

        let (sender, handle) = start_block_processor_task(
            storage,
            ExporterCancellationSignal::new(shutdown_notifier.clone()),
            self.config.limits,
            self.node_options,
            self.config.id,
            self.config.destination_config,
        )?;

        let service = ExporterService::new(sender);
        service
            .run(shutdown_notifier, self.config.service_config.port)
            .await?;
        handle.join().unwrap()
    }
}

impl ExporterContext {
    fn new(node_options: NodeOptions, config: BlockExporterConfig) -> ExporterContext {
        Self {
            config,
            node_options,
        }
    }
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
        let mut config: BlockExporterConfig =
            toml::from_str(&config_string).expect("Invalid configuration file format");

        let node_options = NodeOptions {
            send_timeout: self.send_timeout,
            recv_timeout: self.recv_timeout,
            retry_delay: self.retry_delay,
            max_retries: self.max_retries,
        };

        if let Some(port) = self.metrics_port {
            if IS_WITH_METRICS {
                tracing::info!("overriding metrics port to {}", port);
                config.metrics_port = port;
            } else {
                tracing::warn!(
                    "Metrics are not enabled in this build, ignoring metrics port configuration."
                );
            }
        }

        let context = ExporterContext::new(node_options, config);

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("block-exporter-worker")
            .worker_threads(self.max_exporter_threads)
            .enable_all()
            .build()?;

        let future = async {
            let store_config = self
                .storage_config
                .add_common_storage_options(&self.common_storage_options)
                .await
                .unwrap();
            store_config.run_with_storage(None, context).boxed().await
        };

        runtime.block_on(future)?.map_err(|e| e.into())
    }
}

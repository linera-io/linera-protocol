// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{path::PathBuf, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use common::{ExporterCancellationSignal, ExporterError};
use exporter_service::ExporterService;
use futures::FutureExt;
use linera_base::listen_for_shutdown_signals;
use linera_client::config::{BlockExporterConfig, GenesisConfig};
use linera_rpc::NodeOptions;
use linera_service::{
    storage::{Runnable, StorageConfigNamespace},
    util,
};
use linera_storage::Storage;
use linera_views::{lru_caching::StorageCacheConfig, store::CommonStoreConfig};
use runloops::start_runloops;
use tokio_util::sync::CancellationToken;

mod common;
mod exporter_service;
mod runloops;
mod state;
mod storage;

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
    storage_config: StorageConfigNamespace,

    /// Clients per thread
    #[arg(long, default_value = "16")]
    max_clients_per_thread: usize,

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

struct ExporterContext {
    clients_per_thread: usize,
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

        let sender = start_runloops(
            ExporterCancellationSignal::new(shutdown_notifier.clone()),
            storage,
            self.config.limits,
            self.node_options,
            self.config.id,
            self.clients_per_thread,
            self.config.destination_config,
        )?;

        let service = ExporterService::new(sender);
        let port = self.config.service_config.port;
        service.run(shutdown_notifier, port).await
    }
}

impl ExporterContext {
    fn new(
        clients_per_thread: usize,
        node_options: NodeOptions,
        config: BlockExporterConfig,
    ) -> ExporterContext {
        Self {
            config,
            clients_per_thread,
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
        let config: BlockExporterConfig =
            toml::from_str(&config_string).expect("Invalid configuration file format");

        let node_options = NodeOptions {
            send_timeout: self.send_timeout,
            recv_timeout: self.recv_timeout,
            retry_delay: self.retry_delay,
            max_retries: self.max_retries,
        };

        let context = ExporterContext::new(self.max_clients_per_thread, node_options, config);

        let storage_cache_config = StorageCacheConfig {
            max_cache_size: self.max_cache_size,
            max_entry_size: self.max_entry_size,
            max_cache_entries: self.max_cache_entries,
        };
        let common_config = CommonStoreConfig {
            max_concurrent_queries: self.max_concurrent_queries,
            max_stream_queries: self.max_stream_queries,
            storage_cache_config,
            replication_factor: 1,
        };

        let genesis_config: GenesisConfig = util::read_json(&self.genesis_config_path)?;

        let future = async {
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

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(future)?.map_err(|e| e.into())
    }
}

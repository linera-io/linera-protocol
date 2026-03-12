// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use anyhow::Result;
use async_trait::async_trait;
use common::{ExporterCancellationSignal, ExporterError};
use exporter_service::ExporterService;
use futures::FutureExt;
use linera_base::listen_for_shutdown_signals;
#[cfg(with_metrics)]
use linera_metrics::monitoring_server;
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

/// CLI for the linera block exporter.
#[derive(clap::Parser, Debug)]
#[command(
    name = "Linera Exporter",
    version = linera_version::VersionInfo::default_clap_str(),
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Run the block exporter
    Run(RunOptions),
    /// Manage destination states
    Destinations {
        #[command(subcommand)]
        command: DestinationsCommand,
    },
}

#[derive(clap::Subcommand, Debug)]
enum DestinationsCommand {
    /// List all destinations and their current block indices
    List(DestinationsOptions),
    /// Show a specific destination's state
    Show {
        /// The address of the destination
        address: String,
        #[command(flatten)]
        options: DestinationsOptions,
    },
    /// Set a destination's block index
    Set {
        /// The address of the destination.
        /// Can be acquired from the `list` command.
        address: String,
        /// The block index to set
        index: u64,
        #[command(flatten)]
        options: DestinationsOptions,
    },
}

/// Options for destination management commands
#[derive(clap::Args, Debug, Clone)]
struct DestinationsOptions {
    /// Storage configuration for the blockchain history, chain states and binary blobs.
    #[arg(long = "storage")]
    storage_config: StorageConfig,

    /// Common storage options.
    #[command(flatten)]
    common_storage_options: CommonStorageOptions,

    /// Exporter ID
    #[arg(long, default_value = "1")]
    exporter_id: u32,
}

/// Options for running the linera block exporter.
#[derive(clap::Args, Debug, Clone)]
struct RunOptions {
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

    /// Maximum backoff delay for retrying to connect to a destination.
    #[arg(
        long = "max-backoff-ms",
        default_value = "30000",
        value_parser = util::parse_millis
    )]
    pub max_backoff: Duration,

    /// Port for the metrics server.
    #[arg(long)]
    pub metrics_port: Option<u16>,

    /// Enable jemalloc memory profiling endpoints on the metrics server.
    #[cfg(feature = "jemalloc")]
    #[arg(long, env = "LINERA_ENABLE_MEMORY_PROFILING")]
    pub enable_memory_profiling: bool,
}

async fn start_health_server(
    address: std::net::SocketAddr,
    shutdown_signal: CancellationToken,
    health: Arc<AtomicBool>,
    enable_memory_profiling: bool,
) {
    let health_router = axum::Router::new().route(
        "/health",
        axum::routing::get(move || {
            let is_healthy = health.load(std::sync::atomic::Ordering::Acquire);
            async move {
                if is_healthy {
                    (axum::http::StatusCode::OK, "OK")
                } else {
                    (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "unhealthy")
                }
            }
        }),
    );

    #[cfg(with_metrics)]
    {
        let memory_profiling = if enable_memory_profiling {
            monitoring_server::MemoryProfiling::Enabled
        } else {
            monitoring_server::MemoryProfiling::Disabled
        };
        monitoring_server::start_metrics_with_extras(
            address,
            shutdown_signal,
            memory_profiling,
            Some(health_router),
        );
    }

    #[cfg(not(with_metrics))]
    {
        let listener = tokio::net::TcpListener::bind(address)
            .await
            .expect("Failed to bind health server");
        let addr = listener.local_addr().expect("Failed to get local address");
        tracing::info!("Serving /health on {:?}", addr);
        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, health_router)
                .with_graceful_shutdown(shutdown_signal.cancelled_owned())
                .await
            {
                tracing::error!("Health server error: {}", e);
            }
        });
    }
}

struct ExporterContext {
    node_options: NodeOptions,
    config: BlockExporterConfig,
    #[cfg(with_metrics)]
    enable_memory_profiling: bool,
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

        let health = Arc::new(AtomicBool::new(true));
        let enable_memory_profiling = {
            #[cfg(with_metrics)]
            {
                self.enable_memory_profiling
            }
            #[cfg(not(with_metrics))]
            {
                false
            }
        };
        start_health_server(
            self.config.metrics_address(),
            shutdown_notifier.clone(),
            health.clone(),
            enable_memory_profiling,
        )
        .await;

        let (sender, handle) = start_block_processor_task(
            storage,
            ExporterCancellationSignal::new(shutdown_notifier.clone()),
            self.config.limits,
            self.node_options,
            self.config.id,
            self.config.destination_config,
            health,
        );

        let service = ExporterService::new(sender);

        let mut block_processor_task = tokio::task::spawn_blocking(move || {
            handle.join().unwrap_or_else(|_| {
                Err(ExporterError::GenericError(
                    "block processor thread panicked".into(),
                ))
            })
        });
        tokio::select! {
            result = service.run(shutdown_notifier, self.config.service_config.port) => {
                result?;
                block_processor_task.await
                    .map_err(|e| ExporterError::GenericError(e.into()))?
            }
            result = &mut block_processor_task => {
                result.map_err(|e| ExporterError::GenericError(e.into()))?
            }
        }
    }
}

fn main() -> Result<()> {
    linera_service::tracing::init("linera-exporter");
    let cli = <Cli as clap::Parser>::parse();
    match cli.command {
        Command::Run(options) => options.run(),
        Command::Destinations { command } => command.run(),
    }
}

impl RunOptions {
    #[cfg(with_metrics)]
    fn enable_memory_profiling(&self) -> bool {
        #[cfg(feature = "jemalloc")]
        {
            self.enable_memory_profiling
        }
        #[cfg(not(feature = "jemalloc"))]
        {
            false
        }
    }

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
            max_backoff: self.max_backoff,
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

        let context = ExporterContext {
            node_options,
            config,
            #[cfg(with_metrics)]
            enable_memory_profiling: self.enable_memory_profiling(),
        };

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("block-exporter-worker")
            .worker_threads(self.max_exporter_threads)
            .enable_all()
            .build()?;

        let future = async {
            let store_config = self
                .storage_config
                .add_common_storage_options(&self.common_storage_options)
                .unwrap();
            // Exporters are part of validator infrastructure and should not output contract logs.
            let allow_application_logs = false;
            store_config
                .run_with_storage(None, allow_application_logs, context)
                .boxed()
                .await
        };

        runtime.block_on(future)?.map_err(|e| e.into())
    }
}

impl DestinationsCommand {
    fn run(self) -> Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        runtime.block_on(self.run_async())
    }

    async fn run_async(self) -> Result<()> {
        let (options, action) = match &self {
            DestinationsCommand::List(opts) => (opts, DestinationAction::List),
            DestinationsCommand::Show { address, options } => {
                (options, DestinationAction::Show(address.clone()))
            }
            DestinationsCommand::Set {
                address,
                index,
                options,
            } => (options, DestinationAction::Set(address.clone(), *index)),
        };

        let store_config = options
            .storage_config
            .add_common_storage_options(&options.common_storage_options)?;

        let context = DestinationsContext {
            exporter_id: options.exporter_id,
            action,
        };

        store_config
            .run_with_storage(None, false, context)
            .await?
            .map_err(Into::into)
    }
}

enum DestinationAction {
    List,
    Show(String),
    Set(String, u64),
}

struct DestinationsContext {
    exporter_id: u32,
    action: DestinationAction,
}

#[async_trait]
impl Runnable for DestinationsContext {
    type Output = Result<(), ExporterError>;

    async fn run<S>(self, storage: S) -> Self::Output
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        use linera_sdk::views::{RootView, View};
        use linera_service::config::DestinationKind;
        use state::BlockExporterStateView;

        let context = storage
            .block_exporter_context(self.exporter_id)
            .await
            .map_err(ExporterError::StateError)?;
        let mut view = BlockExporterStateView::load(context)
            .await
            .map_err(ExporterError::StateError)?;
        let states = view.get_destination_states().clone();

        match self.action {
            DestinationAction::List => {
                println!("{:<50} {:<12} {:>10}", "DESTINATION", "KIND", "INDEX");
                for (id, index) in states.iter() {
                    let kind = match id.kind() {
                        DestinationKind::Validator => "validator",
                        DestinationKind::Indexer => "indexer",
                        DestinationKind::Logging => "logging",
                        DestinationKind::EvmChain => "evm_chain",
                    };
                    println!("{:<50} {:<12} {:>10}", id.address(), kind, index);
                }
            }
            DestinationAction::Show(address) => {
                let matches: Vec<_> = states
                    .iter()
                    .filter(|(id, _)| id.address() == address)
                    .collect();

                match matches.len() {
                    0 => {
                        eprintln!("Error: No destination found with address \"{}\"", address);
                        std::process::exit(1);
                    }
                    1 => {
                        let (id, index) = &matches[0];
                        let kind = match id.kind() {
                            DestinationKind::Validator => "validator",
                            DestinationKind::Indexer => "indexer",
                            DestinationKind::Logging => "logging",
                            DestinationKind::EvmChain => "evm_chain",
                        };
                        println!("Address: {}", id.address());
                        println!("Kind:    {}", kind);
                        println!("Index:   {}", index);
                    }
                    _ => {
                        eprintln!(
                            "Error: Multiple destinations found for \"{}\". Specify kind with --kind validator|indexer",
                            address
                        );
                        std::process::exit(1);
                    }
                }
            }
            DestinationAction::Set(address, new_index) => {
                let matches: Vec<_> = states
                    .iter()
                    .filter(|(id, _)| id.address() == address)
                    .collect();

                match matches.len() {
                    0 => {
                        eprintln!("Error: No destination found with address \"{}\"", address);
                        std::process::exit(1);
                    }
                    1 => {
                        let (id, old_index) = &matches[0];
                        let kind = match id.kind() {
                            DestinationKind::Validator => "validator",
                            DestinationKind::Indexer => "indexer",
                            DestinationKind::Logging => "logging",
                            DestinationKind::EvmChain => "evm_chain",
                        };

                        // Update in-memory and save
                        states.set(id, new_index);
                        view.set_destination_states(states);
                        view.save().await.map_err(ExporterError::StateError)?;
                        println!(
                            "Updated {} ({}): {} -> {}",
                            id.address(),
                            kind,
                            old_index,
                            new_index
                        );
                    }
                    _ => {
                        eprintln!(
                            "Error: Multiple destinations found for \"{}\". Specify kind with --kind validator|indexer",
                            address
                        );
                        std::process::exit(1);
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod health_tests {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    use linera_base::port::get_free_port;
    use linera_rpc::{config::TlsConfig, NodeOptions};
    use linera_service::{
        cli_wrappers::local_net::LocalNet,
        config::{Destination, DestinationConfig, LimitsConfig},
    };
    use linera_storage::{DbStorage, TestClock};
    use linera_views::memory::MemoryDatabase;
    use tokio::time::{sleep, Duration};
    use tokio_util::sync::CancellationToken;

    use super::start_health_server;
    use crate::{
        common::ExporterCancellationSignal,
        runloops::start_block_processor_task,
        test_utils::{make_simple_state_with_blobs, DummyIndexer, TestDestination},
    };

    #[test_log::test(tokio::test)]
    async fn test_health_endpoint_reflects_exporter_errors() -> anyhow::Result<()> {
        let cancellation_token = CancellationToken::new();
        let health = Arc::new(AtomicBool::new(true));

        // Start the production health server on a free port.
        let health_port = get_free_port().await?;
        let health_addr = std::net::SocketAddr::from(([127, 0, 0, 1], health_port));
        start_health_server(
            health_addr,
            cancellation_token.clone(),
            health.clone(),
            false,
        )
        .await;

        // Start a faulty indexer destination.
        let indexer_port = get_free_port().await?;
        let indexer = DummyIndexer::default();
        indexer.set_faulty();
        tokio::spawn(
            indexer
                .clone()
                .start(indexer_port, cancellation_token.clone()),
        );
        LocalNet::ensure_grpc_server_has_started("faulty indexer", indexer_port as usize, "http")
            .await?;

        // Prepare storage with test blocks.
        let storage = DbStorage::<MemoryDatabase, TestClock>::make_test_storage(None).await;
        let (notification, _state) = make_simple_state_with_blobs(&storage).await;

        // Start the block processor with the faulty indexer and shared health flag.
        let signal = ExporterCancellationSignal::new(cancellation_token.clone());
        let (notifier, _handle) = start_block_processor_task(
            storage,
            signal,
            LimitsConfig::default(),
            NodeOptions {
                send_timeout: Duration::from_millis(4000),
                recv_timeout: Duration::from_millis(4000),
                retry_delay: Duration::from_millis(1000),
                max_retries: 10,
                ..Default::default()
            },
            0,
            DestinationConfig {
                committee_destination: false,
                destinations: vec![Destination::Indexer {
                    port: indexer_port,
                    tls: TlsConfig::ClearText,
                    endpoint: "127.0.0.1".to_owned(),
                }],
            },
            health.clone(),
        );

        let base = format!("http://127.0.0.1:{health_port}");
        let client = reqwest::Client::new();

        // Before any errors, health should be 200.
        let resp = client.get(format!("{base}/health")).send().await?;
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.text().await?, "OK");

        // Send a block notification — the faulty indexer will cause a stream error.
        notifier.send(notification)?;

        // Wait for the error to propagate and flip the health flag.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while health.load(Ordering::Acquire) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "health flag did not flip to unhealthy within timeout"
            );
            sleep(Duration::from_millis(100)).await;
        }

        // After the stream error, health should be 500.
        let resp = client.get(format!("{base}/health")).send().await?;
        assert_eq!(resp.status(), 500);
        assert_eq!(resp.text().await?, "unhealthy");

        cancellation_token.cancel();
        Ok(())
    }
}

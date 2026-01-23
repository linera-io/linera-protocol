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
        monitoring_server::start_metrics(self.config.metrics_address(), shutdown_notifier.clone());

        let (sender, handle) = start_block_processor_task(
            storage,
            ExporterCancellationSignal::new(shutdown_notifier.clone()),
            self.config.limits,
            self.node_options,
            self.config.id,
            self.config.destination_config,
        );

        let service = ExporterService::new(sender);

        let mut block_processor_task = tokio::task::spawn_blocking(move || handle.join().unwrap());
        tokio::select! {
            result = service.run(shutdown_notifier, self.config.service_config.port) => {
                result?;
                block_processor_task.await.expect("block processor task panicked")
            }
            result = &mut block_processor_task => {
                result.expect("block processor task panicked")
            }
        }
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
    linera_service::tracing::init("linera-exporter");
    let cli = <Cli as clap::Parser>::parse();
    match cli.command {
        Command::Run(options) => options.run(),
        Command::Destinations { command } => command.run(),
    }
}

impl RunOptions {
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

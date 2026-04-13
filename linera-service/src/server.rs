// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "256"]

#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Configure jemalloc profiling infrastructure at startup with sampling disabled.
/// Profiling is activated at runtime only when `--enable-memory-profiling` is passed.
#[cfg(feature = "jemalloc")]
#[export_name = "malloc_conf"]
pub static MALLOC_CONF: &[u8] = b"prof:true,prof_active:false,lg_prof_sample:19\0";

use std::{
    borrow::Cow,
    num::NonZeroU16,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{bail, Context};
use async_trait::async_trait;
use futures::{stream::FuturesUnordered, FutureExt as _, StreamExt, TryFutureExt as _};
use linera_base::{
    crypto::{CryptoRng, Ed25519SecretKey},
    listen_for_shutdown_signals,
};
use linera_client::config::{CommitteeConfig, ValidatorConfig, ValidatorServerConfig};
use linera_core::{
    chain_worker::DynamicTtl, worker::WorkerState, ChainWorkerConfig, JoinSetExt as _,
    CHAIN_INFO_MAX_RECEIVED_LOG_ENTRIES,
};
use linera_execution::{WasmRuntime, WithWasmDefault};
#[cfg(with_metrics)]
use linera_metrics::monitoring_server;
use linera_persistent::{self as persistent, Persist};
use linera_rpc::{
    config::{
        CrossChainConfig, ExporterServiceConfig, NetworkProtocol, NotificationConfig, ProxyConfig,
        ShardConfig, ShardId, TlsConfig, ValidatorInternalNetworkConfig,
        ValidatorPublicNetworkConfig,
    },
    grpc, simple,
};
use linera_sdk::linera_base_types::{AccountSecretKey, ValidatorKeypair};
use linera_service::{
    storage::{CommonStorageOptions, Runnable, StorageConfig},
    util,
};
use linera_storage::Storage;
use serde::Deserialize;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

struct ServerContext {
    server_config: ValidatorServerConfig,
    cross_chain_config: CrossChainConfig,
    notification_config: NotificationConfig,
    shard: Option<usize>,
    block_time_grace_period: Duration,
    chain_worker_ttl: Option<Arc<DynamicTtl>>,
    block_cache_size: usize,
    execution_state_cache_size: usize,
    chain_info_max_received_log_entries: usize,
    cross_chain_message_chunk_limit: usize,
    allow_revert_confirm: bool,
    reset_on_incorrect_outcome_mins: Option<u64>,
    chain_worker_memory_limit: linera_service::memory_monitor::MemoryLimit,
    chain_worker_memory_monitor_interval_ms: u64,
    #[cfg(with_metrics)]
    enable_memory_profiling: bool,
}

impl ServerContext {
    fn make_shard_state<S>(
        &self,
        local_ip_addr: &str,
        shard_id: ShardId,
        storage: S,
    ) -> (WorkerState<S>, ShardId, ShardConfig)
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        let shard = self.server_config.internal_network.shard(shard_id);
        info!("Shard booted on {}", shard.host);
        info!(
            "Public key: {}",
            self.server_config.validator_secret.public()
        );
        let config = ChainWorkerConfig {
            nickname: format!("Shard {} @ {}:{}", shard_id, local_ip_addr, shard.port),
            key_pair: Some(Arc::new(self.server_config.validator_secret.copy())),
            allow_inactive_chains: false,
            allow_messages_from_deprecated_epochs: false,
            block_time_grace_period: self.block_time_grace_period,
            ttl: self.chain_worker_ttl.clone(),
            chain_info_max_received_log_entries: self.chain_info_max_received_log_entries,
            cross_chain_message_chunk_limit: self.cross_chain_message_chunk_limit,
            block_cache_size: self.block_cache_size,
            execution_state_cache_size: self.execution_state_cache_size,
            allow_revert_confirm: self.allow_revert_confirm,
            reset_on_incorrect_outcome: self
                .reset_on_incorrect_outcome_mins
                .map(|m| Duration::from_secs(m * 60)),
            ..ChainWorkerConfig::default()
        };
        let state = WorkerState::new(storage, config, None);
        (state, shard_id, shard.clone())
    }

    #[cfg_attr(not(with_metrics), allow(unused_variables))]
    fn spawn_simple<S>(
        &self,
        listen_address: &str,
        states: Vec<(WorkerState<S>, ShardId, ShardConfig)>,
        protocol: simple::TransportProtocol,
        shutdown_signal: &CancellationToken,
        enable_memory_profiling: bool,
    ) -> JoinSet<()>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        let mut join_set = JoinSet::new();
        let handles = FuturesUnordered::new();

        let internal_network = self
            .server_config
            .internal_network
            .clone_with_protocol(protocol);

        for (state, shard_id, shard) in states {
            let internal_network = internal_network.clone();
            let cross_chain_config = self.cross_chain_config.clone();
            let listen_address = listen_address.to_owned();

            #[cfg(with_metrics)]
            if let Some(port) = shard.metrics_port {
                monitoring_server::start_metrics(
                    (listen_address.clone(), port),
                    shutdown_signal.clone(),
                    monitoring_server::MemoryProfiling::from(enable_memory_profiling),
                );
            }

            let server_handle = simple::Server::new(
                internal_network,
                listen_address,
                shard.port,
                state,
                shard_id,
                cross_chain_config,
            )
            .spawn(shutdown_signal.clone(), &mut join_set);

            handles.push(
                server_handle
                    .join()
                    .inspect_err(move |error| {
                        error!("Error running server for shard {shard_id}: {error:?}")
                    })
                    .map(|_| ()),
            );
        }

        join_set.spawn_task(handles.collect::<()>());

        join_set
    }

    #[cfg_attr(not(with_metrics), allow(unused_variables))]
    fn spawn_grpc<S>(
        &self,
        listen_address: &str,
        states: Vec<(WorkerState<S>, ShardId, ShardConfig)>,
        shutdown_signal: &CancellationToken,
        enable_memory_profiling: bool,
    ) -> JoinSet<()>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        let mut join_set = JoinSet::new();
        let handles = FuturesUnordered::new();

        for (state, shard_id, shard) in states {
            #[cfg(with_metrics)]
            if let Some(port) = shard.metrics_port {
                monitoring_server::start_metrics(
                    (listen_address.to_string(), port),
                    shutdown_signal.clone(),
                    monitoring_server::MemoryProfiling::from(enable_memory_profiling),
                );
            }

            let server_handle = grpc::GrpcServer::spawn(
                listen_address.to_string(),
                shard.port,
                state,
                shard_id,
                self.server_config.internal_network.clone(),
                &self.cross_chain_config,
                &self.notification_config,
                shutdown_signal.clone(),
                &mut join_set,
            );

            handles.push(
                server_handle
                    .join()
                    .inspect_err(move |error| {
                        error!("Error running server for shard {shard_id}: {error:?}")
                    })
                    .map(|_| ()),
            );
        }

        join_set.spawn_task(handles.collect::<()>());

        join_set
    }

    fn get_listen_address() -> String {
        // Allow local IP address to be different from the public one.
        "0.0.0.0".to_string()
    }
}

#[async_trait]
impl Runnable for ServerContext {
    type Output = anyhow::Result<()>;

    async fn run<S>(self, storage: S) -> anyhow::Result<()>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        let shutdown_notifier = CancellationToken::new();
        let listen_address = Self::get_listen_address();

        tokio::spawn(listen_for_shutdown_signals(shutdown_notifier.clone()));

        // Activate memory profiling once before per-shard start_metrics calls.
        #[cfg(with_metrics)]
        let enable_memory_profiling = {
            let memory_profiling =
                monitoring_server::MemoryProfiling::try_activate(self.enable_memory_profiling)
                    .await;
            memory_profiling == monitoring_server::MemoryProfiling::Enabled
        };
        #[cfg(not(with_metrics))]
        let enable_memory_profiling = false;

        // Run the server
        let states = match self.shard {
            Some(shard) => {
                info!("Running shard number {}", shard);
                vec![self.make_shard_state(&listen_address, shard, storage)]
            }
            None => {
                info!("Running all shards");
                let num_shards = self.server_config.internal_network.shards.len();
                (0..num_shards)
                    .map(|shard| self.make_shard_state(&listen_address, shard, storage.clone()))
                    .collect()
            }
        };

        {
            let mut ttls = Vec::new();
            if let Some(ttl) = &self.chain_worker_ttl {
                ttls.push(Arc::clone(ttl));
            }
            linera_service::memory_monitor::spawn_memory_monitor(
                linera_service::memory_monitor::MemoryMonitorConfig {
                    memory_limit: self.chain_worker_memory_limit.clone(),
                    poll_interval: Duration::from_millis(
                        self.chain_worker_memory_monitor_interval_ms,
                    ),
                    ttls,
                },
            );
        }

        let mut join_set = match self.server_config.internal_network.protocol {
            NetworkProtocol::Simple(protocol) => self.spawn_simple(
                &listen_address,
                states,
                protocol,
                &shutdown_notifier,
                enable_memory_profiling,
            ),
            NetworkProtocol::Grpc(tls_config) => match tls_config {
                TlsConfig::ClearText => self.spawn_grpc(
                    &listen_address,
                    states,
                    &shutdown_notifier,
                    enable_memory_profiling,
                ),
                TlsConfig::Tls => bail!("TLS not supported between proxy and shards."),
            },
        };

        join_set.await_all_tasks().await;

        Ok(())
    }
}

#[derive(clap::Parser)]
#[command(
    name = "linera-server",
    about = "Server implementation (aka validator shard) for the Linera blockchain",
    version = linera_version::VersionInfo::default_clap_str(),
)]
struct ServerOptions {
    /// Subcommands. Acceptable values are run and generate.
    #[command(subcommand)]
    command: ServerCommand,

    /// The number of Tokio worker threads to use.
    #[arg(long, env = "LINERA_SERVER_TOKIO_THREADS")]
    tokio_threads: Option<usize>,

    /// The number of Tokio blocking threads to use.
    #[arg(long, env = "LINERA_SERVER_TOKIO_BLOCKING_THREADS")]
    tokio_blocking_threads: Option<usize>,

    /// Size of the block cache (default: 5000)
    #[arg(long, env = "LINERA_BLOCK_CACHE_SIZE", default_value = "5000")]
    block_cache_size: usize,

    /// Size of the execution state cache (default: 10000)
    #[arg(
        long,
        env = "LINERA_EXECUTION_STATE_CACHE_SIZE",
        default_value = "10000"
    )]
    execution_state_cache_size: usize,

    /// Enable jemalloc memory profiling endpoints on the metrics server.
    #[cfg(feature = "jemalloc")]
    #[arg(long, env = "LINERA_ENABLE_MEMORY_PROFILING")]
    enable_memory_profiling: bool,
}

impl ServerOptions {
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
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
struct ValidatorOptions {
    /// Path to the file containing the server configuration of this Linera validator (including its secret key)
    server_config_path: PathBuf,

    /// The host of the validator (IP address or hostname)
    host: String,

    /// The port of the validator
    port: u16,

    /// The server configurations for the linera-exporter.
    #[serde(default)]
    block_exporters: Vec<ExporterServiceConfig>,

    /// The network protocol for the frontend.
    external_protocol: NetworkProtocol,

    /// The network protocol for workers.
    internal_protocol: NetworkProtocol,

    /// The public name and the port of each of the shards
    shards: Vec<ShardConfig>,

    /// The name and the port of the proxies
    proxies: Vec<ProxyConfig>,
}

fn make_server_config<R: CryptoRng>(
    path: &Path,
    rng: &mut R,
    options: ValidatorOptions,
) -> anyhow::Result<persistent::File<ValidatorServerConfig>> {
    let validator_keypair = ValidatorKeypair::generate_from(rng);
    let account_secret = AccountSecretKey::Ed25519(Ed25519SecretKey::generate_from(rng));
    let public_key = validator_keypair.public_key;
    let network = ValidatorPublicNetworkConfig {
        protocol: options.external_protocol,
        host: options.host,
        port: options.port,
    };
    let internal_network = ValidatorInternalNetworkConfig {
        public_key,
        protocol: options.internal_protocol,
        shards: options.shards,
        block_exporters: options.block_exporters,
        proxies: options.proxies,
    };
    let validator = ValidatorConfig {
        network,
        public_key,
        account_key: account_secret.public(),
    };
    Ok(persistent::File::new(
        path,
        ValidatorServerConfig {
            validator,
            validator_secret: validator_keypair.secret_key,
            internal_network,
        },
    )?)
}

#[derive(clap::Parser)]
enum ServerCommand {
    /// Runs a service for each shard of the Linera validator")
    #[command(name = "run")]
    Run {
        /// Path to the file containing the server configuration of this Linera validator (including its secret key)
        #[arg(long = "server")]
        server_config_path: PathBuf,

        /// Storage configuration for the blockchain history, chain states and binary blobs.
        #[arg(long = "storage")]
        storage_config: StorageConfig,

        /// Common storage options.
        #[command(flatten)]
        common_storage_options: Box<CommonStorageOptions>,

        /// Configuration for cross-chain requests
        #[command(flatten)]
        cross_chain_config: CrossChainConfig,

        /// Configuration for notifications
        #[command(flatten)]
        notification_config: NotificationConfig,

        /// Runs a specific shard (from 0 to shards-1)
        #[arg(long)]
        shard: Option<usize>,

        /// Blocks with a timestamp this far in the future will still be accepted, but the validator
        /// will wait until that timestamp before voting.
        #[arg(long = "block-time-grace-period-ms", default_value = "500", value_parser = util::parse_millis)]
        block_time_grace_period: Duration,

        /// The WebAssembly runtime to use.
        #[arg(long)]
        wasm_runtime: Option<WasmRuntime>,

        /// The duration in milliseconds after which an idle chain worker will free its memory.
        /// Use 0 to disable expiry.
        #[arg(
            long = "chain-worker-ttl-ms",
            default_value = "30000",
            value_parser = util::parse_millis
        )]
        chain_worker_ttl: Duration,

        /// Maximum size for received_log entries in chain info responses. This should
        /// generally only be increased from the default value.
        #[arg(
            long,
            default_value_t = CHAIN_INFO_MAX_RECEIVED_LOG_ENTRIES,
            env = "LINERA_SERVER_CHAIN_INFO_MAX_RECEIVED_LOG_ENTRIES",
        )]
        chain_info_max_received_log_entries: usize,

        /// Maximum estimated serialized size (in bytes) of bundles in a single
        /// cross-chain `UpdateRecipient` message. Larger sets of bundles are split
        /// into multiple messages.
        #[arg(
            long,
            default_value_t = grpc::GRPC_CHUNKED_MESSAGE_FILL_LIMIT,
        )]
        cross_chain_message_chunk_limit: usize,

        /// Enable the RevertConfirm recovery mechanism for inbox gaps caused by
        /// lost persisted state.
        #[arg(long, default_value_t = false)]
        allow_revert_confirm: bool,

        /// On IncorrectOutcome errors, reset the chain state and re-execute all
        /// blocks from scratch. Sends RevertConfirm to all known senders. The
        /// value is the minimum number of minutes since the last reset before
        /// another reset is allowed (to prevent loops).
        #[arg(long)]
        reset_on_incorrect_outcome_mins: Option<u64>,

        /// Memory limit for chain worker eviction. Accepts either megabytes
        /// (e.g. "4096") or a percentage of total system/cgroup memory
        /// (e.g. "60%"). When process RSS approaches this limit, idle chain
        /// worker TTLs are dynamically reduced.
        #[arg(
            long,
            env = "LINERA_CHAIN_WORKER_MEMORY_LIMIT",
            default_value = linera_service::memory_monitor::DEFAULT_MEMORY_LIMIT,
            value_parser = linera_service::memory_monitor::parse_memory_limit,
        )]
        chain_worker_memory_limit: linera_service::memory_monitor::MemoryLimit,

        /// Polling interval in milliseconds for the chain worker memory monitor.
        #[arg(
            long,
            env = "LINERA_CHAIN_WORKER_MEMORY_MONITOR_INTERVAL_MS",
            default_value = "1000"
        )]
        chain_worker_memory_monitor_interval_ms: u64,

        /// OpenTelemetry OTLP exporter endpoint (requires opentelemetry feature).
        #[arg(long, env = "LINERA_OTLP_EXPORTER_ENDPOINT")]
        otlp_exporter_endpoint: Option<String>,
    },

    /// Act as a trusted third-party and generate all server configurations
    #[command(name = "generate")]
    Generate {
        /// Configuration file of each validator in the committee
        #[arg(long, num_args(0..))]
        validators: Vec<PathBuf>,

        /// Path where to write the description of the Linera committee
        #[arg(long)]
        committee: Option<PathBuf>,

        /// Force this command to generate keys using a PRNG and a given seed. USE FOR
        /// TESTING ONLY.
        #[arg(long)]
        testing_prng_seed: Option<u64>,
    },

    /// Replaces the configurations of the shards by following the given template.
    #[command(name = "edit-shards")]
    EditShards {
        /// Path to the file containing the server configuration of this Linera validator.
        #[arg(long = "server")]
        server_config_path: PathBuf,

        /// The number N of shard configs to generate, possibly starting with zeroes. If
        /// `N` was written using `D` digits, we will replace the first occurrence of the
        /// string `"%" * D` (`%` repeated D times) by the shard number.
        #[arg(long)]
        num_shards: String,

        /// The host of the validator (IP address or hostname), possibly containing `%`
        /// for digits of the shard number.
        #[arg(long)]
        host: String,

        /// The port of the main endpoint, possibly containing `%` for digits of the shard
        /// number.
        #[arg(long)]
        port: String,

        /// The port for the metrics endpoint, possibly containing `%` for digits of the
        /// shard number.
        #[arg(long)]
        metrics_port: Option<String>,
    },
}

fn main() {
    let options = <ServerOptions as clap::Parser>::parse();

    let mut runtime = if options.tokio_threads == Some(1) {
        tokio::runtime::Builder::new_current_thread()
    } else {
        let mut builder = tokio::runtime::Builder::new_multi_thread();

        if let Some(threads) = options.tokio_threads {
            builder.worker_threads(threads);
        }

        builder
    };

    if let Some(blocking_threads) = options.tokio_blocking_threads {
        runtime.max_blocking_threads(blocking_threads);
    }

    runtime
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
        .block_on(run(options))
}

/// Returns the log file name to use based on the [`ServerCommand`] that will run.
fn otlp_exporter_endpoint_for(command: &ServerCommand) -> Option<&str> {
    match command {
        ServerCommand::Run {
            otlp_exporter_endpoint,
            ..
        } => otlp_exporter_endpoint.as_deref(),
        ServerCommand::Generate { .. } | ServerCommand::EditShards { .. } => None,
    }
}

fn log_file_name_for(command: &ServerCommand) -> Cow<'static, str> {
    match command {
        ServerCommand::Run {
            shard,
            server_config_path,
            ..
        } => {
            let server_config: ValidatorServerConfig =
                util::read_json(server_config_path).expect("Failed to read server config");
            let public_key = &server_config.validator.public_key;

            if let Some(shard) = shard {
                format!("validator-{public_key}-shard-{shard}")
            } else {
                format!("validator-{public_key}")
            }
            .into()
        }
        ServerCommand::Generate { .. } | ServerCommand::EditShards { .. } => "server".into(),
    }
}

async fn run(options: ServerOptions) {
    linera_service::tracing::opentelemetry::init(
        &log_file_name_for(&options.command),
        otlp_exporter_endpoint_for(&options.command),
    );

    #[cfg(with_metrics)]
    let enable_memory_profiling = options.enable_memory_profiling();
    match options.command {
        ServerCommand::Run {
            server_config_path,
            storage_config,
            common_storage_options,
            cross_chain_config,
            notification_config,
            shard,
            block_time_grace_period,
            wasm_runtime,
            chain_worker_ttl,
            chain_info_max_received_log_entries,
            cross_chain_message_chunk_limit,
            allow_revert_confirm,
            reset_on_incorrect_outcome_mins,
            chain_worker_memory_limit,
            chain_worker_memory_monitor_interval_ms,
            otlp_exporter_endpoint: _,
        } => {
            linera_version::VERSION_INFO.log();

            let server_config: ValidatorServerConfig =
                util::read_json(&server_config_path).expect("Failed to read server config");

            let chain_worker_ttl =
                util::non_zero_duration(chain_worker_ttl).map(|d| Arc::new(DynamicTtl::new(d)));
            let job = ServerContext {
                server_config,
                cross_chain_config,
                notification_config,
                shard,
                block_time_grace_period,
                chain_worker_ttl,
                block_cache_size: options.block_cache_size,
                execution_state_cache_size: options.execution_state_cache_size,
                chain_info_max_received_log_entries,
                cross_chain_message_chunk_limit,
                allow_revert_confirm,
                reset_on_incorrect_outcome_mins,
                chain_worker_memory_limit,
                chain_worker_memory_monitor_interval_ms,
                #[cfg(with_metrics)]
                enable_memory_profiling,
            };
            let wasm_runtime = wasm_runtime.with_wasm_default();
            let store_config = storage_config
                .add_common_storage_options(&common_storage_options)
                .unwrap();
            // Validators should not output contract logs.
            let allow_application_logs = false;
            let cache_sizes = common_storage_options.storage_cache_config();
            store_config
                .run_with_storage(wasm_runtime, allow_application_logs, cache_sizes, job)
                .boxed()
                .await
                .unwrap()
                .unwrap();
        }

        ServerCommand::Generate {
            validators,
            committee,
            testing_prng_seed,
        } => {
            let mut config_validators = Vec::new();
            let mut rng = Box::<dyn CryptoRng>::from(testing_prng_seed);
            for options_path in validators {
                let options_string = fs_err::tokio::read_to_string(options_path)
                    .await
                    .expect("Unable to read validator options file");
                let options: ValidatorOptions =
                    toml::from_str(&options_string).unwrap_or_else(|_| {
                        panic!("Invalid options file format: \n {}", options_string)
                    });
                let path = options.server_config_path.clone();
                let mut server = make_server_config(&path, &mut rng, options)
                    .expect("Unable to open server config file");
                Persist::persist(&mut server)
                    .await
                    .expect("Unable to write server config file");
                info!("Wrote server config {}", path.to_str().unwrap());
                println!(
                    "{},{}",
                    server.validator.public_key, server.validator.account_key
                );
                config_validators.push(Persist::into_value(server).validator);
            }
            if let Some(committee) = committee {
                let mut config = persistent::File::new(
                    &committee,
                    CommitteeConfig {
                        validators: config_validators,
                    },
                )
                .expect("Unable to open committee configuration");
                Persist::persist(&mut config)
                    .await
                    .expect("Unable to write committee description");
                info!("Wrote committee config {}", committee.to_str().unwrap());
            }
        }

        ServerCommand::EditShards {
            server_config_path,
            num_shards,
            host,
            port,
            metrics_port,
        } => {
            let mut server_config =
                persistent::File::<ValidatorServerConfig>::read(&server_config_path)
                    .expect("Failed to read server config");
            let shards = generate_shard_configs(&num_shards, &host, &port, &metrics_port)
                .expect("Failed to generate shard configs");
            server_config.internal_network.shards = shards;
            Persist::persist(&mut server_config)
                .await
                .expect("Failed to write updated server config");
        }
    }
}

fn generate_shard_configs(
    num_shards: &str,
    host: &str,
    port: &str,
    metrics_port: &Option<String>,
) -> anyhow::Result<Vec<ShardConfig>> {
    let mut shards = Vec::new();
    let len = num_shards.len();
    let num_shards = num_shards
        .parse::<NonZeroU16>()
        .context("Failed to parse the number of shards")?;
    let pattern = "%".repeat(len);

    for i in 1u16..=num_shards.into() {
        let index = format!("{i:0len$}");
        let host = host.replacen(&pattern, &index, 1);
        let port = port
            .replacen(&pattern, &index, 1)
            .parse()
            .context("Failed to decode port into an integers")?;
        let metrics_port = metrics_port
            .as_ref()
            .map(|port| {
                port.replacen(&pattern, &index, 1)
                    .parse()
                    .context("Failed to decode metrics port into an integers")
            })
            .transpose()?;
        let shard = ShardConfig {
            host,
            port,
            metrics_port,
        };
        shards.push(shard);
    }
    Ok(shards)
}

#[cfg(test)]
mod test {
    use linera_rpc::simple::TransportProtocol;

    use super::*;

    #[test]
    fn test_validator_options() {
        let toml_str = r#"
            server_config_path = "server.json"
            host = "host"
            port = 9000
            external_protocol = { Simple = "Tcp" }
            internal_protocol = { Simple = "Udp" }

            [[proxies]]
            host = "proxy"
            public_port = 20100
            private_port = 20200
            metrics_port = 21100

            [[shards]]
            host = "host1"
            port = 9001
            metrics_port = 5001

            [[shards]]
            host = "host2"
            port = 9002
            metrics_port = 5002

            [[block_exporters]]
            host = "exporter"
            port = 12000
        "#;
        let options: ValidatorOptions = toml::from_str(toml_str).unwrap();
        assert_eq!(
            options,
            ValidatorOptions {
                server_config_path: "server.json".into(),
                external_protocol: NetworkProtocol::Simple(TransportProtocol::Tcp),
                internal_protocol: NetworkProtocol::Simple(TransportProtocol::Udp),
                host: "host".into(),
                port: 9000,
                proxies: vec![ProxyConfig {
                    host: "proxy".into(),
                    public_port: 20100,
                    private_port: 20200,
                    metrics_port: 21100,
                }],
                block_exporters: vec![ExporterServiceConfig {
                    host: "exporter".into(),
                    port: 12000
                }],
                shards: vec![
                    ShardConfig {
                        host: "host1".into(),
                        port: 9001,
                        metrics_port: Some(5001),
                    },
                    ShardConfig {
                        host: "host2".into(),
                        port: 9002,
                        metrics_port: Some(5002),
                    },
                ],
            }
        );
    }

    #[test]
    fn test_generate_shard_configs() {
        assert_eq!(
            generate_shard_configs("02", "host%%", "10%%", &Some("11%%".into())).unwrap(),
            vec![
                ShardConfig {
                    host: "host01".into(),
                    port: 1001,
                    metrics_port: Some(1101),
                },
                ShardConfig {
                    host: "host02".into(),
                    port: 1002,
                    metrics_port: Some(1102),
                },
            ],
        );

        assert!(generate_shard_configs("2", "host%%", "10%%", &Some("11%%".into())).is_err());
    }
}

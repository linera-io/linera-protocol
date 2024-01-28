// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::bail;
use async_trait::async_trait;
use futures::future::join_all;
use linera_base::crypto::{CryptoRng, KeyPair};
use linera_core::worker::WorkerState;
use linera_execution::{committee::ValidatorName, WasmRuntime, WithWasmDefault};
use linera_rpc::{
    config::{
        CrossChainConfig, NetworkProtocol, NotificationConfig, ShardConfig, ShardId, TlsConfig,
        ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig,
    },
    grpc_network::GrpcServer,
    simple_network,
    transport::TransportProtocol,
};
use linera_service::{
    config::{
        CommitteeConfig, Export, GenesisConfig, Import, ValidatorConfig, ValidatorServerConfig,
    },
    prometheus_server,
    storage::{full_initialize_storage, run_with_storage, Runnable, StorageConfig},
    util,
};
use linera_storage::Storage;
use linera_views::{common::CommonStoreConfig, views::ViewError};
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf, time::Duration};
use tracing::{error, info};

struct ServerContext {
    server_config: ValidatorServerConfig,
    cross_chain_config: CrossChainConfig,
    notification_config: NotificationConfig,
    shard: Option<usize>,
    grace_period: Duration,
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
        let state = WorkerState::new(
            format!("Shard {} @ {}:{}", shard_id, local_ip_addr, shard.port),
            Some(self.server_config.key.copy()),
            storage,
        )
        .with_allow_inactive_chains(false)
        .with_allow_messages_from_deprecated_epochs(false)
        .with_grace_period(self.grace_period);
        (state, shard_id, shard.clone())
    }

    async fn spawn_simple<S>(
        &self,
        listen_address: &str,
        states: Vec<(WorkerState<S>, ShardId, ShardConfig)>,
        protocol: TransportProtocol,
    ) -> Result<(), anyhow::Error>
    where
        S: Storage + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        let internal_network = self
            .server_config
            .internal_network
            .clone_with_protocol(protocol);

        let mut handles = Vec::new();
        for (state, shard_id, shard) in states {
            let internal_network = internal_network.clone();
            let cross_chain_config = self.cross_chain_config.clone();
            handles.push(async move {
                if let Some(port) = shard.metrics_port {
                    Self::start_metrics(listen_address, &port);
                }
                let server = simple_network::Server::new(
                    internal_network,
                    listen_address.to_string(),
                    shard.port,
                    state,
                    shard_id,
                    cross_chain_config,
                );
                let spawned_server = match server.spawn().await {
                    Ok(server) => server,
                    Err(err) => {
                        error!("Failed to start server: {}", err);
                        return;
                    }
                };
                if let Err(err) = spawned_server.join().await {
                    error!("Server ended with an error: {}", err);
                }
            });
        }

        join_all(handles).await;

        Ok(())
    }

    async fn spawn_grpc<S>(
        &self,
        listen_address: &str,
        states: Vec<(WorkerState<S>, ShardId, ShardConfig)>,
    ) -> Result<(), anyhow::Error>
    where
        S: Storage + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        let mut handles = Vec::new();
        for (state, shard_id, shard) in states {
            let cross_chain_config = self.cross_chain_config.clone();
            let notification_config = self.notification_config.clone();
            handles.push(async move {
                if let Some(port) = shard.metrics_port {
                    Self::start_metrics(listen_address, &port);
                }
                let spawned_server = match GrpcServer::spawn(
                    listen_address.to_string(),
                    shard.port,
                    state,
                    shard_id,
                    self.server_config.internal_network.clone(),
                    cross_chain_config,
                    notification_config,
                )
                .await
                {
                    Ok(spawned_server) => spawned_server,
                    Err(err) => {
                        error!("Failed to start server: {:?}", err);
                        return;
                    }
                };
                if let Err(err) = spawned_server.join().await {
                    error!("Server ended with an error: {}", err);
                }
            });
        }

        join_all(handles).await;

        Ok(())
    }

    fn start_metrics(host: &str, port: &u16) {
        match format!("{}:{}", host, port).parse::<SocketAddr>() {
            Err(err) => panic!("Invalid metrics address for {host}:{port}: {err}"),
            Ok(address) => prometheus_server::start_metrics(address),
        }
    }

    fn get_listen_address(&self) -> String {
        // Allow local IP address to be different from the public one.
        "0.0.0.0".to_string()
    }
}

#[async_trait]
impl Runnable for ServerContext {
    type Output = ();

    async fn run<S>(self, storage: S) -> Result<(), anyhow::Error>
    where
        S: Storage + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        let listen_address = self.get_listen_address();

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

        match self.server_config.internal_network.protocol {
            NetworkProtocol::Simple(protocol) => {
                self.spawn_simple(&listen_address, states, protocol).await?
            }
            NetworkProtocol::Grpc(tls_config) => match tls_config {
                TlsConfig::ClearText => self.spawn_grpc(&listen_address, states).await?,
                TlsConfig::Tls => bail!("TLS not supported between proxy and shards."),
            },
        };

        Ok(())
    }
}

#[derive(clap::Parser)]
#[command(
    name = "linera-server",
    about = "A byzantine fault tolerant payments sidechain with low-latency finality and high throughput",
    version = linera_base::VersionInfo::default_str(),
)]
struct ServerOptions {
    /// Subcommands. Acceptable values are run and generate.
    #[command(subcommand)]
    command: ServerCommand,

    /// The number of Tokio worker threads to use.
    #[arg(long, env = "LINERA_SERVER_TOKIO_THREADS")]
    tokio_threads: Option<usize>,
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
struct ValidatorOptions {
    /// Path to the file containing the server configuration of this Linera validator (including its secret key)
    server_config_path: PathBuf,

    /// The host of the validator (IP address or hostname)
    host: String,

    /// The port of the validator
    port: u16,

    /// The host for the metrics endpoint
    metrics_host: String,

    /// The port for the metrics endpoint
    metrics_port: u16,

    /// The host of the proxy in the internal network.
    internal_host: String,

    /// The port of the proxy on the internal network.
    internal_port: u16,

    /// The network protocol for the frontend.
    external_protocol: NetworkProtocol,

    /// The network protocol for workers.
    internal_protocol: NetworkProtocol,

    /// The public name and the port of each of the shards
    shards: Vec<ShardConfig>,
}

fn make_server_config<R: CryptoRng>(
    rng: &mut R,
    options: ValidatorOptions,
) -> ValidatorServerConfig {
    let network = ValidatorPublicNetworkConfig {
        protocol: options.external_protocol,
        host: options.host,
        port: options.port,
    };
    let internal_network = ValidatorInternalNetworkConfig {
        protocol: options.internal_protocol,
        shards: options.shards,
        host: options.internal_host,
        port: options.internal_port,
        metrics_host: options.metrics_host,
        metrics_port: options.metrics_port,
    };
    let key = KeyPair::generate_from(rng);
    let name = ValidatorName(key.public());
    let validator = ValidatorConfig { network, name };
    ValidatorServerConfig {
        validator,
        key,
        internal_network,
    }
}

#[derive(clap::Parser)]
enum ServerCommand {
    /// Runs a service for each shard of the Linera validator")
    #[command(name = "run")]
    Run {
        /// Path to the file containing the server configuration of this Linera validator (including its secret key)
        #[arg(long = "server")]
        server_config_path: PathBuf,

        /// Storage configuration for the blockchain history and security states.
        #[arg(long = "storage")]
        storage_config: StorageConfig,

        /// Configuration for cross-chain requests
        #[command(flatten)]
        cross_chain_config: CrossChainConfig,

        /// Configuration for notifications
        #[command(flatten)]
        notification_config: NotificationConfig,

        /// Path to the file describing the initial user chains (aka genesis state)
        #[arg(long = "genesis")]
        genesis_config_path: PathBuf,

        /// Runs a specific shard (from 0 to shards-1)
        #[arg(long)]
        shard: Option<usize>,

        /// Blocks with a timestamp this far in the future will still be accepted, but the validator
        /// will wait until that timestamp before voting.
        #[arg(long = "grace-period-ms", default_value = "500", value_parser = util::parse_millis)]
        grace_period: Duration,

        /// The WebAssembly runtime to use.
        #[arg(long)]
        wasm_runtime: Option<WasmRuntime>,

        /// The maximal number of simultaneous queries to the database
        #[arg(long)]
        max_concurrent_queries: Option<usize>,

        /// The maximal number of stream queries to the database
        #[arg(long, default_value = "10")]
        max_stream_queries: usize,

        /// The maximal number of entries in the storage cache.
        #[arg(long, default_value = "1000")]
        cache_size: usize,
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

    /// Initialize the database
    #[command(name = "initialize")]
    Initialize {
        /// Storage configuration for the blockchain history and security states.
        #[arg(long = "storage")]
        storage_config: StorageConfig,

        /// Path to the file describing the initial user chains (aka genesis state)
        #[arg(long = "genesis")]
        genesis_config_path: PathBuf,

        /// The maximal number of simultaneous queries to the database
        #[arg(long)]
        max_concurrent_queries: Option<usize>,

        /// The maximal number of stream queries to the database
        #[arg(long, default_value = "10")]
        max_stream_queries: usize,

        /// The maximal number of entries in the storage cache.
        #[arg(long, default_value = "1000")]
        cache_size: usize,
    },
}

fn main() {
    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(env_filter)
        .init();

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

    runtime
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
        .block_on(run(options))
}

async fn run(options: ServerOptions) {
    linera_base::VERSION_INFO.log();

    match options.command {
        ServerCommand::Run {
            server_config_path,
            storage_config,
            cross_chain_config,
            notification_config,
            genesis_config_path,
            shard,
            grace_period,
            wasm_runtime,
            max_concurrent_queries,
            max_stream_queries,
            cache_size,
        } => {
            let genesis_config = GenesisConfig::read(&genesis_config_path)
                .expect("Fail to read initial chain config");
            let server_config = ValidatorServerConfig::read(&server_config_path)
                .expect("Fail to read server config");

            #[cfg(feature = "rocksdb")]
            if server_config.internal_network.shards.len() > 1
                && matches!(storage_config, StorageConfig::RocksDb { .. })
            {
                panic!("Multiple shards not supported with RocksDB");
            }

            let job = ServerContext {
                server_config,
                cross_chain_config,
                notification_config,
                shard,
                grace_period,
            };
            let wasm_runtime = wasm_runtime.with_wasm_default();
            let common_config = CommonStoreConfig {
                max_concurrent_queries,
                max_stream_queries,
                cache_size,
            };
            let full_storage_config = storage_config
                .add_common_config(common_config)
                .await
                .unwrap();
            run_with_storage(full_storage_config, &genesis_config, wasm_runtime, job)
                .await
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
                    toml::from_str(&options_string).expect("Invalid options file format");
                let path = options.server_config_path.clone();
                let server = make_server_config(&mut rng, options);
                server
                    .write(&path)
                    .expect("Unable to write server config file");
                info!("Wrote server config {}", path.to_str().unwrap());
                println!("{}", server.validator.name);
                config_validators.push(server.validator);
            }
            if let Some(committee) = committee {
                let config = CommitteeConfig {
                    validators: config_validators,
                };
                config
                    .write(&committee)
                    .expect("Unable to write committee description");
                info!("Wrote committee config {}", committee.to_str().unwrap());
            }
        }

        ServerCommand::Initialize {
            storage_config,
            genesis_config_path,
            max_concurrent_queries,
            max_stream_queries,
            cache_size,
        } => {
            let genesis_config = GenesisConfig::read(&genesis_config_path)
                .expect("Fail to read initial chain config");
            let common_config = CommonStoreConfig {
                max_concurrent_queries,
                max_stream_queries,
                cache_size,
            };
            let full_storage_config = storage_config
                .add_common_config(common_config)
                .await
                .unwrap();
            full_initialize_storage(full_storage_config, &genesis_config)
                .await
                .unwrap();
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use linera_rpc::transport::TransportProtocol;

    #[test]
    fn test_validator_options() {
        let toml_str = r#"
            server_config_path = "server.json"
            host = "host"
            port = 9000
            internal_host = "internal_host"
            internal_port = 10000
            metrics_host = "metrics_host"
            metrics_port = 5000
            external_protocol = { Simple = "Tcp" }
            internal_protocol = { Simple = "Udp" }

            [[shards]]
            host = "host1"
            port = 9001
            metrics_host = "metrics_host1"
            metrics_port = 5001

            [[shards]]
            host = "host2"
            port = 9002
            metrics_host = "metrics_host2"
            metrics_port = 5002
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
                internal_host: "internal_host".into(),
                internal_port: 10000,
                metrics_host: "metrics_host".into(),
                metrics_port: 5000,
                shards: vec![
                    ShardConfig {
                        host: "host1".into(),
                        port: 9001,
                        metrics_host: "metrics_host1".into(),
                        metrics_port: Some(5001),
                    },
                    ShardConfig {
                        host: "host2".into(),
                        port: 9002,
                        metrics_host: "metrics_host2".into(),
                        metrics_port: Some(5002),
                    },
                ],
            }
        );
    }
}

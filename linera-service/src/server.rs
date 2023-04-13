// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use futures::future::join_all;
use linera_base::crypto::KeyPair;
use linera_core::worker::WorkerState;
use linera_execution::{committee::ValidatorName, WasmRuntime, WithWasmDefault};
use linera_rpc::{
    config::{
        CrossChainConfig, NetworkProtocol, NotificationConfig, ShardConfig, ShardId,
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
    storage::{Runnable, StorageConfig},
};
use linera_storage::Store;
use linera_views::views::ViewError;
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf};
use structopt::StructOpt;
use tokio::fs;
use tracing::{error, info};

struct ServerContext {
    server_config: ValidatorServerConfig,
    cross_chain_config: CrossChainConfig,
    notification_config: NotificationConfig,
    shard: Option<usize>,
    grace_period_micros: u64,
}

impl ServerContext {
    fn make_shard_state<S>(
        &self,
        local_ip_addr: &str,
        shard_id: ShardId,
        storage: S,
    ) -> (WorkerState<S>, ShardId, ShardConfig)
    where
        S: Store + Clone + Send + Sync + 'static,
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
        .with_grace_period_micros(self.grace_period_micros);
        (state, shard_id, shard.clone())
    }

    async fn spawn_simple<S>(
        &self,
        listen_address: &str,
        states: Vec<(WorkerState<S>, ShardId, ShardConfig)>,
        protocol: TransportProtocol,
    ) -> Result<(), anyhow::Error>
    where
        S: Store + Clone + Send + Sync + 'static,
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
                    let host = &shard.metrics_host;
                    match format!("{}:{}", host, port).parse::<SocketAddr>() {
                        Err(err) => error!("Invalid metrics address: {err}"),
                        Ok(address) => {
                            if let Err(error) = metrics_exporter_tcp::TcpBuilder::new()
                                .listen_address(address)
                                .install()
                            {
                                tracing::warn!(
                                    ?error, %address,
                                    "Could not install TCP metrics exporter."
                                );
                            }
                        }
                    }
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
        S: Store + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        let mut handles = Vec::new();
        for (state, shard_id, shard) in states {
            let cross_chain_config = self.cross_chain_config.clone();
            let notification_config = self.notification_config.clone();
            handles.push(async move {
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
}

#[async_trait]
impl<S> Runnable<S> for ServerContext
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    type Output = ();

    async fn run(self, storage: S) -> Result<(), anyhow::Error> {
        // Allow local IP address to be different from the public one.
        let listen_address = "0.0.0.0";
        // Run the server
        let states = match self.shard {
            Some(shard) => {
                info!("Running shard number {}", shard);
                vec![self.make_shard_state(listen_address, shard, storage)]
            }
            None => {
                info!("Running all shards");
                let num_shards = self.server_config.internal_network.shards.len();
                (0..num_shards)
                    .into_iter()
                    .map(|shard| self.make_shard_state(listen_address, shard, storage.clone()))
                    .collect()
            }
        };

        match self.server_config.internal_network.protocol {
            NetworkProtocol::Simple(protocol) => {
                self.spawn_simple(listen_address, states, protocol).await?
            }
            NetworkProtocol::Grpc => self.spawn_grpc(listen_address, states).await?,
        };

        Ok(())
    }
}

#[derive(StructOpt)]
#[structopt(
    name = "Linera Server",
    about = "A byzantine fault tolerant payments sidechain with low-latency finality and high throughput"
)]
struct ServerOptions {
    /// Subcommands. Acceptable values are run and generate.
    #[structopt(subcommand)]
    command: ServerCommand,
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
struct ValidatorOptions {
    /// Path to the file containing the server configuration of this Linera validator (including its secret key)
    server_config_path: PathBuf,

    /// The host of the validator (IP address or hostname)
    host: String,

    /// The port of the validator
    port: u16,

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

fn make_server_config(options: ValidatorOptions) -> ValidatorServerConfig {
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
    };
    let key = KeyPair::generate();
    let name = ValidatorName(key.public());
    let validator = ValidatorConfig { network, name };
    ValidatorServerConfig {
        validator,
        key,
        internal_network,
    }
}

#[derive(StructOpt)]
enum ServerCommand {
    /// Runs a service for each shard of the Linera validator")
    #[structopt(name = "run")]
    Run {
        /// Path to the file containing the server configuration of this Linera validator (including its secret key)
        #[structopt(long = "server")]
        server_config_path: PathBuf,

        /// Storage configuration for the blockchain history and security states.
        #[structopt(long = "storage")]
        storage_config: StorageConfig,

        /// Configuration for cross-chain requests
        #[structopt(flatten)]
        cross_chain_config: CrossChainConfig,

        /// Configuration for notifications
        #[structopt(flatten)]
        notification_config: NotificationConfig,

        /// Path to the file describing the initial user chains (aka genesis state)
        #[structopt(long = "genesis")]
        genesis_config_path: PathBuf,

        /// Runs a specific shard (from 0 to shards-1)
        #[structopt(long)]
        shard: Option<usize>,

        /// Blocks with a timestamp this far in the future will still be accepted, but the validator
        /// will wait until that timestamp before voting.
        #[structopt(long, default_value = "500ms", parse(try_from_str = parse_duration))]
        grace_period: u64,

        /// The WebAssembly runtime to use.
        #[structopt(long)]
        wasm_runtime: Option<WasmRuntime>,
    },

    /// Act as a trusted third-party and generate all server configurations
    #[structopt(name = "generate")]
    Generate {
        /// Configuration file of each validator in the committee
        #[structopt(long)]
        validators: Vec<PathBuf>,

        /// Path where to write the description of the Linera committee
        #[structopt(long)]
        committee: Option<PathBuf>,
    },
}

/// Parses a string into a duration and returns the duration in microseconds.
fn parse_duration(s: &str) -> Result<u64, parse_duration::parse::Error> {
    Ok(parse_duration::parse(s)?
        .as_micros()
        .try_into()
        .unwrap_or(u64::MAX))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let options = ServerOptions::from_args();

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
        } => {
            let genesis_config = GenesisConfig::read(&genesis_config_path)
                .expect("Fail to read initial chain config");
            let server_config = ValidatorServerConfig::read(&server_config_path)
                .expect("Fail to read server config");
            let job = ServerContext {
                server_config,
                cross_chain_config,
                notification_config,
                shard,
                grace_period_micros: grace_period,
            };
            let wasm_runtime = wasm_runtime.with_wasm_default();
            storage_config
                .run_with_storage(&genesis_config, wasm_runtime, job)
                .await
                .unwrap();
        }

        ServerCommand::Generate {
            validators,
            committee,
        } => {
            let mut config_validators = Vec::new();
            for options_path in validators {
                let options_string = fs::read_to_string(options_path)
                    .await
                    .expect("Unable to read validator options file");
                let options: ValidatorOptions =
                    toml::from_str(&options_string).expect("Invalid options file format");
                let path = options.server_config_path.clone();
                let server = make_server_config(options);
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

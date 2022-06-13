// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![deny(warnings)]

use anyhow::{anyhow, ensure};
use futures::future::join_all;
use log::*;
use std::{
    path::{Path, PathBuf},
    str::FromStr,
};
use structopt::StructOpt;
use zef_base::{crypto::*, messages::ValidatorName};
use zef_core::worker::*;
use zef_service::{
    config::*,
    network,
    network::{ShardConfig, ShardId, ValidatorNetworkConfig},
    storage::{make_storage, MixedStorage},
    transport,
};

#[allow(clippy::too_many_arguments)]
async fn make_shard_server(
    local_ip_addr: &str,
    server_config: &ValidatorServerConfig,
    cross_chain_config: network::CrossChainConfig,
    shard_id: ShardId,
    storage: MixedStorage,
) -> network::Server<MixedStorage> {
    let shard = server_config.validator.network.shard(shard_id);
    info!("Shard booted on {}", shard.host);
    let state = WorkerState::new(
        format!("Shard {} @ {}:{}", shard_id, local_ip_addr, shard.port),
        Some(server_config.key.copy()),
        storage,
    )
    .allow_inactive_chains(false);
    network::Server::new(
        server_config.validator.network.clone(),
        local_ip_addr.to_string(),
        shard.port,
        state,
        shard_id,
        cross_chain_config,
    )
}

async fn make_servers(
    local_ip_addr: &str,
    server_config: &ValidatorServerConfig,
    genesis_config: &GenesisConfig,
    cross_chain_config: network::CrossChainConfig,
    storage: Option<&PathBuf>,
) -> Vec<network::Server<MixedStorage>> {
    let num_shards = server_config.validator.network.shards.len();
    let mut servers = Vec::new();
    for shard in 0..num_shards {
        let storage = make_storage(storage, genesis_config).await.unwrap();
        let server = make_shard_server(
            local_ip_addr,
            server_config,
            cross_chain_config.clone(),
            shard,
            storage,
        )
        .await;
        servers.push(server)
    }
    servers
}

#[derive(StructOpt)]
#[structopt(
    name = "Zef Server",
    about = "A byzantine fault tolerant payments sidechain with low-latency finality and high throughput"
)]
struct ServerOptions {
    /// Subcommands. Acceptable values are run and generate.
    #[structopt(subcommand)]
    cmd: ServerCommands,
}

#[derive(Debug, PartialEq, Eq)]
struct ValidatorOptions {
    /// Path to the file containing the server configuration of this Zef validator (including its secret key)
    server_config_path: PathBuf,

    /// The network protocol: either Udp or Tcp
    protocol: transport::NetworkProtocol,

    /// The address of the validator (IP address or hostname)
    address: String,

    /// The port of the validator
    port: u16,

    /// The public name and the port of each of the shards
    shards: Vec<ShardConfig>,
}

impl FromStr for ValidatorOptions {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        ensure!(
            parts.len() >= 4 && parts.len() % 2 == 0,
            "Expecting format `file.json:host:port:(udp|tcp):host1:port1:...:hostN:portN`"
        );

        let server_config_path = Path::new(parts[0]).to_path_buf();
        let address = parts[1].to_owned();
        let port = parts[2].parse()?;
        let protocol = parts[3].parse().map_err(|s| anyhow!("{}", s))?;
        let mut shards = Vec::new();
        for i in 2..parts.len() / 2 {
            let host = parts[2 * i].to_string();
            let port = parts[2 * i + 1].parse()?;
            shards.push(ShardConfig { host, port });
        }
        Ok(Self {
            server_config_path,
            protocol,
            address,
            port,
            shards,
        })
    }
}

fn make_server_config(options: ValidatorOptions) -> ValidatorServerConfig {
    let network = ValidatorNetworkConfig {
        protocol: options.protocol,
        address: options.address,
        port: options.port,
        shards: options.shards,
    };
    let key = KeyPair::generate();
    let name = ValidatorName(key.public());
    let validator = ValidatorConfig { network, name };
    ValidatorServerConfig { validator, key }
}

#[derive(StructOpt)]
enum ServerCommands {
    /// Runs a service for each shard of the Zef validator")
    #[structopt(name = "run")]
    Run {
        /// Path to the file containing the server configuration of this Zef validator (including its secret key)
        #[structopt(long = "server")]
        server_config_path: PathBuf,

        /// Optional directory containing the on-disk database
        #[structopt(long = "storage")]
        storage_path: Option<PathBuf>,

        /// Configuration for cross-chain requests
        #[structopt(flatten)]
        cross_chain_config: network::CrossChainConfig,

        /// Path to the file describing the initial user chains (aka genesis state)
        #[structopt(long = "genesis")]
        genesis_config_path: PathBuf,

        /// Runs a specific shard (from 0 to shards-1)
        #[structopt(long)]
        shard: Option<usize>,
    },

    /// Act as a trusted third-party and generate all server configurations
    #[structopt(name = "generate-all")]
    GenerateAll {
        /// Configuration of each validator in the committee encoded as `(Udp|Tcp):host:port:num-shards`
        #[structopt(long)]
        validators: Vec<ValidatorOptions>,

        /// Path where to write the description of the Zef committee
        #[structopt(long)]
        committee: PathBuf,
    },
}

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();
    let options = ServerOptions::from_args();

    match options.cmd {
        ServerCommands::Run {
            server_config_path,
            storage_path,
            cross_chain_config,
            genesis_config_path,
            shard,
        } => {
            let genesis_config = GenesisConfig::read(&genesis_config_path)
                .expect("Fail to read initial chain config");
            let server_config = ValidatorServerConfig::read(&server_config_path)
                .expect("Fail to read server config");

            // Run the server
            let servers = match shard {
                Some(shard) => {
                    info!("Running shard number {}", shard);
                    let storage = make_storage(storage_path.as_ref(), &genesis_config)
                        .await
                        .unwrap();
                    let server = make_shard_server(
                        "0.0.0.0", // Allow local IP address to be different from the public one.
                        &server_config,
                        cross_chain_config,
                        shard,
                        storage,
                    )
                    .await;
                    vec![server]
                }
                None => {
                    info!("Running all shards");
                    make_servers(
                        "0.0.0.0", // Allow local IP address to be different from the public one.
                        &server_config,
                        &genesis_config,
                        cross_chain_config,
                        storage_path.as_ref(),
                    )
                    .await
                }
            };

            let mut handles = Vec::new();
            for server in servers {
                handles.push(async move {
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
        }

        ServerCommands::GenerateAll {
            validators,
            committee,
        } => {
            let mut config_validators = Vec::new();
            for options in validators {
                let path = options.server_config_path.clone();
                let server = make_server_config(options);
                server
                    .write(&path)
                    .expect("Unable to write server config file");
                info!("Wrote server config {}", path.to_str().unwrap());
                config_validators.push(server.validator);
            }
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_validator_options() {
        let options =
            ValidatorOptions::from_str("server.json:host:9000:udp:host1:9001:host2:9002").unwrap();
        assert_eq!(
            options,
            ValidatorOptions {
                server_config_path: "server.json".into(),
                protocol: transport::NetworkProtocol::Udp,
                address: "host".into(),
                port: 9000,
                shards: vec![
                    ShardConfig {
                        host: "host1".into(),
                        port: 9001,
                    },
                    ShardConfig {
                        host: "host2".into(),
                        port: 9002,
                    },
                ],
            }
        );
    }
}

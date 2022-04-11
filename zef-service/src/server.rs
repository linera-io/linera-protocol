// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![deny(warnings)]

use futures::future::join_all;
use log::*;
use std::{
    path::{Path, PathBuf},
    str::FromStr,
};
use structopt::StructOpt;
use zef_base::{base_types::*};
use zef_core::{worker::*};
use zef_service::{
    config::*,
    network,
    storage::{make_storage, Storage},
    transport,
};

#[allow(clippy::too_many_arguments)]
async fn make_shard_server(
    local_ip_addr: &str,
    server_config: &AuthorityServerConfig,
    buffer_size: usize,
    cross_shard_config: network::CrossShardConfig,
    shard: u32,
    storage: Storage,
) -> network::Server<Storage> {
    // NOTE: This log entry is used to compute performance.
    info!("Shard booted on {}", server_config.authority.host);
    let num_shards = server_config.authority.num_shards;
    let state = WorkerState::new(Some(server_config.key.copy()), storage);
    network::Server::new(
        server_config.authority.network_protocol,
        local_ip_addr.to_string(),
        server_config.authority.base_port,
        state,
        shard,
        num_shards,
        buffer_size,
        cross_shard_config,
    )
}

async fn make_servers(
    local_ip_addr: &str,
    server_config: &AuthorityServerConfig,
    genesis_config: &GenesisConfig,
    buffer_size: usize,
    cross_shard_config: network::CrossShardConfig,
    storage: Option<&PathBuf>,
) -> Vec<network::Server<Storage>> {
    let num_shards = server_config.authority.num_shards;
    let mut servers = Vec::new();
    // TODO: create servers in parallel
    for shard in 0..num_shards {
        let storage = make_storage(storage, genesis_config).await.unwrap();
        let server = make_shard_server(
            local_ip_addr,
            server_config,
            buffer_size,
            cross_shard_config.clone(),
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

#[derive(StructOpt, Debug, PartialEq, Eq)]
struct AuthorityOptions {
    /// Path to the file containing the server configuration of this Zef authority (including its secret key)
    #[structopt(long = "server")]
    server_config_path: PathBuf,

    /// Chooses a network protocol between Udp and Tcp
    #[structopt(long, default_value = "Tcp")]
    protocol: transport::NetworkProtocol,

    /// Sets the public name of the host
    #[structopt(long)]
    host: String,

    /// Sets the base port, i.e. the port on which the server listens for the first shard
    #[structopt(long)]
    port: u32,

    /// Number of shards for this authority
    #[structopt(long)]
    shards: u32,
}

impl FromStr for AuthorityOptions {
    type Err = failure::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        failure::ensure!(
            parts.len() == 5,
            "Expecting format `file.json:(udp|tcp):host:port:num-shards`"
        );

        let server_config_path = Path::new(parts[0]).to_path_buf();
        let protocol = parts[1]
            .parse()
            .map_err(|s| failure::format_err!("{}", s))?;
        let host = parts[2].to_string();
        let port = parts[3].parse()?;
        let shards = parts[4].parse()?;

        Ok(Self {
            server_config_path,
            protocol,
            host,
            port,
            shards,
        })
    }
}

fn make_server_config(options: AuthorityOptions) -> AuthorityServerConfig {
    let key = KeyPair::generate();
    let name = key.public();
    let authority = AuthorityConfig {
        network_protocol: options.protocol,
        name,
        host: options.host,
        base_port: options.port,
        num_shards: options.shards,
    };
    AuthorityServerConfig { authority, key }
}

#[derive(StructOpt)]
enum ServerCommands {
    /// Runs a service for each shard of the Zef authority")
    #[structopt(name = "run")]
    Run {
        /// Path to the file containing the server configuration of this Zef authority (including its secret key)
        #[structopt(long = "server")]
        server_config_path: PathBuf,

        /// Optional directory containing the on-disk database
        #[structopt(long = "storage")]
        storage_path: Option<PathBuf>,

        /// Maximum size of datagrams received and sent (bytes)
        #[structopt(long, default_value = transport::DEFAULT_MAX_DATAGRAM_SIZE)]
        buffer_size: usize,

        /// Configuration for cross shard requests
        #[structopt(flatten)]
        cross_shard_config: network::CrossShardConfig,

        /// Path to the file describing the initial user accounts (aka genesis state)
        #[structopt(long = "genesis")]
        genesis_config_path: PathBuf,

        /// Runs a specific shard (from 0 to shards-1)
        #[structopt(long)]
        shard: Option<u32>,
    },

    /// Generate a new server configuration and output its public description
    #[structopt(name = "generate")]
    Generate {
        #[structopt(flatten)]
        options: AuthorityOptions,
    },

    /// Act as a trusted third-party and generate all server configurations
    #[structopt(name = "generate-all")]
    GenerateAll {
        /// Configuration of each authority in the committee encoded as `(Udp|Tcp):host:port:num-shards`
        #[structopt(long)]
        authorities: Vec<AuthorityOptions>,

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
            buffer_size,
            cross_shard_config,
            genesis_config_path,
            shard,
        } => {
            #[cfg(feature = "benchmark")]
            warn!("The server is running in benchmark mode: Do not use it in production");

            let genesis_config = GenesisConfig::read(&genesis_config_path)
                .expect("Fail to read initial account config");
            let server_config = AuthorityServerConfig::read(&server_config_path)
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
                        buffer_size,
                        cross_shard_config,
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
                        buffer_size,
                        cross_shard_config,
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

        ServerCommands::Generate { options } => {
            let path = options.server_config_path.clone();
            let server = make_server_config(options);
            server
                .write(&path)
                .expect("Unable to write server config file");
            info!("Wrote server config file");
            server.authority.print();
        }

        ServerCommands::GenerateAll {
            authorities,
            committee,
        } => {
            let mut config_authorities = Vec::new();
            for options in authorities {
                let path = options.server_config_path.clone();
                let server = make_server_config(options);
                server
                    .write(&path)
                    .expect("Unable to write server config file");
                #[cfg(not(feature = "benchmark"))]
                info!("Wrote server config {}", path.to_str().unwrap());
                config_authorities.push(server.authority);
            }
            let config = CommitteeConfig {
                authorities: config_authorities,
            };
            config
                .write(&committee)
                .expect("Unable to write committee description");
            #[cfg(not(feature = "benchmark"))]
            info!("Wrote committee config {}", committee.to_str().unwrap());
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_authority_options() {
        let options = AuthorityOptions::from_str("server.json:udp:localhost:9001:2").unwrap();
        assert_eq!(
            options,
            AuthorityOptions {
                server_config_path: "server.json".into(),
                protocol: transport::NetworkProtocol::Udp,
                host: "localhost".into(),
                port: 9001,
                shards: 2
            }
        );
    }
}

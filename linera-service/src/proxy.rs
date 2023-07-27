// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{bail, Result};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use linera_rpc::{
    config::{
        NetworkProtocol, ShardConfig, ValidatorInternalNetworkPreConfig,
        ValidatorPublicNetworkPreConfig,
    },
    transport::{MessageHandler, TransportProtocol},
    RpcMessage,
};
use linera_service::{
    config::{Import, ValidatorServerConfig},
    grpc_proxy::GrpcProxy,
    kubernetes::{fetch_pod_ips, find_value_with_key_prefix},
};
use std::{path::PathBuf, time::Duration};
use structopt::StructOpt;
use tracing::{error, info, instrument};

/// Options for running the proxy.
#[derive(Debug, StructOpt)]
#[structopt(
    name = "Linera Proxy",
    about = "A proxy to redirect incoming requests to Linera Server shards"
)]
pub struct ProxyOptions {
    /// Path to server configuration.
    config_path: PathBuf,

    /// Timeout for sending queries (us)
    #[structopt(long, default_value = "4000000")]
    send_timeout_us: u64,

    /// Timeout for receiving responses (us)
    #[structopt(long, default_value = "4000000")]
    recv_timeout_us: u64,

    /// Use this when running from a Kubernetes cluster (including from within GCP)
    /// Won't use config files to determine host IPs, but will do it dynamically
    /// based on cluster info
    #[structopt(long)]
    kube: bool,
}

/// A Linera Proxy, either gRPC or over 'Simple Transport', meaning TCP or UDP.
/// The proxy can be configured to have a gRPC ingress and egress, or a combination
/// of TCP / UDP ingress and egress.
enum Proxy {
    Simple(SimpleProxy),
    Grpc(GrpcProxy),
}

impl Proxy {
    /// Run the proxy.
    async fn run(self) -> Result<()> {
        match self {
            Proxy::Simple(simple_proxy) => simple_proxy.run().await,
            Proxy::Grpc(grpc_proxy) => grpc_proxy.run().await,
        }
    }

    /// Constructs and configures the [`Proxy`] given [`ProxyOptions`].
    async fn from_options(options: ProxyOptions) -> Result<Self> {
        let mut tmp_config = ValidatorServerConfig::read(&options.config_path)?;
        if options.kube {
            let pod_name_to_ip = fetch_pod_ips().await?;

            // Assumes that each service will have just one pod. Since we have one service per shard, the first pod will always have
            // -0 at the end of it for servers and some hash for the validators. So we check the prefix and assume only one entry
            // will have that prefix.
            tmp_config.validator.network.host =
                find_value_with_key_prefix(&pod_name_to_ip, &tmp_config.validator.network.host)
                    .expect("All hosts should be a prefix of a pod name!");
            tmp_config.internal_network.host =
                find_value_with_key_prefix(&pod_name_to_ip, &tmp_config.internal_network.host)
                    .expect("All hosts should be a prefix of a pod name!");
            for shard in &mut tmp_config.internal_network.shards {
                shard.host = find_value_with_key_prefix(&pod_name_to_ip, &shard.host)
                    .expect("All hosts should be a prefix of a pod name!");
                shard.metrics_host =
                    find_value_with_key_prefix(&pod_name_to_ip, &shard.metrics_host)
                        .expect("All hosts should be a prefix of a pod name!");
            }
        }
        let config = tmp_config;
        let internal_protocol = config.internal_network.protocol;
        let external_protocol = config.validator.network.protocol;

        let proxy = match (internal_protocol, external_protocol) {
            (NetworkProtocol::Grpc, NetworkProtocol::Grpc) => Self::Grpc(GrpcProxy::new(
                config.validator.network,
                config.internal_network,
                Duration::from_micros(options.send_timeout_us),
                Duration::from_micros(options.recv_timeout_us),
            )),
            (
                NetworkProtocol::Simple(internal_transport),
                NetworkProtocol::Simple(public_transport),
            ) => Self::Simple(SimpleProxy {
                internal_config: config
                    .internal_network
                    .clone_with_protocol(internal_transport),
                public_config: config
                    .validator
                    .network
                    .clone_with_protocol(public_transport),
                send_timeout: Duration::from_micros(options.send_timeout_us),
                recv_timeout: Duration::from_micros(options.recv_timeout_us),
                kube: options.kube,
            }),
            _ => {
                bail!(
                    "network protocol mismatch: cannot have {} and {} ",
                    internal_protocol,
                    external_protocol,
                );
            }
        };

        Ok(proxy)
    }
}

#[derive(Debug, Clone)]
pub struct SimpleProxy {
    public_config: ValidatorPublicNetworkPreConfig<TransportProtocol>,
    internal_config: ValidatorInternalNetworkPreConfig<TransportProtocol>,
    send_timeout: Duration,
    recv_timeout: Duration,
    kube: bool,
}

#[async_trait]
impl MessageHandler for SimpleProxy {
    #[instrument(skip_all, fields(chain_id = ?message.target_chain_id()))]
    async fn handle_message(&mut self, message: RpcMessage) -> Option<RpcMessage> {
        let Some(chain_id) = message.target_chain_id() else {
            error!("Can't proxy unexpected message");
            return None;
        };
        let shard = self.internal_config.get_shard_for(chain_id).clone();
        let protocol = self.internal_config.protocol;

        match Self::try_proxy_message(
            message,
            shard,
            protocol,
            self.send_timeout,
            self.recv_timeout,
        )
        .await
        {
            Ok(maybe_response) => maybe_response,
            Err(error) => {
                error!(error = %error, "Failed to proxy message");
                None
            }
        }
    }
}

impl SimpleProxy {
    #[instrument(skip_all, fields(port = self.public_config.port), err)]
    async fn run(self) -> Result<()> {
        info!("Starting simple server");
        let address = if !self.kube {
            format!("0.0.0.0:{}", self.public_config.port)
        } else {
            format!(
                "{}:{}",
                std::env::var("MY_POD_IP").expect("Could not get env variable MY_POD_IP"),
                self.public_config.port
            )
        };
        self.public_config
            .protocol
            .spawn_server(&address, self)
            .await?
            .join()
            .await?;
        Ok(())
    }

    async fn try_proxy_message(
        message: RpcMessage,
        shard: ShardConfig,
        protocol: TransportProtocol,
        send_timeout: Duration,
        recv_timeout: Duration,
    ) -> Result<Option<RpcMessage>> {
        let shard_address = format!("{}:{}", shard.host, shard.port);
        let mut connection = protocol.connect(shard_address).await?;
        tokio::time::timeout(send_timeout, connection.send(message)).await??;
        let message = tokio::time::timeout(recv_timeout, connection.next())
            .await?
            .transpose()?;
        Ok(message)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(env_filter)
        .init();

    let proxy = Proxy::from_options(ProxyOptions::from_args()).await?;
    proxy.run().await
}

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
    prometheus_server,
};
use std::{net::SocketAddr, path::PathBuf, time::Duration};
use tracing::{error, info, instrument};

/// Options for running the proxy.
#[derive(clap::Parser, Debug)]
#[command(
    name = "Linera Proxy",
    about = "A proxy to redirect incoming requests to Linera Server shards",
    version = clap::crate_version!(),
)]
pub struct ProxyOptions {
    /// Path to server configuration.
    config_path: PathBuf,

    /// Timeout for sending queries (us)
    #[arg(long, default_value = "4000000")]
    send_timeout_us: u64,

    /// Timeout for receiving responses (us)
    #[arg(long, default_value = "4000000")]
    recv_timeout_us: u64,

    /// The number of Tokio worker threads to use.
    #[arg(long, env = "LINERA_PROXY_TOKIO_THREADS")]
    tokio_threads: Option<usize>,
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
        let config = ValidatorServerConfig::read(&options.config_path)?;

        let internal_protocol = config.internal_network.protocol;
        let external_protocol = config.validator.network.protocol;
        let proxy = match (internal_protocol, external_protocol) {
            (NetworkProtocol::Grpc { .. }, NetworkProtocol::Grpc(tls)) => {
                Self::Grpc(GrpcProxy::new(
                    config.validator.network,
                    config.internal_network,
                    Duration::from_micros(options.send_timeout_us),
                    Duration::from_micros(options.recv_timeout_us),
                    tls,
                ))
            }
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
    #[instrument(skip_all, fields(port = self.public_config.port, metrics_port = self.internal_config.metrics_port), err)]
    async fn run(self) -> Result<()> {
        info!("Starting simple server");
        let address = self.get_listen_address(self.public_config.port);

        Self::start_metrics(&self.get_listen_address(self.internal_config.metrics_port));

        self.public_config
            .protocol
            .spawn_server(&address, self)
            .await?
            .join()
            .await?;
        Ok(())
    }

    pub fn start_metrics(address: &String) {
        match address.parse::<SocketAddr>() {
            Err(err) => panic!("Invalid metrics address for {address}: {err}"),
            Ok(address) => prometheus_server::start_metrics(address),
        }
    }

    fn get_listen_address(&self, port: u16) -> String {
        format!("0.0.0.0:{}", port)
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

fn main() -> Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(env_filter)
        .init();

    let options = <ProxyOptions as clap::Parser>::parse();

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
        .block_on(async move { Proxy::from_options(options).await?.run().await })
}

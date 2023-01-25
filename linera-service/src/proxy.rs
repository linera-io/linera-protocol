// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{bail, Result};
use futures::{future::BoxFuture, FutureExt, SinkExt, StreamExt};
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
};
use std::path::PathBuf;
use structopt::StructOpt;

/// Options for running the proxy.
#[derive(Debug, StructOpt)]
#[structopt(
    name = "Linera Proxy",
    about = "A proxy to redirect incoming requests to Linera Server shards"
)]
pub struct ProxyOptions {
    /// Path to server configuration.
    config_path: PathBuf,
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
    fn from_options(options: ProxyOptions) -> Result<Self> {
        let config = ValidatorServerConfig::read(&options.config_path)?;
        let internal_protocol = config.internal_network.protocol;
        let external_protocol = config.validator.network.protocol;

        let proxy = match (internal_protocol, external_protocol) {
            (NetworkProtocol::Grpc, NetworkProtocol::Grpc) => Self::Grpc(GrpcProxy::new(
                config.validator.network,
                config.internal_network,
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

#[derive(Clone)]
pub struct SimpleProxy {
    public_config: ValidatorPublicNetworkPreConfig<TransportProtocol>,
    internal_config: ValidatorInternalNetworkPreConfig<TransportProtocol>,
}

impl MessageHandler for SimpleProxy {
    fn handle_message(&mut self, message: RpcMessage) -> BoxFuture<Option<RpcMessage>> {
        let shard = self.select_shard_for(&message);
        let protocol = self.internal_config.protocol;

        async move {
            if let Some(shard) = shard {
                match Self::try_proxy_message(message, shard, protocol).await {
                    Ok(maybe_response) => maybe_response,
                    Err(error) => {
                        log::warn!("Failed to proxy message: {error}");
                        None
                    }
                }
            } else {
                None
            }
        }
        .boxed()
    }
}

impl SimpleProxy {
    async fn run(self) -> Result<()> {
        let address = format!("0.0.0.0:{}", self.public_config.port);
        log::info!("Starting simple proxy on {}...", &address);
        self.public_config
            .protocol
            .spawn_server(&address, self)
            .await?
            .join()
            .await?;
        Ok(())
    }

    fn select_shard_for(&self, request: &RpcMessage) -> Option<ShardConfig> {
        let chain_id = match request {
            RpcMessage::BlockProposal(proposal) => proposal.content.block.chain_id,
            RpcMessage::HashCertificate(certificate) => certificate.chain_id,
            RpcMessage::Certificate(certificate) => certificate.value.chain_id(),
            RpcMessage::ChainInfoQuery(query) => query.chain_id,
            RpcMessage::Vote(_) | RpcMessage::ChainInfoResponse(_) | RpcMessage::Error(_) => {
                log::debug!("Can't proxy an incoming response message");
                return None;
            }
            RpcMessage::CrossChainRequest(cross_chain_request) => {
                cross_chain_request.target_chain_id()
            }
        };

        Some(self.internal_config.get_shard_for(chain_id).clone())
    }

    async fn try_proxy_message(
        message: RpcMessage,
        shard: ShardConfig,
        protocol: TransportProtocol,
    ) -> Result<Option<RpcMessage>> {
        let shard_address = format!("{}:{}", shard.host, shard.port);
        let mut connection = protocol.connect(shard_address).await?;
        connection.send(message).await?;
        Ok(connection.next().await.transpose()?)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    log::info!("Initialising proxy...");

    let proxy = Proxy::from_options(ProxyOptions::from_args())?;
    proxy.run().await
}

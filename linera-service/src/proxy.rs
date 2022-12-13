use linera_service::{
    config::{Import, ValidatorServerConfig},
    grpc_proxy::GrpcProxy,
};
use anyhow::{bail, Result};
use futures::{future::BoxFuture, FutureExt, SinkExt, StreamExt};
use linera_rpc::{
    config::{NetworkProtocol, Address, Shards},
    transport::{MessageHandler, TransportProtocol},
    Message,
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

enum Proxy {
    Simple(SimpleProxy),
    Grpc(GrpcProxy)
}

impl Proxy {
    async fn run(self) -> Result<()> {
        match self {
            Proxy::Simple(simple_proxy) => simple_proxy.run().await,
            Proxy::Grpc(grpc_proxy) => grpc_proxy.run().await
        }
    }

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
                public_transport,
                address: config.validator.network.address,
                internal_transport,
                shards: config.internal_network.shards,
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
    public_transport: TransportProtocol,
    address: Address,
    internal_transport: TransportProtocol,
    shards: Shards,
}

impl MessageHandler for SimpleProxy {
    fn handle_message(&mut self, message: Message) -> BoxFuture<Option<Message>> {
        let shard = self.select_shard_for(&message);
        let protocol = self.internal_transport;

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
    fn select_shard_for(&self, request: &Message) -> Option<Address> {
        let chain_id = match request {
            Message::BlockProposal(proposal) => proposal.content.block.chain_id,
            Message::Certificate(certificate) => certificate.value.chain_id(),
            Message::ChainInfoQuery(query) => query.chain_id,
            Message::Vote(_) | Message::ChainInfoResponse(_) | Message::Error(_) => {
                log::debug!("Can't proxy an incoming response message");
                return None;
            }
            Message::CrossChainRequest(cross_chain_request) => {
                cross_chain_request.target_chain_id()
            }
        };

        Some(self.shards.get_shard_for(chain_id).clone())
    }

    async fn try_proxy_message(
        message: Message,
        shard: Address,
        protocol: TransportProtocol,
    ) -> Result<Option<Message>> {
        let mut connection = protocol.connect(shard.to_string()).await?;
        connection.send(message).await?;
        Ok(connection.next().await.transpose()?)
    }

    async fn run(self) -> Result<()> {
        let address = self.address.to_string();
        self.public_transport
            .spawn_server(&address, self)
            .await?
            .join()
            .await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    log::info!("Initialising proxy...");

    let proxy = Proxy::from_options(ProxyOptions::from_args())?;

    log::info!("Starting proxy running...");

    if let Err(error) = proxy.run().await {
        log::error!("Failed to run proxy: {error}");
    }

    Ok(())
}

use anyhow::Result;
use futures::{future::BoxFuture, FutureExt, SinkExt, StreamExt};
use linera_rpc::{
    config::{
        NetworkProtocol, ShardConfig, ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig,
    },
    transport::MessageHandler,
    Message,
};
use linera_service::config::{Import, ValidatorServerConfig};
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

#[derive(Clone)]
pub struct Proxy {
    public_config: ValidatorPublicNetworkConfig,
    internal_config: ValidatorInternalNetworkConfig,
}

impl MessageHandler for Proxy {
    fn handle_message(&mut self, message: Message) -> BoxFuture<Option<Message>> {
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

impl Proxy {
    async fn run(self) -> Result<()> {
        let address = format!("0.0.0.0:{}", self.public_config.port);
        let NetworkProtocol::Simple(protocol) = self.public_config.protocol;
        protocol.spawn_server(&address, self).await?.join().await?;
        Ok(())
    }

    fn select_shard_for(&self, request: &Message) -> Option<ShardConfig> {
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

        Some(self.internal_config.get_shard_for(chain_id).clone())
    }

    async fn try_proxy_message(
        message: Message,
        shard: ShardConfig,
        protocol: NetworkProtocol,
    ) -> Result<Option<Message>> {
        let shard_address = format!("{}:{}", shard.host, shard.port);
        let NetworkProtocol::Simple(protocol) = protocol;
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
    let options = ProxyOptions::from_args();
    let config = ValidatorServerConfig::read(&options.config_path)?;

    let handler = Proxy {
        public_config: config.validator.network,
        internal_config: config.internal_network,
    };

    if let Err(error) = handler.run().await {
        log::error!("Failed to run proxy: {error}");
    }

    Ok(())
}

use anyhow::Result;
use async_trait::async_trait;
use linera_base::messages::ChainId;
use linera_chain::messages::{BlockAndRound, Value};
use linera_rpc::{
    config::{ShardConfig, ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig},
    grpc_network::{
        grpc::{
            validator_node_client::ValidatorNodeClient,
            validator_node_server::{ValidatorNode, ValidatorNodeServer},
            validator_worker_client::ValidatorWorkerClient,
            validator_worker_server::{ValidatorWorker, ValidatorWorkerServer},
            ChainInfoResult,
        },
        BlockProposal, Certificate, ChainInfoQuery, CrossChainRequest,
    },
    pool::ConnectionPool,
};
use linera_service::config::{Import, ValidatorServerConfig};
use std::{net::SocketAddr, path::PathBuf, str::FromStr};
use structopt::StructOpt;
use tonic::{
    transport::{Channel, Server},
    Request, Response, Status,
};

/// Boilerplate to extract the underlying chain id, use it to get the corresponding shard
/// and forward the message.
macro_rules! proxy {
    ($self:ident, $handler:ident, $req:ident, $client:ident) => {{
        log::debug!(
            "handler [{}:{}] proxying request [{:?}] from {:?}",
            stringify!($client),
            stringify!($handler),
            $req,
            $req.remote_addr()
        );
        let inner = $req.into_inner();
        let shard = $self
            .shard_for(&inner)
            .ok_or(Status::not_found("could not find shard for message"))?;
        let mut client = $self
            .$client(&shard)
            .await
            .map_err(|_| Status::internal("could not connect to shard"))?;
        client.$handler(inner).await
    }};
}

/// Options for running the proxy.
#[derive(Debug, StructOpt)]
#[structopt(
    name = "Linera gRPC Proxy",
    about = "A proxy to redirect incoming requests to Linera Server shards"
)]
pub struct GrpcProxyOptions {
    /// Path to server configuration.
    config_path: PathBuf,
}

#[derive(Clone)]
pub struct GrpcProxy {
    public_config: ValidatorPublicNetworkConfig,
    internal_config: ValidatorInternalNetworkConfig,
    node_connection_pool: ConnectionPool<ValidatorNodeClient<Channel>>,
    worker_connection_pool: ConnectionPool<ValidatorWorkerClient<Channel>>,
}

impl GrpcProxy {
    async fn spawn(
        public_config: ValidatorPublicNetworkConfig,
        internal_config: ValidatorInternalNetworkConfig,
    ) -> Result<()> {
        let grpc_proxy = GrpcProxy {
            public_config,
            internal_config,
            node_connection_pool: ConnectionPool::new(),
            worker_connection_pool: ConnectionPool::new(),
        };

        let address = grpc_proxy.address()?;

        Ok(Server::builder()
            .add_service(grpc_proxy.as_validator_worker())
            .add_service(grpc_proxy.as_validator_node())
            .serve(address)
            .await?)
    }

    fn as_validator_worker(&self) -> ValidatorWorkerServer<Self> {
        ValidatorWorkerServer::new(self.clone())
    }

    fn as_validator_node(&self) -> ValidatorNodeServer<Self> {
        ValidatorNodeServer::new(self.clone())
    }

    fn address(&self) -> Result<SocketAddr> {
        Ok(SocketAddr::from_str(&format!(
            "0.0.0.0:{}",
            self.public_config.port
        ))?)
    }

    fn shard_for(&self, proxyable: &impl Proxyable) -> Option<ShardConfig> {
        Some(
            self.internal_config
                .get_shard_for(proxyable.chain_id()?)
                .clone(),
        )
    }

    // todo: if we want to use a pool here we'll need to wrap it up in an Arc<Mutex>
    async fn worker_client_for_shard(
        &self,
        shard: &ShardConfig,
    ) -> Result<ValidatorWorkerClient<Channel>> {
        let address = format!("http://{}:{}", shard.host, shard.port);
        let client = self
            .worker_connection_pool
            .cloned_client_for_address(address)
            .await?;

        Ok(client)
    }

    async fn node_client_for_shard(
        &self,
        shard: &ShardConfig,
    ) -> Result<ValidatorNodeClient<Channel>> {
        let address = format!("http://{}:{}", shard.host, shard.port);
        let client = self
            .node_connection_pool
            .cloned_client_for_address(address)
            .await?;

        Ok(client)
    }
}

#[async_trait]
impl ValidatorWorker for GrpcProxy {
    async fn handle_block_proposal(
        &self,
        request: Request<BlockProposal>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        proxy!(
            self,
            handle_block_proposal,
            request,
            worker_client_for_shard
        )
    }

    async fn handle_certificate(
        &self,
        request: Request<Certificate>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        proxy!(self, handle_certificate, request, worker_client_for_shard)
    }

    async fn handle_chain_info_query(
        &self,
        request: Request<ChainInfoQuery>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        proxy!(
            self,
            handle_chain_info_query,
            request,
            worker_client_for_shard
        )
    }

    async fn handle_cross_chain_request(
        &self,
        request: Request<CrossChainRequest>,
    ) -> Result<Response<()>, Status> {
        proxy!(
            self,
            handle_cross_chain_request,
            request,
            worker_client_for_shard
        )
    }
}

#[async_trait]
impl ValidatorNode for GrpcProxy {
    async fn handle_block_proposal(
        &self,
        request: Request<BlockProposal>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        proxy!(self, handle_block_proposal, request, node_client_for_shard)
    }

    async fn handle_certificate(
        &self,
        request: Request<Certificate>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        proxy!(self, handle_certificate, request, node_client_for_shard)
    }

    async fn handle_chain_info_query(
        &self,
        request: Request<ChainInfoQuery>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        proxy!(
            self,
            handle_chain_info_query,
            request,
            node_client_for_shard
        )
    }
}

/// Types which are proxyable and expose the appropriate methods to be handled
/// by the `GrpcProxy`
trait Proxyable {
    fn chain_id(&self) -> Option<ChainId>;
}

impl Proxyable for BlockProposal {
    fn chain_id(&self) -> Option<ChainId> {
        match bcs::from_bytes::<BlockAndRound>(&self.content) {
            Ok(block_and_round) => Some(block_and_round.block.chain_id),
            Err(_) => None,
        }
    }
}

impl Proxyable for Certificate {
    fn chain_id(&self) -> Option<ChainId> {
        match bcs::from_bytes::<Value>(&self.value) {
            Ok(value) => Some(value.chain_id()),
            Err(_) => None,
        }
    }
}

impl Proxyable for ChainInfoQuery {
    fn chain_id(&self) -> Option<ChainId> {
        match self
            .chain_id
            .as_ref()
            .map(|id| ChainId::try_from(id.clone()))?
        {
            Ok(id) => Some(id),
            Err(_) => None,
        }
    }
}

impl Proxyable for CrossChainRequest {
    fn chain_id(&self) -> Option<ChainId> {
        match linera_core::messages::CrossChainRequest::try_from(self.clone()) {
            Ok(cross_chain_request) => Some(cross_chain_request.target_chain_id()),
            Err(_) => None,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let options = GrpcProxyOptions::from_args();
    let config = ValidatorServerConfig::read(&options.config_path)?;

    let handler = GrpcProxy::spawn(config.validator.network, config.internal_network);

    log::info!("Proxy spawning...");

    if let Err(error) = handler.await {
        log::error!("Failed to run proxy: {error}");
    }

    Ok(())
}

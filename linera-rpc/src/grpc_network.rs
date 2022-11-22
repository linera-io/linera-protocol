pub use crate::{
    config::{CrossChainConfig, ShardId},
    grpc_network::grpc_network::{
        bcs_service_server::BcsService, BcsMessage, BlockProposal, Certificate, ChainInfoQuery,
        ChainInfoResponse, CrossChainRequest,
    },
    transport::MessageHandler,
    Message,
};
use async_trait::async_trait;
use linera_core::{
    node::NodeError,
    worker::{ValidatorWorker, WorkerState},
};
use linera_views::views::ViewError;
use log::{debug, error, info};
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use std::net::AddrParseError;
use thiserror::Error;
use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status};

// to avoid confusion with existing ValidatorNode
use crate::{
    client_delegate, convert_and_delegate,
    grpc_network::grpc_network::{
        validator_node_server::{
            ValidatorNode as ValidatorNodeRpc, ValidatorNode, ValidatorNodeServer,
        },
        validator_worker_server::{ValidatorWorker as ValidatorWorkerRpc, ValidatorWorkerServer},
        ChainInfoResult,
    },
    simple_network::SharedStore,
};

use crate::{
    config::{ShardConfig, ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig},
    conversions::ProtoConversionError,
    grpc_network::grpc_network::{
        chain_info_result::Inner, validator_node_client::ValidatorNodeClient,
        validator_worker_client::ValidatorWorkerClient,
    },
    transport::SpawnedServer,
};
use futures::{
    channel::{mpsc, oneshot::Sender},
    FutureExt, SinkExt, StreamExt,
};
use linera_base::messages::ChainId;
use linera_core::worker::WorkerError;
use tokio::task::{JoinError, JoinHandle};
use tonic::transport::Channel;
use linera_core::client::ValidatorNodeProvider;
use crate::pool::ClientPool;

pub mod grpc_network {
    tonic::include_proto!("rpc.v1");
}

type CrossChainSender = mpsc::Sender<(linera_core::messages::CrossChainRequest, ShardId)>;

#[derive(Clone)]
pub struct GrpcServer<S> {
    state: WorkerState<S>,
    shard_id: ShardId,
    network: ValidatorInternalNetworkConfig,
    cross_chain_sender: CrossChainSender,
}

pub struct SpawnedGrpcServer {
    complete: Sender<()>,
    handle: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl SpawnedGrpcServer {
    pub async fn join(self) -> Result<(), GrpcError> {
        Ok(self.handle.await??)
    }
}

#[derive(Error, Debug)]
pub enum GrpcError {
    #[error("failed to connect to address: {0}")]
    ConnectionFailed(#[from] tonic::transport::Error),

    #[error("failed to convert to proto: {0}")]
    ProtoConversion(#[from] ProtoConversionError),

    #[error("failed to communicate cross-chain queries: {0}")]
    CrossChain(#[from] Status),

    #[error("failed to execute task to completion: {0}")]
    Join(#[from] JoinError),

    #[error("failed to parse socket address: {0}")]
    SocketAddr(#[from] AddrParseError)
}

impl<S: SharedStore> GrpcServer<S>
where
    ViewError: From<S::ContextError>,
{

    pub async fn spawn(
        host: String,
        port: u16,
        state: WorkerState<S>,
        shard_id: ShardId,
        network: ValidatorInternalNetworkConfig,
        cross_chain_config: CrossChainConfig,
    ) -> Result<SpawnedGrpcServer, GrpcError> {
        info!(
            "spawning gRPC server  on {}:{} for shard {}",
            host, port, shard_id
        );

        let address = SocketAddr::from_str(&format!("{}:{}", host, port))?;

        let (cross_chain_sender, cross_chain_receiver) =
            mpsc::channel(cross_chain_config.queue_size);

        tokio::spawn({
            info!("spawning cross-chain queries thread for {}", shard_id);
            Self::forward_cross_chain_queries(
                state.nickname().to_string(),
                network.clone(),
                cross_chain_config.max_retries,
                cross_chain_receiver,
            )
        });

        let (complete, receiver) = futures::channel::oneshot::channel();

        let grpc_server = GrpcServer {
            state,
            shard_id,
            network,
            cross_chain_sender,
        };

        let validator_node = ValidatorNodeServer::new(grpc_server);

        let handle = tokio::spawn(
            Server::builder()
                .add_service(validator_node)
                .serve_with_shutdown(address, receiver.map(|_| ())),
        );

        Ok(SpawnedGrpcServer { complete, handle })
    }

    async fn handle_continuation(&self, requests: Vec<linera_core::messages::CrossChainRequest>) {
        for request in requests {
            let shard_id = self.network.get_shard_id(request.target_chain_id());
            debug!(
                "[{}] Scheduling cross-chain query: {} -> {}",
                self.state.nickname(),
                self.shard_id,
                shard_id
            );
            // todo change to `send_all` or `feed`
            self.cross_chain_sender
                .clone()
                .feed((request, shard_id))
                .await
                .expect("internal channel should not fail") // why would this fail?
        }
    }

    async fn forward_cross_chain_queries(
        nickname: String,
        network: ValidatorInternalNetworkConfig,
        this_shard: ShardId,
        mut receiver: mpsc::Receiver<(linera_core::messages::CrossChainRequest, ShardId)>,
    ) {
        let mut queries_sent = 0u64;
        let mut pool = ClientPool::new();

        while let Some((cross_chain_request, shard_id)) = receiver.next().await {
            let shard = network.shard(shard_id);

            let request = Request::new(cross_chain_request.try_into().expect("todo"));
            let client = pool.mut_client_for_address(shard.address()).await.expect("todo");

            match client.handle_cross_chain_request(request).await {
                Ok(_) => queries_sent += 1,
                Err(error) => error!("[{}] cross chain query failed: {:?}", nickname, error),
            }

            if queries_sent % 2000 == 0 {
                // is this correct? does the `shard_id` not change?
                debug!(
                    "[{}] {} has sent {} cross-chain queries to {}:{} (shard {})",
                    nickname, this_shard, queries_sent, shard.host, shard.port, shard_id,
                );
            }
        }
    }
}

#[tonic::async_trait]
impl<S: SharedStore> ValidatorNodeRpc for GrpcServer<S>
where
    ViewError: From<S::ContextError>,
{
    async fn handle_block_proposal(
        &self,
        request: Request<BlockProposal>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        convert_and_delegate!(self, handle_block_proposal, request)
    }

    async fn handle_certificate(
        &self,
        request: Request<Certificate>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        match self
            .state
            .clone()
            .handle_certificate(request.into_inner().try_into()?)
            .await
        {
            Ok((info, continuation)) => {
                self.handle_continuation(continuation).await;
                Ok(Response::new(info.try_into()?))
            }
            Err(error) => {
                error!(
                    "[{}] Failed to handle cross-chain request: {}",
                    self.state.nickname(),
                    error
                );
                Ok(Response::new(NodeError::from(error).into()))
            }
        }
    }

    async fn handle_chain_info_query(
        &self,
        request: Request<ChainInfoQuery>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        convert_and_delegate!(self, handle_chain_info_query, request)
    }
}

#[tonic::async_trait]
impl<S: SharedStore> ValidatorWorkerRpc for GrpcServer<S>
where
    ViewError: From<S::ContextError>,
{
    async fn handle_block_proposal(
        &self,
        request: Request<BlockProposal>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        convert_and_delegate!(self, handle_block_proposal, request)
    }

    async fn handle_certificate(
        &self,
        request: Request<Certificate>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        match self
            .state
            .clone()
            .handle_certificate(request.into_inner().try_into()?)
            .await
        {
            Ok((info, continuation)) => {
                self.handle_continuation(continuation).await;
                Ok(Response::new(info.try_into()?))
            }
            Err(error) => {
                error!(
                    "[{}] Failed to handle cross-chain request: {}",
                    self.state.nickname(),
                    error
                );
                Ok(Response::new(NodeError::from(error).into()))
            }
        }
    }

    async fn handle_chain_info_query(
        &self,
        request: Request<ChainInfoQuery>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        convert_and_delegate!(self, handle_chain_info_query, request)
    }

    async fn handle_cross_chain_request(
        &self,
        request: Request<CrossChainRequest>,
    ) -> Result<Response<CrossChainRequest>, Status> {
        match self
            .state
            .clone()
            .handle_cross_chain_request(request.into_inner().try_into()?)
            .await
        {
            Ok(continuation) => self.handle_continuation(continuation).await,
            Err(error) => {
                error!(
                    "[{}] Failed to handle cross-chain request: {}",
                    self.state.nickname(),
                    error
                );
            }
        }
        unimplemented!(
            "what do we do here since original method does not return anything to the user"
        )
    }
}

#[derive(Clone)]
pub struct GrpcClient(ValidatorNodeClient<Channel>);

impl GrpcClient {
    async fn new(network: ValidatorPublicNetworkConfig) -> Result<Self, GrpcError> {
        Ok(Self(ValidatorNodeClient::connect(network.address()).await?))
    }
}

#[async_trait]
impl linera_core::node::ValidatorNode for GrpcClient {
    async fn handle_block_proposal(
        &mut self,
        proposal: linera_chain::messages::BlockProposal,
    ) -> Result<linera_core::messages::ChainInfoResponse, NodeError> {
        client_delegate!(self, handle_block_proposal, proposal)
    }

    async fn handle_certificate(
        &mut self,
        certificate: linera_chain::messages::Certificate,
    ) -> Result<linera_core::messages::ChainInfoResponse, NodeError> {
        client_delegate!(self, handle_certificate, certificate)
    }

    async fn handle_chain_info_query(
        &mut self,
        query: linera_core::messages::ChainInfoQuery,
    ) -> Result<linera_core::messages::ChainInfoResponse, NodeError> {
        client_delegate!(self, handle_chain_info_query, query)
    }
}

pub struct GrpcNodeProvider {}

impl ValidatorNodeProvider for GrpcNodeProvider {
    type Node = GrpcClient;

    fn make_node(&self, address: &str) -> anyhow::Result<Self::Node, NodeError> {
        let network = ValidatorPublicNetworkConfig::from_str(address).map_err(|_| {
            NodeError::CannotResolveValidatorAddress {
                address: address.to_string(),
            }
        })?;
        unimplemented!()
        // Ok(GrpcClient::new(network))
    }
}

#[derive(Debug, Default)]
pub struct GenericBcsService<S> {
    state: Arc<Mutex<S>>,
}

impl<S> From<S> for GenericBcsService<S> {
    fn from(s: S) -> Self {
        GenericBcsService {
            state: Arc::new(Mutex::new(s)),
        }
    }
}

#[tonic::async_trait]
impl<S> BcsService for GenericBcsService<S>
where
    S: MessageHandler + Send + Sync + 'static,
{
    async fn handle(&self, request: Request<BcsMessage>) -> Result<Response<BcsMessage>, Status> {
        let message: Message = bcs::from_bytes(&request.get_ref().inner).unwrap();

        let mut state = self
            .state
            .try_lock()
            .map_err(|_| Status::internal("service lock poisoned"))?;

        let response: Option<Message> = state.handle_message(message).await;

        let response_bytes = match response {
            Some(response) => bcs::to_bytes(&response),
            None => bcs::to_bytes::<Vec<()>>(&vec![]), // todo(security): do we want the error msg showing the serialization internals?
        }
        .map_err(|e| {
            Status::data_loss(format!(
                "there was an error while serializing the response: {:?}",
                e
            ))
        })?;

        Ok(Response::new(BcsMessage {
            inner: response_bytes,
        }))
    }
}

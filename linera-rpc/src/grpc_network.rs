pub use crate::{
    config::{CrossChainConfig, ShardId},
    grpc_network::grpc_network::{
        BlockProposal, Certificate, ChainInfoQuery, ChainInfoResponse, CrossChainRequest,
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
    net::{AddrParseError, SocketAddr},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
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
        validator_worker_server::ValidatorWorker as ValidatorWorkerRpc,
        ChainInfoResult,
    },
    mass_client_delegate,
    simple_network::SharedStore,
};

use crate::{
    config::{ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig},
    conversions::ProtoConversionError,
    grpc_network::grpc_network::{
        chain_info_result::Inner, validator_node_client::ValidatorNodeClient,
    },
    pool::ClientPool,
};
use futures::{
    channel::{mpsc, oneshot::Sender},
    FutureExt, SinkExt, StreamExt,
};

use crate::{
    grpc_network::grpc_network::validator_worker_server::ValidatorWorkerServer,
    mass::{MassClient, MassClientError},
};
use linera_core::client::ValidatorNodeProvider;
use tokio::task::{JoinError, JoinHandle};
use tonic::transport::Channel;

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
    SocketAddr(#[from] AddrParseError),
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
            info!(
                "[{}] spawning cross-chain queries thread on {} for shard {}",
                state.nickname(),
                host,
                shard_id
            );
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

        let validator_node = ValidatorNodeServer::new(grpc_server.clone());
        let worker_node = ValidatorWorkerServer::new(grpc_server);

        let handle = tokio::spawn(
            Server::builder()
                .add_service(validator_node)
                .add_service(worker_node)
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
        let mut failed = 0u64;

        let mut pool = ClientPool::new();

        while let Some((cross_chain_request, shard_id)) = receiver.next().await {
            let shard = network.shard(shard_id);

            let client = pool
                .mut_client_for_address(format!("http://{}", shard.address()))
                .await
                .expect("todo");
            for i in 0..10 {
                let request = Request::new(cross_chain_request.clone().try_into().expect("todo"));
                match client.handle_cross_chain_request(request).await {
                    Ok(_) => {
                        queries_sent += 1;
                        break;
                    }
                    Err(error) => {
                        if i < 10 {
                            failed += 1;
                            error!(
                                "[{}] cross chain query to {} failed: {:?}",
                                nickname,
                                shard.address(),
                                error
                            );
                            error!(
                                "[{}] queries succeeded: {}. queries failed: {}",
                                nickname, queries_sent, failed
                            );
                            tokio::time::sleep(Duration::from_millis(2000)).await;
                        } else {
                            error!(
                                "[{}] Failed to send cross-chain query (giving up after {} retries): {}",
                                nickname, i, error
                            );
                        }
                    }
                }
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
        debug!(
            "server handler [handle_certificate] received delegating request [{:?}] ",
            request
        );
        match self
            .state
            .clone()
            .handle_certificate({ request.into_inner().try_into()? })
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
        debug!(
            "server handler [handle_certificate] received delegating request [{:?}] ",
            request
        );
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
    ) -> Result<Response<()>, Status> {
        debug!(
            "server handler [handle_cross_chain_request] received delegating request [{:?}] ",
            request
        );
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
        Ok(Response::new(()))
    }
}

#[derive(Clone)]
pub struct GrpcClient(ValidatorPublicNetworkConfig);

impl GrpcClient {
    async fn new(network: ValidatorPublicNetworkConfig) -> Result<Self, GrpcError> {
        // Ok(Self(ValidatorNodeClient::connect(network.address()).await?))
        Ok(Self(network))
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

#[async_trait]
impl ValidatorNodeProvider for GrpcNodeProvider {
    type Node = GrpcClient;

    async fn make_node(&self, address: &str) -> anyhow::Result<Self::Node, NodeError> {
        let network = ValidatorPublicNetworkConfig::from_str(&address).map_err(|_| {
            NodeError::CannotResolveValidatorAddress {
                address: address.to_string(),
            }
        })?;
        Ok(GrpcClient::new(network).await.expect("todo"))
    }
}

pub struct GrpcMassClient(ValidatorPublicNetworkConfig);

impl GrpcMassClient {
    pub fn new(network: ValidatorPublicNetworkConfig) -> Self {
        Self(network)
    }
}

#[async_trait]
impl MassClient for GrpcMassClient {
    async fn send(&self, requests: Vec<Message>) -> Result<Vec<Message>, MassClientError> {
        // todo - for now we are instantiated the client here so that we don't need the
        // constructor to be asynchronous

        let mut client = ValidatorNodeClient::connect(self.0.address()).await?;
        let mut responses = Vec::new();

        for request in requests {
            match request {
                Message::BlockProposal(proposal) => {
                    mass_client_delegate!(client, handle_block_proposal, proposal, responses)
                }
                Message::Certificate(certificate) => {
                    mass_client_delegate!(client, handle_certificate, certificate, responses)
                }
                msg => panic!("attempted to send msg: {:?}", msg),
            }
        }
        Ok(responses)
    }
}

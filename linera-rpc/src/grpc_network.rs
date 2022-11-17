pub use crate::{
    config::{CrossChainConfig, ShardId},
    grpc_network::grpc_network::{
        bcs_service_server::BcsService, BcsMessage, BlockProposal, Certificate, ChainInfoQuery,
        ChainInfoResponse, CrossChainRequest,
    },
    transport::MessageHandler,
    Message,
};
use linera_core::worker::{ValidatorWorker, WorkerState};
use linera_views::views::ViewError;
use log::info;
use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::Arc,
};
use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status};

// to avoid confusion with existing ValidatorNode
use crate::grpc_network::grpc_network::validator_node_server::{
    ValidatorNode as ValidatorNodeRpc, ValidatorNodeServer,
};
// to avoid confusion with existing ValidatorNode
use crate::{
    grpc_network::grpc_network::validator_worker_server::{
        ValidatorWorker as ValidatorWorkerRpc, ValidatorWorkerServer,
    },
    simple_network::SharedStore,
};

pub mod grpc_network {
    tonic::include_proto!("rpc.v1");
}

#[derive(Clone)]
pub struct GrpcServer<S> {
    host: String,
    port: u16,
    state: WorkerState<S>,
    shard_id: ShardId,
    cross_chain_config: CrossChainConfig,
}

impl<S: SharedStore> GrpcServer<S>
where
    ViewError: From<S::ContextError>,
{
    pub fn new(
        host: String,
        port: u16,
        state: WorkerState<S>,
        shard_id: ShardId,
        cross_chain_config: CrossChainConfig,
    ) -> Self {
        Self {
            host,
            port,
            state,
            shard_id,
            cross_chain_config,
        }
    }

    pub async fn spawn_validator_node(self) -> Result<(), std::io::Error> {
        info!(
            "gRPC server listening for traffic on {}:{}",
            self.host, self.port
        );

        let address = SocketAddr::new(
            IpAddr::from_str(self.host.as_str()).expect("todo"),
            self.port,
        );

        let validator_node = ValidatorNodeServer::new(self);

        let server = Server::builder()
            .add_service(validator_node)
            //.serve_with_shutdown(address, receiver.map(|_| ()))
            .serve(address)
            .await;
        Ok(())
    }

    pub async fn spawn_validator_worker(self) -> Result<(), std::io::Error> {
        info!(
            "gRPC server listening for traffic on {}:{}",
            self.host, self.port
        );

        let address = SocketAddr::new(
            IpAddr::from_str(self.host.as_str()).expect("todo"),
            self.port,
        );

        let validator_worker = ValidatorWorkerServer::new(self);

        let server = Server::builder()
            .add_service(validator_worker)
            //.serve_with_shutdown(address, receiver.map(|_| ()))
            .serve(address)
            .await;
        Ok(())
    }
}

// probably want to change this to `impl ValidatorNode for LocalNode`?
#[tonic::async_trait]
impl<S: SharedStore> ValidatorNodeRpc for GrpcServer<S>
where
    ViewError: From<S::ContextError>,
{
    async fn handle_block_proposal(
        &self,
        request: Request<BlockProposal>,
    ) -> Result<Response<ChainInfoResponse>, Status> {
        self.state
            .clone()
            .handle_block_proposal(request.into_inner().try_into()?)
            .await;
        unimplemented!()
    }

    async fn handle_certificate(
        &self,
        request: Request<Certificate>,
    ) -> Result<Response<ChainInfoResponse>, Status> {
        self.state
            .clone()
            .handle_certificate(request.into_inner().try_into()?)
            .await;
        unimplemented!()
    }

    async fn handle_chain_info_query(
        &self,
        request: Request<ChainInfoQuery>,
    ) -> Result<Response<ChainInfoResponse>, Status> {
        self.state
            .clone()
            .handle_chain_info_query(request.into_inner().try_into()?)
            .await;
        unimplemented!()
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
    ) -> Result<Response<ChainInfoResponse>, Status> {
        self.state
            .clone()
            .handle_block_proposal(request.into_inner().try_into()?)
            .await;
        unimplemented!()
    }

    async fn handle_certificate(
        &self,
        request: Request<Certificate>,
    ) -> Result<Response<ChainInfoResponse>, Status> {
        self.state
            .clone()
            .handle_certificate(request.into_inner().try_into()?)
            .await;
        unimplemented!()
    }

    async fn handle_chain_info_query(
        &self,
        request: Request<ChainInfoQuery>,
    ) -> Result<Response<ChainInfoResponse>, Status> {
        self.state
            .clone()
            .handle_chain_info_query(request.into_inner().try_into()?)
            .await;
        unimplemented!()
    }

    async fn handle_cross_chain_request(
        &self,
        request: Request<CrossChainRequest>,
    ) -> Result<Response<CrossChainRequest>, Status> {
        self.state
            .clone()
            .handle_cross_chain_request(request.into_inner().try_into()?)
            .await;
        unimplemented!()
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

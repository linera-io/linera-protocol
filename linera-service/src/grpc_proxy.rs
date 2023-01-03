// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use async_trait::async_trait;
use linera_base::data_types::ChainId;
use linera_chain::data_types::{BlockAndRound, Value};
use linera_rpc::{
    config::{ShardConfig, ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig},
    grpc_network::{
        grpc::{
            notification_service_server::{NotificationService, NotificationServiceServer},
            notifier_service_server::NotifierService,
            validator_node_client::ValidatorNodeClient,
            validator_node_server::{ValidatorNode, ValidatorNodeServer},
            validator_worker_client::ValidatorWorkerClient,
            validator_worker_server::{ValidatorWorker, ValidatorWorkerServer},
            ChainInfoResult, Notification, SubscriptionRequest,
        },
        BlockProposal, Certificate, ChainInfoQuery, CrossChainRequest,
    },
    notifier::{Notifier, NotifierError},
    pool::ConnectionPool,
};
use log::warn;
use std::{net::SocketAddr, pin::Pin, sync::Arc};
use std::fmt::Debug;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{
    codegen::futures_core::Stream,
    transport::{Channel, Server},
    Request, Response, Status,
};

#[derive(Clone)]
pub struct GrpcProxy(Arc<GrpcProxyInner>);

struct GrpcProxyInner {
    public_config: ValidatorPublicNetworkConfig,
    internal_config: ValidatorInternalNetworkConfig,
    node_connection_pool: ConnectionPool<ValidatorNodeClient<Channel>>,
    worker_connection_pool: ConnectionPool<ValidatorWorkerClient<Channel>>,
    notifier: Notifier<Notification>,
}

impl GrpcProxy {
    pub fn new(
        public_config: ValidatorPublicNetworkConfig,
        internal_config: ValidatorInternalNetworkConfig,
    ) -> Self {
        Self(Arc::new(GrpcProxyInner {
            public_config,
            internal_config,
            node_connection_pool: ConnectionPool::new(),
            worker_connection_pool: ConnectionPool::new(),
            notifier: Notifier::default(),
        }))
    }

    fn as_validator_worker(&self) -> ValidatorWorkerServer<Self> {
        ValidatorWorkerServer::new(self.clone())
    }

    fn as_validator_node(&self) -> ValidatorNodeServer<Self> {
        ValidatorNodeServer::new(self.clone())
    }

    fn as_notification_service(&self) -> NotificationServiceServer<Self> {
        NotificationServiceServer::new(self.clone())
    }

    fn address(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], self.0.public_config.port))
    }

    fn shard_for(&self, proxyable: &impl Proxyable) -> Option<ShardConfig> {
        Some(
            self.0
                .internal_config
                .get_shard_for(proxyable.chain_id()?)
                .clone(),
        )
    }

    async fn worker_client_for_shard(
        &self,
        shard: &ShardConfig,
    ) -> Result<ValidatorWorkerClient<Channel>> {
        let address = shard.http_address();
        let client = self
            .0
            .worker_connection_pool
            .cloned_client_for_address(address)
            .await?;

        Ok(client)
    }

    async fn node_client_for_shard(
        &self,
        shard: &ShardConfig,
    ) -> Result<ValidatorNodeClient<Channel>> {
        let address = shard.http_address();
        let client = self
            .0
            .node_connection_pool
            .cloned_client_for_address(address)
            .await?;

        Ok(client)
    }

    pub async fn run(self) -> Result<()> {
        log::info!("Starting gRPC proxy on {}...", self.address());
        Ok(Server::builder()
            .add_service(self.as_validator_node())
            .add_service(self.as_validator_worker())
            .add_service(self.as_notification_service())
            .serve(self.address())
            .await?)
    }

    async fn client_for_proxy_worker<R>(
        &self,
        request: Request<R>,
    ) -> Result<(ValidatorWorkerClient<Channel>, R), Status>
    where
        R: Debug + Proxyable,
    {
        log::debug!(
            "handler [ValidatorWorker] proxying request [{:?}] from {:?}",
            request,
            request.remote_addr()
        );
        let inner = request.into_inner();
        let shard = self
            .shard_for(&inner)
            .ok_or_else(|| Status::not_found("could not find shard for message"))?;
        let client = self
            .worker_client_for_shard(&shard)
            .await
            .map_err(|_| Status::internal("could not connect to shard"))?;
        Ok((client, inner))
    }

    async fn client_for_proxy_node<R>(
        &self,
        request: Request<R>,
    ) -> Result<(ValidatorNodeClient<Channel>, R), Status>
    where
        R: Debug + Proxyable,
    {
        log::debug!(
            "handler [ValidatorNode] proxying request [{:?}] from {:?}",
            request,
            request.remote_addr()
        );
        let inner = request.into_inner();
        let shard = self
            .shard_for(&inner)
            .ok_or_else(|| Status::not_found("could not find shard for message"))?;
        let client = self
            .node_client_for_shard(&shard)
            .await
            .map_err(|_| Status::internal("could not connect to shard"))?;
        Ok((client, inner))
    }
}

#[async_trait]
impl ValidatorWorker for GrpcProxy {
    async fn handle_block_proposal(
        &self,
        request: Request<BlockProposal>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.client_for_proxy_worker(request).await?;
        client.handle_block_proposal(inner).await
    }

    async fn handle_certificate(
        &self,
        request: Request<Certificate>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.client_for_proxy_worker(request).await?;
        client.handle_certificate(inner).await
    }

    async fn handle_chain_info_query(
        &self,
        request: Request<ChainInfoQuery>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.client_for_proxy_worker(request).await?;
        client.handle_chain_info_query(inner).await
    }

    async fn handle_cross_chain_request(
        &self,
        request: Request<CrossChainRequest>,
    ) -> Result<Response<()>, Status> {
        let (mut client, inner) = self.client_for_proxy_worker(request).await?;
        client.handle_cross_chain_request(inner).await
    }
}

#[async_trait]
impl ValidatorNode for GrpcProxy {
    async fn handle_block_proposal(
        &self,
        request: Request<BlockProposal>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.client_for_proxy_node(request).await?;
        client.handle_block_proposal(inner).await
    }

    async fn handle_certificate(
        &self,
        request: Request<Certificate>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.client_for_proxy_node(request).await?;
        client.handle_certificate(inner).await
    }

    async fn handle_chain_info_query(
        &self,
        request: Request<ChainInfoQuery>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.client_for_proxy_node(request).await?;
        client.handle_chain_info_query(inner).await
    }
}

type NotificationStream = Pin<Box<dyn Stream<Item = Result<Notification, Status>> + Send>>;

#[async_trait]
impl NotificationService for GrpcProxy {
    type SubscribeStream = NotificationStream;

    async fn subscribe(
        &self,
        request: Request<SubscriptionRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let subscription_request = request.into_inner();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let chain_ids = subscription_request
            .chain_ids
            .into_iter()
            .map(ChainId::try_from)
            .collect::<Result<Vec<ChainId>, _>>()?;

        self.0.notifier.create_subscription(chain_ids, tx);

        Ok(Response::new(
            Box::pin(UnboundedReceiverStream::new(rx)) as Self::SubscribeStream
        ))
    }
}

#[async_trait]
impl NotifierService for GrpcProxy {
    async fn notify(&self, request: Request<Notification>) -> Result<Response<()>, Status> {
        let notification = request.into_inner();

        let chain_id: ChainId = notification
            .chain_id
            .clone()
            .ok_or_else(|| Status::invalid_argument("Missing field: chain_id."))?
            .try_into()?;

        if let Err(e) = self.0.notifier.notify(&chain_id, notification) {
            match e {
                NotifierError::ChainDoesNotExist => warn!(
                    "Tried to notify for chain {:?} which has no subscribers.",
                    &chain_id
                ),
            }
        }

        Ok(Response::new(()))
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
        self.chain_id.clone()?.try_into().ok()
    }
}

impl Proxyable for CrossChainRequest {
    fn chain_id(&self) -> Option<ChainId> {
        match linera_core::data_types::CrossChainRequest::try_from(self.clone()) {
            Ok(cross_chain_request) => Some(cross_chain_request.target_chain_id()),
            Err(_) => None,
        }
    }
}

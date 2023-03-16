// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use async_trait::async_trait;
use linera_base::data_types::ChainId;
use linera_core::notifier::Notifier;
use linera_rpc::{
    config::{ShardConfig, ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig},
    grpc_network::{
        grpc::{
            notifier_service_server::{NotifierService, NotifierServiceServer},
            validator_node_server::{ValidatorNode, ValidatorNodeServer},
            validator_worker_client::ValidatorWorkerClient,
            BlockProposal, CertificateWithDependencies, ChainInfoQuery, ChainInfoResult,
            LiteCertificate, Notification, SubscriptionRequest,
        },
        Proxyable,
    },
    grpc_pool::ConnectionPool,
};
use std::{fmt::Debug, net::SocketAddr, sync::Arc, time::Duration};
use tokio::select;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{
    transport::{Channel, Server},
    Request, Response, Status,
};
use tracing::{debug, info, instrument};

#[derive(Clone)]
pub struct GrpcProxy(Arc<GrpcProxyInner>);

struct GrpcProxyInner {
    public_config: ValidatorPublicNetworkConfig,
    internal_config: ValidatorInternalNetworkConfig,
    worker_connection_pool: ConnectionPool,
    notifier: Notifier<Result<Notification, Status>>,
}

impl GrpcProxy {
    pub fn new(
        public_config: ValidatorPublicNetworkConfig,
        internal_config: ValidatorInternalNetworkConfig,
        connect_timeout: Duration,
        timeout: Duration,
    ) -> Self {
        Self(Arc::new(GrpcProxyInner {
            public_config,
            internal_config,
            worker_connection_pool: ConnectionPool::default()
                .with_connect_timeout(connect_timeout)
                .with_timeout(timeout),
            notifier: Notifier::default(),
        }))
    }

    fn as_validator_node(&self) -> ValidatorNodeServer<Self> {
        ValidatorNodeServer::new(self.clone())
    }

    fn as_notifier_service(&self) -> NotifierServiceServer<Self> {
        NotifierServiceServer::new(self.clone())
    }

    fn public_address(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], self.0.public_config.port))
    }

    fn internal_address(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], self.0.internal_config.port))
    }

    fn shard_for(&self, proxyable: &impl Proxyable) -> Option<ShardConfig> {
        Some(
            self.0
                .internal_config
                .get_shard_for(proxyable.chain_id()?)
                .clone(),
        )
    }

    fn worker_client_for_shard(
        &self,
        shard: &ShardConfig,
    ) -> Result<ValidatorWorkerClient<Channel>> {
        let address = shard.http_address();
        let channel = self.0.worker_connection_pool.channel(address)?;
        let client = ValidatorWorkerClient::new(channel);
        Ok(client)
    }

    /// Runs the proxy. If either the public server or private server dies for whatever
    /// reason we'll kill the proxy.
    #[instrument(skip_all, fields(public_address = %self.public_address(), internal_address = %self.internal_address()), err)]
    pub async fn run(self) -> Result<()> {
        info!("Starting gRPC server");
        let internal_server = Server::builder()
            .add_service(self.as_notifier_service())
            .serve(self.internal_address());
        let public_server = Server::builder()
            .add_service(self.as_validator_node())
            .serve(self.public_address());
        select! {
            internal_res = internal_server => internal_res?,
            public_res = public_server => public_res?,
        }
        Ok(())
    }

    async fn client_for_proxy_worker<R>(
        &self,
        request: Request<R>,
    ) -> Result<(ValidatorWorkerClient<Channel>, R), Status>
    where
        R: Debug + Proxyable,
    {
        debug!(
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
            .map_err(|_| Status::internal("could not connect to shard"))?;
        Ok((client, inner))
    }
}

#[async_trait]
impl ValidatorNode for GrpcProxy {
    type SubscribeStream = UnboundedReceiverStream<Result<Notification, Status>>;

    #[instrument(skip_all, err(Display))]
    async fn handle_block_proposal(
        &self,
        request: Request<BlockProposal>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.client_for_proxy_worker(request).await?;
        client.handle_block_proposal(inner).await
    }

    #[instrument(skip_all, err(Display))]
    async fn handle_lite_certificate(
        &self,
        request: Request<LiteCertificate>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.client_for_proxy_worker(request).await?;
        client.handle_lite_certificate(inner).await
    }

    #[instrument(skip_all, err(Display))]
    async fn handle_certificate(
        &self,
        request: Request<CertificateWithDependencies>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.client_for_proxy_worker(request).await?;
        client.handle_certificate(inner).await
    }

    #[instrument(skip_all, err(Display))]
    async fn handle_chain_info_query(
        &self,
        request: Request<ChainInfoQuery>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.client_for_proxy_worker(request).await?;
        client.handle_chain_info_query(inner).await
    }

    #[instrument(skip_all, err(Display))]
    async fn subscribe(
        &self,
        request: Request<SubscriptionRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let subscription_request = request.into_inner();
        let chain_ids = subscription_request
            .chain_ids
            .into_iter()
            .map(ChainId::try_from)
            .collect::<Result<Vec<ChainId>, _>>()?;
        let rx = self.0.notifier.subscribe(chain_ids);
        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }
}

#[async_trait]
impl NotifierService for GrpcProxy {
    #[instrument(skip_all, err(Display))]
    async fn notify(&self, request: Request<Notification>) -> Result<Response<()>, Status> {
        let notification = request.into_inner();
        let chain_id = notification
            .chain_id
            .clone()
            .ok_or_else(|| Status::invalid_argument("Missing field: chain_id."))?
            .try_into()?;
        self.0.notifier.notify(&chain_id, Ok(notification));
        Ok(Response::new(()))
    }
}

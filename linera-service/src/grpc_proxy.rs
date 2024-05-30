// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// `tracing::instrument` is not compatible with this nightly Clippy lint
#![allow(unknown_lints)]
#![allow(clippy::blocks_in_conditions)]

use std::{
    fmt::Debug,
    net::SocketAddr,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use anyhow::Result;
use async_trait::async_trait;
use futures::{future::BoxFuture, FutureExt};
use linera_base::identifiers::ChainId;
use linera_core::{notifier::Notifier, JoinSetExt as _};
use linera_rpc::{
    config::{
        ShardConfig, TlsConfig, ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig,
    },
    grpc::{
        api::{
            notifier_service_server::{NotifierService, NotifierServiceServer},
            validator_node_server::{ValidatorNode, ValidatorNodeServer},
            validator_worker_client::ValidatorWorkerClient,
            Blob, BlobId, BlockProposal, Certificate, ChainInfoQuery, ChainInfoResult,
            LiteCertificate, Notification, SubscriptionRequest, VersionInfo,
        },
        pool::GrpcConnectionPool,
        GrpcProxyable, GRPC_MAX_MESSAGE_SIZE,
    },
};
use linera_storage::Storage;
use rcgen::generate_simple_self_signed;
use tokio::{select, task::JoinSet};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{
    transport::{Body, Channel, Identity, Server, ServerTlsConfig},
    Request, Response, Status,
};
use tower::{builder::ServiceBuilder, Layer, Service};
use tracing::{debug, info, instrument};
#[cfg(with_metrics)]
use {
    linera_base::{prometheus_util, sync::Lazy},
    prometheus::{HistogramVec, IntCounterVec},
};

#[cfg(with_metrics)]
use crate::prometheus_server;

#[cfg(with_metrics)]
static PROXY_REQUEST_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    prometheus_util::register_histogram_vec(
        "proxy_request_latency",
        "Proxy request latency",
        &[],
        Some(vec![
            0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0,
            50.0, 100.0, 200.0, 300.0, 400.0,
        ]),
    )
    .expect("Counter creation should not fail")
});

#[cfg(with_metrics)]
static PROXY_REQUEST_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    prometheus_util::register_int_counter_vec("proxy_request_count", "Proxy request count", &[])
        .expect("Counter creation should not fail")
});

#[cfg(with_metrics)]
static PROXY_REQUEST_SUCCESS: Lazy<IntCounterVec> = Lazy::new(|| {
    prometheus_util::register_int_counter_vec(
        "proxy_request_success",
        "Proxy request success",
        &["method_name"],
    )
    .expect("Counter creation should not fail")
});

#[cfg(with_metrics)]
static PROXY_REQUEST_ERROR: Lazy<IntCounterVec> = Lazy::new(|| {
    prometheus_util::register_int_counter_vec(
        "proxy_request_error",
        "Proxy request error",
        &["method_name"],
    )
    .expect("Counter creation should not fail")
});

#[derive(Clone)]
pub struct PrometheusMetricsMiddlewareLayer;

#[derive(Clone)]
pub struct PrometheusMetricsMiddlewareService<T> {
    service: T,
}

impl<S> Layer<S> for PrometheusMetricsMiddlewareLayer {
    type Service = PrometheusMetricsMiddlewareService<S>;

    fn layer(&self, service: S) -> Self::Service {
        PrometheusMetricsMiddlewareService { service }
    }
}

impl<S> Service<tonic::codegen::http::Request<Body>> for PrometheusMetricsMiddlewareService<S>
where
    S::Future: Send + 'static,
    S: Service<tonic::codegen::http::Request<Body>> + std::marker::Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<S::Response, S::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: tonic::codegen::http::Request<Body>) -> Self::Future {
        #[cfg(with_metrics)]
        let start = std::time::Instant::now();
        let future = self.service.call(request);
        async move {
            let response = future.await?;
            #[cfg(with_metrics)]
            {
                PROXY_REQUEST_LATENCY
                    .with_label_values(&[])
                    .observe(start.elapsed().as_secs_f64() * 1000.0);
                PROXY_REQUEST_COUNT.with_label_values(&[]).inc();
            }
            Ok(response)
        }
        .boxed()
    }
}

#[derive(Clone)]
pub struct GrpcProxy<S>(Arc<GrpcProxyInner<S>>);

struct GrpcProxyInner<S> {
    public_config: ValidatorPublicNetworkConfig,
    internal_config: ValidatorInternalNetworkConfig,
    worker_connection_pool: GrpcConnectionPool,
    notifier: Notifier<Result<Notification, Status>>,
    tls: TlsConfig,
    storage: S,
}

impl<S> GrpcProxy<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    pub fn new(
        public_config: ValidatorPublicNetworkConfig,
        internal_config: ValidatorInternalNetworkConfig,
        connect_timeout: Duration,
        timeout: Duration,
        tls: TlsConfig,
        storage: S,
    ) -> Self {
        Self(Arc::new(GrpcProxyInner {
            public_config,
            internal_config,
            worker_connection_pool: GrpcConnectionPool::default()
                .with_connect_timeout(connect_timeout)
                .with_timeout(timeout),
            notifier: Notifier::default(),
            tls,
            storage,
        }))
    }

    fn as_validator_node(&self) -> ValidatorNodeServer<Self> {
        ValidatorNodeServer::new(self.clone())
            .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
    }

    fn as_notifier_service(&self) -> NotifierServiceServer<Self> {
        NotifierServiceServer::new(self.clone())
    }

    fn public_address(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], self.0.public_config.port))
    }

    fn metrics_address(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], self.0.internal_config.metrics_port))
    }

    fn internal_address(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], self.0.internal_config.port))
    }

    fn shard_for(&self, proxyable: &impl GrpcProxyable) -> Option<ShardConfig> {
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
        let client = ValidatorWorkerClient::new(channel)
            .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE);

        Ok(client)
    }

    /// Runs the proxy. If either the public server or private server dies for whatever
    /// reason we'll kill the proxy.
    #[instrument(skip_all, fields(public_address = %self.public_address(), internal_address = %self.internal_address(), metrics_address = %self.metrics_address()), err)]
    pub async fn run(self, shutdown_signal: CancellationToken) -> Result<()> {
        info!("Starting gRPC server");
        let mut join_set = JoinSet::new();

        #[cfg(with_metrics)]
        prometheus_server::start_metrics(self.metrics_address(), shutdown_signal.clone());

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<ValidatorNodeServer<GrpcProxy<S>>>()
            .await;
        let internal_server = join_set.spawn_task(
            Server::builder()
                .add_service(self.as_notifier_service())
                .serve(self.internal_address()),
        );
        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(linera_rpc::FILE_DESCRIPTOR_SET)
            .build()?;
        let public_server = join_set.spawn_task(
            self.public_server()?
                .layer(
                    ServiceBuilder::new()
                        .layer(PrometheusMetricsMiddlewareLayer)
                        .into_inner(),
                )
                .accept_http1(true)
                .add_service(health_service)
                .add_service(tonic_web::enable(self.as_validator_node()))
                .add_service(tonic_web::enable(reflection_service))
                .serve_with_shutdown(self.public_address(), shutdown_signal.cancelled_owned()),
        );

        select! {
            internal_res = internal_server => internal_res??,
            public_res = public_server => public_res??,
        }
        Ok(())
    }

    /// Pre-configures the public server with no services attached.
    /// If a certificate and key are defined, creates a TLS server.
    fn public_server(&self) -> Result<Server> {
        match self.0.tls {
            TlsConfig::Tls => {
                let cert = generate_simple_self_signed(vec![self.0.public_config.host.clone()])?;
                let identity =
                    Identity::from_pem(cert.serialize_pem()?, cert.serialize_private_key_pem());
                let tls_config = ServerTlsConfig::new().identity(identity);
                Ok(Server::builder().tls_config(tls_config)?)
            }
            TlsConfig::ClearText => Ok(Server::builder()),
        }
    }

    async fn client_for_proxy_worker<R>(
        &self,
        request: Request<R>,
    ) -> Result<(ValidatorWorkerClient<Channel>, R), Status>
    where
        R: Debug + GrpcProxyable,
    {
        debug!("proxying request from {:?}", request.remote_addr());
        let inner = request.into_inner();
        let shard = self
            .shard_for(&inner)
            .ok_or_else(|| Status::not_found("could not find shard for message"))?;
        let client = self
            .worker_client_for_shard(&shard)
            .map_err(|_| Status::internal("could not connect to shard"))?;
        Ok((client, inner))
    }

    fn log_and_return_proxy_request_outcome(
        result: Result<Response<ChainInfoResult>, Status>,
        method_name: &str,
    ) -> Result<Response<ChainInfoResult>, Status> {
        #![allow(unused_variables)]
        match result {
            Ok(chain_info_result) => {
                #[cfg(with_metrics)]
                PROXY_REQUEST_SUCCESS
                    .with_label_values(&[method_name])
                    .inc();
                Ok(chain_info_result)
            }
            Err(status) => {
                #[cfg(with_metrics)]
                PROXY_REQUEST_ERROR.with_label_values(&[method_name]).inc();
                Err(status)
            }
        }
    }
}

#[async_trait]
impl<S> ValidatorNode for GrpcProxy<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    type SubscribeStream = UnboundedReceiverStream<Result<Notification, Status>>;

    #[instrument(skip_all, err(Display))]
    async fn handle_block_proposal(
        &self,
        request: Request<BlockProposal>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.client_for_proxy_worker(request).await?;
        Self::log_and_return_proxy_request_outcome(
            client.handle_block_proposal(inner).await,
            "handle_block_proposal",
        )
    }

    #[instrument(skip_all, err(Display))]
    async fn handle_lite_certificate(
        &self,
        request: Request<LiteCertificate>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.client_for_proxy_worker(request).await?;
        Self::log_and_return_proxy_request_outcome(
            client.handle_lite_certificate(inner).await,
            "handle_lite_certificate",
        )
    }

    #[instrument(skip_all, err(Display))]
    async fn handle_certificate(
        &self,
        request: Request<Certificate>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.client_for_proxy_worker(request).await?;
        Self::log_and_return_proxy_request_outcome(
            client.handle_certificate(inner).await,
            "handle_certificate",
        )
    }

    #[instrument(skip_all, err(Display))]
    async fn handle_chain_info_query(
        &self,
        request: Request<ChainInfoQuery>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.client_for_proxy_worker(request).await?;
        Self::log_and_return_proxy_request_outcome(
            client.handle_chain_info_query(inner).await,
            "handle_chain_info_query",
        )
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

    #[instrument(skip_all, err(Display))]
    async fn get_version_info(
        &self,
        _request: Request<()>,
    ) -> Result<Response<VersionInfo>, Status> {
        // We assume each shard is running the same version as the proxy
        Ok(Response::new(linera_version::VersionInfo::default().into()))
    }

    #[instrument(skip_all, err(Display))]
    async fn download_blob(&self, request: Request<BlobId>) -> Result<Response<Blob>, Status> {
        let blob_id = request.into_inner().try_into()?;
        let hashed_blob = self
            .0
            .storage
            .read_hashed_blob(blob_id)
            .await
            .map_err(|err| Status::from_error(Box::new(err)))?;
        Ok(Response::new(hashed_blob.into_inner().into()))
    }
}

#[async_trait]
impl<S> NotifierService for GrpcProxy<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    #[instrument(skip_all, err(Display))]
    async fn notify(&self, request: Request<Notification>) -> Result<Response<()>, Status> {
        let notification = request.into_inner();
        let chain_id = notification
            .chain_id
            .clone()
            .ok_or_else(|| Status::invalid_argument("Missing field: chain_id."))?
            .try_into()?;
        self.0.notifier.notify(&chain_id, &Ok(notification));
        Ok(Response::new(()))
    }
}

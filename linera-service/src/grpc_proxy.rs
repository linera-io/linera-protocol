// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::prometheus_server;
use anyhow::Result;
use async_trait::async_trait;
use futures::{future::BoxFuture, FutureExt};
use linera_base::identifiers::ChainId;
use linera_core::notifier::Notifier;
use linera_rpc::{
    config::{
        ShardConfig, TlsConfig, ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig,
    },
    grpc_network::{
        grpc::{
            notifier_service_server::{NotifierService, NotifierServiceServer},
            validator_node_server::{ValidatorNode, ValidatorNodeServer},
            validator_worker_client::ValidatorWorkerClient,
            BlockProposal, Certificate, ChainInfoQuery, ChainInfoResult, LiteCertificate,
            Notification, SubscriptionRequest,
        },
        Proxyable,
    },
    grpc_pool::ConnectionPool,
};
use prometheus::{register_histogram_vec, register_int_counter_vec, HistogramVec, IntCounterVec};
use rcgen::generate_simple_self_signed;
use std::{
    fmt::Debug,
    net::SocketAddr,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
    time::Duration,
};
use tokio::select;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{
    transport::{Body, Channel, Identity, Server, ServerTlsConfig},
    Request, Response, Status,
};
use tower::{builder::ServiceBuilder, Layer, Service};
use tracing::{debug, info, instrument};

pub static PROXY_REQUEST_LATENCY: OnceLock<HistogramVec> = OnceLock::new();

pub static PROXY_REQUEST_COUNT: OnceLock<IntCounterVec> = OnceLock::new();

pub static PROXY_REQUEST_SUCCESS: OnceLock<IntCounterVec> = OnceLock::new();

pub static PROXY_REQUEST_ERROR: OnceLock<IntCounterVec> = OnceLock::new();

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

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let start = std::time::Instant::now();
        let future = self.service.call(request);
        async move {
            let response = future.await?;
            PROXY_REQUEST_LATENCY
                .get_or_init(|| {
                    register_histogram_vec!("proxy_request_latency", "Proxy request latency", &[])
                        .expect("Counter creation should not fail")
                })
                .with_label_values(&[])
                .observe(start.elapsed().as_secs_f64());
            PROXY_REQUEST_COUNT
                .get_or_init(|| {
                    register_int_counter_vec!("proxy_request_count", "Proxy request count", &[])
                        .expect("Counter creation should not fail")
                })
                .with_label_values(&[])
                .inc();
            Ok(response)
        }
        .boxed()
    }
}

#[derive(Clone)]
pub struct GrpcProxy(Arc<GrpcProxyInner>);

struct GrpcProxyInner {
    public_config: ValidatorPublicNetworkConfig,
    internal_config: ValidatorInternalNetworkConfig,
    worker_connection_pool: ConnectionPool,
    notifier: Notifier<Result<Notification, Status>>,
    tls: TlsConfig,
}

impl GrpcProxy {
    pub fn new(
        public_config: ValidatorPublicNetworkConfig,
        internal_config: ValidatorInternalNetworkConfig,
        connect_timeout: Duration,
        timeout: Duration,
        tls: TlsConfig,
    ) -> Self {
        Self(Arc::new(GrpcProxyInner {
            public_config,
            internal_config,
            worker_connection_pool: ConnectionPool::default()
                .with_connect_timeout(connect_timeout)
                .with_timeout(timeout),
            notifier: Notifier::default(),
            tls,
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

    fn metrics_address(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], self.0.internal_config.metrics_port))
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
    #[instrument(skip_all, fields(public_address = %self.public_address(), internal_address = %self.internal_address(), metrics_address = %self.metrics_address()), err)]
    pub async fn run(self) -> Result<()> {
        info!("Starting gRPC server");

        prometheus_server::start_metrics(self.metrics_address());

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<ValidatorNodeServer<GrpcProxy>>()
            .await;
        let internal_server = Server::builder()
            .add_service(self.as_notifier_service())
            .serve(self.internal_address());
        let public_server = self
            .public_server()?
            .layer(
                ServiceBuilder::new()
                    .layer(PrometheusMetricsMiddlewareLayer)
                    .into_inner(),
            )
            .add_service(health_service)
            .add_service(self.as_validator_node())
            .serve(self.public_address());

        select! {
            internal_res = internal_server => internal_res?,
            public_res = public_server => public_res?,
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
        R: Debug + Proxyable,
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
        match result {
            Ok(chain_info_result) => {
                PROXY_REQUEST_SUCCESS
                    .get_or_init(|| {
                        register_int_counter_vec!(
                            "proxy_request_success",
                            "Proxy request success",
                            &["method_name"]
                        )
                        .expect("Counter creation should not fail")
                    })
                    .with_label_values(&[method_name])
                    .inc();
                Ok(chain_info_result)
            }
            Err(status) => {
                PROXY_REQUEST_ERROR
                    .get_or_init(|| {
                        register_int_counter_vec!(
                            "proxy_request_error",
                            "Proxy request error",
                            &["method_name"]
                        )
                        .expect("Counter creation should not fail")
                    })
                    .with_label_values(&[method_name])
                    .inc();
                Err(status)
            }
        }
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
        self.0.notifier.notify(&chain_id, &Ok(notification));
        Ok(Response::new(()))
    }
}

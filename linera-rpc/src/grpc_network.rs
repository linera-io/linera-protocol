// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(clippy::derive_partial_eq_without_eq)]
// https://github.com/hyperium/tonic/issues/1056
pub mod grpc {
    tonic::include_proto!("rpc.v1");
}

use crate::{
    config::{
        CrossChainConfig, NotificationConfig, ShardId, ValidatorInternalNetworkConfig,
        ValidatorPublicNetworkConfig,
    },
    conversions::ProtoConversionError,
    grpc_pool::ConnectionPool,
    mass::{MassClient, MassClientError},
    node_provider::NodeOptions,
    rpc::{HandleCertificateRequest, HandleLiteCertificateRequest},
    RpcMessage,
};
use async_trait::async_trait;
use futures::{
    channel::{mpsc, mpsc::Receiver, oneshot::Sender},
    future,
    future::BoxFuture,
    stream, FutureExt, StreamExt,
};
use grpc::{
    chain_info_result::Inner,
    notifier_service_client::NotifierServiceClient,
    validator_node_client::ValidatorNodeClient,
    validator_worker_client::ValidatorWorkerClient,
    validator_worker_server::{ValidatorWorker as ValidatorWorkerRpc, ValidatorWorkerServer},
    BlockProposal, Certificate, ChainInfoQuery, ChainInfoResult, CrossChainRequest,
    LiteCertificate, SubscriptionRequest,
};
use linera_base::{identifiers::ChainId, prometheus_util, sync::Lazy};
use linera_chain::data_types;
use linera_core::{
    node::{CrossChainMessageDelivery, NodeError, NotificationStream, ValidatorNode},
    worker::{NetworkActions, Notification, ValidatorWorker, WorkerError, WorkerState},
};
use linera_storage::Storage;
use linera_version::VersionInfo;
use linera_views::views::ViewError;
use prometheus::{HistogramVec, IntCounterVec};
use rand::Rng;
use std::{
    fmt::Debug,
    iter,
    net::{AddrParseError, SocketAddr},
    str::FromStr,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::{
    sync::oneshot,
    task::{JoinError, JoinHandle},
};
use tonic::{
    transport::{Body, Channel, Server},
    Code, Request, Response, Status,
};
use tower::{builder::ServiceBuilder, Layer, Service};
use tracing::{debug, error, info, instrument, warn};

type CrossChainSender = mpsc::Sender<(linera_core::data_types::CrossChainRequest, ShardId)>;
type NotificationSender = mpsc::Sender<Notification>;

static SERVER_REQUEST_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    prometheus_util::register_histogram_vec(
        "server_request_latency",
        "Server request latency",
        &[],
        Some(vec![0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0]),
    )
    .expect("Counter creation should not fail")
});

static SERVER_REQUEST_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    prometheus_util::register_int_counter_vec("server_request_count", "Server request count", &[])
        .expect("Counter creation should not fail")
});

static SERVER_REQUEST_SUCCESS: Lazy<IntCounterVec> = Lazy::new(|| {
    prometheus_util::register_int_counter_vec(
        "server_request_success",
        "Server request success",
        &["method_name"],
    )
    .expect("Counter creation should not fail")
});

static SERVER_REQUEST_ERROR: Lazy<IntCounterVec> = Lazy::new(|| {
    prometheus_util::register_int_counter_vec(
        "server_request_error",
        "Server request error",
        &["method_name"],
    )
    .expect("Counter creation should not fail")
});

static SERVER_REQUEST_LATENCY_PER_REQUEST_TYPE: Lazy<HistogramVec> = Lazy::new(|| {
    prometheus_util::register_histogram_vec(
        "server_request_latency_per_request_type",
        "Server request latency per request type",
        &["method_name"],
        Some(vec![0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0]),
    )
    .expect("Counter creation should not fail")
});

#[derive(Clone)]
pub struct GrpcServer<S> {
    state: WorkerState<S>,
    shard_id: ShardId,
    network: ValidatorInternalNetworkConfig,
    cross_chain_sender: CrossChainSender,
    notification_sender: NotificationSender,
}

pub struct GrpcServerHandle {
    _complete: Sender<()>,
    handle: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl GrpcServerHandle {
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

    #[error(transparent)]
    InvalidUri(#[from] http::uri::InvalidUri),
}

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
        let start = Instant::now();
        let future = self.service.call(request);
        async move {
            let response = future.await?;
            SERVER_REQUEST_LATENCY
                .with_label_values(&[])
                .observe(start.elapsed().as_secs_f64() * 1000.0);
            SERVER_REQUEST_COUNT.with_label_values(&[]).inc();
            Ok(response)
        }
        .boxed()
    }
}

impl<S> GrpcServer<S>
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    #[allow(clippy::too_many_arguments)]
    pub async fn spawn(
        host: String,
        port: u16,
        state: WorkerState<S>,
        shard_id: ShardId,
        internal_network: ValidatorInternalNetworkConfig,
        cross_chain_config: CrossChainConfig,
        notification_config: NotificationConfig,
    ) -> Result<GrpcServerHandle, GrpcError> {
        info!(
            "spawning gRPC server on {}:{} for shard {}",
            host, port, shard_id
        );

        let server_address = SocketAddr::from_str(&format!("{}:{}", host, port))?;

        let (cross_chain_sender, cross_chain_receiver) =
            mpsc::channel(cross_chain_config.queue_size);

        let (notification_sender, notification_receiver) =
            mpsc::channel(notification_config.notification_queue_size);

        tokio::spawn({
            info!(
                nickname = state.nickname(),
                "spawning cross-chain queries thread on {} for shard {}", host, shard_id
            );
            Self::forward_cross_chain_queries(
                state.nickname().to_string(),
                internal_network.clone(),
                cross_chain_config.max_retries,
                Duration::from_millis(cross_chain_config.retry_delay_ms),
                Duration::from_millis(cross_chain_config.sender_delay_ms),
                cross_chain_config.sender_failure_rate,
                cross_chain_config.max_concurrent_tasks,
                shard_id,
                cross_chain_receiver,
            )
        });

        tokio::spawn({
            info!(
                nickname = state.nickname(),
                "spawning notifications thread on {} for shard {}", host, shard_id
            );
            Self::forward_notifications(
                state.nickname().to_string(),
                internal_network.proxy_address(),
                notification_receiver,
            )
        });

        let (complete, receiver) = futures::channel::oneshot::channel();

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<ValidatorWorkerServer<Self>>()
            .await;

        let grpc_server = GrpcServer {
            state,
            shard_id,
            network: internal_network,
            cross_chain_sender,
            notification_sender,
        };
        let worker_node = ValidatorWorkerServer::new(grpc_server);

        let handle = tokio::spawn(
            Server::builder()
                .layer(
                    ServiceBuilder::new()
                        .layer(PrometheusMetricsMiddlewareLayer)
                        .into_inner(),
                )
                .add_service(health_service)
                .add_service(worker_node)
                .serve_with_shutdown(server_address, receiver.map(|_| ())),
        );

        Ok(GrpcServerHandle {
            _complete: complete,
            handle,
        })
    }

    /// Continuously waits for receiver to receive a notification which is then sent to
    /// the proxy.
    #[instrument(skip(receiver))]
    async fn forward_notifications(
        nickname: String,
        proxy_address: String,
        mut receiver: Receiver<Notification>,
    ) {
        let channel = Channel::from_shared(proxy_address.clone())
            .expect("Proxy URI should be valid")
            .connect_lazy();
        let mut client = NotifierServiceClient::new(channel);

        while let Some(notification) = receiver.next().await {
            let notification: grpc::Notification = match notification.clone().try_into() {
                Ok(notification) => notification,
                Err(error) => {
                    warn!(%error, nickname, "could not deserialize notification");
                    continue;
                }
            };
            let request = Request::new(notification.clone());
            if let Err(error) = client.notify(request).await {
                error!(
                    %error,
                    nickname,
                    ?notification,
                    "could not send notification",
                )
            }
        }
    }

    fn handle_network_actions(&self, actions: NetworkActions) {
        let mut cross_chain_sender = self.cross_chain_sender.clone();
        let mut notification_sender = self.notification_sender.clone();

        for request in actions.cross_chain_requests {
            let shard_id = self.network.get_shard_id(request.target_chain_id());
            debug!(
                source_shard_id = self.shard_id,
                target_shard_id = shard_id,
                "Scheduling cross-chain query",
            );

            if let Err(error) = cross_chain_sender.try_send((request, shard_id)) {
                error!(%error, "dropping cross-chain request");
                break;
            }
        }

        for notification in actions.notifications {
            debug!("Scheduling notification query");
            if let Err(error) = notification_sender.try_send(notification) {
                error!(%error, "dropping notification");
                break;
            }
        }
    }

    #[instrument(skip_all, fields(nickname, %this_shard))]
    #[allow(clippy::too_many_arguments)]
    async fn forward_cross_chain_queries(
        nickname: String,
        network: ValidatorInternalNetworkConfig,
        cross_chain_max_retries: u32,
        cross_chain_retry_delay: Duration,
        cross_chain_sender_delay: Duration,
        cross_chain_sender_failure_rate: f32,
        cross_chain_max_concurrent_tasks: usize,
        this_shard: ShardId,
        receiver: mpsc::Receiver<(linera_core::data_types::CrossChainRequest, ShardId)>,
    ) {
        let pool = ConnectionPool::default();
        let max_concurrent_tasks = Some(cross_chain_max_concurrent_tasks);

        receiver
            .for_each_concurrent(max_concurrent_tasks, |(cross_chain_request, shard_id)| {
                let shard = network.shard(shard_id);
                let remote_address = format!("http://{}", shard.address());

                let pool = pool.clone();
                let nickname = nickname.clone();

                // Send the cross-chain query and retry if needed.
                async move {
                    if cross_chain_sender_failure_rate > 0.0
                        && rand::thread_rng().gen::<f32>() < cross_chain_sender_failure_rate
                    {
                        warn!("Dropped 1 cross-chain message intentionally.");
                        return;
                    }

                    for i in 0..cross_chain_max_retries {
                        // Delay increases linearly with the attempt number.
                        tokio::time::sleep(cross_chain_sender_delay + cross_chain_retry_delay * i)
                            .await;

                        let result = || async {
                            let cross_chain_request = cross_chain_request.clone().try_into()?;
                            let request = Request::new(cross_chain_request);
                            let mut client =
                                ValidatorWorkerClient::new(pool.channel(remote_address.clone())?);
                            let response = client.handle_cross_chain_request(request).await?;
                            Ok::<_, anyhow::Error>(response)
                        };
                        match result().await {
                            Err(error) => {
                                warn!(
                                    nickname,
                                    %error,
                                    i,
                                    from_shard = this_shard,
                                    to_shard = shard_id,
                                    "Failed to send cross-chain query",
                                );
                            }
                            _ => {
                                debug!(
                                    from_shard = this_shard,
                                    to_shard = shard_id,
                                    "Sent cross-chain query",
                                );
                                break;
                            }
                        }
                        error!(
                            nickname,
                            from_shard = this_shard,
                            to_shard = shard_id,
                            "Dropping cross-chain query",
                        );
                    }
                }
            })
            .await;
    }

    fn log_request_success_and_latency(start: Instant, method_name: &str) {
        SERVER_REQUEST_LATENCY_PER_REQUEST_TYPE
            .with_label_values(&[method_name])
            .observe(start.elapsed().as_secs_f64() * 1000.0);
        SERVER_REQUEST_SUCCESS
            .with_label_values(&[method_name])
            .inc();
    }
}

#[tonic::async_trait]
impl<S> ValidatorWorkerRpc for GrpcServer<S>
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    #[instrument(target = "grpc_server", skip_all, err, fields(nickname = self.state.nickname(), chain_id = ?request.get_ref().chain_id()))]
    async fn handle_block_proposal(
        &self,
        request: Request<BlockProposal>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let start = Instant::now();
        let proposal = request.into_inner().try_into()?;
        debug!(?proposal, "Handling block proposal");
        Ok(Response::new(
            match self.state.clone().handle_block_proposal(proposal).await {
                Ok((info, actions)) => {
                    Self::log_request_success_and_latency(start, "handle_block_proposal");
                    self.handle_network_actions(actions);
                    info.try_into()?
                }
                Err(error) => {
                    SERVER_REQUEST_ERROR
                        .with_label_values(&["handle_block_proposal"])
                        .inc();
                    warn!(nickname = self.state.nickname(), %error, "Failed to handle block proposal");
                    NodeError::from(error).try_into()?
                }
            },
        ))
    }

    #[instrument(target = "grpc_server", skip_all, err, fields(nickname = self.state.nickname(), chain_id = ?request.get_ref().chain_id()))]
    async fn handle_lite_certificate(
        &self,
        request: Request<LiteCertificate>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let start = Instant::now();
        let HandleLiteCertificateRequest {
            certificate,
            wait_for_outgoing_messages,
        } = request.into_inner().try_into()?;
        debug!(?certificate, "Handling lite certificate");
        let (sender, receiver) = wait_for_outgoing_messages.then(oneshot::channel).unzip();
        match self
            .state
            .clone()
            .handle_lite_certificate(certificate, sender)
            .await
        {
            Ok((info, actions)) => {
                Self::log_request_success_and_latency(start, "handle_lite_certificate");
                self.handle_network_actions(actions);
                if let Some(receiver) = receiver {
                    if let Err(e) = receiver.await {
                        error!("Failed to wait for message delivery: {e}");
                    }
                }
                Ok(Response::new(info.try_into()?))
            }
            Err(error) => {
                SERVER_REQUEST_ERROR
                    .with_label_values(&["handle_lite_certificate"])
                    .inc();
                if let WorkerError::MissingCertificateValue = &error {
                    debug!(nickname = self.state.nickname(), %error, "Failed to handle lite certificate");
                } else {
                    error!(nickname = self.state.nickname(), %error, "Failed to handle lite certificate");
                }
                Ok(Response::new(NodeError::from(error).try_into()?))
            }
        }
    }

    #[instrument(target = "grpc_server", skip_all, err, fields(nickname = self.state.nickname(), chain_id = ?request.get_ref().chain_id()))]
    async fn handle_certificate(
        &self,
        request: Request<Certificate>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let start = Instant::now();
        let HandleCertificateRequest {
            certificate,
            blobs,
            wait_for_outgoing_messages,
        } = request.into_inner().try_into()?;
        debug!(?certificate, "Handling certificate");
        let (sender, receiver) = wait_for_outgoing_messages.then(oneshot::channel).unzip();
        match self
            .state
            .clone()
            .handle_certificate(certificate, blobs, sender)
            .await
        {
            Ok((info, actions)) => {
                Self::log_request_success_and_latency(start, "handle_certificate");
                self.handle_network_actions(actions);
                if let Some(receiver) = receiver {
                    if let Err(e) = receiver.await {
                        error!("Failed to wait for message delivery: {e}");
                    }
                }
                Ok(Response::new(info.try_into()?))
            }
            Err(error) => {
                SERVER_REQUEST_ERROR
                    .with_label_values(&["handle_certificate"])
                    .inc();
                error!(nickname = self.state.nickname(), %error, "Failed to handle certificate");
                Ok(Response::new(NodeError::from(error).try_into()?))
            }
        }
    }

    #[instrument(target = "grpc_server", skip_all, err, fields(nickname = self.state.nickname(), chain_id = ?request.get_ref().chain_id()))]
    async fn handle_chain_info_query(
        &self,
        request: Request<ChainInfoQuery>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let start = Instant::now();
        let query = request.into_inner().try_into()?;
        debug!(?query, "Handling chain info query");
        match self.state.clone().handle_chain_info_query(query).await {
            Ok((info, actions)) => {
                Self::log_request_success_and_latency(start, "handle_chain_info_query");
                self.handle_network_actions(actions);
                Ok(Response::new(info.try_into()?))
            }
            Err(error) => {
                SERVER_REQUEST_ERROR
                    .with_label_values(&["handle_chain_info_query"])
                    .inc();
                error!(nickname = self.state.nickname(), %error, "Failed to handle chain info query");
                Ok(Response::new(NodeError::from(error).try_into()?))
            }
        }
    }

    #[instrument(target = "grpc_server", skip_all, err, fields(nickname = self.state.nickname(), chain_id= ?request.get_ref().chain_id()))]
    async fn handle_cross_chain_request(
        &self,
        request: Request<CrossChainRequest>,
    ) -> Result<Response<()>, Status> {
        let start = Instant::now();
        let request = request.into_inner().try_into()?;
        debug!(?request, "Handling cross-chain request");
        match self.state.clone().handle_cross_chain_request(request).await {
            Ok(actions) => {
                Self::log_request_success_and_latency(start, "handle_cross_chain_request");
                self.handle_network_actions(actions)
            }
            Err(error) => {
                SERVER_REQUEST_ERROR
                    .with_label_values(&["handle_cross_chain_request"])
                    .inc();
                error!(nickname = self.state.nickname(), %error, "Failed to handle cross-chain request");
            }
        }
        Ok(Response::new(()))
    }
}

#[derive(Clone)]
pub struct GrpcClient {
    address: String,
    client: ValidatorNodeClient<Channel>,
    notification_retry_delay: Duration,
    notification_retries: u32,
}

impl GrpcClient {
    pub fn new(
        network: ValidatorPublicNetworkConfig,
        options: NodeOptions,
    ) -> Result<Self, GrpcError> {
        let address = network.http_address();
        let channel = Channel::from_shared(address.clone())?
            .connect_timeout(options.send_timeout)
            .timeout(options.recv_timeout)
            .connect_lazy();
        let client = ValidatorNodeClient::new(channel);
        Ok(Self {
            address,
            client,
            notification_retry_delay: options.notification_retry_delay,
            notification_retries: options.notification_retries,
        })
    }

    /// Returns whether this gRPC status means the server stream should be reconnected to, or not.
    /// Logs a warning on unexpected status codes.
    fn is_retryable(status: &Status) -> bool {
        match status.code() {
            Code::DeadlineExceeded | Code::Aborted | Code::Unavailable | Code::Unknown => {
                info!("Notification stream interrupted: {}; retrying", status);
                true
            }
            Code::Ok
            | Code::Cancelled
            | Code::NotFound
            | Code::AlreadyExists
            | Code::ResourceExhausted => {
                warn!("Unexpected gRPC status: {}; retrying", status);
                true
            }
            Code::InvalidArgument
            | Code::PermissionDenied
            | Code::FailedPrecondition
            | Code::OutOfRange
            | Code::Unimplemented
            | Code::Internal
            | Code::DataLoss
            | Code::Unauthenticated => {
                warn!("Unexpected gRPC status: {}", status);
                false
            }
        }
    }
}

macro_rules! client_delegate {
    ($self:ident, $handler:ident, $req:ident) => {{
        debug!(request = ?$req, "sending gRPC request");
        let request_inner = $req.try_into().map_err(|_| NodeError::GrpcError {
            error: "could not convert request to proto".to_string(),
        })?;
        let request = Request::new(request_inner);
        match $self
            .client
            .$handler(request)
            .await
            .map_err(|s| NodeError::GrpcError {
                error: format!(
                    "remote request [{}] failed with status: {:?}",
                    stringify!($handler),
                    s
                ),
            })?
            .into_inner()
            .inner
            .ok_or(NodeError::GrpcError {
                error: "missing body from response".to_string(),
            })? {
            Inner::ChainInfoResponse(response) => {
                Ok(response.try_into().map_err(|err| NodeError::GrpcError {
                    error: format!("failed to marshal response: {}", err),
                })?)
            }
            Inner::Error(error) => {
                Err(bincode::deserialize(&error).map_err(|err| NodeError::GrpcError {
                    error: format!("failed to marshal error message: {}", err),
                })?)
            }
        }
    }};
}

#[async_trait]
impl ValidatorNode for GrpcClient {
    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn handle_block_proposal(
        &mut self,
        proposal: data_types::BlockProposal,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        client_delegate!(self, handle_block_proposal, proposal)
    }

    #[instrument(target = "grpc_client", skip_all, fields(address = self.address))]
    async fn handle_lite_certificate(
        &mut self,
        certificate: data_types::LiteCertificate<'_>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        let wait_for_outgoing_messages = delivery.wait_for_outgoing_messages();
        let request = HandleLiteCertificateRequest {
            certificate,
            wait_for_outgoing_messages,
        };
        client_delegate!(self, handle_lite_certificate, request)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn handle_certificate(
        &mut self,
        certificate: data_types::Certificate,
        blobs: Vec<data_types::HashedValue>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        let wait_for_outgoing_messages = delivery.wait_for_outgoing_messages();
        let request = HandleCertificateRequest {
            certificate,
            blobs,
            wait_for_outgoing_messages,
        };
        client_delegate!(self, handle_certificate, request)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn handle_chain_info_query(
        &mut self,
        query: linera_core::data_types::ChainInfoQuery,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        client_delegate!(self, handle_chain_info_query, query)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn subscribe(&mut self, chains: Vec<ChainId>) -> Result<NotificationStream, NodeError> {
        let notification_retry_delay = self.notification_retry_delay;
        let notification_retries = self.notification_retries;
        let mut retry_count = 0;
        let subscription_request = SubscriptionRequest {
            chain_ids: chains.into_iter().map(|chain| chain.into()).collect(),
        };
        let mut client = self.client.clone();

        // Make the first connection attempt before returning from this method.
        let mut stream = Some(
            client
                .subscribe(subscription_request.clone())
                .await
                .map_err(|status| NodeError::SubscriptionFailed {
                    status: status.to_string(),
                })?
                .into_inner(),
        );

        // A stream of `Result<grpc::Notification, tonic::Status>` that keeps calling
        // `client.subscribe(request)` endlessly and without delay.
        let endlessly_retrying_notification_stream = stream::unfold((), move |()| {
            let mut client = client.clone();
            let subscription_request = subscription_request.clone();
            let mut stream = stream.take();
            async move {
                let stream = if let Some(stream) = stream.take() {
                    future::Either::Right(stream)
                } else {
                    match client.subscribe(subscription_request.clone()).await {
                        Err(err) => future::Either::Left(stream::iter(iter::once(Err(err)))),
                        Ok(response) => future::Either::Right(response.into_inner()),
                    }
                };
                Some((stream, ()))
            }
        })
        .flatten();

        // The stream of `Notification`s that inserts increasing delays after retriable errors, and
        // terminates after unexpected or fatal errors.
        let notification_stream = endlessly_retrying_notification_stream
            .map(|result| {
                Notification::try_from(result?).map_err(|err| {
                    let message = format!("Could not deserialize notification: {}", err);
                    tonic::Status::new(Code::Internal, message)
                })
            })
            .take_while(move |result| {
                let Err(status) = result else {
                    retry_count = 0;
                    return future::Either::Left(future::ready(true));
                };
                if !Self::is_retryable(status) || retry_count >= notification_retries {
                    return future::Either::Left(future::ready(false));
                }
                let delay = notification_retry_delay.saturating_mul(retry_count);
                retry_count += 1;
                future::Either::Right(async move {
                    tokio::time::sleep(delay).await;
                    true
                })
            })
            .filter_map(|result| future::ready(result.ok()));

        Ok(Box::pin(notification_stream))
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn get_version_info(&mut self) -> Result<VersionInfo, NodeError> {
        Ok(self.client.get_version_info(()).await?.into_inner().into())
    }
}

#[async_trait]
impl MassClient for GrpcClient {
    #[instrument(skip_all, err)]
    async fn send(
        &mut self,
        requests: Vec<RpcMessage>,
        max_in_flight: usize,
    ) -> Result<Vec<RpcMessage>, MassClientError> {
        let client = &mut self.client;
        let responses = stream::iter(requests)
            .map(|request| {
                let mut client = client.clone();
                async move {
                    let response = match request {
                        RpcMessage::BlockProposal(proposal) => {
                            let request = Request::new((*proposal).try_into()?);
                            client.handle_block_proposal(request).await?
                        }
                        RpcMessage::Certificate(request) => {
                            let request = Request::new((*request).try_into()?);
                            client.handle_certificate(request).await?
                        }
                        msg => panic!("attempted to send msg: {:?}", msg),
                    };
                    match response
                        .into_inner()
                        .inner
                        .ok_or(ProtoConversionError::MissingField)?
                    {
                        Inner::ChainInfoResponse(chain_info_response) => {
                            Ok(Some(RpcMessage::ChainInfoResponse(Box::new(
                                chain_info_response.try_into()?,
                            ))))
                        }
                        Inner::Error(error) => {
                            let error = bincode::deserialize::<NodeError>(&error)
                                .map_err(ProtoConversionError::BincodeError)?;
                            error!(?error, "received error response");
                            Ok(None)
                        }
                    }
                }
            })
            .buffer_unordered(max_in_flight)
            .filter_map(
                |result: Result<Option<_>, MassClientError>| async move { result.transpose() },
            )
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        Ok(responses)
    }
}

/// Types which are proxyable and expose the appropriate methods to be handled
/// by the `GrpcProxy`
pub trait Proxyable {
    fn chain_id(&self) -> Option<ChainId>;
}

impl Proxyable for BlockProposal {
    fn chain_id(&self) -> Option<ChainId> {
        self.chain_id.clone()?.try_into().ok()
    }
}

impl Proxyable for LiteCertificate {
    fn chain_id(&self) -> Option<ChainId> {
        self.chain_id.clone()?.try_into().ok()
    }
}

impl Proxyable for Certificate {
    fn chain_id(&self) -> Option<ChainId> {
        self.chain_id.clone()?.try_into().ok()
    }
}

impl Proxyable for ChainInfoQuery {
    fn chain_id(&self) -> Option<ChainId> {
        self.chain_id.clone()?.try_into().ok()
    }
}

impl Proxyable for CrossChainRequest {
    fn chain_id(&self) -> Option<ChainId> {
        use grpc::cross_chain_request::Inner;

        match self.inner.as_ref()? {
            Inner::UpdateRecipient(grpc::UpdateRecipient { recipient, .. })
            | Inner::ConfirmUpdatedRecipient(grpc::ConfirmUpdatedRecipient { recipient, .. }) => {
                recipient.clone()?.try_into().ok()
            }
        }
    }
}

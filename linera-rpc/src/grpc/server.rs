// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(with_metrics)]
use std::sync::LazyLock;
use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use futures::{
    channel::{mpsc, mpsc::Receiver},
    future::BoxFuture,
    FutureExt as _, StreamExt,
};
use linera_base::{data_types::Blob, identifiers::ChainId};
use linera_core::{
    node::NodeError,
    worker::{NetworkActions, Notification, WorkerError, WorkerState},
    JoinSetExt as _, TaskHandle,
};
use linera_storage::Storage;
use rand::Rng;
use tokio::{sync::oneshot, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tower::{builder::ServiceBuilder, Layer, Service};
use tracing::{debug, error, info, instrument, trace, warn};
#[cfg(with_metrics)]
use {
    linera_base::prometheus_util::{
        bucket_interval, register_histogram_vec, register_int_counter_vec,
    },
    prometheus::{HistogramVec, IntCounterVec},
};

use super::{
    api::{
        self,
        notifier_service_client::NotifierServiceClient,
        validator_worker_client::ValidatorWorkerClient,
        validator_worker_server::{ValidatorWorker as ValidatorWorkerRpc, ValidatorWorkerServer},
        BlockProposal, ChainInfoQuery, ChainInfoResult, CrossChainRequest,
        HandlePendingBlobRequest, LiteCertificate, PendingBlobRequest, PendingBlobResult,
    },
    pool::GrpcConnectionPool,
    GrpcError, GRPC_MAX_MESSAGE_SIZE,
};
use crate::{
    config::{CrossChainConfig, NotificationConfig, ShardId, ValidatorInternalNetworkConfig},
    HandleConfirmedCertificateRequest, HandleLiteCertRequest, HandleTimeoutCertificateRequest,
    HandleValidatedCertificateRequest,
};

type CrossChainSender = mpsc::Sender<(linera_core::data_types::CrossChainRequest, ShardId)>;
type NotificationSender = mpsc::Sender<Notification>;

#[cfg(with_metrics)]
static SERVER_REQUEST_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "server_request_latency",
        "Server request latency",
        &[],
        bucket_interval(0.1, 50.0),
    )
});

#[cfg(with_metrics)]
static SERVER_REQUEST_COUNT: LazyLock<IntCounterVec> =
    LazyLock::new(|| register_int_counter_vec("server_request_count", "Server request count", &[]));

#[cfg(with_metrics)]
static SERVER_REQUEST_SUCCESS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "server_request_success",
        "Server request success",
        &["method_name"],
    )
});

#[cfg(with_metrics)]
static SERVER_REQUEST_ERROR: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "server_request_error",
        "Server request error",
        &["method_name"],
    )
});

#[cfg(with_metrics)]
static SERVER_REQUEST_LATENCY_PER_REQUEST_TYPE: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "server_request_latency_per_request_type",
        "Server request latency per request type",
        &["method_name"],
        bucket_interval(0.1, 50.0),
    )
});

#[derive(Clone)]
pub struct GrpcServer<S>
where
    S: Storage,
{
    state: WorkerState<S>,
    shard_id: ShardId,
    network: ValidatorInternalNetworkConfig,
    cross_chain_sender: CrossChainSender,
    notification_sender: NotificationSender,
}

pub struct GrpcServerHandle {
    handle: TaskHandle<Result<(), GrpcError>>,
}

impl GrpcServerHandle {
    pub async fn join(self) -> Result<(), GrpcError> {
        self.handle.await?
    }
}

#[derive(Clone)]
pub struct GrpcPrometheusMetricsMiddlewareLayer;

#[derive(Clone)]
pub struct GrpcPrometheusMetricsMiddlewareService<T> {
    service: T,
}

impl<S> Layer<S> for GrpcPrometheusMetricsMiddlewareLayer {
    type Service = GrpcPrometheusMetricsMiddlewareService<S>;

    fn layer(&self, service: S) -> Self::Service {
        GrpcPrometheusMetricsMiddlewareService { service }
    }
}

impl<S, Req> Service<Req> for GrpcPrometheusMetricsMiddlewareService<S>
where
    S::Future: Send + 'static,
    S: Service<Req> + std::marker::Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<S::Response, S::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Req) -> Self::Future {
        #[cfg(with_metrics)]
        let start = Instant::now();
        let future = self.service.call(request);
        async move {
            let response = future.await?;
            #[cfg(with_metrics)]
            {
                SERVER_REQUEST_LATENCY
                    .with_label_values(&[])
                    .observe(start.elapsed().as_secs_f64() * 1000.0);
                SERVER_REQUEST_COUNT.with_label_values(&[]).inc();
            }
            Ok(response)
        }
        .boxed()
    }
}

impl<S> GrpcServer<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    #[expect(clippy::too_many_arguments)]
    pub fn spawn(
        host: String,
        port: u16,
        state: WorkerState<S>,
        shard_id: ShardId,
        internal_network: ValidatorInternalNetworkConfig,
        cross_chain_config: CrossChainConfig,
        notification_config: NotificationConfig,
        shutdown_signal: CancellationToken,
        join_set: &mut JoinSet<()>,
    ) -> GrpcServerHandle {
        info!(
            "spawning gRPC server on {}:{} for shard {}",
            host, port, shard_id
        );

        let (cross_chain_sender, cross_chain_receiver) =
            mpsc::channel(cross_chain_config.queue_size);

        let (notification_sender, notification_receiver) =
            mpsc::channel(notification_config.notification_queue_size);

        join_set.spawn_task({
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

        join_set.spawn_task({
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

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();

        let grpc_server = GrpcServer {
            state,
            shard_id,
            network: internal_network,
            cross_chain_sender,
            notification_sender,
        };

        let worker_node = ValidatorWorkerServer::new(grpc_server)
            .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE);

        let handle = join_set.spawn_task(async move {
            let server_address = SocketAddr::from((IpAddr::from_str(&host)?, port));

            let reflection_service = tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(crate::FILE_DESCRIPTOR_SET)
                .build_v1()?;

            health_reporter
                .set_serving::<ValidatorWorkerServer<Self>>()
                .await;

            tonic::transport::Server::builder()
                .layer(
                    ServiceBuilder::new()
                        .layer(GrpcPrometheusMetricsMiddlewareLayer)
                        .into_inner(),
                )
                .add_service(health_service)
                .add_service(reflection_service)
                .add_service(worker_node)
                .serve_with_shutdown(server_address, shutdown_signal.cancelled_owned())
                .await?;

            Ok(())
        });

        GrpcServerHandle { handle }
    }

    /// Continuously waits for receiver to receive a notification which is then sent to
    /// the proxy.
    #[instrument(skip(receiver))]
    async fn forward_notifications(
        nickname: String,
        proxy_address: String,
        mut receiver: Receiver<Notification>,
    ) {
        let channel = tonic::transport::Channel::from_shared(proxy_address.clone())
            .expect("Proxy URI should be valid")
            .connect_lazy();
        let mut client = NotifierServiceClient::new(channel)
            .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE);

        while let Some(notification) = receiver.next().await {
            let notification: api::Notification = match notification.clone().try_into() {
                Ok(notification) => notification,
                Err(error) => {
                    warn!(%error, nickname, "could not deserialize notification");
                    continue;
                }
            };
            let request = tonic::Request::new(notification.clone());
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
            trace!(
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
            trace!("Scheduling notification query");
            if let Err(error) = notification_sender.try_send(notification) {
                error!(%error, "dropping notification");
                break;
            }
        }
    }

    #[instrument(skip_all, fields(nickname, %this_shard))]
    #[expect(clippy::too_many_arguments)]
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
        let pool = GrpcConnectionPool::default();
        let max_concurrent_tasks = Some(cross_chain_max_concurrent_tasks);

        receiver
            .for_each_concurrent(max_concurrent_tasks, |(cross_chain_request, shard_id)| {
                let shard = network.shard(shard_id);
                let remote_address = shard.http_address();

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
                        linera_base::time::timer::sleep(
                            cross_chain_sender_delay + cross_chain_retry_delay * i,
                        )
                        .await;

                        let result = || async {
                            let cross_chain_request = cross_chain_request.clone().try_into()?;
                            let request = Request::new(cross_chain_request);
                            let mut client =
                                ValidatorWorkerClient::new(pool.channel(remote_address.clone())?)
                                    .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
                                    .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE);
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
                                trace!(
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
        #![allow(unused_variables)]
        #[cfg(with_metrics)]
        {
            SERVER_REQUEST_LATENCY_PER_REQUEST_TYPE
                .with_label_values(&[method_name])
                .observe(start.elapsed().as_secs_f64() * 1000.0);
            SERVER_REQUEST_SUCCESS
                .with_label_values(&[method_name])
                .inc();
        }
    }
}

#[tonic::async_trait]
impl<S> ValidatorWorkerRpc for GrpcServer<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    #[instrument(
        target = "grpc_server",
        skip_all,
        err,
        fields(
            nickname = self.state.nickname(),
            chain_id = ?request.get_ref().chain_id()
        )
    )]
    async fn handle_block_proposal(
        &self,
        request: Request<BlockProposal>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let start = Instant::now();
        let proposal = request.into_inner().try_into()?;
        trace!(?proposal, "Handling block proposal");
        Ok(Response::new(
            match self.state.clone().handle_block_proposal(proposal).await {
                Ok((info, actions)) => {
                    Self::log_request_success_and_latency(start, "handle_block_proposal");
                    self.handle_network_actions(actions);
                    info.try_into()?
                }
                Err(error) => {
                    #[cfg(with_metrics)]
                    {
                        SERVER_REQUEST_ERROR
                            .with_label_values(&["handle_block_proposal"])
                            .inc();
                    }
                    warn!(nickname = self.state.nickname(), %error, "Failed to handle block proposal");
                    NodeError::from(error).try_into()?
                }
            },
        ))
    }

    #[instrument(
        target = "grpc_server",
        skip_all,
        err,
        fields(
            nickname = self.state.nickname(),
            chain_id = ?request.get_ref().chain_id()
        )
    )]
    async fn handle_lite_certificate(
        &self,
        request: Request<LiteCertificate>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let start = Instant::now();
        let HandleLiteCertRequest {
            certificate,
            wait_for_outgoing_messages,
        } = request.into_inner().try_into()?;
        trace!(?certificate, "Handling lite certificate");
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
                #[cfg(with_metrics)]
                {
                    SERVER_REQUEST_ERROR
                        .with_label_values(&["handle_lite_certificate"])
                        .inc();
                }
                if let WorkerError::MissingCertificateValue = &error {
                    debug!(nickname = self.state.nickname(), %error, "Failed to handle lite certificate");
                } else {
                    error!(nickname = self.state.nickname(), %error, "Failed to handle lite certificate");
                }
                Ok(Response::new(NodeError::from(error).try_into()?))
            }
        }
    }

    #[instrument(
        target = "grpc_server",
        skip_all,
        err,
        fields(
            nickname = self.state.nickname(),
            chain_id = ?request.get_ref().chain_id()
        )
    )]
    async fn handle_confirmed_certificate(
        &self,
        request: Request<api::HandleConfirmedCertificateRequest>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let start = Instant::now();
        let HandleConfirmedCertificateRequest {
            certificate,
            wait_for_outgoing_messages,
        } = request.into_inner().try_into()?;
        trace!(?certificate, "Handling certificate");
        let (sender, receiver) = wait_for_outgoing_messages.then(oneshot::channel).unzip();
        match self
            .state
            .clone()
            .handle_confirmed_certificate(certificate, sender)
            .await
        {
            Ok((info, actions)) => {
                Self::log_request_success_and_latency(start, "handle_confirmed_certificate");
                self.handle_network_actions(actions);
                if let Some(receiver) = receiver {
                    if let Err(e) = receiver.await {
                        error!("Failed to wait for message delivery: {e}");
                    }
                }
                Ok(Response::new(info.try_into()?))
            }
            Err(error) => {
                #[cfg(with_metrics)]
                {
                    SERVER_REQUEST_ERROR
                        .with_label_values(&["handle_confirmed_certificate"])
                        .inc();
                }
                error!(nickname = self.state.nickname(), %error, "Failed to handle confirmed certificate");
                Ok(Response::new(NodeError::from(error).try_into()?))
            }
        }
    }

    #[instrument(
        target = "grpc_server",
        skip_all,
        err,
        fields(
            nickname = self.state.nickname(),
            chain_id = ?request.get_ref().chain_id()
        )
    )]
    async fn handle_validated_certificate(
        &self,
        request: Request<api::HandleValidatedCertificateRequest>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let start = Instant::now();
        let HandleValidatedCertificateRequest { certificate } = request.into_inner().try_into()?;
        trace!(?certificate, "Handling certificate");
        match self
            .state
            .clone()
            .handle_validated_certificate(certificate)
            .await
        {
            Ok((info, actions)) => {
                Self::log_request_success_and_latency(start, "handle_validated_certificate");
                self.handle_network_actions(actions);
                Ok(Response::new(info.try_into()?))
            }
            Err(error) => {
                #[cfg(with_metrics)]
                {
                    SERVER_REQUEST_ERROR
                        .with_label_values(&["handle_validated_certificate"])
                        .inc();
                }
                error!(nickname = self.state.nickname(), %error, "Failed to handle validated certificate");
                Ok(Response::new(NodeError::from(error).try_into()?))
            }
        }
    }

    #[instrument(
        target = "grpc_server",
        skip_all,
        err,
        fields(
            nickname = self.state.nickname(),
            chain_id = ?request.get_ref().chain_id()
        )
    )]
    async fn handle_timeout_certificate(
        &self,
        request: Request<api::HandleTimeoutCertificateRequest>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let start = Instant::now();
        let HandleTimeoutCertificateRequest { certificate } = request.into_inner().try_into()?;
        trace!(?certificate, "Handling Timeout certificate");
        match self
            .state
            .clone()
            .handle_timeout_certificate(certificate)
            .await
        {
            Ok((info, _actions)) => {
                Self::log_request_success_and_latency(start, "handle_timeout_certificate");
                Ok(Response::new(info.try_into()?))
            }
            Err(error) => {
                #[cfg(with_metrics)]
                {
                    SERVER_REQUEST_ERROR
                        .with_label_values(&["handle_timeout_certificate"])
                        .inc();
                }
                error!(nickname = self.state.nickname(), %error, "Failed to handle timeout certificate");
                Ok(Response::new(NodeError::from(error).try_into()?))
            }
        }
    }

    #[instrument(
        target = "grpc_server",
        skip_all,
        err,
        fields(
            nickname = self.state.nickname(),
            chain_id = ?request.get_ref().chain_id()
        )
    )]
    async fn handle_chain_info_query(
        &self,
        request: Request<ChainInfoQuery>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let start = Instant::now();
        let query = request.into_inner().try_into()?;
        trace!(?query, "Handling chain info query");
        match self.state.clone().handle_chain_info_query(query).await {
            Ok((info, actions)) => {
                Self::log_request_success_and_latency(start, "handle_chain_info_query");
                self.handle_network_actions(actions);
                Ok(Response::new(info.try_into()?))
            }
            Err(error) => {
                #[cfg(with_metrics)]
                {
                    SERVER_REQUEST_ERROR
                        .with_label_values(&["handle_chain_info_query"])
                        .inc();
                }
                error!(nickname = self.state.nickname(), %error, "Failed to handle chain info query");
                Ok(Response::new(NodeError::from(error).try_into()?))
            }
        }
    }

    #[instrument(
        target = "grpc_server",
        skip_all,
        err,
        fields(
            nickname = self.state.nickname(),
            chain_id = ?request.get_ref().chain_id()
        )
    )]
    async fn download_pending_blob(
        &self,
        request: Request<PendingBlobRequest>,
    ) -> Result<Response<PendingBlobResult>, Status> {
        let start = Instant::now();
        let (chain_id, blob_id) = request.into_inner().try_into()?;
        trace!(?chain_id, ?blob_id, "Download pending blob");
        match self
            .state
            .clone()
            .download_pending_blob(chain_id, blob_id)
            .await
        {
            Ok(blob) => {
                Self::log_request_success_and_latency(start, "download_pending_blob");
                Ok(Response::new(blob.into_content().try_into()?))
            }
            Err(error) => {
                #[cfg(with_metrics)]
                {
                    SERVER_REQUEST_ERROR
                        .with_label_values(&["download_pending_blob"])
                        .inc();
                }
                error!(nickname = self.state.nickname(), %error, "Failed to download pending blob");
                Ok(Response::new(NodeError::from(error).try_into()?))
            }
        }
    }

    #[instrument(
        target = "grpc_server",
        skip_all,
        err,
        fields(
            nickname = self.state.nickname(),
            chain_id = ?request.get_ref().chain_id
        )
    )]
    async fn handle_pending_blob(
        &self,
        request: Request<HandlePendingBlobRequest>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let start = Instant::now();
        let (chain_id, blob_content) = request.into_inner().try_into()?;
        let blob = Blob::new(blob_content);
        let blob_id = blob.id();
        trace!(?chain_id, ?blob_id, "Handle pending blob");
        match self.state.clone().handle_pending_blob(chain_id, blob).await {
            Ok(info) => {
                Self::log_request_success_and_latency(start, "handle_pending_blob");
                Ok(Response::new(info.try_into()?))
            }
            Err(error) => {
                #[cfg(with_metrics)]
                {
                    SERVER_REQUEST_ERROR
                        .with_label_values(&["handle_pending_blob"])
                        .inc();
                }
                error!(nickname = self.state.nickname(), %error, "Failed to handle pending blob");
                Ok(Response::new(NodeError::from(error).try_into()?))
            }
        }
    }

    #[instrument(
        target = "grpc_server",
        skip_all,
        err,
        fields(
            nickname = self.state.nickname(),
            chain_id = ?request.get_ref().chain_id()
        )
    )]
    async fn handle_cross_chain_request(
        &self,
        request: Request<CrossChainRequest>,
    ) -> Result<Response<()>, Status> {
        let start = Instant::now();
        let request = request.into_inner().try_into()?;
        trace!(?request, "Handling cross-chain request");
        match self.state.clone().handle_cross_chain_request(request).await {
            Ok(actions) => {
                Self::log_request_success_and_latency(start, "handle_cross_chain_request");
                self.handle_network_actions(actions)
            }
            Err(error) => {
                #[cfg(with_metrics)]
                {
                    SERVER_REQUEST_ERROR
                        .with_label_values(&["handle_cross_chain_request"])
                        .inc();
                }
                error!(nickname = self.state.nickname(), %error, "Failed to handle cross-chain request");
            }
        }
        Ok(Response::new(()))
    }
}

/// Types which are proxyable and expose the appropriate methods to be handled
/// by the `GrpcProxy`
pub trait GrpcProxyable {
    fn chain_id(&self) -> Option<ChainId>;
}

impl GrpcProxyable for BlockProposal {
    fn chain_id(&self) -> Option<ChainId> {
        self.chain_id.clone()?.try_into().ok()
    }
}

impl GrpcProxyable for LiteCertificate {
    fn chain_id(&self) -> Option<ChainId> {
        self.chain_id.clone()?.try_into().ok()
    }
}

impl GrpcProxyable for api::HandleConfirmedCertificateRequest {
    fn chain_id(&self) -> Option<ChainId> {
        self.chain_id.clone()?.try_into().ok()
    }
}

impl GrpcProxyable for api::HandleTimeoutCertificateRequest {
    fn chain_id(&self) -> Option<ChainId> {
        self.chain_id.clone()?.try_into().ok()
    }
}

impl GrpcProxyable for api::HandleValidatedCertificateRequest {
    fn chain_id(&self) -> Option<ChainId> {
        self.chain_id.clone()?.try_into().ok()
    }
}

impl GrpcProxyable for ChainInfoQuery {
    fn chain_id(&self) -> Option<ChainId> {
        self.chain_id.clone()?.try_into().ok()
    }
}

impl GrpcProxyable for PendingBlobRequest {
    fn chain_id(&self) -> Option<ChainId> {
        self.chain_id.clone()?.try_into().ok()
    }
}

impl GrpcProxyable for HandlePendingBlobRequest {
    fn chain_id(&self) -> Option<ChainId> {
        self.chain_id.clone()?.try_into().ok()
    }
}

impl GrpcProxyable for CrossChainRequest {
    fn chain_id(&self) -> Option<ChainId> {
        use super::api::cross_chain_request::Inner;

        match self.inner.as_ref()? {
            Inner::UpdateRecipient(api::UpdateRecipient { recipient, .. })
            | Inner::ConfirmUpdatedRecipient(api::ConfirmUpdatedRecipient { recipient, .. }) => {
                recipient.clone()?.try_into().ok()
            }
        }
    }
}

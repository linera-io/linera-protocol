// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
    task::{Context, Poll},
};

use futures::{
    channel::mpsc, future::BoxFuture, stream::FuturesUnordered, FutureExt as _, StreamExt as _,
};
use linera_base::{
    data_types::Blob,
    identifiers::ChainId,
    time::{Duration, Instant},
};
use linera_core::{
    join_set_ext::JoinSet,
    node::NodeError,
    worker::{NetworkActions, Notification, Reason, WorkerState},
    JoinSetExt as _, TaskHandle,
};
use linera_storage::Storage;
use tokio::sync::{broadcast::error::RecvError, oneshot};
use tokio_util::sync::CancellationToken;
use tonic::{transport::Channel, Request, Response, Status};
use tower::{builder::ServiceBuilder, Layer, Service};
use tracing::{debug, error, info, instrument, trace, warn};

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
    cross_chain_message_queue, HandleConfirmedCertificateRequest, HandleLiteCertRequest,
    HandleTimeoutCertificateRequest, HandleValidatedCertificateRequest,
};

type CrossChainSender = mpsc::Sender<(linera_core::data_types::CrossChainRequest, ShardId)>;
type NotificationSender = tokio::sync::broadcast::Sender<Notification>;

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{
        exponential_bucket_interval, linear_bucket_interval, register_histogram_vec,
        register_int_counter_vec,
    };
    use prometheus::{HistogramVec, IntCounterVec};

    pub static SERVER_REQUEST_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "server_request_latency",
            "Server request latency",
            &[],
            linear_bucket_interval(1.0, 25.0, 2000.0),
        )
    });

    pub static SERVER_REQUEST_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec("server_request_count", "Server request count", &[])
    });

    pub static SERVER_REQUEST_SUCCESS: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "server_request_success",
            "Server request success",
            &["method_name"],
        )
    });

    pub static SERVER_REQUEST_ERROR: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "server_request_error",
            "Server request error",
            &["method_name"],
        )
    });

    pub static SERVER_REQUEST_LATENCY_PER_REQUEST_TYPE: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "server_request_latency_per_request_type",
                "Server request latency per request type",
                &["method_name"],
                linear_bucket_interval(1.0, 25.0, 2000.0),
            )
        });

    pub static CROSS_CHAIN_MESSAGE_CHANNEL_FULL: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "cross_chain_message_channel_full",
            "Cross-chain message channel full",
            &[],
        )
    });

    pub static NOTIFICATIONS_SKIPPED_RECEIVER_LAG: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "notifications_skipped_receiver_lag",
            "Number of notifications skipped because receiver lagged behind sender",
            &[],
        )
    });

    pub static NOTIFICATIONS_DROPPED_NO_RECEIVER: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "notifications_dropped_no_receiver",
            "Number of notifications dropped because no receiver was available",
            &[],
        )
    });

    pub static NOTIFICATION_BATCH_SIZE: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "notification_batch_size",
            "Number of notifications per batch sent to proxy",
            &[],
            exponential_bucket_interval(1.0, 250.0),
        )
    });

    pub static NOTIFICATION_BATCHES_SENT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "notification_batches_sent",
            "Total notification batches sent",
            &["status"],
        )
    });
}

/// Handles batched forwarding of notifications to proxy and exporters.
struct BatchForwarder {
    nickname: String,
    client: NotifierServiceClient<Channel>,
    exporter_clients: Vec<NotifierServiceClient<Channel>>,
    pending_notifications: Vec<Notification>,
    futures: FuturesUnordered<BoxFuture<'static, ()>>,
    batch_limit: usize,
    max_tasks: usize,
}

impl BatchForwarder {
    /// Spawns batch send tasks up to max_tasks limit.
    fn spawn_batches(&mut self) {
        while !self.pending_notifications.is_empty() && self.futures.len() < self.max_tasks {
            let chunk_size = std::cmp::min(self.batch_limit, self.pending_notifications.len());
            let batch: Vec<Notification> = self.pending_notifications.drain(..chunk_size).collect();

            #[cfg(with_metrics)]
            metrics::NOTIFICATION_BATCH_SIZE
                .with_label_values(&[])
                .observe(batch.len() as f64);

            let client = self.client.clone();
            let exporter_clients = self.exporter_clients.clone();
            let nickname = self.nickname.clone();

            self.futures.push(
                async move {
                    Self::send_batch(nickname, client, exporter_clients, batch).await;
                }
                .boxed(),
            );
        }
    }

    /// Returns true if there are no pending notifications and no in-flight tasks.
    fn is_fully_drained(&self) -> bool {
        self.pending_notifications.is_empty() && self.futures.is_empty()
    }

    /// Sends a batch of notifications to the proxy and exporters.
    async fn send_batch(
        nickname: String,
        mut client: NotifierServiceClient<Channel>,
        mut exporter_clients: Vec<NotifierServiceClient<Channel>>,
        batch: Vec<Notification>,
    ) {
        // Convert to proto notifications, logging any deserialization errors
        let mut proto_notifications = Vec::with_capacity(batch.len());
        for notification in &batch {
            match notification.clone().try_into() {
                Ok(proto) => proto_notifications.push(proto),
                Err(error) => {
                    warn!(
                        %error,
                        nickname,
                        ?notification.chain_id,
                        ?notification.reason,
                        "could not deserialize notification"
                    );
                }
            }
        }

        // Collect chain_ids for error logging
        let chain_ids: Vec<_> = batch.iter().map(|n| n.chain_id).collect();

        // Send batch to proxy
        let request = Request::new(api::NotificationBatch {
            notifications: proto_notifications.clone(),
        });
        let result = client.notify_batch(request).await;

        #[cfg(with_metrics)]
        {
            let status = if result.is_ok() { "success" } else { "error" };
            metrics::NOTIFICATION_BATCHES_SENT
                .with_label_values(&[status])
                .inc();
        }

        if let Err(error) = result {
            error!(
                %error,
                nickname,
                batch_size = proto_notifications.len(),
                ?chain_ids,
                "proxy: could not send notification batch",
            );
        }

        // Send NewBlock notifications to exporters
        let new_block_notifications: Vec<_> = batch
            .iter()
            .filter(|n| matches!(n.reason, Reason::NewBlock { .. }))
            .collect();

        let exporter_notifications: Vec<api::Notification> = new_block_notifications
            .iter()
            .filter_map(|n| (*n).clone().try_into().ok())
            .collect();

        if !exporter_notifications.is_empty() {
            let exporter_chain_ids: Vec<_> =
                new_block_notifications.iter().map(|n| n.chain_id).collect();

            for exporter_client in &mut exporter_clients {
                let request = Request::new(api::NotificationBatch {
                    notifications: exporter_notifications.clone(),
                });
                if let Err(error) = exporter_client.notify_batch(request).await {
                    error!(
                        %error,
                        nickname,
                        batch_size = exporter_notifications.len(),
                        ?exporter_chain_ids,
                        "block exporter: could not send notification batch",
                    );
                }
            }
        }
    }
}

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
                metrics::SERVER_REQUEST_LATENCY
                    .with_label_values(&[])
                    .observe(start.elapsed().as_secs_f64() * 1000.0);
                metrics::SERVER_REQUEST_COUNT.with_label_values(&[]).inc();
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
        join_set: &mut JoinSet,
    ) -> GrpcServerHandle {
        info!(
            "spawning gRPC server on {}:{} for shard {}",
            host, port, shard_id
        );

        let (cross_chain_sender, cross_chain_receiver) =
            mpsc::channel(cross_chain_config.queue_size);

        let (notification_sender, _) =
            tokio::sync::broadcast::channel(notification_config.notification_queue_size);

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
                shard_id,
                cross_chain_receiver,
            )
        });

        let mut exporter_forwarded = false;
        for proxy in &internal_network.proxies {
            let receiver = notification_sender.subscribe();
            join_set.spawn_task({
                info!(
                    nickname = state.nickname(),
                    "spawning notifications thread on {} for shard {}", host, shard_id
                );
                let exporter_addresses = if exporter_forwarded {
                    vec![]
                } else {
                    exporter_forwarded = true;
                    internal_network.exporter_addresses()
                };
                Self::forward_notifications(
                    state.nickname().to_string(),
                    proxy.internal_address(&internal_network.protocol),
                    exporter_addresses,
                    receiver,
                    notification_config.clone(),
                )
            });
        }

        let (health_reporter, health_service) = tonic_health::server::health_reporter();

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

    /// Continuously waits for receiver to receive notifications and sends them to
    /// the proxy in batches for improved throughput.
    #[instrument(skip(receiver, config))]
    async fn forward_notifications(
        nickname: String,
        proxy_address: String,
        exporter_addresses: Vec<String>,
        mut receiver: tokio::sync::broadcast::Receiver<Notification>,
        config: NotificationConfig,
    ) {
        let channel = tonic::transport::Channel::from_shared(proxy_address.clone())
            .expect("Proxy URI should be valid")
            .connect_lazy();
        let client = NotifierServiceClient::new(channel)
            .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE);

        let exporter_clients: Vec<NotifierServiceClient<Channel>> = exporter_addresses
            .iter()
            .map(|address| {
                let channel = tonic::transport::Channel::from_shared(address.clone())
                    .expect("Exporter URI should be valid")
                    .connect_lazy();
                NotifierServiceClient::new(channel)
                    .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
                    .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
            })
            .collect::<Vec<_>>();

        let mut forwarder = BatchForwarder {
            nickname: nickname.clone(),
            client,
            exporter_clients,
            pending_notifications: Vec::new(),
            futures: FuturesUnordered::new(),
            batch_limit: config.notification_batch_size,
            max_tasks: config.notification_max_in_flight,
        };

        loop {
            tokio::select! {
                biased;

                result = receiver.recv() => {
                    match result {
                        Ok(notification) => {
                            forwarder.pending_notifications.push(notification);

                            if forwarder.futures.is_empty()
                               || (forwarder.pending_notifications.len() >= forwarder.batch_limit
                                   && forwarder.futures.len() < forwarder.max_tasks) {
                                forwarder.spawn_batches();
                            }
                        }
                        Err(RecvError::Lagged(skipped_count)) => {
                            warn!(
                                nickname,
                                skipped_count, "notification receiver lagged, messages were skipped"
                            );
                            #[cfg(with_metrics)]
                            metrics::NOTIFICATIONS_SKIPPED_RECEIVER_LAG
                                .with_label_values(&[])
                                .inc_by(skipped_count);
                        }
                        Err(RecvError::Closed) => {
                            warn!(
                                nickname,
                                "notification channel closed, draining pending notifications"
                            );
                            // Drain all pending notifications before exiting
                            loop {
                                forwarder.spawn_batches();
                                if forwarder.is_fully_drained() {
                                    break;
                                }
                                forwarder.futures.next().await;
                            }
                            break;
                        }
                    }
                }

                Some(()) = forwarder.futures.next() => {
                    forwarder.spawn_batches();
                }
            }
        }
    }

    fn handle_network_actions(&self, actions: NetworkActions) {
        let mut cross_chain_sender = self.cross_chain_sender.clone();
        let notification_sender = self.notification_sender.clone();

        for request in actions.cross_chain_requests {
            let shard_id = self.network.get_shard_id(request.target_chain_id());
            trace!(
                source_shard_id = self.shard_id,
                target_shard_id = shard_id,
                "Scheduling cross-chain query",
            );

            if let Err(error) = cross_chain_sender.try_send((request, shard_id)) {
                error!(%error, "dropping cross-chain request");
                #[cfg(with_metrics)]
                if error.is_full() {
                    metrics::CROSS_CHAIN_MESSAGE_CHANNEL_FULL
                        .with_label_values(&[])
                        .inc();
                }
            }
        }

        for notification in actions.notifications {
            trace!("Scheduling notification query");
            if let Err(error) = notification_sender.send(notification) {
                error!(%error, "dropping notification");
                #[cfg(with_metrics)]
                metrics::NOTIFICATIONS_DROPPED_NO_RECEIVER
                    .with_label_values(&[])
                    .inc();
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
        this_shard: ShardId,
        receiver: mpsc::Receiver<(linera_core::data_types::CrossChainRequest, ShardId)>,
    ) {
        let pool = GrpcConnectionPool::default();
        let handle_request =
            move |shard_id: ShardId, request: linera_core::data_types::CrossChainRequest| {
                let channel_result = pool.channel(network.shard(shard_id).http_address());
                async move {
                    let mut client = ValidatorWorkerClient::new(channel_result?)
                        .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
                        .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE);
                    client
                        .handle_cross_chain_request(Request::new(request.try_into()?))
                        .await?;
                    anyhow::Result::<_, anyhow::Error>::Ok(())
                }
            };
        cross_chain_message_queue::forward_cross_chain_queries(
            nickname,
            cross_chain_max_retries,
            cross_chain_retry_delay,
            cross_chain_sender_delay,
            cross_chain_sender_failure_rate,
            this_shard,
            receiver,
            handle_request,
        )
        .await;
    }

    fn log_request_outcome_and_latency(start: Instant, success: bool, method_name: &str) {
        #![cfg_attr(not(with_metrics), allow(unused_variables))]
        #[cfg(with_metrics)]
        {
            metrics::SERVER_REQUEST_LATENCY_PER_REQUEST_TYPE
                .with_label_values(&[method_name])
                .observe(start.elapsed().as_secs_f64() * 1000.0);
            if success {
                metrics::SERVER_REQUEST_SUCCESS
                    .with_label_values(&[method_name])
                    .inc();
            } else {
                metrics::SERVER_REQUEST_ERROR
                    .with_label_values(&[method_name])
                    .inc();
            }
        }
    }

    fn log_error(&self, error: &linera_core::worker::WorkerError, context: &str) {
        let nickname = self.state.nickname();
        if error.is_local() {
            error!(nickname, %error, "{}", context);
        } else {
            debug!(nickname, %error, "{}", context);
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
                    Self::log_request_outcome_and_latency(start, true, "handle_block_proposal");
                    self.handle_network_actions(actions);
                    info.try_into()?
                }
                Err(error) => {
                    Self::log_request_outcome_and_latency(start, false, "handle_block_proposal");
                    self.log_error(&error, "Failed to handle block proposal");
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
        match Box::pin(
            self.state
                .clone()
                .handle_lite_certificate(certificate, sender),
        )
        .await
        {
            Ok((info, actions)) => {
                Self::log_request_outcome_and_latency(start, true, "handle_lite_certificate");
                self.handle_network_actions(actions);
                if let Some(receiver) = receiver {
                    if let Err(e) = receiver.await {
                        error!("Failed to wait for message delivery: {e}");
                    }
                }
                Ok(Response::new(info.try_into()?))
            }
            Err(error) => {
                Self::log_request_outcome_and_latency(start, false, "handle_lite_certificate");
                self.log_error(&error, "Failed to handle lite certificate");
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
                Self::log_request_outcome_and_latency(start, true, "handle_confirmed_certificate");
                self.handle_network_actions(actions);
                if let Some(receiver) = receiver {
                    if let Err(e) = receiver.await {
                        error!("Failed to wait for message delivery: {e}");
                    }
                }
                Ok(Response::new(info.try_into()?))
            }
            Err(error) => {
                Self::log_request_outcome_and_latency(start, false, "handle_confirmed_certificate");
                self.log_error(&error, "Failed to handle confirmed certificate");
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
                Self::log_request_outcome_and_latency(start, true, "handle_validated_certificate");
                self.handle_network_actions(actions);
                Ok(Response::new(info.try_into()?))
            }
            Err(error) => {
                Self::log_request_outcome_and_latency(start, false, "handle_validated_certificate");
                self.log_error(&error, "Failed to handle validated certificate");
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
                Self::log_request_outcome_and_latency(start, true, "handle_timeout_certificate");
                Ok(Response::new(info.try_into()?))
            }
            Err(error) => {
                Self::log_request_outcome_and_latency(start, false, "handle_timeout_certificate");
                self.log_error(&error, "Failed to handle timeout certificate");
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
                Self::log_request_outcome_and_latency(start, true, "handle_chain_info_query");
                self.handle_network_actions(actions);
                Ok(Response::new(info.try_into()?))
            }
            Err(error) => {
                Self::log_request_outcome_and_latency(start, false, "handle_chain_info_query");
                self.log_error(&error, "Failed to handle chain info query");
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
                Self::log_request_outcome_and_latency(start, true, "download_pending_blob");
                Ok(Response::new(blob.into_content().try_into()?))
            }
            Err(error) => {
                Self::log_request_outcome_and_latency(start, false, "download_pending_blob");
                self.log_error(&error, "Failed to download pending blob");
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
                Self::log_request_outcome_and_latency(start, true, "handle_pending_blob");
                Ok(Response::new(info.try_into()?))
            }
            Err(error) => {
                Self::log_request_outcome_and_latency(start, false, "handle_pending_blob");
                self.log_error(&error, "Failed to handle pending blob");
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
                Self::log_request_outcome_and_latency(start, true, "handle_cross_chain_request");
                self.handle_network_actions(actions)
            }
            Err(error) => {
                Self::log_request_outcome_and_latency(start, false, "handle_cross_chain_request");
                let nickname = self.state.nickname();
                error!(nickname, %error, "Failed to handle cross-chain request");
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

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
    RpcMessage,
};
use async_trait::async_trait;
use futures::{
    channel::{mpsc, mpsc::Receiver, oneshot::Sender},
    future, stream, FutureExt, StreamExt,
};
use grpc::{
    chain_info_result::Inner,
    notifier_service_client::NotifierServiceClient,
    validator_node_client::ValidatorNodeClient,
    validator_worker_client::ValidatorWorkerClient,
    validator_worker_server::{ValidatorWorker as ValidatorWorkerRpc, ValidatorWorkerServer},
    BlockProposal, CertificateWithDependencies, ChainInfoQuery, ChainInfoResult, CrossChainRequest,
    LiteCertificate, SubscriptionRequest,
};
use linera_base::identifiers::ChainId;
use linera_chain::data_types;
use linera_core::{
    node::{NodeError, NotificationStream, ValidatorNode},
    worker::{NetworkActions, Notification, ValidatorWorker, WorkerError, WorkerState},
};
use linera_storage::Store;
use linera_views::views::ViewError;
use rand::Rng;
use std::{
    fmt::Debug,
    iter,
    net::{AddrParseError, SocketAddr},
    str::FromStr,
    time::Duration,
};
use thiserror::Error;
use tokio::{
    sync::oneshot,
    task::{JoinError, JoinHandle},
};
use tonic::{
    transport::{Channel, Server},
    Code, Request, Response, Status,
};
use tracing::{debug, error, info, instrument, warn};

type CrossChainSender = mpsc::Sender<(linera_core::data_types::CrossChainRequest, ShardId)>;
type NotificationSender = mpsc::Sender<Notification>;

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

impl<S> GrpcServer<S>
where
    S: Store + Clone + Send + Sync + 'static,
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
        this_shard: ShardId,
        mut receiver: mpsc::Receiver<(linera_core::data_types::CrossChainRequest, ShardId)>,
    ) {
        let pool = ConnectionPool::default();

        while let Some((cross_chain_request, shard_id)) = receiver.next().await {
            if rand::thread_rng().gen::<f32>() < cross_chain_sender_failure_rate {
                warn!("Dropped 1 cross-chain message intentionally.");
                continue;
            }

            let shard = network.shard(shard_id);
            let remote_address = format!("http://{}", shard.address());

            // Send the cross-chain query and retry if needed.
            for i in 0..cross_chain_max_retries {
                // Delay increases linearly with the attempt number.
                tokio::time::sleep(cross_chain_sender_delay + cross_chain_retry_delay * i).await;

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
    }
}

#[tonic::async_trait]
impl<S> ValidatorWorkerRpc for GrpcServer<S>
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    #[instrument(target = "grpc_server", skip_all, err, fields(nickname = self.state.nickname(), chain_id = ?request.get_ref().chain_id()))]
    async fn handle_block_proposal(
        &self,
        request: Request<BlockProposal>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let proposal = request.into_inner().try_into()?;
        debug!(?proposal, "Handling block proposal");
        Ok(Response::new(
            match self.state.clone().handle_block_proposal(proposal).await {
                Ok((info, actions)) => {
                    self.handle_network_actions(actions);
                    info.try_into()?
                }
                Err(error) => {
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
        let certificate = request.into_inner().try_into()?;
        debug!(?certificate, "Handling lite certificate");
        let (sender, receiver) = oneshot::channel();
        match self
            .state
            .clone()
            .handle_lite_certificate(certificate, Some(sender))
            .await
        {
            Ok((info, actions)) => {
                self.handle_network_actions(actions);
                if let Err(e) = receiver.await {
                    error!("Failed to wait for message delivery: {e}");
                }
                Ok(Response::new(info.try_into()?))
            }
            Err(error) => {
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
        request: Request<CertificateWithDependencies>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let cert_with_deps: data_types::CertificateWithDependencies =
            request.into_inner().try_into()?;
        debug!(certificate = ?cert_with_deps.certificate, "Handling certificate");
        let (sender, receiver) = oneshot::channel();
        match self
            .state
            .clone()
            .handle_certificate(
                cert_with_deps.certificate,
                cert_with_deps.blobs,
                Some(sender),
            )
            .await
        {
            Ok((info, actions)) => {
                self.handle_network_actions(actions);
                if let Err(e) = receiver.await {
                    error!("Failed to wait for message delivery: {e}");
                }
                Ok(Response::new(info.try_into()?))
            }
            Err(error) => {
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
        let query = request.into_inner().try_into()?;
        debug!(?query, "Handling chain info query");
        match self.state.clone().handle_chain_info_query(query).await {
            Ok((info, actions)) => {
                self.handle_network_actions(actions);
                Ok(Response::new(info.try_into()?))
            }
            Err(error) => {
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
        let request = request.into_inner().try_into()?;
        debug!(?request, "Handling cross-chain request");
        match self.state.clone().handle_cross_chain_request(request).await {
            Ok(actions) => self.handle_network_actions(actions),
            Err(error) => {
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
        connect_timeout: Duration,
        timeout: Duration,
        notification_retry_delay: Duration,
        notification_retries: u32,
    ) -> Result<Self, GrpcError> {
        let address = network.http_address();
        let channel = Channel::from_shared(address.clone())?
            .connect_timeout(connect_timeout)
            .timeout(timeout)
            .connect_lazy();
        let client = ValidatorNodeClient::new(channel);
        Ok(Self {
            address,
            client,
            notification_retry_delay,
            notification_retries,
        })
    }

    /// Returns whether this gRPC status means the server stream should be reconnected to, or not.
    /// Logs a warning on unexpected status codes.
    fn is_retryable(status: &Status) -> bool {
        match status.code() {
            Code::DeadlineExceeded | Code::Aborted | Code::Unavailable => true,
            Code::Unknown
            | Code::Ok
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
        tracing::debug!(
            request = ?$req,
            "sending gRPC request",
        );
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

macro_rules! mass_client_delegate {
    ($client:ident, $handler:ident, $msg:ident, $responses: ident) => {{
        let response = $client.$handler(Request::new((*$msg).try_into()?)).await?;
        match response
            .into_inner()
            .inner
            .ok_or(ProtoConversionError::MissingField)?
        {
            Inner::ChainInfoResponse(chain_info_response) => {
                $responses.push(RpcMessage::ChainInfoResponse(Box::new(
                    chain_info_response.try_into()?,
                )));
            }
            Inner::Error(error) => {
                let error = bincode::deserialize::<NodeError>(&error)
                    .map_err(ProtoConversionError::BincodeError)?;
                error!(?error, "received error response")
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
        certificate: data_types::LiteCertificate,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        client_delegate!(self, handle_lite_certificate, certificate)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn handle_certificate(
        &mut self,
        certificate: data_types::Certificate,
        blobs: Vec<data_types::HashedValue>,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        let cert_with_deps = data_types::CertificateWithDependencies { certificate, blobs };
        client_delegate!(self, handle_certificate, cert_with_deps)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn handle_chain_info_query(
        &mut self,
        query: linera_core::data_types::ChainInfoQuery,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        debug!(?query);
        client_delegate!(self, handle_chain_info_query, query)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn subscribe(&mut self, chains: Vec<ChainId>) -> Result<NotificationStream, NodeError> {
        let notification_retry_delay = self.notification_retry_delay;
        let notification_retries = self.notification_retries;
        let mut delay = Duration::ZERO;
        let subscription_request = SubscriptionRequest {
            chain_ids: chains.into_iter().map(|chain| chain.into()).collect(),
        };
        let client = self.client.clone();

        // A stream of `Result<grpc::Notification, tonic::Status>` that keeps calling
        // `client.subscribe(request)` endlessly and without delay.
        let endlessly_retrying_notification_stream = stream::unfold((), move |()| {
            let mut client = client.clone();
            let subscription_request = subscription_request.clone();
            async move {
                let stream = match client.subscribe(subscription_request.clone()).await {
                    Err(err) => future::Either::Left(stream::iter(iter::once(Err(err)))),
                    Ok(response) => future::Either::Right(response.into_inner()),
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
                    delay = Duration::ZERO;
                    return future::Either::Left(future::ready(true));
                };
                if !Self::is_retryable(status)
                    || delay >= notification_retry_delay.saturating_mul(notification_retries)
                {
                    return future::Either::Left(future::ready(false));
                }
                delay = delay.saturating_add(notification_retry_delay);
                future::Either::Right(async move {
                    tokio::time::sleep(delay).await;
                    true
                })
            })
            .filter_map(|result| future::ready(result.ok()));

        Ok(Box::pin(notification_stream))
    }
}

#[async_trait]
impl MassClient for GrpcClient {
    #[instrument(skip_all, err)]
    async fn send(
        &mut self,
        requests: Vec<RpcMessage>,
    ) -> Result<Vec<RpcMessage>, MassClientError> {
        let client = &mut self.client;
        let mut responses = Vec::new();

        for request in requests {
            match request {
                RpcMessage::BlockProposal(proposal) => {
                    mass_client_delegate!(client, handle_block_proposal, proposal, responses)
                }
                RpcMessage::Certificate(certificate, blobs) => {
                    let cert_with_deps = Box::new(data_types::CertificateWithDependencies {
                        certificate: *certificate,
                        blobs,
                    });
                    mass_client_delegate!(client, handle_certificate, cert_with_deps, responses)
                }
                msg => panic!("attempted to send msg: {:?}", msg),
            }
        }
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

impl Proxyable for CertificateWithDependencies {
    fn chain_id(&self) -> Option<ChainId> {
        self.certificate.as_ref()?.chain_id.clone()?.try_into().ok()
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

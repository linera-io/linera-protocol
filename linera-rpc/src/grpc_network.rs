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
    FutureExt, StreamExt,
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
use std::{
    fmt::Debug,
    net::{AddrParseError, SocketAddr},
    str::FromStr,
    time::Duration,
};
use thiserror::Error;
use tokio::task::{JoinError, JoinHandle};
use tonic::{
    transport::{Channel, Server},
    Request, Response, Status,
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
            let request = Request::new(notification.clone().into());
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
    async fn forward_cross_chain_queries(
        nickname: String,
        network: ValidatorInternalNetworkConfig,
        cross_chain_max_retries: usize,
        cross_chain_retry_delay: Duration,
        this_shard: ShardId,
        mut receiver: mpsc::Receiver<(linera_core::data_types::CrossChainRequest, ShardId)>,
    ) {
        let pool = ConnectionPool::default();

        while let Some((cross_chain_request, shard_id)) = receiver.next().await {
            let shard = network.shard(shard_id);
            let remote_address = format!("http://{}", shard.address());
            let mut attempt = 0;
            // Send the cross-chain query and retry if needed.
            loop {
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
                        error!(
                            nickname,
                            %error,
                            attempt,
                            "Failed to send cross-chain query",
                        );
                        if attempt < cross_chain_max_retries {
                            tokio::time::sleep(cross_chain_retry_delay).await;
                            attempt += 1;
                            // retry
                        } else {
                            break;
                        }
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
        debug!(request = ?request, "Handling block proposal");
        Ok(Response::new(
            match self
                .state
                .clone()
                .handle_block_proposal(request.into_inner().try_into()?)
                .await
            {
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
        debug!(request = ?request, "Handling lite certificate");
        match self
            .state
            .clone()
            .handle_lite_certificate(request.into_inner().try_into()?)
            .await
        {
            Ok((info, actions)) => {
                self.handle_network_actions(actions);
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
        debug!(request = ?request, "Handling certificate");
        let cert_with_deps: data_types::CertificateWithDependencies =
            request.into_inner().try_into()?;
        match self
            .state
            .clone()
            .handle_certificate(cert_with_deps.certificate, cert_with_deps.blobs)
            .await
        {
            Ok((info, actions)) => {
                self.handle_network_actions(actions);
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
        let request = request.into_inner().try_into()?;
        debug!(request = ?request, "Handling chain info query");
        match self.state.clone().handle_chain_info_query(request).await {
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
        debug!(request = ?request, "Handling cross-chain request");
        match self
            .state
            .clone()
            .handle_cross_chain_request(request.into_inner().try_into()?)
            .await
        {
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
}

impl GrpcClient {
    pub(crate) async fn new(
        network: ValidatorPublicNetworkConfig,
        connect_timeout: Duration,
        timeout: Duration,
    ) -> Result<Self, GrpcError> {
        let address = network.http_address();
        let channel = Channel::from_shared(address.clone())?
            .connect_timeout(connect_timeout)
            .timeout(timeout)
            .connect()
            .await?;
        let client = ValidatorNodeClient::new(channel);
        Ok(Self { address, client })
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
                Ok(response.try_into().map_err(|_| NodeError::GrpcError {
                    error: "failed to marshal response".to_string(),
                })?)
            }
            Inner::Error(error) => {
                Err(bcs::from_bytes(&error).map_err(|_| NodeError::GrpcError {
                    error: "failed to marshal error message".to_string(),
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
                error!(
                    error = ?bcs::from_bytes::<NodeError>(&error)
                        .map_err(|e| ProtoConversionError::BcsError(e))?,
                    "received error response",
                )
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
        debug!(query = ?query);
        client_delegate!(self, handle_chain_info_query, query)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn subscribe(&mut self, chains: Vec<ChainId>) -> Result<NotificationStream, NodeError> {
        let subscription_request = SubscriptionRequest {
            chain_ids: chains.into_iter().map(|chain| chain.into()).collect(),
        };
        let notification_stream = self
            .client
            .subscribe(Request::new(subscription_request))
            .await
            .map_err(|e| NodeError::GrpcError {
                error: format!("remote request [subscribe] failed with status: {:?}", e),
            })?
            .into_inner();

        Ok(Box::pin(
            notification_stream
                .filter_map(|n| async { n.ok() })
                .filter_map(|n| async { Notification::try_from(n).ok() }),
        ))
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
    #[instrument(skip_all, err)]
    async fn send(&self, requests: Vec<RpcMessage>) -> Result<Vec<RpcMessage>, MassClientError> {
        let mut client = ValidatorNodeClient::connect(self.0.http_address()).await?;
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

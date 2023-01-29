// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::Debug,
    net::{AddrParseError, SocketAddr},
    str::FromStr,
    time::Duration,
};

use async_trait::async_trait;
use futures::{
    channel::{mpsc, mpsc::Receiver, oneshot::Sender},
    FutureExt, SinkExt, StreamExt,
};
use log::{debug, error, info, warn};
use thiserror::Error;
use tokio::task::{JoinError, JoinHandle};
use tonic::{
    transport::{Channel, Server},
    Request, Response, Status,
};

use linera_base::data_types::ChainId;
use linera_chain::data_types;
use linera_core::{
    node::{NodeError, NotificationStream, ValidatorNode},
    worker::{NetworkActions, Notification, ValidatorWorker, WorkerState},
};
use linera_storage::Store;
use linera_views::views::ViewError;

pub use crate::{
    client_delegate,
    config::{
        CrossChainConfig, ShardId, ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig,
    },
    conversions::ProtoConversionError,
    grpc_network::grpc::{
        chain_info_result::Inner,
        validator_node_client::ValidatorNodeClient,
        validator_worker_client::ValidatorWorkerClient,
        validator_worker_server::{ValidatorWorker as ValidatorWorkerRpc, ValidatorWorkerServer},
        BlockProposal, Certificate, ChainInfoQuery, ChainInfoResult, CrossChainRequest,
        LiteCertificate,
    },
    mass::{MassClient, MassClientError},
    mass_client_delegate,
    pool::Connect,
    RpcMessage,
};
use crate::{
    config::NotificationConfig,
    grpc_network::grpc::{notifier_service_client::NotifierServiceClient, SubscriptionRequest},
};

#[allow(clippy::derive_partial_eq_without_eq)]
// https://github.com/hyperium/tonic/issues/1056
pub mod grpc {
    tonic::include_proto!("rpc.v1");
}

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
            "spawning gRPC server  on {}:{} for shard {}",
            host, port, shard_id
        );

        let server_address = SocketAddr::from_str(&format!("{}:{}", host, port))?;

        let (cross_chain_sender, cross_chain_receiver) =
            mpsc::channel(cross_chain_config.queue_size);

        let (notification_sender, notification_receiver) =
            mpsc::channel(notification_config.notification_queue_size);

        tokio::spawn({
            info!(
                "[{}] spawning cross-chain queries thread on {} for shard {}",
                state.nickname(),
                host,
                shard_id
            );
            Self::forward_cross_chain_queries(
                state.nickname().to_string(),
                internal_network.clone(),
                cross_chain_config.max_retries,
                cross_chain_receiver,
            )
        });

        tokio::spawn({
            info!(
                "[{}] spawning notifications thread on {} for shard {}",
                state.nickname(),
                host,
                shard_id
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
    async fn forward_notifications(
        nickname: String,
        proxy_address: String,
        mut receiver: Receiver<Notification>,
    ) {
        // The `ConnectionPool` here acts as a 'lazy' client even though we only have
        // one connection. This is so that the connection is instantiated on-demand
        // and not when the server is started to avoid race conditions.
        let pool = NotifierServiceClient::<Channel>::pool();
        while let Some(notification) = receiver.next().await {
            let mut client = match pool.client_for_address_mut(proxy_address.clone()).await {
                Ok(client) => client,
                Err(error) => {
                    error!(
                        "[{}] could not create client for the proxy at {} with error: {}",
                        nickname, proxy_address, error
                    );
                    continue;
                }
            };
            let request = Request::new(notification.clone().into());
            if let Err(e) = client.notify(request).await {
                warn!(
                    "[{}] could not send notification: {:?} with error: {:?}",
                    nickname, notification, e
                )
            }
        }
    }

    async fn handle_network_actions(&self, actions: NetworkActions) {
        let mut cross_chain_sender = self.cross_chain_sender.clone();
        let mut notification_sender = self.notification_sender.clone();

        for request in actions.cross_chain_requests {
            let shard_id = self.network.get_shard_id(request.target_chain_id());
            debug!(
                "[{}] Scheduling cross-chain query: {} -> {}",
                self.state.nickname(),
                self.shard_id,
                shard_id
            );

            cross_chain_sender
                .feed((request, shard_id))
                .await
                .expect("sinks are never closed so `feed` should not fail.")
        }

        for notification in actions.notifications {
            notification_sender
                .feed(notification)
                .await
                .expect("sinks are never closed so call to `feed` should not fail");
        }

        let _ = cross_chain_sender.flush();
        let _ = notification_sender.flush();
    }

    async fn forward_cross_chain_queries(
        nickname: String,
        network: ValidatorInternalNetworkConfig,
        this_shard: ShardId,
        mut receiver: mpsc::Receiver<(linera_core::data_types::CrossChainRequest, ShardId)>,
    ) {
        let mut queries_sent = 0u64;
        let mut failed = 0u64;

        let pool = ValidatorWorkerClient::<Channel>::pool();

        while let Some((cross_chain_request, shard_id)) = receiver.next().await {
            let shard = network.shard(shard_id);
            let http_address = format!("http://{}", shard.address());
            let mut client = match pool.client_for_address_mut(http_address.clone()).await {
                Ok(client) => client,
                Err(error) => {
                    error!(
                        "[{}] could not create client to {} with error: {}",
                        nickname, http_address, error
                    );
                    continue;
                }
            };

            let mut back_off = Duration::from_millis(100);

            for attempt in 0..10 {
                let request = Request::new(cross_chain_request.clone().try_into().expect("todo"));
                match client.handle_cross_chain_request(request).await {
                    Ok(_) => {
                        queries_sent += 1;
                        break;
                    }
                    Err(error) => {
                        if attempt < 10 {
                            failed += 1;
                            back_off *= 2;
                            error!(
                                "[{}] cross chain query to {} failed: {:?}, backing off for {} ms...",
                                nickname,
                                shard.address(),
                                error,
                                back_off.as_millis()
                            );
                            error!(
                                "[{}] queries succeeded: {}. queries failed: {}",
                                nickname, queries_sent, failed
                            );
                            tokio::time::sleep(back_off).await;
                        } else {
                            error!(
                                "[{}] Failed to send cross-chain query (giving up after {} retries): {}",
                                nickname, attempt, error
                            );
                        }
                    }
                }
            }

            if queries_sent % 2000 == 0 {
                // is this correct? does the `shard_id` not change?
                debug!(
                    "[{}] {} has sent {} cross-chain queries to {}:{} (shard {})",
                    nickname, this_shard, queries_sent, shard.host, shard.port, shard_id,
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
    async fn handle_block_proposal(
        &self,
        request: Request<BlockProposal>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        debug!(
            "server handler [handle_block_proposal] received delegating request [{:?}]",
            request
        );
        Ok(Response::new(
            match self
                .state
                .clone()
                .handle_block_proposal(request.into_inner().try_into()?)
                .await
            {
                Ok(chain_info_response) => chain_info_response.try_into()?,
                Err(error) => NodeError::from(error).try_into()?,
            },
        ))
    }

    async fn handle_lite_certificate(
        &self,
        request: Request<LiteCertificate>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        debug!(
            "server handler [handle_certificate] received delegating request [{:?}] ",
            request
        );
        match self
            .state
            .clone()
            .handle_lite_certificate(request.into_inner().try_into()?)
            .await
        {
            Ok((info, actions)) => {
                self.handle_network_actions(actions).await;
                Ok(Response::new(info.try_into()?))
            }
            Err(error) => {
                warn!(
                    "[{}] Failed to handle lite certificate: {}",
                    self.state.nickname(),
                    error
                );
                Ok(Response::new(NodeError::from(error).try_into()?))
            }
        }
    }

    async fn handle_certificate(
        &self,
        request: Request<Certificate>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        debug!(
            "server handler [handle_certificate] received delegating request [{:?}] ",
            request
        );
        match self
            .state
            .clone()
            .handle_certificate(request.into_inner().try_into()?)
            .await
        {
            Ok((info, actions)) => {
                self.handle_network_actions(actions).await;
                Ok(Response::new(info.try_into()?))
            }
            Err(error) => {
                error!(
                    "[{}] Failed to handle certificate: {}",
                    self.state.nickname(),
                    error
                );
                Ok(Response::new(NodeError::from(error).try_into()?))
            }
        }
    }

    async fn handle_chain_info_query(
        &self,
        request: Request<ChainInfoQuery>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        debug!(
            "server handler [handle_chain_info_query] received delegating request [{:?}]",
            request
        );
        Ok(Response::new(
            match self
                .state
                .clone()
                .handle_chain_info_query(request.into_inner().try_into()?)
                .await
            {
                Ok(chain_info_response) => chain_info_response.try_into()?,
                Err(error) => NodeError::from(error).try_into()?,
            },
        ))
    }

    async fn handle_cross_chain_request(
        &self,
        request: Request<CrossChainRequest>,
    ) -> Result<Response<()>, Status> {
        debug!(
            "server handler [handle_cross_chain_request] received delegating request [{:?}] ",
            request
        );
        match self
            .state
            .clone()
            .handle_cross_chain_request(request.into_inner().try_into()?)
            .await
        {
            Ok(actions) => self.handle_network_actions(actions).await,
            Err(error) => {
                error!(
                    "[{}] Failed to handle cross-chain request: {}",
                    self.state.nickname(),
                    error
                );
            }
        }
        Ok(Response::new(()))
    }
}

#[derive(Clone)]
pub struct GrpcClient(ValidatorNodeClient<Channel>);

impl GrpcClient {
    pub(crate) async fn new(network: ValidatorPublicNetworkConfig) -> Result<Self, GrpcError> {
        Ok(Self(
            ValidatorNodeClient::connect(network.http_address()).await?,
        ))
    }
}

#[async_trait]
impl ValidatorNode for GrpcClient {
    async fn handle_block_proposal(
        &mut self,
        proposal: data_types::BlockProposal,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        client_delegate!(self, handle_block_proposal, proposal)
    }

    async fn handle_lite_certificate(
        &mut self,
        certificate: data_types::LiteCertificate,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        client_delegate!(self, handle_lite_certificate, certificate)
    }

    async fn handle_certificate(
        &mut self,
        certificate: data_types::Certificate,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        client_delegate!(self, handle_certificate, certificate)
    }

    async fn handle_chain_info_query(
        &mut self,
        query: linera_core::data_types::ChainInfoQuery,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        client_delegate!(self, handle_chain_info_query, query)
    }

    async fn subscribe(&mut self, chains: Vec<ChainId>) -> Result<NotificationStream, NodeError> {
        let subscription_request = SubscriptionRequest {
            chain_ids: chains.into_iter().map(|chain| chain.into()).collect(),
        };
        let notification_stream = self
            .0
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
    async fn send(&self, requests: Vec<RpcMessage>) -> Result<Vec<RpcMessage>, MassClientError> {
        let mut client = ValidatorNodeClient::connect(self.0.http_address()).await?;
        let mut responses = Vec::new();

        for request in requests {
            match request {
                RpcMessage::BlockProposal(proposal) => {
                    mass_client_delegate!(client, handle_block_proposal, proposal, responses)
                }
                RpcMessage::Certificate(certificate) => {
                    mass_client_delegate!(client, handle_certificate, certificate, responses)
                }
                msg => panic!("attempted to send msg: {:?}", msg),
            }
        }
        Ok(responses)
    }
}

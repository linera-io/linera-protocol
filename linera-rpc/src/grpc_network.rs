// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    client_delegate,
    config::{
        CrossChainConfig, ShardId, ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig,
    },
    conversions::ProtoConversionError,
    convert_and_delegate,
    grpc_network::grpc::{
        chain_info_result::Inner,
        validator_node_client::ValidatorNodeClient,
        validator_node_server::{ValidatorNode as ValidatorNodeRpc, ValidatorNodeServer},
        validator_worker_client::ValidatorWorkerClient,
        validator_worker_server::{ValidatorWorker as ValidatorWorkerRpc, ValidatorWorkerServer},
        BlockProposal, Certificate, ChainInfoQuery, ChainInfoResult, CrossChainRequest,
    },
    mass::{MassClient, MassClientError},
    mass_client_delegate,
    pool::Connect,
    RpcMessage,
};
use async_trait::async_trait;
use futures::{
    channel::{mpsc, oneshot::Sender},
    FutureExt, SinkExt, StreamExt,
};
use linera_chain::data_types;
use linera_core::{
    node::{NodeError, ValidatorNode},
    worker::{NetworkActions, ValidatorWorker, WorkerState},
};
use linera_storage::Store;
use linera_views::views::ViewError;
use log::{debug, error, info, warn};
use std::{
    net::{AddrParseError, SocketAddr},
    str::FromStr,
    time::Duration,
};
use thiserror::Error;
use tokio::task::{JoinError, JoinHandle};
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};
use crate::grpc_network::grpc::{ChainId, Notification};
use crate::grpc_network::grpc::notifier_service_client::NotifierServiceClient;

#[allow(clippy::derive_partial_eq_without_eq)]
// https://github.com/hyperium/tonic/issues/1056
pub mod grpc {
    tonic::include_proto!("rpc.v1");
}

type CrossChainSender = mpsc::Sender<(linera_core::data_types::CrossChainRequest, ShardId)>;

#[derive(Clone)]
pub struct GrpcServer<S> {
    state: WorkerState<S>,
    shard_id: ShardId,
    network: ValidatorInternalNetworkConfig,
    cross_chain_sender: CrossChainSender,
    _notifier_client: NotifierServiceClient<Channel>,
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
    pub async fn spawn(
        host: String,
        port: u16,
        state: WorkerState<S>,
        shard_id: ShardId,
        internal_network: ValidatorInternalNetworkConfig,
        public_network: ValidatorPublicNetworkConfig,
        cross_chain_config: CrossChainConfig,
    ) -> Result<GrpcServerHandle, GrpcError> {
        info!(
            "spawning gRPC server  on {}:{} for shard {}",
            host, port, shard_id
        );

        let proxy_address = format!("http://{}:{}", public_network.host, public_network.port);
        let notifier_client = NotifierServiceClient::connect(proxy_address).await?;

        let server_address = SocketAddr::from_str(&format!("{}:{}", host, port))?;

        let (cross_chain_sender, cross_chain_receiver) =
            mpsc::channel(cross_chain_config.queue_size);

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

        let (complete, receiver) = futures::channel::oneshot::channel();

        let grpc_server = GrpcServer {
            state,
            shard_id,
            network: internal_network,
            cross_chain_sender,
            _notifier_client: notifier_client,
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

    /// Notify clients subscribed to a given [`ChainId`] that there are updates
    /// for that chain.
    pub async fn _notify(&mut self, chain_id: &ChainId) {
        let notification = Notification {
            chain_id: Some(chain_id.clone()),
        };

        if let Err(e) = self._notifier_client.notify(notification).await {
            warn!(
                "There was an error while trying to notify for chain {:?}: {:?}",
                chain_id, e
            )
        }
    }

    async fn handle_network_actions(&self, actions: NetworkActions) {
        let mut sender = self.cross_chain_sender.clone();
        for request in actions.cross_chain_requests {
            let shard_id = self.network.get_shard_id(request.target_chain_id());
            debug!(
                "[{}] Scheduling cross-chain query: {} -> {}",
                self.state.nickname(),
                self.shard_id,
                shard_id
            );

            sender
                .feed((request, shard_id))
                .await
                .expect("sinks are never closed so `feed` should not fail.")
        }

        let _ = sender.flush();
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
        convert_and_delegate!(self, handle_block_proposal, request)
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
                    "[{}] Failed to handle cross-chain request: {}",
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
        convert_and_delegate!(self, handle_chain_info_query, request)
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

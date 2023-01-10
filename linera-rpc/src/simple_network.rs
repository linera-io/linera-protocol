// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    codec,
    config::{
        CrossChainConfig, ShardId, ValidatorInternalNetworkPreConfig,
        ValidatorPublicNetworkPreConfig,
    },
    mass::{MassClient, MassClientError},
    transport::{MessageHandler, ServerHandle, TransportProtocol},
    RpcMessage,
};
use async_trait::async_trait;
use futures::{channel::mpsc, sink::SinkExt, stream::StreamExt};
use linera_core::worker::NetworkActions;

use linera_chain::data_types::{BlockProposal, Certificate};
use linera_core::{
    data_types::{ChainInfoQuery, ChainInfoResponse},
    node::{NodeError, ValidatorNode},
    worker::{ValidatorWorker, WorkerState},
};
use linera_storage::Store;
use linera_views::views::ViewError;
use log::{debug, error, info, warn};
use std::{io, time::Duration};
use tokio::time;
use linera_base::data_types::ChainId;
use linera_core::node::NotificationStream;

#[derive(Clone)]
pub struct Server<S> {
    network: ValidatorInternalNetworkPreConfig<TransportProtocol>,
    host: String,
    port: u16,
    state: WorkerState<S>,
    shard_id: ShardId,
    cross_chain_config: CrossChainConfig,
    // Stats
    packets_processed: u64,
    user_errors: u64,
}

impl<S> Server<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        network: ValidatorInternalNetworkPreConfig<TransportProtocol>,
        host: String,
        port: u16,
        state: WorkerState<S>,
        shard_id: ShardId,
        cross_chain_config: CrossChainConfig,
    ) -> Self {
        Self {
            network,
            host,
            port,
            state,
            shard_id,
            cross_chain_config,
            packets_processed: 0,
            user_errors: 0,
        }
    }

    pub fn packets_processed(&self) -> u64 {
        self.packets_processed
    }

    pub fn user_errors(&self) -> u64 {
        self.user_errors
    }
}

impl<S> Server<S>
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    async fn forward_cross_chain_queries(
        nickname: String,
        network: ValidatorInternalNetworkPreConfig<TransportProtocol>,
        cross_chain_max_retries: usize,
        cross_chain_retry_delay: Duration,
        this_shard: ShardId,
        mut receiver: mpsc::Receiver<(RpcMessage, ShardId)>,
    ) {
        let mut pool = network
            .protocol
            .make_outgoing_connection_pool()
            .await
            .expect("Initialization should not fail");

        let mut queries_sent = 0u64;
        while let Some((message, shard_id)) = receiver.next().await {
            // Send cross-chain query.
            let shard = network.shard(shard_id);
            let remote_address = format!("{}:{}", shard.host, shard.port);
            for i in 0..cross_chain_max_retries {
                let status = pool.send_message_to(message.clone(), &remote_address).await;
                match status {
                    Err(error) => {
                        if i < cross_chain_max_retries {
                            error!(
                                "[{}] Failed to send cross-chain query ({}-th retry): {}",
                                nickname, i, error
                            );
                            tokio::time::sleep(cross_chain_retry_delay).await;
                        } else {
                            error!(
                                "[{}] Failed to send cross-chain query (giving up after {} retries): {}",
                                nickname, i, error
                            );
                        }
                    }
                    _ => {
                        debug!(
                            "[{}] Sent cross-chain query: {} -> {}",
                            nickname, this_shard, shard_id
                        );
                        queries_sent += 1;
                        break;
                    }
                }
            }
            if queries_sent % 2000 == 0 {
                debug!(
                    "[{}] {} has sent {} cross-chain queries to {}:{} (shard {})",
                    nickname, this_shard, queries_sent, shard.host, shard.port, shard_id,
                );
            }
        }
    }

    pub async fn spawn(self) -> Result<ServerHandle, io::Error> {
        info!(
            "Listening to {} traffic on {}:{}",
            self.network.protocol, self.host, self.port
        );
        let address = format!("{}:{}", self.host, self.port);

        let (cross_chain_sender, cross_chain_receiver) =
            mpsc::channel(self.cross_chain_config.queue_size);

        tokio::spawn(Self::forward_cross_chain_queries(
            self.state.nickname().to_string(),
            self.network.clone(),
            self.cross_chain_config.max_retries,
            Duration::from_millis(self.cross_chain_config.retry_delay_ms),
            self.shard_id,
            cross_chain_receiver,
        ));

        let protocol = self.network.protocol;
        let state = RunningServerState {
            server: self,
            cross_chain_sender,
        };
        // Launch server for the appropriate protocol.
        protocol.spawn_server(&address, state).await
    }
}

#[derive(Clone)]
struct RunningServerState<S> {
    server: Server<S>,
    cross_chain_sender: mpsc::Sender<(RpcMessage, ShardId)>,
}

impl<S> MessageHandler for RunningServerState<S>
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    fn handle_message(
        &mut self,
        message: RpcMessage,
    ) -> futures::future::BoxFuture<Option<RpcMessage>> {
        Box::pin(async move {
            let reply = match message {
                RpcMessage::BlockProposal(message) => {
                    match self.server.state.handle_block_proposal(*message).await {
                        Ok(info) => Ok(Some(info.into())),
                        Err(error) => Err(error.into()),
                    }
                }
                RpcMessage::Certificate(message) => {
                    match self.server.state.handle_certificate(*message).await {
                        Ok((info, actions)) => {
                            // Cross-shard requests
                            self.handle_network_actions(actions).await;
                            // Response
                            Ok(Some(info.into()))
                        }
                        Err(error) => Err(error.into()),
                    }
                }
                RpcMessage::ChainInfoQuery(message) => {
                    match self.server.state.handle_chain_info_query(*message).await {
                        Ok(info) => Ok(Some(info.into())),
                        Err(error) => Err(error.into()),
                    }
                }
                RpcMessage::CrossChainRequest(request) => {
                    match self.server.state.handle_cross_chain_request(*request).await {
                        Ok(actions) => {
                            self.handle_network_actions(actions).await;
                        }
                        Err(error) => {
                            error!(
                                "[{}] Failed to handle cross-chain request: {}",
                                self.server.state.nickname(),
                                error
                            );
                        }
                    }
                    // No user to respond to.
                    Ok(None)
                }
                RpcMessage::Vote(_) | RpcMessage::Error(_) | RpcMessage::ChainInfoResponse(_) => {
                    Err(NodeError::UnexpectedMessage)
                }
            };

            self.server.packets_processed += 1;
            if self.server.packets_processed % 5000 == 0 {
                debug!(
                    "[{}] {}:{} (shard {}) has processed {} packets",
                    self.server.state.nickname(),
                    self.server.host,
                    self.server.port,
                    self.server.shard_id,
                    self.server.packets_processed
                );
            }

            match reply {
                Ok(x) => x,
                Err(error) => {
                    warn!(
                        "[{}] User query failed: {}",
                        self.server.state.nickname(),
                        error
                    );
                    self.server.user_errors += 1;
                    Some(error.into())
                }
            }
        })
    }
}

impl<S> RunningServerState<S>
where
    S: Send,
{
    fn handle_network_actions(
        &mut self,
        actions: NetworkActions,
    ) -> futures::future::BoxFuture<()> {
        Box::pin(async move {
            for request in actions.cross_chain_requests {
                let shard_id = self.server.network.get_shard_id(request.target_chain_id());
                debug!(
                    "[{}] Scheduling cross-chain query: {} -> {}",
                    self.server.state.nickname(),
                    self.server.shard_id,
                    shard_id
                );
                self.cross_chain_sender
                    .send((request.into(), shard_id))
                    .await
                    .expect("internal channel should not fail");
            }
        })
    }
}

#[derive(Clone)]
pub struct SimpleClient {
    network: ValidatorPublicNetworkPreConfig<TransportProtocol>,
    send_timeout: Duration,
    recv_timeout: Duration,
}

impl SimpleClient {
    pub(crate) fn new(
        network: ValidatorPublicNetworkPreConfig<TransportProtocol>,
        send_timeout: Duration,
        recv_timeout: Duration,
    ) -> Self {
        Self {
            network,
            send_timeout,
            recv_timeout,
        }
    }

    async fn send_recv_internal(
        &mut self,
        message: RpcMessage,
    ) -> Result<RpcMessage, codec::Error> {
        let address = format!("{}:{}", self.network.host, self.network.port);
        let mut stream = self.network.protocol.connect(address).await?;
        // Send message
        time::timeout(self.send_timeout, stream.send(message))
            .await
            .map_err(|timeout| codec::Error::Io(timeout.into()))??;
        // Wait for reply
        time::timeout(self.recv_timeout, stream.next())
            .await
            .map_err(|timeout| codec::Error::Io(timeout.into()))?
            .transpose()?
            .ok_or_else(|| codec::Error::Io(std::io::ErrorKind::UnexpectedEof.into()))
    }

    async fn send_recv_info(
        &mut self,
        message: RpcMessage,
    ) -> Result<ChainInfoResponse, NodeError> {
        match self.send_recv_internal(message).await {
            Ok(RpcMessage::ChainInfoResponse(response)) => Ok(*response),
            Ok(RpcMessage::Error(error)) => Err(*error),
            Ok(_) => Err(NodeError::UnexpectedMessage),
            Err(error) => match error {
                codec::Error::Io(io_error) => Err(NodeError::ClientIoError {
                    error: format!("{}", io_error),
                }),
                _ => Err(NodeError::InvalidDecoding),
            },
        }
    }
}

#[async_trait]
impl ValidatorNode for SimpleClient {
    /// Initiate a new block.
    async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.send_recv_info(proposal.into()).await
    }

    /// Process a certificate.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.send_recv_info(certificate.into()).await
    }

    /// Handle information queries for this chain.
    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.send_recv_info(query.into()).await
    }

    async fn subscribe(&mut self, _chains: Vec<ChainId>) -> Result<NotificationStream, NodeError> {
        Err(NodeError::SubscriptionError {
            transport: self.network.protocol.to_string()
        })
    }
}

#[derive(Clone)]
pub struct SimpleMassClient {
    pub network: ValidatorPublicNetworkPreConfig<TransportProtocol>,
    send_timeout: std::time::Duration,
    recv_timeout: std::time::Duration,
    max_in_flight: u64,
}

impl SimpleMassClient {
    pub fn new(
        network: ValidatorPublicNetworkPreConfig<TransportProtocol>,
        send_timeout: std::time::Duration,
        recv_timeout: std::time::Duration,
        max_in_flight: u64,
    ) -> Self {
        Self {
            network,
            send_timeout,
            recv_timeout,
            max_in_flight,
        }
    }
}

#[async_trait]
impl MassClient for SimpleMassClient {
    async fn send(&self, requests: Vec<RpcMessage>) -> Result<Vec<RpcMessage>, MassClientError> {
        let address = format!("{}:{}", self.network.host, self.network.port);
        let mut stream = self.network.protocol.connect(address).await?;
        let mut requests = requests.into_iter();
        let mut in_flight = 0u64;
        let mut responses = Vec::new();

        loop {
            while in_flight < self.max_in_flight {
                let request = match requests.next() {
                    None => {
                        if in_flight == 0 {
                            return Ok(responses);
                        }
                        // No more entries to send.
                        break;
                    }
                    Some(request) => request,
                };
                let status = time::timeout(self.send_timeout, stream.send(request)).await;
                if let Err(error) = status {
                    error!("Failed to send request: {}", error);
                    continue;
                }
                in_flight += 1;
            }
            if requests.len() % 5000 == 0 && requests.len() > 0 {
                info!("In flight {} Remaining {}", in_flight, requests.len());
            }
            match time::timeout(self.recv_timeout, stream.next()).await {
                Ok(Some(Ok(message))) => {
                    in_flight -= 1;
                    responses.push(message);
                }
                Ok(Some(Err(error))) => {
                    error!("Received error response: {}", error);
                }
                Ok(None) => {
                    info!("Socket closed by server");
                    return Ok(responses);
                }
                Err(error) => {
                    error!(
                        "Timeout while receiving response: {} (in flight: {})",
                        error, in_flight
                    );
                }
            }
        }
    }
}

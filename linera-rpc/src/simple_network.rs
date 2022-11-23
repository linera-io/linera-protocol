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
    transport::{MessageHandler, SpawnedServer, TransportProtocol},
    Message,
};
use async_trait::async_trait;
use futures::{channel::mpsc, sink::SinkExt, stream::StreamExt};
use linera_chain::messages::{BlockProposal, Certificate};
use linera_core::{
    client::ValidatorNodeProvider,
    messages::{ChainInfoQuery, ChainInfoResponse, CrossChainRequest},
    node::{NodeError, ValidatorNode},
    worker::{ValidatorWorker, WorkerState},
};
use linera_storage::Store;
use linera_views::views::ViewError;
use log::{debug, error, info, warn};
use std::{io, str::FromStr, time::Duration};
use tokio::time;

pub trait SharedStore: Store + Clone + Send + Sync + 'static {}
impl<All> SharedStore for All where All: Store + Clone + Send + Sync + 'static {}

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
        mut receiver: mpsc::Receiver<(Message, ShardId)>,
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

    pub async fn spawn(self) -> Result<SpawnedServer, io::Error> {
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
    cross_chain_sender: mpsc::Sender<(Message, ShardId)>,
}

impl<S> MessageHandler for RunningServerState<S>
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    fn handle_message(&mut self, message: Message) -> futures::future::BoxFuture<Option<Message>> {
        Box::pin(async move {
            let reply = match message {
                Message::BlockProposal(message) => {
                    match self.server.state.handle_block_proposal(*message).await {
                        Ok(info) => Ok(Some(info.into())),
                        Err(error) => Err(error.into()),
                    }
                }
                Message::Certificate(message) => {
                    match self.server.state.handle_certificate(*message).await {
                        Ok((info, continuation)) => {
                            // Cross-shard requests
                            self.handle_continuation(continuation).await;
                            // Response
                            Ok(Some(info.into()))
                        }
                        Err(error) => Err(error.into()),
                    }
                }
                Message::ChainInfoQuery(message) => {
                    match self.server.state.handle_chain_info_query(*message).await {
                        Ok(info) => Ok(Some(info.into())),
                        Err(error) => Err(error.into()),
                    }
                }
                Message::CrossChainRequest(request) => {
                    match self.server.state.handle_cross_chain_request(*request).await {
                        Ok(continuation) => {
                            self.handle_continuation(continuation).await;
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
                Message::Vote(_) | Message::Error(_) | Message::ChainInfoResponse(_) => {
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
    fn handle_continuation(
        &mut self,
        requests: Vec<CrossChainRequest>,
    ) -> futures::future::BoxFuture<()> {
        Box::pin(async move {
            for request in requests {
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

/// A client without an address - serves as a client factory.
pub struct NodeProvider {
    pub send_timeout: Duration,
    pub recv_timeout: Duration,
}

#[async_trait]
impl ValidatorNodeProvider for NodeProvider {
    type Node = Client;

    async fn make_node(&self, address: &str) -> Result<Self::Node, NodeError> {
        let network = ValidatorPublicNetworkPreConfig::from_str(address).map_err(|_| {
            NodeError::CannotResolveValidatorAddress {
                address: address.to_string(),
            }
        })?;
        Ok(Client::new(network, self.send_timeout, self.recv_timeout))
    }
}

#[derive(Clone)]
pub struct Client {
    network: ValidatorPublicNetworkPreConfig<TransportProtocol>,
    send_timeout: std::time::Duration,
    recv_timeout: std::time::Duration,
}

impl Client {
    fn new(
        network: ValidatorPublicNetworkPreConfig<TransportProtocol>,
        send_timeout: std::time::Duration,
        recv_timeout: std::time::Duration,
    ) -> Self {
        Self {
            network,
            send_timeout,
            recv_timeout,
        }
    }

    async fn send_recv_internal(&mut self, message: Message) -> Result<Message, codec::Error> {
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

    async fn send_recv_info(&mut self, message: Message) -> Result<ChainInfoResponse, NodeError> {
        match self.send_recv_internal(message).await {
            Ok(Message::ChainInfoResponse(response)) => Ok(*response),
            Ok(Message::Error(error)) => Err(*error),
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
impl ValidatorNode for Client {
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
    async fn send(&self, requests: Vec<Message>) -> Result<Vec<Message>, MassClientError> {
        let address = format!("{}:{}", self.network.host, self.network.port);
        let mut stream = self.network.protocol.connect(address).await?;
        let mut requests = requests.into_iter();
        let mut in_flight: u64 = 0;
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

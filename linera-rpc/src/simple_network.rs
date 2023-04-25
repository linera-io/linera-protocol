// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{io, time::Duration};

use async_trait::async_trait;
use futures::{channel::mpsc, sink::SinkExt, stream::StreamExt};
use tokio::time;
use tracing::{debug, error, info, instrument, warn};

use linera_base::identifiers::ChainId;
use linera_chain::data_types::{BlockProposal, Certificate, HashedValue, LiteCertificate};
use linera_core::{
    data_types::{ChainInfoQuery, ChainInfoResponse},
    node::{NodeError, NotificationStream, ValidatorNode},
    worker::{NetworkActions, ValidatorWorker, WorkerError, WorkerState},
};
use linera_storage::Store;
use linera_views::views::ViewError;
use tokio::sync::oneshot;

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
        cross_chain_max_retries: u32,
        cross_chain_retry_delay: Duration,
        cross_chain_sender_delay: Duration,
        this_shard: ShardId,
        mut receiver: mpsc::Receiver<(RpcMessage, ShardId)>,
    ) {
        let mut pool = network
            .protocol
            .make_outgoing_connection_pool()
            .await
            .expect("Initialization should not fail");

        while let Some((message, shard_id)) = receiver.next().await {
            let shard = network.shard(shard_id);
            let remote_address = format!("{}:{}", shard.host, shard.port);

            // Send the cross-chain query and retry if needed.
            for i in 0..cross_chain_max_retries {
                // Delay increases linearly with the attempt number.
                tokio::time::sleep(cross_chain_sender_delay + cross_chain_retry_delay * i).await;

                let status = pool.send_message_to(message.clone(), &remote_address).await;
                match status {
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
            Duration::from_millis(self.cross_chain_config.sender_delay_ms),
            self.shard_id,
            cross_chain_receiver,
        ));

        let protocol = self.network.protocol;
        let state = RunningServerState {
            server: self,
            cross_chain_sender,
            // Udp servers cannot process several requests (e.g. user requests and
            // cross-chain requests) at the same time.
            wait_for_outgoing_messages: protocol == TransportProtocol::Tcp,
        };
        // Launch server for the appropriate protocol.
        protocol.spawn_server(&address, state).await
    }
}

#[derive(Clone)]
struct RunningServerState<S> {
    server: Server<S>,
    cross_chain_sender: mpsc::Sender<(RpcMessage, ShardId)>,
    wait_for_outgoing_messages: bool,
}

#[async_trait]
impl<S> MessageHandler for RunningServerState<S>
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    #[instrument(target = "simple_server", skip_all, fields(nickname = self.server.state.nickname(), chain_id = ?message.target_chain_id()))]
    async fn handle_message(&mut self, message: RpcMessage) -> Option<RpcMessage> {
        let reply = match message {
            RpcMessage::BlockProposal(message) => {
                match self.server.state.handle_block_proposal(*message).await {
                    Ok((info, actions)) => {
                        // Cross-shard requests
                        self.handle_network_actions(actions);
                        // Response
                        Ok(Some(info.into()))
                    }
                    Err(error) => {
                        warn!(nickname = self.server.state.nickname(), %error, "Failed to handle block proposal");
                        Err(error.into())
                    }
                }
            }
            RpcMessage::LiteCertificate(message) => {
                let (sender, receiver) = self
                    .wait_for_outgoing_messages
                    .then(oneshot::channel)
                    .unzip();
                match self
                    .server
                    .state
                    .handle_lite_certificate(*message, sender)
                    .await
                {
                    Ok((info, actions)) => {
                        // Cross-shard requests
                        self.handle_network_actions(actions);
                        if let Some(receiver) = receiver {
                            if let Err(e) = receiver.await {
                                error!("Failed to wait for message delivery: {e}");
                            }
                        }
                        // Response
                        Ok(Some(info.into()))
                    }
                    Err(error) => {
                        if let WorkerError::MissingCertificateValue = &error {
                            debug!(nickname = self.server.state.nickname(), %error, "Failed to handle lite certificate");
                        } else {
                            error!(nickname = self.server.state.nickname(), %error, "Failed to handle lite certificate");
                        }
                        Err(error.into())
                    }
                }
            }
            RpcMessage::Certificate(message, blobs) => {
                let (sender, receiver) = self
                    .wait_for_outgoing_messages
                    .then(oneshot::channel)
                    .unzip();
                match self
                    .server
                    .state
                    .handle_certificate(*message, blobs, sender)
                    .await
                {
                    Ok((info, actions)) => {
                        // Cross-shard requests
                        self.handle_network_actions(actions);
                        if let Some(receiver) = receiver {
                            if let Err(e) = receiver.await {
                                error!("Failed to wait for message delivery: {e}");
                            }
                        }
                        // Response
                        Ok(Some(info.into()))
                    }
                    Err(error) => {
                        error!(nickname = self.server.state.nickname(), %error, "Failed to handle certificate");
                        Err(error.into())
                    }
                }
            }
            RpcMessage::ChainInfoQuery(message) => {
                match self.server.state.handle_chain_info_query(*message).await {
                    Ok((info, actions)) => {
                        // Cross-shard requests
                        self.handle_network_actions(actions);
                        // Response
                        Ok(Some(info.into()))
                    }
                    Err(error) => {
                        error!(nickname = self.server.state.nickname(), %error, "Failed to handle chain info query");
                        Err(error.into())
                    }
                }
            }
            RpcMessage::CrossChainRequest(request) => {
                match self.server.state.handle_cross_chain_request(*request).await {
                    Ok(actions) => {
                        self.handle_network_actions(actions);
                    }
                    Err(error) => {
                        error!(nickname = self.server.state.nickname(), %error, "Failed to handle cross-chain request");
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
    }
}

impl<S> RunningServerState<S>
where
    S: Send,
{
    fn handle_network_actions(&mut self, actions: NetworkActions) {
        for request in actions.cross_chain_requests {
            let shard_id = self.server.network.get_shard_id(request.target_chain_id());
            debug!(
                "[{}] Scheduling cross-chain query: {} -> {}",
                self.server.state.nickname(),
                self.server.shard_id,
                shard_id
            );
            if let Err(error) = self.cross_chain_sender.try_send((request.into(), shard_id)) {
                error!(%error, "dropping cross-chain request");
                break;
            }
        }
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
    /// Initiates a new block.
    async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.send_recv_info(proposal.into()).await
    }

    /// Processes a hash certificate.
    async fn handle_lite_certificate(
        &mut self,
        certificate: LiteCertificate,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.send_recv_info(certificate.into()).await
    }

    /// Processes a certificate.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
        blobs: Vec<HashedValue>,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.send_recv_info((certificate, blobs).into()).await
    }

    /// Handles information queries for this chain.
    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.send_recv_info(query.into()).await
    }

    async fn subscribe(&mut self, _chains: Vec<ChainId>) -> Result<NotificationStream, NodeError> {
        Err(NodeError::SubscriptionError {
            transport: self.network.protocol.to_string(),
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
    async fn send(
        &mut self,
        requests: Vec<RpcMessage>,
    ) -> Result<Vec<RpcMessage>, MassClientError> {
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

// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    codec,
    transport::{MessageHandler, NetworkProtocol, SpawnedServer},
};
use async_trait::async_trait;
use futures::{channel::mpsc, sink::SinkExt, stream::StreamExt};
use linera_base::{error::Error, messages::ChainId};
use linera_chain::messages::{BlockProposal, Certificate};
use linera_core::{
    messages::{ChainInfoQuery, ChainInfoResponse, CrossChainRequest},
    node::ValidatorNode,
    worker::{ValidatorWorker, WorkerState},
};
use linera_rpc::Message;
use linera_storage::Store;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::{io, time::Duration};
use structopt::StructOpt;
use tokio::time;

#[derive(Clone, Debug, StructOpt)]
pub struct CrossChainConfig {
    /// Number of cross-chains messages allowed before blocking the main server loop
    #[structopt(long = "cross_chain_queue_size", default_value = "1")]
    queue_size: usize,
    /// Maximum number of retries for a cross-chain message.
    #[structopt(long = "cross_chain_max_retries", default_value = "10")]
    max_retries: usize,
    /// Delay before retrying of cross-chain message.
    #[structopt(long = "cross_chain_retry_delay_ms", default_value = "2000")]
    retry_delay_ms: u64,
}

pub type ShardId = usize;

/// The network configuration of a shard.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardConfig {
    /// The host name (e.g an IP address).
    pub host: String,
    /// The port.
    pub port: u16,
}

/// The network configuration for all shards.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatorInternalNetworkConfig {
    /// The network protocol to use for all shards.
    pub protocol: NetworkProtocol,
    /// The available shards. Each chain UID is mapped to a unique shard in the vector in
    /// a static way.
    pub shards: Vec<ShardConfig>,
}

/// The public network configuration for a validator.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatorPublicNetworkConfig {
    /// The host name of the validator (IP or hostname).
    pub host: String,
    /// The port the validator listens on.
    pub port: u16,
}

impl std::fmt::Display for ValidatorPublicNetworkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl std::str::FromStr for ValidatorPublicNetworkConfig {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        anyhow::ensure!(parts.len() == 2, "Expecting format `host:port`");
        let host = parts[0].to_owned();
        let port = parts[1].parse()?;
        Ok(ValidatorPublicNetworkConfig { host, port })
    }
}

impl ValidatorInternalNetworkConfig {
    /// Static shard assignment
    pub fn get_shard_id(&self, chain_id: ChainId) -> ShardId {
        use std::hash::{Hash, Hasher};
        let mut s = std::collections::hash_map::DefaultHasher::new();
        chain_id.hash(&mut s);
        (s.finish() as ShardId) % self.shards.len()
    }

    pub fn shard(&self, shard_id: ShardId) -> &ShardConfig {
        &self.shards[shard_id]
    }

    /// Get the [`ShardConfig`] of the shard assigned to the `chain_id`.
    pub fn get_shard_for(&self, chain_id: ChainId) -> &ShardConfig {
        self.shard(self.get_shard_id(chain_id))
    }
}

#[derive(Clone)]
pub struct Server<S> {
    network: ValidatorInternalNetworkConfig,
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
        network: ValidatorInternalNetworkConfig,
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
    Error: From<S::Error>,
{
    async fn forward_cross_chain_queries(
        network: ValidatorInternalNetworkConfig,
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
                                "Failed to send cross-chain query ({}-th retry): {}",
                                i, error
                            );
                            tokio::time::sleep(cross_chain_retry_delay).await;
                        } else {
                            error!(
                                "Failed to send cross-chain query (giving up after {} retries): {}",
                                i, error
                            );
                        }
                    }
                    _ => {
                        debug!("Sent cross-chain query: {} -> {}", this_shard, shard_id);
                        queries_sent += 1;
                        break;
                    }
                }
            }
            if queries_sent % 2000 == 0 {
                debug!(
                    "{} has sent {} cross-chain queries to {}:{} (shard {})",
                    this_shard, queries_sent, shard.host, shard.port, shard_id,
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
    Error: From<S::Error>,
{
    fn handle_message(&mut self, message: Message) -> futures::future::BoxFuture<Option<Message>> {
        Box::pin(async move {
            let reply = match message {
                Message::BlockProposal(message) => self
                    .server
                    .state
                    .handle_block_proposal(*message)
                    .await
                    .map(|info| Some(info.into())),
                Message::Certificate(message) => {
                    match self.server.state.handle_certificate(*message).await {
                        Ok((info, continuation)) => {
                            // Cross-shard requests
                            self.handle_continuation(continuation).await;
                            // Response
                            Ok(Some(info.into()))
                        }
                        Err(error) => Err(error),
                    }
                }
                Message::ChainInfoQuery(message) => self
                    .server
                    .state
                    .handle_chain_info_query(*message)
                    .await
                    .map(|info| Some(info.into())),
                Message::CrossChainRequest(request) => {
                    match self.server.state.handle_cross_chain_request(*request).await {
                        Ok(continuation) => {
                            self.handle_continuation(continuation).await;
                        }
                        Err(error) => {
                            error!("Failed to handle cross-chain request: {}", error);
                        }
                    }
                    // No user to respond to.
                    Ok(None)
                }
                Message::Vote(_) | Message::Error(_) | Message::ChainInfoResponse(_) => {
                    Err(Error::UnexpectedMessage)
                }
            };

            self.server.packets_processed += 1;
            if self.server.packets_processed % 5000 == 0 {
                debug!(
                    "{}:{} (shard {}) has processed {} packets",
                    self.server.host,
                    self.server.port,
                    self.server.shard_id,
                    self.server.packets_processed
                );
            }

            match reply {
                Ok(x) => x,
                Err(error) => {
                    warn!("User query failed: {}", error);
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
                    "Scheduling cross-chain query: {} -> {}",
                    self.server.shard_id, shard_id
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
pub struct Client {
    network: ValidatorPublicNetworkConfig,
    send_timeout: std::time::Duration,
    recv_timeout: std::time::Duration,
}

impl Client {
    pub fn new(
        network: ValidatorPublicNetworkConfig,
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
        let mut stream = NetworkProtocol::Tcp.connect(address).await?;
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

    pub async fn send_recv_info(&mut self, message: Message) -> Result<ChainInfoResponse, Error> {
        match self.send_recv_internal(message).await {
            Ok(Message::ChainInfoResponse(response)) => Ok(*response),
            Ok(Message::Error(error)) => Err(*error),
            Ok(_) => Err(Error::UnexpectedMessage),
            Err(error) => match error {
                codec::Error::Io(io_error) => Err(Error::ClientIoError {
                    error: format!("{}", io_error),
                }),
                _ => Err(Error::InvalidDecoding),
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
    ) -> Result<ChainInfoResponse, Error> {
        self.send_recv_info(proposal.into()).await
    }

    /// Process a certificate.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<ChainInfoResponse, Error> {
        self.send_recv_info(certificate.into()).await
    }

    /// Handle information queries for this chain.
    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, Error> {
        self.send_recv_info(query.into()).await
    }
}

#[derive(Clone)]
pub struct MassClient {
    pub network: ValidatorPublicNetworkConfig,
    send_timeout: std::time::Duration,
    recv_timeout: std::time::Duration,
    max_in_flight: u64,
}

impl MassClient {
    pub fn new(
        network: ValidatorPublicNetworkConfig,
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

    pub async fn send(&self, requests: Vec<Message>) -> Result<Vec<Message>, io::Error> {
        let address = format!("{}:{}", self.network.host, self.network.port);
        let mut stream = NetworkProtocol::Tcp.connect(address).await?;
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

// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{codec, transport::*};
use async_trait::async_trait;
use zef_base::{base_types::*, error::*, messages::*, serialize::*};
use zef_core::{node::ValidatorNode, worker::*};

#[cfg(feature = "benchmark")]
use crate::network_server::BenchmarkServer;
use futures::{channel::mpsc, future::FutureExt, sink::SinkExt, stream::StreamExt};
use log::*;
use std::{io, time::Duration};
use structopt::StructOpt;
use tokio::time;

/// Static shard assignment
pub fn get_shard(num_shards: u32, chain_id: ChainId) -> u32 {
    use std::hash::{Hash, Hasher};
    let mut s = std::collections::hash_map::DefaultHasher::new();
    chain_id.hash(&mut s);
    (s.finish() % num_shards as u64) as u32
}

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

pub type ShardId = u32;

pub struct Server<Storage> {
    network_protocol: NetworkProtocol,
    base_address: String,
    base_port: u32,
    state: WorkerState<Storage>,
    shard_id: ShardId,
    num_shards: u32,
    cross_chain_config: CrossChainConfig,
    // Stats
    packets_processed: u64,
    user_errors: u64,
}

impl<Storage> Server<Storage> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        network_protocol: NetworkProtocol,
        base_address: String,
        base_port: u32,
        state: WorkerState<Storage>,
        shard_id: ShardId,
        num_shards: u32,
        cross_chain_config: CrossChainConfig,
    ) -> Self {
        Self {
            network_protocol,
            base_address,
            base_port,
            state,
            shard_id,
            num_shards,
            cross_chain_config,
            packets_processed: 0,
            user_errors: 0,
        }
    }

    pub(crate) fn which_shard(&self, chain_id: ChainId) -> ShardId {
        get_shard(self.num_shards, chain_id)
    }

    pub fn packets_processed(&self) -> u64 {
        self.packets_processed
    }

    pub fn user_errors(&self) -> u64 {
        self.user_errors
    }
}

impl<Storage> Server<Storage>
where
    Storage: zef_storage::Storage + Clone + 'static,
{
    async fn forward_cross_chain_queries(
        network_protocol: NetworkProtocol,
        base_address: String,
        base_port: u32,
        cross_chain_max_retries: usize,
        cross_chain_retry_delay: Duration,
        this_shard: ShardId,
        mut receiver: mpsc::Receiver<(SerializedMessage, ShardId)>,
    ) {
        let mut pool = network_protocol
            .make_outgoing_connection_pool()
            .await
            .expect("Initialization should not fail");

        let mut queries_sent = 0u64;
        while let Some((message, shard)) = receiver.next().await {
            // Send cross-chain query.
            let remote_address = format!("{}:{}", base_address, base_port + shard);
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
                        debug!("Sent cross-chain query: {} -> {}", this_shard, shard);
                        queries_sent += 1;
                        break;
                    }
                }
            }
            if queries_sent % 2000 == 0 {
                debug!(
                    "{}:{} (shard {}) has sent {} cross-chain queries",
                    base_address,
                    base_port + this_shard,
                    this_shard,
                    queries_sent
                );
            }
        }
    }

    pub async fn spawn(self) -> Result<SpawnedServer, io::Error> {
        info!(
            "Listening to {} traffic on {}:{}",
            self.network_protocol,
            self.base_address,
            self.base_port + self.shard_id
        );
        let address = format!("{}:{}", self.base_address, self.base_port + self.shard_id);

        let (cross_chain_sender, cross_chain_receiver) =
            mpsc::channel(self.cross_chain_config.queue_size);
        tokio::spawn(Self::forward_cross_chain_queries(
            self.network_protocol,
            self.base_address.clone(),
            self.base_port,
            self.cross_chain_config.max_retries,
            Duration::from_millis(self.cross_chain_config.retry_delay_ms),
            self.shard_id,
            cross_chain_receiver,
        ));

        let protocol = self.network_protocol;
        let state = RunningServerState {
            server: self,
            cross_chain_sender,
        };
        // Launch server for the appropriate protocol.
        #[cfg(feature = "benchmark")]
        {
            let _protocol = protocol;
            BenchmarkServer::spawn(address, state)
        }

        #[cfg(not(feature = "benchmark"))]
        {
            protocol.spawn_server(&address, state).await
        }
    }
}

struct RunningServerState<Storage> {
    server: Server<Storage>,
    cross_chain_sender: mpsc::Sender<(SerializedMessage, ShardId)>,
}

impl<Storage> MessageHandler for RunningServerState<Storage>
where
    Storage: zef_storage::Storage + Clone + 'static,
{
    fn handle_message(
        &mut self,
        message: SerializedMessage,
    ) -> futures::future::BoxFuture<Option<SerializedMessage>> {
        Box::pin(async move {
            let reply = match message {
                SerializedMessage::BlockProposal(message) => self
                    .server
                    .state
                    .handle_block_proposal(*message)
                    .await
                    .map(|info| Some(info.into())),
                SerializedMessage::Certificate(message) => {
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
                SerializedMessage::ChainInfoQuery(message) => self
                    .server
                    .state
                    .handle_chain_info_query(*message)
                    .await
                    .map(|info| Some(info.into())),
                SerializedMessage::CrossChainRequest(request) => {
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
                SerializedMessage::Vote(_)
                | SerializedMessage::Error(_)
                | SerializedMessage::ChainInfoResponse(_) => Err(Error::UnexpectedMessage),
            };

            self.server.packets_processed += 1;
            if self.server.packets_processed % 5000 == 0 {
                debug!(
                    "{}:{} (shard {}) has processed {} packets",
                    self.server.base_address,
                    self.server.base_port + self.server.shard_id,
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

impl<Storage> RunningServerState<Storage>
where
    Storage: Send,
{
    fn handle_continuation(
        &mut self,
        requests: Vec<CrossChainRequest>,
    ) -> futures::future::BoxFuture<()> {
        Box::pin(async move {
            for request in requests {
                let shard_id = self.server.which_shard(request.target_chain_id());
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
    network_protocol: NetworkProtocol,
    base_address: String,
    base_port: u32,
    num_shards: u32,
    send_timeout: std::time::Duration,
    recv_timeout: std::time::Duration,
}

impl Client {
    pub fn new(
        network_protocol: NetworkProtocol,
        base_address: String,
        base_port: u32,
        num_shards: u32,
        send_timeout: std::time::Duration,
        recv_timeout: std::time::Duration,
    ) -> Self {
        Self {
            network_protocol,
            base_address,
            base_port,
            num_shards,
            send_timeout,
            recv_timeout,
        }
    }

    async fn send_recv_internal(
        &mut self,
        shard: ShardId,
        message: SerializedMessage,
    ) -> Result<SerializedMessage, codec::Error> {
        let address = format!("{}:{}", self.base_address, self.base_port + shard);
        let mut stream = self.network_protocol.connect(address).await?;
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

    pub async fn send_recv_info(
        &mut self,
        shard: ShardId,
        message: SerializedMessage,
    ) -> Result<ChainInfoResponse, Error> {
        match self.send_recv_internal(shard, message).await {
            Ok(SerializedMessage::ChainInfoResponse(response)) => Ok(*response),
            Ok(SerializedMessage::Error(error)) => Err(*error),
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
        let shard = get_shard(self.num_shards, proposal.content.block.chain_id);
        self.send_recv_info(shard, proposal.into()).await
    }

    /// Process a certificate.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<ChainInfoResponse, Error> {
        let shard = get_shard(self.num_shards, certificate.value.chain_id());
        self.send_recv_info(shard, certificate.into()).await
    }

    /// Handle information queries for this chain.
    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, Error> {
        let shard = get_shard(self.num_shards, query.chain_id);
        self.send_recv_info(shard, query.into()).await
    }
}

#[derive(Clone)]
pub struct MassClient {
    network_protocol: NetworkProtocol,
    base_address: String,
    base_port: u32,
    send_timeout: std::time::Duration,
    recv_timeout: std::time::Duration,
    max_in_flight: u64,
}

impl MassClient {
    pub fn new(
        network_protocol: NetworkProtocol,
        base_address: String,
        base_port: u32,
        send_timeout: std::time::Duration,
        recv_timeout: std::time::Duration,
        max_in_flight: u64,
    ) -> Self {
        Self {
            network_protocol,
            base_address,
            base_port,
            send_timeout,
            recv_timeout,
            max_in_flight,
        }
    }

    async fn run_shard(
        &self,
        shard: u32,
        requests: Vec<SerializedMessage>,
    ) -> Result<Vec<SerializedMessage>, io::Error> {
        let address = format!("{}:{}", self.base_address, self.base_port + shard);
        let mut stream = self.network_protocol.connect(address).await?;
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

    /// Spin off one task for each shard based on this validator client.
    pub fn run<I>(
        &self,
        sharded_requests: I,
    ) -> impl futures::stream::Stream<Item = Vec<SerializedMessage>>
    where
        I: IntoIterator<Item = (ShardId, Vec<SerializedMessage>)>,
    {
        let handles = futures::stream::FuturesUnordered::new();
        for (shard, requests) in sharded_requests {
            let client = self.clone();
            handles.push(
                tokio::spawn(async move {
                    info!(
                        "Sending {} requests to {}:{} (shard {})",
                        client.network_protocol,
                        client.base_address,
                        client.base_port + shard,
                        shard
                    );
                    let responses = client
                        .run_shard(shard, requests)
                        .await
                        .unwrap_or_else(|_| Vec::new());
                    info!(
                        "Done sending {} requests to {}:{} (shard {})",
                        client.network_protocol,
                        client.base_address,
                        client.base_port + shard,
                        shard
                    );
                    responses
                })
                .then(|x| async { x.unwrap_or_else(|_| Vec::new()) }),
            );
        }
        handles
    }
}

#[test]
fn test_get_shards() {
    let num_shards = 16u32;
    let mut found = vec![false; num_shards as usize];
    let mut left = num_shards;
    let mut i: usize = 1;
    loop {
        let shard = get_shard(num_shards, ChainId::root(i)) as usize;
        println!("found {}", shard);
        if !found[shard] {
            found[shard] = true;
            left -= 1;
            if left == 0 {
                break;
            }
        }
        i += 1;
    }
}

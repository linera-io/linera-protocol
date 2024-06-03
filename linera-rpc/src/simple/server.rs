// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use async_trait::async_trait;
use futures::{channel::mpsc, stream::StreamExt};
use linera_core::{
    node::NodeError,
    worker::{NetworkActions, ValidatorWorker, WorkerError, WorkerState},
    JoinSetExt as _,
};
use linera_storage::Storage;
use linera_views::views::ViewError;
use rand::Rng;
use tokio::{sync::oneshot, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

use super::transport::{MessageHandler, ServerHandle, TransportProtocol};
use crate::{
    config::{CrossChainConfig, ShardId, ValidatorInternalNetworkPreConfig},
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
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    #[allow(clippy::too_many_arguments)]
    async fn forward_cross_chain_queries(
        nickname: String,
        network: ValidatorInternalNetworkPreConfig<TransportProtocol>,
        cross_chain_max_retries: u32,
        cross_chain_retry_delay: Duration,
        cross_chain_sender_delay: Duration,
        cross_chain_sender_failure_rate: f32,
        this_shard: ShardId,
        mut receiver: mpsc::Receiver<(RpcMessage, ShardId)>,
    ) {
        let mut pool = network
            .protocol
            .make_outgoing_connection_pool()
            .await
            .expect("Initialization should not fail");

        while let Some((message, shard_id)) = receiver.next().await {
            if cross_chain_sender_failure_rate > 0.0
                && rand::thread_rng().gen::<f32>() < cross_chain_sender_failure_rate
            {
                warn!("Dropped 1 cross-message intentionally.");
                continue;
            }

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

    pub fn spawn(
        self,
        shutdown_signal: CancellationToken,
        join_set: &mut JoinSet<()>,
    ) -> ServerHandle {
        info!(
            "Listening to {:?} traffic on {}:{}",
            self.network.protocol, self.host, self.port
        );
        let address = (self.host.clone(), self.port);

        let (cross_chain_sender, cross_chain_receiver) =
            mpsc::channel(self.cross_chain_config.queue_size);

        join_set.spawn_task(Self::forward_cross_chain_queries(
            self.state.nickname().to_string(),
            self.network.clone(),
            self.cross_chain_config.max_retries,
            Duration::from_millis(self.cross_chain_config.retry_delay_ms),
            Duration::from_millis(self.cross_chain_config.sender_delay_ms),
            self.cross_chain_config.sender_failure_rate,
            self.shard_id,
            cross_chain_receiver,
        ));

        let protocol = self.network.protocol;
        let state = RunningServerState {
            server: self,
            cross_chain_sender,
        };
        // Launch server for the appropriate protocol.
        protocol.spawn_server(address, state, shutdown_signal, join_set)
    }
}

#[derive(Clone)]
struct RunningServerState<S> {
    server: Server<S>,
    cross_chain_sender: mpsc::Sender<(RpcMessage, ShardId)>,
}

#[async_trait]
impl<S> MessageHandler for RunningServerState<S>
where
    S: Storage + Clone + Send + Sync + 'static,
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
            RpcMessage::LiteCertificate(request) => {
                let (sender, receiver) = request
                    .wait_for_outgoing_messages
                    .then(oneshot::channel)
                    .unzip();
                match self
                    .server
                    .state
                    .handle_lite_certificate(request.certificate, sender)
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
            RpcMessage::Certificate(request) => {
                let (sender, receiver) = request
                    .wait_for_outgoing_messages
                    .then(oneshot::channel)
                    .unzip();
                match self
                    .server
                    .state
                    .handle_certificate(
                        request.certificate,
                        request.hashed_certificate_values,
                        request.hashed_blobs,
                        sender,
                    )
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

            RpcMessage::VersionInfoQuery => Ok(Some(linera_version::VersionInfo::default().into())),

            RpcMessage::Vote(_)
            | RpcMessage::Error(_)
            | RpcMessage::ChainInfoResponse(_)
            | RpcMessage::VersionInfoResponse(_)
            | RpcMessage::DownloadBlob(_)
            | RpcMessage::DownloadBlobResponse(_) => Err(NodeError::UnexpectedMessage),
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
                // TODO(#459): Make it a warning or an error again.
                debug!(
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

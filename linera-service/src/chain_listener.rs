// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{config::WalletState, node_service::ChainClients};
use async_trait::async_trait;
use futures::{lock::Mutex, StreamExt};
use linera_base::{
    crypto::KeyPair,
    data_types::Timestamp,
    identifiers::{ChainId, Destination},
};
use linera_chain::data_types::OutgoingMessage;
use linera_core::{
    client::ChainClient,
    node::ValidatorNodeProvider,
    worker::{Notification, Reason},
};
use linera_execution::{Message, SystemMessage};
use linera_storage::Storage;
use linera_views::view::ViewError;
use std::{collections::btree_map, sync::Arc, time::Duration};
use structopt::StructOpt;
use tracing::{error, info, warn};

#[derive(Debug, Clone, StructOpt)]
pub struct ChainListenerConfig {
    /// Wait before processing any notification (useful for testing).
    #[structopt(long = "listener-delay-before-ms", default_value = "0")]
    pub delay_before_ms: u64,

    /// Wait after processing any notification (useful for rate limiting).
    #[structopt(long = "listener-delay-after-ms", default_value = "0")]
    pub delay_after_ms: u64,
}

#[async_trait]
pub trait ClientContext<P: ValidatorNodeProvider> {
    fn wallet_state(&self) -> &WalletState;

    fn make_chain_client<S>(
        &self,
        storage: S,
        chain_id: impl Into<Option<ChainId>>,
    ) -> ChainClient<P, S>;

    fn update_wallet_for_new_chain(
        &mut self,
        chain_id: ChainId,
        key_pair: Option<KeyPair>,
        timestamp: Timestamp,
    );

    async fn update_wallet<'a, S>(&'a mut self, client: &'a mut ChainClient<P, S>)
    where
        S: Storage + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>;
}

/// A `ChainListener` is a process that listens to notifications from validators and reacts
/// appropriately.
pub struct ChainListener<P, S> {
    config: ChainListenerConfig,
    clients: ChainClients<P, S>,
}

impl<P, S> ChainListener<P, S>
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    /// Creates a new chain listener given client chains.
    pub(crate) fn new(config: ChainListenerConfig, clients: ChainClients<P, S>) -> Self {
        Self { config, clients }
    }

    /// Runs the chain listener.
    pub async fn run<C>(self, context: Arc<Mutex<C>>, storage: S)
    where
        C: ClientContext<P> + Send + 'static,
    {
        let chain_ids = context.lock().await.wallet_state().chain_ids();
        for chain_id in chain_ids {
            Self::run_with_chain_id(
                chain_id,
                self.clients.clone(),
                context.clone(),
                storage.clone(),
                self.config.clone(),
            );
        }
    }

    fn run_with_chain_id<C>(
        chain_id: ChainId,
        clients: ChainClients<P, S>,
        context: Arc<Mutex<C>>,
        storage: S,
        config: ChainListenerConfig,
    ) where
        C: ClientContext<P> + Send + 'static,
    {
        let _handle = tokio::task::spawn(async move {
            if let Err(err) =
                Self::run_client_stream(chain_id, clients, context, storage, config).await
            {
                error!("Stream for chain {} failed: {}", chain_id, err);
            }
        });
    }

    async fn run_client_stream<C>(
        chain_id: ChainId,
        clients: ChainClients<P, S>,
        context: Arc<Mutex<C>>,
        storage: S,
        config: ChainListenerConfig,
    ) -> Result<(), anyhow::Error>
    where
        C: ClientContext<P> + Send + 'static,
    {
        let client = {
            let mut map_guard = clients.map_lock().await;
            let context_guard = context.lock().await;
            let btree_map::Entry::Vacant(entry) = map_guard.entry(chain_id) else {
                // For every entry in the client map we are already listening to notifications, so
                // there's nothing to do. This can happen if we download a child before the parent
                // chain, and then process the OpenChain message in the parent.
                return Ok(());
            };
            let client = context_guard.make_chain_client(storage.clone(), chain_id);
            let client = Arc::new(Mutex::new(client));
            entry.insert(client.clone());
            client
        };
        ChainClient::listen(client.clone()).await?;
        let mut local_stream = {
            let mut guard = client.lock().await;
            let stream = guard.subscribe().await?;
            // Process the inbox: For messages that are already there we won't receive a
            // notification.
            guard.synchronize_from_validators().await?;
            if let Err(error) = guard.process_inbox_if_owned().await {
                warn!(%error, "Failed to process inbox after starting stream.");
            }
            stream
        };
        while let Some(notification) = local_stream.next().await {
            info!("Received new notification: {:?}", notification);
            if config.delay_before_ms > 0 {
                tokio::time::sleep(Duration::from_millis(config.delay_before_ms)).await;
            }
            {
                let mut client = client.lock().await;
                Self::handle_notification(&mut *client, notification.clone()).await;
            }
            if config.delay_after_ms > 0 {
                tokio::time::sleep(Duration::from_millis(config.delay_after_ms)).await;
            }
            if let Reason::NewBlock { hash, .. } = notification.reason {
                let value = storage.read_value(hash).await?;
                let Some(executed_block) = value.inner().executed_block() else {
                    continue;
                };
                let timestamp = executed_block.block.timestamp;
                for outgoing_message in &executed_block.messages {
                    if let OutgoingMessage {
                        destination: Destination::Recipient(new_id),
                        message: Message::System(SystemMessage::OpenChain { ownership, .. }),
                        ..
                    } = outgoing_message
                    {
                        {
                            let mut context_guard = context.lock().await;
                            let key_pair = ownership
                                .owners
                                .values()
                                .map(|(public_key, _)| public_key)
                                .chain(ownership.super_owners.values())
                                .find_map(|public_key| {
                                    context_guard.wallet_state().key_pair_for_pk(public_key)
                                });
                            context_guard.update_wallet_for_new_chain(*new_id, key_pair, timestamp);
                        }
                        Self::run_with_chain_id(
                            *new_id,
                            clients.clone(),
                            context.clone(),
                            storage.clone(),
                            config.clone(),
                        );
                    }
                }
            }
            let mut client_guard = client.lock().await;
            context.lock().await.update_wallet(&mut *client_guard).await;
        }
        Ok(())
    }

    async fn handle_notification(client: &mut ChainClient<P, S>, notification: Notification) {
        match &notification.reason {
            Reason::NewBlock { .. } => {
                if let Err(e) = client.update_validators().await {
                    warn!(
                        "Failed to update validators about the local chain after \
                        receiving notification {:?} with error: {:?}",
                        notification, e
                    );
                }
            }
            Reason::NewIncomingMessage { .. } => {
                if let Err(e) = client.process_inbox_if_owned().await {
                    warn!(
                        "Failed to process inbox after receiving new message: {:?} \
                        with error: {:?}",
                        notification, e
                    );
                }
            }
            Reason::NewRound { .. } => {
                if let Err(e) = client.update_validators().await {
                    warn!(
                        "Failed to update validators about the local chain after \
                        receiving notification {:?} with error: {:?}",
                        notification, e
                    );
                }
            }
        }
    }
}

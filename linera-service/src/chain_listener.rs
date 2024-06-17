// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::btree_map, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::{
    future::{self, Either},
    lock::Mutex,
    StreamExt,
};
use linera_base::{
    crypto::KeyPair,
    data_types::Timestamp,
    identifiers::{ChainId, Destination},
};
use linera_chain::data_types::OutgoingMessage;
use linera_core::{
    client::{ArcChainClient, ChainClient},
    node::{LocalValidatorNodeProvider, ValidatorNode, ValidatorNodeProvider},
    worker::Reason,
};
use linera_execution::{Message, SystemMessage};
use linera_storage::Storage;
use linera_views::views::ViewError;
use tracing::{error, info, warn};

use crate::{node_service::ChainClients, wallet::Wallet};

#[cfg(test)]
#[path = "unit_tests/chain_listener.rs"]
mod tests;

#[derive(Debug, Default, Clone, clap::Args)]
pub struct ChainListenerConfig {
    /// Wait before processing any notification (useful for testing).
    #[arg(long = "listener-delay-before-ms", default_value = "0")]
    pub delay_before_ms: u64,

    /// Wait after processing any notification (useful for rate limiting).
    #[arg(long = "listener-delay-after-ms", default_value = "0")]
    pub delay_after_ms: u64,
}

#[async_trait]
pub trait ClientContext {
    type ValidatorNodeProvider: LocalValidatorNodeProvider;
    type Storage: Storage;

    fn wallet(&self) -> &Wallet;

    fn make_chain_client(
        &self,
        chain_id: ChainId,
    ) -> ChainClient<Self::ValidatorNodeProvider, Self::Storage>;

    fn update_wallet_for_new_chain(
        &mut self,
        chain_id: ChainId,
        key_pair: Option<KeyPair>,
        timestamp: Timestamp,
    );

    async fn update_wallet<'a>(
        &'a mut self,
        client: &'a mut ChainClient<Self::ValidatorNodeProvider, Self::Storage>,
    );
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
    <<P as ValidatorNodeProvider>::Node as ValidatorNode>::NotificationStream: Send,
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
        C: ClientContext<ValidatorNodeProvider = P, Storage = S> + Send + 'static,
    {
        let chain_ids = context.lock().await.wallet().chain_ids();
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
        C: ClientContext<ValidatorNodeProvider = P, Storage = S> + Send + 'static,
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
    ) -> anyhow::Result<()>
    where
        C: ClientContext<ValidatorNodeProvider = P, Storage = S> + Send + 'static,
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
            let client = context_guard.make_chain_client(chain_id);
            let client = ArcChainClient::new(client);
            entry.insert(client.clone());
            client
        };
        let (listener, _listen_handle, mut local_stream) = client.listen().await?;
        tokio::spawn(listener);
        let mut timeout = storage.clock().current_time();
        loop {
            let sleep = Box::pin(storage.clock().sleep_until(timeout));
            let notification = match future::select(local_stream.next(), sleep).await {
                Either::Left((Some(notification), _)) => notification,
                Either::Left((None, _)) => break,
                Either::Right(((), _)) => {
                    match client.lock().await.process_inbox_if_owned().await {
                        Err(error) => {
                            warn!(%error, "Failed to process inbox.");
                            timeout = Timestamp::from(u64::MAX);
                        }
                        Ok((_, None)) => timeout = Timestamp::from(u64::MAX),
                        Ok((_, Some(new_timeout))) => timeout = new_timeout.timestamp,
                    }
                    continue;
                }
            };
            info!("Received new notification: {:?}", notification);
            Self::maybe_sleep(config.delay_before_ms).await;
            match &notification.reason {
                Reason::NewIncomingMessage { .. } => timeout = storage.clock().current_time(),
                Reason::NewBlock { .. } | Reason::NewRound { .. } => {
                    if let Err(error) = client.lock().await.update_validators().await {
                        warn!(
                            "Failed to update validators about the local chain after \
                            receiving notification {:?} with error: {:?}",
                            notification, error
                        );
                    }
                }
            }
            Self::maybe_sleep(config.delay_after_ms).await;
            let Reason::NewBlock { hash, .. } = notification.reason else {
                continue;
            };
            {
                let mut client_guard = client.lock().await;
                context.lock().await.update_wallet(&mut *client_guard).await;
            }
            let value = storage.read_hashed_certificate_value(hash).await?;
            let Some(executed_block) = value.inner().executed_block() else {
                error!("NewBlock notification about value without a block: {hash}");
                continue;
            };
            let new_chains = executed_block
                .messages()
                .iter()
                .flatten()
                .filter_map(|outgoing_message| {
                    if let OutgoingMessage {
                        destination: Destination::Recipient(new_id),
                        message: Message::System(SystemMessage::OpenChain(open_chain_config)),
                        ..
                    } = outgoing_message
                    {
                        let keys = open_chain_config
                            .ownership
                            .all_public_keys()
                            .cloned()
                            .collect::<Vec<_>>();
                        let timestamp = executed_block.block.timestamp;
                        Some((*new_id, keys, timestamp))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            if new_chains.is_empty() {
                continue;
            }
            let mut context_guard = context.lock().await;
            for (new_id, owners, timestamp) in new_chains {
                let key_pair = owners
                    .iter()
                    .find_map(|public_key| context_guard.wallet().key_pair_for_pk(public_key));
                context_guard.update_wallet_for_new_chain(new_id, key_pair, timestamp);
                Self::run_with_chain_id(
                    new_id,
                    clients.clone(),
                    context.clone(),
                    storage.clone(),
                    config.clone(),
                );
            }
        }
        Ok(())
    }

    async fn maybe_sleep(delay_ms: u64) {
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
    }
}

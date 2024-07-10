// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::{
    future::{self, Either, FutureExt as _},
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
    client::ChainClient,
    node::{ValidatorNode, ValidatorNodeProvider},
    worker::Reason,
};
use linera_execution::{Message, SystemMessage};
use linera_storage::Storage;
use linera_views::views::ViewError;
use tracing::{error, info, warn};

use crate::wallet::Wallet;

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
pub trait ClientContext: Send + 'static {
    type ValidatorNodeProvider: ValidatorNodeProvider<Node: ValidatorNode<NotificationStream: Send>> + Send + Sync;
    type Storage: Storage + Clone + Send + Sync;

    fn wallet(&self) -> &Wallet;

    fn make_chain_client(
        &self,
        chain_id: ChainId,
    ) -> ChainClient<Self::ValidatorNodeProvider, Self::Storage>
    where
        ViewError: From<<Self::Storage as Storage>::StoreError>;

    fn update_wallet_for_new_chain(
        &mut self,
        chain_id: ChainId,
        key_pair: Option<KeyPair>,
        timestamp: Timestamp,
    );

    async fn update_wallet(
        &mut self,
        client: &ChainClient<Self::ValidatorNodeProvider, Self::Storage>,
    ) where
        ViewError: From<<Self::Storage as Storage>::StoreError>;
}

/// A `ChainListener` is a process that listens to notifications from validators and reacts
/// appropriately.
pub struct ChainListener
{
    config: ChainListenerConfig,
}

impl ChainListener {
    /// Creates a new chain listener.
    pub fn new(config: ChainListenerConfig) -> Self {
        Self { config }
    }

    /// Runs the chain listener.
    pub async fn run<C>(self, context: Arc<Mutex<C>>, storage: C::Storage)
    where
        C: ClientContext + Send + 'static,
        ViewError: From<<C::Storage as linera_storage::Storage>::StoreError>,
    {
        let chain_ids = context.lock().await.wallet().chain_ids();
        let running: Arc<Mutex<HashSet<ChainId>>> = Arc::default();
        for chain_id in chain_ids {
            Self::run_with_chain_id(
                chain_id,
                context.clone(),
                storage.clone(),
                self.config.clone(),
                running.clone(),
            );
        }
    }

    fn run_with_chain_id<C>(
        chain_id: ChainId,
        context: Arc<Mutex<C>>,
        storage: C::Storage,
        config: ChainListenerConfig,
        running: Arc<Mutex<HashSet<ChainId>>>,
    ) where
        C: ClientContext + Send + 'static,
        ViewError: From<<C::Storage as linera_storage::Storage>::StoreError>,
    {
        let _handle = tokio::task::spawn(async move {
            if let Err(err) =
                Self::run_client_stream(chain_id, context, storage, config, running).await
            {
                error!("Stream for chain {} failed: {}", chain_id, err);
            }
        });
    }

    async fn run_client_stream<C>(
        chain_id: ChainId,
        context: Arc<Mutex<C>>,
        storage: C::Storage,
        config: ChainListenerConfig,
        running: Arc<Mutex<HashSet<ChainId>>>,
    ) -> anyhow::Result<()>
    where
        C: ClientContext + Send + 'static,
        ViewError: From<<C::Storage as linera_storage::Storage>::StoreError>,
    {
        let chain_client = if running.lock().await.contains(&chain_id) {
            return Ok(());
        } else {
            context.lock().await.make_chain_client(chain_id)
        };
        let (listener, listen_handle, local_stream) = chain_client.listen().await?;
        let ((), ()) = futures::try_join!(
            listener.map(Ok),
            Self::process_notifications(
                local_stream,
                listen_handle,
                chain_client,
                context,
                storage,
                config,
                running,
            ))?;
        Ok(())
    }

    async fn process_notifications<C>(
        mut local_stream: impl futures::Stream<Item = linera_core::worker::Notification> + Unpin,
        _listen_handle: linera_core::client::AbortOnDrop,
        chain_client: ChainClient<C::ValidatorNodeProvider, C::Storage>,
        context: Arc<Mutex<C>>,
        storage: C::Storage,
        config: ChainListenerConfig,
        running: Arc<Mutex<HashSet<ChainId>>>,
    ) -> anyhow::Result<()>
    where
        C: ClientContext + Send + 'static,
        ViewError: From<<C::Storage as linera_storage::Storage>::StoreError>,
    {
        let mut timeout = storage.clock().current_time();
        loop {
            let sleep = Box::pin(storage.clock().sleep_until(timeout));
            let notification = match future::select(local_stream.next(), sleep).await {
                Either::Left((Some(notification), _)) => notification,
                Either::Left((None, _)) => return Ok(()),
                Either::Right(((), _)) => {
                    match chain_client.process_inbox_if_owned().await {
                        Err(error) => {
                            warn!(%error, "Failed to process inbox.");
                            timeout = Timestamp::from(u64::MAX);
                        }
                        Ok((_, None)) => timeout = Timestamp::from(u64::MAX),
                        Ok((_, Some(new_timeout))) => timeout = new_timeout.timestamp,
                    }
                    context.lock().await.update_wallet(&chain_client).await;
                    continue;
                }
            };
            info!("Received new notification: {:?}", notification);
            Self::maybe_sleep(config.delay_before_ms).await;
            match &notification.reason {
                Reason::NewIncomingMessage { .. } => timeout = storage.clock().current_time(),
                Reason::NewBlock { .. } | Reason::NewRound { .. } => {
                    if let Err(error) = chain_client.update_validators().await {
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
                context.lock().await.update_wallet(&chain_client).await;
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
                    context.clone(),
                    storage.clone(),
                    config.clone(),
                    running.clone(),
                );
            }
        }
    }

    async fn maybe_sleep(delay_ms: u64) {
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
    }
}

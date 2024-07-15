// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::{
    future::{self, Either},
    lock::Mutex,
    stream::{self, Stream, StreamExt as _},
};
use linera_base::{
    crypto::KeyPair,
    data_types::Timestamp,
    identifiers::{ChainId, Destination},
};
use linera_chain::data_types::OutgoingMessage;
use linera_core::{
    client::{ChainClient, Client},
    node::{ValidatorNode, ValidatorNodeProvider},
    worker::{Notification, Reason},
};
use linera_execution::{Message, SystemMessage};
use linera_storage::Storage;
use linera_views::views::ViewError;

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
pub trait ClientContext: Send {
    type ValidatorNodeProvider: ValidatorNodeProvider<Node: ValidatorNode<NotificationStream: Send>>
        + Send
        + Sync;
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

    fn client(&self) -> Arc<Client<Self::ValidatorNodeProvider, Self::Storage>>
    where
        ViewError: From<<Self::Storage as Storage>::StoreError>;
}

enum Event {
    Notification(Notification),
    RoundTimeout(ChainId),
}

impl Event {
    fn chain_id(&self) -> ChainId {
        match self {
            Event::Notification(Notification { chain_id, .. }) => *chain_id,
            Event::RoundTimeout(chain_id) => *chain_id,
        }
    }
}

/// Return a stream of notifications from a chain, handling round timeouts as appropriate.
async fn listener<P, S>(
    config: ChainListenerConfig,
    chain_client: ChainClient<P, S>,
) -> anyhow::Result<impl Stream<Item = Event>>
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::StoreError>,
{
    let storage = chain_client.storage_client();
    let (listener, _aborter, mut notifications) = chain_client.listen().await?;
    let mut listener = Box::pin(listener);
    let mut timeout = storage.clock().current_time();

    Ok(async_stream::stream! {
        loop {
            let next = future::select(
                notifications.next(),
                future::select(
                    Box::pin(storage.clock().sleep_until(timeout)),
                    &mut listener,
                ),
            );

            let Either::Left((item, _)) = next.await else {
                timeout = next_inbox_timeout(&chain_client).await;
                yield Event::RoundTimeout(chain_client.chain_id());
                continue;
            };

            let Some(notification) = item else { break };

            tracing::info!("Received new notification: {:?}", notification);

            maybe_sleep(config.delay_before_ms).await;
            match notification.reason {
                Reason::NewIncomingMessage { .. } => timeout = storage.clock().current_time(),
                Reason::NewBlock { .. } | Reason::NewRound { .. } => {
                    if let Err(error) = chain_client.update_validators().await {
                        tracing::warn!(
                            "Failed to update validators about the local chain after \
                            receiving notification {notification:?} with error: {error:?}",
                        );
                    }
                }
            }
            maybe_sleep(config.delay_after_ms).await;

            yield Event::Notification(notification);
        }
    })
}

/// Process the chain client's inbox to get the current round timeout.
async fn next_inbox_timeout<P, S>(chain_client: &ChainClient<P, S>) -> Timestamp
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::StoreError>,
{
    match chain_client.process_inbox_if_owned().await {
        Err(error) => {
            tracing::warn!(%error, "Failed to process inbox.");
            Timestamp::from(u64::MAX)
        }
        Ok((_, None)) => Timestamp::from(u64::MAX),
        Ok((_, Some(timeout))) => timeout.timestamp,
    }
}

/// A ‘chain listener’ is a process that listens to events from validators and
/// reacts appropriately.
pub async fn chain_listener<C>(
    config: ChainListenerConfig,
    context: Arc<Mutex<C>>,
) -> anyhow::Result<()>
where
    C: ClientContext + Send + 'static,
    ViewError: From<<C::Storage as linera_storage::Storage>::StoreError>,
{
    let client = context.lock().await.client();
    let storage = client.storage_client().clone();
    let chain_ids = context.lock().await.wallet().chain_ids();
    let mut running: HashSet<ChainId> = HashSet::new();
    let mut events = stream::SelectAll::new();

    for chain_id in chain_ids {
        if !running.contains(&chain_id) {
            running.insert(chain_id);
            events.push(Box::pin(
                listener(
                    config.clone(),
                    context.lock().await.make_chain_client(chain_id),
                )
                .await?,
            ));
        }
    }

    while let Some(event) = events.next().await {
        let chain_client = context.lock().await.make_chain_client(event.chain_id());

        let notification = match event {
            Event::Notification(notification) => notification,
            Event::RoundTimeout(_) => {
                context.lock().await.update_wallet(&chain_client).await;
                continue;
            }
        };

        let Reason::NewBlock { hash, .. } = notification.reason else {
            continue;
        };

        let value = storage.read_hashed_certificate_value(hash).await?;
        let Some(executed_block) = value.inner().executed_block() else {
            tracing::error!("NewBlock notification about value without a block: {hash}");
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
            let new_chain_client = context_guard.make_chain_client(new_id);
            if !running.contains(&new_id) {
                running.insert(new_id);
                events.push(Box::pin(listener(config.clone(), new_chain_client).await?));
            }
        }
    }

    Ok(())
}

async fn maybe_sleep(delay_ms: u64) {
    if delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}

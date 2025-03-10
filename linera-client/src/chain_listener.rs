// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use futures::{lock::Mutex, stream, StreamExt};
use linera_base::{
    crypto::AccountSecretKey,
    data_types::Timestamp,
    identifiers::{ChainId, Destination},
};
use linera_core::{
    client::{ChainClient, ChainClientError},
    node::{NotificationStream, ValidatorNodeProvider},
    worker::{Notification, Reason},
};
use linera_execution::{Message, OutgoingMessage, SystemMessage};
use linera_storage::{Clock as _, Storage};
use tracing::{debug, error, info, instrument, warn, Instrument as _};

use crate::{wallet::Wallet, Error};

#[derive(Debug, Default, Clone, clap::Args)]
pub struct ChainListenerConfig {
    /// Do not create blocks automatically to receive incoming messages. Instead, wait for
    /// an explicit mutation `processInbox`.
    #[arg(
        long = "listener-skip-process-inbox",
        env = "LINERA_LISTENER_SKIP_PROCESS_INBOX"
    )]
    pub skip_process_inbox: bool,

    /// Wait before processing any notification (useful for testing).
    #[arg(
        long = "listener-delay-before-ms",
        default_value = "0",
        env = "LINERA_LISTENER_DELAY_BEFORE"
    )]
    pub delay_before_ms: u64,

    /// Wait after processing any notification (useful for rate limiting).
    #[arg(
        long = "listener-delay-after-ms",
        default_value = "0",
        env = "LINERA_LISTENER_DELAY_AFTER"
    )]
    pub delay_after_ms: u64,
}

type ContextChainClient<C> =
    ChainClient<<C as ClientContext>::ValidatorNodeProvider, <C as ClientContext>::Storage>;

#[cfg_attr(not(web), async_trait, trait_variant::make(Send))]
#[cfg_attr(web, async_trait(?Send))]
pub trait ClientContext: 'static {
    type ValidatorNodeProvider: ValidatorNodeProvider + Sync;
    type Storage: Storage + Clone + Send + Sync + 'static;

    fn wallet(&self) -> &Wallet;

    fn make_chain_client(&self, chain_id: ChainId) -> Result<ContextChainClient<Self>, Error>;

    async fn update_wallet_for_new_chain(
        &mut self,
        chain_id: ChainId,
        key_pair: Option<AccountSecretKey>,
        timestamp: Timestamp,
    ) -> Result<(), Error>;

    async fn update_wallet(&mut self, client: &ContextChainClient<Self>) -> Result<(), Error>;

    fn clients(&self) -> Result<Vec<ContextChainClient<Self>>, Error> {
        let mut clients = vec![];
        for chain_id in &self.wallet().chain_ids() {
            clients.push(self.make_chain_client(*chain_id)?);
        }
        Ok(clients)
    }
}

/// A `ChainListener` is a process that listens to notifications from validators and reacts
/// appropriately.
pub struct ChainListener {
    config: ChainListenerConfig,
    listening: Arc<Mutex<HashSet<ChainId>>>,
}

impl ChainListener {
    /// Creates a new chain listener given client chains.
    pub fn new(config: ChainListenerConfig) -> Self {
        Self {
            config,
            listening: Default::default(),
        }
    }

    /// Runs the chain listener.
    pub async fn run<C>(self, context: Arc<Mutex<C>>, storage: C::Storage)
    where
        C: ClientContext,
    {
        let chain_ids = {
            let guard = context.lock().await;
            let mut chain_ids = BTreeSet::from_iter(guard.wallet().chain_ids());
            chain_ids.insert(guard.wallet().genesis_admin_chain());
            chain_ids
        };
        for chain_id in chain_ids {
            Self::run_with_chain_id(
                chain_id,
                context.clone(),
                storage.clone(),
                self.config.clone(),
                self.listening.clone(),
            );
        }
    }

    #[instrument(level = "trace", skip_all, fields(?chain_id))]
    fn run_with_chain_id<C>(
        chain_id: ChainId,
        context: Arc<Mutex<C>>,
        storage: C::Storage,
        config: ChainListenerConfig,
        listening: Arc<Mutex<HashSet<ChainId>>>,
    ) where
        C: ClientContext,
    {
        let _handle = linera_base::task::spawn(
            async move {
                if let Err(err) =
                    Self::run_client_stream(chain_id, context, storage, config, listening).await
                {
                    error!("Stream for chain {} failed: {}", chain_id, err);
                }
            }
            .in_current_span(),
        );
    }

    #[instrument(level = "trace", skip_all, fields(?chain_id))]
    async fn run_client_stream<C>(
        chain_id: ChainId,
        context: Arc<Mutex<C>>,
        storage: C::Storage,
        config: ChainListenerConfig,
        listening: Arc<Mutex<HashSet<ChainId>>>,
    ) -> Result<(), Error>
    where
        C: ClientContext,
    {
        if !listening.lock().await.insert(chain_id) {
            // If we are already listening to notifications, there's nothing to do.
            // This can happen if we download a child before the parent
            // chain, and then process the OpenChain message in the parent.
            return Ok(());
        }
        // If the client is not present, we can request it.
        let client = context.lock().await.make_chain_client(chain_id)?;
        let (listener, _listen_handle, local_stream) = client.listen().await?;
        let mut local_stream = local_stream.fuse();
        let admin_listener: NotificationStream = if client.admin_id() == chain_id {
            Box::pin(stream::pending())
        } else {
            Box::pin(client.subscribe_to(client.admin_id()).await?)
        };
        let mut admin_listener = admin_listener.fuse();
        client.synchronize_from_validators().await?;
        drop(linera_base::task::spawn(listener.in_current_span()));
        let mut timeout = storage.clock().current_time();
        loop {
            let mut sleep = Box::pin(futures::FutureExt::fuse(
                storage.clock().sleep_until(timeout),
            ));
            let notification = futures::select! {
                maybe_notification = local_stream.next() => {
                    if let Some(notification) = maybe_notification {
                        notification
                    } else {
                        break;
                    }
                }
                maybe_notification = admin_listener.next() => {
                    // A new block on the admin chain may mean a new committee. Set the timer to
                    // process the inbox.
                    if let Some(
                        Notification { reason: Reason::NewBlock { .. }, .. }
                    ) = maybe_notification {
                        timeout = storage.clock().current_time();
                    }
                    continue;
                }
                () = sleep => {
                    timeout = Timestamp::from(u64::MAX);
                    if config.skip_process_inbox {
                        debug!("Not processing inbox due to listener configuration");
                        continue;
                    }
                    debug!("Processing inbox");
                    match client.process_inbox_without_prepare().await {
                        Err(ChainClientError::CannotFindKeyForChain(_)) => {}
                        Err(error) => warn!(%error, "Failed to process inbox."),
                        Ok((certs, None)) => {
                            info!("Done processing inbox. {} blocks created.", certs.len());
                        }
                        Ok((certs, Some(new_timeout))) => {
                            info!(
                                "{} blocks created. Will try processing the inbox later based \
                                 on the given round timeout: {new_timeout:?}",
                                certs.len(),
                            );
                            timeout = new_timeout.timestamp;
                        }
                    }
                    context.lock().await.update_wallet(&client).await?;
                    continue;
                }
            };
            info!("Received new notification: {:?}", notification);
            Self::maybe_sleep(config.delay_before_ms).await;
            match &notification.reason {
                Reason::NewIncomingBundle { .. } => timeout = storage.clock().current_time(),
                Reason::NewBlock { .. } | Reason::NewRound { .. } => {
                    if let Err(error) = client.update_validators(None).await {
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
                context.lock().await.update_wallet(&client).await?;
            }
            let value = storage.read_hashed_confirmed_block(hash).await?;
            let block = value.inner().block();
            let new_chains = block
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
                        let owners = open_chain_config
                            .ownership
                            .all_owners()
                            .cloned()
                            .collect::<Vec<_>>();
                        let timestamp = block.header.timestamp;
                        Some((new_id, owners, timestamp))
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
                    .find_map(|owner| context_guard.wallet().key_pair_for_owner(owner));
                if key_pair.is_some() {
                    context_guard
                        .update_wallet_for_new_chain(*new_id, key_pair, timestamp)
                        .await?;
                    Self::run_with_chain_id(
                        *new_id,
                        context.clone(),
                        storage.clone(),
                        config.clone(),
                        listening.clone(),
                    );
                }
            }
        }
        Ok(())
    }

    async fn maybe_sleep(delay_ms: u64) {
        if delay_ms > 0 {
            linera_base::time::timer::sleep(Duration::from_millis(delay_ms)).await;
        }
    }
}

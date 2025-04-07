// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use futures::{channel::mpsc, lock::Mutex, FutureExt as _, SinkExt as _, Stream, StreamExt};
use linera_base::{
    crypto::AccountSecretKey,
    data_types::Timestamp,
    identifiers::{ChainId, Destination},
};
use linera_core::{
    client::{ChainClient, ChainClientError},
    node::ValidatorNodeProvider,
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
pub struct ChainListener<C: ClientContext> {
    context: Arc<Mutex<C>>,
    storage: C::Storage,
    config: Arc<ChainListenerConfig>,
    listening: Arc<std::sync::Mutex<HashSet<ChainId>>>,
}

impl<C: ClientContext> Clone for ChainListener<C> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
            storage: self.storage.clone(),
            config: self.config.clone(),
            listening: self.listening.clone(),
        }
    }
}

/// A `ChainListener` together with a specific client.
pub struct ChainClientListener<C: ClientContext> {
    client: ContextChainClient<C>,
    listener: ChainListener<C>,
    timeout: Timestamp,
}

impl<C: ClientContext> ChainListener<C> {
    /// Creates a new chain listener given client chains.
    pub fn new(config: ChainListenerConfig, context: Arc<Mutex<C>>, storage: C::Storage) -> Self {
        Self {
            storage,
            context,
            config: Arc::new(config),
            listening: Default::default(),
        }
    }

    /// Runs the chain listener.
    pub async fn run(self) {
        let chain_ids = {
            let guard = self.context.lock().await;
            let mut chain_ids = BTreeSet::from_iter(guard.wallet().chain_ids());
            chain_ids.insert(guard.wallet().genesis_admin_chain());
            chain_ids
        };
        for chain_id in chain_ids {
            self.run_with_chain_id(chain_id);
        }
    }

    /// Spawns a task running the listener for the given chain, if it is not already running.
    #[instrument(level = "trace", skip_all, fields(?chain_id))]
    fn run_with_chain_id(&self, chain_id: ChainId) {
        if !self.listening.lock().unwrap().insert(chain_id) {
            // If we are already listening to notifications, there's nothing to do.
            // This can happen if we download a child before the parent
            // chain, and then process the OpenChain message in the parent.
            return;
        }
        let listener = self.clone();
        let _handle = linera_base::task::spawn(
            async move {
                if let Err(err) = listener.run_client_stream(chain_id).await {
                    error!("Stream for chain {} failed: {}", chain_id, err);
                }
            }
            .in_current_span(),
        );
    }

    #[instrument(level = "trace", skip_all, fields(?chain_id))]
    async fn run_client_stream(self, chain_id: ChainId) -> Result<(), Error> {
        let client = self.context.lock().await.make_chain_client(chain_id)?;
        let (event_tx, event_rx) = mpsc::channel(1);
        if client.admin_id() != chain_id {
            let mut admin_event_tx = event_tx.clone();
            let mut admin_stream = client.subscribe_to(client.admin_id()).await?;
            let _handle = linera_base::task::spawn(
                async move {
                    while let Some(notification) = admin_stream.next().await {
                        if let Reason::NewBlock { .. } = notification.reason {
                            if admin_event_tx.send(()).await.is_err() {
                                return; // The receiver was dropped.
                            }
                        }
                    }
                }
                .in_current_span(),
            );
        }
        let (listener, _listen_handle, local_stream) = client.listen().await?;
        client.synchronize_from_validators().await?;
        let _handle = linera_base::task::spawn(listener.in_current_span());
        let client_listener = ChainClientListener::new(client, self);
        client_listener.run(local_stream, event_rx).await
    }
}

impl<C: ClientContext> ChainClientListener<C> {
    /// Creates a new chain client listener.
    fn new(client: ContextChainClient<C>, listener: ChainListener<C>) -> Self {
        Self {
            client,
            listener,
            timeout: Timestamp::from(u64::MAX),
        }
    }

    /// Listens to notifications from the local stream and to new events, and processes them.
    async fn run(
        mut self,
        local_stream: impl Stream<Item = Notification> + Unpin,
        mut event_rx: mpsc::Receiver<()>,
    ) -> Result<(), Error> {
        let mut local_stream = local_stream.fuse();
        self.maybe_process_inbox().await?;
        let storage = self.listener.storage.clone();
        loop {
            let mut sleep = Box::pin(storage.clock().sleep_until(self.timeout).fuse());
            futures::select! {
                maybe_notification = local_stream.next() => {
                    let Some(notification) = maybe_notification else {
                        break;
                    };
                    self.process_notification(notification).await?;
                }
                maybe_event_msg = event_rx.next() => {
                    let Some(()) = maybe_event_msg else {
                        break;
                    };
                    // A new block on a publisher chain may have created a new event.
                    self.maybe_process_inbox().await?;
                }
                () = sleep => self.maybe_process_inbox().await?,
            }
        }
        Ok(())
    }

    /// Processes a notification from the local stream.
    /// * If there are incoming messages, the inbox is processed.
    /// * If there are new blocks or a new round, the validators are updated.
    /// * If a new block opened a chain that we own, starts listening to it.
    async fn process_notification(&mut self, notification: Notification) -> Result<(), Error> {
        info!("Received new notification: {:?}", notification);
        Self::sleep(self.listener.config.delay_before_ms).await;
        match &notification.reason {
            Reason::NewIncomingBundle { .. } => self.maybe_process_inbox().await?,
            Reason::NewBlock { .. } | Reason::NewRound { .. } => {
                if let Err(error) = self.client.update_validators(None).await {
                    warn!(
                        "Failed to update validators about the local chain after \
                         receiving {notification:?} with error: {error:?}"
                    );
                }
            }
        }
        Self::sleep(self.listener.config.delay_after_ms).await;
        let Reason::NewBlock { hash, .. } = notification.reason else {
            return Ok(());
        };
        self.listener
            .context
            .lock()
            .await
            .update_wallet(&self.client)
            .await?;
        let value = self.listener.storage.read_confirmed_block(hash).await?;
        let block = value.block();
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
                    let owners = open_chain_config.ownership.all_owners().cloned();
                    Some((new_id, owners.collect::<Vec<_>>()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if new_chains.is_empty() {
            return Ok(());
        }
        let mut context_guard = self.listener.context.lock().await;
        let timestamp = block.header.timestamp;
        for (new_id, owners) in new_chains {
            let key_pair = owners
                .iter()
                .find_map(|owner| context_guard.wallet().key_pair_for_owner(owner));
            if key_pair.is_some() {
                context_guard
                    .update_wallet_for_new_chain(*new_id, key_pair, timestamp)
                    .await?;
                self.listener.run_with_chain_id(*new_id);
            }
        }
        Ok(())
    }

    /// Processes the inbox, unless `skip_process_inbox` is set.
    ///
    /// If no block can be produced because we are not the round leader, a timeout is returned
    /// for when to retry; otherwise `u64::MAX` is returned.
    ///
    /// The wallet is persisted with any blocks that processing the inbox added. An error
    /// is returned if persisting the wallet fails.
    async fn maybe_process_inbox(&mut self) -> Result<(), Error> {
        self.timeout = Timestamp::from(u64::MAX);
        if self.listener.config.skip_process_inbox {
            debug!("Not processing inbox due to listener configuration");
            return Ok(());
        }
        debug!("Processing inbox");
        match self.client.process_inbox_without_prepare().await {
            Err(ChainClientError::CannotFindKeyForChain(_)) => {}
            Err(error) => warn!(%error, "Failed to process inbox."),
            Ok((certs, None)) => info!("Done processing inbox. {} blocks created.", certs.len()),
            Ok((certs, Some(new_timeout))) => {
                info!(
                    "{} blocks created. Will try processing the inbox later based \
                     on the given round timeout: {new_timeout:?}",
                    certs.len(),
                );
                self.timeout = new_timeout.timestamp;
            }
        }
        let mut context_guard = self.listener.context.lock().await;
        context_guard.update_wallet(&self.client).await?;
        Ok(())
    }

    /// Sleeps for the given number of milliseconds, if greater than 0.
    async fn sleep(delay_ms: u64) {
        if delay_ms > 0 {
            linera_base::time::timer::sleep(Duration::from_millis(delay_ms)).await;
        }
    }
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use futures::{channel::mpsc, lock::Mutex, SinkExt as _, StreamExt};
use linera_base::{
    crypto::{AccountSecretKey, CryptoHash},
    data_types::Timestamp,
    identifiers::{ChainId, Destination},
};
use linera_core::{
    client::{ChainClient, ChainClientError},
    node::ValidatorNodeProvider,
    worker::Reason,
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
#[derive(Debug)]
pub struct ChainListener<C: ClientContext> {
    context: Arc<Mutex<C>>,
    storage: C::Storage,
    config: Arc<ChainListenerConfig>,
    listening: Arc<Mutex<HashSet<ChainId>>>,
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

    #[instrument(level = "trace", skip_all, fields(?chain_id))]
    fn run_with_chain_id(&self, chain_id: ChainId) {
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
    async fn run_client_stream(&self, chain_id: ChainId) -> Result<(), Error> {
        if !self.listening.lock().await.insert(chain_id) {
            // If we are already listening to notifications, there's nothing to do.
            // This can happen if we download a child before the parent
            // chain, and then process the OpenChain message in the parent.
            return Ok(());
        }
        // If the client is not present, we can request it.
        let client = self.context.lock().await.make_chain_client(chain_id)?;
        let (listener, _listen_handle, local_stream) = client.listen().await?;
        let mut local_stream = local_stream.fuse();
        let (event_tx, mut event_rx) = mpsc::channel(1);
        let mut admin_event_tx = event_tx.clone();
        let mut admin_stream = client.subscribe_to(client.admin_id()).await?;
        if client.admin_id() != chain_id {
            linera_base::task::spawn(async move {
                while let Some(notification) = admin_stream.next().await {
                    if let Reason::NewBlock { .. } = notification.reason {
                        if admin_event_tx.send(()).await.is_err() {
                            return; // The receiver was dropped.
                        }
                    }
                }
            });
        }
        client.synchronize_from_validators().await?;
        drop(linera_base::task::spawn(listener.in_current_span()));
        let mut timeout = self.maybe_process_inbox(&client).await?;
        loop {
            let mut sleep = Box::pin(futures::FutureExt::fuse(
                self.storage.clock().sleep_until(timeout),
            ));
            futures::select! {
                () = sleep => {
                    timeout = self.maybe_process_inbox(&client).await?;
                }
                maybe_event_msg = event_rx.next() => {
                    if maybe_event_msg == Some(()) {
                        // A new block on a publisher chain may have created a new event.
                        timeout = self.maybe_process_inbox(&client).await?;
                    } else {
                        break;
                    }
                }
                maybe_notification = local_stream.next() => {
                    let Some(notification) = maybe_notification else {
                        break;
                    };
                    info!("Received new notification: {:?}", notification);
                    Self::maybe_sleep(self.config.delay_before_ms).await;
                    match &notification.reason {
                        Reason::NewIncomingBundle { .. } => {
                            timeout = self.maybe_process_inbox(&client).await?;
                        }
                        Reason::NewBlock { .. } | Reason::NewRound { .. } => {
                            if let Err(error) = client.update_validators(None).await {
                                warn!(
                                    "Failed to update validators about the local chain after \
                                     receiving {notification:?} with error: {error:?}"
                                );
                            }
                        }
                    }
                    Self::maybe_sleep(self.config.delay_after_ms).await;
                    if let Reason::NewBlock { hash, .. } = notification.reason {
                        self.process_new_block(&client, hash).await?;
                    };
                }
            }
        }
        Ok(())
    }

    async fn process_new_block(
        &self,
        client: &ContextChainClient<C>,
        hash: CryptoHash,
    ) -> Result<(), Error> {
        self.context.lock().await.update_wallet(client).await?;
        let value = self.storage.read_confirmed_block(hash).await?;
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
            return Ok(());
        }
        let mut context_guard = self.context.lock().await;
        for (new_id, owners, timestamp) in new_chains {
            let key_pair = owners
                .iter()
                .find_map(|owner| context_guard.wallet().key_pair_for_owner(owner));
            if key_pair.is_some() {
                context_guard
                    .update_wallet_for_new_chain(*new_id, key_pair, timestamp)
                    .await?;
                self.run_with_chain_id(*new_id);
            }
        }
        Ok(())
    }

    async fn maybe_sleep(delay_ms: u64) {
        if delay_ms > 0 {
            linera_base::time::timer::sleep(Duration::from_millis(delay_ms)).await;
        }
    }

    /// Processes the inbox, unless `skip_process_inbox` is set.
    ///
    /// If no block can be produced because we are not the round leader, a timeout is returned
    /// for when to retry; otherwise `u64::MAX` is returned.
    ///
    /// The wallet is persisted with any blocks that processing the inbox added. An error
    /// is returned if persisting the wallet fails.
    async fn maybe_process_inbox(
        &self,
        client: &ChainClient<C::ValidatorNodeProvider, C::Storage>,
    ) -> Result<Timestamp, Error> {
        let mut timeout = Timestamp::from(u64::MAX);
        if self.config.skip_process_inbox {
            debug!("Not processing inbox due to listener configuration");
            return Ok(timeout);
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
        self.context.lock().await.update_wallet(client).await?;
        Ok(timeout)
    }
}

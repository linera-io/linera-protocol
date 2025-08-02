// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use futures::{
    future::{join_all, select_all},
    lock::Mutex,
    FutureExt as _, StreamExt,
};
use linera_base::{
    crypto::{CryptoHash, Signer},
    data_types::{ChainDescription, Timestamp},
    identifiers::{AccountOwner, BlobType, ChainId},
    task::NonBlockingFuture,
};
use linera_core::{
    client::{AbortOnDrop, ChainClient, ChainClientError},
    node::NotificationStream,
    worker::{Notification, Reason},
    Environment,
};
use linera_storage::{Clock as _, Storage as _};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, warn, Instrument as _};

use crate::{
    wallet::{UserChain, Wallet},
    Error,
};

#[derive(Debug, Default, Clone, clap::Args, serde::Serialize)]
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

type ContextChainClient<C> = ChainClient<<C as ClientContext>::Environment>;

#[cfg_attr(not(web), trait_variant::make(Send))]
#[allow(async_fn_in_trait)]
pub trait ClientContext {
    type Environment: linera_core::Environment;

    fn wallet(&self) -> &Wallet;

    fn storage(&self) -> &<Self::Environment as linera_core::Environment>::Storage;

    fn client(&self) -> &Arc<linera_core::client::Client<Self::Environment>>;

    /// Gets the timing sender for benchmarking, if available.
    #[cfg(not(web))]
    fn timing_sender(
        &self,
    ) -> Option<tokio::sync::mpsc::UnboundedSender<(u64, linera_core::client::TimingType)>>;

    #[cfg(web)]
    fn timing_sender(
        &self,
    ) -> Option<tokio::sync::mpsc::UnboundedSender<(u64, linera_core::client::TimingType)>> {
        None
    }

    fn make_chain_client(&self, chain_id: ChainId) -> ChainClient<Self::Environment> {
        let chain = self
            .wallet()
            .get(chain_id)
            .cloned()
            .unwrap_or_else(|| UserChain::make_other(chain_id, Timestamp::from(0)));
        self.client().create_chain_client(
            chain_id,
            chain.block_hash,
            chain.next_block_height,
            chain.pending_proposal,
            chain.owner,
            self.timing_sender(),
        )
    }

    async fn update_wallet_for_new_chain(
        &mut self,
        chain_id: ChainId,
        owner: Option<AccountOwner>,
        timestamp: Timestamp,
    ) -> Result<(), Error>;

    async fn update_wallet(&mut self, client: &ContextChainClient<Self>) -> Result<(), Error>;
}

#[allow(async_fn_in_trait)]
pub trait ClientContextExt: ClientContext {
    fn clients(&self) -> Vec<ContextChainClient<Self>> {
        let chain_ids = self.wallet().chain_ids();
        let mut clients = vec![];
        for chain_id in chain_ids {
            clients.push(self.make_chain_client(chain_id));
        }
        clients
    }
}

impl<T: ClientContext> ClientContextExt for T {}

/// A chain client together with the stream of notifications from the local node.
///
/// A background task listens to the validators and updates the local node, so any updates to
/// this chain will trigger a notification. The background task is terminated when this gets
/// dropped.
struct ListeningClient<C: ClientContext> {
    /// The chain client.
    client: ContextChainClient<C>,
    /// The abort handle for the task that listens to the validators.
    abort_handle: AbortOnDrop,
    /// The listening task's join handle.
    join_handle: NonBlockingFuture<()>,
    /// The stream of notifications from the local node.
    notification_stream: Arc<Mutex<NotificationStream>>,
    /// This is only `< u64::MAX` when the client is waiting for a timeout to process the inbox.
    timeout: Timestamp,
}

impl<C: ClientContext> ListeningClient<C> {
    fn new(
        client: ContextChainClient<C>,
        abort_handle: AbortOnDrop,
        join_handle: NonBlockingFuture<()>,
        notification_stream: NotificationStream,
    ) -> Self {
        Self {
            client,
            abort_handle,
            join_handle,
            #[allow(clippy::arc_with_non_send_sync)] // Only `Send` with `futures-util/alloc`.
            notification_stream: Arc::new(Mutex::new(notification_stream)),
            timeout: Timestamp::from(u64::MAX),
        }
    }

    async fn stop(self) {
        drop(self.abort_handle);
        if let Err(error) = self.join_handle.await {
            warn!("Failed to join listening task: {error:?}");
        }
    }
}

/// A `ChainListener` is a process that listens to notifications from validators and reacts
/// appropriately.
pub struct ChainListener<C: ClientContext> {
    context: Arc<Mutex<C>>,
    storage: <C::Environment as Environment>::Storage,
    config: Arc<ChainListenerConfig>,
    listening: BTreeMap<ChainId, ListeningClient<C>>,
    /// Map from publishing chain to subscriber chains.
    /// Events emitted on the _publishing chain_ are of interest to the _subscriber chains_.
    event_subscribers: BTreeMap<ChainId, BTreeSet<ChainId>>,
    cancellation_token: CancellationToken,
}

impl<C: ClientContext> ChainListener<C> {
    /// Creates a new chain listener given client chains.
    pub fn new(
        config: ChainListenerConfig,
        context: Arc<Mutex<C>>,
        storage: <C::Environment as Environment>::Storage,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            storage,
            context,
            config: Arc::new(config),
            listening: Default::default(),
            event_subscribers: Default::default(),
            cancellation_token,
        }
    }

    /// Runs the chain listener.
    #[instrument(skip(self))]
    pub async fn run(mut self) -> Result<(), Error> {
        let chain_ids = {
            let guard = self.context.lock().await;
            let mut chain_ids = BTreeSet::from_iter(guard.wallet().chain_ids());
            chain_ids.insert(guard.wallet().genesis_admin_chain());
            chain_ids
        };
        self.listen_recursively(chain_ids).await?;
        loop {
            match self.next_action().await? {
                Action::ProcessInbox(chain_id) => self.maybe_process_inbox(chain_id).await?,
                Action::Notification(notification) => {
                    self.process_notification(notification).await?
                }
                Action::Stop => break,
            }
        }
        join_all(self.listening.into_values().map(|client| client.stop())).await;
        Ok(())
    }

    /// Processes a notification, updating local chains and validators as needed.
    async fn process_notification(&mut self, notification: Notification) -> Result<(), Error> {
        Self::sleep(self.config.delay_before_ms).await;
        match &notification.reason {
            Reason::NewIncomingBundle { .. } => {
                self.maybe_process_inbox(notification.chain_id).await?;
            }
            Reason::NewRound { .. } => self.update_validators(&notification).await?,
            Reason::NewBlock { hash, .. } => {
                self.update_validators(&notification).await?;
                self.update_wallet(notification.chain_id).await?;
                self.add_new_chains(*hash).await?;
                let publishers = self
                    .update_event_subscriptions(notification.chain_id)
                    .await?;
                if !publishers.is_empty() {
                    self.listen_recursively(publishers).await?;
                    self.maybe_process_inbox(notification.chain_id).await?;
                }
                self.process_new_events(notification.chain_id).await?;
            }
        }
        Self::sleep(self.config.delay_after_ms).await;
        Ok(())
    }

    /// If any new chains were created by the given block, and we have a key pair for them,
    /// add them to the wallet and start listening for notifications.
    async fn add_new_chains(&mut self, hash: CryptoHash) -> Result<(), Error> {
        let block = self
            .storage
            .read_confirmed_block(hash)
            .await?
            .ok_or(ChainClientError::MissingConfirmedBlock(hash))?
            .into_block();
        let blobs = block.created_blobs().into_iter();
        let new_chains = blobs
            .filter_map(|(blob_id, blob)| {
                if blob_id.blob_type == BlobType::ChainDescription {
                    let chain_desc: ChainDescription = bcs::from_bytes(blob.content().bytes())
                        .expect("ChainDescription should deserialize correctly");
                    let owners = chain_desc.config().ownership.all_owners().cloned();
                    Some((ChainId(blob_id.hash), owners.collect::<Vec<_>>()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if new_chains.is_empty() {
            return Ok(());
        }
        let mut new_ids = BTreeSet::new();
        let mut context_guard = self.context.lock().await;
        for (new_chain_id, owners) in new_chains {
            for chain_owner in owners {
                if context_guard
                    .client()
                    .signer()
                    .contains_key(&chain_owner)
                    .await
                    .map_err(ChainClientError::signer_failure)?
                {
                    context_guard
                        .update_wallet_for_new_chain(
                            new_chain_id,
                            Some(chain_owner),
                            block.header.timestamp,
                        )
                        .await?;
                    new_ids.insert(new_chain_id);
                }
            }
        }
        drop(context_guard);
        self.listen_recursively(new_ids).await?;
        Ok(())
    }

    /// Processes the inboxes of all chains that are subscribed to `chain_id`.
    async fn process_new_events(&mut self, chain_id: ChainId) -> Result<(), Error> {
        let Some(subscribers) = self.event_subscribers.get(&chain_id).cloned() else {
            return Ok(());
        };
        for subscriber_id in subscribers {
            self.maybe_process_inbox(subscriber_id).await?;
        }
        Ok(())
    }

    /// Starts listening for notifications about the given chains, and any chains that publish
    /// event streams those chains are subscribed to.
    async fn listen_recursively(&mut self, mut chain_ids: BTreeSet<ChainId>) -> Result<(), Error> {
        while let Some(chain_id) = chain_ids.pop_first() {
            chain_ids.extend(self.listen(chain_id).await?);
        }
        Ok(())
    }

    /// Starts listening for notifications about the given chain.
    ///
    /// Returns all publishing chains, that we also need to listen to.
    async fn listen(&mut self, chain_id: ChainId) -> Result<BTreeSet<ChainId>, Error> {
        if self.listening.contains_key(&chain_id) {
            return Ok(BTreeSet::new());
        }
        let client = self.context.lock().await.make_chain_client(chain_id);
        let (listener, abort_handle, notification_stream) = client.listen().await?;
        let join_handle = linera_base::task::spawn(listener.in_current_span());
        let listening_client =
            ListeningClient::new(client, abort_handle, join_handle, notification_stream);
        self.listening.insert(chain_id, listening_client);
        let publishing_chains = self.update_event_subscriptions(chain_id).await?;
        self.maybe_process_inbox(chain_id).await?;
        Ok(publishing_chains)
    }

    /// Updates the event subscribers map, and returns all publishing chains we need to listen to.
    async fn update_event_subscriptions(
        &mut self,
        chain_id: ChainId,
    ) -> Result<BTreeSet<ChainId>, Error> {
        let listening_client = self.listening.get_mut(&chain_id).expect("missing client");
        if !listening_client.client.is_tracked() {
            return Ok(BTreeSet::new());
        }
        let publishing_chains = listening_client.client.event_stream_publishers().await?;
        for publisher_id in &publishing_chains {
            self.event_subscribers
                .entry(*publisher_id)
                .or_default()
                .insert(chain_id);
        }
        Ok(publishing_chains)
    }

    /// Returns the next notification or timeout to process.
    async fn next_action(&mut self) -> Result<Action, Error> {
        loop {
            let (timeout_chain_id, timeout) = self.next_timeout()?;
            let notification_futures = self
                .listening
                .values_mut()
                .map(|client| {
                    let stream = client.notification_stream.clone();
                    Box::pin(async move { stream.lock().await.next().await })
                })
                .collect::<Vec<_>>();
            futures::select! {
                () = self.cancellation_token.cancelled().fuse() => {
                    return Ok(Action::Stop);
                }
                () = self.storage.clock().sleep_until(timeout).fuse() => {
                    return Ok(Action::ProcessInbox(timeout_chain_id));
                }
                (maybe_notification, index, _) = select_all(notification_futures).fuse() => {
                    let Some(notification) = maybe_notification else {
                        let chain_id = *self.listening.keys().nth(index).unwrap();
                        self.listening.remove(&chain_id);
                        warn!("Notification stream for {chain_id} closed");
                        continue;
                    };
                    return Ok(Action::Notification(notification));
                }
            }
        }
    }

    /// Returns the next timeout to process, and the chain to which it applies.
    fn next_timeout(&self) -> Result<(ChainId, Timestamp), Error> {
        let (chain_id, client) = self
            .listening
            .iter()
            .min_by_key(|(_, client)| client.timeout)
            .expect("No chains left to listen to");
        Ok((*chain_id, client.timeout))
    }

    /// Updates the validators about the chain.
    async fn update_validators(&self, notification: &Notification) -> Result<(), Error> {
        let chain_id = notification.chain_id;
        let listening_client = self.listening.get(&chain_id).expect("missing client");
        if let Err(error) = listening_client.client.update_validators(None).await {
            warn!(
                "Failed to update validators about the local chain after \
                 receiving {notification:?} with error: {error:?}"
            );
        }
        Ok(())
    }

    /// Updates the wallet based on the client for this chain.
    async fn update_wallet(&self, chain_id: ChainId) -> Result<(), Error> {
        let client = &self
            .listening
            .get(&chain_id)
            .expect("missing client")
            .client;
        self.context.lock().await.update_wallet(client).await?;
        Ok(())
    }

    /// Processes the inbox, unless `skip_process_inbox` is set.
    ///
    /// If no block can be produced because we are not the round leader, a timeout is returned
    /// for when to retry; otherwise `u64::MAX` is returned.
    ///
    /// The wallet is persisted with any blocks that processing the inbox added. An error
    /// is returned if persisting the wallet fails.
    async fn maybe_process_inbox(&mut self, chain_id: ChainId) -> Result<(), Error> {
        if self.config.skip_process_inbox {
            debug!("Not processing inbox for {chain_id:.8} due to listener configuration");
            return Ok(());
        }
        let listening_client = self.listening.get_mut(&chain_id).expect("missing client");
        if !listening_client.client.is_tracked() {
            debug!("Not processing inbox for non-tracked chain {chain_id:.8}");
            return Ok(());
        }
        debug!("Processing inbox for {chain_id:.8}");
        listening_client.timeout = Timestamp::from(u64::MAX);
        match listening_client
            .client
            .process_inbox_without_prepare()
            .await
        {
            Err(ChainClientError::CannotFindKeyForChain(chain_id)) => {
                debug!(%chain_id, "Cannot find key for chain");
            }
            Err(error) => warn!(%error, "Failed to process inbox."),
            Ok((certs, None)) => info!("Done processing inbox. {} blocks created.", certs.len()),
            Ok((certs, Some(new_timeout))) => {
                info!(
                    "{} blocks created. Will try processing the inbox later based \
                     on the given round timeout: {new_timeout:?}",
                    certs.len(),
                );
                listening_client.timeout = new_timeout.timestamp;
            }
        }
        let mut context_guard = self.context.lock().await;
        context_guard
            .update_wallet(&listening_client.client)
            .await?;
        Ok(())
    }

    /// Sleeps for the given number of milliseconds, if greater than 0.
    async fn sleep(delay_ms: u64) {
        if delay_ms > 0 {
            linera_base::time::timer::sleep(Duration::from_millis(delay_ms)).await;
        }
    }
}

enum Action {
    ProcessInbox(ChainId),
    Notification(Notification),
    Stop,
}

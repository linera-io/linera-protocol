// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use futures::{
    future::{join_all, select_all},
    lock::Mutex,
    Future, FutureExt as _, StreamExt,
};
use linera_base::{
    crypto::CryptoHash,
    data_types::{ChainDescription, Epoch, MessagePolicy, Timestamp},
    identifiers::{AccountOwner, BlobType, ChainId},
    task::NonBlockingFuture,
    util::future::FutureSyncExt as _,
};
use linera_core::{
    client::{
        chain_client::{self, ChainClient},
        AbortOnDrop, ListeningMode,
    },
    node::NotificationStream,
    worker::{Notification, Reason},
    Environment, Wallet,
};
use linera_storage::{Clock as _, Storage as _};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn, Instrument as _};

use crate::error::{self, Error};

#[derive(Default, Debug, Clone, clap::Args, serde::Serialize, serde::Deserialize, tsify::Tsify)]
#[serde(rename_all = "camelCase")]
pub struct ChainListenerConfig {
    /// Do not create blocks automatically to receive incoming messages. Instead, wait for
    /// an explicit mutation `processInbox`.
    #[serde(default)]
    #[arg(
        long = "listener-skip-process-inbox",
        env = "LINERA_LISTENER_SKIP_PROCESS_INBOX"
    )]
    pub skip_process_inbox: bool,

    /// Wait before processing any notification (useful for testing).
    #[serde(default)]
    #[arg(
        long = "listener-delay-before-ms",
        default_value = "0",
        env = "LINERA_LISTENER_DELAY_BEFORE"
    )]
    pub delay_before_ms: u64,

    /// Wait after processing any notification (useful for rate limiting).
    #[serde(default)]
    #[arg(
        long = "listener-delay-after-ms",
        default_value = "0",
        env = "LINERA_LISTENER_DELAY_AFTER"
    )]
    pub delay_after_ms: u64,
}

type ContextChainClient<C> = ChainClient<<C as ClientContext>::Environment>;

#[cfg_attr(not(web), trait_variant::make(Send + Sync))]
#[allow(async_fn_in_trait)]
pub trait ClientContext {
    type Environment: linera_core::Environment;

    fn wallet(&self) -> &<Self::Environment as linera_core::Environment>::Wallet;

    fn storage(&self) -> &<Self::Environment as linera_core::Environment>::Storage;

    fn client(&self) -> &Arc<linera_core::client::Client<Self::Environment>>;

    fn admin_chain_id(&self) -> ChainId {
        self.client().admin_chain_id()
    }

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

    fn make_chain_client(
        &self,
        chain_id: ChainId,
    ) -> impl Future<Output = Result<ChainClient<Self::Environment>, Error>> {
        async move {
            let chain = self
                .wallet()
                .get(chain_id)
                .make_sync()
                .await
                .map_err(error::Inner::wallet)?
                .unwrap_or_default();
            let follow_only = chain.is_follow_only();
            Ok(self.client().create_chain_client(
                chain_id,
                chain.block_hash,
                chain.next_block_height,
                chain.pending_proposal,
                chain.owner,
                self.timing_sender(),
                follow_only,
            ))
        }
    }

    async fn update_wallet_for_new_chain(
        &mut self,
        chain_id: ChainId,
        owner: Option<AccountOwner>,
        timestamp: Timestamp,
        epoch: Epoch,
    ) -> Result<(), Error>;

    async fn update_wallet(&mut self, client: &ContextChainClient<Self>) -> Result<(), Error>;
}

#[allow(async_fn_in_trait)]
pub trait ClientContextExt: ClientContext {
    async fn clients(&self) -> Result<Vec<ContextChainClient<Self>>, Error> {
        use futures::stream::TryStreamExt as _;
        self.wallet()
            .chain_ids()
            .map_err(|e| error::Inner::wallet(e).into())
            .and_then(|chain_id| self.make_chain_client(chain_id))
            .try_collect()
            .await
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
    /// The cancellation token for the background sync process, if started.
    maybe_sync_cancellation_token: Option<CancellationToken>,
}

impl<C: ClientContext> ListeningClient<C> {
    fn new(
        client: ContextChainClient<C>,
        abort_handle: AbortOnDrop,
        join_handle: NonBlockingFuture<()>,
        notification_stream: NotificationStream,
        maybe_sync_cancellation_token: Option<CancellationToken>,
    ) -> Self {
        Self {
            client,
            abort_handle,
            join_handle,
            #[allow(clippy::arc_with_non_send_sync)] // Only `Send` with `futures-util/alloc`.
            notification_stream: Arc::new(Mutex::new(notification_stream)),
            timeout: Timestamp::from(u64::MAX),
            maybe_sync_cancellation_token,
        }
    }

    async fn stop(self) {
        // TODO(#4965): this is unnecessary: the join handle now also acts as an abort handle
        drop(self.abort_handle);
        if let Some(cancellation_token) = self.maybe_sync_cancellation_token {
            cancellation_token.cancel();
        }
        self.join_handle.await;
    }
}

/// Commands to the chain listener.
pub enum ListenerCommand {
    /// Command: start listening to the given chains, using specified listening modes.
    Listen(BTreeMap<ChainId, ListeningMode>),
    /// Command: stop listening to the given chains.
    StopListening(BTreeSet<ChainId>),
    /// Command: set the message policies of some chain clients.
    SetMessagePolicy(BTreeMap<ChainId, MessagePolicy>),
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
    /// The channel through which the listener can receive commands.
    command_receiver: UnboundedReceiver<ListenerCommand>,
    /// Whether to fully sync chains in the background.
    enable_background_sync: bool,
}

impl<C: ClientContext + 'static> ChainListener<C> {
    /// Creates a new chain listener given client chains.
    pub fn new(
        config: ChainListenerConfig,
        context: Arc<Mutex<C>>,
        storage: <C::Environment as Environment>::Storage,
        cancellation_token: CancellationToken,
        command_receiver: UnboundedReceiver<ListenerCommand>,
        enable_background_sync: bool,
    ) -> Self {
        Self {
            storage,
            context,
            config: Arc::new(config),
            listening: Default::default(),
            event_subscribers: Default::default(),
            cancellation_token,
            command_receiver,
            enable_background_sync,
        }
    }

    /// Runs the chain listener.
    #[instrument(skip(self))]
    pub async fn run(mut self) -> Result<impl Future<Output = Result<(), Error>>, Error> {
        let chain_ids = {
            let guard = self.context.lock().await;
            let admin_chain_id = guard.admin_chain_id();
            guard
                .make_chain_client(admin_chain_id)
                .await?
                .synchronize_chain_state(admin_chain_id)
                .await?;
            let mut chain_ids: BTreeMap<_, _> = guard
                .wallet()
                .items()
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .map(|result| {
                    let (chain_id, chain) = result?;
                    let mode = if chain.is_follow_only() {
                        ListeningMode::FollowChain
                    } else {
                        ListeningMode::FullChain
                    };
                    Ok((chain_id, mode))
                })
                .collect::<Result<BTreeMap<_, _>, _>>()
                .map_err(
                    |e: <<C::Environment as Environment>::Wallet as Wallet>::Error| {
                        crate::error::Inner::Wallet(Box::new(e) as _)
                    },
                )?;
            // If the admin chain is not in the wallet, add it as follow-only since we
            // typically don't own it.
            chain_ids
                .entry(admin_chain_id)
                .or_insert(ListeningMode::FollowChain);
            chain_ids
        };

        Ok(async {
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
        })
    }

    /// Processes a notification, updating local chains and validators as needed.
    async fn process_notification(&mut self, notification: Notification) -> Result<(), Error> {
        Self::sleep(self.config.delay_before_ms).await;
        let Some(listening_client) = self.listening.get(&notification.chain_id) else {
            warn!(
                ?notification,
                "ChainListener::process_notification: got a notification without listening to the chain"
            );
            return Ok(());
        };
        let Some(listening_mode) = listening_client.client.listening_mode() else {
            warn!(
                ?notification,
                "ChainListener::process_notification: chain has no listening mode"
            );
            return Ok(());
        };

        if !listening_mode.is_relevant(&notification.reason) {
            debug!(
                reason = ?notification.reason,
                "ChainListener: ignoring notification due to listening mode"
            );
            return Ok(());
        }
        match &notification.reason {
            Reason::NewIncomingBundle { .. } => {
                self.maybe_process_inbox(notification.chain_id).await?;
            }
            Reason::NewRound { .. } => {
                self.update_validators(&notification).await?;
            }
            Reason::NewBlock { hash, .. } => {
                self.update_wallet(notification.chain_id).await?;
                if listening_mode.is_full() {
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
            Reason::NewEvents { .. } => {
                self.process_new_events(notification.chain_id).await?;
            }
            Reason::BlockExecuted { .. } => {}
        }
        Self::sleep(self.config.delay_after_ms).await;
        Ok(())
    }

    /// If any new chains were created by the given block, and we have a key pair for them,
    /// add them to the wallet and start listening for notifications. (This is not done for
    /// fallback owners, as those would have to monitor all chains anyway.)
    async fn add_new_chains(&mut self, hash: CryptoHash) -> Result<(), Error> {
        let block = self
            .storage
            .read_confirmed_block(hash)
            .await?
            .ok_or(chain_client::Error::MissingConfirmedBlock(hash))?
            .into_block();
        let parent_chain_id = block.header.chain_id;
        let blobs = block.created_blobs().into_iter();
        let new_chains = blobs
            .filter_map(|(blob_id, blob)| {
                if blob_id.blob_type == BlobType::ChainDescription {
                    let chain_desc: ChainDescription = bcs::from_bytes(blob.content().bytes())
                        .expect("ChainDescription should deserialize correctly");
                    Some((ChainId(blob_id.hash), chain_desc))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if new_chains.is_empty() {
            return Ok(());
        }
        let mut new_ids = BTreeMap::new();
        let mut context_guard = self.context.lock().await;
        for (new_chain_id, chain_desc) in new_chains {
            for chain_owner in chain_desc.config().ownership.all_owners() {
                if context_guard.client().has_key_for(chain_owner).await? {
                    context_guard
                        .update_wallet_for_new_chain(
                            new_chain_id,
                            Some(*chain_owner),
                            block.header.timestamp,
                            block.header.epoch,
                        )
                        .await?;
                    context_guard
                        .client()
                        .extend_chain_mode(new_chain_id, ListeningMode::FullChain);
                    new_ids.insert(new_chain_id, ListeningMode::FullChain);
                }
            }
        }
        // Re-process the parent chain's outboxes now that the new chains are tracked.
        // This ensures cross-chain messages to newly created chains are delivered.
        if !new_ids.is_empty() {
            context_guard
                .client()
                .local_node
                .retry_pending_cross_chain_requests(parent_chain_id)
                .await?;
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
    async fn listen_recursively(
        &mut self,
        mut chain_ids: BTreeMap<ChainId, ListeningMode>,
    ) -> Result<(), Error> {
        while let Some((chain_id, listening_mode)) = chain_ids.pop_first() {
            for (new_chain_id, new_listening_mode) in self.listen(chain_id, listening_mode).await? {
                match chain_ids.entry(new_chain_id) {
                    Entry::Vacant(vacant) => {
                        vacant.insert(new_listening_mode);
                    }
                    Entry::Occupied(mut occupied) => {
                        occupied.get_mut().extend(Some(new_listening_mode));
                    }
                }
            }
        }

        Ok(())
    }

    /// Background task that syncs received certificates in small batches.
    /// This discovers unacknowledged sender blocks gradually without overwhelming the system.
    #[instrument(skip(context, cancellation_token))]
    async fn background_sync_received_certificates(
        context: Arc<Mutex<C>>,
        chain_id: ChainId,
        cancellation_token: CancellationToken,
    ) -> Result<(), Error> {
        info!("Starting background certificate sync for chain {chain_id}");
        let client = context.lock().await.make_chain_client(chain_id).await?;

        Ok(client
            .find_received_certificates(Some(cancellation_token))
            .await?)
    }

    /// Starts listening for notifications about the given chain.
    ///
    /// Returns all publishing chains, that we also need to listen to.
    async fn listen(
        &mut self,
        chain_id: ChainId,
        listening_mode: ListeningMode,
    ) -> Result<BTreeMap<ChainId, ListeningMode>, Error> {
        let context_guard = self.context.lock().await;
        let existing_mode = context_guard.client().chain_mode(chain_id);
        // If we already have a listener with a sufficient mode, nothing to do.
        if self.listening.contains_key(&chain_id)
            && existing_mode.as_ref().is_some_and(|m| *m >= listening_mode)
        {
            return Ok(BTreeMap::new());
        }
        // Extend the mode in the central map.
        context_guard
            .client()
            .extend_chain_mode(chain_id, listening_mode);
        drop(context_guard);

        // Start background tasks to sync received certificates, if enabled.
        let maybe_sync_cancellation_token = self.start_background_sync(chain_id).await;
        let client = self
            .context
            .lock()
            .await
            .make_chain_client(chain_id)
            .await?;
        let (listener, abort_handle, notification_stream) = client.listen().await?;
        let join_handle = linera_base::task::spawn(listener.in_current_span());
        let listening_client = ListeningClient::new(
            client,
            abort_handle,
            join_handle,
            notification_stream,
            maybe_sync_cancellation_token,
        );
        self.listening.insert(chain_id, listening_client);
        let publishing_chains = self.update_event_subscriptions(chain_id).await?;
        self.maybe_process_inbox(chain_id).await?;
        Ok(publishing_chains)
    }

    async fn start_background_sync(&mut self, chain_id: ChainId) -> Option<CancellationToken> {
        if !self.enable_background_sync {
            return None;
        }
        let is_full = self
            .context
            .lock()
            .await
            .client()
            .chain_mode(chain_id)
            .is_some_and(|m| m.is_full());
        if !is_full {
            return None;
        }
        let context = Arc::clone(&self.context);
        let cancellation_token = CancellationToken::new();
        let cancellation_token_clone = cancellation_token.clone();
        linera_base::task::spawn(async move {
            if let Err(e) = Self::background_sync_received_certificates(
                context,
                chain_id,
                cancellation_token_clone,
            )
            .await
            {
                warn!("Background sync failed for chain {chain_id}: {e}");
            }
        })
        .forget();
        Some(cancellation_token)
    }

    /// Updates the event subscribers map, and returns all publishing chains we need to listen to.
    async fn update_event_subscriptions(
        &mut self,
        chain_id: ChainId,
    ) -> Result<BTreeMap<ChainId, ListeningMode>, Error> {
        let listening_client = self.listening.get_mut(&chain_id).expect("missing client");
        if !listening_client.client.is_tracked() {
            return Ok(BTreeMap::new());
        }
        let publishing_chains: BTreeMap<_, _> = listening_client
            .client
            .event_stream_publishers()
            .await?
            .into_iter()
            .map(|(chain_id, streams)| (chain_id, ListeningMode::EventsOnly(streams)))
            .collect();
        for publisher_id in publishing_chains.keys() {
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
                command = self.command_receiver.recv().then(async |maybe_command| {
                    if let Some(command) = maybe_command {
                        command
                    } else {
                        std::future::pending().await
                    }
                }).fuse() => {
                    match command {
                        ListenerCommand::Listen(new_chains) => {
                            debug!(?new_chains, "received command to listen to new chains");
                            self.listen_recursively(new_chains.clone()).await?;
                            for chain_id in new_chains.keys() {
                                if let Err(error) = self.update_wallet(*chain_id).await {
                                    error!(%error, %chain_id, "error updating the wallet with a chain");
                                }
                            }
                        }
                        ListenerCommand::StopListening(chains) => {
                            debug!(?chains, "received command to stop listening to chains");
                            for chain_id in chains {
                                debug!(%chain_id, "stopping the listener for chain");
                                let Some(listening_client) = self.listening.remove(&chain_id) else {
                                    error!(%chain_id, "attempted to drop a non-existent listener");
                                    continue;
                                };
                                listening_client.stop().await;
                                if let Err(error) = self.context.lock().await.wallet().remove(chain_id).await {
                                    error!(%error, %chain_id, "error removing a chain from the wallet");
                                }
                            }
                        }
                        ListenerCommand::SetMessagePolicy(policies) => {
                            debug!(?policies, "received command to set message policies");
                            for (chain_id, policy) in policies {
                                let Some(listening_client) = self.listening.get_mut(&chain_id) else {
                                    error!(
                                        %chain_id,
                                        "attempted to set the message policy of a non-existent \
                                        listener"
                                    );
                                    continue;
                                };
                                listening_client.client.options_mut().message_policy = policy;
                            }
                        }
                    }
                }
                (maybe_notification, index, _) = select_all(notification_futures).fuse() => {
                    let Some(notification) = maybe_notification else {
                        let chain_id = *self.listening.keys().nth(index).unwrap();
                        warn!("Notification stream for {chain_id} closed");
                        let Some(listening_client) = self.listening.remove(&chain_id) else {
                            error!(%chain_id, "attempted to drop a non-existent listener");
                            continue;
                        };
                        listening_client.stop().await;
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
        let latest_block = if let Reason::NewBlock { hash, .. } = &notification.reason {
            listening_client.client.read_certificate(*hash).await.ok()
        } else {
            None
        };
        if let Err(error) = listening_client
            .client
            .update_validators(None, latest_block)
            .await
        {
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
        if listening_client.client.preferred_owner().is_none() {
            debug!("Not processing inbox for non-owned chain {chain_id:.8}");
            return Ok(());
        }
        debug!("Processing inbox for {chain_id:.8}");
        listening_client.timeout = Timestamp::from(u64::MAX);
        match listening_client
            .client
            .process_inbox_without_prepare()
            .await
        {
            Err(chain_client::Error::CannotFindKeyForChain(chain_id)) => {
                debug!(%chain_id, "Cannot find key for chain");
            }
            Err(error) => warn!(%error, "Failed to process inbox."),
            Ok((certs, None)) => info!(
                %chain_id,
                created_block_count = %certs.len(),
                "done processing inbox",
            ),
            Ok((certs, Some(new_timeout))) => {
                info!(
                    %chain_id,
                    created_block_count = %certs.len(),
                    timeout = %new_timeout,
                    "waiting for round timeout before continuing to process the inbox",
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

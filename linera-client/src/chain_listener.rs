// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use futures::{future, lock::Mutex, Future, FutureExt as _, StreamExt};
use linera_base::{
    crypto::{CryptoHash, Signer},
    data_types::{ChainDescription, MessagePolicy, TimeDelta, Timestamp},
    identifiers::{AccountOwner, BlobType, ChainId},
    util::future::FutureSyncExt as _,
    Task,
};
use linera_core::{
    client::{AbortOnDrop, ChainClient, ChainClientError, ListeningMode},
    node::NotificationStream,
    worker::{Notification, Reason},
    Environment, Wallet,
};
use linera_storage::Storage as _;
use tokio::sync::{mpsc::UnboundedReceiver, Notify};
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
            Ok(self.client().create_chain_client(
                chain_id,
                chain.block_hash,
                chain.next_block_height,
                chain.pending_proposal,
                chain.owner,
                self.timing_sender(),
            ))
        }
    }

    async fn update_wallet_for_new_chain(
        &mut self,
        chain_id: ChainId,
        owner: Option<AccountOwner>,
        timestamp: Timestamp,
        epoch: linera_base::data_types::Epoch,
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
    /// The listening task.
    listener: Task<()>,
    /// The stream of notifications from the local node.
    notification_stream: Arc<Mutex<NotificationStream>>,
    /// The background sync process.
    background_sync: Task<()>,
    /// Signal to wake the per-chain inbox processing task.
    inbox_notify: Arc<Notify>,
    /// The long-lived per-chain inbox processing task.
    inbox_task: Task<()>,
    /// Cancellation token for the per-chain inbox task (child of the global token).
    inbox_cancellation: CancellationToken,
}

impl<C: ClientContext + 'static> ListeningClient<C> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        client: ContextChainClient<C>,
        abort_handle: AbortOnDrop,
        listener: Task<()>,
        notification_stream: NotificationStream,
        background_sync: Task<()>,
        context: &Arc<Mutex<C>>,
        config: &Arc<ChainListenerConfig>,
        parent_cancellation: &CancellationToken,
    ) -> Self {
        let inbox_notify = Arc::new(Notify::new());
        let inbox_cancellation = parent_cancellation.child_token();
        let inbox_task =
            Self::spawn_inbox_task(&client, context, config, &inbox_notify, &inbox_cancellation);
        Self {
            client,
            abort_handle,
            listener,
            #[allow(clippy::arc_with_non_send_sync)] // Only `Send` with `futures-util/alloc`.
            notification_stream: Arc::new(Mutex::new(notification_stream)),
            background_sync,
            inbox_notify,
            inbox_task,
            inbox_cancellation,
        }
    }

    /// Respawns the per-chain inbox task with a fresh clone of the client.
    /// The `inbox_notify` `Arc` is reused so no pending permits are lost.
    fn respawn_inbox_task(
        &mut self,
        parent_cancellation: &CancellationToken,
        context: &Arc<Mutex<C>>,
        config: &Arc<ChainListenerConfig>,
    ) {
        self.inbox_cancellation.cancel();
        self.inbox_cancellation = parent_cancellation.child_token();
        self.inbox_task = Self::spawn_inbox_task(
            &self.client,
            context,
            config,
            &self.inbox_notify,
            &self.inbox_cancellation,
        );
    }

    fn spawn_inbox_task(
        client: &ContextChainClient<C>,
        context: &Arc<Mutex<C>>,
        config: &Arc<ChainListenerConfig>,
        inbox_notify: &Arc<Notify>,
        inbox_cancellation: &CancellationToken,
    ) -> Task<()> {
        Task::spawn(inbox_processing_loop(
            client.clone(),
            Arc::clone(context),
            Arc::clone(config),
            Arc::clone(inbox_notify),
            inbox_cancellation.clone(),
        ))
    }

    async fn stop(self) {
        // TODO(#4965): this is unnecessary: the join handle now also acts as an abort handle
        drop(self.abort_handle);
        self.inbox_cancellation.cancel();
        futures::future::join3(
            self.listener.cancel(),
            self.background_sync.cancel(),
            self.inbox_task.cancel(),
        )
        .await;
    }
}

/// Commands to the chain listener.
pub enum ListenerCommand {
    /// Command: start listening to the given chains. If the chain must produce blocks,
    /// an owner is required.
    Listen(BTreeMap<ChainId, Option<AccountOwner>>),
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
    cancellation_token: CancellationToken,
    /// Map from publishing chain to subscriber chains.
    /// Events emitted on the _publishing chain_ are of interest to the _subscriber chains_.
    event_subscribers: BTreeMap<ChainId, BTreeSet<ChainId>>,
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
            cancellation_token,
            event_subscribers: Default::default(),
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
                    let mode = if chain.owner.is_some() {
                        ListeningMode::FullChain
                    } else {
                        ListeningMode::FollowChain
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

        Ok(async move {
            self.listen_recursively(chain_ids).await?;
            loop {
                match self.next_action().await? {
                    Action::Stop => break,
                    Action::Notification(notification) => {
                        self.process_notification(notification).await?
                    }
                }
            }
            future::join_all(self.listening.into_values().map(|client| client.stop())).await;
            Ok(())
        })
    }

    /// Processes a notification, updating local chains and validators as needed.
    async fn process_notification(&mut self, notification: Notification) -> Result<(), Error> {
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
        Self::sleep(self.config.delay_before_ms).await;
        match &notification.reason {
            Reason::NewIncomingBundle { .. } => {
                self.maybe_notify_inbox_processing(notification.chain_id);
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
                        self.maybe_notify_inbox_processing(notification.chain_id);
                    }
                }
                // Also process events on NewBlock for compatibility with old validators
                // that don't emit NewEvents notifications.
                self.process_new_events(notification.chain_id);
            }
            Reason::NewEvents { .. } => {
                self.process_new_events(notification.chain_id);
            }
            Reason::BlockExecuted { .. } => {}
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
        let parent_chain_id = block.header.chain_id;
        let blobs = block.created_blobs().into_iter();
        let new_chains = blobs
            .filter_map(|(blob_id, blob)| {
                if blob_id.blob_type == BlobType::ChainDescription {
                    let chain_desc: ChainDescription = bcs::from_bytes(blob.content().bytes())
                        .expect("ChainDescription should deserialize correctly");
                    let owners = chain_desc.config().ownership.all_owners().cloned();
                    let epoch = chain_desc.config().epoch;
                    Some((ChainId(blob_id.hash), owners.collect::<Vec<_>>(), epoch))
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
        for (new_chain_id, owners, epoch) in new_chains {
            for chain_owner in owners {
                if context_guard.client().has_key_for(&chain_owner).await? {
                    context_guard
                        .update_wallet_for_new_chain(
                            new_chain_id,
                            Some(chain_owner),
                            block.header.timestamp,
                            epoch,
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
                .retry_pending_cross_chain_requests(parent_chain_id)
                .await?;
        }
        drop(context_guard);
        self.listen_recursively(new_ids).await?;
        Ok(())
    }

    /// Notifies all chains subscribed to `chain_id` to process their inboxes.
    fn process_new_events(&self, chain_id: ChainId) {
        let Some(subscribers) = self.event_subscribers.get(&chain_id) else {
            return;
        };
        for subscriber_id in subscribers {
            self.maybe_notify_inbox_processing(*subscriber_id);
        }
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
    #[instrument(skip(context))]
    async fn background_sync_received_certificates(
        context: Arc<Mutex<C>>,
        chain_id: ChainId,
    ) -> Result<(), Error> {
        info!("Starting background certificate sync for chain {chain_id}");
        let client = context.lock().await.make_chain_client(chain_id).await?;

        Ok(client.find_received_certificates().await?)
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
        let background_sync_task = self.start_background_sync(chain_id).await;
        let client = self
            .context
            .lock()
            .await
            .make_chain_client(chain_id)
            .await?;
        let (listener, abort_handle, notification_stream) = client.listen().await?;
        let listening_client = ListeningClient::new(
            client,
            abort_handle,
            Task::spawn(listener.in_current_span()),
            notification_stream,
            background_sync_task,
            &self.context,
            &self.config,
            &self.cancellation_token,
        );
        self.listening.insert(chain_id, listening_client);
        let publishing_chains = self.update_event_subscriptions(chain_id).await?;
        self.maybe_notify_inbox_processing(chain_id);
        Ok(publishing_chains)
    }

    async fn start_background_sync(&mut self, chain_id: ChainId) -> Task<()> {
        if !self.enable_background_sync
            || !self
                .context
                .lock()
                .await
                .client()
                .chain_mode(chain_id)
                .is_some_and(|m| m.is_full())
        {
            return Task::ready(());
        }

        let context = Arc::clone(&self.context);
        Task::spawn(async move {
            if let Err(e) = Self::background_sync_received_certificates(context, chain_id).await {
                warn!("Background sync failed for chain {chain_id}: {e}");
            }
        })
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

    /// Returns the next notification to process, or a stop signal.
    async fn next_action(&mut self) -> Result<Action, Error> {
        loop {
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
                            let listening_modes = self.update_wallet_for_listening(new_chains).await?;
                            self.listen_recursively(listening_modes).await?;
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
                                listening_client.respawn_inbox_task(
                                    &self.cancellation_token,
                                    &self.context,
                                    &self.config,
                                );
                            }
                        }
                    }
                }
                (maybe_notification, index, _) = future::select_all(notification_futures).fuse() => {
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

    /// Updates the wallet with the set of chains we're supposed to start listening to,
    /// and returns the appropriate listening modes based on whether we have the private
    /// keys corresponding to the given chains' owners.
    async fn update_wallet_for_listening(
        &self,
        new_chains: BTreeMap<ChainId, Option<AccountOwner>>,
    ) -> Result<BTreeMap<ChainId, ListeningMode>, Error> {
        let mut chains = BTreeMap::new();
        let context_guard = self.context.lock().await;
        for (chain_id, owner) in new_chains {
            if let Some(owner) = owner {
                if context_guard
                    .client()
                    .signer()
                    .contains_key(&owner)
                    .await
                    .map_err(ChainClientError::signer_failure)?
                {
                    // Try to modify existing chain entry, setting the owner.
                    let modified = context_guard
                        .wallet()
                        .modify(chain_id, |chain| chain.owner = Some(owner))
                        .await
                        .map_err(error::Inner::wallet)?;
                    // If the chain didn't exist, insert a new entry.
                    if modified.is_none() {
                        let chain_description = context_guard
                            .client()
                            .get_chain_description(chain_id)
                            .await?;
                        let timestamp = chain_description.timestamp();
                        let epoch = chain_description.config().epoch;
                        context_guard
                            .wallet()
                            .insert(
                                chain_id,
                                linera_core::wallet::Chain {
                                    owner: Some(owner),
                                    timestamp,
                                    epoch: Some(epoch),
                                    ..Default::default()
                                },
                            )
                            .await
                            .map_err(error::Inner::wallet)?;
                    }

                    chains.insert(chain_id, ListeningMode::FullChain);
                }
            } else {
                chains.insert(chain_id, ListeningMode::FollowChain);
            }
        }
        Ok(chains)
    }

    /// Signals the per-chain inbox processing task to wake up and process the inbox.
    fn maybe_notify_inbox_processing(&self, chain_id: ChainId) {
        if let Some(listening_client) = self.listening.get(&chain_id) {
            listening_client.inbox_notify.notify_one();
        }
    }

    /// Sleeps for the given number of milliseconds, if greater than 0.
    async fn sleep(delay_ms: u64) {
        if delay_ms > 0 {
            linera_base::time::timer::sleep(Duration::from_millis(delay_ms)).await;
        }
    }
}

/// Per-chain inbox processing loop. Runs as a long-lived tokio task. Wakes on
/// `inbox_notify` signals and processes the inbox, handling round-leader timeouts
/// internally. Multiple notifications while busy collapse into a single permit.
async fn inbox_processing_loop<C: ClientContext>(
    client: ContextChainClient<C>,
    context: Arc<Mutex<C>>,
    config: Arc<ChainListenerConfig>,
    inbox_notify: Arc<Notify>,
    cancellation_token: CancellationToken,
) {
    let chain_id = client.chain_id();
    loop {
        futures::select! {
            () = cancellation_token.cancelled().fuse() => break,
            () = inbox_notify.notified().fuse() => {
                if config.skip_process_inbox {
                    debug!("Not processing inbox for {chain_id:.8} due to listener configuration");
                    continue;
                }
                if !client.is_tracked() {
                    debug!("Not processing inbox for non-tracked chain {chain_id:.8}");
                    continue;
                }
                if client.preferred_owner().is_none() {
                    debug!("Not processing inbox for follow-only chain {chain_id:.8}");
                    continue;
                }
                debug!("Processing inbox for {chain_id:.8}");

                // Inner loop handles round-leader timeouts: if we can't produce a block
                // because we're not the leader, sleep until the timeout then retry.
                // A new notification or cancellation can interrupt the sleep.
                loop {
                    match client.process_inbox_without_prepare().await {
                        Err(ChainClientError::CannotFindKeyForChain(chain_id)) => {
                            debug!(%chain_id, "Cannot find key for chain");
                            break;
                        }
                        Err(error) => {
                            warn!(%error, "Failed to process inbox");
                            break;
                        }
                        Ok((certs, None)) => {
                            if certs.is_empty() {
                                debug!(%chain_id, "done processing inbox: no blocks created");
                            } else {
                                info!(
                                    %chain_id,
                                    created_block_count = %certs.len(),
                                    "done processing inbox",
                                );
                            }
                            break;
                        }
                        Ok((certs, Some(new_timeout))) => {
                            info!(
                                %chain_id,
                                created_block_count = %certs.len(),
                                timeout = %new_timeout,
                                "waiting for round timeout before continuing to process the inbox",
                            );
                            let delta = new_timeout.timestamp.delta_since(Timestamp::now());
                            if delta > TimeDelta::ZERO {
                                futures::select! {
                                    () = cancellation_token.cancelled().fuse() => return,
                                    () = linera_base::time::timer::sleep(delta.as_duration()).fuse() => {},
                                    () = inbox_notify.notified().fuse() => {},
                                }
                            }
                        }
                    }
                }

                if let Err(error) = context.lock().await.update_wallet(&client).await {
                    warn!(%error, "Failed to update wallet after inbox processing");
                }
            }
        }
    }
}

enum Action {
    Notification(Notification),
    Stop,
}

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    data_types::{Block, Medium, MessageGroup, Origin},
    ChainError, ChainManager,
};
use linera_base::{
    crypto::HashValue,
    data_types::{BlockHeight, ChainId, EffectId},
    ensure,
};
use linera_execution::{
    system::SystemEffect, ApplicationDescription, ApplicationId, ApplicationRegistryView,
    BytecodeId, BytecodeLocation, Destination, Effect, EffectContext, ExecutionResult,
    ExecutionRuntimeContext, ExecutionStateView, OperationContext, RawExecutionResult,
};
use linera_views::{
    collection_view::CollectionView, common::Context, impl_view, log_view::LogView,
    map_view::MapView, queue_view::QueueView, register_view::RegisterView, scoped_view::ScopedView,
    views::ViewError,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// A view accessing the state of a chain.
#[derive(Debug)]
pub struct ChainStateView<C> {
    /// Execution state, including system and user applications.
    pub execution_state: ScopedView<0, ExecutionStateView<C>>,
    /// Hash of the execution state.
    pub execution_state_hash: ScopedView<1, RegisterView<C, Option<HashValue>>>,

    /// Block-chaining state.
    pub tip_state: ScopedView<2, RegisterView<C, ChainTipState>>,

    /// Consensus state.
    pub manager: ScopedView<3, RegisterView<C, ChainManager>>,

    /// Hashes of all certified blocks for this sender.
    /// This ends with `block_hash` and has length `usize::from(next_block_height)`.
    pub confirmed_log: ScopedView<4, LogView<C, HashValue>>,
    /// Hashes of all certified blocks known as a receiver (local ordering).
    pub received_log: ScopedView<5, LogView<C, HashValue>>,

    /// Communication state of applications.
    pub communication_states:
        ScopedView<6, CollectionView<C, ApplicationId, CommunicationStateView<C>>>,

    /// The application bytecodes that have been published.
    pub known_applications: ScopedView<7, ApplicationRegistryView<C>>,
}

impl_view!(ChainStateView {
    execution_state,
    execution_state_hash,
    tip_state,
    manager,
    confirmed_log,
    received_log,
    communication_states,
    known_applications,
});

/// Block-chaining state.
#[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChainTipState {
    /// Hash of the latest certified block in this chain, if any.
    pub block_hash: Option<HashValue>,
    /// Sequence number tracking blocks.
    pub next_block_height: BlockHeight,
}

/// A view accessing the communication state of an application.
#[derive(Debug)]
pub struct CommunicationStateView<C> {
    /// Mailboxes used to receive messages indexed by their origin.
    pub inboxes: ScopedView<0, CollectionView<C, Origin, InboxStateView<C>>>,
    /// Mailboxes used to send messages, indexed by recipient.
    pub outboxes: ScopedView<1, CollectionView<C, ChainId, OutboxStateView<C>>>,
    /// Channels able to multicast messages to subscribers.
    pub channels: ScopedView<2, CollectionView<C, String, ChannelStateView<C>>>,
}

impl_view!(CommunicationStateView {
    inboxes,
    outboxes,
    channels
});

/// An outbox used to send messages to another chain. NOTE: Messages are implied by the
/// execution of blocks, so currently we just send the certified blocks over and let the
/// receivers figure out what was the message for them.
#[derive(Debug)]
pub struct OutboxStateView<C> {
    /// Keep sending these certified blocks of ours until they are acknowledged by
    /// receivers.
    pub queue: ScopedView<0, QueueView<C, BlockHeight>>,
}

impl_view!(OutboxStateView { queue });

impl<C> OutboxStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
{
    pub async fn block_heights(&mut self) -> Result<Vec<BlockHeight>, ChainError> {
        let count = self.queue.count();
        let heights = self.queue.read_front(count).await?;
        Ok(heights)
    }
}

/// An inbox used to receive and execute messages from another chain.
#[derive(Debug)]
pub struct InboxStateView<C> {
    /// We have already received the cross-chain requests and enqueued all the messages
    /// below this height.
    pub next_height_to_receive: ScopedView<0, RegisterView<C, BlockHeight>>,
    /// These events have been received but not yet picked by a block to be executed.
    pub received_events: ScopedView<1, QueueView<C, Event>>,
    /// These events have been executed but the cross-chain requests have not been
    /// received yet.
    pub expected_events: ScopedView<2, QueueView<C, Event>>,
}

impl_view!(InboxStateView {
    next_height_to_receive,
    received_events,
    expected_events
});

/// The state of a channel followed by subscribers.
#[derive(Debug)]
pub struct ChannelStateView<C> {
    /// The current subscribers.
    pub subscribers: ScopedView<0, MapView<C, ChainId, ()>>,
    /// The messages waiting to be delivered to present and past subscribers.
    pub outboxes: ScopedView<1, CollectionView<C, ChainId, OutboxStateView<C>>>,
    /// The latest block height, if any, to be sent to future subscribers.
    pub block_height: ScopedView<2, RegisterView<C, Option<BlockHeight>>>,
}

impl_view!(ChannelStateView {
    subscribers,
    outboxes,
    block_height
});

/// A message sent by some (unspecified) chain at a particular height and index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// The height of the block that created the event.
    pub height: BlockHeight,
    /// The index of the effect.
    pub index: usize,
    /// The effect of the event.
    pub effect: Effect,
}

impl<C> ChainStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
    C::Extra: ExecutionRuntimeContext,
{
    pub fn chain_id(&self) -> ChainId {
        self.execution_state.system.description.extra().chain_id()
    }

    async fn mark_messages_as_received(
        outboxes: &mut CollectionView<C, ChainId, OutboxStateView<C>>,
        application_id: ApplicationId,
        origin: &Origin,
        recipient: ChainId,
        height: BlockHeight,
    ) -> Result<bool, ChainError> {
        let outbox = outboxes.load_entry(recipient).await?;
        if outbox.queue.count() == 0 {
            log::warn!(
                "All messages were already marked as received in the outbox {:?}::{:?} to {:?}",
                application_id,
                origin,
                recipient
            );
            return Ok(false);
        }
        let updated = outbox.mark_messages_as_received(height).await?;
        if updated && outbox.queue.count() == 0 {
            outboxes.remove_entry(recipient)?;
        }
        Ok(updated)
    }

    pub async fn mark_outbox_messages_as_received(
        &mut self,
        application_id: ApplicationId,
        recipient: ChainId,
        height: BlockHeight,
    ) -> Result<bool, ChainError> {
        let origin = Origin {
            chain_id: self.chain_id(),
            medium: Medium::Direct,
        };
        let communication_state = self.communication_states.load_entry(application_id).await?;
        Self::mark_messages_as_received(
            &mut communication_state.outboxes,
            application_id,
            &origin,
            recipient,
            height,
        )
        .await
    }

    pub async fn mark_channel_messages_as_received(
        &mut self,
        name: &str,
        application_id: ApplicationId,
        recipient: ChainId,
        height: BlockHeight,
    ) -> Result<bool, ChainError> {
        let origin = Origin {
            chain_id: self.chain_id(),
            medium: Medium::Channel(name.to_string()),
        };
        let communication_state = self.communication_states.load_entry(application_id).await?;
        let channel = communication_state
            .channels
            .load_entry(name.to_string())
            .await?;
        Self::mark_messages_as_received(
            &mut channel.outboxes,
            application_id,
            &origin,
            recipient,
            height,
        )
        .await
    }

    /// Invariant for the states of active chains.
    pub fn is_active(&self) -> bool {
        self.execution_state.system.is_active()
    }

    /// Invariant for the states of active chains.
    #[allow(clippy::result_large_err)]
    pub fn ensure_is_active(&self) -> Result<(), ChainError> {
        if self.is_active() {
            Ok(())
        } else {
            Err(ChainError::InactiveChain(self.chain_id()))
        }
    }

    /// Verify that this chain is up-to-date and all the messages executed ahead of time
    /// have been properly received by now.
    pub async fn validate_incoming_messages(&mut self) -> Result<(), ChainError> {
        for id in self.communication_states.indices().await? {
            let state = self.communication_states.load_entry(id).await?;
            for origin in state.inboxes.indices().await? {
                let inbox = state.inboxes.load_entry(origin.clone()).await?;
                let expected_event = inbox.expected_events.front().await?;
                ensure!(
                    expected_event.is_none(),
                    ChainError::MissingCrossChainUpdate {
                        chain_id: self.chain_id(),
                        application_id: id,
                        origin,
                        height: expected_event.unwrap().height,
                    }
                );
            }
        }
        Ok(())
    }

    pub async fn next_block_height_to_receive(
        &mut self,
        application_id: ApplicationId,
        origin: Origin,
    ) -> Result<BlockHeight, ChainError> {
        let communication_state = self.communication_states.load_entry(application_id).await?;
        let inbox = communication_state.inboxes.load_entry(origin).await?;
        Ok(*inbox.next_height_to_receive.get())
    }

    pub async fn last_anticipated_block_height(
        &mut self,
        application_id: ApplicationId,
        origin: Origin,
    ) -> Result<Option<BlockHeight>, ChainError> {
        let communication_state = self.communication_states.load_entry(application_id).await?;
        let inbox = communication_state.inboxes.load_entry(origin).await?;
        match inbox.expected_events.back().await? {
            Some(event) => Ok(Some(event.height)),
            None => Ok(None),
        }
    }

    /// Schedule operations to be executed as a recipient, unless this block was already
    /// processed. Returns true if the call changed the chain state. Operations must be
    /// received by order of heights and indices.
    pub async fn receive_block(
        &mut self,
        application_id: ApplicationId,
        origin: &Origin,
        height: BlockHeight,
        effects: Vec<(ApplicationId, Destination, Effect)>,
        certificate_hash: HashValue,
    ) -> Result<bool, ChainError> {
        let chain_id = self.chain_id();
        let communication_state = self.communication_states.load_entry(application_id).await?;
        let inbox = communication_state
            .inboxes
            .load_entry(origin.clone())
            .await?;
        if height < *inbox.next_height_to_receive.get() {
            // We have already received this block.
            return Ok(false);
        }
        log::trace!(
            "Processing new messages to {:?} from {:?}::{:?} at height {}",
            chain_id,
            application_id,
            origin,
            height
        );
        // Mark the block as received.
        inbox.next_height_to_receive.set(height.try_add_one()?);

        self.received_log.push(certificate_hash);

        let mut was_a_recipient = false;
        for (index, (app_id, destination, effect)) in effects.into_iter().enumerate() {
            // Skip events that do not belong to this application.
            if app_id != application_id {
                continue;
            }
            // Skip events that do not belong to this origin OR have no effect on this
            // recipient.
            match destination {
                Destination::Recipient(id) => {
                    if origin.medium != Medium::Direct || id != chain_id {
                        continue;
                    }
                }
                Destination::Subscribers(name) => {
                    if !matches!(&origin.medium, Medium::Channel(n) if n == &name) {
                        continue;
                    }
                }
            }
            was_a_recipient = true;
            if let ApplicationId::System = app_id {
                // Handle special effects to be executed immediately.
                let effect_id = EffectId {
                    chain_id: origin.chain_id,
                    height,
                    index,
                };
                self.execute_immediate_effect(effect_id, &effect, chain_id, certificate_hash)
                    .await?;
            }
            let communication_state = self.communication_states.load_entry(application_id).await?;
            let inbox = communication_state
                .inboxes
                .load_entry(origin.clone())
                .await?;
            // Find if the message was executed ahead of time.
            match inbox.expected_events.front().await? {
                Some(event) => {
                    if height == event.height && index == event.index {
                        // We already executed this message by anticipation. Remove it from the queue.
                        assert_eq!(effect, event.effect, "Unexpected effect in certified block");
                        inbox.expected_events.delete_front();
                    } else {
                        // The receiver has already executed a later event from the same
                        // sender ahead of time so we should skip this one.
                        assert!(
                            (height, index) < (event.height, event.index),
                            "Unexpected event order in certified block"
                        );
                    }
                }
                None => {
                    // Otherwise, schedule the message for execution.
                    inbox.received_events.push_back(Event {
                        height,
                        index,
                        effect,
                    });
                }
            }
        }
        debug_assert!(
            was_a_recipient,
            "The block received by {:?} from {:?} at height {:?} was entirely ignored. This should not happen",
            chain_id, origin, height
        );
        Ok(true)
    }

    async fn execute_immediate_effect(
        &mut self,
        effect_id: EffectId,
        effect: &Effect,
        chain_id: ChainId,
        certificate_hash: HashValue,
    ) -> Result<(), ChainError> {
        match &effect {
            Effect::System(SystemEffect::OpenChain {
                id,
                owner,
                epoch,
                committees,
                admin_id,
            }) if id == &chain_id => {
                self.execution_state.system.open_chain(
                    effect_id,
                    *id,
                    *owner,
                    *epoch,
                    committees.clone(),
                    *admin_id,
                );
                // Recompute the state hash.
                let hash = self.execution_state.hash_value().await?;
                self.execution_state_hash.set(Some(hash));
                // Last, reset the consensus state based on the current ownership.
                self.manager
                    .get_mut()
                    .reset(self.execution_state.system.ownership.get());
            }
            Effect::System(SystemEffect::BytecodePublished) => {
                let bytecode_id = BytecodeId::from(effect_id);
                let bytecode_location = BytecodeLocation {
                    certificate_hash,
                    operation_index: effect_id.index,
                };
                self.known_applications
                    .register_published_bytecode(bytecode_id, bytecode_location);
            }
            _ => {}
        }

        Ok(())
    }

    /// Verify that the incoming_messages are in the right order. This matters for inbox
    /// invariants, notably the fact that inbox.expected_events is sorted.
    #[allow(clippy::result_large_err)]
    fn check_incoming_messages(&self, messages: &[MessageGroup]) -> Result<(), ChainError> {
        let mut next_messages: HashMap<(ApplicationId, Origin), (BlockHeight, usize)> =
            HashMap::new();
        for message_group in messages {
            let next_message = next_messages
                .entry((message_group.application_id, message_group.origin.clone()))
                .or_default();
            for (message_index, _) in &message_group.effects {
                ensure!(
                    (message_group.height, *message_index) >= *next_message,
                    ChainError::InvalidMessageOrder {
                        chain_id: self.chain_id(),
                        application_id: message_group.application_id,
                        origin: message_group.origin.clone(),
                        height: message_group.height,
                        index: *message_index,
                    }
                );
                *next_message = (message_group.height, *message_index + 1);
            }
        }
        Ok(())
    }

    /// Execute a new block: first the incoming messages, then the main operation.
    /// * Modifies the state of inboxes, outboxes, and channels, if needed.
    /// * As usual, in case of errors, `self` may not be consistent any more and should be thrown away.
    /// * Returns the list of effects caused by the block being executed.
    pub async fn execute_block(
        &mut self,
        block: &Block,
    ) -> Result<Vec<(ApplicationId, Destination, Effect)>, ChainError> {
        assert_eq!(block.chain_id, self.chain_id());
        let chain_id = self.chain_id();
        let mut effects = Vec::new();
        // First, process incoming messages.
        self.check_incoming_messages(&block.incoming_messages)?;

        for message_group in &block.incoming_messages {
            log::trace!(
                "Updating inbox {:?}::{:?} in chain {:?}",
                message_group.application_id,
                message_group.origin,
                chain_id
            );
            for (message_index, message_effect) in &message_group.effects {
                let communication_state = self
                    .communication_states
                    .load_entry(message_group.application_id)
                    .await?;
                let inbox = communication_state
                    .inboxes
                    .load_entry(message_group.origin.clone())
                    .await?;
                // Receivers are allowed to skip events from the received queue.
                while let Some(
                    event @ Event {
                        height,
                        index,
                        effect: _,
                    },
                ) = inbox.received_events.front().await?
                {
                    if height > message_group.height
                        || (height == message_group.height && index >= *message_index)
                    {
                        break;
                    }
                    assert!((height, index) < (message_group.height, *message_index));
                    inbox.received_events.delete_front();
                    log::trace!("Skipping received event {:?}", event);
                }
                // Reconcile the event with the received queue, or mark it as "expected".
                match inbox.received_events.front().await? {
                    Some(event) => {
                        let Event {
                            height,
                            index,
                            ref effect,
                        } = event;
                        ensure!(
                            message_group.height == height && message_index == &index,
                            ChainError::InvalidMessage {
                                chain_id: self.chain_id(),
                                application_id: message_group.application_id,
                                origin: message_group.origin.clone(),
                                height: message_group.height,
                                index: *message_index,
                                expected_height: height,
                                expected_index: index,
                            }
                        );
                        ensure!(
                            message_effect == effect,
                            ChainError::InvalidMessageContent {
                                chain_id: self.chain_id(),
                                application_id: message_group.application_id,
                                origin: message_group.origin.clone(),
                                height: message_group.height,
                                index: *message_index,
                            }
                        );
                        inbox.received_events.delete_front();
                        log::trace!("Consuming event {:?}", event);
                    }
                    None => {
                        let event = Event {
                            height: message_group.height,
                            index: *message_index,
                            effect: message_effect.clone(),
                        };
                        log::trace!("Marking event as expected: {:?}", event);
                        inbox.expected_events.push_back(event);
                    }
                }
                // Execute the received effect.
                let context = EffectContext {
                    chain_id,
                    height: block.height,
                    effect_id: EffectId {
                        chain_id: message_group.origin.chain_id,
                        height: message_group.height,
                        index: *message_index,
                    },
                };

                let application = self
                    .describe_application(message_group.application_id)
                    .await?;
                let results = self
                    .execution_state
                    .execute_effect(
                        &application,
                        &context,
                        message_effect,
                        &mut self.known_applications,
                    )
                    .await?;

                let communication_state = self
                    .communication_states
                    .load_entry(message_group.application_id)
                    .await?;
                Self::process_execution_results(
                    &mut communication_state.outboxes,
                    &mut communication_state.channels,
                    &mut effects,
                    context.height,
                    results,
                )
                .await?;
            }
        }
        // Second, execute the operations in the block and remember the recipients to notify.
        for (index, (application_id, operation)) in block.operations.iter().enumerate() {
            let application = self.describe_application(*application_id).await?;
            let context = OperationContext {
                chain_id,
                height: block.height,
                index,
            };
            let communication_state = self
                .communication_states
                .load_entry(*application_id)
                .await?;
            let results = self
                .execution_state
                .execute_operation(
                    &application,
                    &context,
                    operation,
                    &mut self.known_applications,
                )
                .await?;

            Self::process_execution_results(
                &mut communication_state.outboxes,
                &mut communication_state.channels,
                &mut effects,
                context.height,
                results,
            )
            .await?;
        }
        // Recompute the state hash.
        let hash = self.execution_state.hash_value().await?;
        self.execution_state_hash.set(Some(hash));
        // Last, reset the consensus state based on the current ownership.
        self.manager
            .get_mut()
            .reset(self.execution_state.system.ownership.get());
        Ok(effects)
    }

    /// Register a new application in the chain state.
    ///
    /// Allows executing operations and effects for that application later.
    pub fn register_application(&mut self, application: ApplicationDescription) -> ApplicationId {
        match application {
            ApplicationDescription::System => ApplicationId::System,
            ApplicationDescription::User(application) => ApplicationId::User(
                self.known_applications
                    .register_existing_application(application),
            ),
        }
    }

    /// Retrieve an application description.
    ///
    /// Retrieves the application description (with its bytecode location) from the internal map of
    /// applications known by this chain.
    pub async fn describe_application(
        &mut self,
        application_id: ApplicationId,
    ) -> Result<ApplicationDescription, ChainError> {
        match application_id {
            ApplicationId::System => Ok(ApplicationDescription::System),
            ApplicationId::User(id) => {
                let description = self.known_applications.describe_application(id).await?;
                Ok(ApplicationDescription::User(description))
            }
        }
    }

    async fn process_execution_results(
        outboxes: &mut CollectionView<C, ChainId, OutboxStateView<C>>,
        channels: &mut CollectionView<C, String, ChannelStateView<C>>,
        effects: &mut Vec<(ApplicationId, Destination, Effect)>,
        height: BlockHeight,
        results: Vec<ExecutionResult>,
    ) -> Result<(), ChainError> {
        for result in results {
            match result {
                ExecutionResult::System { result, .. } => {
                    Self::process_raw_execution_result(
                        ApplicationId::System,
                        outboxes,
                        channels,
                        effects,
                        height,
                        result,
                    )
                    .await?;
                }
                ExecutionResult::User(application_id, raw) => {
                    Self::process_raw_execution_result(
                        ApplicationId::User(application_id),
                        outboxes,
                        channels,
                        effects,
                        height,
                        raw,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    async fn process_raw_execution_result<E: Into<Effect>>(
        application_id: ApplicationId,
        outboxes: &mut CollectionView<C, ChainId, OutboxStateView<C>>,
        channels: &mut CollectionView<C, String, ChannelStateView<C>>,
        effects: &mut Vec<(ApplicationId, Destination, Effect)>,
        height: BlockHeight,
        application: RawExecutionResult<E>,
    ) -> Result<(), ChainError> {
        // Record the effects of the execution. Effects are understood within an
        // application.
        let mut recipients = HashSet::new();
        let mut channel_broadcasts = HashSet::new();
        for (destination, effect) in application.effects {
            match &destination {
                Destination::Recipient(id) => {
                    recipients.insert(*id);
                }
                Destination::Subscribers(name) => {
                    channel_broadcasts.insert(name.to_string());
                }
            }
            effects.push((application_id, destination, effect.into()));
        }

        // Update the (regular) outboxes.
        for recipient in recipients {
            let outbox = outboxes.load_entry(recipient).await?;
            outbox.schedule_message(height).await?;
        }

        // Update the channels.
        for (name, id) in application.unsubscribe {
            let channel = channels.load_entry(name.to_string()).await?;
            // Remove subscriber. Do not remove the channel outbox yet.
            channel.subscribers.remove(&id)?;
        }
        for name in channel_broadcasts {
            let channel = channels.load_entry(name.to_string()).await?;
            for recipient in channel.subscribers.indices().await? {
                let outbox = channel.outboxes.load_entry(recipient).await?;
                outbox.schedule_message(height).await?;
            }
            channel.block_height.set(Some(height));
        }
        for (name, id) in application.subscribe {
            let channel = channels.load_entry(name.to_string()).await?;
            // Add subscriber.
            if channel.subscribers.get(&id).await?.is_none() {
                // Send the latest message if any.
                if let Some(latest_height) = channel.block_height.get() {
                    let outbox = channel.outboxes.load_entry(id).await?;
                    outbox.schedule_message(*latest_height).await?;
                }
            }
            channel.subscribers.insert(&id, ())?;
        }
        Ok(())
    }
}

impl<C> OutboxStateView<C>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
{
    /// Schedule a message at the given height if we haven't already.
    pub async fn schedule_message(&mut self, height: BlockHeight) -> Result<(), ChainError> {
        let last_value = self.queue.back().await?;
        if last_value != Some(height) {
            assert!(
                last_value < Some(height),
                "Trying to schedule height {} after a message at height {}",
                height,
                last_value.unwrap()
            );
            self.queue.push_back(height);
        }
        Ok(())
    }

    /// Mark all messages as received up to the given height.
    pub async fn mark_messages_as_received(
        &mut self,
        height: BlockHeight,
    ) -> Result<bool, ViewError> {
        let mut updated = false;
        while let Some(h) = self.queue.front().await? {
            if h > height {
                break;
            }
            self.queue.delete_front();
            updated = true;
        }
        Ok(updated)
    }
}

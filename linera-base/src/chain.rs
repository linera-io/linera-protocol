// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    committee::Committee,
    crypto::*,
    ensure,
    error::Error,
    execution::{
        ApplicationResult, EffectContext, ExecutionState, OperationContext, RawApplicationResult,
        SYSTEM,
    },
    manager::ChainManager,
    messages::*,
    system::Balance,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};

/// The state of a chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct ChainState {
    /// The ID of the chain.
    chain_id: ChainId,

    /// Execution state.
    pub state: ExecutionState,
    /// Hash of execution state + the state of all contract states
    pub state_hash: HashValue,

    /// Hash of the latest certified block in this chain, if any.
    pub block_hash: Option<HashValue>,
    /// Sequence number tracking blocks.
    pub next_block_height: BlockHeight,

    /// Hashes of all certified blocks for this sender.
    /// This ends with `block_hash` and has length `usize::from(next_block_height)`.
    pub confirmed_log: Vec<HashValue>,
    /// Hashes of all certified blocks known as a receiver (local ordering).
    pub received_log: Vec<HashValue>,

    /// Communication state of applications.
    pub communication_states: HashMap<ApplicationId, CommunicationState>,
}

/// Communication state of an application.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct CommunicationState {
    /// Mailboxes used to receive messages indexed by their origin.
    pub inboxes: HashMap<Origin, InboxState>,
    /// Mailboxes used to send messages, indexed by recipient.
    pub outboxes: HashMap<ChainId, OutboxState>,
    /// Channels able to multicast messages to subscribers.
    pub channels: HashMap<String, ChannelState>,
}

/// An outbox used to send messages to another chain. NOTE: Messages are implied by the
/// execution of blocks, so currently we just send the certified blocks over and let the
/// receivers figure out what was the message for them.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct OutboxState {
    /// Keep sending these certified blocks of ours until they are acknowledged by
    /// receivers.
    pub queue: VecDeque<BlockHeight>,
}

/// An inbox used to receive and execute messages from another chain.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct InboxState {
    /// We have already received the cross-chain requests and enqueued all the messages
    /// below this height.
    pub next_height_to_receive: BlockHeight,
    /// These events have been received but not yet picked by a block to be executed.
    pub received_events: VecDeque<Event>,
    /// These events have been executed but the cross-chain requests have not been
    /// received yet.
    pub expected_events: VecDeque<Event>,
}

/// The state of a channel followed by subscribers.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct ChannelState {
    /// The current subscribers.
    pub subscribers: HashMap<ChainId, ()>,
    /// The messages waiting to be delivered to present and past subscribers.
    pub outboxes: HashMap<ChainId, OutboxState>,
    /// The latest block height, if any, to be sent to future subscribers.
    pub block_height: Option<BlockHeight>,
}

/// A message sent by some (unspecified) chain at a particular height and index.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct Event {
    /// The height of the block that created the event.
    pub height: BlockHeight,
    /// The index of the effect.
    pub index: usize,
    /// The effect of the event.
    pub effect: Effect,
}

impl ChainState {
    pub fn new(chain_id: ChainId) -> Self {
        let state = ExecutionState::default();
        let state_hash = HashValue::new(&state);
        Self {
            chain_id,
            state,
            state_hash,
            block_hash: None,
            next_block_height: BlockHeight::default(),
            confirmed_log: Vec::new(),
            received_log: Vec::new(),
            communication_states: HashMap::new(),
        }
    }

    pub fn create(
        committee: Committee,
        admin_id: ChainId,
        description: ChainDescription,
        owner: Owner,
        balance: Balance,
    ) -> Self {
        let mut chain = Self::new(description.into());
        chain.state.system.description = Some(description);
        chain.state.system.epoch = Some(Epoch::from(0));
        chain.state.system.admin_id = Some(admin_id);
        chain
            .state
            .system
            .committees
            .insert(Epoch::from(0), committee);
        chain.state.system.manager = ChainManager::single(owner);
        chain.state.system.balance = balance;
        chain.state_hash = HashValue::new(&chain.state);
        assert!(chain.is_active());
        chain
    }

    pub fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    fn mark_messages_as_received(
        outboxes: &mut HashMap<ChainId, OutboxState>,
        application_id: ApplicationId,
        origin: &Origin,
        recipient: ChainId,
        height: BlockHeight,
    ) -> bool {
        let mut updated = false;
        match outboxes.entry(recipient) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                updated = entry.get_mut().mark_messages_as_received(height);
                if updated && entry.get().queue.is_empty() {
                    entry.remove();
                }
            }
            _ => {
                log::warn!(
                    "All messages were already marked as received in the outbox {:?}::{:?} to {:?}",
                    application_id,
                    origin,
                    recipient
                );
            }
        }
        updated
    }

    pub fn mark_outbox_messages_as_received(
        &mut self,
        application_id: ApplicationId,
        recipient: ChainId,
        height: BlockHeight,
    ) -> bool {
        let origin = Origin {
            chain_id: self.chain_id(),
            medium: Medium::Direct,
        };
        let communication_state = self
            .communication_states
            .get_mut(&application_id)
            .expect("Application should exist already");
        Self::mark_messages_as_received(
            &mut communication_state.outboxes,
            application_id,
            &origin,
            recipient,
            height,
        )
    }

    pub fn mark_channel_messages_as_received(
        &mut self,
        name: &str,
        application_id: ApplicationId,
        recipient: ChainId,
        height: BlockHeight,
    ) -> bool {
        let origin = Origin {
            chain_id: self.chain_id(),
            medium: Medium::Channel(name.to_string()),
        };
        let communication_state = self
            .communication_states
            .get_mut(&application_id)
            .expect("Application should exist already");
        let channel = communication_state
            .channels
            .get_mut(name)
            .expect("Channel should exist already");
        Self::mark_messages_as_received(
            &mut channel.outboxes,
            application_id,
            &origin,
            recipient,
            height,
        )
    }

    /// Invariant for the states of active chains.
    pub fn is_active(&self) -> bool {
        self.state.system.is_active()
    }

    pub fn make_chain_info(&self, key_pair: Option<&KeyPair>) -> ChainInfoResponse {
        let info = ChainInfo {
            chain_id: self.chain_id,
            epoch: self.state.system.epoch,
            description: self.state.system.description,
            manager: self.state.system.manager.clone(),
            system_balance: self.state.system.balance,
            block_hash: self.block_hash,
            next_block_height: self.next_block_height,
            state_hash: Some(self.state_hash),
            requested_system_execution_state: None,
            requested_pending_messages: Vec::new(),
            requested_sent_certificates: Vec::new(),
            count_received_certificates: self.received_log.len(),
            requested_received_certificates: Vec::new(),
        };
        ChainInfoResponse::new(info, key_pair)
    }

    /// Verify that this chain is up-to-date and all the messages executed ahead of time
    /// have been properly received by now.
    pub fn validate_incoming_messages(&self) -> Result<(), Error> {
        for (app_id, state) in &self.communication_states {
            for (origin, inbox) in &state.inboxes {
                ensure!(
                    inbox.expected_events.is_empty(),
                    Error::MissingCrossChainUpdate {
                        application_id: *app_id,
                        origin: origin.clone(),
                        height: inbox.expected_events.front().unwrap().height,
                    }
                );
            }
        }
        Ok(())
    }

    /// Schedule operations to be executed as a recipient, unless this block was already
    /// processed. Returns true if the call changed the chain state. Operations must be
    /// received by order of heights and indices.
    pub fn receive_block(
        &mut self,
        application_id: ApplicationId,
        origin: &Origin,
        height: BlockHeight,
        effects: Vec<(ApplicationId, Destination, Effect)>,
        key: HashValue,
    ) -> Result<bool, Error> {
        let communication_state = self.communication_states.entry(application_id).or_default();
        let inbox = communication_state
            .inboxes
            .entry(origin.clone())
            .or_default();
        if height < inbox.next_height_to_receive {
            // We have already received this block.
            log::warn!(
                "Ignoring repeated messages to {:?} from {:?} at height {}",
                self.chain_id,
                origin,
                height
            );
            return Ok(false);
        }
        log::trace!(
            "Processing new messages to {:?} from {:?} at height {}",
            self.chain_id,
            origin,
            height
        );
        // Mark the block as received.
        inbox.next_height_to_receive = height.try_add_one()?;
        self.received_log.push(key);

        let mut was_a_recipient = false;
        for (index, (app_id, destination, effect)) in effects.into_iter().enumerate() {
            // Skip events that do not belong to this origin OR have no effect on this
            // recipient.
            match destination {
                Destination::Recipient(id) => {
                    if origin.medium != Medium::Direct || id != self.chain_id {
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
            if app_id == SYSTEM {
                let effect_id = EffectId {
                    chain_id: origin.chain_id,
                    height,
                    index,
                };
                // Handle special effects to be executed immediately.
                if self
                    .state
                    .system
                    .apply_immediate_effect(self.chain_id, effect_id, &effect)?
                {
                    self.state_hash = HashValue::new(&self.state);
                }
            }
            // Find if the message was executed ahead of time.
            match inbox.expected_events.front() {
                Some(event) => {
                    if height == event.height && index == event.index {
                        // We already executed this message by anticipation. Remove it from the queue.
                        assert_eq!(effect, event.effect, "Unexpected effect in certified block");
                        inbox.expected_events.pop_front();
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
            self.chain_id, origin, height
        );
        Ok(true)
    }

    /// Verify that the incoming_messages are in the right order. This matters for inbox
    /// invariants, notably the fact that inbox.expected_events is sorted.
    fn check_incoming_messages(&self, messages: &[MessageGroup]) -> Result<(), Error> {
        let mut next_messages: HashMap<(ApplicationId, Origin), (BlockHeight, usize)> =
            HashMap::new();
        for message_group in messages {
            let next_message = next_messages
                .entry((message_group.application_id, message_group.origin.clone()))
                .or_default();
            for (message_index, _) in &message_group.effects {
                ensure!(
                    (message_group.height, *message_index) >= *next_message,
                    Error::InvalidMessageOrder {
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
    pub fn execute_block(
        &mut self,
        block: &Block,
    ) -> Result<Vec<(ApplicationId, Destination, Effect)>, Error> {
        assert_eq!(block.chain_id, self.chain_id);
        let mut effects = Vec::new();
        // First, process incoming messages.
        self.check_incoming_messages(&block.incoming_messages)?;

        for message_group in &block.incoming_messages {
            let communication_state = self
                .communication_states
                .entry(message_group.application_id)
                .or_default();
            let inbox = communication_state
                .inboxes
                .entry(message_group.origin.clone())
                .or_default();
            log::trace!(
                "Updating inbox {:?}::{:?} in chain {:?}",
                message_group.application_id,
                message_group.origin,
                self.chain_id
            );
            for (message_index, message_effect) in &message_group.effects {
                // Receivers are allowed to skip events from the received queue.
                while let Some(Event {
                    height,
                    index,
                    effect: _,
                }) = inbox.received_events.front()
                {
                    if *height > message_group.height
                        || (*height == message_group.height && index >= message_index)
                    {
                        break;
                    }
                    assert!((*height, index) < (message_group.height, message_index));
                    let event = inbox.received_events.pop_front().unwrap();
                    log::trace!("Skipping received event {:?}", event);
                }
                // Reconcile the event with the received queue, or mark it as "expected".
                match inbox.received_events.front() {
                    Some(Event {
                        height,
                        index,
                        effect,
                    }) => {
                        ensure!(
                            message_group.height == *height && message_index == index,
                            Error::InvalidMessage {
                                application_id: message_group.application_id,
                                origin: message_group.origin.clone(),
                                height: message_group.height,
                                index: *message_index,
                                expected_height: *height,
                                expected_index: *index,
                            }
                        );
                        ensure!(
                            message_effect == effect,
                            Error::InvalidMessageContent {
                                application_id: message_group.application_id,
                                origin: message_group.origin.clone(),
                                height: message_group.height,
                                index: *message_index,
                            }
                        );
                        let event = inbox.received_events.pop_front().unwrap();
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
                    chain_id: self.chain_id,
                    height: block.height,
                    effect_id: EffectId {
                        chain_id: message_group.origin.chain_id,
                        height: message_group.height,
                        index: *message_index,
                    },
                };
                let result = self.state.apply_effect(
                    message_group.application_id,
                    &context,
                    message_effect,
                )?;
                Self::process_application_result(
                    message_group.application_id,
                    &mut communication_state.outboxes,
                    &mut communication_state.channels,
                    &mut effects,
                    context.height,
                    result,
                );
            }
        }
        // Second, execute the operations in the block and remember the recipients to notify.
        for (index, (application_id, operation)) in block.operations.iter().enumerate() {
            let communication_state = self
                .communication_states
                .entry(*application_id)
                .or_default();
            let context = OperationContext {
                chain_id: self.chain_id,
                height: block.height,
                index,
            };
            let result = self
                .state
                .apply_operation(*application_id, &context, operation)?;
            Self::process_application_result(
                *application_id,
                &mut communication_state.outboxes,
                &mut communication_state.channels,
                &mut effects,
                context.height,
                result,
            );
        }
        // Last, recompute the state hash.
        self.state_hash = HashValue::new(&self.state);
        Ok(effects)
    }

    fn process_application_result(
        application_id: ApplicationId,
        outboxes: &mut HashMap<ChainId, OutboxState>,
        channels: &mut HashMap<String, ChannelState>,
        effects: &mut Vec<(ApplicationId, Destination, Effect)>,
        height: BlockHeight,
        application: ApplicationResult,
    ) {
        match application {
            ApplicationResult::System(raw) => Self::process_raw_application_result(
                application_id,
                outboxes,
                channels,
                effects,
                height,
                raw,
            ),
            ApplicationResult::User(raw) => Self::process_raw_application_result(
                application_id,
                outboxes,
                channels,
                effects,
                height,
                raw,
            ),
        }
    }

    fn process_raw_application_result<E: Into<Effect>>(
        application_id: ApplicationId,
        outboxes: &mut HashMap<ChainId, OutboxState>,
        channels: &mut HashMap<String, ChannelState>,
        effects: &mut Vec<(ApplicationId, Destination, Effect)>,
        height: BlockHeight,
        application: RawApplicationResult<E>,
    ) {
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
            let outbox = outboxes.entry(recipient).or_default();
            outbox.schedule_message(height);
        }
        // Update the channels.
        if let Some((name, id)) = application.unsubscribe {
            let channel = channels.entry(name).or_insert_with(ChannelState::default);
            // Remove subscriber. Do not remove the channel outbox yet.
            channel.subscribers.remove(&id);
        }
        for name in channel_broadcasts {
            let channel = channels.entry(name).or_insert_with(ChannelState::default);
            for recipient in channel.subscribers.keys() {
                let outbox = channel.outboxes.entry(*recipient).or_default();
                outbox.schedule_message(height);
            }
            channel.block_height = Some(height);
        }
        if let Some((name, id)) = application.subscribe {
            let channel = channels.entry(name).or_insert_with(ChannelState::default);
            // Add subscriber.
            if channel.subscribers.insert(id, ()).is_none() {
                // Send the latest message if any.
                if let Some(latest_height) = channel.block_height {
                    let outbox = channel.outboxes.entry(id).or_default();
                    outbox.schedule_message(latest_height);
                }
            }
        }
    }
}

impl OutboxState {
    /// Schedule a message at the given height if we haven't already.
    pub fn schedule_message(&mut self, height: BlockHeight) {
        if self.queue.back() != Some(&height) {
            assert!(
                self.queue.back() < Some(&height),
                "Trying to schedule height {} after a message at height {}",
                height,
                self.queue.back().unwrap()
            );
            self.queue.push_back(height);
        }
    }

    /// Mark all messages as received up to the given height.
    pub fn mark_messages_as_received(&mut self, height: BlockHeight) -> bool {
        let mut updated = false;
        while let Some(h) = self.queue.front() {
            if *h > height {
                break;
            }
            self.queue.pop_front().unwrap();
            updated = true;
        }
        updated
    }
}

// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    crypto::*,
    ensure,
    error::Error,
    execution::{ApplicationResult, Effect, ExecutionState, ADMIN_CHANNEL},
    manager::BlockManager,
    messages::*,
};
use getset::{CopyGetters, Getters};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    ops::{Deref, DerefMut},
};
use tokio::sync::OwnedMutexGuard;

/// The state of a chain as a serializable value.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct ChainState {
    /// How the chain was created. May be unknown for inactive chains.
    pub description: Option<ChainDescription>,
    /// Execution state.
    pub state: ExecutionState,
    /// Hash of the execution state.
    pub state_hash: HashValue,
    /// Hash of the latest certified block in this chain, if any.
    pub block_hash: Option<HashValue>,
    /// Sequence number tracking blocks.
    pub next_block_height: BlockHeight,

    /// Hashes of all certified blocks (aka "key") for this sender.
    /// This ends with `block_hash` and has length `usize::from(next_block_height)`.
    pub confirmed_keys: Vec<HashValue>,
    /// Hashes of all certified blocks known as a receiver (local ordering).
    pub received_keys: Vec<HashValue>,

    /// Mailboxes used to send messages, indexed by recipient.
    pub outboxes: HashMap<ChainId, OutboxState>,
    /// Mailboxes used to receive messages indexed by their origin.
    pub inboxes: HashMap<Origin, InboxState>,
    /// Channels able to multicast messages to subscribers.
    pub channels: HashMap<String, ChannelState>,
}

/// A structure describing updates to the state of a chain.
#[derive(Debug, CopyGetters, Getters)]
pub struct ChainView<T> {
    /// A lock on the original data underlying the view.
    base: T,

    /// How the chain was created. May be unknown for inactive chains.
    #[getset(get = "pub")]
    description: Option<ChainDescription>,
    /// Execution state.
    #[getset(get = "pub")]
    state: ExecutionState,
    /// Hash of the execution state.
    #[getset(get_copy = "pub")]
    state_hash: HashValue,
    /// Hash of the latest certified block in this chain, if any.
    #[getset(get_copy = "pub")]
    block_hash: Option<HashValue>,
    /// Sequence number tracking blocks.
    #[getset(get_copy = "pub")]
    next_block_height: BlockHeight,

    /// Hashes of all certified blocks (aka "key") for this sender.
    /// This ends with `block_hash` and has length `usize::from(next_block_height)`.
    confirmed_keys: Vec<HashValue>,
    /// Hashes of all certified blocks known as a receiver (local ordering).
    received_keys: Vec<HashValue>,

    /// Mailboxes used to send messages, indexed by recipient.
    #[getset(get = "pub")]
    outboxes: HashMap<ChainId, OutboxState>,
    /// Mailboxes used to receive messages indexed by their origin.
    #[getset(get = "pub")]
    inboxes: HashMap<Origin, InboxState>,
    /// Channels able to multicast messages to subscribers.
    #[getset(get = "pub")]
    channels: HashMap<String, ChannelState>,

    /// Whether the state has been modified since the last read.
    #[getset(get = "pub")]
    modified: bool,
}

/// An outbox used to send messages to another chain.
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
    pub fn new(id: ChainId) -> Self {
        let state = ExecutionState::new(id);
        let state_hash = HashValue::new(&state);
        Self {
            description: None,
            state,
            state_hash,
            block_hash: None,
            next_block_height: BlockHeight::default(),
            confirmed_keys: Vec::new(),
            received_keys: Vec::new(),
            inboxes: HashMap::new(),
            outboxes: HashMap::new(),
            channels: HashMap::new(),
        }
    }

    pub fn chain_id(&self) -> ChainId {
        self.state.chain_id
    }
}

impl From<ChainState> for ChainView<ChainState> {
    fn from(base: ChainState) -> Self {
        let value = &base;
        let description = value.description;
        let state = value.state.clone();
        let state_hash = value.state_hash;
        let block_hash = value.block_hash;
        let next_block_height = value.next_block_height;
        let confirmed_keys = value.confirmed_keys.clone();
        let received_keys = value.received_keys.clone();
        let inboxes = value.inboxes.clone();
        let outboxes = value.outboxes.clone();
        let channels = value.channels.clone();

        Self {
            base,
            description,
            state,
            state_hash,
            block_hash,
            next_block_height,
            confirmed_keys,
            received_keys,
            inboxes,
            outboxes,
            channels,
            modified: false,
        }
    }
}

impl From<OwnedMutexGuard<ChainState>> for ChainView<OwnedMutexGuard<ChainState>> {
    fn from(base: OwnedMutexGuard<ChainState>) -> Self {
        let value = base.deref();
        let description = value.description;
        let state = value.state.clone();
        let state_hash = value.state_hash;
        let block_hash = value.block_hash;
        let next_block_height = value.next_block_height;
        let confirmed_keys = value.confirmed_keys.clone();
        let received_keys = value.received_keys.clone();
        let inboxes = value.inboxes.clone();
        let outboxes = value.outboxes.clone();
        let channels = value.channels.clone();

        Self {
            base,
            description,
            state,
            state_hash,
            block_hash,
            next_block_height,
            confirmed_keys,
            received_keys,
            inboxes,
            outboxes,
            channels,
            modified: false,
        }
    }
}

impl ChainView<ChainState> {
    #[cfg(any(test, feature = "test"))]
    pub fn chain_state(&self) -> &ChainState {
        &self.base
    }

    pub fn reset(&mut self) {
        if self.modified {
            let value = &self.base;
            self.description = value.description;
            self.state = value.state.clone();
            self.state_hash = value.state_hash;
            self.block_hash = value.block_hash;
            self.next_block_height = value.next_block_height;
            self.confirmed_keys = value.confirmed_keys.clone();
            self.received_keys = value.received_keys.clone();
            self.inboxes = value.inboxes.clone();
            self.outboxes = value.outboxes.clone();
            self.channels = value.channels.clone();
        }
    }

    pub fn save(mut self) -> ChainState {
        if self.modified {
            let value = &mut self.base;
            value.description = self.description;
            value.state = self.state;
            value.state_hash = self.state_hash;
            value.block_hash = self.block_hash;
            value.next_block_height = self.next_block_height;
            value.confirmed_keys = self.confirmed_keys;
            value.received_keys = self.received_keys;
            value.inboxes = self.inboxes;
            value.outboxes = self.outboxes;
            value.channels = self.channels;
        }
        self.base
    }
}

impl ChainView<OwnedMutexGuard<ChainState>> {
    #[cfg(any(test, feature = "test"))]
    pub fn chain_state(&self) -> &ChainState {
        self.base.deref()
    }

    pub fn reset(&mut self) {
        if self.modified {
            let value = self.base.deref();
            self.description = value.description;
            self.state = value.state.clone();
            self.state_hash = value.state_hash;
            self.block_hash = value.block_hash;
            self.next_block_height = value.next_block_height;
            self.confirmed_keys = value.confirmed_keys.clone();
            self.received_keys = value.received_keys.clone();
            self.inboxes = value.inboxes.clone();
            self.outboxes = value.outboxes.clone();
            self.channels = value.channels.clone();
        }
    }

    pub fn save(mut self) -> OwnedMutexGuard<ChainState> {
        if self.modified {
            let value = self.base.deref_mut();
            value.description = self.description;
            value.state = self.state;
            value.state_hash = self.state_hash;
            value.block_hash = self.block_hash;
            value.next_block_height = self.next_block_height;
            value.confirmed_keys = self.confirmed_keys;
            value.received_keys = self.received_keys;
            value.inboxes = self.inboxes;
            value.outboxes = self.outboxes;
            value.channels = self.channels;
        }
        self.base
    }
}

impl<T> ChainView<T> {
    pub fn chain_id(&self) -> ChainId {
        self.state.chain_id
    }

    pub fn description_mut(&mut self) -> &mut Option<ChainDescription> {
        self.modified = true;
        &mut self.description
    }

    pub fn state_mut(&mut self) -> &mut ExecutionState {
        self.modified = true;
        &mut self.state
    }

    pub fn state_hash_mut(&mut self) -> &mut HashValue {
        self.modified = true;
        &mut self.state_hash
    }

    pub fn block_hash_mut(&mut self) -> &mut Option<HashValue> {
        self.modified = true;
        &mut self.block_hash
    }

    pub fn next_block_height_mut(&mut self) -> &mut BlockHeight {
        self.modified = true;
        &mut self.next_block_height
    }

    pub fn confirmed_key(&self, index: usize) -> Option<HashValue> {
        self.confirmed_keys.get(index).copied()
    }

    pub fn confirmed_keys<R: std::slice::SliceIndex<[HashValue]>>(&self, range: R) -> &R::Output {
        &self.confirmed_keys[range]
    }

    pub fn add_confirmed_key(&mut self, key: HashValue) {
        self.modified = true;
        self.confirmed_keys.push(key)
    }

    pub fn received_key(&self, index: usize) -> Option<HashValue> {
        self.received_keys.get(index).copied()
    }

    pub fn received_keys<R: std::slice::SliceIndex<[HashValue]>>(&self, range: R) -> &R::Output {
        &self.received_keys[range]
    }

    pub fn add_received_key(&mut self, key: HashValue) {
        self.modified = true;
        self.received_keys.push(key)
    }

    fn mark_messages_as_received(
        outboxes: &mut HashMap<ChainId, OutboxState>,
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
                    "All messages were already marked as received in the outbox {:?} to {:?}",
                    origin,
                    recipient
                );
            }
        }
        updated
    }

    pub fn mark_outbox_messages_as_received(
        &mut self,
        recipient: ChainId,
        height: BlockHeight,
    ) -> bool {
        let origin = Origin::Chain(self.chain_id());
        let updated =
            Self::mark_messages_as_received(&mut self.outboxes, &origin, recipient, height);
        if updated {
            self.modified = true;
        }
        updated
    }

    pub fn mark_channel_messages_as_received(
        &mut self,
        name: &str,
        recipient: ChainId,
        height: BlockHeight,
    ) -> bool {
        let origin = Origin::Channel(ChannelId {
            chain_id: self.chain_id(),
            name: name.to_string(),
        });
        let channel = match self.channels.get_mut(name) {
            Some(channel) => channel,
            None => {
                panic!(
                    "Trying to mark messages as received for a non-existing channel {:?} {} to {:?} at height {}",
                    self.chain_id(),
                    name,
                    recipient,
                    height,
                );
            }
        };
        let updated =
            Self::mark_messages_as_received(&mut channel.outboxes, &origin, recipient, height);
        if updated {
            self.modified = true;
        }
        updated
    }

    /// Invariant for the states of active chains.
    pub fn is_active(&self) -> bool {
        self.description.is_some()
            && self.state.manager.is_active()
            && self.state.epoch.is_some()
            && self
                .state
                .committees
                .contains_key(self.state.epoch.as_ref().unwrap())
            && self.state.admin_id.is_some()
    }

    pub fn make_chain_info(&self, key_pair: Option<&KeyPair>) -> ChainInfoResponse {
        let info = ChainInfo {
            chain_id: self.state.chain_id,
            epoch: self.state.epoch,
            description: self.description,
            manager: self.state.manager.clone(),
            balance: self.state.balance,
            block_hash: self.block_hash,
            next_block_height: self.next_block_height,
            state_hash: self.state_hash,
            requested_execution_state: None,
            requested_pending_messages: Vec::new(),
            requested_sent_certificates: Vec::new(),
            count_received_certificates: self.received_keys.len(),
            requested_received_certificates: Vec::new(),
        };
        ChainInfoResponse::new(info, key_pair)
    }

    /// Verify that this chain is up-to-date and all the messages executed ahead of time
    /// have been properly received by now.
    pub fn validate_incoming_messages(&self) -> Result<(), Error> {
        for (origin, inbox) in &self.inboxes {
            ensure!(
                inbox.expected_events.is_empty(),
                Error::MissingCrossChainUpdate {
                    origin: origin.clone(),
                    height: inbox.expected_events.front().unwrap().height,
                }
            );
        }
        Ok(())
    }

    /// Schedule operations to be executed as a recipient, unless this block was already
    /// processed. Returns true if the call changed the chain state. Operations must be
    /// received by order of heights and indices.
    pub fn receive_block(
        &mut self,
        origin: &Origin,
        height: BlockHeight,
        effects: Vec<Effect>,
        key: HashValue,
    ) -> Result<bool, Error> {
        let inbox = self.inboxes.entry(origin.clone()).or_default();
        if height < inbox.next_height_to_receive {
            // We have already received this block.
            log::warn!(
                "Ignoring repeated messages to {:?} from {:?} at height {}",
                self.state.chain_id,
                origin,
                height
            );
            return Ok(false);
        }
        log::trace!(
            "Processing new messages to {:?} from {:?} at height {}",
            self.state.chain_id,
            origin,
            height
        );
        // Mark the block as received.
        inbox.next_height_to_receive = height.try_add_one()?;
        self.modified = true;
        self.received_keys.push(key);

        let mut was_a_recipient = false;
        for (index, effect) in effects.into_iter().enumerate() {
            // Skip events that do not belong to this origin OR have no effect on this
            // recipient.
            if !self.state.is_recipient(origin, &effect) {
                continue;
            }
            was_a_recipient = true;
            // Chain creation effects are special and executed (only) in this callback.
            // For simplicity, they will still appear in the received messages.
            match &effect {
                Effect::OpenChain {
                    id,
                    owner,
                    epoch,
                    committees,
                    admin_id,
                } if id == &self.state.chain_id => {
                    // Guaranteed under BFT assumptions.
                    assert!(self.description.is_none());
                    assert!(!self.state.manager.is_active());
                    assert!(self.state.committees.is_empty());
                    let description = ChainDescription::Child(EffectId {
                        chain_id: origin.sender(),
                        height,
                        index,
                    });
                    assert_eq!(self.state.chain_id, description.into());
                    self.description = Some(description);
                    self.state.epoch = Some(*epoch);
                    self.state.committees = committees.clone();
                    self.state.admin_id = Some(*admin_id);
                    self.state.subscriptions.insert(
                        ChannelId {
                            chain_id: *admin_id,
                            name: ADMIN_CHANNEL.into(),
                        },
                        (),
                    );
                    self.state.manager = BlockManager::single(*owner);
                    self.state_hash = HashValue::new(&self.state);
                }
                _ => (),
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
            self.state.chain_id, origin, height
        );
        Ok(true)
    }

    /// Verify that the incoming_messages are in the right order. This matters for inbox
    /// invariants, notably the fact that inbox.expected_events is sorted.
    fn check_incoming_messages(&self, messages: &[MessageGroup]) -> Result<(), Error> {
        let mut next_messages: HashMap<Origin, (BlockHeight, usize)> = HashMap::new();
        for message_group in messages {
            let next_message = next_messages
                .entry(message_group.origin.clone())
                .or_default();
            for (message_index, _) in &message_group.effects {
                ensure!(
                    (message_group.height, *message_index) >= *next_message,
                    Error::InvalidMessageOrder {
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
    pub fn execute_block(&mut self, block: &Block) -> Result<Vec<Effect>, Error> {
        assert_eq!(block.chain_id, self.state.chain_id);
        let mut effects = Vec::new();
        // First, process incoming messages.
        self.check_incoming_messages(&block.incoming_messages)?;

        for message_group in &block.incoming_messages {
            let inbox = self
                .inboxes
                .entry(message_group.origin.clone())
                .or_default();
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
                    log::trace!(
                        "Chain {:?} is skipping received event from {:?}: {:?}",
                        self.state.chain_id,
                        message_group.origin,
                        event
                    );
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
                                origin: message_group.origin.clone(),
                                height: message_group.height,
                                index: *message_index,
                            }
                        );
                        inbox.received_events.pop_front().unwrap();
                    }
                    None => {
                        inbox.expected_events.push_back(Event {
                            height: message_group.height,
                            index: *message_index,
                            effect: message_effect.clone(),
                        });
                    }
                }
                // Execute the received effect.
                let application = self.state.apply_effect(message_effect)?;
                Self::process_application_result(
                    &mut self.outboxes,
                    &mut self.channels,
                    &mut effects,
                    block.height,
                    application,
                );
            }
        }
        // Second, execute the operations in the block and remember the recipients to notify.
        for (index, operation) in block.operations.iter().enumerate() {
            let application = self.state.apply_operation(block.height, index, operation)?;
            Self::process_application_result(
                &mut self.outboxes,
                &mut self.channels,
                &mut effects,
                block.height,
                application,
            );
        }
        // Last, recompute the state hash.
        self.state_hash = HashValue::new(&self.state);
        self.modified = true;
        Ok(effects)
    }

    fn process_application_result(
        outboxes: &mut HashMap<ChainId, OutboxState>,
        channels: &mut HashMap<String, ChannelState>,
        effects: &mut Vec<Effect>,
        height: BlockHeight,
        application: ApplicationResult,
    ) {
        // Record the effects of the execution.
        effects.extend(application.effects);
        // Update the (regular) outboxes.
        for recipient in application.recipients {
            let outbox = outboxes.entry(recipient).or_default();
            outbox.schedule_message(height);
        }
        // Update the channels.
        if let Some((name, id)) = application.unsubscribe {
            let channel = channels.entry(name).or_insert_with(ChannelState::default);
            // Remove subscriber. Do not remove the channel outbox yet.
            channel.subscribers.remove(&id);
        }
        for name in application.need_channel_broadcast {
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

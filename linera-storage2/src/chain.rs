// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use linera_base::{
    crypto::HashValue,
    execution::ExecutionState,
    messages::{ApplicationId, BlockHeight, ChainId, Effect, Origin},
};
use linera_views::{
    hash::{HashView, Hasher, HashingContext},
    impl_view,
    views::{
        AppendOnlyLogOperations, AppendOnlyLogView, CollectionOperations, CollectionView, Context,
        MapOperations, MapView, QueueOperations, QueueView, RegisterOperations, RegisterView,
        ScopedOperations, ScopedView, View,
    },
};
use serde::{Deserialize, Serialize};

/// A view accessing the state of a chain.
#[derive(Debug)]
pub struct ChainStateView<C> {
    /// Execution state, including system and user applications.
    pub execution_state: ScopedView<0, RegisterView<C, ExecutionState>>,
    /// Hash of the execution state.
    pub execution_state_hash: ScopedView<1, RegisterView<C, Option<HashValue>>>,

    /// Block-chaining state.
    pub chaining_state: ScopedView<2, RegisterView<C, ChainingState>>,

    /// Hashes of all certified blocks for this sender.
    /// This ends with `block_hash` and has length `usize::from(next_block_height)`.
    pub confirmed_log: ScopedView<3, AppendOnlyLogView<C, HashValue>>,
    /// Hashes of all certified blocks known as a receiver (local ordering).
    pub received_log: ScopedView<4, AppendOnlyLogView<C, HashValue>>,

    /// Communication state of applications.
    pub communication_states:
        ScopedView<5, CollectionView<C, ApplicationId, CommunicationStateView<C>>>,
}

impl_view!(
    ChainStateView{ execution_state, execution_state_hash, chaining_state, confirmed_log, received_log, communication_states};
    RegisterOperations<ExecutionState>,
    RegisterOperations<Option<HashValue>>,
    RegisterOperations<ChainingState>,
    AppendOnlyLogOperations<HashValue>,
    CollectionOperations<ApplicationId>,
    // from OutboxStateView
    QueueOperations<BlockHeight>,
    // from InboxStateView
    RegisterOperations<BlockHeight>,
    QueueOperations<Event>,
    // from ChannelStateView
    MapOperations<ChainId, ()>,
    CollectionOperations<ChainId>,
    RegisterOperations<Option<BlockHeight>>,
    // from CommunicationStateView
    CollectionOperations<Origin>,
    CollectionOperations<ChainId>,
    CollectionOperations<String>
);

/// Block-chaining state.
#[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChainingState {
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

impl_view!(
    CommunicationStateView { inboxes, outboxes, channels };
    CollectionOperations<Origin>,
    CollectionOperations<ChainId>,
    CollectionOperations<String>,
    // from OutboxStateView
    QueueOperations<BlockHeight>,
    // from InboxStateView
    RegisterOperations<BlockHeight>,
    QueueOperations<Event>,
    // from ChannelStateView
    MapOperations<ChainId, ()>,
    CollectionOperations<ChainId>,
    RegisterOperations<Option<BlockHeight>>
);

/// An outbox used to send messages to another chain. NOTE: Messages are implied by the
/// execution of blocks, so currently we just send the certified blocks over and let the
/// receivers figure out what was the message for them.
#[derive(Debug)]
pub struct OutboxStateView<C> {
    /// Keep sending these certified blocks of ours until they are acknowledged by
    /// receivers.
    pub queue: ScopedView<0, QueueView<C, BlockHeight>>,
}

impl_view!(
    OutboxStateView { queue };
    QueueOperations<BlockHeight>
);

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

impl_view!(
    InboxStateView { next_height_to_receive, received_events, expected_events };
    RegisterOperations<BlockHeight>,
    QueueOperations<Event>
);

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

impl_view!(
    ChannelStateView { subscribers, outboxes, block_height };
    MapOperations<ChainId, ()>,
    CollectionOperations<ChainId>,
    RegisterOperations<Option<BlockHeight>>,
    // From OutboxStateView
    QueueOperations<BlockHeight>
);

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

// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::SimpleObject;
use linera_base::{
    data_types::{ArithmeticError, BlockHeight},
    ensure,
    identifiers::ChainId,
};
#[cfg(with_testing)]
use linera_views::memory::{MemoryContext, TEST_MEMORY_MAX_STREAM_QUERIES};
use linera_views::{
    common::Context,
    queue_view::QueueView,
    register_view::RegisterView,
    views::{ClonableView, View, ViewError},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{data_types::Event, ChainError, Origin};

#[cfg(test)]
#[path = "unit_tests/inbox_tests.rs"]
mod inbox_tests;

/// The state of a inbox.
/// * An inbox is used to track events received and executed locally.
/// * An `Event` consists of a logical cursor `(height, index)` and some message content `message`.
/// * On the surface, an inbox looks like a FIFO queue: the main APIs are `add_event` and
/// `remove_event`.
/// * However, events can also be removed before they are added. When this happens,
/// the events removed by anticipation are tracked in a separate queue. Any event added
/// later will be required to match the first removed event and so on.
/// * The cursors of added events (resp. removed events) must be increasing over time.
/// * Reconciliation of added and removed events is allowed to skip some added events.
/// However, the opposite is not true: every removed event must be eventually added.
#[derive(Debug, ClonableView, View, async_graphql::SimpleObject)]
pub struct InboxStateView<C>
where
    C: Clone + Context + Send + Sync,
    ViewError: From<C::Error>,
{
    /// We have already added all the messages below this height and index.
    pub next_cursor_to_add: RegisterView<C, Cursor>,
    /// We have already removed all the messages below this height and index.
    pub next_cursor_to_remove: RegisterView<C, Cursor>,
    /// These events have been added and are waiting to be removed.
    pub added_events: QueueView<C, Event>,
    /// These events have been removed by anticipation and are waiting to be added.
    /// At least one of `added_events` and `removed_events` should be empty.
    pub removed_events: QueueView<C, Event>,
}

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    Hash,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    SimpleObject,
)]
pub struct Cursor {
    height: BlockHeight,
    index: u32,
}

#[derive(Error, Debug)]
pub(crate) enum InboxError {
    #[error(transparent)]
    ViewError(#[from] ViewError),
    #[error(transparent)]
    ArithmeticError(#[from] ArithmeticError),
    #[error("Cannot reconcile {event:?} with {previous_event:?}")]
    UnexpectedEvent { event: Event, previous_event: Event },
    #[error("{event:?} is out of order. Block and height should be at least: {next_cursor:?}")]
    IncorrectOrder { event: Event, next_cursor: Cursor },
    #[error("{event:?} cannot be skipped: it must be received before the next messages from the same origin")]
    UnskippableEvent { event: Event },
}

impl From<&Event> for Cursor {
    #[inline]
    fn from(event: &Event) -> Self {
        Self {
            height: event.height,
            index: event.index,
        }
    }
}

impl Cursor {
    fn try_add_one(self) -> Result<Self, ArithmeticError> {
        let value = Self {
            height: self.height,
            index: self.index.checked_add(1).ok_or(ArithmeticError::Overflow)?,
        };
        Ok(value)
    }
}

impl From<(ChainId, Origin, InboxError)> for ChainError {
    fn from(value: (ChainId, Origin, InboxError)) -> Self {
        let (chain_id, origin, error) = value;
        match error {
            InboxError::ViewError(e) => ChainError::ViewError(e),
            InboxError::ArithmeticError(e) => ChainError::ArithmeticError(e),
            InboxError::UnexpectedEvent {
                event,
                previous_event,
            } => ChainError::UnexpectedMessage {
                chain_id,
                origin: origin.into(),
                event,
                previous_event,
            },
            InboxError::IncorrectOrder { event, next_cursor } => {
                ChainError::IncorrectMessageOrder {
                    chain_id,
                    origin: origin.into(),
                    event,
                    next_height: next_cursor.height,
                    next_index: next_cursor.index,
                }
            }
            InboxError::UnskippableEvent { event } => ChainError::CannotSkipMessage {
                chain_id,
                origin: origin.into(),
                event,
            },
        }
    }
}

impl<C> InboxStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
{
    /// Converts the internal cursor for added events into an externally-visible block height.
    /// This makes sense because the rest of the system always adds events one block at a time.
    pub fn next_block_height_to_receive(&self) -> Result<BlockHeight, ChainError> {
        let cursor = self.next_cursor_to_add.get();
        if cursor.index == 0 {
            Ok(cursor.height)
        } else {
            Ok(cursor.height.try_add_one()?)
        }
    }

    /// Consumes an event from the inbox.
    ///
    /// Returns `true` if the event was already known, i.e. it was present in `added_events`.
    pub(crate) async fn remove_event(&mut self, event: &Event) -> Result<bool, InboxError> {
        // Record the latest cursor.
        let cursor = Cursor::from(event);
        ensure!(
            cursor >= *self.next_cursor_to_remove.get(),
            InboxError::IncorrectOrder {
                event: event.clone(),
                next_cursor: *self.next_cursor_to_remove.get(),
            }
        );
        // Discard added events with lower cursors (if any).
        while let Some(previous_event) = self.added_events.front().await? {
            if Cursor::from(&previous_event) >= cursor {
                break;
            }
            ensure!(
                previous_event.is_skippable(),
                InboxError::UnskippableEvent {
                    event: previous_event
                }
            );
            self.added_events.delete_front();
            tracing::trace!("Skipping previously received event {:?}", previous_event);
        }
        // Reconcile the event with the next added event, or mark it as removed.
        let already_known = match self.added_events.front().await? {
            Some(previous_event) => {
                // Rationale: If the two cursors are equal, then the events should match.
                // Otherwise, at this point we know that `self.next_cursor_to_add >
                // Cursor::from(&previous_event) > cursor`. Notably, `event` will never be
                // added in the future. Therefore, we should fail instead of adding
                // it to `self.removed_events`.
                ensure!(
                    event == &previous_event,
                    InboxError::UnexpectedEvent {
                        previous_event,
                        event: event.clone(),
                    }
                );
                self.added_events.delete_front();
                tracing::trace!("Consuming event {:?}", event);
                true
            }
            None => {
                tracing::trace!("Marking event as expected: {:?}", event);
                self.removed_events.push_back(event.clone());
                false
            }
        };
        self.next_cursor_to_remove.set(cursor.try_add_one()?);
        Ok(already_known)
    }

    /// Pushes an event to the inbox. The verifications should not fail in production unless
    /// many validators are faulty.
    ///
    /// Returns `true` if the event was new, `false` if it was already in `removed_events`.
    pub(crate) async fn add_event(&mut self, event: Event) -> Result<bool, InboxError> {
        // Record the latest cursor.
        let cursor = Cursor::from(&event);
        ensure!(
            cursor >= *self.next_cursor_to_add.get(),
            InboxError::IncorrectOrder {
                event: event.clone(),
                next_cursor: *self.next_cursor_to_add.get(),
            }
        );
        // Find if the message was removed ahead of time.
        let newly_added = match self.removed_events.front().await? {
            Some(previous_event) => {
                if Cursor::from(&previous_event) == cursor {
                    // We already executed this message by anticipation. Remove it from
                    // the queue.
                    ensure!(
                        event == previous_event,
                        InboxError::UnexpectedEvent {
                            previous_event,
                            event,
                        }
                    );
                    self.removed_events.delete_front();
                } else {
                    // The receiver has already executed a later event from the same
                    // sender ahead of time so we should skip this one.
                    ensure!(
                        cursor < Cursor::from(&previous_event) && event.is_skippable(),
                        InboxError::UnexpectedEvent {
                            previous_event,
                            event,
                        }
                    );
                }
                false
            }
            None => {
                // Otherwise, schedule the message for execution.
                self.added_events.push_back(event);
                true
            }
        };
        self.next_cursor_to_add.set(cursor.try_add_one()?);
        Ok(newly_added)
    }
}

#[cfg(with_testing)]
impl InboxStateView<MemoryContext<()>>
where
    MemoryContext<()>: Context + Clone + Send + Sync + 'static,
    ViewError: From<<MemoryContext<()> as linera_views::common::Context>::Error>,
{
    pub async fn new() -> Self {
        let context = MemoryContext::new(TEST_MEMORY_MAX_STREAM_QUERIES, ());
        Self::load(context)
            .await
            .expect("Loading from memory should work")
    }
}

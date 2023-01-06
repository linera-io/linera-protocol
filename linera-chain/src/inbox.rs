// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{data_types::Event, ChainError, Origin};
use linera_base::{
    data_types::{ArithmeticError, BlockHeight, ChainId},
    ensure,
};
use linera_execution::ApplicationId;
use linera_views::{
    common::Context,
    queue_view::QueueView,
    register_view::RegisterView,
    views::{HashableContainerView, ViewError},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[cfg(test)]
#[path = "unit_tests/inbox_tests.rs"]
mod inbox_tests;

/// The state of a inbox.
/// * An inbox is used to track events received and executed locally.
/// * An `Event` consists of a logical cursor `(height, index)` and some message content `effect`.
/// * On the surface, an inbox looks like a FIFO queue: the main APIs are `add_event` and
/// `remove_event`.
/// * However, events can also be removed before they are added. When this happens,
/// the events removed by anticipation are tracked in a separate queue. Any event added
/// later will be required to match the first removed event and so on.
/// * The cursors of added events (resp. removed events) must be increasing over time.
/// * Reconciliation of added and removed events is allowed to skip some added events.
/// However, the opposite is not true: every removed event must be eventually added.
#[derive(Debug, HashableContainerView)]
pub struct InboxStateView<C> {
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

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Cursor {
    height: BlockHeight,
    index: usize,
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
            index: self
                .index
                .checked_add(1)
                .ok_or(ArithmeticError::SequenceOverflow)?,
        };
        Ok(value)
    }
}

impl From<(ChainId, ApplicationId, Origin, InboxError)> for ChainError {
    fn from(value: (ChainId, ApplicationId, Origin, InboxError)) -> Self {
        let (chain_id, application_id, origin, error) = value;
        match error {
            InboxError::ViewError(e) => ChainError::ViewError(e),
            InboxError::ArithmeticError(e) => ChainError::ArithmeticError(e),
            InboxError::UnexpectedEvent {
                event,
                previous_event,
            } => ChainError::UnexpectedMessage {
                chain_id,
                application_id,
                origin,
                event,
                previous_event,
            },
            InboxError::IncorrectOrder { event, next_cursor } => {
                ChainError::IncorrectMessageOrder {
                    chain_id,
                    application_id,
                    origin,
                    event,
                    next_height: next_cursor.height,
                    next_index: next_cursor.index,
                }
            }
        }
    }
}

impl<C> InboxStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
{
    /// Convert the internal cursor for added events into an externally-visible block height.
    /// This makes sense because the rest of the system always adds events one block at a time.
    pub fn next_block_height_to_receive(&self) -> Result<BlockHeight, ChainError> {
        let cursor = self.next_cursor_to_add.get();
        if cursor.index == 0 {
            Ok(cursor.height)
        } else {
            Ok(cursor.height.try_add_one()?)
        }
    }

    pub(crate) async fn remove_event(&mut self, event: &Event) -> Result<(), InboxError> {
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
            self.added_events.delete_front();
            log::trace!("Skipping previously received event {:?}", previous_event);
        }
        // Reconcile the event with the next added event, or mark it as removed.
        match self.added_events.front().await? {
            Some(previous_event) => {
                ensure!(
                    event == &previous_event,
                    InboxError::UnexpectedEvent {
                        previous_event,
                        event: event.clone(),
                    }
                );
                self.added_events.delete_front();
                log::trace!("Consuming event {:?}", event);
            }
            None => {
                log::trace!("Marking event as expected: {:?}", event);
                self.removed_events.push_back(event.clone());
            }
        }
        self.next_cursor_to_remove.set(cursor.try_add_one()?);
        Ok(())
    }

    pub(crate) async fn add_event(&mut self, event: Event) -> Result<(), InboxError> {
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
        match self.removed_events.front().await? {
            Some(previous_event) => {
                if Cursor::from(&previous_event) == cursor {
                    // We already executed this message by anticipation. Remove it from
                    // the queue.
                    ensure!(
                        event.effect == previous_event.effect,
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
                        cursor < Cursor::from(&previous_event),
                        InboxError::UnexpectedEvent {
                            previous_event,
                            event,
                        }
                    );
                }
            }
            None => {
                // Otherwise, schedule the message for execution.
                self.added_events.push_back(event)
            }
        }
        self.next_cursor_to_add.set(cursor.try_add_one()?);
        Ok(())
    }
}

#[cfg(any(test, feature = "test"))]
use {
    linera_views::memory::MemoryContext, linera_views::views::View, std::collections::BTreeMap,
    std::sync::Arc, tokio::sync::Mutex,
};

#[cfg(any(test, feature = "test"))]
impl InboxStateView<MemoryContext<()>>
where
    MemoryContext<()>: Context + Clone + Send + Sync + 'static,
    ViewError: From<<MemoryContext<()> as linera_views::common::Context>::Error>,
{
    pub async fn new() -> Self {
        let guard = Arc::new(Mutex::new(BTreeMap::new())).lock_owned().await;
        let context = MemoryContext::new(guard, ());
        Self::load(context)
            .await
            .expect("Loading from memory should work")
    }
}

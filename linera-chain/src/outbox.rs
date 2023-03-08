// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::data_types::{ArithmeticError, BlockHeight};
use linera_views::{
    common::Context, queue_view::QueueView, register_view::RegisterView, views::ViewError,
};

#[cfg(test)]
#[path = "unit_tests/outbox_tests.rs"]
mod outbox_tests;

/// The state of an outbox
/// * An outbox is used to send messages to another chain.
/// * Internally, this is implemented as a FIFO queue of (increasing) block heights.
/// Messages are contained in blocks, together with destination information, so currently
/// we just send the certified blocks over and let the receivers figure out what were the
/// messages for them.
/// * When marking block heights as received, messages at lower heights are also marked (ie. dequeued).
#[derive(Debug, View, GraphQLView)]
pub struct OutboxStateView<C> {
    /// The minimum block height accepted in the future.
    pub next_height_to_schedule: RegisterView<C, BlockHeight>,
    /// Keep sending these certified blocks of ours until they are acknowledged by
    /// receivers.
    pub queue: QueueView<C, BlockHeight>,
}

impl<C> OutboxStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
{
    pub async fn block_heights(&self) -> Result<Vec<BlockHeight>, ViewError> {
        let count = self.queue.count();
        let heights = self.queue.read_front(count).await?;
        Ok(heights)
    }

    /// Schedule a message at the given height if we haven't already.
    /// Return true if a change was made.
    pub(crate) fn schedule_message(
        &mut self,
        height: BlockHeight,
    ) -> Result<bool, ArithmeticError> {
        if height < *self.next_height_to_schedule.get() {
            return Ok(false);
        }
        self.next_height_to_schedule.set(height.try_add_one()?);
        self.queue.push_back(height);
        Ok(true)
    }

    /// Mark all messages as received up to the given height.
    /// Return true if a change was made.
    pub(crate) async fn mark_messages_as_received(
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

use linera_views::views::GraphQLView;
#[cfg(any(test, feature = "test"))]
use {
    async_lock::Mutex, linera_views::memory::MemoryContext, linera_views::views::View,
    std::collections::BTreeMap, std::sync::Arc,
};

#[cfg(any(test, feature = "test"))]
impl OutboxStateView<MemoryContext<()>>
where
    MemoryContext<()>: Context + Clone + Send + Sync + 'static,
    ViewError: From<<MemoryContext<()> as linera_views::common::Context>::Error>,
{
    pub async fn new() -> Self {
        let guard = Arc::new(Mutex::new(BTreeMap::new())).lock_arc().await;
        let context = MemoryContext::new(guard, ());
        Self::load(context)
            .await
            .expect("Loading from memory should work")
    }
}

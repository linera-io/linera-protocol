// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::data_types::{ArithmeticError, BlockHeight};
#[cfg(with_testing)]
use linera_views::context::{create_test_memory_context, MemoryContext};
use linera_views::{
    context::Context,
    queue_view::QueueView,
    register_view::RegisterView,
    views::{ClonableView, View, ViewError},
};
use log::info; // Import logging for debugging

#[cfg(test)]
#[path = "unit_tests/outbox_tests.rs"]
mod outbox_tests;

/// The state of an outbox.
/// 
/// * An outbox is used to send messages to another chain.
/// * Internally, this is implemented as a FIFO queue of (increasing) block heights.
///   Messages are contained in blocks, together with destination information, so currently,
///   we just send the certified blocks over and let the receivers figure out what the messages are for them.
/// * When marking block heights as received, messages at lower heights are also marked (i.e., dequeued).
#[derive(Debug, ClonableView, View, async_graphql::SimpleObject)]
pub struct OutboxStateView<C>
where
    C: Context + Send + Sync + 'static,
{
    /// The minimum block height accepted in the future.
    pub next_height_to_schedule: RegisterView<C, BlockHeight>,
    
    /// The queue of certified blocks that need to be sent until they are acknowledged by receivers.
    pub queue: QueueView<C, BlockHeight>,
}

impl<C> OutboxStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
{
    /// Schedules a message at the given block height if it hasn't already been scheduled.
    /// Returns `true` if a change was made.
    ///
    /// # Errors
    /// Returns `ArithmeticError` if there is an overflow when incrementing the block height.
    pub(crate) fn schedule_message(
        &mut self,
        height: BlockHeight,
    ) -> Result<bool, ArithmeticError> {
        if height < *self.next_height_to_schedule.get() {
            info!("Skipping scheduling for block height {}: already past", height);
            return Ok(false);
        }

        // Increment the height and schedule the message
        self.next_height_to_schedule.set(height.try_add_one()?);
        self.queue.push_back(height);
        
        info!("Scheduled block height {} successfully", height);
        Ok(true)
    }

    /// Marks all messages as received up to the given block height.
    ///
    /// This method processes all messages in the queue that have a height less than or equal to the provided `height`,
    /// removing them from the queue and returning a list of processed heights.
    /// This ensures that messages are acknowledged in a FIFO manner.
    ///
    /// # Returns
    /// - `Ok(Vec<BlockHeight>)` – a list of heights that were successfully marked as received.
    /// - `Err(ViewError)` – if an error occurs while processing the queue.
    pub(crate) async fn mark_messages_as_received(
        &mut self,
        height: BlockHeight,
    ) -> Result<Vec<BlockHeight>, ViewError> {
        let mut updates = Vec::new();
        while let Some(h) = self.queue.front().await? {
            if h > height {
                break;
            }
            self.queue.delete_front();
            updates.push(h);
        }

        info!("Marked {} messages as received", updates.len());
        Ok(updates)
    }
}

#[cfg(with_testing)]
impl OutboxStateView<MemoryContext<()>>
where
    MemoryContext<()>: Context + Clone + Send + Sync + 'static,
{
    /// Creates a new OutboxStateView with an in-memory test context.
    ///
    /// This is used for testing purposes to simulate the behavior of the outbox.
    pub async fn new() -> Self {
        let context = create_test_memory_context();
        Self::load(context)
            .await
            .expect("Loading from memory should work")
    }

    /// Clears the state of the outbox, including the queue and the next height to schedule.
    pub async fn clear(&mut self) {
        self.queue.clear().await;
        self.next_height_to_schedule.set(BlockHeight::default());
        info!("Outbox state cleared.");
    }
}

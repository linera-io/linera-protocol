// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(with_metrics)]
use std::sync::LazyLock;

use linera_base::data_types::{ArithmeticError, BlockHeight};
#[cfg(with_testing)]
use linera_views::context::{create_test_memory_context, MemoryContext};
use linera_views::{
    context::Context,
    queue_view::QueueView,
    register_view::RegisterView,
    views::{ClonableView, View, ViewError},
};

#[cfg(test)]
#[path = "unit_tests/outbox_tests.rs"]
mod outbox_tests;

#[cfg(with_metrics)]
use {
    linera_base::prometheus_util::{exponential_bucket_interval, register_histogram_vec},
    prometheus::HistogramVec,
};

#[cfg(with_metrics)]
static OUTBOX_SIZE: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "outbox_size",
        "Outbox size",
        &[],
        exponential_bucket_interval(1.0, 10_000.0),
    )
});

/// The state of an outbox
/// * An outbox is used to send messages to another chain.
/// * Internally, this is implemented as a FIFO queue of (increasing) block heights.
///   Messages are contained in blocks, together with destination information, so currently
///   we just send the certified blocks over and let the receivers figure out what were the
///   messages for them.
/// * When marking block heights as received, messages at lower heights are also marked (i.e. dequeued).
#[derive(Debug, ClonableView, View, async_graphql::SimpleObject)]
pub struct OutboxStateView<C>
where
    C: Context + Send + Sync + 'static,
{
    /// The minimum block height accepted in the future.
    pub next_height_to_schedule: RegisterView<C, BlockHeight>,
    /// Keep sending these certified blocks of ours until they are acknowledged by
    /// receivers.
    pub queue: QueueView<C, BlockHeight>,
}

impl<C> OutboxStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
{
    /// Schedules a message at the given height if we haven't already.
    /// Returns true if a change was made.
    pub(crate) fn schedule_message(
        &mut self,
        height: BlockHeight,
    ) -> Result<bool, ArithmeticError> {
        if height < *self.next_height_to_schedule.get() {
            return Ok(false);
        }
        self.next_height_to_schedule.set(height.try_add_one()?);
        self.queue.push_back(height);
        #[cfg(with_metrics)]
        OUTBOX_SIZE
            .with_label_values(&[])
            .observe(self.queue.count() as f64);
        Ok(true)
    }

    /// Marks all messages as received up to the given height.
    /// Returns true if a change was made.
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
        #[cfg(with_metrics)]
        OUTBOX_SIZE
            .with_label_values(&[])
            .observe(self.queue.count() as f64);
        Ok(updates)
    }
}

#[cfg(with_testing)]
impl OutboxStateView<MemoryContext<()>>
where
    MemoryContext<()>: Context + Clone + Send + Sync + 'static,
{
    pub async fn new() -> Self {
        let context = create_test_memory_context();
        Self::load(context)
            .await
            .expect("Loading from memory should work")
    }
}
